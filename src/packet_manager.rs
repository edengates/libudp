/*
 * Copyright 2026 Ada Morgan, John Regan and the libev contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::entities::channel::concrete::ThreadChannel;
use crate::Packet;
use crate::utils::cache::ChannelCacheView;
use crate::utils::concurrent::ScheduledThreadPoolExecutor;
use crate::utils::config::{PacketConfig, ThreadingConfig};
use crate::utils::{Buffer, EventManagerProxy, EventQueue};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub(crate) type PacketEventManagerProvider = Arc<
    dyn Fn(&PacketManager, &Rc<RefCell<Buffer>>, &ThreadChannel<Packet, Packet>, &EventQueue) -> Rc<dyn EventManagerProxy>
        + Send
        + Sync,
>;

/// The core of the UDP support. Acts as a manager for the channel of a UDP socket.
///
/// To create new instances use the [`DefaultPacketManagerBuilder`](crate::DefaultPacketManagerBuilder).
#[derive(Clone)]
pub struct PacketManager {
    inner: Arc<PacketManagerImpl>,
}

struct PacketManagerImpl {
    threading_config: ThreadingConfig,
    packet_config: PacketConfig,
    address: SocketAddr,
    provider: PacketEventManagerProvider,
    context_map: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>,
    channel_cache: ChannelCacheView,
}

impl PacketManager {
    pub(crate) fn new(
        threading_config: ThreadingConfig,
        packet_config: PacketConfig,
        address: SocketAddr,
        provider: PacketEventManagerProvider,
    ) -> Self {
        Self {
            inner: Arc::new(PacketManagerImpl {
                threading_config,
                packet_config,
                address,
                provider,
                context_map: RwLock::default(),
                channel_cache: ChannelCacheView::default(),
            }),
        }
    }

    /// The local address the socket is bound to.
    ///
    /// If the socket was requested with port `0`, this has the port that the operating system assigned.
    pub fn local_address(&self) -> SocketAddr {
        self.inner.address
    }

    /// The peer address the socket is connected to, or `None` if not connected.
    pub fn remote_address(&self) -> Option<SocketAddr> {
        self.inner.packet_config.remote_address()
    }

    /// Whether `SO_REUSEPORT` is set on the socket.
    pub fn is_address_shared(&self) -> bool {
        self.inner.packet_config.is_address_shared()
    }

    /// Whether the `SO_BROADCAST` option is set on the socket.
    pub fn is_broadcast_enabled(&self) -> bool {
        self.inner.packet_config.is_broadcast_enabled()
    }

    /// The `SO_RCVBUF` size, or `None` for the value of the operating system.
    pub fn receive_buffer_size(&self) -> Option<usize> {
        self.inner.packet_config.receive_buffer_size()
    }

    /// The `SO_SNDBUF` size, or `None` for the value of the operating system.
    pub fn send_buffer_size(&self) -> Option<usize> {
        self.inner.packet_config.send_buffer_size()
    }

    /// The AFK timeout of the channel, or `None` if disabled.
    pub fn afk_timeout(&self) -> Option<Duration> {
        self.inner.packet_config.afk_timeout()
    }

    /// How long the listeners of a closed channel may still run.
    pub fn timeout(&self) -> Duration {
        self.inner.packet_config.timeout()
    }

    /// The pool used for the socket.
    pub fn socket_pool(&self) -> &ScheduledThreadPoolExecutor {
        self.inner.threading_config.socket_pool()
    }

    /// The read buffer size in bytes.
    pub fn buffer_size(&self) -> usize {
        self.inner.threading_config.buffer_size()
    }

    /// The low water mark of the write buffer, where slow mode ends.
    pub fn threshold(&self) -> usize {
        self.inner.threading_config.threshold()
    }

    /// The high water mark of the write buffer, where slow mode starts.
    pub fn large_threshold(&self) -> usize {
        self.inner.threading_config.large_threshold()
    }

    /// How long [`shutdown`](Self::shutdown) and a closed channel wait for pending packets.
    pub fn shutdown_timeout(&self) -> Duration {
        self.inner.threading_config.shutdown_timeout()
    }

    /// The maximum number of packets to read per read loop.
    pub fn chunk_limit(&self) -> usize {
        self.inner.threading_config.chunk_limit()
    }

    /// The maximum number of packets to send per batch.
    pub fn packet_limit(&self) -> usize {
        self.inner.threading_config.packet_limit()
    }

    /// Whether the system may suppress the power of the pool threads to save energy.
    pub fn is_power_suppressed_enabled(&self) -> bool {
        self.inner.threading_config.is_power_suppressed_enabled()
    }

    /// The data mappings to use in this [`PacketManager`], shared by all its channels.
    pub fn context_map(&self) -> &RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>> {
        &self.inner.context_map
    }

    /// [`ChannelCacheView`] of all cached channels of this [`PacketManager`].
    pub fn channel_cache(&self) -> &ChannelCacheView {
        &self.inner.channel_cache
    }

    /// Gets all channels of this [`PacketManager`].
    ///
    /// This copies the backing store into a list. This means every call creates a new list with O(n) complexity. It is
    /// recommended to store this into a local variable or use [`channel_cache`](Self::channel_cache) and use its more
    /// efficient versions of handling these values.
    pub fn channels<N: 'static, P: 'static>(&self) -> Vec<ThreadChannel<N, P>> {
        self.inner.channel_cache.to_vec()
    }

    /// Gets a channel that has the same id as the one provided.
    ///
    /// If there is no channel with an id that matches the provided one, then this returns `None`.
    pub fn get_channel_by_id<N: 'static, P: 'static>(&self, id: u64) -> Option<ThreadChannel<N, P>> {
        self.inner.channel_cache.get_element_by_id(id)
    }

    pub(crate) fn proxy(
        &self,
        buffer: &Rc<RefCell<Buffer>>,
        thread_channel: &ThreadChannel<Packet, Packet>,
        inbound: &EventQueue,
    ) -> Rc<dyn EventManagerProxy> {
        (self.inner.provider)(self, buffer, thread_channel, inbound)
    }

    /// Shuts down this [`PacketManager`], closing its channel.
    ///
    /// After this command is issued the [`PacketManager`] can not be used anymore. Already enqueued packets are still
    /// going to be sent, for at most the [`shutdown_timeout`](Self::shutdown_timeout). If you want this instance to
    /// shut down without sending, use [`shutdown_now`](Self::shutdown_now).
    pub fn shutdown(&self) {
        self.inner.threading_config.shutdown()
    }

    /// Shuts down this [`PacketManager`] instantly, closing its channel.
    ///
    /// This will also cancel all queued packets.
    ///
    /// # Errors
    ///
    /// Returns an error if one of the shards could not be shut down.
    pub fn shutdown_now(&self) -> io::Result<()> {
        self.inner.threading_config.shutdown_now()
    }

    /// Blocks the current thread until the shutdown has completed.
    ///
    /// This will not implicitly call [`shutdown`](Self::shutdown), you are responsible to ensure that the shutdown
    /// process has started. Returns `false` if the `timeout` elapsed first.
    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        self.inner.threading_config.await_shutdown(timeout)
    }
}

pub(crate) fn sync(data: Receiver<io::Result<()>>) -> io::Result<()> {
    data.recv().map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
}
