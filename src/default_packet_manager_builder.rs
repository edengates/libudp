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
use crate::handle::{self, socket};
use crate::requests::BufferedSink;
use crate::hooks::PacketListener;
use crate::packet_manager::{PacketEventManagerProvider, sync};
use crate::{Packet, PacketManager};
use crate::utils::concurrent::ScheduledThreadPoolExecutor;
use crate::utils::config::{PacketConfig, ThreadingConfig};
use crate::utils::{Buffer, EventManager, EventManagerProxy, EventQueue, InterfacedEventManager};
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

type PacketPipelineFactory<P> = Arc<
    dyn Fn(&PacketManager, &Rc<RefCell<Buffer>>, &ThreadChannel<Packet, Packet>, &EventQueue) -> P
        + Send
        + Sync,
>;

/// Used to create new instances of [`PacketManager`].
pub struct DefaultPacketManagerBuilder<P = Rc<BufferedSink>> {
    socket_pool: Option<ScheduledThreadPoolExecutor>,
    factory: PacketPipelineFactory<P>,
    listeners: usize,
    address: Option<SocketAddr>,
    remote_address: Option<SocketAddr>,
    is_address_shared: bool,
    is_broadcast_enabled: bool,
    receive_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    buffer_size: usize,
    shutdown_timeout: Duration,
    chunk_limit: usize,
    packet_limit: usize,
    is_power_suppressed_enabled: bool,
    afk_timeout: Option<Duration>,
    timeout: Duration,
}

impl Default for DefaultPacketManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultPacketManagerBuilder {
    /// Creates a [`DefaultPacketManagerBuilder`] with recommended default settings.
    ///
    /// Note that these defaults can potentially change in the future.
    pub fn new() -> Self {
        Self {
            socket_pool: None,
            factory: Arc::new(BufferedSink::new),
            listeners: 0,
            address: None,
            remote_address: None,
            is_address_shared: false,
            is_broadcast_enabled: false,
            receive_buffer_size: None,
            send_buffer_size: None,
            buffer_size: 4096,
            shutdown_timeout: Duration::from_secs(15),
            chunk_limit: 16,
            packet_limit: 1024,
            is_power_suppressed_enabled: false,
            afk_timeout: None,
            timeout: Duration::from_secs(30),
        }
    }
}

impl<P: EventManager> DefaultPacketManagerBuilder<P> {
    /// Sets the local address the socket is bound to.
    ///
    /// Default: the unspecified address of the family of the [`remote_address`](Self::set_remote_address) and port `0`
    pub fn set_local_address(mut self, address: SocketAddr) -> Self {
        self.address = Some(address);
        self
    }

    /// Sets the peer address the socket is connected to.
    ///
    /// A connected socket only receives the packets of this address.
    ///
    /// Default: `None` (not connected)
    pub fn set_remote_address(mut self, remote_address: SocketAddr) -> Self {
        self.remote_address = Some(remote_address);
        self
    }

    /// Sets value for the `SO_REUSEPORT` option on the socket.
    ///
    /// This allows multiple threads to bind to the same port. On Windows this uses `SO_REUSEADDR` instead of
    /// `SO_EXCLUSIVEADDRUSE`. On FreeBSD this uses `SO_REUSEPORT_LB` instead, so incoming packets are load balanced.
    ///
    /// Default: `false`
    pub fn set_address_shared(mut self, is_address_shared: bool) -> Self {
        self.is_address_shared = is_address_shared;
        self
    }

    /// Sets value for the `SO_BROADCAST` option on the socket.
    ///
    /// Default: `false`
    pub fn set_broadcast_enabled(mut self, is_broadcast_enabled: bool) -> Self {
        self.is_broadcast_enabled = is_broadcast_enabled;
        self
    }

    /// Sets value for the `SO_RCVBUF` option on the socket.
    ///
    /// Changes the size of the operating system's receive buffer associated with the socket.
    ///
    /// Default: `None` (the value of the operating system)
    pub fn set_receive_buffer_size(mut self, receive_buffer_size: usize) -> Self {
        self.receive_buffer_size = Some(receive_buffer_size);
        self
    }

    /// Sets value for the `SO_SNDBUF` option on the socket.
    ///
    /// Changes the size of the operating system's send buffer associated with the socket.
    ///
    /// Default: `None` (the value of the operating system)
    pub fn set_send_buffer_size(mut self, send_buffer_size: usize) -> Self {
        self.send_buffer_size = Some(send_buffer_size);
        self
    }

    /// Sets the size, in bytes, of the buffer used to read packets.
    ///
    /// Every read reserves this size. A packet that is larger is cut off.
    ///
    /// Default: `4096`
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Sets how long the channel waits for pending packets to be sent when it is closed or shut down.
    ///
    /// Default: `15` seconds
    pub fn set_shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    /// Sets the maximum number of packets to read per read loop.
    ///
    /// Default: `16`
    pub fn set_chunk_limit(mut self, chunk_limit: usize) -> Self {
        self.chunk_limit = chunk_limit;
        self
    }

    /// Sets the maximum number of packets to send per batch.
    ///
    /// Default: `1024`
    pub fn set_packet_limit(mut self, packet_limit: usize) -> Self {
        self.packet_limit = packet_limit;
        self
    }

    /// Allows the system to suppress the power of the threads of the [`socket_pool`](Self::set_socket_pool) to save
    /// energy.
    ///
    /// If `false`, Windows is asked to run these threads at full speed, so they are not moved to the efficiency cores
    /// and slowed down under load. Other platforms ignore this setting.
    ///
    /// Default: `false`
    pub fn set_power_suppressed_enabled(mut self, enable: bool) -> Self {
        self.is_power_suppressed_enabled = enable;
        self
    }

    /// Sets the AFK timeout of the channel.
    ///
    /// A [`ChannelTimeoutEvent`] is fired after each period of this length without read or write.
    ///
    /// Default: `None` (disabled)
    ///
    /// [`ChannelTimeoutEvent`]: crate::events::channel::ChannelTimeoutEvent
    pub fn set_afk_timeout(mut self, afk_timeout: Duration) -> Self {
        self.afk_timeout = Some(afk_timeout);
        self
    }

    /// Sets how long the listeners of a closed channel may still handle the remaining events.
    ///
    /// The time starts with the [`ChannelDisconnectEvent`], after it the processing is canceled.
    ///
    /// Default: `30` seconds
    ///
    /// [`ChannelDisconnectEvent`]: crate::events::channel::ChannelDisconnectEvent
    pub fn set_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the pool used for the socket.
    ///
    /// Default: Shared [`ScheduledThreadPoolExecutor`] with (`2 *` [`available_parallelism`]) shards.
    ///
    /// [`available_parallelism`]: std::thread::available_parallelism
    pub fn set_socket_pool(mut self, socket_pool: ScheduledThreadPoolExecutor) -> Self {
        self.socket_pool = Some(socket_pool);
        self
    }

    /// Adds the listener to the list of listeners that will be used to populate the [`PacketManager`] object.
    ///
    /// The channel uses its own clone of the listener. Listeners are called in the order they are added.
    pub fn add_event_listener<L, O>(
        self,
        listener: L,
    ) -> DefaultPacketManagerBuilder<InterfacedEventManager<P, L, O>>
    where
        L: PacketListener<P::Output, O> + Clone + Sync + 'static,
        O: Any + Send,
    {
        self.add_event_listener_provider(move || listener.clone())
    }

    /// Like [`add_event_listener`](Self::add_event_listener), but the provider is called once for each new channel.
    pub fn add_event_listener_provider<F, L, O>(
        self,
        provider: F,
    ) -> DefaultPacketManagerBuilder<InterfacedEventManager<P, L, O>>
    where
        F: Fn() -> L + Send + Sync + 'static,
        L: PacketListener<P::Output, O> + 'static,
        O: Any + Send,
    {
        let factory = Arc::clone(&self.factory);
        self.set_factory(Arc::new(move |packet_manager, buffer, thread_channel, inbound| {
            let lower = factory(packet_manager, buffer, thread_channel, inbound);
            InterfacedEventManager::new(lower, provider(), packet_manager, buffer, thread_channel)
        }))
    }

    fn set_factory<Q>(self, factory: PacketPipelineFactory<Q>) -> DefaultPacketManagerBuilder<Q> {
        DefaultPacketManagerBuilder {
            socket_pool: self.socket_pool,
            factory,
            listeners: self.listeners + 1,
            address: self.address,
            remote_address: self.remote_address,
            is_address_shared: self.is_address_shared,
            is_broadcast_enabled: self.is_broadcast_enabled,
            receive_buffer_size: self.receive_buffer_size,
            send_buffer_size: self.send_buffer_size,
            buffer_size: self.buffer_size,
            shutdown_timeout: self.shutdown_timeout,
            chunk_limit: self.chunk_limit,
            packet_limit: self.packet_limit,
            is_power_suppressed_enabled: self.is_power_suppressed_enabled,
            afk_timeout: self.afk_timeout,
            timeout: self.timeout,
        }
    }

    /// Builds a new [`PacketManager`] instance, which binds its socket.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidInput`] if no listener was added, or if the default socket pool cannot be created or the
    /// socket cannot be bound.
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    pub fn build(self) -> io::Result<PacketManager> {
        if self.listeners == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let factory = self.factory;
        let provider: PacketEventManagerProvider = Arc::new(move |packet_manager, buffer, thread_channel, inbound| {
            Rc::new(RefCell::new(factory(packet_manager, buffer, thread_channel, inbound))) as Rc<dyn EventManagerProxy>
        });
        let socket_pool = match self.socket_pool {
            Some(socket_pool) => socket_pool,
            None => ScheduledThreadPoolExecutor::new(0, 64)?,
        };
        let threading_config = ThreadingConfig {
            socket_pool,
            buffer_size: self.buffer_size,
            threshold: 32 * 1024,
            large_threshold: 64 * 1024,
            shutdown_timeout: self.shutdown_timeout,
            chunk_limit: self.chunk_limit,
            packet_limit: self.packet_limit,
            is_power_suppressed_enabled: self.is_power_suppressed_enabled,
        };
        #[cfg(windows)]
        for shard in threading_config.socket_pool().shards() {
            let is_power_suppressed_enabled = threading_config.is_power_suppressed_enabled();
            shard.execute(move || handle::set_power_suppressed(is_power_suppressed_enabled))?;
        }
        let packet_config = PacketConfig {
            address: self.address,
            remote_address: self.remote_address,
            is_address_shared: self.is_address_shared,
            is_broadcast_enabled: self.is_broadcast_enabled,
            receive_buffer_size: self.receive_buffer_size,
            send_buffer_size: self.send_buffer_size,
            afk_timeout: self.afk_timeout,
            timeout: self.timeout,
        };
        let socket = socket::packet(&packet_config)?;
        let address = socket.local_addr()?;
        let packet_manager = PacketManager::new(threading_config, packet_config, address, provider);
        let pool = packet_manager.socket_pool().provide();
        sync(handle::register_packet(pool, socket, packet_manager.clone()))?;
        Ok(packet_manager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::sockactions::SocketAction;
    use crate::hooks::PacketListenerAdapter;
    use crate::DatagramChannel;
    use std::net::UdpSocket;

    #[derive(Clone)]
    struct EchoAdapter;

    impl PacketListenerAdapter for EchoAdapter {
        async fn on_packet_received(&mut self, channel: &mut DatagramChannel, packet: Packet) -> io::Result<()> {
            channel.send_packet(packet).complete()
        }
    }

    fn pool() -> ScheduledThreadPoolExecutor {
        #[cfg(target_os = "linux")]
        return ScheduledThreadPoolExecutor::with_factory(1, 64, crate::handle::CompletionService::epoll_factory())
            .unwrap();
        #[cfg(not(target_os = "linux"))]
        ScheduledThreadPoolExecutor::new(1, 64).unwrap()
    }

    #[test]
    fn build() {
        assert_eq!(
            DefaultPacketManagerBuilder::new().build().err().map(|error| error.kind()),
            Some(io::ErrorKind::InvalidInput)
        );
    }

    #[test]
    fn bind() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote_address = peer.local_addr().unwrap();
        let manager = DefaultPacketManagerBuilder::new()
            .set_socket_pool(pool())
            .set_local_address("127.0.0.1:0".parse().unwrap())
            .set_remote_address(remote_address)
            .set_address_shared(true)
            .set_broadcast_enabled(true)
            .set_receive_buffer_size(65536)
            .set_send_buffer_size(32768)
            .set_buffer_size(8192)
            .set_afk_timeout(Duration::from_secs(5))
            .add_event_listener(EchoAdapter)
            .build()
            .unwrap();

        assert_ne!(manager.local_address().port(), 0);
        assert_eq!(manager.remote_address(), Some(remote_address));
        assert!(manager.is_address_shared());
        assert!(manager.is_broadcast_enabled());
        assert_eq!(manager.receive_buffer_size(), Some(65536));
        assert_eq!(manager.send_buffer_size(), Some(32768));
        assert_eq!(manager.buffer_size(), 8192);
        assert_eq!(manager.afk_timeout(), Some(Duration::from_secs(5)));
        manager.shutdown_now().unwrap();
    }

    #[test]
    fn echo() {
        exchange(pool());
    }

    #[test]
    fn native() {
        exchange(ScheduledThreadPoolExecutor::new(1, 64).unwrap());
    }

    fn exchange(pool: ScheduledThreadPoolExecutor) {
        let manager = DefaultPacketManagerBuilder::new()
            .set_socket_pool(pool)
            .set_local_address("127.0.0.1:0".parse().unwrap())
            .add_event_listener(EchoAdapter)
            .build()
            .unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        client.send_to(b"ping", manager.local_address()).unwrap();

        let mut data = [0; 16];
        let (length, sender) = client.recv_from(&mut data).unwrap();
        assert_eq!(&data[..length], b"ping");
        assert_eq!(sender, manager.local_address());
        manager.shutdown_now().unwrap();
    }
}
