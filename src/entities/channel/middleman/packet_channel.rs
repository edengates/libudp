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

use crate::entities::ISnowflake;
use crate::entities::channel::concrete::ThreadChannel;
use crate::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};
use crate::events::packet::PacketReceivedEvent;
use crate::events::{Event, GenericEvent};
use crate::requests::Sink;
use crate::requests::sockactions::PacketCreateAction;
use crate::{Packet, PacketManager};
use crate::utils::Buffer;
use crate::utils::EventQueue;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

/// Represents a channel that can have packets sent to it.
///
/// Every registered [`PacketListenerAdapter`] has its own [`PacketChannel`] with the position of the listener.
/// The `on_*` methods fire events to the next listener.
///
/// `N` is the type of packets for the next listener, `P` is the type of packets for the parent channel.
///
/// [`PacketListenerAdapter`]: crate::hooks::PacketListenerAdapter
pub struct PacketChannel<N = Packet, P = Packet> {
    packet_manager: PacketManager,
    buffer: Rc<RefCell<Buffer>>,
    thread_channel: ThreadChannel<N, P>,
    next: EventQueue,
    parent: Rc<dyn Sink<P>>,
    marker: PhantomData<N>,
}

impl<N, P> Clone for PacketChannel<N, P> {
    fn clone(&self) -> Self {
        Self {
            packet_manager: self.packet_manager.clone(),
            buffer: Rc::clone(&self.buffer),
            thread_channel: self.thread_channel.clone(),
            next: Rc::clone(&self.next),
            parent: Rc::clone(&self.parent),
            marker: PhantomData,
        }
    }
}

impl<N, P> PacketChannel<N, P> {
    pub(crate) fn new(
        packet_manager: PacketManager,
        buffer: Rc<RefCell<Buffer>>,
        thread_channel: ThreadChannel<N, P>,
        next: EventQueue,
        parent: Rc<dyn Sink<P>>,
    ) -> Self {
        Self {
            packet_manager,
            buffer,
            thread_channel,
            next,
            parent,
            marker: PhantomData,
        }
    }

    /// Returns the [`PacketManager`] instance of this channel.
    pub fn packet_manager(&self) -> &PacketManager {
        &self.packet_manager
    }

    /// Returns the [`ThreadChannel`] of this channel.
    ///
    /// The [`ThreadChannel`] can be shared with other threads to send packets to this channel.
    pub fn thread_channel(&self) -> &ThreadChannel<N, P> {
        &self.thread_channel
    }

    /// The data mappings to use in [`PacketManager`], shared by all of its channels.
    pub fn context_map(&self) -> &RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>> {
        self.packet_manager.context_map()
    }

    /// Fires a [`ChannelReadyEvent`].
    pub fn on_ready(&mut self) {
        self.handle(Box::new(ChannelReadyEvent));
    }

    /// Passes a received packet on as a [`PacketReceivedEvent`].
    pub fn on_packet_received(&mut self, packet: N)
    where
        N: Any + Send,
    {
        self.handle(Box::new(PacketReceivedEvent::new(packet)));
    }

    /// Tells the next listener that the channel is closed, with a [`ChannelDisconnectEvent`].
    pub fn on_channel_disconnect(&mut self) {
        self.handle(Box::new(ChannelDisconnectEvent));
    }

    /// Fires a [`ChannelTimeoutEvent`] once the [AFK timeout] is reached.
    ///
    /// [AFK timeout]: crate::DefaultPacketManagerBuilder::set_afk_timeout
    pub fn on_channel_timeout(&mut self) {
        self.handle(Box::new(ChannelTimeoutEvent));
    }

    /// Fires a user defined event.
    pub fn on_generic_event(&mut self, event: Box<dyn GenericEvent>) {
        self.handle(Box::new(event));
    }

    fn handle(&mut self, event: Box<dyn Event>) {
        self.next.borrow_mut().push_back(event);
    }

    /// Request to write a packet via this channel.
    ///
    /// The previous listener gets it in [`PacketListenerAdapter::write`], and the first one sends it to the socket as
    /// [`Packet`]. This does not request a flush, call [`flush`] for that.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed.
    ///
    /// [`PacketListenerAdapter::write`]: crate::hooks::PacketListenerAdapter::write
    /// [`flush`]: Self::flush
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    pub fn write(&mut self, packet: P) -> io::Result<()> {
        Sink::write(self, packet)
    }

    /// Whether this channel is in slow mode.
    ///
    /// Slow mode starts when the queue of pending packets is larger than the [`large_threshold`], and ends when it
    /// is smaller than the [`threshold`] again.
    ///
    /// [`large_threshold`]: PacketManager::large_threshold
    /// [`threshold`]: PacketManager::threshold
    pub fn is_locked(&self) -> bool {
        self.buffer.borrow().is_slowmode()
    }

    /// Request to flush all pending packets via this channel.
    ///
    /// [`PacketCreateAction::queue`] calls this method for you.
    ///
    /// [`PacketCreateAction::queue`]: crate::requests::sockactions::SocketAction::queue
    pub fn flush(&mut self) {
        Sink::flush(self);
    }

    /// Send a packet to this channel.
    ///
    /// This requires an additional step to complete the execution, for example when you call
    /// [`PacketCreateAction::queue`].
    ///
    /// [`PacketCreateAction::queue`]: crate::requests::sockactions::SocketAction::queue
    pub fn send_packet(&mut self, packet: P) -> PacketCreateAction<'_, N, P> {
        PacketCreateAction::new(self, packet)
    }

    /// Close down this channel.
    ///
    /// The channel is closed after all pending packets have been sent. After it is closed the channel can not be
    /// used anymore.
    pub fn close(&mut self) {
        Sink::close(self);
    }

    /// Sets whether this channel should be deafened.
    ///
    /// When deafened, no more packets are read from the socket until it is undeafened.
    pub fn set_self_deafened(&mut self, deafened: bool) {
        self.buffer.borrow_mut().set_deafened(deafened);
    }

    /// Whether this channel is deafened.
    pub fn is_self_deafened(&self) -> bool {
        self.buffer.borrow().is_deafened()
    }
}

impl<N, P> ISnowflake for PacketChannel<N, P> {
    fn id(&self) -> u64 {
        self.thread_channel.id()
    }
}

impl<N, P> Sink<P> for PacketChannel<N, P> {
    fn write(&self, packet: P) -> io::Result<()> {
        // If the buffer is shutdown we know the channel was closed and so
        // need to fail the result right away. If it is not shutdown the handling of the rest
        // will be done in execute()
        if !self.buffer.borrow().is_open() {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            self.parent.write(packet)
        }
    }

    fn flush(&self) {
        self.parent.flush();
    }

    fn close(&self) {
        self.parent.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::BufferedSink;
    use crate::requests::sockactions::SocketAction;
    use crate::packet_manager::PacketEventManagerProvider;
    use crate::utils::concurrent::ScheduledThreadPoolExecutor;
    use crate::utils::config::{PacketConfig, ThreadingConfig};
    use crate::utils::{EventManager, EventManagerProxy};
    use bytes::BytesMut;
    use std::time::Duration;

    fn packet_manager() -> PacketManager {
        let provider: PacketEventManagerProvider = Arc::new(|manager, buffer, thread_channel, events| {
            Rc::new(RefCell::new(BufferedSink::new(manager, buffer, thread_channel, events))) as Rc<dyn EventManagerProxy>
        });
        let threading_config = ThreadingConfig {
            socket_pool: ScheduledThreadPoolExecutor::new(1, 64).unwrap(),
            buffer_size: 4096,
            threshold: 32 * 1024,
            large_threshold: 64 * 1024,
            shutdown_timeout: Duration::from_secs(15),
            chunk_limit: 16,
            packet_limit: 1024,
            is_power_suppressed_enabled: false,
        };
        let packet_config = PacketConfig {
            address: None,
            remote_address: None,
            is_address_shared: false,
            is_broadcast_enabled: false,
            receive_buffer_size: None,
            send_buffer_size: None,
            afk_timeout: None,
            timeout: Duration::from_secs(30),
        };
        PacketManager::new(threading_config, packet_config, "127.0.0.1:0".parse().unwrap(), provider)
    }

    #[test]
    fn send() {
        let packet_manager = packet_manager();
        let buffer = Rc::default();
        let port = Arc::new(crate::handle::CompletionService::new_factory()(8).unwrap());
        let thread_channel =
            ThreadChannel::new(0, 0, Arc::new(crate::utils::concurrent::Dispatcher::new(&port).0));
        let head = BufferedSink::new(&packet_manager, &buffer, &thread_channel, &EventQueue::default());
        let mut channel: PacketChannel = PacketChannel::new(
            packet_manager,
            Rc::clone(&buffer),
            thread_channel,
            EventQueue::default(),
            head.channel(),
        );
        let address = "127.0.0.1:1".parse().unwrap();
        channel.send_packet(Packet::new(BytesMut::from(&b"ping"[..]), address)).complete().unwrap();
        channel.close();
        assert!(channel.write(Packet::new(BytesMut::from(&b"pong"[..]), address)).is_err());
        let packets = buffer.borrow_mut().read(usize::MAX);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].address(), address);
    }
}
