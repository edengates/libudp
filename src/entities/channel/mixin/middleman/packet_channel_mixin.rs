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
use crate::events::Event;
use crate::{Packet, PacketManager};
use crate::utils::{EventQueue, EventManagerProxy};
use crate::utils::concurrent::ScheduledExecutorService;
use crate::utils::Buffer;
use libev::ext::ChannelMixin;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;

/// The state shared by the platform specific socket channels, such as the queue of pending packets.
pub struct PacketChannelMixin {
    packet_manager: PacketManager,
    buffer: Rc<RefCell<Buffer>>,
    thread_channel: ThreadChannel<Packet, Packet>,
    events: EventQueue,
    channels: RefCell<Vec<Box<dyn Any>>>,
    top: Cell<Option<Box<dyn Any + Send + Sync>>>,
    proxy: Rc<dyn EventManagerProxy>,
    is_connected: Cell<bool>,
    is_ready: Cell<bool>,
    heartbeat_start_time: Cell<Instant>,
}

impl PacketChannelMixin {
    pub(crate) fn new(packet_manager: PacketManager) -> Self {
        let buffer = Rc::default();
        let thread_channel = ThreadChannel::new(
            ScheduledExecutorService::next_id(),
            0,
            ScheduledExecutorService::dispatcher(),
        );
        let events = EventQueue::default();
        let proxy = packet_manager.proxy(&buffer, &thread_channel, &events);
        Self {
            packet_manager,
            buffer,
            thread_channel,
            events,
            channels: RefCell::new(proxy.channels()),
            top: Cell::new(Some(proxy.thread_channel())),
            proxy,
            is_connected: Cell::new(false),
            is_ready: Cell::new(false),
            heartbeat_start_time: Cell::new(Instant::now()),
        }
    }

    pub(crate) fn packet_manager(&self) -> &PacketManager {
        &self.packet_manager
    }

    pub(crate) fn buffer(&self) -> &RefCell<Buffer> {
        &self.buffer
    }

    pub(crate) fn channels(&self) -> &RefCell<Vec<Box<dyn Any>>> {
        &self.channels
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.is_connected.get()
    }

    pub(crate) fn connect(&self, is_connected: bool) {
        self.is_connected.set(is_connected);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready.get()
    }

    pub(crate) fn ready(self: &Rc<Self>, manager: &mut ScheduledExecutorService) {
        self.is_ready.set(true);
        manager.add_channel(self.thread_channel.id(), self.clone());
        if let Some(top) = self.top.take() {
            self.packet_manager.channel_cache().add(self.thread_channel.id(), top);
        }
        self.handle_event(manager, Box::new(ChannelReadyEvent));
        if let Some(timeout) = self.packet_manager.afk_timeout() {
            manager.setup_keep_alive(timeout);
        }
    }

    pub(crate) fn read(self: &Rc<Self>, manager: &mut ScheduledExecutorService, message: Packet) {
        if self.packet_manager.afk_timeout().is_some() {
            self.heartbeat_start_time.set(Instant::now());
        }
        self.handle_event(manager, Box::new(PacketReceivedEvent::new(message)));
    }

    pub(crate) fn handle_response(&self, size: usize) {
        if self.packet_manager.afk_timeout().is_some() {
            self.heartbeat_start_time.set(Instant::now());
        }
        self.buffer
            .borrow_mut()
            .skip(size, self.packet_manager.threshold(), self.packet_manager.large_threshold());
    }

    pub(crate) fn check_expired(self: &Rc<Self>, manager: &mut ScheduledExecutorService) {
        let Some(timeout) = self.packet_manager.afk_timeout() else {
            return;
        };
        let elapsed = self.heartbeat_start_time.get().elapsed();
        if elapsed >= timeout {
            self.heartbeat_start_time.set(Instant::now());
            self.handle_event(manager, Box::new(ChannelTimeoutEvent));
            manager.setup_keep_alive(timeout);
        } else {
            manager.setup_keep_alive(timeout - elapsed);
        }
    }

    pub(crate) fn close(self: &Rc<Self>, manager: &mut ScheduledExecutorService) {
        if !self.is_ready() {
            return;
        }
        manager.remove_channel(self.thread_channel.id());
        self.packet_manager.channel_cache().remove(self.thread_channel.id());
        self.buffer.borrow_mut().close();
        self.handle_event(manager, Box::new(ChannelDisconnectEvent));
        if self.is_connected() {
            manager.deadline(self.packet_manager.timeout());
        }
    }

    fn handle_event(self: &Rc<Self>, manager: &mut ScheduledExecutorService, event: Box<dyn Event>) {
        self.events.borrow_mut().push_back(event);
        self.wake(manager);
    }

    pub(crate) fn wake(self: &Rc<Self>, manager: &mut ScheduledExecutorService) {
        if !self.is_connected() {
            self.connect(true);
            manager.enqueue(Rc::clone(&self.proxy).handle(Rc::clone(self)));
        }
    }
}

impl ChannelMixin for PacketChannelMixin {
    fn channels(&self) -> &RefCell<Vec<Box<dyn Any>>> {
        PacketChannelMixin::channels(self)
    }

    fn wake(self: Rc<Self>, manager: &mut ScheduledExecutorService) {
        PacketChannelMixin::wake(&self, manager);
    }
}
