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
use crate::entities::channel::middleman::DatagramChannel;
use crate::{Packet, PacketManager};
use crate::requests::Sink;
use crate::utils::{EventManager, EventQueue};
use crate::utils::Buffer;
use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

pub struct BufferedSink {
    packet_manager: PacketManager,
    buffer: Rc<RefCell<Buffer>>,
    thread_channel: ThreadChannel<Packet, Packet>,
    events: EventQueue,
}

impl BufferedSink {
    pub(crate) fn new(
        packet_manager: &PacketManager,
        buffer: &Rc<RefCell<Buffer>>,
        thread_channel: &ThreadChannel<Packet, Packet>,
        events: &EventQueue,
    ) -> Rc<Self> {
        Rc::new(Self {
            packet_manager: packet_manager.clone(),
            buffer: Rc::clone(buffer),
            thread_channel: thread_channel.at_position(0),
            events: Rc::clone(events),
        })
    }
}

impl Sink<Packet> for BufferedSink {
    fn write(&self, packet: Packet) -> io::Result<()> {
        self.buffer
            .borrow_mut()
            .write(packet, self.packet_manager.threshold(), self.packet_manager.large_threshold())
    }

    fn flush(&self) {
        self.buffer.borrow_mut().emit();
    }

    fn close(&self) {
        self.buffer.borrow_mut().close();
    }
}

impl EventManager for Rc<BufferedSink> {
    type Output = Packet;

    fn events(&self) -> &EventQueue {
        &self.events
    }

    fn channel(&self) -> Rc<dyn Sink<Packet>> {
        Rc::clone(self) as Rc<dyn Sink<Packet>>
    }

    fn position(&self) -> usize {
        0
    }

    fn channels(&self, channels: &mut Vec<Box<dyn Any>>) {
        channels.push(Box::new(DatagramChannel::new(
            self.packet_manager.clone(),
            Rc::clone(&self.buffer),
            self.thread_channel.clone(),
            Rc::clone(&self.events),
            self.channel(),
        )));
    }

    fn thread_channel(&self) -> Box<dyn Any + Send + Sync> {
        Box::new(self.thread_channel.clone())
    }

    async fn handle(&mut self) -> io::Result<bool> {
        Ok(false)
    }

    fn clear(&mut self) {
        self.events.borrow_mut().clear();
    }
}
