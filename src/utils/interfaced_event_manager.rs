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

use crate::entities::channel::middleman::PacketChannel;
use crate::entities::channel::concrete::ThreadChannel;
use crate::hooks::PacketListener;
use crate::requests::Sink;
use crate::{Packet, PacketManager};
use crate::utils::{EventManager, EventQueue};
use crate::utils::Buffer;
use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;

pub struct InterfacedEventManager<P: EventManager, L, O> {
    lower: P,
    listener: L,
    channel: PacketChannel<O, P::Output>,
    events: EventQueue,
    outbound: Rc<Outbound<O>>,
}

impl<P: EventManager, L, O: Any + Send> InterfacedEventManager<P, L, O> {
    pub(crate) fn new(
        lower: P,
        listener: L,
        packet_manager: &PacketManager,
        buffer: &Rc<RefCell<Buffer>>,
        thread_channel: &ThreadChannel<Packet, Packet>,
    ) -> Self {
        let events = EventQueue::default();
        let channel = PacketChannel::new(
            packet_manager.clone(),
            Rc::clone(buffer),
            thread_channel.at_position(lower.position() + 1),
            Rc::clone(&events),
            lower.channel(),
        );
        Self {
            lower,
            listener,
            channel,
            events,
            outbound: Rc::default(),
        }
    }
}

impl<P, L, O> InterfacedEventManager<P, L, O>
where
    P: EventManager,
    L: PacketListener<P::Output, O> + 'static,
    O: Any + Send,
{
    async fn handle_command(&mut self, command: Command<O>) -> io::Result<()> {
        match command {
            Command::Write(message) => self.listener.write(&mut self.channel, message).await,
            Command::Flush => {
                self.channel.flush();
                Ok(())
            }
            Command::Close => self.listener.close(&mut self.channel).await,
        }
    }
}

impl<P, L, O> EventManager for InterfacedEventManager<P, L, O>
where
    P: EventManager,
    L: PacketListener<P::Output, O> + 'static,
    O: Any + Send,
{
    type Output = O;

    fn events(&self) -> &EventQueue {
        &self.events
    }

    fn channel(&self) -> Rc<dyn Sink<O>> {
        Rc::clone(&self.outbound) as Rc<dyn Sink<O>>
    }

    fn position(&self) -> usize {
        self.lower.position() + 1
    }

    fn channels(&self, channels: &mut Vec<Box<dyn Any>>) {
        self.lower.channels(channels);
        channels.push(Box::new(self.channel.clone()));
    }

    fn thread_channel(&self) -> Box<dyn Any + Send + Sync> {
        Box::new(self.channel.thread_channel().clone())
    }

    async fn handle(&mut self) -> io::Result<bool> {
        let mut data = self.lower.handle().await?;
        while let Some(event) = pop_front(self.lower.events()) {
            data = true;
            self.listener.on_event(&mut self.channel, event).await?;
        }
        while let Some(command) = pop_front(&self.outbound.commands) {
            data = true;
            self.handle_command(command).await?;
        }
        Ok(data)
    }

    fn clear(&mut self) {
        self.lower.clear();
        self.events.borrow_mut().clear();
        self.outbound.commands.borrow_mut().clear();
    }
}

fn pop_front<T>(queue: &RefCell<VecDeque<T>>) -> Option<T> {
    queue.borrow_mut().pop_front()
}

enum Command<T> {
    Write(T),
    Flush,
    Close,
}

struct Outbound<T> {
    commands: RefCell<VecDeque<Command<T>>>,
}

impl<T> Default for Outbound<T> {
    fn default() -> Self {
        Self {
            commands: RefCell::default(),
        }
    }
}

impl<T> Sink<T> for Outbound<T> {
    fn write(&self, message: T) -> io::Result<()> {
        self.commands.borrow_mut().push_back(Command::Write(message));
        Ok(())
    }

    fn flush(&self) {
        self.commands.borrow_mut().push_back(Command::Flush);
    }

    fn close(&self) {
        self.commands.borrow_mut().push_back(Command::Close);
    }
}
