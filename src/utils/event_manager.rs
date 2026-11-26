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

use crate::entities::channel::mixin::middleman::PacketChannelMixin;
use crate::requests::Sink;
use crate::events::Event;
use futures_core::future::LocalBoxFuture;
use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::rc::Rc;

pub type EventQueue = Rc<RefCell<VecDeque<Box<dyn Event>>>>;

/// An interface for libudp's event manager system.
#[allow(async_fn_in_trait)]
pub trait EventManager: 'static {
    type Output: Any + Send;

    fn events(&self) -> &EventQueue;

    fn channel(&self) -> Rc<dyn Sink<Self::Output>>;

    fn position(&self) -> usize;

    fn channels(&self, channels: &mut Vec<Box<dyn Any>>);

    fn thread_channel(&self) -> Box<dyn Any + Send + Sync>;

    async fn handle(&mut self) -> io::Result<bool>;

    fn clear(&mut self);
}

pub trait EventManagerProxy {
    fn channels(&self) -> Vec<Box<dyn Any>>;

    fn thread_channel(&self) -> Box<dyn Any + Send + Sync>;

    fn handle(self: Rc<Self>, channel: Rc<PacketChannelMixin>) -> LocalBoxFuture<'static, ()>;
}

impl<P: EventManager> EventManagerProxy for RefCell<P> {
    fn channels(&self) -> Vec<Box<dyn Any>> {
        let mut channels = Vec::new();
        self.borrow().channels(&mut channels);
        channels
    }

    fn thread_channel(&self) -> Box<dyn Any + Send + Sync> {
        self.borrow().thread_channel()
    }

    #[allow(clippy::await_holding_refcell_ref)]
    fn handle(self: Rc<Self>, channel: Rc<PacketChannelMixin>) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move {
            let mut data = Ok(true);
            while let Ok(true) = data {
                data = self.borrow_mut().handle().await;
                self.borrow().events().borrow_mut().clear();
            }
            if data.is_err() {
                channel.buffer().borrow_mut().close();
                self.borrow_mut().clear();
            }
            channel.connect(false);
        })
    }
}

pub(crate) fn cast<A: Any, B: Any>(value: A) -> Option<B> {
    (&mut Some(value) as &mut dyn Any).downcast_mut::<Option<B>>().and_then(Option::take)
}

#[cfg(test)]
mod tests {
    #[test]
    fn cast() {
        assert_eq!(super::cast::<u32, u32>(7), Some(7));
        assert_eq!(super::cast::<u32, String>(7), None);
    }
}
