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

use crate::events::GenericEvent;
use crate::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};

pub trait Event: GenericEvent {
    fn identifier(&self) -> usize;
}

impl Event for Box<dyn GenericEvent> {
    fn identifier(&self) -> usize {
        0
    }
}

impl Event for ChannelReadyEvent {
    fn identifier(&self) -> usize {
        Self::IDENTIFIER
    }
}

impl Event for ChannelDisconnectEvent {
    fn identifier(&self) -> usize {
        Self::IDENTIFIER
    }
}

impl Event for ChannelTimeoutEvent {
    fn identifier(&self) -> usize {
        Self::IDENTIFIER
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::channel::ChannelReadyEvent;
    use crate::events::packet::PacketReceivedEvent;
    use std::any::Any;

    struct CustomEvent;

    impl GenericEvent for CustomEvent {}

    #[test]
    fn identifier() {
        assert_eq!(ChannelReadyEvent.identifier(), ChannelReadyEvent::IDENTIFIER);

        let event: Box<dyn Event> = Box::new(PacketReceivedEvent::new(7u32));
        assert_eq!(event.identifier(), PacketReceivedEvent::IDENTIFIER);
        assert_eq!((event as Box<dyn Any>).downcast::<PacketReceivedEvent<u32>>().unwrap().packet(), 7);

        let event: Box<dyn Event> = Box::new(Box::new(CustomEvent) as Box<dyn GenericEvent>);
        assert_eq!(event.identifier(), 0);
        let event = (event as Box<dyn Any>).downcast::<Box<dyn GenericEvent>>().unwrap();
        assert!((**event).is::<CustomEvent>());
    }
}
