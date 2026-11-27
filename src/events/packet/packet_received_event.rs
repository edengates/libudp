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

use crate::events::{Event, GenericEvent};
use crate::Packet;
use std::any::Any;

/// Indicates that a packet was received in a [`DatagramChannel`].
///
/// [`DatagramChannel`]: crate::entities::channel::middleman::DatagramChannel
pub struct PacketReceivedEvent<T = Packet> {
    packet: T,
}

impl PacketReceivedEvent {
    pub const IDENTIFIER: usize = 1 << 1;
}

impl<T> PacketReceivedEvent<T> {
    pub(crate) fn new(packet: T) -> Self {
        Self { packet }
    }

    /// The received packet.
    pub fn packet(self) -> T {
        self.packet
    }
}

impl<T: Any + Send> GenericEvent for PacketReceivedEvent<T> {}

impl<T: Any + Send> Event for PacketReceivedEvent<T> {
    fn identifier(&self) -> usize {
        PacketReceivedEvent::IDENTIFIER
    }
}
