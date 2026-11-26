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
use crate::events::Event;
use std::any::Any;
use std::io;

/// An interface for receiving events and packets of a [`PacketChannel`].
///
/// Implement [`PacketListenerAdapter`] instead to have the events divided for you.
///
/// [`PacketListenerAdapter`]: crate::hooks::PacketListenerAdapter
#[allow(async_fn_in_trait)]
pub trait PacketListener<I: Any + Send, O: Any + Send>: Send {
    /// Handles an event of the channel.
    async fn on_event(&mut self, channel: &mut PacketChannel<O, I>, event: Box<dyn Event>) -> io::Result<()>;

    /// Handles a packet from the next listener.
    async fn write(&mut self, channel: &mut PacketChannel<O, I>, packet: O) -> io::Result<()>;

    /// Handles [`PacketChannel::close`] of the next listener.
    async fn close(&mut self, channel: &mut PacketChannel<O, I>) -> io::Result<()>;
}
