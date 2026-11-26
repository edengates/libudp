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

//! The core events that are fired, informing the end-user about the state of the current
//! `Channel`.
//!
//! This module contains all implementations of [`GenericEvent`]. These are specific depending on the event that
//! has been received by the socket.
//!
//! All events are forwarded by the registered [`PacketListenerAdapter`]s. Some events are specific for internal events
//! such as the [`ChannelReadyEvent`] which is only fired when a `Channel` is ready to be used.
//!
//! [`PacketListenerAdapter`]: crate::hooks::PacketListenerAdapter
//! [`ChannelReadyEvent`]: channel::ChannelReadyEvent
pub mod channel {
    //! Events indicating that the state of a `Channel` has changed.
    pub use libev::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};
}
mod event;
pub(crate) use event::Event;
pub use libev::events::GenericEvent;
pub mod packet;
