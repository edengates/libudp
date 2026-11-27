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

//! An asynchronous event-driven UDP application framework for rapid development of maintainable high performance
//! protocol servers & clients.
//!
//! You can use [`DefaultPacketManagerBuilder`] to create a [`PacketManager`] instance.
pub mod entities;
pub mod events;
pub mod handle;
pub mod hooks;
pub mod requests;
pub mod utils;

mod packet;
pub use packet::Packet;
pub use entities::channel::middleman::DatagramChannel;
pub use requests::sockactions::PacketCreateAction;
pub(crate) mod packet_manager;
pub use packet_manager::PacketManager;
mod default_packet_manager_builder;
pub use default_packet_manager_builder::DefaultPacketManagerBuilder;
