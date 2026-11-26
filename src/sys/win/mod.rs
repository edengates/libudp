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

use crate::PacketManager;
use crate::utils::concurrent::ExecutorCompletionService;
use std::io;
use std::net::UdpSocket;
pub(crate) use std::os::windows::io::BorrowedSocket;
use std::sync::mpsc::Receiver;

pub(crate) mod socket;
pub(crate) mod event;
mod scheduled_executor_service;
pub(crate) use scheduled_executor_service::DatagramOperations;
mod packet_socket_channel;
pub use packet_socket_channel::PacketSocketChannel;

pub(crate) fn register_packet(
    pool: &ExecutorCompletionService,
    socket: UdpSocket,
    packet_manager: PacketManager,
) -> Receiver<io::Result<()>> {
    pool.register(move || Box::new(PacketSocketChannel::new(socket, packet_manager)))
}
