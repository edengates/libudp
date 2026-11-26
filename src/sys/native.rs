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
use crate::utils::concurrent::{ExecutorCompletionService, ScheduledExecutorService};
use std::io;
use std::net::UdpSocket;
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
use std::os::fd::AsRawFd;
use std::sync::mpsc::Receiver;

#[cfg(target_os = "linux")]
pub(crate) mod event {
    pub(crate) use libev::ext::Event;
}
#[cfg(target_os = "linux")]
#[path = "uring/mod.rs"]
pub(crate) mod uring;
#[path = "socket.rs"]
pub(crate) mod socket;
#[path = "packet_socket_channel.rs"]
mod packet_socket_channel;
pub use packet_socket_channel::PacketSocketChannel;

#[cfg(target_os = "linux")]
pub(crate) const READABLE: u32 = rustix::event::epoll::EventFlags::IN.bits();
#[cfg(target_os = "linux")]
pub(crate) const WRITABLE: u32 = rustix::event::epoll::EventFlags::OUT.bits();
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) const READABLE: u32 = 1;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) const WRITABLE: u32 = 2;

#[cfg(target_os = "linux")]
pub(crate) fn submit<S: AsFd>(manager: &mut ScheduledExecutorService, socket: &S, flags: u32, _flag: u32) -> io::Result<()> {
    manager.submit(socket, flags)
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) fn submit<S: AsRawFd>(manager: &mut ScheduledExecutorService, socket: &S, flags: u32, flag: u32) -> io::Result<()> {
    let filter = if flag == WRITABLE {
        rustix::event::kqueue::EventFilter::Write
    } else {
        rustix::event::kqueue::EventFilter::Read
    };
    manager.submit(socket, filter, flags & flag != 0)
}

pub(crate) fn register_packet(
    pool: &ExecutorCompletionService,
    socket: UdpSocket,
    packet_manager: PacketManager,
) -> Receiver<io::Result<()>> {
    #[cfg(target_os = "linux")]
    if pool.port().is_io_uring() {
        return pool.register(move || Box::new(uring::PacketSocketChannel::new(socket, packet_manager)));
    }
    pool.register(move || Box::new(PacketSocketChannel::new(socket, packet_manager)))
}
