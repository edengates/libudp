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
use crate::handle::socket;
use crate::handle::{self, Completion, READABLE, SocketHandler, WRITABLE};
use crate::{Packet, PacketManager};
use crate::utils::concurrent::ScheduledExecutorService;
use std::collections::VecDeque;
use std::io;
use std::net::UdpSocket;
use std::os::fd::{AsFd, BorrowedFd};
use std::rc::Rc;

pub struct DatagramSocketChannel {
    socket: UdpSocket,
    channel: Rc<PacketChannelMixin>,
    has_remote: bool,
    pending: VecDeque<Packet>,
    flags: u32,
    closing: bool,
}

impl DatagramSocketChannel {
    pub(crate) fn new(socket: UdpSocket, packet_manager: PacketManager) -> Self {
        let has_remote = packet_manager.remote_address().is_some();
        Self {
            socket,
            channel: Rc::new(PacketChannelMixin::new(packet_manager)),
            has_remote,
            pending: VecDeque::new(),
            flags: 0,
            closing: false,
        }
    }

    fn enable_flag(&mut self, manager: &mut ScheduledExecutorService, flag: u32) -> io::Result<()> {
        if self.flags & flag != 0 {
            return Ok(());
        }
        self.flags |= flag;
        self.submit(manager, flag)
    }

    fn disable_flag(&mut self, manager: &mut ScheduledExecutorService, flag: u32) -> io::Result<()> {
        if self.flags & flag == 0 {
            return Ok(());
        }
        self.flags &= !flag;
        self.submit(manager, flag)
    }

    fn submit(&self, manager: &mut ScheduledExecutorService, flag: u32) -> io::Result<()> {
        handle::submit(manager, &self.socket, self.flags, flag)
    }

    fn ready(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.channel.ready(manager);
        self.execute(manager)
    }

    fn next_packet(&mut self) {
        let limit = self.channel.packet_manager().packet_limit();
        self.pending.extend(self.channel.buffer().borrow_mut().read(limit));
    }

    fn send(&mut self) -> io::Result<bool> {
        if self.pending.is_empty() {
            self.next_packet();
            if self.pending.is_empty() {
                return Ok(false);
            }
        }
        while let Some(packet) = self.pending.front() {
            let data = if self.has_remote {
                self.socket.send(packet.data())
            } else {
                self.socket.send_to(packet.data(), packet.socket_address())
            };
            match data {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
                Err(error) => return Err(error),
            }
            self.channel.handle_response(packet.data().len());
            self.pending.pop_front();
        }
        Ok(true)
    }

    fn read(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        let buffer_size = self.channel.packet_manager().buffer_size();
        for _ in 0..self.channel.packet_manager().chunk_limit() {
            let data = {
                let buffer = manager.read_buffer();
                buffer.reserve(buffer_size);
                socket::receive_from(&self.socket, buffer, buffer_size).map(|(_, address)| (address, buffer.split()))
            };
            match data {
                Err(error)
                    if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::ConnectionRefused) =>
                {
                    break;
                }
                Ok((address, content)) => {
                    self.channel.read(manager, Packet::new(content, address));
                    if self.channel.is_connected() || {
                        let buffer = self.channel.buffer().borrow();
                        !buffer.is_open() || buffer.is_deafened()
                    } {
                        break;
                    }
                }
                Err(_) => return Ok(false),
            }
        }
        self.execute(manager)
    }
}

impl SocketHandler for DatagramSocketChannel {
    fn execute(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        while self.send()? {}
        if self.pending.is_empty() {
            self.disable_flag(manager, WRITABLE)?;
        } else {
            self.enable_flag(manager, WRITABLE)?;
        }
        let (is_open, is_deafened) = {
            let buffer = self.channel.buffer().borrow();
            (buffer.is_open(), buffer.is_deafened())
        };
        if !self.channel.is_connected() && is_open && !is_deafened {
            self.enable_flag(manager, READABLE)?;
        } else {
            self.disable_flag(manager, READABLE)?;
        }
        if !is_open && !self.pending.is_empty() && !self.closing {
            self.closing = true;
            manager.close_after(self.channel.packet_manager().shutdown_timeout());
        }
        Ok(is_open || !self.pending.is_empty())
    }

    fn socket(&self) -> BorrowedFd<'_> {
        self.socket.as_fd()
    }

    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.ready(manager)
    }

    fn handle(&mut self, manager: &mut ScheduledExecutorService, completion: Completion) -> io::Result<bool> {
        let packet = completion?;
        if packet.is_readable() {
            return self.read(manager);
        }
        self.execute(manager)
    }

    fn shutdown(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.channel.buffer().borrow_mut().close();
        self.execute(manager)
    }

    fn check_expired(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.channel.check_expired(manager);
        self.execute(manager)
    }

    fn close(&mut self, manager: &mut ScheduledExecutorService) {
        self.channel.close(manager);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::requests::sockactions::SocketAction;
    use crate::hooks::PacketListenerAdapter;
    use crate::{DatagramChannel, DefaultPacketManagerBuilder};
    use crate::utils::concurrent::{Dispatcher, ScheduledThreadPoolExecutor};
    use crate::utils::config::PacketConfig;
    use libev::ext::CompletionService;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct EchoAdapter;

    impl PacketListenerAdapter for EchoAdapter {
        async fn on_packet_received(&mut self, channel: &mut DatagramChannel, packet: Packet) -> io::Result<()> {
            channel.send_packet(packet).complete()
        }
    }

    fn manager() -> ScheduledExecutorService {
        let port = Arc::new(CompletionService::epoll_factory()(64).unwrap());
        let dispatcher = Dispatcher::new(&port).0;
        ScheduledExecutorService::new(port, dispatcher, 64)
    }

    fn packet_manager(builder: DefaultPacketManagerBuilder) -> PacketManager {
        let pool = ScheduledThreadPoolExecutor::with_factory(1, 64, CompletionService::epoll_factory());
        builder
            .set_socket_pool(pool.unwrap())
            .set_local_address("127.0.0.1:0".parse().unwrap())
            .add_event_listener(EchoAdapter)
            .build()
            .unwrap()
    }

    fn config() -> PacketConfig {
        PacketConfig {
            address: Some("127.0.0.1:0".parse().unwrap()),
            remote_address: None,
            is_address_shared: false,
            is_broadcast_enabled: false,
            receive_buffer_size: None,
            send_buffer_size: None,
            afk_timeout: None,
            timeout: Duration::from_secs(30),
        }
    }

    #[test]
    fn echo() {
        let socket = socket::packet(&config()).unwrap();
        let address = socket.local_addr().unwrap();
        let mut manager = manager();
        let packet_manager = packet_manager(DefaultPacketManagerBuilder::new());
        manager.register(Box::new(DatagramSocketChannel::new(socket, packet_manager))).unwrap();
        manager.schedule(Duration::from_secs(5), |_| ());

        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        client.send_to(b"ping", address).unwrap();
        manager.run_worker().unwrap();
        manager.execute();

        let mut data = [0; 16];
        let (length, sender) = client.recv_from(&mut data).unwrap();
        assert_eq!(&data[..length], b"ping");
        assert_eq!(sender, address);
    }

    #[test]
    fn connected() {
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let remote_address = client.local_addr().unwrap();
        let socket = socket::packet(&PacketConfig { remote_address: Some(remote_address), ..config() }).unwrap();
        let address = socket.local_addr().unwrap();
        let mut manager = manager();
        let packet_manager = packet_manager(DefaultPacketManagerBuilder::new().set_remote_address(remote_address));
        manager.register(Box::new(DatagramSocketChannel::new(socket, packet_manager))).unwrap();
        manager.schedule(Duration::from_secs(5), |_| ());

        client.send_to(b"ping", address).unwrap();
        manager.run_worker().unwrap();
        manager.execute();

        let mut data = [0; 16];
        let (length, sender) = client.recv_from(&mut data).unwrap();
        assert_eq!(&data[..length], b"ping");
        assert_eq!(sender, address);
    }
}
