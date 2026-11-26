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
use crate::handle::{BorrowedSocket, Completion, DatagramOperations, SocketHandler};
use crate::handle::event::Event;
use crate::utils::concurrent::ScheduledExecutorService;
use crate::{Packet, PacketManager};
use std::collections::VecDeque;
use std::io;
use std::net::UdpSocket;
use std::os::windows::io::AsSocket;
use std::rc::Rc;
use windows_sys::Win32::Foundation::{ERROR_OPERATION_ABORTED, ERROR_PORT_UNREACHABLE};
use windows_sys::Win32::Networking::WinSock::{WSAECONNREFUSED, WSAECONNRESET};

pub struct PacketSocketChannel {
    socket: UdpSocket,
    channel: Rc<PacketChannelMixin>,
    has_remote: bool,
    pending: VecDeque<Packet>,
    sending: usize,
    receiving: usize,
    closing: bool,
}

impl PacketSocketChannel {
    pub(crate) fn new(socket: UdpSocket, packet_manager: PacketManager) -> Self {
        let has_remote = packet_manager.remote_address().is_some();
        Self {
            socket,
            channel: Rc::new(PacketChannelMixin::new(packet_manager)),
            has_remote,
            pending: VecDeque::new(),
            sending: 0,
            receiving: 0,
            closing: false,
        }
    }

    fn is_ignored(result: i32) -> bool {
        [ERROR_OPERATION_ABORTED as i32, ERROR_PORT_UNREACHABLE as i32, WSAECONNREFUSED, WSAECONNRESET].contains(&result)
    }

    fn next_packet(&mut self) {
        let limit = self.channel.packet_manager().packet_limit();
        self.pending.extend(self.channel.buffer().borrow_mut().read(limit));
    }
}

impl SocketHandler for PacketSocketChannel {
    fn execute(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        let limit = self.channel.packet_manager().chunk_limit();
        while self.sending < limit {
            if self.pending.is_empty() {
                self.next_packet();
            }
            let Some(packet) = self.pending.pop_front() else {
                break;
            };
            manager.send_to(&self.socket, packet, self.has_remote)?;
            self.sending += 1;
        }
        let (is_open, is_deafened) = {
            let buffer = self.channel.buffer().borrow();
            (buffer.is_open(), buffer.is_deafened())
        };
        if !self.channel.is_connected() && is_open && !is_deafened {
            while self.receiving < limit {
                manager.receive_from(&self.socket, self.channel.packet_manager().buffer_size())?;
                self.receiving += 1;
            }
        }
        let is_sending = self.sending > 0 || !self.pending.is_empty();
        if !is_open && is_sending && !self.closing {
            self.closing = true;
            manager.close_after(self.channel.packet_manager().shutdown_timeout());
        }
        Ok(is_open || is_sending)
    }

    fn socket(&self) -> BorrowedSocket<'_> {
        self.socket.as_socket()
    }

    fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
        self.channel.ready(manager);
        self.execute(manager)
    }

    fn handle(&mut self, manager: &mut ScheduledExecutorService, completion: Completion) -> io::Result<bool> {
        let Ok(event) = Event::from_completion(completion) else {
            return Ok(false);
        };
        let result = event.result();
        match event.opcode() {
            Event::RECEIVE => {
                self.receiving -= 1;
                if result == 0 {
                    if let Some(packet) = event.into_packet() {
                        self.channel.read(manager, packet);
                    }
                } else if !Self::is_ignored(result) {
                    return Ok(false);
                }
            }
            Event::SEND => {
                self.sending -= 1;
                if result == 0 || Self::is_ignored(result) {
                    self.channel.handle_response(event.size());
                }
                if result != 0 && !Self::is_ignored(result) {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::CompletionService;
    use crate::handle::socket;
    use crate::hooks::PacketListenerAdapter;
    use crate::requests::sockactions::SocketAction;
    use crate::utils::concurrent::{Dispatcher, ScheduledThreadPoolExecutor};
    use crate::utils::config::PacketConfig;
    use crate::{DefaultPacketManagerBuilder, PacketChannel};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[derive(Clone)]
    struct EchoAdapter;

    impl PacketListenerAdapter for EchoAdapter {
        async fn on_packet_received(&mut self, channel: &mut PacketChannel, packet: Packet) -> io::Result<()> {
            channel.send_packet(packet).complete()
        }
    }

    fn manager() -> ScheduledExecutorService {
        let port = Arc::new(CompletionService::new().unwrap());
        let dispatcher = Dispatcher::new(&port).0;
        ScheduledExecutorService::new(port, dispatcher, 64)
    }

    fn packet_manager(builder: DefaultPacketManagerBuilder) -> PacketManager {
        builder
            .set_socket_pool(ScheduledThreadPoolExecutor::new(1, 64).unwrap())
            .set_address("127.0.0.1:0".parse().unwrap())
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

    fn exchange(manager: &mut ScheduledExecutorService, client: UdpSocket, address: std::net::SocketAddr) {
        let client = thread::spawn(move || {
            client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            for message in [&b"ping"[..], b"pong"] {
                client.send_to(message, address).unwrap();
                let mut data = [0; 16];
                let (length, sender) = client.recv_from(&mut data).unwrap();
                assert_eq!(&data[..length], message);
                assert_eq!(sender, address);
            }
        });
        while !client.is_finished() {
            manager.schedule(Duration::from_millis(10), |_| ());
            manager.run_worker().unwrap();
            manager.execute();
        }
        client.join().unwrap();
    }

    #[test]
    fn echo() {
        let socket = socket::packet(&config()).unwrap();
        let address = socket.local_addr().unwrap();
        let mut manager = manager();
        let packet_manager = packet_manager(DefaultPacketManagerBuilder::new());
        manager.register(Box::new(PacketSocketChannel::new(socket, packet_manager))).unwrap();

        exchange(&mut manager, UdpSocket::bind("127.0.0.1:0").unwrap(), address);
        assert_eq!(manager.sockets_total(), 1);

        manager.close();
        assert_eq!(manager.pending(), 0);
    }

    #[test]
    fn connected() {
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote_address = client.local_addr().unwrap();
        let socket = socket::packet(&PacketConfig { remote_address: Some(remote_address), ..config() }).unwrap();
        let address = socket.local_addr().unwrap();
        let mut manager = manager();
        let packet_manager = packet_manager(DefaultPacketManagerBuilder::new().set_remote_address(remote_address));
        manager.register(Box::new(PacketSocketChannel::new(socket, packet_manager))).unwrap();

        exchange(&mut manager, client, address);
        manager.close();
        assert_eq!(manager.pending(), 0);
    }
}
