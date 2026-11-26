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

use crate::Packet;
use crate::handle::event::Event;
use crate::utils::concurrent::ScheduledExecutorService;
use std::io;
use std::os::windows::io::AsRawSocket;
use windows_sys::Win32::Networking::WinSock::SOCKET;

pub(crate) trait DatagramOperations {
    fn receive_from<S: AsRawSocket>(&mut self, socket: &S, capacity: usize) -> io::Result<()>;

    fn send_to<S: AsRawSocket>(&mut self, socket: &S, packet: Packet, has_remote: bool) -> io::Result<()>;
}

impl DatagramOperations for ScheduledExecutorService {
    fn receive_from<S: AsRawSocket>(&mut self, socket: &S, capacity: usize) -> io::Result<()> {
        self.submit_request(Event::receive(capacity), |event| event.start(socket.as_raw_socket() as SOCKET))
    }

    fn send_to<S: AsRawSocket>(&mut self, socket: &S, packet: Packet, has_remote: bool) -> io::Result<()> {
        self.submit_request(Event::send(packet, has_remote), |event| event.start(socket.as_raw_socket() as SOCKET))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::socket;
    use crate::handle::{Completion, SocketHandler};
    use crate::utils::concurrent::Dispatcher;
    use crate::utils::config::PacketConfig;
    use bytes::BytesMut;
    use libev::ext::CompletionService;
    use std::net::UdpSocket;
    use std::os::windows::io::{AsSocket, BorrowedSocket};
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Duration;

    type Start = Box<dyn FnOnce(&mut ScheduledExecutorService, &UdpSocket) -> io::Result<()>>;

    struct Collector {
        socket: UdpSocket,
        sender: Sender<Box<Event>>,
        start: Option<Start>,
    }

    impl SocketHandler for Collector {
        fn socket(&self) -> BorrowedSocket<'_> {
            self.socket.as_socket()
        }

        fn on_registered(&mut self, manager: &mut ScheduledExecutorService) -> io::Result<bool> {
            if let Some(start) = self.start.take() {
                start(manager, &self.socket)?;
            }
            Ok(true)
        }

        fn handle(&mut self, _manager: &mut ScheduledExecutorService, completion: Completion) -> io::Result<bool> {
            self.sender.send(Event::from_completion(completion)?).ok();
            Ok(true)
        }
    }

    fn manager() -> ScheduledExecutorService {
        let port = Arc::new(CompletionService::new().unwrap());
        let dispatcher = Dispatcher::new(&port).0;
        ScheduledExecutorService::new(port, dispatcher, 64)
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

    fn register(manager: &mut ScheduledExecutorService, socket: UdpSocket, start: Start) -> Receiver<Box<Event>> {
        let (sender, receiver) = mpsc::channel();
        manager.register(Box::new(Collector { socket, sender, start: Some(start) })).unwrap();
        receiver
    }

    fn on_success(manager: &mut ScheduledExecutorService, receiver: &Receiver<Box<Event>>) -> Box<Event> {
        manager.run_worker().unwrap();
        receiver.recv_timeout(Duration::from_secs(5)).unwrap()
    }

    #[test]
    fn receive() {
        let mut manager = manager();
        let socket = socket::packet(&config()).unwrap();
        let address = socket.local_addr().unwrap();
        let receiver = register(&mut manager, socket, Box::new(|manager, socket| manager.receive_from(socket, 16)));
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();

        client.send_to(b"ping", address).unwrap();

        let event = on_success(&mut manager, &receiver);
        assert_eq!(event.opcode(), Event::RECEIVE);
        assert_eq!(event.result(), 0);
        let packet = event.into_packet().unwrap();
        assert_eq!(&packet.content()[..], b"ping");
        assert_eq!(packet.address(), client.local_addr().unwrap());
    }

    #[test]
    fn truncate() {
        let mut manager = manager();
        let socket = socket::packet(&config()).unwrap();
        let address = socket.local_addr().unwrap();
        let receiver = register(&mut manager, socket, Box::new(|manager, socket| manager.receive_from(socket, 4)));
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();

        client.send_to(b"hello world", address).unwrap();

        let packet = on_success(&mut manager, &receiver).into_packet().unwrap();
        assert_eq!(&packet.content()[..], b"hell");
    }

    #[test]
    fn send() {
        let mut manager = manager();
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let socket = socket::packet(&config()).unwrap();
        let address = socket.local_addr().unwrap();
        let target = client.local_addr().unwrap();
        let receiver = register(
            &mut manager,
            socket,
            Box::new(move |manager, socket| {
                manager.send_to(socket, Packet::new(BytesMut::from(&b"pong"[..]), target), false)
            }),
        );

        let event = on_success(&mut manager, &receiver);
        assert_eq!(event.opcode(), Event::SEND);
        assert_eq!(event.result(), 0);

        let mut data = [0; 16];
        let (length, sender) = client.recv_from(&mut data).unwrap();
        assert_eq!(&data[..length], b"pong");
        assert_eq!(sender, address);
    }

    #[test]
    fn connected() {
        let mut manager = manager();
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let target = client.local_addr().unwrap();
        let socket = socket::packet(&PacketConfig { remote_address: Some(target), ..config() }).unwrap();
        let receiver = register(
            &mut manager,
            socket,
            Box::new(move |manager, socket| {
                manager.send_to(socket, Packet::new(BytesMut::from(&b"pong"[..]), target), true)
            }),
        );

        assert_eq!(on_success(&mut manager, &receiver).result(), 0);

        let mut data = [0; 16];
        let (length, _) = client.recv_from(&mut data).unwrap();
        assert_eq!(&data[..length], b"pong");
    }

    #[test]
    fn close() {
        let mut manager = manager();
        let socket = socket::packet(&config()).unwrap();
        let _receiver = register(&mut manager, socket, Box::new(|manager, socket| manager.receive_from(socket, 16)));
        assert_eq!(manager.pending(), 1);

        manager.close();

        assert_eq!(manager.pending(), 0);
    }
}
