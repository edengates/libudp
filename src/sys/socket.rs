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

use crate::utils::config::PacketConfig;
use bytes::BytesMut;
use rustix::io::Errno;
use socket2::{Domain, SockAddr, SockRef, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

fn new_socket(domain: Domain, kind: Type) -> io::Result<Socket> {
    #[cfg(target_os = "macos")]
    {
        let socket = Socket::new(domain, kind, None)?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }
    #[cfg(not(target_os = "macos"))]
    Socket::new(domain, kind.nonblocking(), None)
}

pub(crate) fn packet(config: &PacketConfig) -> io::Result<UdpSocket> {
    let address = config.address().unwrap_or_else(|| match config.remote_address() {
        Some(SocketAddr::V6(_)) => SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        _ => SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
    });
    let socket = new_socket(Domain::for_address(address), Type::DGRAM)?;
    if config.is_address_shared() {
        #[cfg(target_os = "freebsd")]
        socket.set_reuse_port_lb(true)?;
        #[cfg(not(target_os = "freebsd"))]
        socket.set_reuse_port(true)?;
    }
    if address.is_ipv6() {
        if let Err(error) = socket.set_only_v6(false) {
            if error.raw_os_error() != Some(Errno::AFNOSUPPORT.raw_os_error()) {
                return Err(error);
            }
        }
    }
    if config.is_broadcast_enabled() {
        socket.set_broadcast(true)?;
    }
    if let Some(size) = config.receive_buffer_size() {
        socket.set_recv_buffer_size(size)?;
    }
    if let Some(size) = config.send_buffer_size() {
        socket.set_send_buffer_size(size)?;
    }
    socket.bind(&SockAddr::from(address))?;
    if let Some(remote_address) = config.remote_address() {
        socket.connect(&SockAddr::from(remote_address))?;
    }
    Ok(UdpSocket::from(socket))
}

pub(crate) fn receive_from(
    socket: &UdpSocket,
    buffer: &mut BytesMut,
    limit: usize,
) -> io::Result<(usize, SocketAddr)> {
    loop {
        let spare = buffer.spare_capacity_mut();
        let limit = limit.min(spare.len());
        match SockRef::from(socket).recv_from(&mut spare[..limit]) {
            Ok((length, address)) => {
                unsafe { buffer.set_len(buffer.len() + length) };
                return Ok((length, address.as_socket().ok_or(io::ErrorKind::InvalidData)?));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    use std::os::fd::AsFd;
    use std::time::Duration;

    fn await_ready(fd: &impl AsFd, events: PollFlags) {
        let timeout = Timespec { tv_sec: 5, tv_nsec: 0 };
        assert_eq!(poll(&mut [PollFd::new(fd, events)], Some(&timeout)).unwrap(), 1);
    }

    fn packet_config() -> PacketConfig {
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
    fn packet() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let config = PacketConfig {
            remote_address: Some(peer.local_addr().unwrap()),
            is_address_shared: true,
            is_broadcast_enabled: true,
            receive_buffer_size: Some(65536),
            send_buffer_size: Some(32768),
            ..packet_config()
        };
        let socket = super::packet(&config).unwrap();
        assert_eq!(socket.peer_addr().unwrap(), peer.local_addr().unwrap());
        assert!(socket.broadcast().unwrap());
        assert!(SockRef::from(&socket).recv_buffer_size().unwrap() >= 65536);
        assert!(SockRef::from(&socket).send_buffer_size().unwrap() >= 32768);
        assert_eq!(socket.recv(&mut [0; 1]).unwrap_err().kind(), io::ErrorKind::WouldBlock);

        let shared = PacketConfig {
            address: Some(socket.local_addr().unwrap()),
            remote_address: None,
            ..config
        };
        assert!(super::packet(&shared).is_ok());
        assert!(super::packet(&PacketConfig { is_address_shared: false, ..shared }).is_err());
    }

    #[test]
    fn receive_from() {
        let server = super::packet(&packet_config()).unwrap();
        assert_eq!(
            super::receive_from(&server, &mut BytesMut::with_capacity(4), 4).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.send_to(b"hello world", server.local_addr().unwrap()).unwrap();
        await_ready(&server, PollFlags::IN);

        let mut buffer = BytesMut::with_capacity(5);
        let (length, address) = super::receive_from(&server, &mut buffer, 5).unwrap();
        assert_eq!(length, 5);
        assert_eq!(address, client.local_addr().unwrap());
        assert_eq!(&buffer[..], b"hello");
    }

    #[test]
    fn receive_limit() {
        let server = super::packet(&packet_config()).unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.send_to(b"hello world", server.local_addr().unwrap()).unwrap();
        await_ready(&server, PollFlags::IN);

        let mut buffer = BytesMut::with_capacity(64);
        let (length, _) = super::receive_from(&server, &mut buffer, 4).unwrap();
        assert_eq!(length, 4);
        assert_eq!(&buffer[..], b"hell");
    }
}
