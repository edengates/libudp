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
use socket2::{Domain, SockAddr, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use windows_sys::Win32::Networking::WinSock::{
    SIO_UDP_CONNRESET, SOCKET, SOCKET_ERROR, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, WSAGetLastError, WSAIoctl, setsockopt,
};

pub(crate) fn packet(config: &PacketConfig) -> io::Result<UdpSocket> {
    let address = config.address().unwrap_or_else(|| match config.remote_address() {
        Some(SocketAddr::V6(_)) => SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        _ => SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0),
    });
    let socket = Socket::new(Domain::for_address(address), Type::DGRAM, None)?;
    if config.is_address_shared() {
        socket.set_reuse_address(true)?;
    } else {
        exclusive_address(&socket)?;
    }
    if address.is_ipv6() {
        socket.set_only_v6(false)?;
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
    disable_connection_reset(&socket)?;
    socket.bind(&SockAddr::from(address))?;
    if let Some(remote_address) = config.remote_address() {
        socket.connect(&SockAddr::from(remote_address))?;
    }
    Ok(UdpSocket::from(socket))
}

fn exclusive_address(socket: &Socket) -> io::Result<()> {
    let enable = 1i32;
    let data = unsafe {
        setsockopt(
            socket.as_raw_socket() as SOCKET,
            SOL_SOCKET,
            SO_EXCLUSIVEADDRUSE,
            (&enable as *const i32).cast(),
            size_of::<i32>() as i32,
        )
    };
    if data == SOCKET_ERROR { Err(last_error()) } else { Ok(()) }
}

fn disable_connection_reset(socket: &Socket) -> io::Result<()> {
    let enable = 0u32;
    let mut returned = 0;
    let data = unsafe {
        WSAIoctl(
            socket.as_raw_socket() as SOCKET,
            SIO_UDP_CONNRESET,
            (&enable as *const u32).cast(),
            size_of::<u32>() as u32,
            ptr::null_mut(),
            0,
            &mut returned,
            ptr::null_mut(),
            None,
        )
    };
    if data == SOCKET_ERROR { Err(last_error()) } else { Ok(()) }
}

pub(crate) fn last_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use socket2::SockRef;
    use std::time::Duration;

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

        let exclusive = PacketConfig {
            address: Some(socket.local_addr().unwrap()),
            remote_address: None,
            ..config
        };
        assert!(super::packet(&exclusive).is_err());
        assert!(super::packet(&PacketConfig { is_address_shared: true, ..exclusive }).is_err());
    }

    #[test]
    fn shared() {
        let first = super::packet(&PacketConfig { is_address_shared: true, ..packet_config() }).unwrap();
        let shared = PacketConfig {
            address: Some(first.local_addr().unwrap()),
            is_address_shared: true,
            ..packet_config()
        };
        assert!(super::packet(&shared).is_ok());
    }
}
