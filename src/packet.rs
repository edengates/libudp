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

use bytes::BytesMut;
use std::net::SocketAddr;

/// A message of a UDP socket together with the address of its peer.
///
/// The address is the sender of a received packet and the recipient of a sent packet.
#[derive(Debug)]
pub struct Packet {
    content: BytesMut,
    address: SocketAddr,
}

impl Packet {
    pub fn new(content: BytesMut, address: SocketAddr) -> Self {
        Self { content, address }
    }

    pub fn content(&self) -> &BytesMut {
        &self.content
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn into_content(self) -> BytesMut {
        self.content
    }
}
