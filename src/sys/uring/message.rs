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
use bytes::BytesMut;
use libev::ext::Request;
use socket2::{SockAddr, SockAddrStorage};
use std::any::Any;
use std::mem;
use std::net::SocketAddr;
use std::ptr;

/// Once submitted, the message must not be moved out of its box or released until the kernel has completed the
/// operation.
pub struct Message {
    header: libc::msghdr,
    vector: libc::iovec,
    address: SockAddrStorage,
    content: BytesMut,
}

impl Message {
    pub(crate) fn receive(capacity: usize) -> Box<Self> {
        let content = BytesMut::with_capacity(capacity);
        let mut message = Self::new(content);
        message.vector.iov_base = message.content.spare_capacity_mut().as_mut_ptr().cast();
        message.vector.iov_len = message.content.capacity();
        message.link(mem::size_of::<SockAddrStorage>() as libc::socklen_t);
        message
    }

    pub(crate) fn send(packet: Packet, has_remote: bool) -> Box<Self> {
        let address = SockAddr::from(packet.socket_address());
        let mut message = Self::new(packet.into_content());
        message.vector.iov_base = message.content.as_ptr().cast_mut().cast();
        message.vector.iov_len = message.content.len();
        let length = if has_remote { 0 } else { address.len() };
        unsafe {
            ptr::copy_nonoverlapping(
                address.as_ptr().cast::<u8>(),
                ptr::addr_of_mut!(message.address).cast::<u8>(),
                length as usize,
            );
        }
        message.link(length);
        message
    }

    pub(crate) fn header(&mut self) -> *mut libc::msghdr {
        &mut self.header
    }

    pub(crate) fn complete(self: Box<Self>, length: usize) -> (BytesMut, Option<SocketAddr>) {
        let mut message = *self;
        unsafe { message.content.set_len(length.min(message.content.capacity())) };
        let address = unsafe { SockAddr::new(message.address, message.header.msg_namelen) };
        (message.content, address.as_socket())
    }

    pub(crate) fn into_content(self: Box<Self>) -> BytesMut {
        self.content
    }

    fn new(content: BytesMut) -> Box<Self> {
        Box::new(Self {
            header: unsafe { mem::zeroed() },
            vector: unsafe { mem::zeroed() },
            address: SockAddrStorage::zeroed(),
            content,
        })
    }

    fn link(&mut self, name_length: libc::socklen_t) {
        self.header.msg_name = if name_length == 0 { ptr::null_mut() } else { ptr::addr_of_mut!(self.address).cast() };
        self.header.msg_namelen = name_length;
        self.header.msg_iov = &mut self.vector;
        self.header.msg_iovlen = 1;
    }
}

impl Request for Message {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
