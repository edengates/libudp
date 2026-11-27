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
use bytes::{BufMut, BytesMut};
use libev::ext::{Completion, Request};
use socket2::{SockAddr, SockAddrStorage};
use std::any::Any;
use std::io;
use std::mem;
use std::ptr;
use windows_sys::Win32::Foundation::ERROR_MORE_DATA;
use windows_sys::Win32::Networking::WinSock::{SOCKADDR, SOCKET, WSABUF, WSARecvFrom, WSASend, WSASendTo};
use windows_sys::Win32::System::IO::OVERLAPPED;

/// Once submitted, the event must not be moved or released until Windows has completed the operation.
/// The event is found again from its `OVERLAPPED`, which must stay the first field.
#[repr(C)]
pub struct Event {
    overlapped: OVERLAPPED,
    content: BytesMut,
    address: SockAddrStorage,
    address_length: i32,
    flags: u32,
    opcode: u32,
    result: i32,
}

impl Event {
    pub(crate) const RECEIVE: u32 = 1 << 0;
    pub(crate) const SEND: u32 = 1 << 1;

    pub(crate) fn receive(capacity: usize) -> Box<Self> {
        let mut event = Self::new(BytesMut::with_capacity(capacity), Event::RECEIVE);
        event.address_length = mem::size_of::<SockAddrStorage>() as i32;
        event
    }

    pub(crate) fn send(packet: Packet, has_remote: bool) -> Box<Self> {
        let address = SockAddr::from(packet.socket_address());
        let mut event = Self::new(packet.into_content(), Event::SEND);
        if !has_remote {
            unsafe {
                ptr::copy_nonoverlapping(
                    address.as_ptr().cast::<u8>(),
                    ptr::addr_of_mut!(event.address).cast::<u8>(),
                    address.len() as usize,
                );
            }
            event.address_length = address.len();
        }
        event
    }

    fn new(content: BytesMut, opcode: u32) -> Box<Self> {
        Box::new(Self {
            overlapped: unsafe { mem::zeroed() },
            content,
            address: SockAddrStorage::zeroed(),
            address_length: 0,
            flags: 0,
            opcode,
            result: 0,
        })
    }

    pub(crate) fn opcode(&self) -> u32 {
        self.opcode
    }

    pub(crate) fn result(&self) -> i32 {
        self.result
    }

    pub(crate) fn size(&self) -> usize {
        self.content.len()
    }

    pub(crate) fn into_packet(self: Box<Self>) -> Option<Packet> {
        let event = *self;
        let address = unsafe { SockAddr::new(event.address, event.address_length) }.as_socket()?;
        Some(Packet::new(event.content, address))
    }

    pub(crate) fn start(&mut self, socket: SOCKET) -> i32 {
        self.overlapped = unsafe { mem::zeroed() };
        unsafe {
            match self.opcode {
                Event::RECEIVE => {
                    let chunk = self.content.spare_capacity_mut();
                    let buffer = WSABUF { len: chunk.len() as u32, buf: chunk.as_mut_ptr().cast() };
                    WSARecvFrom(
                        socket,
                        &buffer,
                        1,
                        ptr::null_mut(),
                        &mut self.flags,
                        ptr::addr_of_mut!(self.address).cast::<SOCKADDR>(),
                        &mut self.address_length,
                        &mut self.overlapped,
                        None,
                    )
                }
                _ => {
                    let buffer = WSABUF { len: self.content.len() as u32, buf: self.content.as_mut_ptr() };
                    if self.address_length == 0 {
                        WSASend(socket, &buffer, 1, ptr::null_mut(), 0, &mut self.overlapped, None)
                    } else {
                        WSASendTo(
                            socket,
                            &buffer,
                            1,
                            ptr::null_mut(),
                            0,
                            ptr::addr_of!(self.address).cast::<SOCKADDR>(),
                            self.address_length,
                            &mut self.overlapped,
                            None,
                        )
                    }
                }
            }
        }
    }

    pub(crate) fn from_completion(completion: Completion) -> io::Result<Box<Self>> {
        let (mut event, length, error) = completion.into_parts::<Self>()?;
        if let Some(code) = error.as_ref().and_then(io::Error::raw_os_error) {
            if code != ERROR_MORE_DATA as i32 {
                event.result = code;
                return Ok(event);
            }
        }
        if event.opcode == Event::RECEIVE {
            unsafe { event.content.advance_mut(length) };
        }
        Ok(event)
    }
}

impl Request for Event {
    fn overlapped(&mut self) -> *mut OVERLAPPED {
        &raw mut self.overlapped
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
