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
use std::{io, mem};

#[derive(Default)]
pub struct Buffer {
    queued: Vec<Packet>,
    ready: Vec<Packet>,
    closed: bool,
    size: usize,
    slowmode: bool,
    deafened: bool,
}

impl Buffer {
    fn update(&mut self, threshold: usize, large_threshold: usize) {
        self.slowmode = if self.slowmode {
            self.size >= threshold
        } else {
            self.size > large_threshold
        };
    }

    pub fn write(&mut self, packet: Packet, threshold: usize, large_threshold: usize) -> io::Result<()> {
        if self.closed {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.size += packet.content().len();
        self.update(threshold, large_threshold);
        self.queued.push(packet);
        Ok(())
    }

    pub fn skip(&mut self, size: usize, threshold: usize, large_threshold: usize) {
        self.size -= size;
        self.update(threshold, large_threshold);
    }

    pub fn emit(&mut self) {
        if self.ready.is_empty() {
            mem::swap(&mut self.ready, &mut self.queued);
        } else {
            self.ready.append(&mut self.queued);
        }
    }

    pub fn read(&mut self, limit: usize) -> Vec<Packet> {
        if self.ready.len() <= limit {
            mem::take(&mut self.ready)
        } else {
            let rest = self.ready.split_off(limit);
            mem::replace(&mut self.ready, rest)
        }
    }

    pub fn is_slowmode(&self) -> bool {
        self.slowmode
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn is_open(&self) -> bool {
        !self.closed
    }

    pub fn set_deafened(&mut self, deafened: bool) {
        self.deafened = deafened;
    }

    pub fn is_deafened(&self) -> bool {
        self.deafened
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn packet(length: usize) -> Packet {
        Packet::new(BytesMut::zeroed(length), "127.0.0.1:1".parse().unwrap())
    }

    #[test]
    fn slowmode() {
        let mut buffer = Buffer::default();
        buffer.write(packet(40 * 1024), 32 * 1024, 64 * 1024).unwrap();
        assert!(!buffer.is_slowmode());
        buffer.write(packet(40 * 1024), 32 * 1024, 64 * 1024).unwrap();
        assert!(buffer.is_slowmode());

        buffer.skip(40 * 1024, 32 * 1024, 64 * 1024);
        assert!(buffer.is_slowmode());

        buffer.skip(10 * 1024, 32 * 1024, 64 * 1024);
        assert!(!buffer.is_slowmode());
    }

    #[test]
    fn read() {
        let mut buffer = Buffer::default();
        buffer.write(packet(1), 32 * 1024, 64 * 1024).unwrap();
        buffer.write(packet(2), 32 * 1024, 64 * 1024).unwrap();
        buffer.write(packet(3), 32 * 1024, 64 * 1024).unwrap();
        assert!(buffer.read(usize::MAX).is_empty());

        buffer.emit();
        assert_eq!(buffer.read(2).len(), 2);
        assert_eq!(buffer.read(2).len(), 1);

        buffer.close();
        assert!(buffer.write(packet(1), 32 * 1024, 64 * 1024).is_err());
    }
}
