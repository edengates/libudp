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

use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone)]
pub struct PacketConfig {
    pub(crate) address: Option<SocketAddr>,
    pub(crate) remote_address: Option<SocketAddr>,
    pub(crate) is_address_shared: bool,
    pub(crate) is_broadcast_enabled: bool,
    pub(crate) receive_buffer_size: Option<usize>,
    pub(crate) send_buffer_size: Option<usize>,
    pub(crate) afk_timeout: Option<Duration>,
    pub(crate) timeout: Duration,
}

impl PacketConfig {
    pub fn address(&self) -> Option<SocketAddr> {
        self.address
    }

    pub fn remote_address(&self) -> Option<SocketAddr> {
        self.remote_address
    }

    pub fn is_address_shared(&self) -> bool {
        self.is_address_shared
    }

    pub fn is_broadcast_enabled(&self) -> bool {
        self.is_broadcast_enabled
    }

    pub fn receive_buffer_size(&self) -> Option<usize> {
        self.receive_buffer_size
    }

    pub fn send_buffer_size(&self) -> Option<usize> {
        self.send_buffer_size
    }

    pub fn afk_timeout(&self) -> Option<Duration> {
        self.afk_timeout
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}
