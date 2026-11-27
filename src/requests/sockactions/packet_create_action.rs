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

use crate::entities::channel::middleman::DatagramChannel;
use crate::requests::sockactions::SocketAction;
use std::future::{self, IntoFuture, Ready};
use std::io;

#[must_use]
pub struct PacketCreateAction<'a, N, P> {
    channel: &'a mut DatagramChannel<N, P>,
    packet: P,
}

impl<'a, N, P> PacketCreateAction<'a, N, P> {
    pub(crate) fn new(channel: &'a mut DatagramChannel<N, P>, packet: P) -> Self {
        Self { channel, packet }
    }
}

impl<N, P> SocketAction<()> for PacketCreateAction<'_, N, P> {
    fn complete(self) -> io::Result<()> {
        let data = self.channel.write(self.packet);
        self.channel.flush();
        data
    }
}

impl<N, P> IntoFuture for PacketCreateAction<'_, N, P> {
    type Output = io::Result<()>;
    type IntoFuture = Ready<io::Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        future::ready(self.complete())
    }
}
