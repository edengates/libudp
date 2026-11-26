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

use crate::entities::channel::concrete::ThreadChannel;
use crate::requests::sockactions::SocketAction;
use std::future::{self, IntoFuture, Ready};
use std::io;

#[must_use]
pub struct ThreadPacketCreateAction<'a, N, P> {
    channel: &'a ThreadChannel<N, P>,
    packet: P,
}

impl<'a, N, P> ThreadPacketCreateAction<'a, N, P> {
    pub(crate) fn new(channel: &'a ThreadChannel<N, P>, packet: P) -> Self {
        Self { channel, packet }
    }
}

impl<N: 'static, P: Send + 'static> ThreadPacketCreateAction<'_, N, P> {
    /// Submits a request for execution without waiting for it.
    ///
    /// The `success` callback will be called at a convenient time, the `failure` callback will be called if the
    /// request encounters an error at its execution point or can not be executed anymore. Both are called on the
    /// thread of the channel, unlike [`complete`](SocketAction::complete) this does not wait for the result.
    pub fn queue<S, F>(self, success: S, failure: F)
    where
        S: FnOnce(()) + Send + 'static,
        F: FnOnce(io::Error) + Send + 'static,
    {
        let packet = self.packet;
        let mut failure = Failure(Some(failure));
        self.channel
            .execute(move |channel| {
                let Some(failure) = failure.0.take() else {
                    return;
                };
                match channel.send_packet(packet).complete() {
                    Ok(data) => success(data),
                    Err(error) => failure(error),
                }
            })
            .ok();
    }
}

struct Failure<F: FnOnce(io::Error)>(Option<F>);

impl<F: FnOnce(io::Error)> Drop for Failure<F> {
    fn drop(&mut self) {
        if let Some(failure) = self.0.take() {
            failure(io::ErrorKind::NotConnected.into());
        }
    }
}

impl<N: 'static, P: Send + 'static> SocketAction<()> for ThreadPacketCreateAction<'_, N, P> {
    fn complete(self) -> io::Result<()> {
        let packet = self.packet;
        self.channel.submit(move |channel| channel.send_packet(packet).complete())
    }
}

impl<N: 'static, P: Send + 'static> IntoFuture for ThreadPacketCreateAction<'_, N, P> {
    type Output = io::Result<()>;
    type IntoFuture = Ready<io::Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        future::ready(self.complete())
    }
}
