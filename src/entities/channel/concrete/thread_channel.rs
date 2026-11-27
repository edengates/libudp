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

use crate::entities::ISnowflake;
use crate::entities::channel::middleman::DatagramChannel;
use crate::requests::sockactions::ThreadPacketCreateAction;
use crate::Packet;
use crate::utils::concurrent::{Dispatcher, ScheduledExecutorService};
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, mpsc};

/// A [`DatagramChannel`] that can be shared with other threads.
///
/// Every method runs the same method of the [`DatagramChannel`] on the thread of the channel.
///
/// `N` is the type of packets for the next listener, `P` is the type of packets for the parent channel.
pub struct ThreadChannel<N = Packet, P = Packet> {
    id: u64,
    position: usize,
    dispatcher: Arc<Dispatcher>,
    marker: PhantomData<fn() -> (N, P)>,
}

impl<N, P> Clone for ThreadChannel<N, P> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            position: self.position,
            dispatcher: Arc::clone(&self.dispatcher),
            marker: PhantomData,
        }
    }
}

impl<N, P> ThreadChannel<N, P> {
    pub(crate) fn new(id: u64, position: usize, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            id,
            position,
            dispatcher,
            marker: PhantomData,
        }
    }

    pub(crate) fn at_position<A, B>(&self, position: usize) -> ThreadChannel<A, B> {
        ThreadChannel::new(self.id, position, Arc::clone(&self.dispatcher))
    }
}

impl<N: 'static, P: Send + 'static> ThreadChannel<N, P> {
    /// Request to write a packet via this channel from any thread.
    ///
    /// This does not request a flush, call [`flush`] for that. The packet is written on the thread of the channel,
    /// the calling thread waits for the result.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed, [`BrokenPipe`] if the thread of the channel has been
    /// shut down and [`Deadlock`] if called on the thread of the channel.
    ///
    /// [`flush`]: Self::flush
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    /// [`Deadlock`]: io::ErrorKind::Deadlock
    pub fn write(&self, packet: P) -> io::Result<()> {
        self.submit(move |channel| channel.write(packet))
    }

    /// Whether this channel is in slow mode, from any thread.
    ///
    /// Slow mode starts when the queue of pending packets is larger than the [`large_threshold`], and ends when it
    /// is smaller than the [`threshold`] again. The state is read on the thread of the channel, the calling thread
    /// waits for the result.
    ///
    /// # Errors
    ///
    /// Returns [`NotConnected`] if the channel has been closed, [`BrokenPipe`] if the thread of the channel has been
    /// shut down and [`Deadlock`] if called on the thread of the channel.
    ///
    /// [`large_threshold`]: crate::PacketManager::large_threshold
    /// [`threshold`]: crate::PacketManager::threshold
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    /// [`Deadlock`]: io::ErrorKind::Deadlock
    pub fn is_locked(&self) -> io::Result<bool> {
        self.submit(|channel| Ok(channel.is_locked()))
    }

    /// Request to flush all pending packets via this channel from any thread.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`] if the thread of the channel has been shut down.
    ///
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    pub fn flush(&self) -> io::Result<()> {
        self.execute(|channel| channel.flush())
    }

    /// Send a packet to this channel from any thread.
    ///
    /// This requires an additional step to complete the execution, for example when you call
    /// [`ThreadPacketCreateAction::queue`]. The packet is written on the thread of the channel, the calling thread
    /// waits for the result. Called on the thread of the channel, the execution fails with [`Deadlock`].
    ///
    /// [`ThreadPacketCreateAction::queue`]: crate::requests::sockactions::SocketAction::queue
    /// [`Deadlock`]: io::ErrorKind::Deadlock
    pub fn send_packet(&self, packet: P) -> ThreadPacketCreateAction<'_, N, P> {
        ThreadPacketCreateAction::new(self, packet)
    }

    /// Closes this channel from any thread, once all pending packets have been sent.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`] if the thread of the channel has been shut down.
    ///
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    pub fn close(&self) -> io::Result<()> {
        self.execute(|channel| channel.close())
    }

    /// Sets whether this channel should be deafened, from any thread.
    ///
    /// When deafened, no more packets are read from the socket until it is undeafened.
    ///
    /// # Errors
    ///
    /// Returns [`BrokenPipe`] if the thread of the channel has been shut down.
    ///
    /// [`BrokenPipe`]: io::ErrorKind::BrokenPipe
    pub fn set_self_deafened(&self, deafened: bool) -> io::Result<()> {
        self.execute(move |channel| channel.set_self_deafened(deafened))
    }

    pub(crate) fn submit<T, F>(&self, action: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut DatagramChannel<N, P>) -> io::Result<T> + Send + 'static,
    {
        if ScheduledExecutorService::is_current(self.dispatcher.port()) {
            return Err(io::ErrorKind::Deadlock.into());
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        self.execute(move |channel| {
            sender.send(action(channel)).ok();
        })?;
        receiver.recv().map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?
    }

    pub(crate) fn execute<F: FnOnce(&mut DatagramChannel<N, P>) + Send + 'static>(&self, action: F) -> io::Result<()> {
        let (id, position) = (self.id, self.position);
        self.dispatcher
            .dispatch(Box::new(move |manager| manager.execute_channel(id, position, action)))
    }
}

impl<N, P> ISnowflake for ThreadChannel<N, P> {
    fn id(&self) -> u64 {
        self.id
    }
}
