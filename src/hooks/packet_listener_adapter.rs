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

use crate::entities::channel::middleman::PacketChannel;
use crate::events::channel::{ChannelDisconnectEvent, ChannelReadyEvent, ChannelTimeoutEvent};
use crate::events::packet::PacketReceivedEvent;
use crate::events::{Event, GenericEvent};
use crate::hooks::PacketListener;
use crate::Packet;
use crate::utils::cast;
use std::any::Any;
use std::io;

/// A trait which divides events for you. You should override the methods provided by this trait for your packet
/// listener implementation.
///
/// Events from the socket reach the listeners in the order they were registered, and a packet written by a listener
/// passes the listeners before it on the way to the socket. By default every method passes the event or packet on
/// through the [`PacketChannel`].
///
/// `I` is the type of the packets from the socket side, `O` is the type of the packets for the next listener.
/// If a method returns an error, the [`PacketChannel`] is closed.
#[allow(async_fn_in_trait)]
pub trait PacketListenerAdapter<I: Any + Send = Packet, O: Any + Send = Packet>: Send {
    /// The channel is bound now.
    async fn on_ready(&mut self, channel: &mut PacketChannel<O, I>) -> io::Result<()> {
        channel.on_ready();
        Ok(())
    }

    /// Gets called once the channel received a packet.
    ///
    /// # Errors
    ///
    /// By default returns [`InvalidInput`] if `I` and `O` are different types.
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    async fn on_packet_received(&mut self, channel: &mut PacketChannel<O, I>, packet: I) -> io::Result<()> {
        let packet = cast(packet).ok_or(io::ErrorKind::InvalidInput)?;
        channel.on_packet_received(packet);
        Ok(())
    }

    /// The channel is closed now.
    async fn on_channel_disconnect(&mut self, channel: &mut PacketChannel<O, I>) -> io::Result<()> {
        channel.on_channel_disconnect();
        Ok(())
    }

    /// Nothing was read or written for the [AFK timeout].
    ///
    /// [AFK timeout]: crate::DefaultPacketManagerBuilder::set_afk_timeout
    async fn on_channel_timeout(&mut self, channel: &mut PacketChannel<O, I>) -> io::Result<()> {
        channel.on_channel_timeout();
        Ok(())
    }

    /// Handles any other [`GenericEvent`].
    async fn on_generic_event(
        &mut self,
        channel: &mut PacketChannel<O, I>,
        event: Box<dyn GenericEvent>,
    ) -> io::Result<()> {
        channel.on_generic_event(event);
        Ok(())
    }

    /// Handles a packet from the next listener, for example to encode it.
    ///
    /// # Errors
    ///
    /// By default returns [`InvalidInput`] if `I` and `O` are different types, or [`NotConnected`] if the channel has
    /// been closed.
    ///
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    /// [`NotConnected`]: io::ErrorKind::NotConnected
    async fn write(&mut self, channel: &mut PacketChannel<O, I>, packet: O) -> io::Result<()> {
        let packet = cast(packet).ok_or(io::ErrorKind::InvalidInput)?;
        channel.write(packet)
    }

    /// Handles [`PacketChannel::close`] of the next listener.
    async fn close(&mut self, channel: &mut PacketChannel<O, I>) -> io::Result<()> {
        channel.close();
        Ok(())
    }
}

impl<I: Any + Send, O: Any + Send, L: PacketListenerAdapter<I, O>> PacketListener<I, O> for L {
    async fn on_event(&mut self, channel: &mut PacketChannel<O, I>, event: Box<dyn Event>) -> io::Result<()> {
        match event.identifier() {
            ChannelReadyEvent::IDENTIFIER => self.on_ready(channel).await,
            PacketReceivedEvent::IDENTIFIER => {
                let event = (event as Box<dyn Any>)
                    .downcast::<PacketReceivedEvent<I>>()
                    .map_err(|_| io::ErrorKind::InvalidInput)?;
                self.on_packet_received(channel, event.packet()).await
            }
            ChannelDisconnectEvent::IDENTIFIER => self.on_channel_disconnect(channel).await,
            ChannelTimeoutEvent::IDENTIFIER => self.on_channel_timeout(channel).await,
            _ => {
                let event = (event as Box<dyn Any>)
                    .downcast::<Box<dyn GenericEvent>>()
                    .map_err(|_| io::ErrorKind::InvalidInput)?;
                self.on_generic_event(channel, *event).await
            }
        }
    }

    async fn write(&mut self, channel: &mut PacketChannel<O, I>, packet: O) -> io::Result<()> {
        PacketListenerAdapter::write(self, channel, packet).await
    }

    async fn close(&mut self, channel: &mut PacketChannel<O, I>) -> io::Result<()> {
        PacketListenerAdapter::close(self, channel).await
    }
}
