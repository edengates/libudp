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
use libudp::hooks::PacketListenerAdapter;
use libudp::requests::sockactions::SocketAction;
use libudp::{DefaultPacketManagerBuilder, Packet, DatagramChannel};
use std::net::SocketAddr;
use std::{env, io};

// The channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct PacketCreateExample {
    address: SocketAddr,
    content: String,
}

impl PacketListenerAdapter for PacketCreateExample {
    // This overrides the method called on_ready in the PacketListenerAdapter trait
    // It gets called once the socket is bound
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        // send_packet(...) creates a PacketCreateAction, nothing is sent until you queue it
        // queue(...) adds the packet to the channel buffer and calls one of the callbacks with the result
        let packet = Packet::new(BytesMut::from(self.content.as_bytes()), self.address);
        channel.send_packet(packet).queue(
            |_| println!("[queued] {}", self.content),
            |error| eprintln!("The packet could not be sent: {}", error),
        )
    }

    // Gets called for every reply of the address, this one prints it and closes the channel
    async fn on_packet_received(&mut self, channel: &mut DatagramChannel, packet: Packet) -> io::Result<()> {
        println!("[reply] {}", String::from_utf8_lossy(packet.data()));
        channel.close();
        Ok(())
    }
}

fn main() -> io::Result<()> {
    // The address to send to and the content of the packet, both can be provided from the command line
    let address: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| String::from("127.0.0.1:8080"))
        .parse()
        .map_err(|_| io::ErrorKind::InvalidInput)?;
    let content = env::args().nth(2).unwrap_or_else(|| String::from("ping"));

    // To start the packet manager, you have to use the DefaultPacketManagerBuilder.
    let manager = DefaultPacketManagerBuilder::new()
        // A connected socket only receives the packets of the address it is connected to
        .set_remote_address(address)
        // On this builder, you are adding all your event listeners
        .add_event_listener(PacketCreateExample { address, content })
        // Once you're done configuring your packet manager, call build to bind the socket and start it.
        .build()?;

    println!("Sending to address: {}", address);

    // The channel is handled on the thread pool, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued packets are still going to be sent before the channel is closed
    manager.shutdown();
    Ok(())
}
