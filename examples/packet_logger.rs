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

use libudp::hooks::PacketListenerAdapter;
use libudp::{DefaultPacketManagerBuilder, Packet, PacketChannel};
use std::{env, io};

// The channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct PacketLoggerExample;

impl PacketListenerAdapter for PacketLoggerExample {
    // This overrides the method called on_packet_received in the PacketListenerAdapter trait
    // Gets called for every packet received from the socket
    async fn on_packet_received(&mut self, _channel: &mut PacketChannel, packet: Packet) -> io::Result<()> {
        // The content is the raw bytes of the datagram, this converts it to a readable string,
        // the address is the sender of the packet
        println!("[packet] {}: {}", packet.address(), String::from_utf8_lossy(packet.content()));
        Ok(())
    }
}

fn main() -> io::Result<()> {
    // The address to bind to, it can be provided from the command line
    let address = env::args().nth(1).unwrap_or_else(|| String::from("127.0.0.1:8080"));

    // To start the packet manager, you have to use the DefaultPacketManagerBuilder.
    let manager = DefaultPacketManagerBuilder::new()
        // On this builder, you are setting the address the socket is bound to
        .set_address(address.parse().map_err(|_| io::ErrorKind::InvalidInput)?)
        // and adding all your event listeners
        .add_event_listener(PacketLoggerExample)
        // You can do lots of configuration before starting, checkout all the setters on the
        // DefaultPacketManagerBuilder struct!
        // Once you're done configuring your packet manager, call build to bind the socket and start it.
        .build()?;

    // The packet manager returns the address the socket is bound to
    println!("Listening on address: {}", manager.address());

    // The channel is handled on the thread pool, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued packets are still going to be sent before the channel is closed
    manager.shutdown();
    Ok(())
}
