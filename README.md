# libudp

libudp is the UDP extension of [libev](https://github.com/edengates/libev), a Rust library for event driven network
applications that runs on the native I/O of each operating system. It brings the same design to datagrams: IOCP on
Windows, io_uring or epoll on Linux, and kqueue on macOS and FreeBSD drive the sockets, and your code only has to react
to events.

Where libev moves streams of bytes, libudp moves packets. A `Packet` holds the content of one datagram together with the
address of its peer: the sender of a received packet, or the recipient of a sent one. A socket either receives from every
address or is connected to a single remote address. It is configured and started with the
`DefaultPacketManagerBuilder`, which returns the `PacketManager` that owns it.

Every socket is handled by a `PacketChannel`, which tells your listeners when it is ready, when a packet has been
received, when it has stayed idle for longer than the AFK timeout and when it has been closed. A listener is a
`PacketListenerAdapter`. Several listeners can be chained, and each one hands its own type of packets on to the next.

Besides the events, libudp provides:

* the `SocketAction`s of libev to send packets, such as `queue`, `complete` and `map`
* a `ThreadChannel` to write, flush, close or deafen a channel from any other thread, and a channel cache to find the
  channels of a `PacketManager` again
* a graceful shutdown that still sends the enqueued packets, and a shutdown that cancels them
* the thread pool of libev, which sockets can share, and settings for the buffers, for the limits of a read loop and of a
  batch, and for the power of the threads on Windows

## Installation

Add the library to your `Cargo.toml`:

```toml
[dependencies]
libudp = "$version" # replace $version with the latest version
bytes = "1" # required for the BytesMut contents of the packets received and sent by the listeners
```

The minimum Rust version supported by libudp is Rust 1.86.

## Getting started

There are a number of [examples](examples) to introduce you to libudp.

Every socket implemented by libudp starts out using the `DefaultPacketManagerBuilder`. The builder provides a set of
settings for the socket, the thread pool, the buffers and the timeouts it wants to use:

* `set_address` and `set_remote_address` - Choose the address the socket is bound to, and the address it is connected to
* `set_address_shared` and `set_broadcast_enabled` - Choose whether the port is shared with other sockets and whether
  broadcasting is allowed
* `set_receive_buffer_size` and `set_send_buffer_size` - Choose the sizes of the buffers of the operating system
* `set_buffer_size` - Choose the size, in bytes, of the buffer used to read a packet
* `set_chunk_limit` and `set_packet_limit` - Choose the maximum number of packets to read per read loop, and to send
  per batch
* `set_socket_pool` - Choose the thread pool that serves the socket
* `set_power_suppressed_enabled` - Choose whether the system may suppress the power of the threads of the socket pool
  to save energy, which only affects Windows
* `set_afk_timeout`, `set_shutdown_timeout` and `set_timeout` - Choose how long a channel may stay idle, how long it
  may take to flush its packets, and how long the listeners may handle the remaining events after it is closed
* `add_event_listener` and `add_event_listener_provider` - Choose the listeners that receive the events of the socket

Here are some possible use-cases:

### Example: Packet Logging

Simply logging packets to the console. Making use of `DefaultPacketManagerBuilder`, the intended entry point for every
socket.

Starting your server and attaching an event listener:

```rust
// The channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct PacketLoggerExample;

impl PacketListenerAdapter for PacketLoggerExample {
    // Gets called for every packet received from the socket
    async fn on_packet_received(&mut self, _channel: &mut PacketChannel, packet: Packet) -> io::Result<()> {
        // The content is the raw bytes of the datagram, the address is the sender of the packet
        println!("[packet] {}: {}", packet.address(), String::from_utf8_lossy(packet.content()));
        Ok(())
    }
}

fn main() -> io::Result<()> {
    // On this builder, you are adding all your event listeners and socket configuration
    let manager = DefaultPacketManagerBuilder::new()
        .set_address("127.0.0.1:8080".parse().unwrap())
        .add_event_listener(PacketLoggerExample)
        .build()?;

    // The packet manager returns the address the socket is bound to
    println!("Listening on address: {}", manager.address());

    // The channel is handled on the thread pool, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued packets are still going to be sent before the channel is closed
    manager.shutdown();
    Ok(())
}
```
You can find a more thorough example with the [PacketLoggerExample](examples/packet_logger.rs) struct.

### Example: Packet Create

Simply sending a packet to an address. Making use of the same `DefaultPacketManagerBuilder`, this time connecting the
socket to the address.

Starting your client and sending a packet once the socket is ready:

```rust
// The channel uses its own clone of the listener, so it has to implement Clone
#[derive(Clone)]
struct PacketCreateExample {
    address: SocketAddr,
}

impl PacketListenerAdapter for PacketCreateExample {
    // Gets called once the socket is bound
    async fn on_ready(&mut self, channel: &mut PacketChannel) -> io::Result<()> {
        // send_packet(...) creates a PacketCreateAction, nothing is sent until you queue it
        let packet = Packet::new(BytesMut::from(&b"ping"[..]), self.address);
        channel.send_packet(packet).queue(
            |_| println!("[queued] ping"),
            |error| eprintln!("The packet could not be sent: {}", error),
        )
    }

    // Gets called for every reply of the address, this one prints it and closes the channel
    async fn on_packet_received(&mut self, channel: &mut PacketChannel, packet: Packet) -> io::Result<()> {
        println!("[reply] {}", String::from_utf8_lossy(packet.content()));
        channel.close();
        Ok(())
    }
}

fn main() -> io::Result<()> {
    let address: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    // A connected socket only receives the packets of the address it is connected to
    let manager = DefaultPacketManagerBuilder::new()
        .set_remote_address(address)
        .add_event_listener(PacketCreateExample { address })
        .build()?;

    // The channel is handled on the thread pool, so the main thread can wait here until Enter is pressed
    io::stdin().read_line(&mut String::new())?;

    // Already enqueued packets are still going to be sent before the channel is closed
    manager.shutdown();
    Ok(())
}
```

You can find a more thorough example with the [PacketCreateExample](examples/packet_create.rs) struct.

## [libev](https://github.com/edengates/libev)

Created and maintained by [edengates](https://github.com/edengates) and extended with UDP sockets by libudp  
Provides the event loops, channels, channel events and `Sink` that libudp **builds its UDP sockets on**, using the
native I/O APIs of each operating system.

Note that libudp re-exports the types of libev it uses, so your project does not need a separate dependency on libev.

Every libudp `0.4.x` release requires libev `0.4`.
