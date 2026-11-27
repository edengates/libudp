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
use libudp::entities::ISnowflake;
use libudp::entities::channel::concrete::ThreadChannel;
use libudp::hooks::PacketListenerAdapter;
use libudp::requests::sockactions::SocketAction;
use libudp::{DefaultPacketManagerBuilder, Packet, DatagramChannel, PacketManager};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct EchoAdapter;

impl PacketListenerAdapter for EchoAdapter {
    async fn on_packet_received(&mut self, channel: &mut DatagramChannel, packet: Packet) -> io::Result<()> {
        channel.send_packet(packet).complete()
    }
}

#[derive(Clone)]
struct ThreadChannelAdapter {
    callback: Sender<ThreadChannel>,
}

impl PacketListenerAdapter for ThreadChannelAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        self.callback.send(channel.thread_channel().clone()).ok();
        Ok(())
    }
}

#[derive(Clone)]
struct ThreadEchoAdapter {
    callback: Sender<ThreadChannel>,
}

impl PacketListenerAdapter for ThreadEchoAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        self.callback.send(channel.thread_channel().clone()).ok();
        Ok(())
    }

    async fn on_packet_received(&mut self, channel: &mut DatagramChannel, packet: Packet) -> io::Result<()> {
        channel.send_packet(packet).complete()
    }
}

#[derive(Clone)]
struct ReadyAdapter {
    callback: Sender<SocketAddr>,
}

impl PacketListenerAdapter for ReadyAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        self.callback.send(channel.packet_manager().local_address()).ok();
        channel.on_ready();
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelDisconnectAdapter {
    callback: Sender<bool>,
}

impl PacketListenerAdapter for ChannelDisconnectAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        self.callback.send(true).ok();
        channel.on_ready();
        Ok(())
    }

    async fn on_channel_disconnect(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        self.callback.send(false).ok();
        channel.on_channel_disconnect();
        Ok(())
    }
}

#[derive(Clone)]
struct SessionInvalidateAdapter;

impl PacketListenerAdapter for SessionInvalidateAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        channel.close();
        Ok(())
    }
}

#[derive(Clone)]
struct DeadLockAdapter {
    callback: Sender<io::Result<()>>,
}

impl PacketListenerAdapter for DeadLockAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        let address = "127.0.0.1:9".parse().unwrap();
        let data = channel.thread_channel().send_packet(Packet::new(BytesMut::from(&b"ping"[..]), address)).complete();
        self.callback.send(data).ok();
        Ok(())
    }
}

#[derive(Clone)]
struct SelfDeafenAdapter {
    handle: Sender<ThreadChannel>,
    callback: Sender<Vec<u8>>,
}

impl PacketListenerAdapter for SelfDeafenAdapter {
    async fn on_ready(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        channel.set_self_deafened(true);
        self.handle.send(channel.thread_channel().clone()).ok();
        Ok(())
    }

    async fn on_packet_received(&mut self, _channel: &mut DatagramChannel, packet: Packet) -> io::Result<()> {
        self.callback.send(packet.data().to_vec()).ok();
        Ok(())
    }
}

#[derive(Clone)]
struct ChannelTimeoutAdapter {
    callback: Sender<()>,
}

impl PacketListenerAdapter for ChannelTimeoutAdapter {
    async fn on_channel_timeout(&mut self, channel: &mut DatagramChannel) -> io::Result<()> {
        self.callback.send(()).ok();
        channel.close();
        Ok(())
    }
}

fn builder() -> DefaultPacketManagerBuilder {
    DefaultPacketManagerBuilder::new()
        .set_local_address("127.0.0.1:0".parse().unwrap())
        .set_socket_pool(pool())
}

fn manager<L>(listener: L) -> PacketManager
where
    L: PacketListenerAdapter + Clone + Sync + 'static,
{
    builder().add_event_listener(listener).build().unwrap()
}

fn client() -> UdpSocket {
    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client
}

fn receive(client: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut data = [0; 2048];
    let (length, sender) = client.recv_from(&mut data).unwrap();
    (data[..length].to_vec(), sender)
}

fn packet(content: &[u8], address: SocketAddr) -> Packet {
    Packet::new(BytesMut::from(content), address)
}

fn wait_until<F: FnMut() -> bool>(mut condition: F) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn echo() {
    let server = manager(EchoAdapter);
    let client = client();

    client.send_to(b"ping", server.local_address()).unwrap();

    let (received, sender) = receive(&client);
    assert_eq!(received, b"ping");
    assert_eq!(sender, server.local_address());
}

#[test]
fn payload() {
    let server = manager(EchoAdapter);
    let client = client();
    let content: Vec<u8> = (0..1400).map(|index| (index % 251) as u8).collect();

    client.send_to(&content, server.local_address()).unwrap();

    assert_eq!(receive(&client).0, content);
}

#[test]
fn burst() {
    let server = builder()
        .set_receive_buffer_size(1024 * 1024)
        .set_send_buffer_size(1024 * 1024)
        .add_event_listener(EchoAdapter)
        .build()
        .unwrap();
    let client = client();

    for index in 0u32..50 {
        client.send_to(&index.to_le_bytes(), server.local_address()).unwrap();
    }

    let mut seen = [false; 50];
    for _ in 0..50 {
        let (data, _) = receive(&client);
        seen[u32::from_le_bytes(data[..4].try_into().unwrap()) as usize] = true;
    }
    assert!(seen.iter().all(|seen| *seen));
}

#[test]
fn truncate() {
    let server = builder().set_buffer_size(4).add_event_listener(EchoAdapter).build().unwrap();
    let client = client();

    client.send_to(b"hello world", server.local_address()).unwrap();

    assert_eq!(receive(&client).0, b"hell");
}

#[test]
fn connected() {
    let client = client();
    let stranger = UdpSocket::bind("127.0.0.1:0").unwrap();
    stranger.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let server = builder()
        .set_remote_address(client.local_addr().unwrap())
        .add_event_listener(EchoAdapter)
        .build()
        .unwrap();

    stranger.send_to(b"ping", server.local_address()).unwrap();
    assert!(stranger.recv_from(&mut [0; 16]).is_err());

    client.send_to(b"pong", server.local_address()).unwrap();
    assert_eq!(receive(&client).0, b"pong");
}

#[test]
fn ready() {
    let (sender, receiver) = mpsc::channel();
    let server = manager(ReadyAdapter { callback: sender });

    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), server.local_address());
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn disconnect() {
    let (sender, receiver) = mpsc::channel();
    let server = manager(ChannelDisconnectAdapter { callback: sender });
    let event = || receiver.recv_timeout(Duration::from_secs(5)).unwrap();

    assert!(event());
    server.shutdown_now().unwrap();
    assert!(!event());
    assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
}

#[test]
fn close() {
    let server = manager(SessionInvalidateAdapter);
    let address = server.local_address();

    wait_until(|| UdpSocket::bind(address).is_ok());
}

#[test]
fn shutdown() {
    let server = manager(EchoAdapter);
    let address = server.local_address();
    assert!(!server.await_shutdown(Duration::ZERO));

    server.shutdown();

    assert!(server.await_shutdown(Duration::from_secs(5)));
    assert!(UdpSocket::bind(address).is_ok());
}

#[test]
fn shutdown_now() {
    let server = manager(EchoAdapter);
    let client = client();
    client.send_to(b"ping", server.local_address()).unwrap();
    assert_eq!(receive(&client).0, b"ping");

    server.shutdown_now().unwrap();
    assert!(server.await_shutdown(Duration::from_secs(5)));

    client.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    client.send_to(b"pong", server.local_address()).unwrap();
    assert!(client.recv_from(&mut [0; 16]).is_err());
}

#[test]
fn await_shutdown() {
    let server = manager(EchoAdapter);

    server.shutdown();

    assert!(server.await_shutdown(Duration::MAX));
}

#[test]
fn flush() {
    let (sender, receiver) = mpsc::channel();
    let server = manager(ThreadChannelAdapter { callback: sender });
    let client = client();
    let target = client.local_addr().unwrap();
    let channel = receiver.recv_timeout(Duration::from_secs(5)).unwrap();

    for index in 0u8..50 {
        channel.write(packet(&[index], target)).unwrap();
    }
    channel.flush().unwrap();
    server.shutdown();

    let mut next = 0u8;
    while next < 50 {
        assert_eq!(receive(&client).0, [next]);
        next += 1;
    }
}

#[test]
fn thread_channel() {
    let (sender, receiver) = mpsc::channel();
    let _server = manager(ThreadChannelAdapter { callback: sender });
    let client = client();
    let target = client.local_addr().unwrap();
    let channel = receiver.recv_timeout(Duration::from_secs(5)).unwrap();

    thread::spawn(move || {
        channel.write(packet(b"ping", target)).unwrap();
        channel.flush().unwrap();
        let sent = channel.send_packet(packet(b"pong", target)).map(|_| "sent").complete();
        assert_eq!(sent.unwrap(), "sent");
        channel.set_self_deafened(true).unwrap();
        channel.close().unwrap();
        wait_until(|| {
            matches!(channel.write(packet(b"gone", target)), Err(error) if error.kind() == io::ErrorKind::NotConnected)
        });
    })
    .join()
    .unwrap();

    assert_eq!(receive(&client).0, b"ping");
    assert_eq!(receive(&client).0, b"pong");
}

#[test]
fn thread_queue() {
    let (sender, receiver) = mpsc::channel();
    let _server = manager(ThreadChannelAdapter { callback: sender });
    let client = client();
    let target = client.local_addr().unwrap();
    let channel = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    let (callback, results) = mpsc::channel();
    let (success, failure) = (callback.clone(), callback);

    channel.send_packet(packet(b"ping", target)).queue(
        move |_| {
            success.send(Ok(())).ok();
        },
        move |error| {
            failure.send(Err(error)).ok();
        },
    );

    assert!(results.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    assert_eq!(receive(&client).0, b"ping");
}

#[test]
fn deadlock() {
    let (sender, receiver) = mpsc::channel();
    let _server = manager(DeadLockAdapter { callback: sender });

    let data = receiver.recv_timeout(Duration::from_secs(5)).unwrap();

    assert_eq!(data.unwrap_err().kind(), io::ErrorKind::Deadlock);
}

#[test]
fn channel_cache() {
    let (sender, receiver) = mpsc::channel();
    let server = manager(ThreadChannelAdapter { callback: sender });
    let channel = receiver.recv_timeout(Duration::from_secs(5)).unwrap();

    assert_eq!(server.channel_cache().len(), 1);
    assert_eq!(server.channels::<Packet, Packet>().len(), 1);
    assert!(server.get_channel_by_id::<Packet, Packet>(channel.id()).is_some());
    assert!(server.get_channel_by_id::<String, String>(channel.id()).is_none());

    channel.close().unwrap();
    wait_until(|| server.channel_cache().is_empty());
    assert!(server.get_channel_by_id::<Packet, Packet>(channel.id()).is_none());
}

#[test]
fn deafen() {
    let (handle, _receiver) = mpsc::channel();
    let (sender, receiver) = mpsc::channel();
    let server = manager(SelfDeafenAdapter { handle, callback: sender });
    let client = client();

    client.send_to(b"ping", server.local_address()).unwrap();

    assert!(receiver.recv_timeout(Duration::from_millis(300)).is_err());
}

#[test]
fn undeafen() {
    let (handle, channels) = mpsc::channel();
    let (sender, receiver) = mpsc::channel();
    let server = manager(SelfDeafenAdapter { handle, callback: sender });
    let client = client();
    let channel = channels.recv_timeout(Duration::from_secs(5)).unwrap();
    client.send_to(b"ping", server.local_address()).unwrap();
    assert!(receiver.recv_timeout(Duration::from_millis(300)).is_err());

    channel.set_self_deafened(false).unwrap();

    assert_eq!(receiver.recv_timeout(Duration::from_secs(5)).unwrap(), b"ping");
}

#[test]
fn idle() {
    let (sender, receiver) = mpsc::channel();
    let server = builder()
        .set_afk_timeout(Duration::from_millis(300))
        .add_event_listener(ChannelTimeoutAdapter { callback: sender })
        .build()
        .unwrap();
    let client = client();

    for _ in 0..4 {
        client.send_to(b"ping", server.local_address()).unwrap();
        thread::sleep(Duration::from_millis(50));
    }
    assert!(receiver.try_recv().is_err());

    assert!(receiver.recv_timeout(Duration::from_secs(5)).is_ok());
}

#[test]
fn refused() {
    let (sender, receiver) = mpsc::channel();
    let server = manager(ThreadEchoAdapter { callback: sender });
    let channel = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    let closed = UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();

    channel.send_packet(packet(b"ping", closed)).complete().unwrap();
    channel.send_packet(packet(b"pong", closed)).complete().unwrap();

    let client = client();
    client.send_to(b"ping", server.local_address()).unwrap();
    assert_eq!(receive(&client).0, b"ping");
}

#[test]
fn handler() {
    assert!(DefaultPacketManagerBuilder::new().build().is_err());
}

#[test]
fn builder_settings() {
    let remote = client().local_addr().unwrap();
    let manager = builder()
        .set_remote_address(remote)
        .set_address_shared(true)
        .set_broadcast_enabled(true)
        .set_receive_buffer_size(65536)
        .set_send_buffer_size(32768)
        .set_buffer_size(3)
        .set_shutdown_timeout(Duration::from_secs(8))
        .set_chunk_limit(10)
        .set_packet_limit(13)
        .set_power_suppressed_enabled(true)
        .set_afk_timeout(Duration::from_secs(10))
        .set_timeout(Duration::from_secs(11))
        .add_event_listener(EchoAdapter)
        .build()
        .unwrap();

    assert!(manager.local_address().ip().is_loopback());
    assert_eq!(manager.remote_address(), Some(remote));
    assert!(manager.is_address_shared());
    assert!(manager.is_broadcast_enabled());
    assert_eq!(manager.receive_buffer_size(), Some(65536));
    assert_eq!(manager.send_buffer_size(), Some(32768));
    assert_eq!(manager.buffer_size(), 3);
    assert_eq!(manager.shutdown_timeout(), Duration::from_secs(8));
    assert_eq!(manager.chunk_limit(), 10);
    assert_eq!(manager.packet_limit(), 13);
    assert!(manager.is_power_suppressed_enabled());
    assert_eq!(manager.afk_timeout(), Some(Duration::from_secs(10)));
    assert_eq!(manager.timeout(), Duration::from_secs(11));
}

#[test]
fn context_map() {
    let manager = manager(EchoAdapter);

    manager.context_map().write().unwrap().insert(String::from("id"), Arc::new(7u32));

    let value = manager.context_map().read().unwrap().get("id").cloned().unwrap();
    assert!(value.clone().downcast::<String>().is_err());
    assert_eq!(*value.downcast::<u32>().unwrap(), 7);
}
