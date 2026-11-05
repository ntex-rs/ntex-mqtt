//! MQTT 5 integration tests with shared packet, connection, and polling helpers.

use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, rc::Rc};
use std::{convert::Infallible, future::Future, pin::Pin, time::Duration};

use ntex::service::pipeline::Pipeline;
use ntex::service::{cfg::SharedCfg, fn_service};
use ntex::time::{Millis, Seconds, sleep, timeout};
use ntex::util::{BytePages, ByteString, Bytes, join, lazy};
use ntex::{codec::Encoder, io::Framed, io::IoConfig, rt, server};

use ntex_mqtt::v5::codec::{self, Decoded, Encoded, Packet};
use ntex_mqtt::v5::{
    Connect, ConnectAck, MqttServer, ProtocolMessage, Publish, PublishAck, QoS, Router, Session,
    client, error,
};
use ntex_mqtt::{Control, MqttServiceConfig, Reason};

/// Connection handling, advertised capabilities, and basic protocol integration tests.
mod basic;
/// Client connection, routing, control, and keep-alive integration tests.
mod client_api;
/// Scripted wire-level checks for client and server dispatchers.
mod dispatcher;
/// Packet-size, keep-alive, frame-read, and outbound-credit integration tests.
mod limits;
/// Server routing, protocol acknowledgment, and middleware integration tests.
mod protocol;
/// Publish delivery, QoS exchanges, ordering, and acknowledgment-error integration tests.
mod qos;
/// Sink builders, readiness, encoding recovery, and shutdown integration tests.
mod sink;
/// Payload chunking, backpressure, and response-queue integration tests.
mod streaming;

/// Empty per-session state shared by the test servers.
struct St;

/// Nonconvertible service error used to exercise fatal handler failures.
#[derive(Debug)]
struct TestError;

impl From<Infallible> for TestError {
    fn from(_: Infallible) -> Self {
        TestError
    }
}

impl TryFrom<TestError> for PublishAck {
    type Error = TestError;

    fn try_from(err: TestError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

/// Builds an empty QoS 1 publish to `test` with packet identifier `1`.
fn pkt_publish() -> codec::Publish {
    codec::Publish {
        dup: false,
        retain: false,
        qos: codec::QoS::AtLeastOnce,
        topic: ByteString::from("test"),
        packet_id: Some(pid(1)),
        payload_size: 0,
        properties: Default::default(),
    }
}

/// Extracts a control packet, panicking if the decoded item is a publish or payload chunk.
fn packet(res: Decoded) -> Packet {
    match res {
        Decoded::Packet(pkt, _) => pkt,
        _ => panic!(),
    }
}

/// Accepts a test handshake with empty session state.
async fn connect(msg: Connect) -> Result<ConnectAck<St>, TestError> {
    Ok(msg.ack(St))
}

/// Builds a nonzero packet identifier, panicking if `id` is zero.
fn pid(id: u16) -> NonZeroU16 {
    NonZeroU16::new(id).unwrap()
}

/// Runs the default connector with the supplied connect builder, preserving connection errors.
async fn try_connect_client(
    connect: client::Connect<SocketAddr>,
) -> Result<client::Client, ntex::error::Error<error::MqttClientError<Box<codec::ConnectAck>>>> {
    Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(connect)
        .await
}

/// Connects an unstarted client named `user`, panicking if the handshake fails.
async fn connect_client(addr: SocketAddr) -> client::Client {
    try_connect_client(client::Connect::new(addr).client_id("user"))
        .await
        .unwrap()
}

/// Opens a raw connection and sends a connect packet without waiting for acknowledgment.
async fn connect_raw(srv: &server::TestServer) -> (ntex::io::Io, codec::Codec) {
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    let pkt = codec::Connect::default().client_id("user");
    io.send(Encoded::Packet(pkt.into()), &codec).await.unwrap();
    (io, codec)
}

/// Opens a raw connection and consumes the first handshake response.
async fn handshake(srv: &server::TestServer) -> (ntex::io::Io, codec::Codec) {
    let (io, codec) = connect_raw(srv).await;
    io.recv(&codec).await.unwrap().unwrap();
    (io, codec)
}

/// Polls `f` up to 300 times at ten-millisecond intervals and reports whether it became true.
async fn poll_until(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..300 {
        if f() {
            return true;
        }
        sleep(Millis(10)).await;
    }
    false
}

/// Uses `poll_until` and panics if `f` never returns `true` within its retry budget.
async fn wait_until(f: impl FnMut() -> bool) {
    assert!(poll_until(f).await, "condition is not met in time");
}

/// Builds an empty publish with the supplied topic, QoS, and optional nonzero packet identifier.
fn pkt_publish_to(topic: &str, qos: QoS, packet_id: Option<u16>) -> codec::Publish {
    codec::Publish {
        qos,
        topic: ByteString::from(topic),
        packet_id: packet_id.map(pid),
        ..pkt_publish()
    }
}

/// Sends an encoded item and extracts the next control packet, panicking on transport or
/// decode failure.
async fn send_recv(io: &ntex::io::Io, codec: &codec::Codec, pkt: Encoded) -> Packet {
    io.send(pkt, codec).await.unwrap();
    packet(io.recv(codec).await.unwrap().unwrap())
}

/// Publish error convertible to a reason-bearing acknowledgment except for `UnspecifiedError`.
#[derive(Clone, Debug, PartialEq)]
struct AckError(codec::PublishAckReason);

impl From<Infallible> for AckError {
    fn from(_: Infallible) -> Self {
        AckError(codec::PublishAckReason::UnspecifiedError)
    }
}

impl TryFrom<AckError> for PublishAck {
    type Error = AckError;

    fn try_from(err: AckError) -> Result<Self, Self::Error> {
        if err.0 == codec::PublishAckReason::UnspecifiedError {
            Err(err)
        } else {
            Ok(PublishAck::new(err.0).reason(ByteString::from_static("converted")))
        }
    }
}

/// Builds subscription options with the requested QoS and `no_local` flag.
fn sub_opts(qos: QoS, no_local: bool) -> codec::SubscriptionOptions {
    codec::SubscriptionOptions {
        qos,
        no_local,
        ..Default::default()
    }
}
