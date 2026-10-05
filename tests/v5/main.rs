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

mod basic;
mod client_api;
mod dispatcher;
mod limits;
mod protocol;
mod qos;
mod sink;
mod streaming;

struct St;

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

fn packet(res: Decoded) -> Packet {
    match res {
        Decoded::Packet(pkt, _) => pkt,
        _ => panic!(),
    }
}

async fn connect(msg: Connect) -> Result<ConnectAck<St>, TestError> {
    Ok(msg.ack(St))
}

fn pid(id: u16) -> NonZeroU16 {
    NonZeroU16::new(id).unwrap()
}

async fn try_connect_client(
    connect: client::Connect<SocketAddr>,
) -> Result<client::Client, ntex::error::Error<error::MqttClientError<Box<codec::ConnectAck>>>> {
    Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(connect)
        .await
}

async fn connect_client(addr: SocketAddr) -> client::Client {
    try_connect_client(client::Connect::new(addr).client_id("user"))
        .await
        .unwrap()
}

/// Opens a raw connection and sends CONNECT without waiting for CONNACK
async fn connect_raw(srv: &server::TestServer) -> (ntex::io::Io, codec::Codec) {
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    let pkt = codec::Connect::default().client_id("user");
    io.send(Encoded::Packet(pkt.into()), &codec).await.unwrap();
    (io, codec)
}

/// Opens a raw connection and completes the handshake
async fn handshake(srv: &server::TestServer) -> (ntex::io::Io, codec::Codec) {
    let (io, codec) = connect_raw(srv).await;
    io.recv(&codec).await.unwrap().unwrap();
    (io, codec)
}

/// Polls `f` until it returns `true`, the result tells if it happened in time
async fn poll_until(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..300 {
        if f() {
            return true;
        }
        sleep(Millis(10)).await;
    }
    false
}

/// Polls `f` until it returns `true`, panics if it does not happen in time
async fn wait_until(f: impl FnMut() -> bool) {
    assert!(poll_until(f).await, "condition is not met in time");
}

/// Publish packet for the topic
fn pkt_publish_to(topic: &str, qos: QoS, packet_id: Option<u16>) -> codec::Publish {
    codec::Publish {
        qos,
        topic: ByteString::from(topic),
        packet_id: packet_id.map(pid),
        ..pkt_publish()
    }
}

/// Sends a packet and returns the next packet received from the peer
async fn send_recv(io: &ntex::io::Io, codec: &codec::Codec, pkt: Encoded) -> Packet {
    io.send(pkt, codec).await.unwrap();
    packet(io.recv(codec).await.unwrap().unwrap())
}

/// Publish error that can be converted to a `PublishAck`,
/// `UnspecifiedError` is not convertible
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

/// Subscription options with the requested qos and `no_local` flag
fn sub_opts(qos: QoS, no_local: bool) -> codec::SubscriptionOptions {
    codec::SubscriptionOptions {
        qos,
        no_local,
        ..Default::default()
    }
}
