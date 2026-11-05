//! Integration tests for a listener serving both MQTT v3 and v5.

use std::convert::Infallible;

use ntex::util::{ByteString, Bytes};
use ntex::{Pipeline, SharedCfg, server};
use ntex_mqtt::{MqttServer, v3, v5};

/// Stateless session data attached to accepted test connections.
struct St;

/// Service error used to exercise test-handler failures and satisfy service error bounds.
#[derive(Debug)]
struct TestError;

impl From<Infallible> for TestError {
    fn from(_: Infallible) -> Self {
        TestError
    }
}

impl TryFrom<TestError> for v5::PublishAck {
    type Error = TestError;

    fn try_from(err: TestError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

/// STATEMENT: One server listener accepts both v3 and v5 clients and acknowledges their publishes.
///
/// HOW:
/// - Configure v3 and v5 handshake and publish services on the same server.
/// - Connect one client of each version and assert each QoS 1 publish succeeds.
#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new()
            .v3(v3::MqttServer::new(async |_| Ok::<_, TestError>(()))
                .build(async move |con: v3::Connect| Ok::<_, TestError>(con.ack(St, false))))
            .v5(
                v5::MqttServer::new(async move |p: v5::Publish| Ok::<_, TestError>(p.ack()))
                    .build(async move |con: v5::Connect| Ok::<_, TestError>(con.ack(St))),
            )
    });

    // connect to v5 server
    let client = Pipeline::new(SharedCfg::default(), v5::client::MqttConnector::new())
        .call(v5::client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    sink.close();

    // connect to v3 server
    let client = Pipeline::new(SharedCfg::default(), v3::client::MqttConnector::new())
        .call(v3::client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    sink.close();

    Ok(())
}

/// STATEMENT: The combined server rejects an unsupported protocol level with a negative v3 acknowledgement.
///
/// HOW:
/// - Send a raw `CONNECT` advertising protocol level six.
/// - Assert a `CONNACK` with `UnacceptableProtocolVersion` and no session present, then end of stream.
#[ntex::test]
async fn test_unsupported_protocol_level() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new()
            .v3(v3::MqttServer::new(async |_| Ok::<_, TestError>(()))
                .build(async move |con: v3::Connect| Ok::<_, TestError>(con.ack(St, false))))
            .v5(
                v5::MqttServer::new(async move |p: v5::Publish| Ok::<_, TestError>(p.ack()))
                    .build(async move |con: v5::Connect| Ok::<_, TestError>(con.ack(St))),
            )
    });

    let io = srv.connect().await.unwrap();
    let codec = v3::codec::Codec::default();
    io.encode_slice(b"\x10\x0c\x00\x04MQTT\x06\x02\x00\x3C\x00\x00")
        .unwrap();

    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        ack,
        v3::codec::Decoded::Packet(
            v3::codec::Packet::ConnectAck(v3::codec::ConnectAck {
                return_code: v3::codec::ConnectAckReason::UnacceptableProtocolVersion,
                session_present: false
            }),
            2
        )
    );
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}
