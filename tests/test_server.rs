//! MQTT v3 client and server integration tests.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering::Relaxed};
use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, time::Duration};

use ntex::service::{Pipeline, Service, cfg::SharedCfg, fn_service};
use ntex::time::{Millis, Seconds, sleep};
use ntex::util::{BytePages, ByteString, Bytes, join_all, lazy};
use ntex::{codec::Encoder, io::IoConfig, server};

use ntex_mqtt::v3::codec::{self, Decoded, Encoded, Packet};
use ntex_mqtt::v3::{
    self, Connect, ConnectAck, MqttServer, ProtocolMessage, Publish, Session, client,
};
use ntex_mqtt::{Control, MqttServiceConfig, QoS, Reason, error::MqttProtocolError};

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

impl From<TestError> for () {
    fn from(_: TestError) {}
}

impl From<()> for TestError {
    fn from(_: ()) -> Self {
        TestError
    }
}

impl From<v3::error::SendPacketError> for TestError {
    fn from(_: v3::error::SendPacketError) -> Self {
        TestError
    }
}

/// Accepts a test session without resuming state and sets a 16-second idle timeout.
async fn connect(msg: Connect) -> Result<ConnectAck<St>, ()> {
    msg.packet();
    msg.io();
    msg.sink();
    Ok(msg.ack(St, false).idle_timeout(Seconds(16)))
}

/// Creates a nonzero packet identifier, panicking if `id` is zero.
fn pid(id: u16) -> NonZeroU16 {
    NonZeroU16::new(id).unwrap()
}

/// Builds an empty QoS 1 publish to `test` with packet identifier one.
fn pkt_publish() -> codec::Publish {
    codec::Publish {
        dup: false,
        retain: false,
        qos: codec::QoS::AtLeastOnce,
        topic: ByteString::from("test"),
        packet_id: Some(pid(1)),
        payload_size: 0,
    }
}

/// Runs the v3 client handshake with default shared configuration and returns connection errors.
async fn try_connect_client(
    connect: client::Connect<SocketAddr>,
) -> Result<client::Client, ntex::error::Error<client::MqttClientError<codec::ConnectAck>>> {
    Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(connect)
        .await
}

/// Connects a client named `user` to `addr`, panicking if the handshake fails.
async fn connect_client(addr: SocketAddr) -> client::Client {
    try_connect_client(client::Connect::new(addr).client_id("user"))
        .await
        .unwrap()
}

/// Opens a raw v3 connection and sends `CONNECT` without waiting for `CONNACK`.
async fn connect_raw(srv: &server::TestServer) -> (ntex::io::Io, codec::Codec) {
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    let pkt = codec::Connect::default().client_id("user");
    io.send(Encoded::Packet(pkt.into()), &codec).await.unwrap();
    (io, codec)
}

/// Sends a raw v3 `CONNECT` and consumes the first server response, expecting it to exist.
async fn handshake(srv: &server::TestServer) -> (ntex::io::Io, codec::Codec) {
    let (io, codec) = connect_raw(srv).await;
    io.recv(&codec).await.unwrap().unwrap();
    (io, codec)
}

/// STATEMENT: QoS 1 publishing succeeds for a valid topic and rejects a wildcard topic.
///
/// HOW:
/// - Start a v3 server and client, then publish to `test` and `#`.
/// - Assert the valid publish succeeds and the wildcard publish fails.
#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv =
        server::test_server(async |_| MqttServer::new(async |_| Ok::<_, ()>(())).build(connect));

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("test"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());

    let res = sink
        .publish(ByteString::from_static("#"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_err());

    sink.close();
    Ok(())
}

/// STATEMENT: Streamed publishes preserve payload ordering and combine chunks below the configured minimum.
///
/// HOW:
/// - Set the minimum chunk size to four bytes and send three streamed payloads plus one buffered payload.
/// - Assert all valid sends succeed, a wildcard-topic stream fails, and the server receives the expected five chunks.
#[ntex::test]
async fn test_simple_streaming() -> std::io::Result<()> {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let chunks = chunks2.clone();
        MqttServer::new(async move |p: Publish| {
            let chunks = chunks.clone();
            while let Ok(Some(chunk)) = p.read().await {
                chunks.lock().unwrap().push(chunk);
            }
            Ok::<_, ()>(())
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"111111")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    // pkt 2
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(5);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"22")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"222")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    // pkt 3
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(2);
    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"33")).await.unwrap();
    });
    let res = fut.await;
    assert!(res.is_ok());

    // pkt 4
    let res = sink
        .publish(ByteString::from_static("test"))
        .send_at_least_once(Bytes::from_static(b"123"))
        .await;
    assert!(res.is_ok());

    let (fut, _) = sink
        .publish(ByteString::from_static("#"))
        .stream_at_least_once(12);
    let res = fut.await;
    assert!(res.is_err());

    sink.close();

    assert_eq!(
        &chunks.lock().unwrap()[..],
        vec![
            Bytes::from_static(b"1111"),
            Bytes::from_static(b"111111"),
            Bytes::from_static(b"22222"),
            Bytes::from_static(b"33"),
            Bytes::from_static(b"123"),
        ]
    );
    Ok(())
}

/// STATEMENT: A detached streaming payload retains its publish metadata and can be collected in full.
///
/// HOW:
/// - Take the payload from a QoS 1 `Publish` and verify its flags, topic, identifier, and sizes.
/// - Send ten bytes in two chunks and assert `read_all` collects all ten bytes and the publish succeeds.
#[ntex::test]
async fn test_simple_streaming2() {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let chunks = chunks2.clone();
        MqttServer::new(async move |mut p: Publish| {
            let chunks = chunks.clone();
            let pl = p.take_payload();
            assert!(format!("{:?}", pl).contains("StreamingPayload"));
            assert!(!p.dup());
            assert!(!p.retain());
            assert_eq!(p.id(), Some(pid(1)));
            assert_eq!(p.qos(), QoS::AtLeastOnce);
            assert_eq!(p.topic().path(), "test");
            assert_eq!(p.topic_mut().path(), "test");
            assert_eq!(p.publish_topic(), "test");
            assert_eq!(p.packet_size(), 18);
            assert_eq!(p.payload_size(), 10);
            let chunk = pl.read_all().await.unwrap();
            chunks.lock().unwrap().push(chunk);
            Ok::<_, ()>(())
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"111111")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    assert_eq!(
        &chunks.lock().unwrap()[..],
        vec![Bytes::from_static(b"1111111111"),]
    );
}

/// STATEMENT: Closing a connection during a streamed publish fails the send after its first chunk.
///
/// HOW:
/// - Send four of ten declared bytes, then close the client sink.
/// - Assert the publish fails and the server has recorded only the first four-byte chunk.
#[ntex::test]
async fn test_disconnect_while_streaming() -> std::io::Result<()> {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let chunks = chunks2.clone();
        MqttServer::new(async move |p: Publish| {
            let chunks = chunks.clone();
            loop {
                let res = p.read().await;
                chunks.lock().unwrap().push(res.clone());
                if res.is_err() {
                    break;
                }
            }
            Ok::<_, ()>(())
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        sink.close();
    });
    let res = fut.await;
    assert!(res.is_err());
    sleep(Millis(150)).await;

    assert_eq!(
        &chunks.lock().unwrap()[..],
        vec![Ok(Some(Bytes::from_static(b"1111")))]
    );
    Ok(())
}

/// STATEMENT: A payload sender blocked by write backpressure fails when the peer disconnects.
///
/// HOW:
/// - Trigger a large server stream without reading it and wait until the sent-chunk count stops increasing.
/// - Assert the sender is still pending, drop the peer connection, and verify `SendPacketError::Disconnected`.
#[ntex::test]
async fn test_streaming_waiter_peer_gone() -> std::io::Result<()> {
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = Arc::new(Mutex::new(None));
    let (sent2, result2) = (sent.clone(), result.clone());
    let srv = server::TestServerBuilder::new(async move |_| {
        let sent = sent2.clone();
        let result = result2.clone();
        MqttServer::new(async move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            let sent = sent.clone();
            let result = result.clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                let sink = sink.clone();
                let sent = sent.clone();
                let result = result.clone();
                ntex::rt::spawn(async move {
                    let chunk = Bytes::from(vec![0u8; 65536]);
                    let stream = sink
                        .publish("test")
                        .stream_at_most_once(4000 * 65536)
                        .await
                        .unwrap();
                    let res = loop {
                        if let Err(e) = stream.send(chunk.clone()).await {
                            break e;
                        }
                        sent.fetch_add(1, Relaxed);
                        // let the dispatcher process write backpressure
                        sleep(Millis(1)).await;
                    };
                    *result.lock().unwrap() = Some(res);
                });
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(0)))
    .start();

    let (io, codec) = handshake(&srv).await;

    // trigger server streaming PUBLISH, the payload is not read
    io.send(
        Encoded::Publish(
            codec::Publish {
                qos: codec::QoS::AtMostOnce,
                packet_id: None,
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();

    // wait for write backpressure
    let mut last = usize::MAX;
    for _ in 0..50 {
        sleep(Millis(200)).await;
        let n = sent.load(Relaxed);
        if n > 0 && n == last {
            break;
        }
        last = n;
    }
    assert_eq!(sent.load(Relaxed), last);
    assert!(result.lock().unwrap().is_none());

    drop(io);
    let mut res = None;
    for _ in 0..50 {
        sleep(Millis(100)).await;
        res = result.lock().unwrap().take();
        if res.is_some() {
            break;
        }
    }
    assert_eq!(res, Some(v3::error::SendPacketError::Disconnected));
    Ok(())
}

/// STATEMENT: Server handshake rejection helpers cause client connection attempts to fail.
///
/// HOW:
/// - Reject separate connections for bad credentials, an invalid identifier, missing authorization, and service unavailability.
/// - Require each connection to fail; for acknowledgement errors, check the reason code and absent session flag.
#[ntex::test]
async fn test_connect_fail() -> std::io::Result<()> {
    // bad user name or password
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.bad_username_or_pwd::<St>()))
    });
    let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .err()
        .unwrap();
    if let client::MqttClientError::Ack(codec::ConnectAck {
        session_present,
        return_code,
    }) = &*err
    {
        assert!(!session_present);
        assert_eq!(return_code, &codec::ConnectAckReason::BadUserNameOrPassword);
    }

    // identifier rejected
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_| Ok::<_, TestError>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.identifier_rejected::<St>()))
    });
    let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .err()
        .unwrap();
    if let client::MqttClientError::Ack(codec::ConnectAck {
        session_present,
        return_code,
    }) = &*err
    {
        assert!(!session_present);
        assert_eq!(return_code, &codec::ConnectAckReason::IdentifierRejected);
    }

    // not authorized
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.not_authorized::<St>()))
    });
    let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .err()
        .unwrap();
    if let client::MqttClientError::Ack(codec::ConnectAck {
        session_present,
        return_code,
    }) = &*err
    {
        assert!(!session_present);
        assert_eq!(return_code, &codec::ConnectAckReason::NotAuthorized);
    }

    // service unavailable
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.service_unavailable::<St>()))
    });
    let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .err()
        .unwrap();
    if let client::MqttClientError::Ack(codec::ConnectAck {
        session_present,
        return_code,
    }) = &*err
    {
        assert!(!session_present);
        assert_eq!(return_code, &codec::ConnectAckReason::ServiceUnavailable);
    }

    Ok(())
}

/// STATEMENT: The server completes the incoming QoS 2 acknowledgement exchange through its protocol service.
///
/// HOW:
/// - Enable QoS 2, send a raw `PUBLISH`, and assert the matching `PUBREC`.
/// - Send `PUBREL`, assert the matching `PUBCOMP`, and verify the protocol service observed the release.
#[ntex::test]
async fn test_qos2() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let release = release2.clone();
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .protocol(async move |msg| {
                if let ProtocolMessage::PublishRelease(msg) = msg {
                    release.store(true, Relaxed);
                    Ok::<_, ()>(msg.ack())
                } else {
                    Ok(msg.disconnect())
                }
            })
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let (io, codec) = handshake(&srv).await;

    let id = pid(1);
    io.send(
        Encoded::Publish(
            codec::Publish {
                qos: codec::QoS::ExactlyOnce,
                packet_id: Some(id),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();

    let result = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        result,
        Decoded::Packet(Packet::PublishReceived { packet_id: id }, 2)
    );

    io.send(
        Encoded::Packet(Packet::PublishRelease { packet_id: id }),
        &codec,
    )
    .await
    .unwrap();
    let result = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        result,
        Decoded::Packet(Packet::PublishComplete { packet_id: id }, 2)
    );

    assert!(release.load(Relaxed));
    Ok(())
}

/// STATEMENT: A client QoS 2 publish can be explicitly released through the server protocol service.
///
/// HOW:
/// - Enable QoS 2 and start the client dispatcher.
/// - Send with `send_exactly_once`, await `release`, and assert the server handled `PublishRelease`.
#[ntex::test]
async fn test_qos2_client() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let release = release2.clone();
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::PublishRelease(msg) => {
                    release.store(true, Relaxed);
                    Ok(msg.ack())
                }
                _ => Ok::<_, ()>(msg.disconnect()),
            })
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let received = sink
        .publish(ByteString::from_static("test"))
        .send_exactly_once(Bytes::new())
        .await
        .unwrap();
    received.release().await.unwrap();
    assert!(release.load(Relaxed));
    Ok(())
}

/// STATEMENT: The default server protocol service acknowledges repeated QoS 2 releases without closing the connection.
///
/// HOW:
/// - Enable QoS 2 and publish and release three messages through the client sink.
/// - Require every operation to succeed and assert the sink remains open.
#[ntex::test]
async fn test_qos2_default_protocol() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async |_| {
        MqttServer::new(async |_| Ok::<_, ()>(())).build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // default protocol service acknowledges PUBREL
    for _ in 0..3 {
        let received = sink
            .publish(ByteString::from_static("test"))
            .send_exactly_once(Bytes::new())
            .await
            .unwrap();
        received.release().await.unwrap();
    }
    assert!(sink.is_open());
    Ok(())
}

/// Extracts a control packet from decoded input, returning `None` for publish data.
fn decoded_packet(res: Decoded) -> Option<Packet> {
    if let Decoded::Packet(pkt, _) = res {
        Some(pkt)
    } else {
        None
    }
}

/// STATEMENT: Acknowledging a QoS 1 publish with a QoS 2 acknowledgement is a protocol violation.
///
/// HOW:
/// - Trigger a server QoS 1 publish and reply separately with `PUBREC` and `PUBCOMP`.
/// - Assert the connection closes, the reported violation expects `PUBACK`, and the pending publish fails as disconnected.
#[ntex::test]
async fn test_unexpected_ack_type() -> std::io::Result<()> {
    // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP (MQTT 3.1.1, 4.3.2, 4.3.3)
    let id = pid(1);
    for ack in [
        Packet::PublishReceived { packet_id: id },
        Packet::PublishComplete { packet_id: id },
    ] {
        let error = Arc::new(Mutex::new(None));
        let result = Arc::new(Mutex::new(None));
        let (error2, result2) = (error.clone(), result.clone());
        let srv = server::test_server(async move |_| {
            let error = error2.clone();
            let result = result2.clone();
            MqttServer::new(async move |ses: &Session<St>| {
                let sink = ses.sink().clone();
                let result = result.clone();
                Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                    let sink = sink.clone();
                    let result = result.clone();
                    ntex::rt::spawn(async move {
                        let res = sink.publish("test").send_at_least_once(Bytes::new()).await;
                        *result.lock().unwrap() = Some(res);
                    });
                    Ok::<_, TestError>(())
                }))
            })
            .control(async move |msg| {
                if let Control::Stop(Reason::Protocol(err)) = msg
                    && let MqttProtocolError::ProtocolViolation(e) = err.get_ref()
                {
                    *error.lock().unwrap() = Some(e.message());
                }
                Ok::<_, TestError>(None)
            })
            .build(connect)
        });

        let (io, codec) = handshake(&srv).await;

        // trigger server QoS 1 PUBLISH
        io.send(
            Encoded::Publish(
                codec::Publish {
                    qos: codec::QoS::AtMostOnce,
                    packet_id: None,
                    ..pkt_publish()
                },
                None,
            ),
            &codec,
        )
        .await
        .unwrap();
        let pkt = io.recv(&codec).await.unwrap().unwrap();
        assert!(
            matches!(pkt, Decoded::Publish(ref p, ..) if p.qos == codec::QoS::AtLeastOnce),
            "{pkt:?}"
        );

        // connection is closed
        io.send(Encoded::Packet(ack), &codec).await.unwrap();
        let _ = io.send(Encoded::Packet(Packet::PingRequest), &codec).await;
        let pkt = io.recv(&codec).await;
        assert!(matches!(pkt, Ok(None) | Err(_)), "{pkt:?}");
        assert_eq!(error.lock().unwrap().take(), Some("Expected PUBACK packet"));

        // publish fails instead of being acknowledged
        let mut res = None;
        for _ in 0..50 {
            res = result.lock().unwrap().take();
            if res.is_some() {
                break;
            }
            sleep(Millis(10)).await;
        }
        assert_eq!(res, Some(Err(v3::error::SendPacketError::Disconnected)));
    }
    Ok(())
}

/// STATEMENT: Concurrent QoS 2 publishes can both be released after both receipts arrive.
///
/// HOW:
/// - Trigger two server publishes and send both `PUBREC` packets before either release completes.
/// - Assert both matching `PUBREL` packets arrive, send their `PUBCOMP` packets, and verify both releases succeed.
#[ntex::test]
async fn test_qos2_release_multiple() -> std::io::Result<()> {
    let result = Arc::new(Mutex::new(None));
    let result2 = result.clone();
    let srv = server::test_server(async move |_| {
        let result = result2.clone();
        MqttServer::new(async move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            let result = result.clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                let sink = sink.clone();
                let result = result.clone();
                ntex::rt::spawn(async move {
                    // both PUBREC packets are received before release
                    let (r1, r2) = ntex::util::join(
                        sink.publish("a").send_exactly_once(Bytes::new()),
                        sink.publish("b").send_exactly_once(Bytes::new()),
                    )
                    .await;
                    let res = ntex::util::join(r1.unwrap().release(), r2.unwrap().release()).await;
                    *result.lock().unwrap() = Some(res);
                });
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    });

    let (io, codec) = handshake(&srv).await;

    // trigger server QoS 2 PUBLISH packets
    io.send(
        Encoded::Publish(
            codec::Publish {
                qos: codec::QoS::AtMostOnce,
                packet_id: None,
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    for id in [1, 2] {
        let pkt = io.recv(&codec).await.unwrap().unwrap();
        assert!(
            matches!(pkt, Decoded::Publish(ref p, ..) if p.packet_id == NonZeroU16::new(id)),
            "{pkt:?}"
        );
    }
    for id in [1, 2] {
        let packet_id = pid(id);
        io.send(
            Encoded::Packet(Packet::PublishReceived { packet_id }),
            &codec,
        )
        .await
        .unwrap();
    }

    // PUBREL is sent for each PUBLISH
    for id in [1, 2] {
        let pkt = ntex::time::timeout(Millis(1000), io.recv(&codec)).await;
        assert!(
            matches!(pkt, Ok(Ok(Some(Decoded::Packet(Packet::PublishRelease { packet_id }, _))))
                     if packet_id.get() == id),
            "{pkt:?}"
        );
    }
    for id in [1, 2] {
        let packet_id = pid(id);
        io.send(
            Encoded::Packet(Packet::PublishComplete { packet_id }),
            &codec,
        )
        .await
        .unwrap();
    }

    let mut res = None;
    for _ in 0..50 {
        res = result.lock().unwrap().take();
        if res.is_some() {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert_eq!(res, Some((Ok(()), Ok(()))));
    Ok(())
}

/// Starts a server that publishes QoS 2 data for each incoming publish and shares its eventual release success.
fn qos2_publisher() -> (server::TestServer, Arc<Mutex<Option<bool>>>) {
    let released = Arc::new(Mutex::new(None));
    let released2 = released.clone();

    let srv = server::test_server(async move |_| {
        let released = released2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let sink = con.sink().clone();
            let released = released.clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                let sink = sink.clone();
                let released = released.clone();
                ntex::rt::spawn(async move {
                    let res = match sink
                        .publish(ByteString::from_static("test/qos2"))
                        .send_exactly_once(Bytes::from_static(b"data"))
                        .await
                    {
                        Ok(rec) => rec.release().await.is_ok(),
                        Err(_) => false,
                    };
                    *released.lock().unwrap() = Some(res);
                });
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    });
    (srv, released)
}

/// Polls a shared release result up to 200 times at ten-millisecond intervals, returning `None` on timeout.
async fn wait_released(released: &Mutex<Option<bool>>) -> Option<bool> {
    for _ in 0..200 {
        if let Some(res) = *released.lock().unwrap() {
            return Some(res);
        }
        sleep(Millis(10)).await;
    }
    None
}

/// STATEMENT: Both client routing and protocol-message handlers can complete incoming QoS 2 publishes.
///
/// HOW:
/// - Trigger a server QoS 2 publish with a routed client handler, then repeat with a protocol-message handler.
/// - In both cases assert the payload and QoS, successful server release, and an open client sink.
#[ntex::test]
async fn test_qos2_server_to_client() -> std::io::Result<()> {
    // publish is handled by client router
    let (srv, released) = qos2_publisher();
    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let received = Rc::new(RefCell::new(Vec::new()));
    let received2 = received.clone();
    ntex::rt::spawn(
        client
            .resource(
                "test/qos2",
                fn_service(move |p: Publish| {
                    let received = received2.clone();
                    async move {
                        let payload = p.read_all().await.unwrap();
                        received.borrow_mut().push((p.qos(), payload));
                        Ok::<_, TestError>(())
                    }
                }),
            )
            .start_default(),
    );

    sink.publish(ByteString::from_static("trigger"))
        .send_at_most_once(Bytes::new())
        .await
        .unwrap();
    assert_eq!(wait_released(&released).await, Some(true));
    assert_eq!(
        *received.borrow(),
        vec![(QoS::ExactlyOnce, Bytes::from_static(b"data"))]
    );
    assert!(sink.is_open());

    // publish is handled by client protocol service
    let (srv, released) = qos2_publisher();
    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let received = Rc::new(RefCell::new(Vec::new()));
    let received2 = received.clone();
    ntex::rt::spawn(
        client.start(fn_service(move |msg: client::ProtocolMessage| {
            let received = received2.clone();
            async move {
                Ok::<_, TestError>(match msg {
                    client::ProtocolMessage::Publish(p) => {
                        let payload = p.read_all().await.unwrap();
                        received.borrow_mut().push((p.packet().qos, payload));
                        p.ack()
                    }
                    client::ProtocolMessage::PublishRelease(msg) => msg.ack(),
                    client::ProtocolMessage::Ping(msg) => msg.ack(),
                })
            }
        })),
    );

    sink.publish(ByteString::from_static("trigger"))
        .send_at_most_once(Bytes::new())
        .await
        .unwrap();
    assert_eq!(wait_released(&released).await, Some(true));
    assert_eq!(
        *received.borrow(),
        vec![(QoS::ExactlyOnce, Bytes::from_static(b"data"))]
    );
    assert!(sink.is_open());

    Ok(())
}

/// STATEMENT: QoS 2 redelivery is acknowledged repeatedly but delivered to the client resource only once.
///
/// HOW:
/// - Have a raw server send one publish followed by two duplicates with the same identifier, then `PUBREL`.
/// - Assert three `PUBREC` replies and one `PUBCOMP`, a single resource invocation, and an open sink.
#[ntex::test]
async fn test_qos2_redelivery_to_client() -> std::io::Result<()> {
    // raw server re-sends QoS 2 PUBLISH with DUP flag before PUBREL
    let completed = Arc::new(Mutex::new(None));
    let completed2 = completed.clone();
    let srv = server::test_server(async move |_| {
        let completed = completed2.clone();
        fn_service(async move |io: ntex::io::Io| {
            let codec = codec::Codec::default();
            let _ = io.recv(&codec).await;
            let ack = codec::ConnectAck {
                session_present: false,
                return_code: codec::ConnectAckReason::ConnectionAccepted,
            };
            io.send(Encoded::Packet(Packet::ConnectAck(ack)), &codec)
                .await
                .unwrap();

            let packet_id = pid(1);
            let mut responses = Vec::new();
            for dup in [false, true, true] {
                let pkt = codec::Publish {
                    dup,
                    qos: QoS::ExactlyOnce,
                    topic: ByteString::from_static("test/qos2"),
                    packet_id: Some(packet_id),
                    payload_size: 4,
                    ..pkt_publish()
                };
                io.send(
                    Encoded::Publish(pkt, Some(Bytes::from_static(b"data"))),
                    &codec,
                )
                .await
                .unwrap();
                responses.push(
                    io.recv(&codec)
                        .await
                        .ok()
                        .flatten()
                        .and_then(decoded_packet),
                );
            }
            io.send(
                Encoded::Packet(Packet::PublishRelease { packet_id }),
                &codec,
            )
            .await
            .unwrap();
            responses.push(
                io.recv(&codec)
                    .await
                    .ok()
                    .flatten()
                    .and_then(decoded_packet),
            );
            *completed.lock().unwrap() = Some(responses);
            let _ = io.recv(&codec).await;
            Ok::<_, ()>(())
        })
    });

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let received = Rc::new(RefCell::new(0));
    let received2 = received.clone();
    ntex::rt::spawn(
        client
            .resource(
                "test/qos2",
                fn_service(move |_: Publish| {
                    *received2.borrow_mut() += 1;
                    async { Ok::<_, TestError>(()) }
                }),
            )
            .start_default(),
    );

    let mut responses = None;
    for _ in 0..200 {
        responses = completed.lock().unwrap().take();
        if responses.is_some() {
            break;
        }
        sleep(Millis(10)).await;
    }
    let packet_id = pid(1);
    let pubrec = Some(Packet::PublishReceived { packet_id });
    assert_eq!(
        responses.unwrap(),
        vec![
            pubrec.clone(),
            pubrec.clone(),
            pubrec,
            Some(Packet::PublishComplete { packet_id })
        ]
    );
    assert_eq!(*received.borrow(), 1);
    assert!(sink.is_open());
    Ok(())
}

/// STATEMENT: The server protocol service acknowledges a ping request with a ping response.
///
/// HOW:
/// - Complete a raw handshake and send `PINGREQ`.
/// - Assert `PINGRESP` is received and the custom ping handler ran.
#[ntex::test]
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(async move |_| {
        let ping = ping2.clone();
        MqttServer::new(async |_| Ok::<_, TestError>(()))
            .protocol(async move |msg| {
                if let ProtocolMessage::Ping(msg) = msg {
                    ping.store(true, Relaxed);
                    Ok::<_, TestError>(msg.ack())
                } else {
                    Ok(msg.disconnect())
                }
            })
            .build(connect)
    });

    let (io, codec) = handshake(&srv).await;

    io.send(Encoded::Packet(codec::Packet::PingRequest), &codec)
        .await
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, Decoded::Packet(Packet::PingResponse, 0));
    assert!(ping.load(Relaxed));

    Ok(())
}

/// STATEMENT: A client keeps sending keep-alive pings while the server acknowledges them.
///
/// HOW:
/// - Connect with a one-second keep-alive interval and count server ping-handler calls.
/// - After 3.5 seconds, assert at least three pings and an open client sink.
#[ntex::test]
async fn test_client_keepalive() -> std::io::Result<()> {
    let pings = Arc::new(Mutex::new(0));
    let pings2 = pings.clone();

    let srv = server::test_server(async move |_| {
        let pings = pings2.clone();
        MqttServer::new(async |_| Ok::<_, TestError>(()))
            .protocol(async move |msg| {
                if let ProtocolMessage::Ping(msg) = msg {
                    *pings.lock().unwrap() += 1;
                    Ok::<_, TestError>(msg.ack())
                } else {
                    Ok(msg.disconnect())
                }
            })
            .build(connect)
    });

    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .keep_alive(Seconds(1)),
    )
    .await
    .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // PINGRESP is received, the client keeps pinging
    sleep(Duration::from_millis(3500)).await;
    assert!(sink.is_open());
    assert!(*pings.lock().unwrap() >= 3);

    Ok(())
}

/// Builds an empty QoS 1 publish for `topic` with the supplied packet identifier.
fn qos1_publish(topic: &'static str, id: u16) -> Encoded {
    Encoded::Publish(
        codec::Publish {
            topic: ByteString::from_static(topic),
            packet_id: NonZeroU16::new(id),
            ..pkt_publish()
        },
        None,
    )
}

/// Receives one decoded item within two seconds, panicking on timeout, closure, or a receive error.
async fn recv_pkt(io: &ntex::io::Io, codec: &codec::Codec) -> Decoded {
    ntex::time::timeout(Millis(2000), io.recv(codec))
        .await
        .expect("packet is not received")
        .unwrap()
        .unwrap()
}

/// STATEMENT: The server receive limit delays publishes without blocking acknowledgements or pings.
///
/// HOW:
/// - Set `max_receive` to one and send two publishes whose handlers await acknowledgement of server echo publishes.
/// - Assert a ping is answered while the first handler waits, then acknowledge each echo and verify both original publish acknowledgements.
#[ntex::test]
async fn test_max_receive_publish_only() -> std::io::Result<()> {
    // publish handler waits for the ack of its own publish, the limit
    // of in-flight publishes must not block acks and pings
    let srv = server::TestServerBuilder::new(async |_| {
        MqttServer::new(async |ses: &Session<St>| {
            let sink = ses.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                sink.publish("echo")
                    .send_at_least_once(Bytes::new())
                    .await
                    .unwrap();
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_receive(1)))
    .start();

    let (io, codec) = connect_raw(&srv).await;
    recv_pkt(&io, &codec).await;

    io.send(qos1_publish("test", 1), &codec).await.unwrap();
    io.send(qos1_publish("test", 2), &codec).await.unwrap();
    let Decoded::Publish(pkt, ..) = recv_pkt(&io, &codec).await else {
        panic!()
    };
    let echo1 = pkt.packet_id.unwrap();

    // ping is processed while the limit is reached, the second publish waits
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PingResponse, 0)
    );

    io.send(
        Encoded::Packet(Packet::PublishAck { packet_id: echo1 }),
        &codec,
    )
    .await
    .unwrap();
    let id = pid(1);
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PublishAck { packet_id: id }, 2)
    );
    let Decoded::Publish(pkt, ..) = recv_pkt(&io, &codec).await else {
        panic!()
    };
    let echo2 = pkt.packet_id.unwrap();

    io.send(
        Encoded::Packet(Packet::PublishAck { packet_id: echo2 }),
        &codec,
    )
    .await
    .unwrap();
    let id = pid(2);
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PublishAck { packet_id: id }, 2)
    );

    Ok(())
}

/// STATEMENT: Publishes queued by the receive limit reach the handler before a following disconnect.
///
/// HOW:
/// - Set `max_receive` to one, delay each publish handler, and send two publishes followed by `DISCONNECT`.
/// - Wait for handling and assert both publish identifiers were recorded in arrival order.
#[ntex::test]
async fn test_max_receive_disconnect_order() -> std::io::Result<()> {
    // publishes that wait for a slot are passed to the handler before
    // DISCONNECT is processed, as without the limit
    let handled = Arc::new(Mutex::new(Vec::new()));
    let handled2 = handled.clone();
    let srv = server::TestServerBuilder::new(async move |_| {
        let handled = handled2.clone();
        MqttServer::new(async move |_: &Session<St>| {
            let handled = handled.clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                handled.lock().unwrap().push(p.id().unwrap().get());
                sleep(Millis(50)).await;
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_receive(1)))
    .start();

    let (io, codec) = connect_raw(&srv).await;
    recv_pkt(&io, &codec).await;

    io.send(qos1_publish("test", 1), &codec).await.unwrap();
    io.send(qos1_publish("test", 2), &codec).await.unwrap();
    io.send(Encoded::Packet(Packet::Disconnect), &codec)
        .await
        .unwrap();
    for _ in 0..50 {
        if handled.lock().unwrap().len() == 2 {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert_eq!(*handled.lock().unwrap(), [1, 2]);

    Ok(())
}

/// STATEMENT: Client receive limiting does not deadlock a streamed publish handler awaiting an outgoing acknowledgement.
///
/// HOW:
/// - Test `max_receive` values one and zero with a four-byte minimum chunk size.
/// - Trigger a server publish, verify its full payload in the client handler, and publish an echo from that handler.
/// - Assert the server publish completes successfully in both configurations.
#[ntex::test]
async fn test_client_max_receive_publish_only() -> std::io::Result<()> {
    // client publish handler reads the streamed payload and waits
    // for the ack of its own publish
    for max_receive in [1, 0] {
        let result = Arc::new(Mutex::new(None));
        let result2 = result.clone();
        let srv = server::test_server(async move |_| {
            let result = result2.clone();
            MqttServer::new(async move |ses: &Session<St>| {
                let sink = ses.sink().clone();
                let result = result.clone();
                Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                    if p.topic().path() == "trigger" {
                        let sink = sink.clone();
                        let result = result.clone();
                        ntex::rt::spawn(async move {
                            let res = sink
                                .publish("test")
                                .send_at_least_once(Bytes::from_static(b"0123456789abcdef"))
                                .await;
                            *result.lock().unwrap() = Some(res.is_ok());
                        });
                    }
                    Ok::<_, TestError>(())
                }))
            })
            .build(connect)
        });

        let client = Pipeline::new(
            SharedCfg::new("MQTT")
                .add(
                    MqttServiceConfig::new()
                        .set_max_receive(max_receive)
                        .set_min_chunk_size(4),
                )
                .build(),
            client::MqttConnector::new(),
        )
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
        let sink = client.sink();
        let sink2 = sink.clone();
        ntex::rt::spawn(
            client
                .resource(
                    "test",
                    fn_service(move |p: Publish| {
                        let sink = sink2.clone();
                        async move {
                            // payload is streamed in chunks
                            assert_eq!(&p.read_all().await.unwrap()[..], b"0123456789abcdef");
                            sink.publish("echo")
                                .send_at_least_once(Bytes::new())
                                .await
                                .unwrap();
                            Ok::<_, TestError>(())
                        }
                    }),
                )
                .start_default(),
        );

        sink.publish("trigger")
            .send_at_most_once(Bytes::new())
            .await
            .unwrap();
        for _ in 0..100 {
            if result.lock().unwrap().is_some() {
                break;
            }
            sleep(Millis(10)).await;
        }
        assert_eq!(
            result.lock().unwrap().take(),
            Some(true),
            "max_receive {max_receive}"
        );
        sink.close();
    }

    Ok(())
}

/// STATEMENT: A second connect packet or a client ping response closes the server connection with a protocol violation.
///
/// HOW:
/// - After a valid raw handshake, send either another `CONNECT` or `PINGRESP`.
/// - Assert the connection closes and the control service records the expected violation message for each packet.
#[ntex::test]
async fn test_unexpected_packet() -> std::io::Result<()> {
    let connect_pkt = || {
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        ))
    };
    for (pkt, message) in [
        // a second CONNECT is a protocol violation [MQTT-3.1.0-2]
        (
            connect_pkt(),
            "[MQTT-3.1.0-2] Second CONNECT packet is received",
        ),
        (
            Encoded::Packet(Packet::PingResponse),
            "Packet of the type is not expected from client",
        ),
    ] {
        let error = Arc::new(Mutex::new(None));
        let error2 = error.clone();
        let srv = server::test_server(async move |_| {
            let error = error2.clone();
            MqttServer::new(async |_| Ok::<_, TestError>(()))
                .control(async move |msg| {
                    if let Control::Stop(Reason::Protocol(err)) = msg
                        && let MqttProtocolError::ProtocolViolation(e) = err.get_ref()
                    {
                        *error.lock().unwrap() = Some(e.message());
                    }
                    Ok::<_, TestError>(None)
                })
                .build(connect)
        });

        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.send(connect_pkt(), &codec).await.unwrap();
        io.recv(&codec).await.unwrap().unwrap();

        // control service is called with the protocol error, connection is closed
        io.send(pkt, &codec).await.unwrap();
        let _ = io.send(Encoded::Packet(Packet::PingRequest), &codec).await;
        let result = io.recv(&codec).await;
        assert!(matches!(result, Ok(None) | Err(_)), "{result:?}");

        let err = error.lock().unwrap().take();
        assert!(err == Some(message), "{err:?}");
    }
    Ok(())
}

/// STATEMENT: Publish acknowledgements preserve arrival order without delaying subscription and ping responses.
///
/// HOW:
/// - Send two publishes around a subscription and ping, with the first publish handler completing last.
/// - Assert `SUBACK` and `PINGRESP` arrive first, followed by the two `PUBACK` packets in publish order.
#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
        MqttServer::new(async |p: Publish| {
            // the first publish completes last
            let delay = if p.id() == NonZeroU16::new(1) { 200 } else { 50 };
            sleep(Duration::from_millis(delay)).await;
            Ok::<_, ()>(())
        })
        .protocol(async move |msg| {
            if let ProtocolMessage::Ping(msg) = msg {
                Ok(msg.ack())
            } else if let ProtocolMessage::Subscribe(mut msg) = msg {
                for mut sub in &mut msg {
                    assert_eq!(sub.qos(), codec::QoS::AtLeastOnce);
                    sub.topic();
                    sub.subscribe(codec::QoS::AtLeastOnce);
                }
                Ok::<_, ()>(msg.ack())
            } else {
                Ok(msg.disconnect())
            }
        })
        .build(connect)
    });

    let (io, codec) = handshake(&srv).await;

    io.send(Encoded::Publish(pkt_publish(), None), &codec)
        .await
        .unwrap();
    io.send(
        Encoded::Packet(Packet::Subscribe {
            packet_id: pid(2),
            topic_filters: vec![(ByteString::from("topic1"), codec::QoS::AtLeastOnce)],
        }),
        &codec,
    )
    .await
    .unwrap();
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(pid(3)),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();

    // subscribe and ping responses do not wait for publish acks
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt,
        Decoded::Packet(
            Packet::SubscribeAck {
                packet_id: pid(2),
                status: vec![codec::SubscribeReturnCode::Success(codec::QoS::AtLeastOnce)],
            },
            3
        )
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, Decoded::Packet(Packet::PingResponse, 0));

    // publish acks keep the order of publish packets
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt,
        Decoded::Packet(Packet::PublishAck { packet_id: pid(1) }, 2)
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt,
        Decoded::Packet(Packet::PublishAck { packet_id: pid(3) }, 2)
    );

    Ok(())
}

/// STATEMENT: Concurrent client QoS 1 sends all complete through the acknowledgement queue.
///
/// HOW:
/// - Start a server whose publish handler waits briefly and submit three client sends together with `join_all`.
/// - Assert all three send results are successful.
#[ntex::test]
async fn test_ack_order_sink() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
        MqttServer::new(async |_| {
            sleep(Duration::from_millis(100)).await;
            Ok::<_, ()>(())
        })
        .build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;
    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let topic = ByteString::from_static("test");
    let fut1 = sink
        .publish(topic.clone())
        .send_at_least_once(Bytes::from_static(b"pkt1"));
    let fut2 = sink
        .publish(topic.clone())
        .send_at_least_once(Bytes::from_static(b"pkt2"));
    let fut3 = sink
        .publish(topic.clone())
        .send_at_least_once(Bytes::from_static(b"pkt3"));

    let res = join_all(vec![fut1, fut2, fut3]).await;
    assert!(res[0].is_ok());
    assert!(res[1].is_ok());
    assert!(res[2].is_ok());

    Ok(())
}

/// STATEMENT: A client QoS 1 send to a wildcard topic returns an error.
///
/// HOW:
/// - Connect to a server configured to force-close its sink when handling a publish.
/// - Attempt to publish to `#` and assert the send fails.
#[ntex::test]
async fn test_disconnect() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |con: &Session<St>| {
            let sink = con.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                sink.force_close();
                sleep(Duration::from_millis(100)).await;
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("#"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_err());

    Ok(())
}

/// STATEMENT: Closing the client sink delivers a disconnect message to the server protocol service.
///
/// HOW:
/// - Complete a client QoS 1 publish, then call `close` on its sink.
/// - Wait briefly and assert the server disconnect handler was invoked.
#[ntex::test]
async fn test_client_disconnect() -> std::io::Result<()> {
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::test_server(async move |_| {
        let disconnect = disconnect2.clone();

        MqttServer::new(async |_: &Session<St>| {
            Ok::<_, Infallible>(fn_service(async move |_: Publish| Ok::<_, ()>(())))
        })
        .protocol(async move |msg| {
            if let ProtocolMessage::Disconnect(msg) = msg {
                disconnect.store(true, Relaxed);
                Ok::<_, ()>(msg.ack())
            } else {
                Ok(msg.disconnect())
            }
        })
        .build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("test"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    sink.close();
    sleep(Millis(50)).await;
    assert!(disconnect.load(Relaxed));

    Ok(())
}

/// STATEMENT: Buffered publish and disconnect packets both reach their server handlers.
///
/// HOW:
/// - Send a publish and `DISCONNECT` in one flush while the publish handler remains pending.
/// - Drop the connection after a short delay and assert both handlers were invoked.
#[ntex::test]
async fn test_handle_incoming() -> std::io::Result<()> {
    let publish = Arc::new(AtomicBool::new(false));
    let publish2 = publish.clone();
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::test_server(async move |_| {
        let publish = publish2.clone();
        let disconnect = disconnect2.clone();
        MqttServer::new(async move |_| {
            publish.store(true, Relaxed);
            sleep(Duration::from_millis(100)).await;
            Ok::<_, TestError>(())
        })
        .protocol(async move |msg| {
            if let ProtocolMessage::Disconnect(msg) = msg {
                disconnect.store(true, Relaxed);
                Ok::<_, TestError>(msg.ack())
            } else {
                Ok(msg.disconnect())
            }
        })
        .build(connect)
    });

    let (io, codec) = connect_raw(&srv).await;
    io.encode(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(pid(3)),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .unwrap();
    io.encode(Encoded::Packet(Packet::Disconnect), &codec)
        .unwrap();
    io.flush(true).await.unwrap();
    sleep(Millis(50)).await;
    drop(io);
    sleep(Millis(50)).await;

    assert!(publish.load(Relaxed));
    assert!(disconnect.load(Relaxed));

    Ok(())
}

/// Binds server QoS settings into a runner that reports whether a buffered publish was handled.
fn make_handle_or_drop_test(
    max_qos: QoS,
    handle_qos_after_disconnect: Option<QoS>,
) -> impl Fn(QoS) -> Pin<Box<dyn Future<Output = bool>>> {
    move |publish_qos| {
        Box::pin(handle_or_drop_publish_after_disconnect(
            publish_qos,
            max_qos,
            handle_qos_after_disconnect,
        ))
    }
}

/// Sends a publish followed by disconnect, verifies disconnect handling, and reports publish-handler invocation.
async fn handle_or_drop_publish_after_disconnect(
    publish_qos: QoS,
    max_qos: QoS,
    handle_qos_after_disconnect: Option<QoS>,
) -> bool {
    let publish = Arc::new(AtomicBool::new(false));
    let publish2 = publish.clone();
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let publish = publish2.clone();
        let disconnect = disconnect2.clone();
        MqttServer::new(async move |_| {
            publish.store(true, Relaxed);
            sleep(Duration::from_millis(100)).await;
            Ok::<_, TestError>(())
        })
        .protocol(async move |msg| {
            if let ProtocolMessage::Disconnect(msg) = msg {
                disconnect.store(true, Relaxed);
                Ok::<_, TestError>(msg.ack())
            } else {
                Ok(msg.disconnect())
            }
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_qos(max_qos)
                .set_handle_qos_after_disconnect(handle_qos_after_disconnect),
        ),
    )
    .start();

    let packet_id = match publish_qos {
        QoS::AtMostOnce => None,
        _ => Some(pid(1)),
    };
    let (io, codec) = connect_raw(&srv).await;
    io.encode(
        Encoded::Publish(
            codec::Publish {
                qos: publish_qos,
                packet_id,
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .unwrap();
    io.encode(Encoded::Packet(Packet::Disconnect), &codec)
        .unwrap();
    io.flush(true).await.unwrap();
    sleep(Millis(250)).await;
    io.close();
    drop(io);
    sleep(Millis(250)).await;

    assert!(disconnect.load(Relaxed));

    publish.load(Relaxed)
}

/// STATEMENT: Configured QoS thresholds allow buffered publishes to be handled around disconnect.
///
/// HOW:
/// - Use the shared disconnect scenario with supported maximum QoS and post-disconnect handling thresholds.
/// - Assert every listed QoS combination invokes the publish handler; the helper also verifies disconnect handling.
#[ntex::test]
async fn test_handle_incoming_after_disconnect() -> std::io::Result<()> {
    let handle_publish = make_handle_or_drop_test(QoS::AtMostOnce, Some(QoS::AtMostOnce));
    assert!(handle_publish(QoS::AtMostOnce).await);

    let handle_publish = make_handle_or_drop_test(QoS::AtLeastOnce, Some(QoS::AtMostOnce));
    assert!(handle_publish(QoS::AtMostOnce).await);

    let handle_publish = make_handle_or_drop_test(QoS::AtLeastOnce, Some(QoS::AtLeastOnce));
    assert!(handle_publish(QoS::AtMostOnce).await);
    assert!(handle_publish(QoS::AtLeastOnce).await);

    let handle_publish = make_handle_or_drop_test(QoS::ExactlyOnce, Some(QoS::ExactlyOnce));
    assert!(handle_publish(QoS::AtMostOnce).await);
    assert!(handle_publish(QoS::AtLeastOnce).await);
    assert!(handle_publish(QoS::ExactlyOnce).await);

    Ok(())
}

/// STATEMENT: An error from the control service while reporting a protocol-service error closes the connection.
///
/// HOW:
/// - Make the protocol service fail on `DISCONNECT` and the control service fail on the resulting error notification.
/// - Send `DISCONNECT` after a raw handshake and assert the connection reaches end of stream.
#[ntex::test]
async fn test_nested_errors() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
        MqttServer::new(async |_| Ok::<_, TestError>(()))
            .control(async move |msg| {
                if let Control::Stop(Reason::Error(_)) = msg {
                    Err(())
                } else {
                    Ok(None)
                }
            })
            .protocol(async move |msg| {
                if let ProtocolMessage::Disconnect(_) = msg {
                    Err(())
                } else {
                    Ok(msg.disconnect())
                }
            })
            .build(connect)
    });

    let (io, codec) = handshake(&srv).await;

    // disconnect
    io.send(Encoded::Packet(Packet::Disconnect), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

/// STATEMENT: A raised packet-size limit permits a large streamed payload to be consumed and acknowledged.
///
/// HOW:
/// - Configure a 512 KiB limit and make the publish handler accept only a fully read 270 KiB payload.
/// - Send that payload and assert the send succeeds and a publish acknowledgement arrives.
#[ntex::test]
async fn test_large_publish() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move |_| {
        MqttServer::new(async |p: Publish| match p.read_all().await {
            Ok(pl) if pl.len() == 270 * 1024 => Ok(()),
            _ => Err(TestError),
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(512 * 1024)))
    .start();

    let (io, codec) = handshake(&srv).await;

    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            payload_size: 270 * 1024,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );
    let res = io.send(p, &codec).await;
    assert!(res.is_ok());
    let result = io.recv(&codec).await;
    assert!(
        matches!(
            result,
            Ok(Some(Decoded::Packet(Packet::PublishAck { .. }, _)))
        ),
        "{result:?}"
    );

    Ok(())
}

/// STATEMENT: The default packet-size limit rejects a packet whose payload alone reaches 256 KiB.
///
/// HOW:
/// - Complete a raw handshake against an otherwise default server.
/// - Send a publish with a 256 KiB payload and assert the connection closes or reports an I/O error.
#[ntex::test]
async fn test_default_max_size() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(connect)
    });

    let (io, codec) = connect_raw(&srv).await;
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(ack, Decoded::Packet(Packet::ConnectAck(_), _)));

    // the default 256 KB limit closes the connection
    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            payload_size: 256 * 1024,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 256 * 1024])),
    );
    let _ = io.send(p, &codec).await;
    let result = io.recv(&codec).await;
    assert!(matches!(result, Ok(None) | Err(_)), "{result:?}");

    Ok(())
}

/// Builds a TLS acceptor from the test key and certificate, panicking if either cannot be loaded.
fn ssl_acceptor() -> openssl::ssl::SslAcceptor {
    use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

    // load ssl keys
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file("./tests/key.pem", SslFiletype::PEM)
        .unwrap();
    builder
        .set_certificate_chain_file("./tests/cert.pem")
        .unwrap();
    builder.build()
}

/// STATEMENT: Dropping an incomplete large streaming payload closes the connection.
///
/// HOW:
/// - Configure a 512 KiB limit but use a publish handler that does not read its payload.
/// - Send a 270 KiB payload and assert the next receive observes closure or an I/O error.
#[ntex::test]
async fn test_large_publish_payload_dropped() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move |_| {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(512 * 1024)))
    .start();

    let (io, codec) = handshake(&srv).await;

    // the handler drops the streamed payload before it is received,
    // the connection is closed
    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            payload_size: 270 * 1024,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );
    let _ = io.send(p, &codec).await;
    let result = io.recv(&codec).await;
    assert!(matches!(result, Ok(None) | Err(_)), "{result:?}");

    Ok(())
}

/// STATEMENT: A large payload can be streamed and acknowledged over an OpenSSL TLS connection.
///
/// HOW:
/// - Start a TLS server with a 512 KiB packet limit and a handler that requires a fully read 270 KiB payload.
/// - Connect with certificate verification disabled, perform the MQTT handshake, and send the payload.
/// - Assert the send succeeds and a publish acknowledgement is received.
#[ntex::test]
async fn test_large_publish_openssl() -> std::io::Result<()> {
    use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};

    let srv = server::TestServerBuilder::new(async move |_| {
        server::openssl::SslAcceptor::new(ssl_acceptor())
            .map_err(|_| ())
            .and_then(
                MqttServer::new(async |p: Publish| match p.read_all().await {
                    Ok(pl) if pl.len() == 270 * 1024 => Ok(()),
                    _ => Err(TestError),
                })
                .build(connect)
                .map_err(|_| ()),
            )
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(512 * 1024)))
    .start();

    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let con = Pipeline::new(
        SharedCfg::default(),
        ntex::connect::openssl::SslConnector::new(builder.build()),
    );
    let addr = format!("127.0.0.1:{}", srv.addr().port());
    let io = con.call(addr.into()).await.unwrap();

    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await;

    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            payload_size: 270 * 1024,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );
    let res = io.send(p, &codec).await;
    assert!(res.is_ok());
    let result = io.recv(&codec).await;
    assert!(
        matches!(
            result,
            Ok(Some(Decoded::Packet(Packet::PublishAck { .. }, _)))
        ),
        "{result:?}"
    );

    Ok(())
}

/// STATEMENT: A publish above the configured maximum QoS closes the connection with a protocol violation.
///
/// HOW:
/// - Configure the server for QoS 0 and send a raw QoS 1 publish after handshake.
/// - Assert end of stream and a protocol-violation notification in the control service.
#[ntex::test]
async fn test_max_qos() -> std::io::Result<()> {
    let violated = Arc::new(AtomicBool::new(false));
    let violated2 = violated.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let violated = violated2.clone();
        MqttServer::new(async |_| Ok::<_, TestError>(()))
            .control(async move |msg| {
                if let Control::Stop(Reason::Protocol(err)) = msg
                    && let MqttProtocolError::ProtocolViolation(_) = err.get_ref()
                {
                    violated.store(true, Relaxed);
                }
                Ok::<_, ()>(None)
            })
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::AtMostOnce)))
    .start();

    let (io, codec) = handshake(&srv).await;

    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            ..pkt_publish()
        },
        None,
    );

    io.send(p, &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());
    assert!(violated.load(Relaxed));

    Ok(())
}

/// STATEMENT: A server sink becomes ready only after the handshake acknowledgement is applied.
///
/// HOW:
/// - Poll `ready` during the connect handler and assert it is pending and `is_ready` is false.
/// - Spawn a waiter that asserts readiness and publishes after the handshake, then check the peer receive succeeds.
#[ntex::test]
async fn test_sink_ready() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(async move |packet: Connect| {
            let sink = packet.sink();
            let mut ready = Box::pin(sink.ready());
            let res = lazy(|cx| Pin::new(&mut ready).poll(cx)).await;
            assert!(res.is_pending());
            assert!(!sink.is_ready());

            let sink = sink.clone();
            ntex::rt::spawn(async move {
                sink.ready().await;
                assert!(sink.is_ready());
                sink.publish("/test")
                    .send_at_most_once(Bytes::from_static(b"body"))
                    .await
                    .unwrap();
            });

            Ok::<_, ()>(packet.ack(St, false).idle_timeout(Seconds(16)))
        })
    });

    // connect to server
    let (io, codec) = handshake(&srv).await;

    let result = io.recv(&codec).await;
    assert!(result.is_ok());

    Ok(())
}

/// STATEMENT: Nonblocking QoS 1 sends report acknowledgements through the registered callback.
///
/// HOW:
/// - Register `publish_ack_cb`, enqueue two nonblocking sends, and await a third ordinary send.
/// - Assert all sends succeed and the callback records the first two packet identifiers without disconnection.
#[ntex::test]
async fn test_sink_publish_noblock() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let results = Rc::new(RefCell::new(Vec::new()));
    let results2 = results.clone();

    sink.publish_ack_cb(move |idx, disconnected| {
        assert!(!disconnected);
        results2.borrow_mut().push(idx);
    });

    let res = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once_no_block(Bytes::new());
    assert!(res.is_ok());

    let res = sink
        .publish(ByteString::from_static("test2"))
        .send_at_least_once_no_block(Bytes::new());
    assert!(res.is_ok());

    let res = sink
        .publish(ByteString::from_static("test3"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());

    assert_eq!(*results.borrow(), &[pid(1), pid(2)]);

    sink.close();
    Ok(())
}

/// STATEMENT: The maximum frame-read duration still expires when incremental payload reads satisfy the minimum rate.
///
/// HOW:
/// - Configure a one-second rate interval, a two-second maximum duration, and a ten-byte rate requirement.
/// - Send a large publish header and payload fragments, asserting no timeout after the first interval.
/// - Send another sufficient fragment and assert the control service reports `ReadTimeout` after the maximum duration.
#[ntex::test]
async fn test_frame_read_rate() -> std::io::Result<()> {
    let check = Arc::new(AtomicBool::new(false));
    let check2 = check.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let check = check2.clone();

        MqttServer::new(async move |p: v3::Publish| {
            let _ = p.read_all().await;
            Ok::<_, TestError>(())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(msg)) = msg
                && msg.get_ref() == &MqttProtocolError::ReadTimeout
            {
                check.store(true, Relaxed);
            }
            Ok::<_, TestError>(None)
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT")
            .add(IoConfig::new().set_frame_read_rate(Seconds(1), Seconds(2), 10))
            .add(
                MqttServiceConfig::new()
                    .set_min_chunk_size(32 * 1024)
                    .set_max_size(0),
            ),
    )
    .start();

    let (io, codec) = handshake(&srv).await;

    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            payload_size: 270 * 1024,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );

    let mut buf = BytePages::default();
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();

    io.encode_slice(&buf[..5]).unwrap();
    buf.advance_to(5);
    sleep(Millis(100)).await;
    // completes the publish header and sends enough payload for the first period
    io.encode_slice(&buf[..30]).unwrap();
    buf.advance_to(30);
    sleep(Millis(1500)).await;
    assert!(!check.load(Relaxed));

    // the read rate is satisfied, but the max timeout is reached
    io.encode_slice(&buf[..12]).unwrap();
    buf.advance_to(12);
    sleep(Millis(1000)).await;
    assert!(check.load(Relaxed));

    Ok(())
}

/// STATEMENT: Decoder-level connect rejections send a negative acknowledgement before closing the connection.
///
/// HOW:
/// - Send raw connects with an unsupported protocol level and an empty identifier without a clean session.
/// - Assert the corresponding `CONNACK` reason with no session present, then end of stream for each case.
#[ntex::test]
async fn test_handshake_rejected_by_decoder() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(fn_service(async |_| Ok::<_, TestError>(()))).build(connect)
    });

    let cases: [(&[u8], _); 2] = [
        // unsupported protocol level
        (
            b"\x10\x0c\x00\x04MQTT\x05\x02\x00\x3C\x00\x00",
            codec::ConnectAckReason::UnacceptableProtocolVersion,
        ),
        // empty client id without clean session
        (
            b"\x10\x0c\x00\x04MQTT\x04\x00\x00\x3C\x00\x00",
            codec::ConnectAckReason::IdentifierRejected,
        ),
    ];
    for (connect, return_code) in cases {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.encode_slice(connect).unwrap();

        let ack = io.recv(&codec).await.unwrap().unwrap();
        assert_eq!(
            ack,
            codec::Decoded::Packet(
                codec::Packet::ConnectAck(codec::ConnectAck {
                    return_code,
                    session_present: false
                }),
                2
            )
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }

    Ok(())
}

/// STATEMENT: A failed handshake acknowledgement carries the application-selected reason.
///
/// HOW:
/// - Make the connect handler return `Connect::failed` with `UnacceptableProtocolVersion`.
/// - Send a raw connect and assert the received `CONNACK` has that reason and no session present.
#[ntex::test]
async fn test_handshake_fail() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(fn_service(async |_| Ok::<_, TestError>(()))).build(
            async move |packet: Connect| {
                Ok::<_, ()>(
                    packet.failed::<St>(codec::ConnectAckReason::UnacceptableProtocolVersion),
                )
            },
        )
    });

    // connect to server
    let (io, codec) = connect_raw(&srv).await;
    let ack = io.recv(&codec).await.unwrap().unwrap();

    assert_eq!(
        ack,
        codec::Decoded::Packet(
            codec::Packet::ConnectAck(codec::ConnectAck {
                return_code: codec::ConnectAckReason::UnacceptableProtocolVersion,
                session_present: false
            }),
            2
        )
    );

    Ok(())
}

/// STATEMENT: Invalid last-will topics are rejected before the connect handler runs.
///
/// HOW:
/// - Send connects with an empty will topic or wildcard-containing topics and assert closure without an acknowledgement.
/// - Assert the connect handler was not called, then send a valid will topic and verify an accepted `CONNACK` and handler invocation.
#[ntex::test]
async fn test_handshake_invalid_will_topic() -> std::io::Result<()> {
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();
    let srv = server::test_server(async move |_| {
        let called = called2.clone();
        MqttServer::new(async |_: Publish| Ok::<_, ()>(())).build(async move |msg: Connect| {
            called.store(true, Relaxed);
            Ok::<_, ()>(msg.ack(St, false))
        })
    });
    let connect_pkt = |topic: &str| {
        let mut body = vec![
            0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x06, 0x00, 0x3c, 0x00, 0x02, b'i', b'd',
        ];
        body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&[0x00, 0x00]);
        let mut pkt = vec![0x10, body.len() as u8];
        pkt.extend(body);
        pkt
    };

    // the connection is closed without CONNACK, [MQTT-3.1.4-1]
    for topic in ["", "a/#", "+", "a/+/b"] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.encode_slice(&connect_pkt(topic)).unwrap();
        assert!(io.recv(&codec).await.unwrap().is_none());
    }
    assert!(!called.load(Relaxed));

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode_slice(&connect_pkt("a/b")).unwrap();
    assert_eq!(
        io.recv(&codec).await.unwrap().unwrap(),
        Decoded::Packet(
            Packet::ConnectAck(codec::ConnectAck {
                session_present: false,
                return_code: codec::ConnectAckReason::ConnectionAccepted,
            }),
            2
        )
    );
    assert!(called.load(Relaxed));

    Ok(())
}

/// STATEMENT: A streamed payload continues to be read when its publish fills the response queue.
///
/// HOW:
/// - Set `max_queue` to one and send a 64 KiB publish in parts, followed by a small publish.
/// - Have the handler consume the full large payload and assert both publish acknowledgements arrive in order within the timeout.
#[ntex::test]
async fn test_streaming_publish_full_queue() {
    /// Payload size in bytes used to fill the single-slot response queue.
    const SIZE: usize = 64 * 1024;

    let srv = server::TestServerBuilder::new(async move |_| {
        MqttServer::new(async |p: Publish| {
            if p.packet().payload_size == 1 {
                return Ok(());
            }
            match p.read_all().await {
                Ok(pl) if pl.len() == SIZE => Ok(()),
                _ => Err(TestError),
            }
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_queue(1)
                .set_min_chunk_size(0),
        ),
    )
    .start();

    let (io, codec) = handshake(&srv).await;

    let publish = |id, payload_size| codec::Publish {
        packet_id: NonZeroU16::new(id),
        payload_size,
        ..pkt_publish()
    };
    // the publish fills the queue, the rest of its payload arrives after
    // the publish is dispatched
    let mut buf = BytePages::default();
    let p = Encoded::Publish(publish(1, SIZE as u32), Some(Bytes::from(vec![b'*'; SIZE])));
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();
    io.encode_slice(&buf[..1024]).unwrap();
    buf.advance_to(1024);
    io.flush(true).await.unwrap();
    sleep(Millis(100)).await;
    io.encode_slice(&buf).unwrap();
    io.encode(
        Encoded::Publish(publish(2, 1), Some(Bytes::from_static(b"1"))),
        &codec,
    )
    .unwrap();

    for id in [1, 2] {
        let res = ntex::time::timeout(Seconds(5), io.recv(&codec)).await;
        let packet_id = pid(id);
        assert!(
            matches!(
                res,
                Ok(Ok(Some(Decoded::Packet(Packet::PublishAck { packet_id: pid }, _))))
                    if pid == packet_id
            ),
            "{res:?}"
        );
    }
}

/// STATEMENT: Payload chunks do not retain queue slots while their publish handler remains pending.
///
/// HOW:
/// - Set `max_queue` to two, stream a 16 KiB publish, and block its handler after reading the payload.
/// - Send a small QoS 0 publish and assert its handler runs before the first handler is released.
/// - Release the first handler and assert its publish acknowledgement arrives.
#[ntex::test]
async fn test_streaming_publish_chunk_slots() {
    // below the receive size limit, reading continues after the payload
    /// Payload size in bytes, kept below the receive-size limit to allow reading subsequent packets.
    const SIZE: usize = 16 * 1024;

    let gate = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicBool::new(false));
    let (gate2, received2) = (gate.clone(), received.clone());
    let srv = server::TestServerBuilder::new(async move |_| {
        let (gate, received) = (gate2.clone(), received2.clone());
        MqttServer::new(async move |p: Publish| {
            if p.packet().payload_size == 1 {
                received.store(true, Relaxed);
                return Ok(());
            }
            if p.read_all().await.map_err(|_| TestError)?.len() != SIZE {
                return Err(TestError);
            }
            while !gate.load(Relaxed) {
                sleep(Millis(10)).await;
            }
            Ok(())
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_queue(2)
                .set_min_chunk_size(0),
        ),
    )
    .start();

    let (io, codec) = handshake(&srv).await;

    let publish = |qos, id, payload_size| codec::Publish {
        qos,
        packet_id: NonZeroU16::new(id),
        payload_size,
        ..pkt_publish()
    };

    // the payload arrives in chunks after the publish is dispatched
    let mut buf = BytePages::default();
    let p = Encoded::Publish(
        publish(codec::QoS::AtLeastOnce, 1, SIZE as u32),
        Some(Bytes::from(vec![b'*'; SIZE])),
    );
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();
    io.encode_slice(&buf[..1024]).unwrap();
    buf.advance_to(1024);
    io.flush(true).await.unwrap();
    sleep(Millis(100)).await;
    io.encode_slice(&buf).unwrap();
    io.encode(
        Encoded::Publish(
            publish(codec::QoS::AtMostOnce, 0, 1),
            Some(Bytes::from_static(b"1")),
        ),
        &codec,
    )
    .unwrap();
    io.flush(true).await.unwrap();

    for _ in 0..100 {
        if received.load(Relaxed) {
            break;
        }
        sleep(Millis(20)).await;
    }
    assert!(received.load(Relaxed));

    gate.store(true, Relaxed);
    let res = ntex::time::timeout(Seconds(5), io.recv(&codec)).await;
    assert!(
        matches!(
            res,
            Ok(Ok(Some(Decoded::Packet(Packet::PublishAck { packet_id }, _))))
                if packet_id.get() == 1
        ),
        "{res:?}"
    );
}

/// Starts a raw MQTT server that consumes the first packet before handing the connection and codec to `f`.
fn raw_server<F, Fut>(f: F) -> server::TestServer
where
    F: Fn(ntex::io::Io, codec::Codec) -> Fut + Send + Clone + 'static,
    Fut: Future<Output = ()> + 'static,
{
    server::test_server(async move |_| {
        let f = f.clone();
        fn_service(move |io: ntex::io::Io| {
            let f = f.clone();
            async move {
                let codec = codec::Codec::default();
                let _ = io.recv(&codec).await;
                f(io, codec).await;
                Ok::<_, ()>(())
            }
        })
    })
}

/// STATEMENT: The server router uses matching resources or its default service and closes on resource failure.
///
/// HOW:
/// - Publish to a matching parameterized route and two unmatched topics, checking each acknowledgement and the routing log.
/// - Publish to a failing resource and assert the connection closes or reports an I/O error.
#[ntex::test]
async fn test_server_router() -> std::io::Result<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();

    let srv = server::test_server(async move |_| {
        let (def, res) = (log2.clone(), log2.clone());
        let router = v3::Router::new(fn_service(move |p: Publish| {
            let log = def.clone();
            async move {
                log.lock()
                    .unwrap()
                    .push(format!("default:{}", p.publish_topic()));
                Ok::<_, TestError>(())
            }
        }))
        .resource(
            "topic/{id}",
            fn_service(move |p: Publish| {
                let log = res.clone();
                async move {
                    log.lock()
                        .unwrap()
                        .push(format!("topic:{}", p.topic().get("id").unwrap()));
                    Ok::<_, TestError>(())
                }
            }),
        )
        .resource(
            "other",
            fn_service(move |_: Publish| async move { Err::<(), _>(TestError) }),
        );
        assert!(format!("{router:?}").contains("v3::Router"));
        MqttServer::new(router).build(connect)
    });

    let (io, codec) = handshake(&srv).await;

    // a matching resource handles the publish, the rest goes to the default service
    for (id, topic) in [(1, "topic/one"), (2, "topic/a/b"), (3, "unknown")] {
        io.send(qos1_publish(topic, id), &codec).await.unwrap();
        assert_eq!(
            recv_pkt(&io, &codec).await,
            Decoded::Packet(Packet::PublishAck { packet_id: pid(id) }, 2)
        );
    }
    assert_eq!(
        *log.lock().unwrap(),
        ["topic:one", "default:topic/a/b", "default:unknown"]
    );

    // a failing resource closes the connection
    io.send(qos1_publish("other", 4), &codec).await.unwrap();
    let res = io.recv(&codec).await;
    assert!(matches!(res, Ok(None) | Err(_)), "{res:?}");

    Ok(())
}

/// STATEMENT: Connect builder values reach the server and acknowledgement options configure the connection.
///
/// HOW:
/// - Set client credentials, keep-alive, clean session, will, and packet overrides; mutate the username in the server connect handler.
/// - Assert the received packet fields, debug metadata, and the client session-present flag.
/// - Apply a 64-byte packet limit and verify a small publish succeeds while a larger one fails as disconnected.
#[ntex::test]
async fn test_connect_builder_and_ack() -> std::io::Result<()> {
    let received = Arc::new(Mutex::new(None));
    let received2 = received.clone();

    let srv = server::test_server(async move |_| {
        let received = received2.clone();
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(())).build(
            async move |mut msg: Connect| {
                assert_eq!(msg.st(), &());
                assert!(msg.packet_size() > 0);
                assert!(format!("{msg:?}").contains("packet-id"));
                // the packet can be modified in place
                msg.packet_mut().username = Some(ByteString::from_static("checked"));
                *received.lock().unwrap() = Some(msg.packet().clone());

                let ack = msg
                    .ack(St, true)
                    .max_send(Some(0))
                    .max_send(Some(8))
                    .max_packet_size(NonZeroU32::new(64).unwrap())
                    .idle_timeout(Seconds(16));
                assert!(format!("{ack:?}").contains("max_packet_size: Some(64)"));
                Ok::<_, ()>(ack)
            },
        )
    });

    let will = codec::LastWill {
        qos: codec::QoS::AtLeastOnce,
        retain: true,
        topic: ByteString::from_static("will/topic"),
        message: Bytes::from_static(b"bye"),
    };
    let client = try_connect_client(
        client::Connect::with(srv.addr(), codec::Connect::default())
            .client_id("replaced")
            .clean_session()
            .keep_alive(Seconds(30))
            .last_will(will.clone())
            .username("user-name")
            .password(Bytes::from_static(b"secret"))
            .packet(|pkt| pkt.client_id = ByteString::from_static("packet-id")),
    )
    .await
    .unwrap();

    assert!(client.session_present());
    assert!(format!("{client:?}").contains("v3::Client"));

    assert_eq!(
        received.lock().unwrap().take().unwrap(),
        codec::Connect {
            clean_session: true,
            keep_alive: 30,
            last_will: Some(will),
            client_id: ByteString::from_static("packet-id"),
            username: Some(ByteString::from_static("checked")),
            password: Some(Bytes::from_static(b"secret")),
        }
    );

    // `max_packet_size` is applied to the connection
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());
    sink.publish("t")
        .send_at_least_once(Bytes::from_static(b"small"))
        .await
        .unwrap();
    let res = sink
        .publish("t")
        .send_at_least_once(Bytes::from(vec![b'*'; 128]))
        .await;
    assert_eq!(res, Err(v3::error::SendPacketError::Disconnected));

    Ok(())
}

/// STATEMENT: The client protocol service receives unrouted publishes at each QoS and their QoS 2 release.
///
/// HOW:
/// - Trigger four server publishes and read their payloads through chunked, collected, and `into_inner` acknowledgement paths.
/// - Check mutable packet metadata, then assert the ordered topics, QoS values, payloads, and release notification.
/// - Assert the client sink remains open after acknowledging the messages.
#[ntex::test]
async fn test_client_publish_message() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |ses: &Session<St>| {
            let sink = ses.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                sink.publish("c/0a")
                    .send_at_most_once(Bytes::from_static(b"zero-a"))
                    .await
                    .unwrap();
                sink.publish("c/0b")
                    .send_at_most_once(Bytes::from_static(b"zero-b"))
                    .await
                    .unwrap();
                sink.publish("c/1")
                    .send_at_least_once(Bytes::from_static(b"one"))
                    .await
                    .unwrap();
                sink.publish("c/2")
                    .send_exactly_once(Bytes::from_static(b"two"))
                    .await
                    .unwrap()
                    .release()
                    .await
                    .unwrap();
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    });

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let log = Rc::new(RefCell::new(Vec::new()));
    let log2 = log.clone();
    ntex::rt::spawn(
        client.start(fn_service(move |msg: client::ProtocolMessage| {
            let log = log2.clone();
            async move {
                match msg {
                    client::ProtocolMessage::Publish(mut p) => {
                        assert!(p.packet_size() > p.payload_size() as u32);
                        // the packet can be modified in place
                        p.packet_mut().retain = true;
                        let topic = p.packet().topic.clone();
                        let qos = p.packet().qos;
                        let size = p.payload_size();
                        Ok::<_, TestError>(match topic.as_ref() {
                            "c/0a" => {
                                // payload is read chunk by chunk
                                let mut payload = Vec::new();
                                while let Some(chunk) = p.read().await.unwrap() {
                                    payload.extend_from_slice(&chunk);
                                }
                                assert_eq!(payload.len(), size);
                                log.borrow_mut().push((topic, qos, Bytes::from(payload)));
                                client::ProtocolMessage::Publish(p).ack()
                            }
                            "c/1" => {
                                let payload = p.read_all().await.unwrap();
                                log.borrow_mut().push((topic, qos, payload));
                                p.ack()
                            }
                            _ => {
                                let payload = p.read_all().await.unwrap();
                                let (ack, pkt) = p.into_inner();
                                assert!(pkt.retain);
                                log.borrow_mut().push((topic, qos, payload));
                                ack
                            }
                        })
                    }
                    msg => {
                        log.borrow_mut().push((
                            ByteString::from_static("pubrel"),
                            QoS::AtMostOnce,
                            Bytes::new(),
                        ));
                        Ok(msg.ack())
                    }
                }
            }
        })),
    );

    sink.publish("trigger")
        .send_at_most_once(Bytes::new())
        .await
        .unwrap();

    for _ in 0..100 {
        if log.borrow().len() == 5 {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert_eq!(
        *log.borrow(),
        vec![
            (
                ByteString::from_static("c/0a"),
                QoS::AtMostOnce,
                Bytes::from_static(b"zero-a")
            ),
            (
                ByteString::from_static("c/0b"),
                QoS::AtMostOnce,
                Bytes::from_static(b"zero-b")
            ),
            (
                ByteString::from_static("c/1"),
                QoS::AtLeastOnce,
                Bytes::from_static(b"one")
            ),
            (
                ByteString::from_static("c/2"),
                QoS::ExactlyOnce,
                Bytes::from_static(b"two")
            ),
            (
                ByteString::from_static("pubrel"),
                QoS::AtMostOnce,
                Bytes::new()
            ),
        ]
    );
    assert!(sink.is_open());
    Ok(())
}

/// STATEMENT: Replacing default middleware removes the configured in-flight publish limit.
///
/// HOW:
/// - Replace middleware with `Identity`, append another `Identity`, and configure `max_receive` to one.
/// - Send two publishes whose handlers overlap and assert both acknowledgements and a peak concurrency of two.
#[ntex::test]
async fn test_server_middleware() -> std::io::Result<()> {
    // in-flight limiting is removed, both publishes are handled concurrently
    let max = Arc::new(Mutex::new((0usize, 0usize)));
    let max2 = max.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let max = max2.clone();
        let srv = MqttServer::new(fn_service(move |_: Publish| {
            let max = max.clone();
            async move {
                {
                    let mut g = max.lock().unwrap();
                    g.0 += 1;
                    g.1 = g.1.max(g.0);
                }
                sleep(Millis(50)).await;
                max.lock().unwrap().0 -= 1;
                Ok::<_, TestError>(())
            }
        }))
        .replace_middlewares(ntex::service::Identity)
        .middleware(ntex::service::Identity);
        assert_eq!(format!("{srv:?}"), "v3::MqttServer");
        srv.build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_receive(1)))
    .start();

    let (io, codec) = handshake(&srv).await;
    io.send(qos1_publish("t", 1), &codec).await.unwrap();
    io.send(qos1_publish("t", 2), &codec).await.unwrap();
    for id in 1..=2 {
        assert_eq!(
            recv_pkt(&io, &codec).await,
            Decoded::Packet(Packet::PublishAck { packet_id: pid(id) }, 2)
        );
    }
    assert_eq!(max.lock().unwrap().1, 2);
    Ok(())
}

/// STATEMENT: Non-connect first packets and malformed handshakes close only the offending server connection.
///
/// HOW:
/// - Send a ping, disconnect, publish, and invalid bytes as the first packet on separate connections and assert end of stream.
/// - Drop a connection during handshake, then complete a fresh handshake and verify a ping response.
#[ntex::test]
async fn test_first_packet_is_not_connect() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(())).build(connect)
    });

    for first in [
        Encoded::Packet(Packet::PingRequest),
        Encoded::Packet(Packet::Disconnect),
        Encoded::Publish(
            codec::Publish {
                payload_size: 1,
                ..pkt_publish()
            },
            Some(Bytes::from_static(b"\x00")),
        ),
    ] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.send(first, &codec).await.unwrap();
        assert_eq!(io.recv(&codec).await.unwrap(), None);
    }

    // CONNECT rejected by the decoder, no CONNACK is sent for a generic decode error
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode_slice(b"\x00\x00").unwrap();
    assert_eq!(io.recv(&codec).await.unwrap(), None);

    // peer disconnects during handshake, the server keeps accepting connections
    drop(srv.connect().await.unwrap());
    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PingResponse, 0)
    );
    Ok(())
}

/// STATEMENT: The client connector distinguishes handshake closure and unexpected packets and supports a custom transport connector.
///
/// HOW:
/// - Use raw servers that close before `CONNACK` or send a ping response or publish instead, checking disconnection and protocol errors.
/// - Connect through a custom connector, check debug output and the absent session flag, then use `into_inner` to exchange a raw ping.
#[ntex::test]
async fn test_connector_errors() -> std::io::Result<()> {
    // peer closes the connection without CONNACK
    let srv = raw_server(async |io: ntex::io::Io, _| {
        let _ = io.shutdown().await;
    });
    let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&*err, client::MqttClientError::Disconnected(None)),
        "{err:?}"
    );

    // server sends a packet other than CONNACK
    for first in [
        Encoded::Packet(Packet::PingResponse),
        Encoded::Publish(
            codec::Publish {
                payload_size: 1,
                ..pkt_publish()
            },
            Some(Bytes::from_static(b"\x00")),
        ),
    ] {
        let srv = raw_server(move |io: ntex::io::Io, codec: codec::Codec| {
            let first = first.clone();
            async move {
                io.send(first, &codec).await.unwrap();
                sleep(Millis(300)).await;
            }
        });
        let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
            .await
            .err()
            .unwrap();
        assert!(
            matches!(&*err, client::MqttClientError::Protocol(_)),
            "{err:?}"
        );
    }

    // custom connector
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(())).build(connect)
    });
    let connector = client::MqttConnector::new().connector(ntex::connect::Connector::default());
    assert!(format!("{connector:?}").contains("v3::MqttConnector"));
    let client = Pipeline::new(SharedCfg::default(), connector)
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    assert!(!client.session_present());

    // negotiated io can be used directly
    let (io, codec) = client.into_inner();
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert_eq!(
        io.recv(&codec).await.unwrap(),
        Some(Decoded::Packet(Packet::PingResponse, 0))
    );
    Ok(())
}

/// Starts a server that sends each configured topic and QoS sequentially for every incoming publish.
fn publisher_server(topics: &'static [(&'static str, QoS)]) -> server::TestServer {
    server::test_server(async move |_| {
        MqttServer::new(async move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                for (topic, qos) in topics {
                    let pub_ = sink.publish(*topic);
                    let payload = Bytes::from_static(b"data");
                    match qos {
                        QoS::AtMostOnce => pub_.send_at_most_once(payload).await?,
                        QoS::AtLeastOnce => pub_.send_at_least_once(payload).await?,
                        QoS::ExactlyOnce => {
                            pub_.send_exactly_once(payload).await?.release().await?
                        }
                    }
                }
                Ok::<_, TestError>(())
            }))
        })
        .build(connect)
    })
}

/// STATEMENT: The client router dispatches matching resources, falls back to the protocol service, and closes on resource failure.
///
/// HOW:
/// - Trigger publishes for two resources, an unmatched topic, and a failing resource.
/// - Assert the first three topics produce the expected ordered routing log and the final failure closes the sink.
#[ntex::test]
async fn test_client_router() -> std::io::Result<()> {
    let srv = publisher_server(&[
        ("topic/one", QoS::AtLeastOnce),
        ("other", QoS::AtMostOnce),
        ("unrouted", QoS::AtLeastOnce),
        ("fail", QoS::AtMostOnce),
    ]);

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let log = Rc::new(RefCell::new(Vec::new()));
    let (l1, l2, l3) = (log.clone(), log.clone(), log.clone());

    let router = client
        .resource(
            "topic/{id}",
            fn_service(move |p: Publish| {
                let log = l1.clone();
                async move {
                    log.borrow_mut()
                        .push(format!("res:{}", p.topic().get("id").unwrap()));
                    Ok::<_, TestError>(())
                }
            }),
        )
        .resource(
            "other",
            fn_service(move |p: Publish| {
                let log = l2.clone();
                async move {
                    log.borrow_mut().push(format!("res:{}", p.publish_topic()));
                    Ok::<_, TestError>(())
                }
            }),
        )
        .resource(
            "fail",
            fn_service(async |_: Publish| Err::<(), _>(TestError)),
        );
    assert!(format!("{router:?}").contains("v3::ClientRouter"));

    ntex::rt::spawn(async move {
        let _ = router
            .start(fn_service(move |msg: client::ProtocolMessage| {
                let log = l3.clone();
                async move {
                    if let client::ProtocolMessage::Publish(p) = &msg {
                        log.borrow_mut().push(format!("proto:{}", p.packet().topic));
                    }
                    Ok::<_, TestError>(msg.ack())
                }
            }))
            .await;
    });

    sink.publish("trigger")
        .send_at_most_once(Bytes::new())
        .await
        .unwrap();
    for _ in 0..100 {
        if log.borrow().len() == 3 {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert_eq!(*log.borrow(), ["res:one", "res:other", "proto:unrouted"]);

    // a failing resource closes the connection
    for _ in 0..100 {
        if !sink.is_open() {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert!(!sink.is_open());
    Ok(())
}

/// STATEMENT: The default client router handles a routed QoS 2 publish but closes on an unrouted publish.
///
/// HOW:
/// - Trigger a QoS 2 publish to a parameterized resource followed by an unmatched QoS 0 publish.
/// - Assert only the matched topic is recorded and the client sink closes.
#[ntex::test]
async fn test_client_router_default() -> std::io::Result<()> {
    let srv = publisher_server(&[("topic/x", QoS::ExactlyOnce), ("unrouted", QoS::AtMostOnce)]);

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let log = Rc::new(RefCell::new(Vec::new()));
    let log2 = log.clone();
    ntex::rt::spawn(
        client
            .resource(
                "topic/{id}",
                fn_service(move |p: Publish| {
                    let log = log2.clone();
                    async move {
                        log.borrow_mut().push(p.publish_topic().to_owned());
                        Ok::<_, TestError>(())
                    }
                }),
            )
            .start_default(),
    );

    sink.publish("trigger")
        .send_at_most_once(Bytes::new())
        .await
        .unwrap();

    // the unrouted publish closes the connection
    for _ in 0..100 {
        if !sink.is_open() {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert!(!sink.is_open());
    assert_eq!(*log.borrow(), ["topic/x"]);
    Ok(())
}

/// STATEMENT: The client control service receives a peer-gone stop notification when the server closes.
///
/// HOW:
/// - Have the server force-close its sink shortly after accepting the connection.
/// - Run `start_with_control` and assert it returns successfully while recording a `PeerGone` stop reason.
#[ntex::test]
async fn test_client_start_with_control() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(())).build(async |msg: Connect| {
            let sink = msg.sink();
            ntex::rt::spawn(async move {
                sleep(Millis(100)).await;
                sink.force_close();
            });
            Ok::<_, ()>(msg.ack(St, false))
        })
    });

    let client = connect_client(srv.addr()).await;
    let stopped = Rc::new(RefCell::new(None));
    let stopped2 = stopped.clone();

    let res = client
        .start_with_control(
            fn_service(async |msg: client::ProtocolMessage| Ok::<_, TestError>(msg.ack())),
            fn_service(move |msg: Control<TestError>| {
                let stopped = stopped2.clone();
                async move {
                    if let Control::Stop(reason) = &msg {
                        *stopped.borrow_mut() = Some(format!("{reason:?}"));
                    }
                    Ok::<_, TestError>(None)
                }
            }),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    assert!(
        stopped.borrow().as_deref().unwrap().starts_with("PeerGone"),
        "{:?}",
        stopped.borrow()
    );
    Ok(())
}

/// STATEMENT: The server protocol service can explicitly acknowledge subscriptions, unsubscriptions, and QoS 2 releases.
///
/// HOW:
/// - Reject one subscription filter and confirm another at QoS 2, then assert the returned status codes.
/// - Unsubscribe both filters and complete a QoS 2 publish and release.
/// - Assert the logged subscription and unsubscription sizes, filter count, and release identifier.
#[ntex::test]
async fn test_proto_subscribe_unsubscribe() -> std::io::Result<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let log = log2.clone();
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(()))
            .protocol(fn_service(move |msg: ProtocolMessage| {
                let log = log.clone();
                async move {
                    Ok::<_, TestError>(match msg {
                        ProtocolMessage::Subscribe(mut msg) => {
                            log.lock()
                                .unwrap()
                                .push(format!("sub:{}", msg.packet_size()));
                            let iter = msg.iter_mut();
                            assert!(format!("{iter:?}").contains("SubscribeIter"));
                            for mut sub in iter {
                                if sub.topic().as_ref() == "bad" {
                                    sub.fail();
                                } else {
                                    sub.confirm(sub.qos());
                                }
                            }
                            msg.ack()
                        }
                        ProtocolMessage::Unsubscribe(msg) => {
                            log.lock().unwrap().push(format!(
                                "unsub:{}:{}",
                                msg.packet_size(),
                                msg.iter().count()
                            ));
                            msg.ack()
                        }
                        ProtocolMessage::PublishRelease(msg) => {
                            log.lock().unwrap().push(format!("pubrel:{}", msg.id()));
                            msg.ack()
                        }
                        msg => {
                            log.lock().unwrap().push(format!("{msg:?}"));
                            msg.ack()
                        }
                    })
                }
            }))
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let codes = sink
        .subscribe()
        .topic_filter(ByteString::from_static("bad"), QoS::AtLeastOnce)
        .topic_filter(ByteString::from_static("good"), QoS::ExactlyOnce)
        .send()
        .await
        .unwrap();
    assert_eq!(
        codes,
        vec![
            codec::SubscribeReturnCode::Failure,
            codec::SubscribeReturnCode::Success(QoS::ExactlyOnce)
        ]
    );

    sink.unsubscribe()
        .topic_filter(ByteString::from_static("bad"))
        .topic_filter(ByteString::from_static("good"))
        .send()
        .await
        .unwrap();

    sink.publish("t")
        .send_exactly_once(Bytes::from_static(b"d"))
        .await
        .unwrap()
        .release()
        .await
        .unwrap();

    assert_eq!(*log.lock().unwrap(), ["sub:15", "unsub:13:2", "pubrel:3"]);
    sink.close();
    Ok(())
}

/// STATEMENT: Generic protocol acknowledgement does not support subscribe or unsubscribe messages.
///
/// HOW:
/// - Use a server protocol service that always calls `ProtocolMessage::ack`.
/// - Send subscribe and unsubscribe requests on separate client connections and assert each fails as disconnected with a closed sink.
#[ntex::test]
async fn test_proto_ack_not_supported() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(()))
            .protocol(async |msg: ProtocolMessage| Ok::<_, TestError>(msg.ack()))
            .build(connect)
    });

    for subscribe in [true, false] {
        let client = connect_client(srv.addr()).await;
        let sink = client.sink();
        ntex::rt::spawn(client.start_default());

        let res = if subscribe {
            sink.subscribe()
                .topic_filter(ByteString::from_static("t"), QoS::AtMostOnce)
                .send()
                .await
                .map(|_| ())
        } else {
            sink.unsubscribe()
                .topic_filter(ByteString::from_static("t"))
                .send()
                .await
        };
        assert_eq!(res, Err(v3::error::SendPacketError::Disconnected));
        assert!(!sink.is_open());
    }
    Ok(())
}

/// STATEMENT: Client and router run variants support keep-alive pings and sink closure.
///
/// HOW:
/// - Start four clients with one-second keep-alive intervals using direct and routed run methods, including custom control.
/// - Wait for at least four aggregate pings and assert all sinks remain open.
/// - Close all sinks and assert they are closed after a further keep-alive interval.
#[ntex::test]
async fn test_client_keepalive_variants() -> std::io::Result<()> {
    let pings = Arc::new(Mutex::new(0));
    let pings2 = pings.clone();

    let srv = server::test_server(async move |_| {
        let pings = pings2.clone();
        MqttServer::new(async |_: Publish| Ok::<_, TestError>(()))
            .protocol(move |msg: ProtocolMessage| {
                let pings = pings.clone();
                async move {
                    if let ProtocolMessage::Ping(msg) = msg {
                        *pings.lock().unwrap() += 1;
                        Ok::<_, TestError>(msg.ack())
                    } else {
                        Ok(msg.disconnect())
                    }
                }
            })
            .build(connect)
    });

    let proto = || fn_service(async |msg: client::ProtocolMessage| Ok::<_, TestError>(msg.ack()));
    let publish = || fn_service(async |_: Publish| Ok::<_, TestError>(()));
    let mut sinks = Vec::new();
    for idx in 0..4 {
        let client = try_connect_client(
            client::Connect::new(srv.addr())
                .client_id("user")
                .keep_alive(Seconds(1)),
        )
        .await
        .unwrap();
        sinks.push(client.sink());

        match idx {
            0 => ntex::rt::spawn(async move {
                let _ = client.start(proto()).await;
            }),
            1 => ntex::rt::spawn(async move {
                let _ = client
                    .start_with_control(proto(), fn_service(async |_| Ok::<_, TestError>(None)))
                    .await;
            }),
            2 => ntex::rt::spawn(client.resource("t", publish()).start_default()),
            _ => ntex::rt::spawn(async move {
                let _ = client.resource("t", publish()).start(proto()).await;
            }),
        };
    }

    for _ in 0..200 {
        if *pings.lock().unwrap() >= 4 {
            break;
        }
        sleep(Millis(20)).await;
    }
    assert!(*pings.lock().unwrap() >= 4, "{:?}", pings.lock().unwrap());
    assert!(sinks.iter().all(|s| s.is_open()));

    // the keep-alive task stops once the connection is closed
    sinks.iter().for_each(|s| s.close());
    sleep(Millis(1200)).await;
    assert!(sinks.iter().all(|s| !s.is_open()));
    Ok(())
}

/// STATEMENT: A client closes when its keep-alive ping is not acknowledged in time.
///
/// HOW:
/// - Use a raw server that accepts the handshake but sends no further packets.
/// - Start a client with a one-second keep-alive interval and assert its sink closes within the polling window.
#[ntex::test]
async fn test_client_keepalive_no_pingresp() -> std::io::Result<()> {
    let srv = raw_server(async |io: ntex::io::Io, codec: codec::Codec| {
        io.send(connect_ack(), &codec).await.unwrap();
        sleep(Millis(5000)).await;
    });

    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .keep_alive(Seconds(1)),
    )
    .await
    .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    for _ in 0..300 {
        if !sink.is_open() {
            break;
        }
        sleep(Millis(20)).await;
    }
    assert!(!sink.is_open());
    Ok(())
}

/// Builds an accepted `CONNACK` with no existing session.
fn connect_ack() -> Encoded {
    Encoded::Packet(Packet::ConnectAck(codec::ConnectAck {
        session_present: false,
        return_code: codec::ConnectAckReason::ConnectionAccepted,
    }))
}

/// Encodes packets into one byte buffer, panicking if any packet cannot be encoded.
fn encode_pkts(pkts: Vec<Encoded>) -> Bytes {
    let codec = codec::Codec::default();
    let mut buf = BytePages::default();
    for pkt in pkts {
        codec.encode(pkt, &mut buf).unwrap();
    }
    buf.freeze()
}

/// Runs a client against a raw server sending `data` after `CONNACK` and returns the formatted stop reason.
/// The protocol service waits for `delay` on each message so later packets may be dispatched concurrently.
async fn client_stop_reason(data: Bytes, delay: Millis) -> String {
    let srv = raw_server(move |io: ntex::io::Io, codec: codec::Codec| {
        let data = data.clone();
        async move {
            io.send(connect_ack(), &codec).await.unwrap();
            let _ = io.encode_slice(&data);
            sleep(Millis(300)).await;
        }
    });

    let client = connect_client(srv.addr()).await;
    let reason = Rc::new(RefCell::new(String::new()));
    let reason2 = reason.clone();
    let _ = client
        .start_with_control(
            fn_service(async move |msg: client::ProtocolMessage| {
                sleep(delay).await;
                Ok::<_, TestError>(msg.ack())
            }),
            fn_service(move |msg: Control<TestError>| {
                let reason = reason2.clone();
                async move {
                    if let Control::Stop(r) = &msg {
                        *reason.borrow_mut() = format!("{r:?}");
                    }
                    Ok::<_, TestError>(None)
                }
            }),
        )
        .await;
    reason.take()
}

/// STATEMENT: The client reports invalid incoming packets while accepting permitted redelivery and unknown releases.
///
/// HOW:
/// - Send invalid publish topics, server-forbidden packets, unmatched acknowledgements, an early release, and a reused packet identifier.
/// - Assert each dispatcher stop reason contains the expected protocol error.
/// - Send a duplicate QoS 1 publish and an unknown `PUBREL`, then assert the client stops only when the peer is gone.
#[ntex::test]
async fn test_client_protocol_errors() -> std::io::Result<()> {
    let publish = |topic, id, dup, qos| {
        Encoded::Publish(
            codec::Publish {
                dup,
                qos,
                topic: ByteString::from_static(topic),
                packet_id: NonZeroU16::new(id),
                ..pkt_publish()
            },
            None,
        )
    };

    let cases: Vec<(Bytes, Millis, &str)> = vec![
        // wildcards are not allowed in a publish topic
        (
            Bytes::from_static(b"\x30\x05\x00\x03a/#"),
            Millis::ZERO,
            "Pub_3_3_2_2",
        ),
        // packets of these types are never sent by a server
        (
            encode_pkts(vec![Encoded::Packet(Packet::PingRequest)]),
            Millis::ZERO,
            "UnexpectedPacket { packet_type: 192",
        ),
        (
            encode_pkts(vec![Encoded::Packet(Packet::Unsubscribe {
                packet_id: pid(1),
                topic_filters: vec![ByteString::from_static("t")],
            })]),
            Millis::ZERO,
            "UnexpectedPacket { packet_type: 162",
        ),
        // acks for packets that were never sent
        (
            encode_pkts(vec![Encoded::Packet(Packet::PublishAck {
                packet_id: pid(5),
            })]),
            Millis::ZERO,
            "ack with a packet id that is not in flight",
        ),
        (
            encode_pkts(vec![Encoded::Packet(Packet::PublishComplete {
                packet_id: pid(5),
            })]),
            Millis::ZERO,
            "ack with a packet id that is not in flight",
        ),
        (
            encode_pkts(vec![Encoded::Packet(Packet::PublishReceived {
                packet_id: pid(5),
            })]),
            Millis::ZERO,
            "ack with a packet id that is not in flight",
        ),
        (
            encode_pkts(vec![Encoded::Packet(Packet::SubscribeAck {
                packet_id: pid(5),
                status: vec![codec::SubscribeReturnCode::Success(QoS::AtMostOnce)],
            })]),
            Millis::ZERO,
            "ack with a packet id that is not in flight",
        ),
        (
            encode_pkts(vec![Encoded::Packet(Packet::UnsubscribeAck {
                packet_id: pid(5),
            })]),
            Millis::ZERO,
            "ack with a packet id that is not in flight",
        ),
        // PUBREL before PUBREC
        (
            encode_pkts(vec![
                publish("a", 1, false, QoS::AtLeastOnce),
                Encoded::Packet(Packet::PublishRelease { packet_id: pid(1) }),
            ]),
            Millis(50),
            "PublishRelease packet before PublishReceived",
        ),
        // duplicated packet id
        (
            encode_pkts(vec![
                publish("a", 1, false, QoS::ExactlyOnce),
                publish("a", 1, false, QoS::AtLeastOnce),
            ]),
            Millis::ZERO,
            "PacketId_2_2_1_3_Pub",
        ),
    ];

    for (data, delay, expect) in cases {
        let reason = client_stop_reason(data, delay).await;
        assert!(
            reason.contains(expect),
            "{reason:?} does not match {expect}"
        );
    }

    // re-delivered publishes and an unknown PUBREL are not errors,
    // the client stops when the server is gone
    let reason = client_stop_reason(
        encode_pkts(vec![
            publish("a", 1, false, QoS::AtLeastOnce),
            publish("a", 1, true, QoS::AtLeastOnce),
            Encoded::Packet(Packet::PublishRelease { packet_id: pid(9) }),
        ]),
        Millis(50),
    )
    .await;
    assert!(reason.starts_with("PeerGone"), "{reason:?}");
    Ok(())
}

/// STATEMENT: Generic server protocol acknowledgements answer pings and releases, while disconnect responses close the connection.
///
/// HOW:
/// - Use `ProtocolMessage::ack` to verify `PINGRESP`, the QoS 2 acknowledgement exchange, and closure on `DISCONNECT`.
/// - Use `ProtocolMessage::disconnect` on a second connection and assert a ping closes it without a response.
#[ntex::test]
async fn test_proto_generic_ack_disconnect() -> std::io::Result<()> {
    let proto_server = |ack: bool| {
        server::TestServerBuilder::new(async move |_| {
            MqttServer::new(async |_: Publish| Ok::<_, TestError>(()))
                .protocol(move |msg: ProtocolMessage| async move {
                    Ok::<_, TestError>(if ack { msg.ack() } else { msg.disconnect() })
                })
                .build(connect)
        })
        .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
        .start()
    };

    // generic ack: PINGREQ -> PINGRESP, PUBREL -> PUBCOMP, DISCONNECT -> close
    let srv = proto_server(true);
    let (io, codec) = handshake(&srv).await;

    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PingResponse, 0)
    );

    io.send(
        Encoded::Publish(
            codec::Publish {
                qos: QoS::ExactlyOnce,
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PublishReceived { packet_id: pid(1) }, 2)
    );
    io.send(
        Encoded::Packet(Packet::PublishRelease { packet_id: pid(1) }),
        &codec,
    )
    .await
    .unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PublishComplete { packet_id: pid(1) }, 2)
    );

    io.send(Encoded::Packet(Packet::Disconnect), &codec)
        .await
        .unwrap();
    assert!(
        ntex::time::timeout(Millis(1000), io.recv(&codec))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );

    // generic disconnect: the connection is closed without a response
    let srv = proto_server(false);
    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert!(
        ntex::time::timeout(Millis(1000), io.recv(&codec))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    Ok(())
}

/// Builds raw `PUBLISH` bytes for test topics and remaining lengths below 128 bytes.
fn publish_bytes(topic: &str, id: u16, dup: bool, qos: QoS, payload: &[u8]) -> Bytes {
    let mut pkt = vec![0x30 | ((qos as u8) << 1) | ((dup as u8) << 3)];
    let mut rest = vec![0u8, topic.len() as u8];
    rest.extend_from_slice(topic.as_bytes());
    if qos != QoS::AtMostOnce {
        rest.extend_from_slice(&id.to_be_bytes());
    }
    rest.extend_from_slice(payload);
    pkt.push(rest.len() as u8);
    pkt.extend_from_slice(&rest);
    Bytes::from(pkt)
}

/// STATEMENT: Client payload handling determines whether a streamed publish is acknowledged, closed, or disconnected.
///
/// HOW:
/// - Send a ten-byte QoS 1 payload from a raw server in three parts.
/// - Run client handlers that read the payload, skip it, or fail before consuming it.
/// - Assert the respective `PUBACK`, connection closure, or `DISCONNECT`, successful dispatcher completion, and the collected bytes in the read case.
#[ntex::test]
async fn test_client_payload_stream() -> std::io::Result<()> {
    // "read": payload is consumed and acked, "skip": payload is dropped,
    // "err": protocol service fails while the payload is still incomplete
    for (mode, expect) in [("read", "ack"), ("skip", "closed"), ("err", "disconnect")] {
        let log = Arc::new(Mutex::new(String::new()));
        let log2 = log.clone();
        let srv = raw_server(move |io: ntex::io::Io, codec: codec::Codec| {
            let log = log2.clone();
            async move {
                io.send(connect_ack(), &codec).await.unwrap();
                // the payload is delivered in three parts
                let data = publish_bytes("t", 1, false, QoS::AtLeastOnce, b"0123456789");
                for part in [
                    &data[..data.len() - 6],
                    &data[data.len() - 6..data.len() - 3],
                    &data[data.len() - 3..],
                ] {
                    let _ = io.encode_slice(part);
                    sleep(Millis(30)).await;
                }

                let res = match ntex::time::timeout(Millis(1000), io.recv(&codec)).await {
                    Ok(Ok(Some(Decoded::Packet(Packet::PublishAck { .. }, _)))) => "ack",
                    Ok(Ok(Some(Decoded::Packet(Packet::Disconnect, _)))) => "disconnect",
                    // the client closes the connection, possibly with a reset
                    Ok(Ok(None)) | Ok(Err(_)) => "closed",
                    res => Box::leak(format!("{res:?}").into_boxed_str()),
                };
                *log.lock().unwrap() = res.to_string();
            }
        });

        let payload = Rc::new(RefCell::new(Bytes::new()));
        let payload2 = payload.clone();
        let client = connect_client(srv.addr()).await;
        let res = client
            .start(fn_service(move |msg: client::ProtocolMessage| {
                let payload = payload2.clone();
                async move {
                    let client::ProtocolMessage::Publish(p) = msg else {
                        return Ok(msg.ack());
                    };
                    match mode {
                        "read" => {
                            *payload.borrow_mut() = p.read_all().await.unwrap();
                            Ok(p.ack())
                        }
                        "skip" => Ok(p.ack()),
                        _ => Err(TestError),
                    }
                }
            }))
            .await;

        assert!(res.is_ok(), "{mode}: {res:?}");
        for _ in 0..100 {
            if !log.lock().unwrap().is_empty() {
                break;
            }
            sleep(Millis(10)).await;
        }
        assert_eq!(*log.lock().unwrap(), expect, "{mode}");
        if mode == "read" {
            assert_eq!(payload.borrow().as_ref(), b"0123456789");
        }
    }
    Ok(())
}

/// STATEMENT: The default server protocol service answers pings but closes on unsupported subscriptions.
///
/// HOW:
/// - Send a ping and assert its response, then publish and verify its acknowledgement and mutable retain flag.
/// - Send a subscription and assert the connection closes within the timeout.
#[ntex::test]
async fn test_default_proto_service() -> std::io::Result<()> {
    let retained = Arc::new(Mutex::new(false));
    let retained2 = retained.clone();
    let srv = server::test_server(async move |_| {
        let retained = retained2.clone();
        MqttServer::new(move |mut p: Publish| {
            let retained = retained.clone();
            async move {
                // the packet can be modified in place
                p.packet_mut().retain = true;
                *retained.lock().unwrap() = p.packet().retain;
                Ok::<_, TestError>(())
            }
        })
        .build(connect)
    });

    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PingResponse, 0)
    );

    io.send(qos1_publish("t", 1), &codec).await.unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PublishAck { packet_id: pid(1) }, 2)
    );
    assert!(*retained.lock().unwrap());

    // subscribe is not supported by the default service
    io.send(
        Encoded::Packet(Packet::Subscribe {
            packet_id: pid(2),
            topic_filters: vec![(ByteString::from_static("t"), QoS::AtMostOnce)],
        }),
        &codec,
    )
    .await
    .unwrap();
    assert!(
        ntex::time::timeout(Millis(1000), io.recv(&codec))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    Ok(())
}

/// STATEMENT: The client discards duplicate streamed payloads while preserving QoS-specific acknowledgement behavior.
///
/// HOW:
/// - Send a publish and then a duplicate with a payload split across writes.
/// - Delay QoS 1 handling to overlap the duplicate, and test QoS 2 after its first receipt.
/// - Assert successful dispatcher completion, one `PUBACK` for QoS 1, and two `PUBREC` replies for QoS 2.
#[ntex::test]
async fn test_client_redelivered_payload() -> std::io::Result<()> {
    // `QoS 1` is re-delivered while the first delivery is still handled,
    // `QoS 2` is re-delivered after it has been acked with PUBREC
    for (qos, delay, expect) in [
        (QoS::AtLeastOnce, Millis(300), vec!["PublishAck"]),
        (
            QoS::ExactlyOnce,
            Millis::ZERO,
            vec!["PublishReceived", "PublishReceived"],
        ),
    ] {
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        let srv = raw_server(move |io: ntex::io::Io, codec: codec::Codec| {
            let log = log2.clone();
            async move {
                io.send(connect_ack(), &codec).await.unwrap();
                let _ = io.encode_slice(&publish_bytes("t", 1, false, qos, b"0123456789"));
                sleep(Millis(100)).await;

                // re-delivery of the same packet id, the payload is incomplete
                let data = publish_bytes("t", 1, true, qos, b"0123456789");
                let _ = io.encode_slice(&data[..data.len() - 6]);
                sleep(Millis(50)).await;
                let _ = io.encode_slice(&data[data.len() - 6..]);

                while let Ok(Ok(Some(Decoded::Packet(pkt, _)))) =
                    ntex::time::timeout(Millis(500), io.recv(&codec)).await
                {
                    log.lock()
                        .unwrap()
                        .push(format!("{pkt:?}").split(' ').next().unwrap().to_string());
                }
            }
        });

        let client = connect_client(srv.addr()).await;
        let res = client
            .start(fn_service(move |msg: client::ProtocolMessage| async move {
                sleep(delay).await;
                Ok::<_, TestError>(msg.ack())
            }))
            .await;
        assert!(res.is_ok(), "{qos:?}: {res:?}");
        assert_eq!(*log.lock().unwrap(), expect, "{qos:?}");
    }
    Ok(())
}
