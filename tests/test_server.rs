use std::convert::Infallible;
use std::sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering::Relaxed};
use std::{cell::RefCell, future::Future, num::NonZeroU16, pin::Pin, rc::Rc, time::Duration};

use ntex::service::{Pipeline, Service, cfg::SharedCfg, fn_service};
use ntex::time::{Millis, Seconds, sleep};
use ntex::util::{BytePages, ByteString, Bytes, join_all, lazy};
use ntex::{codec::Encoder, io::IoConfig, server};

use ntex_mqtt::v3::codec::{self, Decoded, Encoded, Packet};
use ntex_mqtt::v3::{
    self, Connect, ConnectAck, MqttServer, ProtocolMessage, Publish, Session, client,
};
use ntex_mqtt::{Control, MqttServiceConfig, QoS, Reason, error::MqttProtocolError};

struct St;

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

async fn connect(msg: Connect) -> Result<ConnectAck<St>, ()> {
    msg.packet();
    msg.io();
    msg.sink();
    Ok(msg.ack(St, false).idle_timeout(Seconds(16)))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv =
        server::test_server(async || MqttServer::new(async |_| Ok::<_, ()>(())).build(connect));

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

#[ntex::test]
async fn test_simple_streaming() -> std::io::Result<()> {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move || {
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
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

#[ntex::test]
async fn test_simple_streaming2() {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let chunks = chunks2.clone();
        MqttServer::new(async move |mut p: Publish| {
            let chunks = chunks.clone();
            let pl = p.take_payload();
            assert!(format!("{:?}", pl).contains("StreamingPayload"));
            assert!(!p.dup());
            assert!(!p.retain());
            assert_eq!(p.id(), Some(NonZeroU16::new(1).unwrap()));
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
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

#[ntex::test]
async fn test_disconnect_while_streaming() -> std::io::Result<()> {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move || {
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
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

/// Payload chunk waiting for write backpressure fails when the peer is gone
#[ntex::test]
async fn test_streaming_waiter_peer_gone() -> std::io::Result<()> {
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = Arc::new(Mutex::new(None));
    let (sent2, result2) = (sent.clone(), result.clone());
    let srv = server::TestServerBuilder::new(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    // trigger server streaming PUBLISH, the payload is not read
    io.send(
        Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtMostOnce,
                topic: ByteString::from("test"),
                packet_id: None,
                payload_size: 0,
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

/// Publish handlers wait for acks of their own publishes, the acks are read
/// while the publishes fill the response queue, `max_queue` is 64 by default.
/// A packet beyond the limit is held back and pauses reading.
#[ntex::test]
async fn test_max_queue_acks() -> std::io::Result<()> {
    const COUNT: u16 = 64;

    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |_: Publish| {
                sink.publish("echo")
                    .send_at_least_once(Bytes::new())
                    .await
                    .map_err(|_| TestError)
            }))
        })
        .build(connect)
    })
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    for id in 1..=COUNT {
        let pkt = codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: NonZeroU16::new(id),
            payload_size: 0,
        };
        io.encode(Encoded::Publish(pkt, None), &codec).unwrap();
    }
    io.flush(true).await.unwrap();

    let acks = ntex::time::timeout(Seconds(10), async {
        let mut acks = 0;
        while acks < COUNT {
            match io.recv(&codec).await.unwrap().unwrap() {
                Decoded::Publish(pkt, ..) => {
                    let packet_id = pkt.packet_id.unwrap();
                    io.send(Encoded::Packet(Packet::PublishAck { packet_id }), &codec)
                        .await
                        .unwrap();
                }
                Decoded::Packet(Packet::PublishAck { .. }, _) => acks += 1,
                pkt => panic!("unexpected packet {pkt:?}"),
            }
        }
        acks
    })
    .await;
    assert_eq!(acks, Ok(COUNT));
    Ok(())
}

#[ntex::test]
async fn test_connect_fail() -> std::io::Result<()> {
    // bad user name or password
    let srv = server::test_server(async || {
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.bad_username_or_pwd::<St>()))
    });
    let err = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
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
    let srv = server::test_server(async || {
        MqttServer::new(async |_| Ok::<_, TestError>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.identifier_rejected::<St>()))
    });
    let err = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
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
    let srv = server::test_server(async || {
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.not_authorized::<St>()))
    });
    let err = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
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
    let srv = server::test_server(async || {
        MqttServer::new(async |_| Ok::<_, ()>(()))
            .build(async |conn: Connect| Ok::<_, ()>(conn.service_unavailable::<St>()))
    });
    let err = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
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

#[ntex::test]
async fn test_qos2() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    let id = NonZeroU16::new(1).unwrap();
    io.send(
        Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::ExactlyOnce,
                topic: ByteString::from("test"),
                packet_id: Some(id),
                payload_size: 0,
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

#[ntex::test]
async fn test_qos2_client() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move || {
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
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

#[ntex::test]
async fn test_qos2_default_protocol() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async || {
        MqttServer::new(async |_| Ok::<_, ()>(())).build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
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

fn decoded_packet(res: Decoded) -> Option<Packet> {
    if let Decoded::Packet(pkt, _) = res {
        Some(pkt)
    } else {
        None
    }
}

#[ntex::test]
async fn test_unexpected_ack_type() -> std::io::Result<()> {
    // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP (MQTT 3.1.1, 4.3.2, 4.3.3)
    let id = NonZeroU16::new(1).unwrap();
    for ack in [
        Packet::PublishReceived { packet_id: id },
        Packet::PublishComplete { packet_id: id },
    ] {
        let error = Arc::new(Mutex::new(None));
        let result = Arc::new(Mutex::new(None));
        let (error2, result2) = (error.clone(), result.clone());
        let srv = server::test_server(async move || {
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

        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.send(
            Encoded::Packet(Packet::Connect(
                codec::Connect::default().client_id("user").into(),
            )),
            &codec,
        )
        .await
        .unwrap();
        io.recv(&codec).await.unwrap().unwrap();

        // trigger server QoS 1 PUBLISH
        io.send(
            Encoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: codec::QoS::AtMostOnce,
                    topic: ByteString::from("test"),
                    packet_id: None,
                    payload_size: 0,
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

#[ntex::test]
async fn test_qos2_release_multiple() -> std::io::Result<()> {
    let result = Arc::new(Mutex::new(None));
    let result2 = result.clone();
    let srv = server::test_server(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    // trigger server QoS 2 PUBLISH packets
    io.send(
        Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtMostOnce,
                topic: ByteString::from("test"),
                packet_id: None,
                payload_size: 0,
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
        let packet_id = NonZeroU16::new(id).unwrap();
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
        let packet_id = NonZeroU16::new(id).unwrap();
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

/// Server publishes QoS 2 message to the client on any client publish
fn qos2_publisher() -> (server::TestServer, Arc<Mutex<Option<bool>>>) {
    let released = Arc::new(Mutex::new(None));
    let released2 = released.clone();

    let srv = server::test_server(async move || {
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

async fn wait_released(released: &Mutex<Option<bool>>) -> Option<bool> {
    for _ in 0..200 {
        if let Some(res) = *released.lock().unwrap() {
            return Some(res);
        }
        sleep(Millis(10)).await;
    }
    None
}

#[ntex::test]
async fn test_qos2_server_to_client() -> std::io::Result<()> {
    // publish is handled by client router
    let (srv, released) = qos2_publisher();
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
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
        .unwrap();
    assert_eq!(wait_released(&released).await, Some(true));
    assert_eq!(
        *received.borrow(),
        vec![(QoS::ExactlyOnce, Bytes::from_static(b"data"))]
    );
    assert!(sink.is_open());

    // publish is handled by client protocol service
    let (srv, released) = qos2_publisher();
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
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
        .unwrap();
    assert_eq!(wait_released(&released).await, Some(true));
    assert_eq!(
        *received.borrow(),
        vec![(QoS::ExactlyOnce, Bytes::from_static(b"data"))]
    );
    assert!(sink.is_open());

    Ok(())
}

#[ntex::test]
async fn test_qos2_redelivery_to_client() -> std::io::Result<()> {
    // raw server re-sends QoS 2 PUBLISH with DUP flag before PUBREL
    let completed = Arc::new(Mutex::new(None));
    let completed2 = completed.clone();
    let srv = server::test_server(async move || {
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

            let packet_id = NonZeroU16::new(1).unwrap();
            let mut responses = Vec::new();
            for dup in [false, true, true] {
                let pkt = codec::Publish {
                    dup,
                    retain: false,
                    qos: QoS::ExactlyOnce,
                    topic: ByteString::from_static("test/qos2"),
                    packet_id: Some(packet_id),
                    payload_size: 4,
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

    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
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
    let packet_id = NonZeroU16::new(1).unwrap();
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

#[ntex::test]
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    io.send(Encoded::Packet(codec::Packet::PingRequest), &codec)
        .await
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, Decoded::Packet(Packet::PingResponse, 0));
    assert!(ping.load(Relaxed));

    Ok(())
}

fn qos1_publish(topic: &'static str, id: u16) -> Encoded {
    Encoded::Publish(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from_static(topic),
            packet_id: NonZeroU16::new(id),
            payload_size: 0,
        },
        None,
    )
}

async fn recv_pkt(io: &ntex::io::Io, codec: &codec::Codec) -> Decoded {
    ntex::time::timeout(Millis(2000), io.recv(codec))
        .await
        .expect("packet is not received")
        .unwrap()
        .unwrap()
}

#[ntex::test]
async fn test_max_receive_publish_only() -> std::io::Result<()> {
    // publish handler waits for the ack of its own publish, the limit
    // of in-flight publishes must not block acks and pings
    let srv = server::TestServerBuilder::new(async || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
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
    let id = NonZeroU16::new(1).unwrap();
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
    let id = NonZeroU16::new(2).unwrap();
    assert_eq!(
        recv_pkt(&io, &codec).await,
        Decoded::Packet(Packet::PublishAck { packet_id: id }, 2)
    );

    Ok(())
}

#[ntex::test]
async fn test_max_receive_disconnect_order() -> std::io::Result<()> {
    // publishes that wait for a slot are passed to the handler before
    // DISCONNECT is processed, as without the limit
    let handled = Arc::new(Mutex::new(Vec::new()));
    let handled2 = handled.clone();
    let srv = server::TestServerBuilder::new(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
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

#[ntex::test]
async fn test_client_max_receive_publish_only() -> std::io::Result<()> {
    // client publish handler reads the streamed payload and waits
    // for the ack of its own publish
    for max_receive in [1, 0] {
        let result = Arc::new(Mutex::new(None));
        let result2 = result.clone();
        let srv = server::test_server(async move || {
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
        let srv = server::test_server(async move || {
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

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                payload_size: 0,
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    io.send(
        Encoded::Packet(Packet::Subscribe {
            packet_id: NonZeroU16::new(2).unwrap(),
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
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(3).unwrap()),
                payload_size: 0,
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
                packet_id: NonZeroU16::new(2).unwrap(),
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
        Decoded::Packet(
            Packet::PublishAck {
                packet_id: NonZeroU16::new(1).unwrap()
            },
            2
        )
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt,
        Decoded::Packet(
            Packet::PublishAck {
                packet_id: NonZeroU16::new(3).unwrap()
            },
            2
        )
    );

    Ok(())
}

#[ntex::test]
async fn test_ack_order_sink() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async |_| {
            sleep(Duration::from_millis(100)).await;
            Ok::<_, ()>(())
        })
        .build(connect)
    });

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
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

#[ntex::test]
async fn test_disconnect() -> std::io::Result<()> {
    let srv = server::test_server(async || {
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
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("#"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_err());

    Ok(())
}

#[ntex::test]
async fn test_client_disconnect() -> std::io::Result<()> {
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::test_server(async move || {
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
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

#[ntex::test]
async fn test_handle_incoming() -> std::io::Result<()> {
    let publish = Arc::new(AtomicBool::new(false));
    let publish2 = publish.clone();
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::test_server(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    io.encode(
        Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(3).unwrap()),
                payload_size: 0,
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

async fn handle_or_drop_publish_after_disconnect(
    publish_qos: QoS,
    max_qos: QoS,
    handle_qos_after_disconnect: Option<QoS>,
) -> bool {
    let publish = Arc::new(AtomicBool::new(false));
    let publish2 = publish.clone();
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::TestServerBuilder::new(async move || {
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
        _ => Some(NonZeroU16::new(1).unwrap()),
    };
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    io.encode(
        Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: publish_qos,
                topic: ByteString::from("test"),
                packet_id,
                payload_size: 0,
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

#[ntex::test]
async fn test_nested_errors() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect
    io.send(Encoded::Packet(Packet::Disconnect), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

#[ntex::test]
async fn test_large_publish() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async |p: Publish| match p.read_all().await {
            Ok(pl) if pl.len() == 270 * 1024 => Ok(()),
            _ => Err(TestError),
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(512 * 1024)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await;

    let p = Encoded::Publish(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload_size: 270 * 1024,
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

#[ntex::test]
async fn test_default_max_size() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(ack, Decoded::Packet(Packet::ConnectAck(_), _)));

    // the default 256 KB limit closes the connection
    let p = Encoded::Publish(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload_size: 256 * 1024,
        },
        Some(Bytes::from(vec![b'*'; 256 * 1024])),
    );
    let _ = io.send(p, &codec).await;
    let result = io.recv(&codec).await;
    assert!(matches!(result, Ok(None) | Err(_)), "{result:?}");

    Ok(())
}

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

#[ntex::test]
async fn test_large_publish_payload_dropped() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(512 * 1024)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await;

    // the handler drops the streamed payload before it is received,
    // the connection is closed
    let p = Encoded::Publish(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload_size: 270 * 1024,
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );
    let _ = io.send(p, &codec).await;
    let result = io.recv(&codec).await;
    assert!(matches!(result, Ok(None) | Err(_)), "{result:?}");

    Ok(())
}

#[ntex::test]
async fn test_large_publish_openssl() -> std::io::Result<()> {
    use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};

    let srv = server::TestServerBuilder::new(async move || {
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
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload_size: 270 * 1024,
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

#[ntex::test]
async fn test_max_qos() -> std::io::Result<()> {
    let violated = Arc::new(AtomicBool::new(false));
    let violated2 = violated.clone();

    let srv = server::TestServerBuilder::new(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    let p = Encoded::Publish(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload_size: 0,
        },
        None,
    );

    io.send(p, &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());
    assert!(violated.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_sink_ready() -> std::io::Result<()> {
    let srv = server::test_server(async || {
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
                    .unwrap();
            });

            Ok::<_, ()>(packet.ack(St, false).idle_timeout(Seconds(16)))
        })
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    let result = io.recv(&codec).await;
    assert!(result.is_ok());

    Ok(())
}

#[ntex::test]
async fn test_sink_publish_noblock() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async |_| Ok::<_, TestError>(())).build(connect)
    });

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

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

    assert_eq!(
        *results.borrow(),
        &[NonZeroU16::new(1).unwrap(), NonZeroU16::new(2).unwrap()]
    );

    sink.close();
    Ok(())
}

// Slow frame rate
#[ntex::test]
async fn test_frame_read_rate() -> std::io::Result<()> {
    let check = Arc::new(AtomicBool::new(false));
    let check2 = check.clone();

    let srv = server::TestServerBuilder::new(async move || {
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    io.recv(&codec).await.unwrap();

    let p = Encoded::Publish(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload_size: 270 * 1024,
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

#[ntex::test]
async fn test_handshake_rejected_by_decoder() -> std::io::Result<()> {
    let srv = server::test_server(async || {
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

#[ntex::test]
async fn test_handshake_fail() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(fn_service(async |_| Ok::<_, TestError>(()))).build(
            async move |packet: Connect| {
                Ok::<_, ()>(
                    packet.failed::<St>(codec::ConnectAckReason::UnacceptableProtocolVersion),
                )
            },
        )
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(
            codec::Connect::default().client_id("user").into(),
        )),
        &codec,
    )
    .await
    .unwrap();
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

#[ntex::test]
async fn test_handshake_invalid_will_topic() -> std::io::Result<()> {
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();
    let srv = server::test_server(async move || {
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

/// A held streaming publish gets its payload once it is dispatched, the
/// payload is read while the publish fills the response queue
#[ntex::test]
async fn test_held_streaming_publish() {
    const SIZE: usize = 64 * 1024;

    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async |p: Publish| {
            if p.packet().payload_size == 1 {
                sleep(Millis(200)).await;
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

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await;

    let publish = |id, payload_size| codec::Publish {
        dup: false,
        retain: false,
        qos: codec::QoS::AtLeastOnce,
        topic: ByteString::from("test"),
        packet_id: NonZeroU16::new(id),
        payload_size,
    };
    io.encode(
        Encoded::Publish(publish(1, 1), Some(Bytes::from_static(b"1"))),
        &codec,
    )
    .unwrap();

    // the second publish is held while the first one is handled, the rest
    // of its payload arrives after the publish is dispatched
    let mut buf = BytePages::default();
    let p = Encoded::Publish(publish(2, SIZE as u32), Some(Bytes::from(vec![b'*'; SIZE])));
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();
    io.encode_slice(&buf[..1024]).unwrap();
    buf.advance_to(1024);
    io.flush(true).await.unwrap();
    sleep(Millis(400)).await;
    io.encode_slice(&buf).unwrap();

    for id in [1, 2] {
        let res = ntex::time::timeout(Seconds(5), io.recv(&codec)).await;
        let packet_id = NonZeroU16::new(id).unwrap();
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
