use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, rc::Rc};
use std::{convert::Infallible, future::Future, num::NonZeroU16, pin::Pin, time::Duration};

use ntex::service::pipeline::Pipeline;
use ntex::service::{cfg::SharedCfg, fn_service};
use ntex::time::{Millis, Seconds, sleep};
use ntex::util::{BytePages, ByteString, Bytes, lazy};
use ntex::{codec::Encoder, io::Framed, io::IoConfig, rt, server};

use ntex_mqtt::v5::codec::{self, Decoded, Encoded, Packet};
use ntex_mqtt::v5::{
    Connect, ConnectAck, MqttServer, ProtocolMessage, Publish, PublishAck, QoS, Session, client,
    error,
};
use ntex_mqtt::{Control, MqttServiceConfig, Reason};

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
        packet_id: Some(NonZeroU16::new(1).unwrap()),
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

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
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
            Ok::<_, TestError>(p.ack())
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
            assert!(!p.dup());
            assert!(p.retain());
            assert_eq!(p.id(), Some(NonZeroU16::new(1).unwrap()));
            assert_eq!(p.qos(), QoS::AtLeastOnce);
            assert_eq!(p.topic().path(), "test");
            assert_eq!(p.topic_mut().path(), "test");
            assert_eq!(p.publish_topic(), "test");
            assert_eq!(p.packet_size(), 19);
            assert_eq!(p.payload_size(), 10);
            let chunk = p.read_all().await.unwrap();
            chunks.lock().unwrap().push(chunk);
            Ok::<_, TestError>(p.ack())
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
        .retain(true)
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
async fn test_connect_failed() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |hnd: Connect| {
                Ok::<_, ()>(hnd.failed::<St>(codec::ConnectAckReason::NotAuthorized))
            },
        )
    });

    // connect to server
    let err = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap_err();
    match &*err {
        error::MqttClientError::Ack(pkt) => {
            assert_eq!(pkt.reason_code, codec::ConnectAckReason::NotAuthorized);
        }
        _ => panic!("error"),
    }

    Ok(())
}

#[ntex::test]
async fn test_disconnect() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |con: &Session<St>| {
            let sink = con.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                sink.close();
                sleep(Duration::from_millis(100)).await;
                Ok::<_, TestError>(p.ack())
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
async fn test_disconnect_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |con: &Session<St>| {
            let sink = con.sink().clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let pkt = codec::Disconnect {
                    reason_code: codec::DisconnectReasonCode::ServerMoved,
                    ..Default::default()
                };
                sink.close_with_reason(pkt);
                sleep(Duration::from_millis(100)).await;
                Ok::<_, TestError>(p.ack())
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
async fn test_nested_errors_handling() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| {
                if let ProtocolMessage::Disconnect(_) = msg {
                    Err(TestError)
                } else {
                    Ok(msg.ack())
                }
            })
            .control(async move |msg| match msg {
                Control::Stop(Reason::Error(_)) => Err(TestError),
                _ => panic!("{:?}", msg),
            })
            .build(connect)
    });

    // connect to server
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
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

#[ntex::test]
async fn test_disconnect_on_error() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| {
                if let ProtocolMessage::Disconnect(_) = msg {
                    Err(TestError)
                } else {
                    Ok(msg.ack())
                }
            })
            .control(async move |msg| match msg {
                Control::Stop(Reason::Error(_)) => Ok(Some(
                    codec::Packet::from(codec::Disconnect {
                        reason_code: codec::DisconnectReasonCode::ImplementationSpecificError,
                        ..Default::default()
                    })
                    .into(),
                )),
                Control::Stop(Reason::PeerGone(_)) => Ok::<_, TestError>(None),
                _ => panic!("{:?}", msg),
            })
            .build(connect)
    });

    // connect to server
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
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();
    let res = io.recv(&codec).await.unwrap();
    assert!(res.is_none());

    Ok(())
}

#[ntex::test]
async fn test_disconnect_after_control_error() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::Subscribe(_) => Err(TestError),
                _ => Ok(msg.disconnect()),
            })
            .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Encoded::Packet(
            codec::Subscribe {
                id: None,
                packet_id: NonZeroU16::new(2).unwrap(),
                user_properties: Default::default(),
                topic_filters: vec![(
                    ByteString::from("topic1"),
                    codec::SubscriptionOptions {
                        qos: codec::QoS::AtLeastOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: codec::RetainHandling::AtSubscribe,
                    },
                )],
            }
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();

    let result = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(packet(result), Packet::Disconnect(_)));
    Ok(())
}

#[ntex::test]
async fn test_qos2() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let release = release2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::PublishRelease(msg) => {
                    release.store(true, Relaxed);
                    Ok::<_, TestError>(msg.ack())
                }
                _ => Ok(msg.disconnect()),
            })
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    let id = NonZeroU16::new(1).unwrap();
    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                qos: QoS::ExactlyOnce,
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
        Decoded::Packet(
            Packet::PublishReceived(codec::PublishAck {
                packet_id: id,
                reason_code: codec::PublishAckReason::Success,
                properties: Default::default(),
                reason_string: None,
            }),
            4
        )
    );

    io.send(
        Encoded::Packet(Packet::PublishRelease(codec::PublishAck2 {
            packet_id: id,
            reason_code: codec::PublishAck2Reason::Success,
            properties: Default::default(),
            reason_string: None,
        })),
        &codec,
    )
    .await
    .unwrap();
    let result = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        result,
        Decoded::Packet(
            Packet::PublishComplete(codec::PublishAck2 {
                packet_id: id,
                reason_code: codec::PublishAck2Reason::Success,
                properties: Default::default(),
                reason_string: None,
            }),
            4
        )
    );

    assert!(release.load(Relaxed));
    Ok(())
}

#[ntex::test]
async fn test_unexpected_packet() -> std::io::Result<()> {
    let connect_pkt = || Encoded::Packet(codec::Connect::default().client_id("user").into());
    for (pkt, message) in [
        // a second CONNECT is a protocol error [MQTT-3.1.0-2]
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
            MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
                .control(async move |msg| {
                    if let Control::Stop(Reason::Protocol(err)) = msg {
                        let pkt = codec::Disconnect::from_proto_error(err.get_ref());
                        if let error::MqttProtocolError::ProtocolViolation(e) = err.get_ref() {
                            *error.lock().unwrap() = Some(e.message());
                        }
                        Ok::<_, TestError>(Some(codec::Packet::from(pkt).into()))
                    } else {
                        Ok(None)
                    }
                })
                .build(connect)
        });

        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::new();
        io.send(connect_pkt(), &codec).await.unwrap();
        let _ = io.recv(&codec).await.unwrap().unwrap();

        // control service is called with the protocol error
        io.send(pkt, &codec).await.unwrap();
        let result = io.recv(&codec).await;
        assert!(
            matches!(result, Ok(Some(Decoded::Packet(Packet::Disconnect(ref d), _)))
                     if d.reason_code == codec::DisconnectReasonCode::ProtocolError
            ),
            "Unexpected result: {result:#?}"
        );
        assert!(matches!(io.recv(&codec).await, Ok(None) | Err(_)));

        let err = error.lock().unwrap().take();
        assert!(err == Some(message), "{err:?}");
    }
    Ok(())
}

#[ntex::test]
async fn test_unexpected_ack_type() -> std::io::Result<()> {
    // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP (MQTT 5.0, 4.3.2, 4.3.3)
    for (ack, message) in [
        (
            Packet::PublishReceived(codec::PublishAck {
                packet_id: NonZeroU16::new(1).unwrap(),
                ..Default::default()
            }),
            "Expected PUBACK packet",
        ),
        (
            Packet::PublishComplete(codec::PublishAck2 {
                packet_id: NonZeroU16::new(1).unwrap(),
                reason_code: codec::PublishAck2Reason::Success,
                properties: Default::default(),
                reason_string: None,
            }),
            "Expected PUBACK packet",
        ),
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
                Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                    let sink = sink.clone();
                    let result = result.clone();
                    rt::spawn(async move {
                        let res = sink.publish("test").send_at_least_once(Bytes::new()).await;
                        *result.lock().unwrap() = Some(res.map(|_| ()));
                    });
                    Ok::<_, TestError>(p.ack())
                }))
            })
            .control(async move |msg| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    let pkt = codec::Disconnect::from_proto_error(err.get_ref());
                    if let error::MqttProtocolError::ProtocolViolation(e) = err.get_ref() {
                        *error.lock().unwrap() = Some(e.message());
                    }
                    Ok::<_, TestError>(Some(codec::Packet::from(pkt).into()))
                } else {
                    Ok(None)
                }
            })
            .build(connect)
        });

        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::new();
        io.send(
            Encoded::Packet(codec::Connect::default().client_id("user").into()),
            &codec,
        )
        .await
        .unwrap();
        let _ = io.recv(&codec).await.unwrap().unwrap();

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

        io.send(Encoded::Packet(ack), &codec).await.unwrap();
        let pkt = io.recv(&codec).await;
        assert!(
            matches!(pkt, Ok(Some(Decoded::Packet(Packet::Disconnect(_), _)))),
            "{pkt:?}"
        );
        assert!(matches!(io.recv(&codec).await, Ok(None) | Err(_)));
        assert_eq!(error.lock().unwrap().take(), Some(message));

        // publish fails instead of panicking
        let mut res = None;
        for _ in 0..50 {
            res = result.lock().unwrap().take();
            if res.is_some() {
                break;
            }
            sleep(Millis(10)).await;
        }
        assert_eq!(res, Some(Err(error::SendPacketError::Disconnected)));
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
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let sink = sink.clone();
                let result = result.clone();
                rt::spawn(async move {
                    // both PUBREC packets are received before release
                    let (r1, r2) = ntex::util::join(
                        sink.publish("a").send_exactly_once(Bytes::new()),
                        sink.publish("b").send_exactly_once(Bytes::new()),
                    )
                    .await;
                    let res = ntex::util::join(r1.unwrap().release(), r2.unwrap().release()).await;
                    *result.lock().unwrap() = Some(res);
                });
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

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
        io.send(
            Encoded::Packet(Packet::PublishReceived(codec::PublishAck {
                packet_id: NonZeroU16::new(id).unwrap(),
                ..Default::default()
            })),
            &codec,
        )
        .await
        .unwrap();
    }

    // PUBREL is sent for each PUBLISH
    for id in [1, 2] {
        let pkt = ntex::time::timeout(Millis(1000), io.recv(&codec)).await;
        assert!(
            matches!(pkt, Ok(Ok(Some(Decoded::Packet(Packet::PublishRelease(ref p), _))))
                     if p.packet_id.get() == id),
            "{pkt:?}"
        );
    }
    for id in [1, 2] {
        io.send(
            Encoded::Packet(Packet::PublishComplete(codec::PublishAck2 {
                packet_id: NonZeroU16::new(id).unwrap(),
                reason_code: codec::PublishAck2Reason::Success,
                properties: Default::default(),
                reason_string: None,
            })),
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

#[ntex::test]
async fn test_qos2_rejected_pubrec() -> std::io::Result<()> {
    let result = Arc::new(Mutex::new(None));
    let result2 = result.clone();
    let srv = server::test_server(async move || {
        let result = result2.clone();
        MqttServer::new(async move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            let result = result.clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let sink = sink.clone();
                let result = result.clone();
                rt::spawn(async move {
                    let rec = sink
                        .publish("a")
                        .send_exactly_once(Bytes::new())
                        .await
                        .unwrap();
                    let code = rec.packet().reason_code;
                    let released = rec.release().await;
                    // packet id is available for reuse
                    let ack = sink
                        .publish("b")
                        .packet_id(1)
                        .send_at_least_once(Bytes::new())
                        .await
                        .map(|ack| ack.reason_code);
                    *result.lock().unwrap() = Some((code, released, ack));
                });
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // trigger server QoS 2 PUBLISH
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
        matches!(pkt, Decoded::Publish(ref p, ..) if p.packet_id == NonZeroU16::new(1)),
        "{pkt:?}"
    );
    io.send(
        Encoded::Packet(Packet::PublishReceived(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::NotAuthorized,
            ..Default::default()
        })),
        &codec,
    )
    .await
    .unwrap();

    // no PUBREL for rejected PUBREC, next packet is QoS 1 PUBLISH with the same id
    let pkt = ntex::time::timeout(Millis(1000), io.recv(&codec)).await;
    assert!(
        matches!(pkt, Ok(Ok(Some(Decoded::Publish(ref p, ..))))
                 if p.topic == "b" && p.packet_id == NonZeroU16::new(1)),
        "{pkt:?}"
    );
    io.send(
        Encoded::Packet(Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            ..Default::default()
        })),
        &codec,
    )
    .await
    .unwrap();

    let mut res = None;
    for _ in 0..50 {
        res = result.lock().unwrap().take();
        if res.is_some() {
            break;
        }
        sleep(Millis(10)).await;
    }
    assert_eq!(
        res,
        Some((
            codec::PublishAckReason::NotAuthorized,
            Err(error::SendPacketError::UnexpectedRelease),
            Ok(codec::PublishAckReason::Success)
        ))
    );
    Ok(())
}

#[ntex::test]
async fn test_qos2_redelivery() -> std::io::Result<()> {
    let published = Arc::new(Mutex::new(0));
    let published2 = published.clone();
    let srv = server::TestServerBuilder::new(async move || {
        let published = published2.clone();
        MqttServer::new(async move |p: Publish| {
            *published.lock().unwrap() += 1;
            Ok::<_, TestError>(p.ack())
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    let id = NonZeroU16::new(1).unwrap();
    let pubrec = Decoded::Packet(
        Packet::PublishReceived(codec::PublishAck {
            packet_id: id,
            ..Default::default()
        }),
        4,
    );

    // re-delivery is acked by PUBREC and is not delivered [MQTT-4.3.3-10]
    for dup in [false, true] {
        let pkt = codec::Publish {
            dup,
            packet_id: Some(id),
            qos: QoS::ExactlyOnce,
            ..pkt_publish()
        };
        io.send(Encoded::Publish(pkt, None), &codec).await.unwrap();
        assert_eq!(io.recv(&codec).await.unwrap().unwrap(), pubrec);
    }

    io.send(
        Encoded::Packet(Packet::PublishRelease(codec::PublishAck2 {
            packet_id: id,
            ..Default::default()
        })),
        &codec,
    )
    .await
    .unwrap();
    let result = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        result,
        Decoded::Packet(
            Packet::PublishComplete(codec::PublishAck2 {
                packet_id: id,
                ..Default::default()
            }),
            4
        )
    );
    assert_eq!(*published.lock().unwrap(), 1);
    Ok(())
}

#[ntex::test]
async fn test_qos2_client() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let release = release2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::PublishRelease(msg) => {
                    release.store(true, Relaxed);
                    Ok::<_, TestError>(msg.ack())
                }
                _ => Ok(msg.disconnect()),
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
    assert_eq!(received.packet().packet_id, NonZeroU16::new(1).unwrap());
    received.properties(|_| ()).release().await.unwrap();
    assert!(release.load(Relaxed));
    Ok(())
}

#[ntex::test]
async fn test_qos2_receive_max() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async |msg: ProtocolMessage| Ok::<_, TestError>(msg.ack()))
            .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_qos(QoS::ExactlyOnce)
                .set_max_receive(1),
        ),
    )
    .start();

    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // packet id is released after PUBCOMP, receive maximum quota is restored
    for _ in 0..3 {
        let received = sink
            .publish(ByteString::from_static("test"))
            .send_exactly_once(Bytes::new())
            .await
            .unwrap();
        received.release().await.unwrap();
    }
    let res = sink
        .publish(ByteString::from_static("test"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    assert!(sink.is_open());
    Ok(())
}

#[ntex::test]
async fn test_qos2_default_protocol() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
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

/// Server publishes QoS 2 message to the client on any client publish
fn qos2_publisher() -> (server::TestServer, Arc<Mutex<Option<bool>>>) {
    let released = Arc::new(Mutex::new(None));
    let released2 = released.clone();

    let srv = server::test_server(async move || {
        let released = released2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let sink = con.sink().clone();
            let released = released.clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
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
                Ok::<_, TestError>(p.ack())
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
                        Ok::<_, TestError>(p.ack())
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
                Ok::<_, ()>(match msg {
                    client::ProtocolMessage::Publish(p) => {
                        let payload = p.read_all().await.unwrap();
                        received.borrow_mut().push((p.packet().qos, payload));
                        p.ack(codec::PublishAckReason::Success)
                    }
                    msg => msg.ack(),
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
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(async move || {
        let ping = ping2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::Ping(msg) => {
                    ping.store(true, Relaxed);
                    Ok::<_, TestError>(msg.ack())
                }
                _ => Ok(msg.disconnect_with(codec::Disconnect::default())),
            })
            .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(packet(pkt), Packet::PingResponse);
    assert!(ping.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(100)).await;
            Ok::<_, TestError>(p.ack())
        })
        .protocol(async move |msg| match msg {
            ProtocolMessage::Ping(msg) => Ok(msg.ack()),
            ProtocolMessage::Subscribe(mut msg) => {
                for mut sub in &mut msg {
                    sub.topic();
                    sub.options();
                    sub.subscribe(codec::QoS::AtLeastOnce);
                }
                Ok::<_, TestError>(msg.ack())
            }
            _ => Ok(msg.disconnect()),
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
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    io.send(
        Encoded::Packet(
            codec::Subscribe {
                id: None,
                packet_id: NonZeroU16::new(2).unwrap(),
                user_properties: Default::default(),
                topic_filters: vec![(
                    ByteString::from("topic1"),
                    codec::SubscriptionOptions {
                        qos: codec::QoS::AtLeastOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: codec::RetainHandling::AtSubscribe,
                    },
                )],
            }
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();

    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();

    // subscribe and ping responses do not wait for publish acks
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::SubscribeAck(codec::SubscribeAck {
            packet_id: NonZeroU16::new(2).unwrap(),
            properties: Default::default(),
            reason_string: None,
            status: vec![codec::SubscribeAckReason::GrantedQos1],
        })
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(packet(pkt), Packet::PingResponse);

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::Success,
            properties: Default::default(),
            reason_string: None,
        })
    );

    Ok(())
}

#[ntex::test]
async fn test_dups() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(100)).await;
            Ok::<_, TestError>(p.ack())
        })
        .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(
            codec::Connect::default()
                .client_id("user")
                .receive_max(2)
                .into(),
        ),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();

    // send packet_id dup
    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();

    // send subscribe dup
    io.send(
        Encoded::Packet(
            codec::Subscribe {
                id: None,
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Default::default(),
                topic_filters: vec![(
                    ByteString::from("topic1"),
                    codec::SubscriptionOptions {
                        qos: codec::QoS::AtLeastOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: codec::RetainHandling::AtSubscribe,
                    },
                )],
            }
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();

    // send unsubscribe dup
    io.send(
        Encoded::Packet(
            codec::Unsubscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Default::default(),
                topic_filters: vec![ByteString::from("topic1")],
            }
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();

    // subscribe acks do not wait for publish acks
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        codec::SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: Default::default(),
            reason_string: None,
            status: vec![codec::SubscribeAckReason::PacketIdentifierInUse],
        }
        .into()
    );

    // UnsubscribeAck
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        codec::UnsubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: Default::default(),
            reason_string: None,
            status: vec![codec::UnsubscribeAckReason::PacketIdentifierInUse],
        }
        .into()
    );

    // publish acks are sent in the order publish packets are received
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::Success,
            properties: Default::default(),
            reason_string: None,
        })
    );

    // PublishAck for the dup
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::PacketIdentifierInUse,
            properties: Default::default(),
            reason_string: None,
        })
    );
}

#[ntex::test]
async fn test_max_receive() {
    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(10000)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(err)) = msg {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::from_proto_error(err.get_ref())).into(),
                ))
            } else {
                Ok::<_, TestError>(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_receive(1)
                .set_max_qos(codec::QoS::AtLeastOnce),
        ),
    )
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();

    io.send(
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .await
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(ack),
        Packet::ConnectAck(Box::new(codec::ConnectAck {
            receive_max: NonZeroU16::new(1).unwrap(),
            max_qos: codec::QoS::AtLeastOnce,
            reason_code: codec::ConnectAckReason::Success,
            topic_alias_max: 32,
            server_keepalive_sec: Some(30),
            max_packet_size: Some(256 * 1024),
            ..Default::default()
        }))
    );

    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(NonZeroU16::new(2).unwrap()),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        })
    );
}

#[ntex::test]
async fn test_default_max_size() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            let _ = p.read_all().await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(err)) = msg {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::from_proto_error(err.get_ref())).into(),
                ))
            } else {
                Ok::<_, TestError>(None)
            }
        })
        .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .await
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    let Packet::ConnectAck(ack) = packet(ack) else {
        panic!()
    };
    assert_eq!(ack.max_packet_size, Some(256 * 1024));

    // 13 bytes of fixed header, topic, packet id and properties length
    let publish = |payload_size: u32| {
        Encoded::Publish(
            codec::Publish {
                payload_size,
                ..pkt_publish()
            },
            Some(Bytes::from(vec![b'*'; payload_size as usize])),
        )
    };
    io.send(publish(256 * 1024 - 13), &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(packet(pkt), Packet::PublishAck(_)));

    io.send(publish(256 * 1024 - 12), &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    let Packet::Disconnect(pkt) = packet(pkt) else {
        panic!()
    };
    assert_eq!(pkt.reason_code, codec::DisconnectReasonCode::PacketTooLarge);
}

#[ntex::test]
async fn test_keepalive() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(async move || {
        let ka = ka2.clone();

        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| match msg {
                Control::Stop(Reason::Protocol(msg)) => {
                    if let &error::MqttProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ok::<_, TestError>(None)
                }
                _ => Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                )),
            })
            .build(async move |con: Connect| Ok::<_, TestError>(con.ack(St).keep_alive(1)))
    });

    // connect to server, client keep-alive is 0
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    // [MQTT-3.2.2-22] server advertises its keep-alive
    assert_eq!(client.packet().server_keepalive_sec, Some(1));

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    // client pings with the server keep-alive
    assert!(sink.is_open());
    sleep(Duration::from_millis(3500)).await;
    assert!(sink.is_open());
    assert!(!ka.load(Relaxed));
}

#[ntex::test]
async fn test_keepalive2() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(async move || {
        let ka = ka2.clone();

        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| match msg {
                Control::Stop(Reason::Protocol(msg)) => {
                    if let &error::MqttProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ok::<_, TestError>(None)
                }
                _ => Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                )),
            })
            .build(async move |con: Connect| Ok::<_, TestError>(con.ack(St).keep_alive(2)))
    });

    // client that does not ping
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(
            codec::Connect {
                keep_alive: 10,
                ..Default::default()
            }
            .client_id("user")
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    let Decoded::Packet(codec::Packet::ConnectAck(ack), _) = ack else {
        panic!("{ack:?}")
    };
    assert_eq!(ack.server_keepalive_sec, Some(2));

    for id in 1..=2 {
        io.send(
            Encoded::Publish(
                codec::Publish {
                    packet_id: NonZeroU16::new(id),
                    ..pkt_publish()
                },
                None,
            ),
            &codec,
        )
        .await
        .unwrap();
        let _ = io.recv(&codec).await.unwrap().unwrap();
        sleep(Duration::from_millis(500)).await;
    }

    // closed after 1.5 times of the server keep-alive
    sleep(Duration::from_millis(1500)).await;
    assert!(!ka.load(Relaxed));
    sleep(Duration::from_millis(2500)).await;
    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_keepalive3() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let ka = ka2.clone();

        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| match msg {
                Control::Stop(Reason::Protocol(msg)) => {
                    if let &error::MqttProtocolError::ReadTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ok::<_, TestError>(None)
                }
                _ => Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                )),
            })
            .build(async move |con: Connect| Ok::<_, TestError>(con.ack(St).keep_alive(1)))
    })
    .config(
        SharedCfg::new("MQTT").add(IoConfig::new().set_frame_read_rate(
            Seconds(1),
            Seconds(5),
            256,
        )),
    )
    .start();

    // connect to server
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
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(500)).await;

    let mut buf = BytePages::default();
    let pkt = Encoded::Publish(
        codec::Publish {
            packet_id: Some(NonZeroU16::new(2).unwrap()),
            ..pkt_publish()
        },
        None,
    );
    codec.encode(pkt, &mut buf).unwrap();
    io.encode_slice(&buf.freeze()[..5]).unwrap();
    sleep(Duration::from_millis(2000)).await;

    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos1() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(_)) = msg {
                Ok::<_, TestError>(None)
            } else {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(async move |con: Connect| {
            let builder = con.sink().publish("test").properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            ntex::rt::spawn(async move {
                let res = builder.send_at_least_once(Bytes::new()).await;
                assert_eq!(
                    res,
                    Err(error::SendPacketError::Encode(
                        error::EncodeError::OverMaxPacketSize
                    ))
                );
            });
            Ok::<_, TestError>(con.ack(St))
        })
    });

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_packet_size(30),
        )
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos0() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(_)) = msg {
                Ok::<_, TestError>(None)
            } else {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(async move |con: Connect| {
            let builder = con.sink().publish("test").properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            let res = builder.send_at_most_once(Bytes::new());
            assert_eq!(
                res,
                Err(error::SendPacketError::Encode(
                    error::EncodeError::OverMaxPacketSize
                ))
            );
            Ok::<_, TestError>(con.ack(St))
        })
    });

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_packet_size(30),
        )
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
}

/// Make sure we can publish message to client after local codec error
#[ntex::test]
async fn test_sink_success_after_encoder_error_qos1() {
    let success = Arc::new(AtomicBool::new(false));
    let success2 = success.clone();

    let srv = server::test_server(async move || {
        let success = success2.clone();
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(_)) = msg {
                Ok::<_, TestError>(None)
            } else {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(async move |con: Connect| {
            let sink = con.sink().clone();
            let success = success.clone();

            ntex::rt::spawn(async move {
                let builder = sink.publish("test").properties(|props| {
                    props.user_properties.push((
                        "ssssssssssssssssssssssssssssssssssss".into(),
                        "ssssssssssssssssssssssssssssssssssss".into(),
                    ));
                });
                let res = builder.send_at_least_once(Bytes::new()).await;
                assert_eq!(
                    res,
                    Err(error::SendPacketError::Encode(
                        error::EncodeError::OverMaxPacketSize
                    ))
                );

                let res = sink.publish("test").send_at_least_once(Bytes::new()).await;
                assert!(res.is_ok());
                success.store(true, Relaxed);
            });
            Ok::<_, TestError>(con.ack(St))
        })
    });

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_packet_size(30),
        )
        .await
        .unwrap();

    let sink = client.sink();

    async fn publish(pkt: Publish) -> Result<PublishAck, TestError> {
        Ok(pkt.ack())
    }

    let router = client.resource("test", publish);
    ntex::rt::spawn(router.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    assert!(success.load(Relaxed));
}

#[ntex::test]
async fn test_request_problem_info() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            Ok::<_, TestError>(
                p.ack()
                    .properties(|props| {
                        props.push((
                            "ssssssssssssssssssssssssssssssssssss".into(),
                            "ssssssssssssssssssssssssssssssssssss".into(),
                        ))
                    })
                    .reason("TEST".into()),
            )
        })
        .build(async move |con: Connect| Ok::<_, ()>(con.ack(St)))
    });

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_packet_size(30)
                .packet(|pkt| pkt.request_problem_info = false),
        )
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await
        .unwrap();
    assert!(res.properties.is_empty());
    assert!(res.reason_string.is_none());
}

#[ntex::test]
async fn test_suback_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::Subscribe(mut msg) => {
                    msg.iter_mut().for_each(|mut s| {
                        s.fail(codec::SubscribeAckReason::ImplementationSpecificError)
                    });
                    Ok::<_, TestError>(msg.ack_reason("some reason".into()).ack())
                }
                _ => Ok(msg.disconnect()),
            })
            .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Packet::Subscribe(codec::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![(
                "topic1".into(),
                codec::SubscriptionOptions {
                    qos: codec::QoS::AtLeastOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: codec::RetainHandling::AtSubscribe,
                },
            )],
            id: None,
            user_properties: codec::UserProperties::default(),
        })
        .into(),
        &codec,
    )
    .await
    .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::SubscribeAck(codec::SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            status: vec![codec::SubscribeAckReason::ImplementationSpecificError],
            properties: codec::UserProperties::default(),
            reason_string: Some("some reason".into()),
        })
    );

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
        MqttServer::new(async move |p: Publish| {
            publish.store(true, Relaxed);
            Ok::<_, TestError>(p.ack())
        })
        .protocol(async move |msg| match msg {
            ProtocolMessage::Disconnect(msg) => {
                disconnect.store(true, Relaxed);
                Ok::<_, TestError>(msg.ack())
            }
            _ => Ok(msg.disconnect()),
        })
        .build(connect)
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .unwrap();
    io.encode(Encoded::Publish(pkt_publish(), Some(Bytes::new())), &codec)
        .unwrap();
    io.encode(
        Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        })
        .into(),
        &codec,
    )
    .unwrap();
    io.flush(true).await.unwrap();
    sleep(Duration::from_millis(50)).await;
    drop(io);
    sleep(Duration::from_millis(50)).await;

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
        MqttServer::new(async move |p: Publish| {
            publish.store(true, Relaxed);
            Ok::<_, TestError>(p.ack())
        })
        .protocol(async move |msg| match msg {
            ProtocolMessage::Disconnect(msg) => {
                disconnect.store(true, Relaxed);
                Ok::<_, TestError>(msg.ack())
            }
            _ => Ok(msg.disconnect()),
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
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .unwrap();

    io.encode(
        Encoded::Publish(
            codec::Publish {
                packet_id,
                dup: false,
                retain: false,
                qos: publish_qos,
                topic: ByteString::from("test"),
                payload_size: 0,
                properties: Default::default(),
            },
            None,
        ),
        &codec,
    )
    .unwrap();

    io.encode(
        Encoded::Packet(Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        })),
        &codec,
    )
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
async fn test_max_qos() -> std::io::Result<()> {
    let violated = Arc::new(AtomicBool::new(false));
    let violated2 = violated.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let violated = violated2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| {
                if let Control::Stop(Reason::Protocol(err)) = msg
                    && let error::MqttProtocolError::ProtocolViolation(_) = err.get_ref()
                {
                    violated.store(true, Relaxed);
                    Ok(Some(
                        codec::Packet::from(codec::Disconnect::from_proto_error(err.get_ref()))
                            .into(),
                    ))
                } else {
                    Ok::<_, TestError>(None)
                }
            })
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::AtMostOnce)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(Packet::Connect(Box::new(
            codec::Connect::default().client_id("user"),
        ))),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.encode(Encoded::Publish(pkt_publish(), None), &codec)
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::QosNotSupported,
            ..Default::default()
        })
    );
    assert!(violated.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_retain_not_available() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async |msg: Connect| {
                Ok::<_, TestError>(msg.ack(St).with(|ack| ack.retain_available = false))
            },
        )
    });

    // RETAIN is not allowed for any QoS [MQTT-3.2.2-14]
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.send(
            Encoded::Packet(Packet::Connect(Box::new(
                codec::Connect::default().client_id("user"),
            ))),
            &codec,
        )
        .await
        .unwrap();
        let ack = io.recv(&codec).await.unwrap().unwrap();
        let Packet::ConnectAck(ack) = packet(ack) else {
            panic!()
        };
        assert!(!ack.retain_available);

        let pkt = codec::Publish {
            retain: true,
            qos,
            packet_id: if qos == QoS::AtMostOnce {
                None
            } else {
                NonZeroU16::new(1)
            },
            ..pkt_publish()
        };
        io.send(Encoded::Publish(pkt, None), &codec).await.unwrap();
        let pkt = io.recv(&codec).await.unwrap().unwrap();
        assert_eq!(
            packet(pkt),
            Packet::Disconnect(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::RetainNotSupported,
                ..Default::default()
            }),
            "{qos:?}"
        );
    }

    Ok(())
}

#[ntex::test]
async fn test_subscription_not_available() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async |msg: Connect| {
                Ok::<_, TestError>(msg.ack(St).with(|ack| {
                    ack.shared_subscription_available = false;
                    ack.wildcard_subscription_available = false;
                }))
            },
        )
    });

    for (tf, reason_code) in [
        // (MQTT 5.0, 3.2.2.3.13)
        (
            "$share/group/test",
            codec::DisconnectReasonCode::SharedSubscriptionNotSupported,
        ),
        // (MQTT 5.0, 3.2.2.3.11)
        (
            "test/#",
            codec::DisconnectReasonCode::WildcardSubscriptionsNotSupported,
        ),
    ] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.send(
            Encoded::Packet(Packet::Connect(Box::new(
                codec::Connect::default().client_id("user"),
            ))),
            &codec,
        )
        .await
        .unwrap();
        let ack = io.recv(&codec).await.unwrap().unwrap();
        let Packet::ConnectAck(ack) = packet(ack) else {
            panic!()
        };
        assert!(!ack.shared_subscription_available);
        assert!(!ack.wildcard_subscription_available);

        io.send(
            Encoded::Packet(Packet::Subscribe(codec::Subscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                id: None,
                user_properties: codec::UserProperties::default(),
                topic_filters: vec![(
                    ByteString::from_static(tf),
                    codec::SubscriptionOptions::default(),
                )],
            })),
            &codec,
        )
        .await
        .unwrap();
        let pkt = io.recv(&codec).await.unwrap().unwrap();
        let Packet::Disconnect(pkt) = packet(pkt) else {
            panic!("{tf}")
        };
        assert_eq!(pkt.reason_code, reason_code, "{tf}");
    }

    Ok(())
}

#[ntex::test]
async fn test_sink_ready() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |packet: Connect| {
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

                Ok::<_, TestError>(packet.ack(St))
            },
        )
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(Packet::Connect(Box::new(
            codec::Connect::default().client_id("user"),
        ))),
        &codec,
    )
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(ack),
        Packet::ConnectAck(Box::new(codec::ConnectAck {
            max_qos: QoS::AtLeastOnce,
            receive_max: NonZeroU16::new(16).unwrap(),
            topic_alias_max: 32,
            server_keepalive_sec: Some(30),
            max_packet_size: Some(256 * 1024),
            ..Default::default()
        }))
    );

    let result = io.recv(&codec).await;
    assert!(result.is_ok());

    Ok(())
}

#[ntex::test]
async fn test_sink_publish_noblock() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
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

    sink.publish_ack_cb(move |pkt, disconnected| {
        assert!(!disconnected);
        results2.borrow_mut().push(pkt.packet_id);
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

        MqttServer::new(async move |p: Publish| {
            let _ = p.read_all().await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(msg)) = msg
                && msg.get_ref() == &error::MqttProtocolError::ReadTimeout
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
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );

    let mut buf = BytePages::default();
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();

    io.encode_slice(&buf[..50]).unwrap();
    buf.advance_to(50);
    sleep(Millis(100)).await;
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

struct SetOnDrop(Arc<AtomicBool>, Option<::oneshot::Sender<()>>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Relaxed);
        let _ = self.1.take().unwrap().send(());
    }
}

#[ntex::test]
async fn test_publish_sink_disconnect() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = ::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let tx = tx.clone();
            let val = val.clone();
            let sink = con.sink().clone();

            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let _st = SetOnDrop(val.clone(), tx.lock().unwrap().take());
                sink.close();
                sleep(Seconds(999)).await;
                Ok::<_, TestError>(p.ack())
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

    let _ = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once(Bytes::new())
        .await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_peergone_after_sink_disconnect() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let sink = con.sink().clone();
            rt::spawn(async move {
                sleep(Seconds(1)).await;
                sink.close();
            });
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                Ok::<_, TestError>(p.ack())
            }))
        })
        .control(async move |pkt: Control<_>| {
            if let Control::Stop(Reason::PeerGone(_)) = pkt {
                val.store(true, Relaxed);
                let _ = tx.lock().unwrap().take().unwrap().send(());
            }
            Ok::<_, TestError>(None)
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

    let _ = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once(Bytes::new())
        .await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_disconnect_once() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async |_: Publish| Err(TestError))
            .control(async move |con: &Session<St>| {
                let sink = con.sink().clone();
                Ok::<_, Infallible>(fn_service(async move |pkt: Control<_>| {
                    if let Control::Stop(Reason::Error(_)) = pkt {
                        sink.close_with_reason(codec::Disconnect {
                            reason_code: codec::DisconnectReasonCode::ServerMoved,
                            ..Default::default()
                        });
                    }
                    Ok::<_, TestError>(None)
                }))
            })
            .build(connect)
    });

    // connect to server
    let cl = Pipeline::new(
        SharedCfg::new("client").build(),
        client::MqttConnector::new(),
    )
    .call(client::Connect::new(srv.addr()).client_id("user"))
    .await
    .unwrap()
    .into_inner();
    let client = Framed::new(cl.0, cl.1);

    client
        .send(Encoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
                topic: "test/test".into(),
                payload_size: 0,
                properties: Default::default(),
            },
            None,
        ))
        .await
        .unwrap();

    // Receive DISCONNECT
    let res = client.recv().await.unwrap().unwrap();
    assert!(matches!(
        res,
        codec::Decoded::Packet(
            codec::Packet::Disconnect(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::ServerMoved,
                ..
            }),
            _
        )
    ));
    // IO Close
    let res = client.recv().await.unwrap();
    assert_eq!(res, None);

    Ok(())
}

#[ntex::test]
/// MqttServiceConfig::set_max_send() limits upper bound of outbound
/// concurrent requests regardless client request
async fn test_max_outbound() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async |con: &Session<St>| {
            assert_eq!(con.sink().credit(), 15);
            Ok::<_, Infallible>(fn_service(async |p: Publish| Ok::<_, TestError>(p.ack())))
        })
        .build(async |hnd: Connect| Ok::<_, TestError>(hnd.ack(St).max_send(Some(15))))
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_send(10)))
    .start();

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_receive(100),
        )
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
    Ok(())
}

#[ntex::test]
async fn test_max_outbound2() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move || {
        MqttServer::new(async |con: &Session<St>| {
            assert_eq!(con.sink().credit(), 10);
            Ok::<_, Infallible>(fn_service(async |p: Publish| Ok::<_, TestError>(p.ack())))
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_send(10)))
    .start();

    // connect to server
    let client = Pipeline::new(SharedCfg::default(), client::MqttConnector::new())
        .call(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_receive(100),
        )
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
    Ok(())
}

/// MQTT 3.14.2-2 Non-Zero Session Expiry Interval is set on DISCONNECT
#[ntex::test]
async fn protocol_error_session_expiry() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    val.store(true, Relaxed);
                    let _ = tx.lock().unwrap().take().unwrap().send(());
                    Ok(Some(
                        codec::Packet::from(codec::Disconnect::from_proto_error(err.get_ref()))
                            .into(),
                    ))
                } else {
                    Ok::<_, TestError>(None)
                }
            })
            .build(connect)
    });

    // connect to server, session expiry to 0
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(
            codec::Connect {
                session_expiry_interval_secs: 0,
                ..Default::default()
            }
            .client_id("user")
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect, session expiry is not zero
    io.send(
        Encoded::Packet(
            codec::Disconnect {
                session_expiry_interval_secs: Some(10),
                ..Default::default()
            }
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();

    let result = io.recv(&codec).await;
    assert!(
        matches!(result, Ok(Some(codec::Decoded::Packet(codec::Packet::Disconnect(ref d), _)))
                 if d.reason_code == codec::DisconnectReasonCode::ProtocolError
        ),
        "Unexpected result: {:#?}",
        result
    );

    let _ = rx.await;
    assert!(val.load(Relaxed));

    let result = io.recv(&codec).await.unwrap();
    assert_eq!(result, None);

    Ok(())
}

#[ntex::test]
async fn test_sink_close_with_no_reason() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = ::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let tx = tx.clone();
            let val = val.clone();
            let sink = con.sink().clone();

            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let _st = SetOnDrop(val.clone(), tx.lock().unwrap().take());
                sink.close_with_no_reason();
                sleep(Seconds(999)).await;
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    });

    // connect to server
    let client = Pipeline::new(
        SharedCfg::new("client").build(),
        client::MqttConnector::new(),
    )
    .call(client::Connect::new(srv.addr()).client_id("user"))
    .await
    .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let _ = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once(Bytes::new())
        .await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_handshake_unsupported_protocol_level() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
    });
    let connect_pkt = |level| {
        [
            0x10, 0x0c, 0x00, 0x04, b'M', b'Q', b'T', b'T', level, 0x02, 0x00, 0x3c, 0x00, 0x00,
        ]
    };

    // MQTT 3.1 and 3.1.1 clients get v3 CONNACK 0x01
    for level in [3, 4] {
        let io = srv.connect().await.unwrap();
        let codec = ntex_mqtt::v3::codec::Codec::default();
        io.encode_slice(&connect_pkt(level)).unwrap();

        let ack = io.recv(&codec).await.unwrap().unwrap();
        assert_eq!(
            ack,
            ntex_mqtt::v3::codec::Decoded::Packet(
                ntex_mqtt::v3::codec::Packet::ConnectAck(ntex_mqtt::v3::codec::ConnectAck {
                    session_present: false,
                    return_code:
                        ntex_mqtt::v3::codec::ConnectAckReason::UnacceptableProtocolVersion,
                }),
                2
            )
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }

    // other levels get v5 CONNACK 0x84
    for level in [0, 6, 0xff] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.encode_slice(&connect_pkt(level)).unwrap();

        let ack = io.recv(&codec).await.unwrap().unwrap();
        assert_eq!(
            ack,
            Decoded::Packet(
                Packet::ConnectAck(Box::new(codec::ConnectAck {
                    reason_code: codec::ConnectAckReason::UnsupportedProtocolVersion,
                    ..Default::default()
                })),
                3
            )
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }

    Ok(())
}

#[ntex::test]
async fn test_handshake_invalid_will_topic() -> std::io::Result<()> {
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();
    let srv = server::test_server(async move || {
        let called = called2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |msg: Connect| {
                called.store(true, Relaxed);
                Ok::<_, TestError>(msg.ack(St))
            },
        )
    });
    let connect_pkt = |topic: &str, response_topic: Option<&str>| {
        let mut will_props = Vec::new();
        if let Some(t) = response_topic {
            will_props.push(0x08);
            will_props.extend_from_slice(&(t.len() as u16).to_be_bytes());
            will_props.extend_from_slice(t.as_bytes());
        }
        let mut body = vec![
            0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, 0x00, 0x3c, 0x00, 0x00, 0x02, b'i',
            b'd',
        ];
        body.push(will_props.len() as u8);
        body.extend(will_props);
        body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&[0x00, 0x00]);
        let mut pkt = vec![0x10, body.len() as u8];
        pkt.extend(body);
        pkt
    };

    for (topic, response_topic, err) in [
        ("", None, error::SpecViolation::Will_4_7_3_1),
        ("a/#", None, error::SpecViolation::Will_4_7_0_1),
        ("+", Some("r/t"), error::SpecViolation::Will_4_7_0_1),
        ("t", Some("r/+"), error::SpecViolation::Will_3_3_2_14),
        ("t", Some("#"), error::SpecViolation::Will_3_3_2_14),
    ] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.encode_slice(&connect_pkt(topic, response_topic))
            .unwrap();

        let Decoded::Packet(Packet::ConnectAck(ack), _) = io.recv(&codec).await.unwrap().unwrap()
        else {
            panic!()
        };
        assert_eq!(
            *ack,
            codec::ConnectAck {
                reason_code: codec::ConnectAckReason::ProtocolError,
                reason_string: Some(ByteString::from(err.to_string())),
                ..Default::default()
            }
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }
    assert!(!called.load(Relaxed));

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode_slice(&connect_pkt("t", Some("r/t"))).unwrap();
    let Decoded::Packet(Packet::ConnectAck(ack), _) = io.recv(&codec).await.unwrap().unwrap()
    else {
        panic!()
    };
    assert_eq!(ack.reason_code, codec::ConnectAckReason::Success);
    assert!(called.load(Relaxed));

    Ok(())
}
