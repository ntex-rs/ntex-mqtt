use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::{cell::RefCell, rc::Rc};
use std::{future::Future, num::NonZeroU16, pin::Pin, time::Duration};

use ntex::service::{ServiceFactory, cfg::SharedCfg, fn_factory_with_config, fn_service};
use ntex::time::{Millis, Seconds, sleep};
use ntex::util::{ByteString, Bytes, BytesMut, Ready, lazy};
use ntex::{codec::Encoder, io::IoConfig, rt, server};

use ntex_mqtt::MqttServiceConfig;
use ntex_mqtt::v5::codec::{self, Decoded, Encoded, Packet};
use ntex_mqtt::v5::{
    Control, CtlFlow, CtlFrame, CtlReason, Handshake, HandshakeAck, MqttServer, Publish,
    PublishAck, QoS, Session, client, error,
};

struct St;

#[derive(Debug)]
struct TestError;

impl From<()> for TestError {
    fn from(_: ()) -> Self {
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

async fn handshake(packet: Handshake) -> Result<HandshakeAck<St>, TestError> {
    Ok(packet.ack(St))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(handshake).publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("test")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());

    let res = sink.publish(ByteString::from_static("#")).send_at_least_once(Bytes::new()).await;
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
        MqttServer::new(handshake).publish(move |p: Publish| {
            let chunks = chunks.clone();
            async move {
                while let Ok(Some(chunk)) = p.read().await {
                    chunks.lock().unwrap().push(chunk);
                }
                Ok::<_, TestError>(p.ack())
            }
        })
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) = sink.publish(ByteString::from_static("test")).stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"111111")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    // pkt 2
    let (fut, payload) = sink.publish(ByteString::from_static("test")).stream_at_least_once(5);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"22")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"222")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    // pkt 3
    let (fut, payload) = sink.publish(ByteString::from_static("test")).stream_at_least_once(2);
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

    let (fut, _) = sink.publish(ByteString::from_static("#")).stream_at_least_once(12);
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
        MqttServer::new(handshake).publish(move |mut p: Publish| {
            let chunks = chunks.clone();
            async move {
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
            }
        })
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) =
        sink.publish(ByteString::from_static("test")).retain(true).stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"111111")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    assert_eq!(&chunks.lock().unwrap()[..], vec![Bytes::from_static(b"1111111111"),]);
}

#[ntex::test]
async fn test_handshake_failed() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(fn_service(|hnd: Handshake| async move {
            Ok(hnd.failed::<St>(codec::ConnectAckReason::NotAuthorized))
        }))
        .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    // connect to server
    let err = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap_err();
    match err {
        error::ClientError::Ack(pkt) => {
            assert_eq!(pkt.reason_code, codec::ConnectAckReason::NotAuthorized);
        }
        _ => panic!("error"),
    }

    Ok(())
}

#[ntex::test]
async fn test_disconnect() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(handshake).publish(ntex::service::fn_factory_with_config(
            |session: Session<St>| {
                Ready::Ok::<_, TestError>(ntex::service::fn_service(move |p: Publish| {
                    session.sink().close();
                    async move {
                        sleep(Duration::from_millis(100)).await;
                        Ok::<_, TestError>(p.ack())
                    }
                }))
            },
        ))
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink.publish(ByteString::from_static("#")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_err());

    Ok(())
}

#[ntex::test]
async fn test_disconnect_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(handshake).publish(ntex::service::fn_factory_with_config(
            |session: Session<St>| {
                Ready::Ok::<_, TestError>(ntex::service::fn_service(move |p: Publish| {
                    let pkt = codec::Disconnect {
                        reason_code: codec::DisconnectReasonCode::ServerMoved,
                        ..Default::default()
                    };
                    session.sink().close_with_reason(pkt);
                    async move {
                        sleep(Duration::from_millis(100)).await;
                        Ok::<_, TestError>(p.ack())
                    }
                }))
            },
        ))
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink.publish(ByteString::from_static("#")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_err());

    Ok(())
}

#[ntex::test]
async fn test_nested_errors_handling() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Disconnect(_)) => Ready::Err(TestError),
                Control::Stop(CtlReason::Error(_)) => Ready::Err(TestError),
                Control::Shutdown(m) => Ready::Ok(m.ack()),
                _ => panic!("{:?}", msg),
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

#[ntex::test]
async fn test_disconnect_on_error() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Disconnect(_)) => Ready::Err(TestError),
                Control::Stop(CtlReason::Error(m)) => {
                    Ready::Ok(m.ack(codec::DisconnectReasonCode::ImplementationSpecificError))
                }
                Control::Shutdown(m) => Ready::Ok(m.ack()),
                Control::Stop(CtlReason::PeerGone(m)) => Ready::Ok(m.ack()),
                _ => panic!("{:?}", msg),
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec).await.unwrap();
    let res = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(packet(res), Packet::Disconnect(_)));

    Ok(())
}

#[ntex::test]
async fn test_disconnect_after_control_error() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Subscribe(_)) => Ready::Err(TestError),
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
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
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::PublishRelease(msg)) => {
                    release.store(true, Relaxed);
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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
async fn test_qos2_client() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let release = release2.clone();
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::PublishRelease(msg)) => {
                    release.store(true, Relaxed);
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
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
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(async move || {
        let ping = ping2.clone();
        MqttServer::new(handshake)
            .control(move |msg| {
                let ping = ping.clone();
                match msg {
                    Control::Flow(CtlFlow::Ping(msg)) => {
                        ping.store(true, Relaxed);
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect_with(codec::Disconnect::default())),
                }
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(Encoded::Packet(Packet::PingRequest), &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(packet(pkt), Packet::PingResponse);
    assert!(ping.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Subscribe(mut msg)) => {
                    for mut sub in &mut msg {
                        sub.topic();
                        sub.options();
                        sub.subscribe(codec::QoS::AtLeastOnce);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| async move {
                sleep(Duration::from_millis(100)).await;
                Ok::<_, TestError>(p.ack())
            })
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Encoded::Publish(
            codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() },
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

    Ok(())
}

#[ntex::test]
async fn test_dups() {
    let srv = server::test_server(async move || {
        MqttServer::new(handshake).publish(|p: Publish| async move {
            sleep(Duration::from_millis(10000)).await;
            Ok::<_, TestError>(p.ack())
        })
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(codec::Connect::default().client_id("user").receive_max(2).into()),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Encoded::Publish(
            codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();

    // send packet_id dup
    io.send(
        Encoded::Publish(
            codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() },
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

    // PublishAck
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

    // SubscribeAck
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
}

#[ntex::test]
async fn test_max_receive() {
    let srv =
        server::TestServerBuilder::new(async move || {
            MqttServer::new(handshake)
                .control(move |msg| match msg {
                    Control::Stop(CtlReason::ProtocolError(msg)) => {
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                })
                .publish(|p: Publish| async move {
                    sleep(Duration::from_millis(10000)).await;
                    Ok::<_, TestError>(p.ack())
                })
        })
        .config(SharedCfg::new("MQTT").add(
            MqttServiceConfig::new().set_max_receive(1).set_max_qos(codec::QoS::AtLeastOnce),
        ))
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
            ..Default::default()
        }))
    );

    io.send(
        Encoded::Publish(
            codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    io.send(
        Encoded::Publish(
            codec::Publish { packet_id: Some(NonZeroU16::new(2).unwrap()), ..pkt_publish() },
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
async fn test_keepalive() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(async move || {
        let ka = ka2.clone();

        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St).keep_alive(1)) })
            .control(move |msg| match msg {
                Control::Stop(CtlReason::ProtocolError(msg)) => {
                    if let &error::ProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| async move { Ok::<_, TestError>(p.ack()) })
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    assert!(sink.is_open());
    sleep(Duration::from_millis(2500)).await;
    assert!(!sink.is_open());
    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_keepalive2() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(async move || {
        let ka = ka2.clone();

        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St).keep_alive(1)) })
            .control(move |msg| match msg {
                Control::Stop(CtlReason::ProtocolError(msg)) => {
                    if let &error::ProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| async move { Ok::<_, TestError>(p.ack()) })
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    assert!(sink.is_open());
    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
    sleep(Duration::from_millis(500)).await;
    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
    sleep(Duration::from_millis(2000)).await;

    assert!(!sink.is_open());
    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_keepalive3() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let ka = ka2.clone();

        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St).keep_alive(1)) })
            .control(move |msg| match msg {
                Control::Stop(CtlReason::ProtocolError(msg)) => {
                    if let &error::ProtocolError::ReadTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| async move { Ok::<_, TestError>(p.ack()) })
    })
    .config(SharedCfg::new("MQTT").add(IoConfig::new().set_frame_read_rate(
        Seconds(1),
        Seconds(5),
        256,
    )))
    .start();

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        Encoded::Publish(
            codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() },
            None,
        ),
        &codec,
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(500)).await;

    let mut buf = BytesMut::new();
    let pkt = Encoded::Publish(
        codec::Publish { packet_id: Some(NonZeroU16::new(2).unwrap()), ..pkt_publish() },
        None,
    );
    codec.encode(pkt, &mut buf).unwrap();
    io.write(&buf[..5]).unwrap();
    sleep(Duration::from_millis(2000)).await;

    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos1() {
    let srv = server::test_server(async move || {
        MqttServer::new(|con: Handshake| async move {
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
                    Err(error::SendPacketError::Encode(error::EncodeError::OverMaxPacketSize))
                );
            });
            Ok(con.ack(St))
        })
        .control(move |msg| match msg {
            Control::Stop(CtlReason::ProtocolError(msg)) => {
                Ready::Ok::<_, TestError>(msg.ack())
            }
            _ => Ready::Ok(msg.disconnect()),
        })
        .publish(|p: Publish| async move {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user").max_packet_size(30))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos0() {
    let srv = server::test_server(async move || {
        MqttServer::new(|con: Handshake| async move {
            let builder = con.sink().publish("test").properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            let res = builder.send_at_most_once(Bytes::new());
            assert_eq!(
                res,
                Err(error::SendPacketError::Encode(error::EncodeError::OverMaxPacketSize))
            );
            Ok(con.ack(St))
        })
        .control(move |msg| match msg {
            Control::Stop(CtlReason::ProtocolError(msg)) => {
                Ready::Ok::<_, TestError>(msg.ack())
            }
            _ => Ready::Ok(msg.disconnect()),
        })
        .publish(|p: Publish| async move {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user").max_packet_size(30))
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
}

/// Make sure we can publish message to client after local codec error
#[ntex::test]
async fn test_sink_success_after_encoder_error_qos1() {
    let success = Arc::new(AtomicBool::new(false));
    let success2 = success.clone();

    let srv = server::test_server(async move || {
        let success = success2.clone();
        MqttServer::new(move |con: Handshake| {
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
                    Err(error::SendPacketError::Encode(error::EncodeError::OverMaxPacketSize))
                );

                let res = sink.publish("test").send_at_least_once(Bytes::new()).await;
                assert!(res.is_ok());
                success.store(true, Relaxed);
            });
            Ready::Ok(con.ack(St))
        })
        .control(move |msg| match msg {
            Control::Stop(CtlReason::ProtocolError(msg)) => {
                Ready::Ok::<_, TestError>(msg.ack())
            }
            _ => Ready::Ok(msg.disconnect()),
        })
        .publish(|p: Publish| async move {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user").max_packet_size(30))
        .await
        .unwrap();

    let sink = client.sink();

    async fn publish(pkt: Publish) -> Result<PublishAck, TestError> {
        Ok(pkt.ack())
    }

    let router = client.resource("test", publish);
    ntex::rt::spawn(router.start_default());

    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
    assert!(success.load(Relaxed));
}

#[ntex::test]
async fn test_request_problem_info() {
    let srv = server::test_server(async move || {
        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St)) }).publish(
            |p: Publish| async move {
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
            },
        )
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
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
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Subscribe(mut msg)) => {
                    msg.iter_mut().for_each(|mut s| {
                        s.fail(codec::SubscribeAckReason::ImplementationSpecificError)
                    });
                    Ready::Ok::<_, TestError>(msg.ack_reason("some reason".into()).ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
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
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Disconnect(msg)) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(move |p: Publish| {
                publish.store(true, Relaxed);
                Ready::Ok::<_, TestError>(p.ack())
            })
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Packet::Connect(Box::new(codec::Connect::default().client_id("user"))).into(),
        &codec,
    )
    .unwrap();
    io.encode(Encoded::Publish(pkt_publish(), Some(Bytes::new())), &codec).unwrap();
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
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Protocol(CtlFrame::Disconnect(msg)) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .publish(move |p: Publish| {
                publish.store(true, Relaxed);
                Ready::Ok::<_, TestError>(p.ack())
            })
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
    sleep(Millis(2750)).await;
    io.close();
    drop(io);
    sleep(Millis(1500)).await;

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
        MqttServer::new(handshake)
            .control(move |msg| {
                let violated = violated.clone();
                match msg {
                    Control::Stop(CtlReason::ProtocolError(msg)) => {
                        if let error::ProtocolError::ProtocolViolation(_) = msg.get_ref() {
                            violated.store(true, Relaxed);
                        }
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                }
            })
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::AtMostOnce)))
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(Packet::Connect(Box::new(codec::Connect::default().client_id("user")))),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.encode(Encoded::Publish(pkt_publish(), None), &codec).unwrap();
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
async fn test_sink_ready() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(fn_service(|packet: Handshake| async move {
            let sink = packet.sink();
            let mut ready = Box::pin(sink.ready());
            let res = lazy(|cx| Pin::new(&mut ready).poll(cx)).await;
            assert!(res.is_pending());
            assert!(!sink.is_ready());

            let sink = sink.clone();
            ntex::rt::spawn(async move {
                sink.ready().await;
                assert!(sink.is_ready());
                sink.publish("/test").send_at_most_once(Bytes::from_static(b"body")).unwrap();
            });

            Ok::<_, TestError>(packet.ack(St))
        }))
        .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        Encoded::Packet(Packet::Connect(Box::new(codec::Connect::default().client_id("user")))),
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
        MqttServer::new(handshake).publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
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

    let res =
        sink.publish(ByteString::from_static("test3")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());

    assert_eq!(*results.borrow(), &[NonZeroU16::new(1).unwrap(), NonZeroU16::new(2).unwrap()]);

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

        MqttServer::new(handshake)
            .control(move |msg| {
                let check = check.clone();
                match msg {
                    Control::Stop(CtlReason::ProtocolError(msg)) => {
                        if msg.get_ref() == &error::ProtocolError::ReadTimeout {
                            check.store(true, Relaxed);
                        }
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                }
            })
            .publish(|p: Publish| async move {
                let _ = p.read_all().await;
                Ok::<_, TestError>(p.ack())
            })
    })
    .config(
        SharedCfg::new("MQTT")
            .add(IoConfig::new().set_frame_read_rate(Seconds(1), Seconds(2), 10))
            .add(MqttServiceConfig::new().set_min_chunk_size(32 * 1024)),
    )
    .start();

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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

    let mut buf = BytesMut::new();
    codec.encode(p, &mut buf).unwrap();

    io.write(&buf[..50]).unwrap();
    buf.split_to(50);
    sleep(Millis(100)).await;
    io.write(&buf[..10]).unwrap();
    buf.split_to(10);
    sleep(Millis(1000)).await;
    assert!(!check.load(Relaxed));

    io.write(&buf[..12]).unwrap();
    buf.split_to(12);
    sleep(Millis(1000)).await;
    assert!(!check.load(Relaxed));

    sleep(Millis(2300)).await;
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
async fn test_publish_sink_disconenct() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = ::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(handshake).publish(fn_factory_with_config(
            async move |session: Session<St>| {
                let tx = tx.clone();
                let val = val.clone();
                let sink = session.sink().clone();

                Ok::<_, ()>(fn_service(async move |p: Publish| {
                    let _st = SetOnDrop(val.clone(), tx.lock().unwrap().take());
                    sink.close();
                    sleep(Seconds(999)).await;
                    Ok::<_, TestError>(p.ack())
                }))
            },
        ))
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::new("client").into())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let _ =
        sink.publish(ByteString::from_static("test1")).send_at_least_once(Bytes::new()).await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_peergone_after_sink_disconenct() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = ::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(handshake)
            .control(fn_service(async move |pkt: Control<_>| {
                if let Control::Stop(CtlReason::PeerGone(_)) = pkt {
                    val.store(true, Relaxed);
                    let _ = tx.lock().unwrap().take().unwrap().send(());
                }
                Ok::<_, TestError>(pkt.ack())
            }))
            .publish(fn_factory_with_config(async move |session: Session<St>| {
                let sink = session.sink().clone();
                rt::spawn(async move {
                    sleep(Seconds(1)).await;
                    sink.close();
                });
                Ok::<_, ()>(fn_service(async move |p: Publish| Ok::<_, TestError>(p.ack())))
            }))
    });

    // connect to server
    let client = client::MqttConnector::new()
        .pipeline(SharedCfg::new("client").into())
        .await
        .unwrap()
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let _ =
        sink.publish(ByteString::from_static("test1")).send_at_least_once(Bytes::new()).await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}
