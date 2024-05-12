use std::sync::{atomic::AtomicBool, atomic::Ordering::Relaxed, Arc};
use std::{cell::RefCell, rc::Rc};
use std::{future::Future, num::NonZeroU16, pin::Pin, time::Duration};

use ntex::time::{sleep, Millis, Seconds};
use ntex::util::{lazy, ByteString, Bytes, BytesMut, Ready};
use ntex::{codec::Encoder, server, service::fn_service};

use ntex_mqtt::v5::{
    client, codec, error, Control, Handshake, HandshakeAck, MqttServer, Publish, PublishAck,
    QoS, Session,
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
        payload: Bytes::new(),
        properties: Default::default(),
    }
}

async fn handshake(packet: Handshake) -> Result<HandshakeAck<St>, TestError> {
    Ok(packet.ack(St))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("test"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_err());

    sink.close();
    Ok(())
}

#[ntex::test]
async fn test_handshake_failed() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(fn_service(|hnd: Handshake| async move {
            Ok(hnd.failed::<St>(codec::ConnectAckReason::NotAuthorized))
        }))
        .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
        .finish()
    });

    // connect to server
    let err =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap_err();
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
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(ntex::service::fn_factory_with_config(|session: Session<St>| {
                Ready::Ok::<_, TestError>(ntex::service::fn_service(move |p: Publish| {
                    session.sink().close();
                    async move {
                        sleep(Duration::from_millis(100)).await;
                        Ok::<_, TestError>(p.ack())
                    }
                }))
            }))
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_err());

    Ok(())
}

#[ntex::test]
async fn test_disconnect_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(ntex::service::fn_factory_with_config(|session: Session<St>| {
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
            }))
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_err());

    Ok(())
}

#[ntex::test]
async fn test_nested_errors_handling() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .control(move |msg| match msg {
                Control::Disconnect(_) => Ready::Err(TestError),
                Control::Error(_) => Ready::Err(TestError),
                Control::Closed(m) => Ready::Ok(m.ack()),
                _ => panic!("{:?}", msg),
            })
            .finish()
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(codec::Connect::default().client_id("user").into(), &codec).await.unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect
    io.send(codec::Disconnect::default().into(), &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

#[ntex::test]
async fn test_disconnect_on_error() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .control(move |msg| match msg {
                Control::Disconnect(_) => Ready::Err(TestError),
                Control::Error(m) => {
                    Ready::Ok(m.ack(codec::DisconnectReasonCode::ImplementationSpecificError))
                }
                Control::Closed(m) => Ready::Ok(m.ack()),
                _ => panic!("{:?}", msg),
            })
            .finish()
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(codec::Connect::default().client_id("user").into(), &codec).await.unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect
    io.send(codec::Disconnect::default().into(), &codec).await.unwrap();
    let res = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(res.0, codec::Packet::Disconnect(_)));

    Ok(())
}

#[ntex::test]
async fn test_disconnect_after_control_error() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .control(move |msg| match msg {
                Control::Subscribe(_) => Ready::Err(TestError),
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
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
        &codec,
    )
    .await
    .unwrap();

    let result = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(result.0, codec::Packet::Disconnect(_)));
    Ok(())
}

#[ntex::test]
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(move || {
        let ping = ping2.clone();
        MqttServer::new(handshake)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .control(move |msg| {
                let ping = ping.clone();
                match msg {
                    Control::Ping(msg) => {
                        ping.store(true, Relaxed);
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect_with(codec::Disconnect::default())),
                }
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(codec::Connect::default().client_id("user").into(), &codec).await.unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(codec::Packet::PingRequest, &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt.0, codec::Packet::PingResponse);
    assert!(ping.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .publish(|p: Publish| async move {
                sleep(Duration::from_millis(100)).await;
                Ok::<_, TestError>(p.ack())
            })
            .control(move |msg| match msg {
                Control::Subscribe(mut msg) => {
                    for mut sub in &mut msg {
                        sub.topic();
                        sub.options();
                        sub.subscribe(codec::QoS::AtLeastOnce);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(codec::Connect::default().client_id("user").into(), &codec).await.unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() }.into(),
        &codec,
    )
    .await
    .unwrap();
    io.send(
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
        &codec,
    )
    .await
    .unwrap();

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
        codec::Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::Success,
            properties: Default::default(),
            reason_string: None,
        })
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
        codec::Packet::SubscribeAck(codec::SubscribeAck {
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
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .publish(|p: Publish| async move {
                sleep(Duration::from_millis(10000)).await;
                Ok::<_, TestError>(p.ack())
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(codec::Connect::default().client_id("user").receive_max(2).into(), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() }.into(),
        &codec,
    )
    .await
    .unwrap();

    // send packet_id dup
    io.send(
        codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() }.into(),
        &codec,
    )
    .await
    .unwrap();

    // send subscribe dup
    io.send(
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
        &codec,
    )
    .await
    .unwrap();

    // send unsubscribe dup
    io.send(
        codec::Unsubscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            user_properties: Default::default(),
            topic_filters: vec![ByteString::from("topic1")],
        }
        .into(),
        &codec,
    )
    .await
    .unwrap();

    // PublishAck
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
        codec::Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::PacketIdentifierInUse,
            properties: Default::default(),
            reason_string: None,
        })
    );

    // SubscribeAck
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
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
        pkt.0,
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
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .receive_max(1)
            .max_qos(codec::QoS::AtLeastOnce)
            .publish(|p: Publish| async move {
                sleep(Duration::from_millis(10000)).await;
                Ok::<_, TestError>(p.ack())
            })
            .control(move |msg| match msg {
                Control::ProtocolError(msg) => Ready::Ok::<_, TestError>(msg.ack()),
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();

    io.send(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .await
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        ack.0,
        codec::Packet::ConnectAck(Box::new(codec::ConnectAck {
            receive_max: NonZeroU16::new(1).unwrap(),
            max_qos: codec::QoS::AtLeastOnce,
            reason_code: codec::ConnectAckReason::Success,
            topic_alias_max: 32,
            ..Default::default()
        }))
    );

    io.send(
        codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() }.into(),
        &codec,
    )
    .await
    .unwrap();
    io.send(
        codec::Publish { packet_id: Some(NonZeroU16::new(2).unwrap()), ..pkt_publish() }.into(),
        &codec,
    )
    .await
    .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
        codec::Packet::Disconnect(codec::Disconnect {
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

    let srv = server::test_server(move || {
        let ka = ka2.clone();

        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St).keep_alive(1)) })
            .publish(|p: Publish| async move { Ok::<_, TestError>(p.ack()) })
            .control(move |msg| match msg {
                Control::ProtocolError(msg) => {
                    if let &error::ProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

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

    let srv = server::test_server(move || {
        let ka = ka2.clone();

        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St).keep_alive(1)) })
            .publish(|p: Publish| async move { Ok::<_, TestError>(p.ack()) })
            .control(move |msg| match msg {
                Control::ProtocolError(msg) => {
                    if let &error::ProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    assert!(sink.is_open());
    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    sleep(Duration::from_millis(500)).await;
    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    sleep(Duration::from_millis(2000)).await;

    assert!(!sink.is_open());
    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_keepalive3() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(move || {
        let ka = ka2.clone();

        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St).keep_alive(1)) })
            .frame_read_rate(Seconds(1), Seconds(5), 256)
            .publish(|p: Publish| async move { Ok::<_, TestError>(p.ack()) })
            .control(move |msg| match msg {
                Control::ProtocolError(msg) => {
                    if let &error::ProtocolError::ReadTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(codec::Connect::default().client_id("user").into(), &codec).await.unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        codec::Publish { packet_id: Some(NonZeroU16::new(1).unwrap()), ..pkt_publish() }.into(),
        &codec,
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(500)).await;

    let mut buf = BytesMut::new();
    let pkt =
        codec::Publish { packet_id: Some(NonZeroU16::new(2).unwrap()), ..pkt_publish() }.into();
    codec.encode(pkt, &mut buf).unwrap();
    io.write(&buf[..5]).unwrap();
    sleep(Duration::from_millis(2000)).await;

    assert!(ka.load(Relaxed));
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos1() {
    let srv = server::test_server(move || {
        MqttServer::new(|con: Handshake| async move {
            let builder = con.sink().publish("test", Bytes::new()).properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            ntex::rt::spawn(async move {
                let res = builder.send_at_least_once().await;
                assert_eq!(
                    res,
                    Err(error::SendPacketError::Encode(error::EncodeError::OverMaxPacketSize))
                );
            });
            Ok(con.ack(St))
        })
        .publish(|p: Publish| async move {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(move |msg| match msg {
            Control::ProtocolError(msg) => Ready::Ok::<_, TestError>(msg.ack()),
            _ => Ready::Ok(msg.disconnect()),
        })
        .finish()
    });

    // connect to server
    let client = client::MqttConnector::new(srv.addr())
        .client_id("user")
        .max_packet_size(30)
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos0() {
    let srv = server::test_server(move || {
        MqttServer::new(|con: Handshake| async move {
            let builder = con.sink().publish("test", Bytes::new()).properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            let res = builder.send_at_most_once();
            assert_eq!(
                res,
                Err(error::SendPacketError::Encode(error::EncodeError::OverMaxPacketSize))
            );
            Ok(con.ack(St))
        })
        .publish(|p: Publish| async move {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(move |msg| match msg {
            Control::ProtocolError(msg) => Ready::Ok::<_, TestError>(msg.ack()),
            _ => Ready::Ok(msg.disconnect()),
        })
        .finish()
    });

    // connect to server
    let client = client::MqttConnector::new(srv.addr())
        .client_id("user")
        .max_packet_size(30)
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
}

/// Make sure we can publish message to client after local codec error
#[ntex::test]
async fn test_sink_success_after_encoder_error_qos1() {
    let success = Arc::new(AtomicBool::new(false));
    let success2 = success.clone();

    let srv = server::test_server(move || {
        let success = success2.clone();
        MqttServer::new(move |con: Handshake| {
            let sink = con.sink().clone();
            let success = success.clone();

            ntex::rt::spawn(async move {
                let builder = sink.publish("test", Bytes::new()).properties(|props| {
                    props.user_properties.push((
                        "ssssssssssssssssssssssssssssssssssss".into(),
                        "ssssssssssssssssssssssssssssssssssss".into(),
                    ));
                });
                let res = builder.send_at_least_once().await;
                assert_eq!(
                    res,
                    Err(error::SendPacketError::Encode(error::EncodeError::OverMaxPacketSize))
                );

                let res = sink.publish("test", Bytes::new()).send_at_least_once().await;
                assert!(res.is_ok());
                success.store(true, Relaxed);
            });
            Ready::Ok(con.ack(St))
        })
        .publish(|p: Publish| async move {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(move |msg| match msg {
            Control::ProtocolError(msg) => Ready::Ok::<_, TestError>(msg.ack()),
            _ => Ready::Ok(msg.disconnect()),
        })
        .finish()
    });

    // connect to server
    let client = client::MqttConnector::new(srv.addr())
        .client_id("user")
        .max_packet_size(30)
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    async fn publish(pkt: Publish) -> Result<PublishAck, TestError> {
        Ok(pkt.ack())
    }

    let router = client.resource("test", publish);
    ntex::rt::spawn(router.start_default());

    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    assert!(success.load(Relaxed));
}

#[ntex::test]
async fn test_request_problem_info() {
    let srv = server::test_server(move || {
        MqttServer::new(|con: Handshake| async move { Ok(con.ack(St)) })
            .publish(|p: Publish| async move {
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
            .finish()
    });

    // connect to server
    let client = client::MqttConnector::new(srv.addr())
        .client_id("user")
        .max_packet_size(30)
        .packet(|pkt| pkt.request_problem_info = false)
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"), Bytes::new())
        .send_at_least_once()
        .await
        .unwrap();
    assert!(res.properties.is_empty());
    assert!(res.reason_string.is_none());
}

#[ntex::test]
async fn test_suback_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .control(move |msg| match msg {
                Control::Subscribe(mut msg) => {
                    msg.iter_mut().for_each(|mut s| {
                        s.fail(codec::SubscribeAckReason::ImplementationSpecificError)
                    });
                    Ready::Ok::<_, TestError>(msg.ack_reason("some reason".into()).ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::new();
    io.send(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .await
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        codec::Packet::Subscribe(codec::Subscribe {
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
        }),
        &codec,
    )
    .await
    .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
        codec::Packet::SubscribeAck(codec::SubscribeAck {
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

    let srv = server::test_server(move || {
        let publish = publish2.clone();
        let disconnect = disconnect2.clone();
        MqttServer::new(handshake)
            .publish(move |p: Publish| {
                publish.store(true, Relaxed);
                Ready::Ok::<_, TestError>(p.ack())
            })
            .control(move |msg| match msg {
                Control::Disconnect(msg) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .unwrap();
    io.encode(pkt_publish().into(), &codec).unwrap();
    io.encode(
        codec::Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        }),
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

    let srv = server::test_server(move || {
        let publish = publish2.clone();
        let disconnect = disconnect2.clone();
        MqttServer::new(handshake)
            .max_qos(max_qos)
            .handle_qos_after_disconnect(handle_qos_after_disconnect)
            .publish(move |p: Publish| {
                publish.store(true, Relaxed);
                Ready::Ok::<_, TestError>(p.ack())
            })
            .control(move |msg| match msg {
                Control::Disconnect(msg) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok::<_, TestError>(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let packet_id = match publish_qos {
        QoS::AtMostOnce => None,
        _ => Some(NonZeroU16::new(1).unwrap()),
    };
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .unwrap();

    io.encode(
        codec::Publish {
            dup: false,
            retain: false,
            qos: publish_qos,
            topic: ByteString::from("test"),
            packet_id,
            payload: Bytes::new(),
            properties: Default::default(),
        }
        .into(),
        &codec,
    )
    .unwrap();

    io.encode(
        codec::Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        }),
        &codec,
    )
    .unwrap();
    io.flush(true).await.unwrap();
    sleep(Millis(1750)).await;
    io.close();
    drop(io);
    sleep(Millis(1000)).await;

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

    let srv = server::test_server(move || {
        let violated = violated2.clone();
        MqttServer::new(handshake)
            .max_qos(QoS::AtMostOnce)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .control(move |msg| {
                let violated = violated.clone();
                match msg {
                    Control::ProtocolError(msg) => {
                        if let error::ProtocolError::ProtocolViolation(_) = msg.get_ref() {
                            violated.store(true, Relaxed);
                        }
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                }
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.encode(pkt_publish().into(), &codec).unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt.0,
        codec::Packet::Disconnect(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::QosNotSupported,
            ..Default::default()
        })
    );
    assert!(violated.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_sink_ready() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(fn_service(|packet: Handshake| async move {
            let sink = packet.sink();
            let mut ready = Box::pin(sink.ready());
            let res = lazy(|cx| Pin::new(&mut ready).poll(cx)).await;
            assert!(res.is_pending());
            assert!(!sink.is_ready());

            ntex::rt::spawn(async move {
                sink.ready().await;
                assert!(sink.is_ready());
                sink.publish("/test", Bytes::from_static(b"body")).send_at_most_once().unwrap();
            });

            Ok::<_, TestError>(packet.ack(St))
        }))
        .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
        .finish()
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(
        codec::Packet::Connect(Box::new(codec::Connect::default().client_id("user"))),
        &codec,
    )
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        ack.0,
        codec::Packet::ConnectAck(Box::new(codec::ConnectAck {
            max_qos: QoS::AtLeastOnce,
            receive_max: NonZeroU16::new(15).unwrap(),
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
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let results = Rc::new(RefCell::new(Vec::new()));
    let results2 = results.clone();

    sink.publish_ack_cb(move |pkt, disconnected| {
        assert!(!disconnected);
        results2.borrow_mut().push(pkt.packet_id);
    });

    let res = sink
        .publish(ByteString::from_static("test1"), Bytes::new())
        .send_at_least_once_no_block();
    assert!(res.is_ok());

    let res = sink
        .publish(ByteString::from_static("test2"), Bytes::new())
        .send_at_least_once_no_block();
    assert!(res.is_ok());

    let res =
        sink.publish(ByteString::from_static("test3"), Bytes::new()).send_at_least_once().await;
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

    let srv = server::test_server(move || {
        let check = check2.clone();

        MqttServer::new(handshake)
            .frame_read_rate(Seconds(1), Seconds(2), 10)
            .publish(|p: Publish| Ready::Ok::<_, TestError>(p.ack()))
            .control(move |msg| {
                let check = check.clone();
                match msg {
                    Control::ProtocolError(msg) => {
                        if msg.get_ref() == &error::ProtocolError::ReadTimeout {
                            check.store(true, Relaxed);
                        }
                        Ready::Ok::<_, TestError>(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                }
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(codec::Connect::default().client_id("user").into(), &codec).unwrap();
    io.recv(&codec).await.unwrap();

    let p = codec::Publish {
        dup: false,
        retain: false,
        qos: codec::QoS::AtLeastOnce,
        topic: ByteString::from("test"),
        packet_id: Some(NonZeroU16::new(3).unwrap()),
        payload: Bytes::from(vec![b'*'; 270 * 1024]),
        ..pkt_publish()
    }
    .into();

    let mut buf = BytesMut::new();
    codec.encode(p, &mut buf).unwrap();

    io.write(&buf[..5]).unwrap();
    buf.split_to(5);
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
