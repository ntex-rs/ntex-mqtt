use std::sync::{atomic::AtomicBool, atomic::Ordering::Relaxed, Arc};
use std::{convert::TryFrom, num::NonZeroU16, time::Duration};

use bytes::Bytes;
use bytestring::ByteString;
use futures::{future::ok, FutureExt, SinkExt, StreamExt};
use ntex::rt::time::delay_for;
use ntex::server;
use ntex_codec::Framed;

use ntex_mqtt::v5::{
    client, codec, error, Connect, ConnectAck, ControlMessage, MqttServer, Publish, PublishAck,
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

async fn connect<Io>(packet: Connect<Io>) -> Result<ConnectAck<Io, St>, TestError> {
    Ok(packet.ack(St))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
    env_logger::init();

    let srv = server::test_server(|| {
        MqttServer::new(connect).publish(|p: Publish| ok::<_, TestError>(p.ack())).finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());

    sink.close();
    Ok(())
}

#[ntex::test]
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(move || {
        let ping = ping2.clone();
        MqttServer::new(connect)
            .publish(|p: Publish| ok::<_, TestError>(p.ack()))
            .control(move |msg| {
                let ping = ping.clone();
                match msg {
                    ControlMessage::Ping(msg) => {
                        ping.store(true, Relaxed);
                        ok::<_, TestError>(msg.ack())
                    }
                    _ => ok(msg.disconnect_with(codec::Disconnect::default())),
                }
            })
            .finish()
    });

    let io = srv.connect().unwrap();
    let mut framed = Framed::new(io, codec::Codec::new());
    framed
        .send(codec::Packet::Connect(codec::Connect::default().client_id("user")))
        .await
        .unwrap();
    let _ = framed.next().await.unwrap().unwrap();

    framed.send(codec::Packet::PingRequest).await.unwrap();
    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(pkt, codec::Packet::PingResponse);
    assert!(ping.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(connect)
            .publish(|p: Publish| {
                delay_for(Duration::from_millis(100)).map(move |_| Ok::<_, TestError>(p.ack()))
            })
            .control(move |msg| match msg {
                ControlMessage::Subscribe(mut msg) => {
                    for mut sub in &mut msg {
                        sub.topic();
                        sub.options();
                        sub.subscribe(codec::QoS::AtLeastOnce);
                    }
                    ok::<_, TestError>(msg.ack())
                }
                _ => ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().unwrap();
    let mut framed = Framed::new(io, codec::Codec::default());
    framed
        .send(codec::Packet::Connect(codec::Connect::default().client_id("user")))
        .await
        .unwrap();
    let _ = framed.next().await.unwrap().unwrap();

    framed
        .send(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                payload: Bytes::new(),
                properties: Default::default(),
            }
            .into(),
        )
        .await
        .unwrap();
    framed
        .send(
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
        )
        .await
        .unwrap();

    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(
        pkt,
        codec::Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::Success,
            properties: Default::default(),
            reason_string: None,
        })
    );

    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(
        pkt,
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
        MqttServer::new(connect)
            .publish(|p: Publish| {
                delay_for(Duration::from_millis(10000))
                    .map(move |_| Ok::<_, TestError>(p.ack()))
            })
            .finish()
    });

    let io = srv.connect().unwrap();
    let mut framed = Framed::new(io, codec::Codec::default());
    framed
        .send(codec::Packet::Connect(
            codec::Connect::default().client_id("user").receive_max(2),
        ))
        .await
        .unwrap();
    let _ = framed.next().await.unwrap().unwrap();

    framed
        .send(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                payload: Bytes::new(),
                properties: Default::default(),
            }
            .into(),
        )
        .await
        .unwrap();

    // send packet_id dup
    framed
        .send(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                payload: Bytes::new(),
                properties: Default::default(),
            }
            .into(),
        )
        .await
        .unwrap();

    // send subscribe dup
    framed
        .send(
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
        )
        .await
        .unwrap();

    // send unsubscribe dup
    framed
        .send(
            codec::Unsubscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                user_properties: Default::default(),
                topic_filters: vec![ByteString::from("topic1")],
            }
            .into(),
        )
        .await
        .unwrap();

    // PublishAck
    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(
        pkt,
        codec::Packet::PublishAck(codec::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            reason_code: codec::PublishAckReason::PacketIdentifierInUse,
            properties: Default::default(),
            reason_string: None,
        })
    );

    // SubscribeAck
    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(
        pkt,
        codec::SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: Default::default(),
            reason_string: None,
            status: vec![codec::SubscribeAckReason::PacketIdentifierInUse],
        }
        .into()
    );

    // UnsubscribeAck
    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(
        pkt,
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
        MqttServer::new(connect)
            .receive_max(1)
            .publish(|p: Publish| {
                delay_for(Duration::from_millis(10000))
                    .map(move |_| Ok::<_, TestError>(p.ack()))
            })
            .control(move |msg| match msg {
                ControlMessage::ProtocolError(msg) => ok::<_, TestError>(msg.ack()),
                _ => ok(msg.disconnect()),
            })
            .finish()
    });
    let io = srv.connect().unwrap();
    let mut framed = Framed::new(io, codec::Codec::default());
    framed
        .send(codec::Packet::Connect(codec::Connect::default().client_id("user")))
        .await
        .unwrap();
    let _ = framed.next().await.unwrap().unwrap();
    framed
        .send(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                payload: Bytes::new(),
                properties: Default::default(),
            }
            .into(),
        )
        .await
        .unwrap();
    framed
        .send(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(2).unwrap()),
                payload: Bytes::new(),
                properties: Default::default(),
            }
            .into(),
        )
        .await
        .unwrap();
    let pkt = framed.next().await.unwrap().unwrap();
    assert_eq!(
        pkt,
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
async fn test_encoder_error() {
    std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
    env_logger::init();

    let srv = server::test_server(move || {
        MqttServer::new(|con: Connect<_>| async move {
            let builder = con.sink().publish("test", Bytes::new()).properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            ntex::rt::spawn(async move {
                println!("0000000000000");
                let res = builder.send_at_least_once().await;
                println!("0000000000001");
                assert_eq!(
                    res,
                    Err(error::PublishQos1Error::Encode(error::EncodeError::InvalidLength))
                );
            });
            Ok(con.ack(St))
        })
        .publish(|p: Publish| {
            delay_for(Duration::from_millis(50)).map(move |_| Ok::<_, TestError>(p.ack()))
        })
        .control(move |msg| match msg {
            ControlMessage::ProtocolError(msg) => ok::<_, TestError>(msg.ack()),
            _ => ok(msg.disconnect()),
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
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
}
