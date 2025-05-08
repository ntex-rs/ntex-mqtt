use std::sync::{atomic::AtomicBool, atomic::Ordering::Relaxed, Arc, Mutex};
use std::{cell::RefCell, future::Future, num::NonZeroU16, pin::Pin, rc::Rc, time::Duration};

use ntex::service::{fn_service, Pipeline, ServiceFactory};
use ntex::time::{sleep, Millis, Seconds};
use ntex::util::{join_all, lazy, ByteString, Bytes, BytesMut, Ready};
use ntex::{codec::Encoder, server, service::chain_factory};

use ntex_mqtt::v3::codec::{self, Decoded, Encoded, Packet};
use ntex_mqtt::v3::{
    self, client, Control, Handshake, HandshakeAck, MqttServer, Publish, Session,
};
use ntex_mqtt::{error::ProtocolError, QoS};

struct St;

async fn handshake(mut packet: Handshake) -> Result<HandshakeAck<St>, ()> {
    packet.packet();
    packet.packet_mut();
    packet.io();
    packet.sink();
    Ok(packet.ack(St, false).idle_timeout(Seconds(16)))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv =
        server::test_server(|| MqttServer::new(handshake).publish(|_t| Ready::Ok(())).finish());

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

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

    let srv = server::test_server(move || {
        let chunks = chunks2.clone();
        MqttServer::new(handshake)
            .min_chunk_size(4)
            .publish(move |p: Publish| {
                let chunks = chunks.clone();
                async move {
                    while let Ok(Some(chunk)) = p.read().await {
                        chunks.lock().unwrap().push(chunk);
                    }
                    Ok(())
                }
            })
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

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

    let srv = server::test_server(move || {
        let chunks = chunks2.clone();
        MqttServer::new(handshake)
            .min_chunk_size(4)
            .publish(move |mut p: Publish| {
                let chunks = chunks.clone();
                async move {
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
                    Ok(())
                }
            })
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

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

    assert_eq!(&chunks.lock().unwrap()[..], vec![Bytes::from_static(b"1111111111"),]);
}

#[ntex::test]
async fn test_connect_fail() -> std::io::Result<()> {
    // bad user name or password
    let srv = server::test_server(|| {
        MqttServer::new(|conn: Handshake| Ready::Ok::<_, ()>(conn.bad_username_or_pwd::<St>()))
            .publish(|_t| Ready::Ok(()))
            .finish()
    });
    let err =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.err().unwrap();
    if let client::ClientError::Ack(codec::ConnectAck { session_present, return_code }) = err {
        assert!(!session_present);
        assert_eq!(return_code, codec::ConnectAckReason::BadUserNameOrPassword);
    }

    // identifier rejected
    let srv = server::test_server(|| {
        MqttServer::new(|conn: Handshake| Ready::Ok::<_, ()>(conn.identifier_rejected::<St>()))
            .publish(|_t| Ready::Ok(()))
            .finish()
    });
    let err =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.err().unwrap();
    if let client::ClientError::Ack(codec::ConnectAck { session_present, return_code }) = err {
        assert!(!session_present);
        assert_eq!(return_code, codec::ConnectAckReason::IdentifierRejected);
    }

    // not authorized
    let srv = server::test_server(|| {
        MqttServer::new(|conn: Handshake| Ready::Ok::<_, ()>(conn.not_authorized::<St>()))
            .publish(|_t| Ready::Ok(()))
            .finish()
    });
    let err =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.err().unwrap();
    if let client::ClientError::Ack(codec::ConnectAck { session_present, return_code }) = err {
        assert!(!session_present);
        assert_eq!(return_code, codec::ConnectAckReason::NotAuthorized);
    }

    // service unavailable
    let srv = server::test_server(|| {
        MqttServer::new(|conn: Handshake| Ready::Ok::<_, ()>(conn.service_unavailable::<St>()))
            .publish(|_t| Ready::Ok(()))
            .finish()
    });
    let err =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.err().unwrap();
    if let client::ClientError::Ack(codec::ConnectAck { session_present, return_code }) = err {
        assert!(!session_present);
        assert_eq!(return_code, codec::ConnectAckReason::ServiceUnavailable);
    }

    Ok(())
}

#[ntex::test]
async fn test_qos2() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::test_server(move || {
        let release = release2.clone();
        MqttServer::new(handshake)
            .publish(|_| Ready::Ok(()))
            .max_qos(QoS::ExactlyOnce)
            .control(move |msg| match msg {
                Control::PublishRelease(msg) => {
                    release.store(true, Relaxed);
                    Ready::Ok(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(codec::Connect::default().client_id("user").into())),
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
    assert_eq!(result, Decoded::Packet(Packet::PublishReceived { packet_id: id }, 2));

    io.send(Encoded::Packet(Packet::PublishRelease { packet_id: id }), &codec).await.unwrap();
    let result = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(result, Decoded::Packet(Packet::PublishComplete { packet_id: id }, 2));

    assert!(release.load(Relaxed));
    Ok(())
}

#[ntex::test]
async fn test_qos2_client() -> std::io::Result<()> {
    let release = Arc::new(AtomicBool::new(false));
    let release2 = release.clone();

    let srv = server::test_server(move || {
        let release = release2.clone();
        MqttServer::new(handshake)
            .publish(|_| Ready::Ok(()))
            .max_qos(QoS::ExactlyOnce)
            .control(move |msg| match msg {
                Control::PublishRelease(msg) => {
                    release.store(true, Relaxed);
                    Ready::Ok(msg.ack())
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
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(move || {
        let ping = ping2.clone();
        MqttServer::new(handshake)
            .publish(|_| Ready::Ok(()))
            .control(move |msg| {
                let ping = ping.clone();
                match msg {
                    Control::Ping(msg) => {
                        ping.store(true, Relaxed);
                        Ready::Ok(msg.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                }
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(codec::Connect::default().client_id("user").into())),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    io.send(Encoded::Packet(codec::Packet::PingRequest), &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, Decoded::Packet(Packet::PingResponse, 0));
    assert!(ping.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .publish(|_| async {
                sleep(Duration::from_millis(100)).await;
                Ok::<_, ()>(())
            })
            .control(move |msg| match msg {
                Control::Subscribe(mut msg) => {
                    for mut sub in &mut msg {
                        assert_eq!(sub.qos(), codec::QoS::AtLeastOnce);
                        sub.topic();
                        sub.subscribe(codec::QoS::AtLeastOnce);
                    }
                    Ready::Ok(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt,
        Decoded::Packet(Packet::PublishAck { packet_id: NonZeroU16::new(1).unwrap() }, 2)
    );

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
    assert_eq!(
        pkt,
        Decoded::Packet(Packet::PublishAck { packet_id: NonZeroU16::new(3).unwrap() }, 2)
    );

    Ok(())
}

#[ntex::test]
async fn test_ack_order_sink() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .publish(|_| async {
                sleep(Duration::from_millis(100)).await;
                Ok::<_, ()>(())
            })
            .finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();
    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let topic = ByteString::from_static("test");
    let fut1 = sink.publish(topic.clone()).send_at_least_once(Bytes::from_static(b"pkt1"));
    let fut2 = sink.publish(topic.clone()).send_at_least_once(Bytes::from_static(b"pkt2"));
    let fut3 = sink.publish(topic.clone()).send_at_least_once(Bytes::from_static(b"pkt3"));

    let res = join_all(vec![fut1, fut2, fut3]).await;
    assert!(res[0].is_ok());
    assert!(res[1].is_ok());
    assert!(res[2].is_ok());

    Ok(())
}

#[ntex::test]
async fn test_disconnect() -> std::io::Result<()> {
    let srv = server::test_server(|| {
        MqttServer::new(handshake)
            .publish(ntex::service::fn_factory_with_config(|session: Session<St>| {
                Ready::Ok(ntex::service::fn_service(move |_: Publish| {
                    session.sink().force_close();
                    async {
                        sleep(Duration::from_millis(100)).await;
                        Ok(())
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

    let res = sink.publish(ByteString::from_static("#")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_err());

    Ok(())
}

#[ntex::test]
async fn test_client_disconnect() -> std::io::Result<()> {
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::test_server(move || {
        let disconnect = disconnect2.clone();

        MqttServer::new(handshake)
            .publish(ntex::service::fn_factory_with_config(|_: Session<St>| {
                Ready::Ok(ntex::service::fn_service(move |_: Publish| async { Ok(()) }))
            }))
            .control(move |msg| match msg {
                Control::Disconnect(msg) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok(msg.ack())
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

    let res =
        sink.publish(ByteString::from_static("test")).send_at_least_once(Bytes::new()).await;
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

    let srv = server::test_server(move || {
        let publish = publish2.clone();
        let disconnect = disconnect2.clone();
        MqttServer::new(handshake)
            .publish(move |_| {
                publish.store(true, Relaxed);
                async {
                    sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            })
            .control(move |msg| match msg {
                Control::Disconnect(msg) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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
    io.encode(Encoded::Packet(Packet::Disconnect), &codec).unwrap();
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

    let srv = server::test_server(move || {
        let publish = publish2.clone();
        let disconnect = disconnect2.clone();
        MqttServer::new(handshake)
            .max_qos(max_qos)
            .handle_qos_after_disconnect(handle_qos_after_disconnect)
            .publish(move |_| {
                publish.store(true, Relaxed);
                async {
                    sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
            })
            .control(move |msg| match msg {
                Control::Disconnect(msg) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok(msg.ack())
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
    io.encode(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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
    io.encode(Encoded::Packet(Packet::Disconnect), &codec).unwrap();
    io.flush(true).await.unwrap();
    sleep(Millis(1750)).await;
    io.close();
    drop(io);
    sleep(Millis(750)).await;

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
    let srv = server::test_server(move || {
        MqttServer::new(handshake)
            .publish(|_| Ready::Ok(()))
            .control(move |msg| match msg {
                Control::Disconnect(_) => Ready::Err(()),
                Control::Error(_) => Ready::Err(()),
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    // disconnect
    io.send(Encoded::Packet(Packet::Disconnect), &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

#[ntex::test]
async fn test_large_publish() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(handshake).publish(|_| Ready::Ok(())).finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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
    assert!(result.is_ok());

    Ok(())
}

fn ssl_acceptor() -> openssl::ssl::SslAcceptor {
    use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

    // load ssl keys
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder.set_private_key_file("./tests/key.pem", SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file("./tests/cert.pem").unwrap();
    builder.build()
}

#[ntex::test]
async fn test_large_publish_openssl() -> std::io::Result<()> {
    use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};

    let srv = server::test_server(move || {
        chain_factory(server::openssl::SslAcceptor::new(ssl_acceptor()).map_err(|_| ()))
            .and_then(
                MqttServer::new(handshake)
                    .publish(|_| Ready::Ok(()))
                    .finish()
                    .map_err(|_| ())
                    .map_init_err(|_| ()),
            )
    });

    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let con = Pipeline::new(ntex::connect::openssl::SslConnector::new(builder.build()));
    let addr = format!("127.0.0.1:{}", srv.addr().port());
    let io = con.call(addr.into()).await.unwrap();

    let codec = codec::Codec::default();
    io.encode(Encoded::Packet(codec::Connect::default().client_id("user").into()), &codec)
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
    assert!(result.is_ok());

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
            .publish(|_| Ready::Ok(()))
            .control(move |msg| {
                let violated = violated.clone();
                match msg {
                    Control::ProtocolError(err) => {
                        if let ProtocolError::ProtocolViolation(_) = err.get_ref() {
                            violated.store(true, Relaxed);
                        }
                        Ready::Ok(err.ack())
                    }
                    _ => Ready::Ok(msg.disconnect()),
                }
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(codec::Connect::default().client_id("user").into())),
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
                sink.publish("/test").send_at_most_once(Bytes::from_static(b"body")).unwrap();
            });

            Ok::<_, ()>(packet.ack(St, false).idle_timeout(Seconds(16)))
        }))
        .publish(|_| Ready::Ok(()))
        .finish()
    });

    // connect to server
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(Packet::Connect(codec::Connect::default().client_id("user").into())),
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
    let srv = server::test_server(move || {
        MqttServer::new(handshake).publish(|_| Ready::Ok(())).finish()
    });

    // connect to server
    let client =
        client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();

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

    let srv = server::test_server(move || {
        let check = check2.clone();

        MqttServer::new(handshake)
            .min_chunk_size(32 * 1024)
            .frame_read_rate(Seconds(1), Seconds(2), 10)
            .publish(|p: v3::Publish| async move {
                let _ = p.read_all().await;
                Ok(())
            })
            .control(move |msg| {
                let check = check.clone();
                match msg {
                    Control::ProtocolError(msg) => {
                        if msg.get_ref() == &ProtocolError::ReadTimeout {
                            check.store(true, Relaxed);
                        }
                        Ready::Ok(msg.ack())
                    }
                    _ => Ready::Ok(msg.ack()),
                }
            })
            .finish()
            .map_err(|_| ())
            .map_init_err(|_| ())
    });

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
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );

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
