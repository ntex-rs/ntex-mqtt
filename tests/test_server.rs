use std::sync::{atomic::AtomicBool, atomic::Ordering::Relaxed, Arc};
use std::{num::NonZeroU16, time::Duration};

use ntex::service::{Service, ServiceFactory};
use ntex::time::{sleep, Millis, Seconds};
use ntex::util::{join_all, ByteString, Bytes, Ready};
use ntex::{server, service::pipeline_factory};

use ntex_mqtt::v3::{
    client, codec, ControlMessage, Handshake, HandshakeAck, MqttServer, Publish, Session,
};

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

    let timeout = Millis(1_000);
    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once(timeout).await;
    assert!(res.is_ok());

    sink.close();
    Ok(())
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
    if let client::ClientError::Ack { session_present, return_code } = err {
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
    if let client::ClientError::Ack { session_present, return_code } = err {
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
    if let client::ClientError::Ack { session_present, return_code } = err {
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
    if let client::ClientError::Ack { session_present, return_code } = err {
        assert!(!session_present);
        assert_eq!(return_code, codec::ConnectAckReason::ServiceUnavailable);
    }

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
                    ControlMessage::Ping(msg) => {
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
    io.send(codec::Packet::Connect(codec::Connect::default().client_id("user").into()), &codec)
        .await
        .unwrap();
    io.recv(&codec).await.unwrap().unwrap();

    io.send(codec::Packet::PingRequest, &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, codec::Packet::PingResponse);
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
                ControlMessage::Subscribe(mut msg) => {
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
    io.send(codec::Connect::default().client_id("user").into(), &codec).await.unwrap();
    let _ = io.recv(&codec).await.unwrap().unwrap();

    io.send(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(1).unwrap()),
            payload: Bytes::new(),
        }
        .into(),
        &codec,
    )
    .await
    .unwrap();
    io.send(
        codec::Packet::Subscribe {
            packet_id: NonZeroU16::new(2).unwrap(),
            topic_filters: vec![(ByteString::from("topic1"), codec::QoS::AtLeastOnce)],
        },
        &codec,
    )
    .await
    .unwrap();
    io.send(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload: Bytes::new(),
        }
        .into(),
        &codec,
    )
    .await
    .unwrap();

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, codec::Packet::PublishAck { packet_id: NonZeroU16::new(1).unwrap() });

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        pkt,
        codec::Packet::SubscribeAck {
            packet_id: NonZeroU16::new(2).unwrap(),
            status: vec![codec::SubscribeReturnCode::Success(codec::QoS::AtLeastOnce)],
        }
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(pkt, codec::Packet::PublishAck { packet_id: NonZeroU16::new(3).unwrap() });

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

    let timeout = Millis(1_000);
    let topic = ByteString::from_static("test");
    let fut1 = sink.publish(topic.clone(), Bytes::from_static(b"pkt1")).send_at_least_once(timeout);
    let fut2 = sink.publish(topic.clone(), Bytes::from_static(b"pkt2")).send_at_least_once(timeout);
    let fut3 = sink.publish(topic.clone(), Bytes::from_static(b"pkt3")).send_at_least_once(timeout);

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

    let timeout = Millis(1_000);
    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once(timeout).await;
    assert!(res.is_err());

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
                ControlMessage::Disconnect(msg) => {
                    disconnect.store(true, Relaxed);
                    Ready::Ok(msg.ack())
                }
                _ => Ready::Ok(msg.disconnect()),
            })
            .finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(codec::Connect::default().client_id("user").into(), &codec).unwrap();
    io.encode(
        codec::Publish {
            dup: false,
            retain: false,
            qos: codec::QoS::AtLeastOnce,
            topic: ByteString::from("test"),
            packet_id: Some(NonZeroU16::new(3).unwrap()),
            payload: Bytes::new(),
        }
        .into(),
        &codec,
    )
    .unwrap();
    io.encode(codec::Packet::Disconnect, &codec).unwrap();
    io.flush(true).await.unwrap();
    sleep(Millis(50)).await;
    drop(io);
    sleep(Millis(50)).await;

    assert!(publish.load(Relaxed));
    assert!(disconnect.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_large_publish() -> std::io::Result<()> {
    let srv = server::test_server(move || {
        MqttServer::new(handshake).publish(|_| Ready::Ok(())).finish()
    });

    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode(codec::Connect::default().client_id("user").into(), &codec).unwrap();
    let _ = io.recv(&codec).await;

    let p = codec::Publish {
        dup: false,
        retain: false,
        qos: codec::QoS::AtLeastOnce,
        topic: ByteString::from("test"),
        packet_id: Some(NonZeroU16::new(3).unwrap()),
        payload: Bytes::from(vec![b'*'; 270 * 1024]),
    }
    .into();
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
    env_logger::init();

    let srv = server::test_server(move || {
        pipeline_factory(server::openssl::Acceptor::new(ssl_acceptor()).map_err(|_| ()))
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
    let con = ntex::connect::openssl::Connector::new(builder.build());
    let addr = format!("127.0.0.1:{}", srv.addr().port());
    let io = con.call(addr.into()).await.unwrap();

    let codec = codec::Codec::default();
    io.encode(codec::Connect::default().client_id("user").into(), &codec).unwrap();
    let _ = io.recv(&codec).await;

    let p = codec::Publish {
        dup: false,
        retain: false,
        qos: codec::QoS::AtLeastOnce,
        topic: ByteString::from("test"),
        packet_id: Some(NonZeroU16::new(3).unwrap()),
        payload: Bytes::from(vec![b'*'; 270 * 1024]),
    }
    .into();
    let res = io.send(p, &codec).await;
    assert!(res.is_ok());
    let result = io.recv(&codec).await;
    assert!(result.is_ok());

    Ok(())
}
