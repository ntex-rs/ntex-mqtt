use super::*;

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

    let (io, codec) = handshake(&srv).await;

    let id = pid(1);
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
        io.send(
            Encoded::Packet(Packet::PublishReceived(codec::PublishAck {
                packet_id: pid(id),
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
                packet_id: pid(id),
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

    let (io, codec) = handshake(&srv).await;

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
            packet_id: pid(1),
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
            packet_id: pid(1),
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

    let (io, codec) = handshake(&srv).await;

    let id = pid(1);
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
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let received = sink
        .publish(ByteString::from_static("test"))
        .send_exactly_once(Bytes::new())
        .await
        .unwrap();
    assert_eq!(received.packet().packet_id, pid(1));
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

    let client = connect_client(srv.addr()).await;
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
                        Ok::<_, TestError>(p.ack())
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

#[ntex::test]
async fn test_ack_order() -> std::io::Result<()> {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(100)).await;
            Ok::<_, TestError>(p.ack())
        })
        .protocol(async move |msg| match msg {
            ProtocolMessage::Ping(msg) => Ok(msg.ack()),
            ProtocolMessage::Auth(msg) => Ok(msg.ack(codec::Auth {
                auth_method: Some(ByteString::from_static("m")),
                ..Default::default()
            })),
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

    let (io, codec) = handshake(&srv).await;

    io.send(Encoded::Publish(pkt_publish(), None), &codec)
        .await
        .unwrap();
    io.send(
        Encoded::Packet(
            codec::Subscribe {
                id: None,
                packet_id: pid(2),
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
    io.send(
        Encoded::Packet(
            codec::Auth {
                reason_code: codec::AuthReasonCode::ReAuth,
                auth_method: Some(ByteString::from_static("m")),
                ..Default::default()
            }
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();

    // subscribe, ping and auth responses do not wait for publish acks
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::SubscribeAck(codec::SubscribeAck {
            packet_id: pid(2),
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
        Packet::from(codec::Auth {
            auth_method: Some(ByteString::from_static("m")),
            ..Default::default()
        })
    );

    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::PublishAck(codec::PublishAck {
            packet_id: pid(1),
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

    io.send(Encoded::Publish(pkt_publish(), None), &codec)
        .await
        .unwrap();

    // send packet_id dup
    io.send(Encoded::Publish(pkt_publish(), None), &codec)
        .await
        .unwrap();

    // send subscribe dup
    io.send(
        Encoded::Packet(
            codec::Subscribe {
                id: None,
                packet_id: pid(1),
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
                packet_id: pid(1),
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
            packet_id: pid(1),
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
            packet_id: pid(1),
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
            packet_id: pid(1),
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
            packet_id: pid(1),
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

    let (io, codec) = connect_raw(&srv).await;
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(ack),
        Packet::ConnectAck(Box::new(codec::ConnectAck {
            receive_max: pid(1),
            max_qos: codec::QoS::AtLeastOnce,
            reason_code: codec::ConnectAckReason::Success,
            topic_alias_max: 32,
            server_keepalive_sec: Some(30),
            max_packet_size: Some(256 * 1024),
            ..Default::default()
        }))
    );

    io.send(Encoded::Publish(pkt_publish(), None), &codec)
        .await
        .unwrap();
    io.send(
        Encoded::Publish(
            codec::Publish {
                packet_id: Some(pid(2)),
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
        Packet::Disconnect(Box::new(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        }))
    );
}

#[ntex::test]
async fn test_publish_error_to_ack() {
    let srv = server::TestServerBuilder::new(async || {
        MqttServer::new(async |p: Publish| {
            Err::<PublishAck, _>(match p.publish_topic() {
                "fatal" => AckError(codec::PublishAckReason::UnspecifiedError),
                _ => AckError(codec::PublishAckReason::QuotaExceeded),
            })
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    // a convertible error is reported with a PUBLISH ack
    let (io, codec) = handshake(&srv).await;
    for (id, qos) in [(1u16, QoS::AtLeastOnce), (2, QoS::ExactlyOnce)] {
        let pkt = pkt_publish_to("t", qos, Some(id));
        let ack = codec::PublishAck {
            packet_id: pid(id),
            reason_code: codec::PublishAckReason::QuotaExceeded,
            properties: Default::default(),
            reason_string: Some(ByteString::from_static("converted")),
        };
        let res = send_recv(&io, &codec, Encoded::Publish(pkt, None)).await;
        assert_eq!(
            res,
            if qos == QoS::AtLeastOnce {
                Packet::PublishAck(ack)
            } else {
                Packet::PublishReceived(ack)
            }
        );
    }

    // an error that cannot be converted terminates the connection
    for (topic, qos, id) in [
        ("fatal", QoS::AtLeastOnce, Some(5u16)),
        ("t", QoS::AtMostOnce, None),
    ] {
        let (io, codec) = handshake(&srv).await;
        let pkt = pkt_publish_to(topic, qos, id);
        assert_eq!(
            send_recv(&io, &codec, Encoded::Publish(pkt, None)).await,
            Packet::Disconnect(Box::new(codec::Disconnect::new(
                codec::DisconnectReasonCode::ImplementationSpecificError
            )))
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }
}

#[ntex::test]
async fn test_publish_wrapped_error_to_ack() {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| {
            Err::<PublishAck, ntex_error::Error<AckError>>(
                match p.publish_topic() {
                    "fatal" => AckError(codec::PublishAckReason::UnspecifiedError),
                    _ => AckError(codec::PublishAckReason::NotAuthorized),
                }
                .into(),
            )
        })
        .build(connect)
    });

    // `ToPublishAck` is forwarded through the `Error<E>` wrapper
    let (io, codec) = handshake(&srv).await;
    let pkt = pkt_publish_to("t", QoS::AtLeastOnce, Some(1));
    assert_eq!(
        send_recv(&io, &codec, Encoded::Publish(pkt, None)).await,
        Packet::PublishAck(codec::PublishAck {
            packet_id: pid(1),
            reason_code: codec::PublishAckReason::NotAuthorized,
            properties: Default::default(),
            reason_string: Some(ByteString::from_static("converted")),
        })
    );

    // a QoS0 publish cannot be acked, the wrapped error closes the connection
    for topic in ["t", "fatal"] {
        let (io, codec) = handshake(&srv).await;
        let pkt = pkt_publish_to(topic, QoS::AtMostOnce, None);
        assert_eq!(
            send_recv(&io, &codec, Encoded::Publish(pkt, None)).await,
            Packet::Disconnect(Box::new(codec::Disconnect::new(
                codec::DisconnectReasonCode::ImplementationSpecificError
            ))),
            "{topic}"
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }

    let pkt = pkt_publish_to("fatal", QoS::AtLeastOnce, Some(2));
    assert_eq!(
        send_recv(&io, &codec, Encoded::Publish(pkt, None)).await,
        Packet::Disconnect(Box::new(codec::Disconnect::new(
            codec::DisconnectReasonCode::ImplementationSpecificError
        )))
    );
}
