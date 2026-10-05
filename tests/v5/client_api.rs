use super::*;

#[ntex::test]
async fn test_client_connect_packet() {
    let seen = Arc::new(Mutex::new(Vec::<codec::Connect>::new()));
    let seen2 = seen.clone();

    let srv = server::test_server(async move || {
        let seen = seen2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |mut msg: Connect| {
                assert!(msg.packet_size() > 0);
                assert!(!msg.io().tag().is_empty());
                assert_eq!(msg.st(), &());
                assert!(msg.sink().is_open());
                assert!(format!("{msg:?}").contains("client_id"));
                seen.lock().unwrap().push(msg.packet().clone());

                msg.packet_mut().client_id = ByteString::from_static("rewritten");
                assert_eq!(msg.packet().client_id, "rewritten");

                if msg.packet().username.as_deref() == Some("bad") {
                    return Ok(msg.fail_with(codec::ConnectAck {
                        reason_code: codec::ConnectAckReason::Banned,
                        reason_string: Some(ByteString::from_static("go away")),
                        ..Default::default()
                    }));
                }
                let ack = msg.ack(St).keep_alive(30).max_send(Some(0));
                assert!(format!("{ack:?}").contains("ConnectAck { packet"));
                Ok::<_, TestError>(ack)
            },
        )
    });

    let will = codec::LastWill {
        qos: QoS::AtLeastOnce,
        retain: true,
        topic: ByteString::from_static("will/topic"),
        message: Bytes::from_static(b"bye"),
        will_delay_interval_sec: Some(1),
        correlation_data: None,
        message_expiry_interval: None,
        content_type: None,
        user_properties: Default::default(),
        is_utf8_payload: None,
        response_topic: None,
    };

    let mut client = try_connect_client(
        client::Connect::with(srv.addr(), codec::Connect::default())
            .client_id("c1")
            .clean_start()
            .keep_alive(Seconds(10))
            .last_will(will)
            .auth(ByteString::from_static("m"), Bytes::from_static(b"d"))
            .username(ByteString::from_static("user"))
            .password(Bytes::from_static(b"pass"))
            .max_packet_size(1024)
            .max_receive(7)
            .properties(|props| {
                props.push((ByteString::from_static("k"), ByteString::from_static("v")))
            })
            .packet(|pkt| pkt.session_expiry_interval_secs = 5),
    )
    .await
    .unwrap();

    assert!(format!("{client:?}").contains("v5::Client"));
    assert!(!client.session_present());
    assert_eq!(
        client.packet().reason_code,
        codec::ConnectAckReason::Success
    );
    // server keep-alive is not sent, client value (10s) is lower than the server value (30s)
    assert!(client.packet().server_keepalive_sec.is_none());
    assert_eq!(client.packet().max_qos, QoS::AtLeastOnce);
    client.packet_mut().session_present = true;
    assert!(client.session_present());

    // connect packet as it is seen by the server
    let pkt = seen.lock().unwrap()[0].clone();
    assert_eq!(pkt.client_id, "c1");
    assert!(pkt.clean_start);
    assert_eq!(pkt.keep_alive, 10);
    let will = pkt.last_will.as_ref().unwrap();
    assert_eq!(will.topic, "will/topic");
    assert_eq!(will.message, Bytes::from_static(b"bye"));
    assert_eq!(pkt.auth_method.as_deref(), Some("m"));
    assert_eq!(pkt.auth_data, Some(Bytes::from_static(b"d")));
    assert_eq!(pkt.username.as_deref(), Some("user"));
    assert_eq!(pkt.password, Some(Bytes::from_static(b"pass")));
    assert_eq!(pkt.max_packet_size.map(|v| v.get()), Some(1024));
    assert_eq!(pkt.receive_max.map(|v| v.get()), Some(7));
    assert_eq!(pkt.session_expiry_interval_secs, 5);
    assert_eq!(
        pkt.user_properties,
        vec![(ByteString::from_static("k"), ByteString::from_static("v"))]
    );

    // zero values disable the corresponding properties, connect gets rejected
    let err = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("c2")
            .max_packet_size(0)
            .max_receive(0)
            .username(ByteString::from_static("bad")),
    )
    .await
    .err()
    .unwrap();
    match &*err {
        error::MqttClientError::Ack(ack) => {
            assert_eq!(ack.reason_code, codec::ConnectAckReason::Banned);
            assert_eq!(ack.reason_string.as_deref(), Some("go away"));
        }
        err => panic!("unexpected error: {err:?}"),
    }

    let pkt = seen.lock().unwrap()[1].clone();
    assert!(pkt.max_packet_size.is_none());
    assert!(pkt.receive_max.is_none());
}

/// Server that does not follow the protocol during the handshake
fn bad_handshake_server(kind: u8) -> server::TestServer {
    server::test_server(async move || {
        fn_service(async move |io: ntex::io::Io| {
            let codec = codec::Codec::default();
            let _ = io.recv(&codec).await;
            match kind {
                // close connection without CONNACK
                0 => (),
                // unexpected packet instead of CONNACK
                1 => {
                    let _ = io.send(Encoded::Packet(Packet::PingResponse), &codec).await;
                }
                // publish packet instead of CONNACK
                _ => {
                    let _ = io.send(Encoded::Publish(pkt_publish(), None), &codec).await;
                }
            }
            let _ = io.shutdown().await;
            Ok::<_, ()>(())
        })
    })
}

#[ntex::test]
async fn test_client_connector() {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
    });

    // custom underlying connector
    let connector = client::MqttConnector::new().connector(ntex::connect::Connector::default());
    assert!(format!("{connector:?}").contains("MqttConnector"));
    let client = Pipeline::new(SharedCfg::default(), connector)
        .call(client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    assert!(client.sink().is_open());

    // handshake failures
    for kind in 0..3u8 {
        let srv = bad_handshake_server(kind);
        let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
            .await
            .err()
            .unwrap();
        match (kind, &*err) {
            (0, error::MqttClientError::Disconnected(None)) => (),
            (1 | 2, error::MqttClientError::Protocol(_)) => (),
            (_, err) => panic!("unexpected error for {kind}: {err:?}"),
        }
    }
}

type EchoAcks = Arc<Mutex<Vec<Result<Option<codec::PublishAck>, error::SendPacketError>>>>;

/// Server that publishes a message back to the client. The topic and the retain flag
/// (requests topic alias `1`) are taken from the client's publish, the qos is taken
/// from the first byte of its payload.
fn echo_publisher() -> (server::TestServer, EchoAcks) {
    let acks: EchoAcks = Arc::new(Mutex::new(Vec::new()));
    let acks2 = acks.clone();

    let srv = server::test_server(async move || {
        let acks = acks2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let sink = con.sink().clone();
            let acks = acks.clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let qos = match p.read_all().await.unwrap()[0] {
                    0 => QoS::AtMostOnce,
                    1 => QoS::AtLeastOnce,
                    _ => QoS::ExactlyOnce,
                };
                let mut pkt = codec::Publish {
                    packet_id: None,
                    topic: ByteString::from(p.publish_topic()),
                    ..pkt_publish()
                };
                if p.retain() {
                    pkt.properties.topic_alias = Some(pid(1));
                }

                let (sink, acks) = (sink.clone(), acks.clone());
                rt::spawn(async move {
                    let payload = Bytes::from_static(b"body");
                    let builder = sink.publish_pkt(pkt);
                    let res = match qos {
                        QoS::AtMostOnce => builder.send_at_most_once(payload).await.map(|()| None),
                        QoS::AtLeastOnce => builder.send_at_least_once(payload).await.map(Some),
                        QoS::ExactlyOnce => match builder.send_exactly_once(payload).await {
                            Ok(rec) => {
                                let ack = rec.packet().clone();
                                rec.release().await.map(|()| Some(ack))
                            }
                            Err(err) => Err(err),
                        },
                    };
                    acks.lock().unwrap().push(res);
                });
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    });
    (srv, acks)
}

/// Requests a server publish to `topic` with `qos`, `alias` requests topic alias `1`
async fn trigger(sink: &client::MqttSink, topic: &str, qos: QoS, alias: bool) {
    sink.publish_pkt(codec::Publish {
        packet_id: None,
        retain: alias,
        ..pkt_publish_to(topic, QoS::AtMostOnce, None)
    })
    .send_at_most_once(Bytes::copy_from_slice(&[qos as u8]))
    .await
    .unwrap();
}

#[ntex::test]
async fn test_client_router() {
    let (srv, acks) = echo_publisher();

    // into_inner returns the negotiated io and codec
    let client = connect_client(srv.addr()).await;
    let (io, codec) = client
        .resource(
            "a",
            fn_service(async |p: Publish| Ok::<_, AckError>(p.ack())),
        )
        .into_inner();
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let routed = Rc::new(RefCell::new(Vec::<(String, Bytes)>::new()));
    let routed2 = routed.clone();
    let unrouted = Rc::new(RefCell::new(Vec::<String>::new()));
    let unrouted2 = unrouted.clone();

    let router = client
        .resource(
            "topic/{name}",
            fn_service(move |p: Publish| {
                let routed = routed2.clone();
                async move {
                    let name = p.topic().get("name").unwrap().to_owned();
                    let payload = p.read_all().await.unwrap();
                    routed.borrow_mut().push((name, payload));
                    Ok::<_, AckError>(p.ack())
                }
            }),
        )
        .resource(
            "fail",
            fn_service(async |_: Publish| {
                Err::<PublishAck, _>(AckError(codec::PublishAckReason::QuotaExceeded))
            }),
        )
        .resource(
            "err",
            fn_service(async |_: Publish| {
                Err::<PublishAck, _>(AckError(codec::PublishAckReason::UnspecifiedError))
            }),
        );
    assert!(format!("{router:?}").contains("v5::ClientRouter"));

    rt::spawn(async move {
        let _ = router
            .start(fn_service(move |msg: client::ProtocolMessage| {
                let unrouted = unrouted2.clone();
                async move {
                    Ok::<_, AckError>(match msg {
                        client::ProtocolMessage::Publish(p) => {
                            assert!(p.packet_size() > 0);
                            assert_eq!(p.payload_size(), 4);
                            assert_eq!(p.read().await.unwrap(), Some(Bytes::from_static(b"body")));
                            unrouted.borrow_mut().push(p.packet().topic.to_string());
                            p.ack_with(
                                codec::PublishAckReason::NoMatchingSubscribers,
                                codec::UserProperties::new(),
                                Some(ByteString::from_static("nope")),
                            )
                        }
                        msg => msg.ack(),
                    })
                }
            }))
            .await;
    });

    // routed publishes, the second one registers topic alias 1
    for (idx, (topic, alias)) in [("topic/abc", false), ("topic/xyz", true)]
        .into_iter()
        .enumerate()
    {
        trigger(&sink, topic, QoS::AtLeastOnce, alias).await;
        wait_until(|| acks.lock().unwrap().len() == idx + 1).await;
        let ack = acks.lock().unwrap()[idx].clone().unwrap().unwrap();
        assert_eq!(ack.reason_code, codec::PublishAckReason::Success, "{topic}");
    }
    assert_eq!(
        *routed.borrow(),
        vec![
            ("abc".to_owned(), Bytes::from_static(b"body")),
            ("xyz".to_owned(), Bytes::from_static(b"body"))
        ]
    );

    // handler error is converted into a publish ack
    trigger(&sink, "fail", QoS::AtLeastOnce, false).await;
    wait_until(|| acks.lock().unwrap().len() == 3).await;
    let ack = acks.lock().unwrap()[2].clone().unwrap().unwrap();
    assert_eq!(ack.reason_code, codec::PublishAckReason::QuotaExceeded);
    assert_eq!(ack.reason_string.as_deref(), Some("converted"));

    // unrouted publish is passed to the protocol service
    trigger(&sink, "other", QoS::AtLeastOnce, false).await;
    wait_until(|| acks.lock().unwrap().len() == 4).await;
    let ack = acks.lock().unwrap()[3].clone().unwrap().unwrap();
    assert_eq!(
        ack.reason_code,
        codec::PublishAckReason::NoMatchingSubscribers
    );
    assert_eq!(ack.reason_string.as_deref(), Some("nope"));
    assert_eq!(*unrouted.borrow(), vec!["other".to_owned()]);

    // handler error that cannot be converted closes the connection
    trigger(&sink, "err", QoS::AtLeastOnce, false).await;
    wait_until(|| !sink.is_open()).await;
}

#[ntex::test]
async fn test_client_router_default() {
    let (srv, acks) = echo_publisher();
    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let routed = Rc::new(RefCell::new(Vec::<String>::new()));
    let routed2 = routed.clone();

    rt::spawn(
        client
            .resource(
                "topic/{name}",
                fn_service(move |mut p: Publish| {
                    let routed = routed2.clone();
                    async move {
                        // the packet of a routed publish can be modified in place
                        assert!(!p.packet().retain);
                        p.packet_mut().retain = true;
                        routed.borrow_mut().push(format!(
                            "{}:{}",
                            p.publish_topic(),
                            p.packet().retain
                        ));
                        Ok::<_, AckError>(p.ack())
                    }
                }),
            )
            .start_default(),
    );

    trigger(&sink, "topic/one", QoS::AtLeastOnce, false).await;
    wait_until(|| !acks.lock().unwrap().is_empty()).await;
    assert_eq!(
        acks.lock().unwrap()[0]
            .clone()
            .unwrap()
            .unwrap()
            .reason_code,
        codec::PublishAckReason::Success
    );
    assert_eq!(*routed.borrow(), vec!["topic/one:true".to_owned()]);

    // a routed QoS0 publish is not acked
    trigger(&sink, "topic/zero", QoS::AtMostOnce, false).await;
    wait_until(|| routed.borrow().len() == 2).await;
    assert!(acks.lock().unwrap()[1].as_ref().unwrap().is_none());

    // default protocol service closes the connection on unrouted publish
    trigger(&sink, "other", QoS::AtLeastOnce, false).await;
    wait_until(|| !sink.is_open()).await;
}

#[ntex::test]
async fn test_client_protocol_message() {
    let (srv, acks) = echo_publisher();
    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let seen = Rc::new(RefCell::new(Vec::<String>::new()));
    let seen2 = seen.clone();

    rt::spawn(async move {
        let _ = client
            .start(fn_service(move |msg: client::ProtocolMessage| {
                let seen = seen2.clone();
                async move {
                    Ok::<_, ()>(match msg {
                        client::ProtocolMessage::Publish(mut p)
                            if p.packet().topic != "unsupported" =>
                        {
                            p.packet_mut().retain = true;
                            assert!(p.packet().retain);
                            seen.borrow_mut()
                                .push(format!("publish:{}", p.packet().topic));
                            match p.packet().topic.as_str() {
                                // QoS 0 publish, no ack is sent
                                "qos0" => p.ack_qos0(),
                                "inner" => {
                                    let (ack, pkt) = p.into_inner(codec::PublishAckReason::Success);
                                    assert_eq!(pkt.topic, "inner");
                                    ack
                                }
                                _ => p.ack(codec::PublishAckReason::NotAuthorized),
                            }
                        }
                        // publish is not supported by ProtocolMessage::ack(),
                        // the client disconnects
                        msg => msg.ack(),
                    })
                }
            }))
            .await;
    });

    // QoS 0 publish is not acknowledged, QoS 1 publish ack carries the reason code
    trigger(&sink, "qos0", QoS::AtMostOnce, false).await;
    trigger(&sink, "inner", QoS::AtLeastOnce, false).await;
    trigger(&sink, "other", QoS::AtLeastOnce, false).await;
    wait_until(|| acks.lock().unwrap().len() == 3).await;
    let acks = acks.lock().unwrap().clone();
    assert!(acks[0].clone().unwrap().is_none());
    assert_eq!(
        acks[1].clone().unwrap().unwrap().reason_code,
        codec::PublishAckReason::Success
    );
    assert_eq!(
        acks[2].clone().unwrap().unwrap().reason_code,
        codec::PublishAckReason::NotAuthorized
    );
    assert_eq!(
        *seen.borrow(),
        vec!["publish:qos0", "publish:inner", "publish:other"]
    );

    // ProtocolMessage::ack() for a publish is not supported, client disconnects
    trigger(&sink, "unsupported", QoS::AtLeastOnce, false).await;
    wait_until(|| !sink.is_open()).await;
}

#[ntex::test]
async fn test_client_start_with_control() {
    let (srv, acks) = echo_publisher();
    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    let controls = Rc::new(RefCell::new(Vec::<String>::new()));
    let controls2 = controls.clone();

    rt::spawn(async move {
        let _ = client
            .start_with_control(
                fn_service(async |_: client::ProtocolMessage| {
                    Err::<client::ProtocolMessageAck, _>(AckError(
                        codec::PublishAckReason::ImplementationSpecificError,
                    ))
                }),
                fn_service(move |msg: Control<AckError>| {
                    let controls = controls2.clone();
                    async move {
                        match msg {
                            Control::Stop(Reason::Error(err)) => {
                                controls.borrow_mut().push(format!("{:?}", err.get_ref()));
                                Ok::<_, AckError>(Some(Encoded::Packet(
                                    codec::Disconnect {
                                        reason_code:
                                            codec::DisconnectReasonCode::ImplementationSpecificError,
                                        ..Default::default()
                                    }
                                    .into(),
                                )))
                            }
                            msg => {
                                controls.borrow_mut().push(format!("{msg:?}"));
                                Ok(None)
                            }
                        }
                    }
                }),
            )
            .await;
    });

    // protocol service error is reported to the control service
    trigger(&sink, "any", QoS::AtLeastOnce, false).await;
    wait_until(|| !acks.lock().unwrap().is_empty()).await;
    assert!(acks.lock().unwrap()[0].is_err());
    assert_eq!(
        *controls.borrow(),
        vec!["AckError(ImplementationSpecificError)".to_owned()]
    );
    wait_until(|| !sink.is_open()).await;
}

/// Raw server that completes the handshake and records received packets,
/// optionally answering PINGREQ
fn ping_server(answer: bool) -> (server::TestServer, Arc<Mutex<Vec<Packet>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();

    let srv = server::test_server(async move || {
        let log = log2.clone();
        fn_service(async move |io: ntex::io::Io| {
            let codec = codec::Codec::default();
            io.recv(&codec).await.unwrap();
            let ack = codec::ConnectAck::default();
            io.send(Encoded::Packet(Packet::ConnectAck(Box::new(ack))), &codec)
                .await
                .unwrap();

            while let Ok(Some(res)) = io.recv(&codec).await {
                let pkt = packet(res);
                let ping = pkt == Packet::PingRequest;
                log.lock().unwrap().push(pkt);
                if ping && answer {
                    io.send(Encoded::Packet(Packet::PingResponse), &codec)
                        .await
                        .unwrap();
                }
            }
            Ok::<_, ()>(())
        })
    });
    (srv, log)
}

async fn keepalive_client(srv: &server::TestServer) -> client::Client {
    try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .keep_alive(Seconds(1)),
    )
    .await
    .unwrap()
}

#[ntex::test]
async fn test_client_keepalive() {
    // PINGREQ is sent periodically while the server answers
    let alive = async {
        let (srv, log) = ping_server(true);
        let client = keepalive_client(&srv).await;
        let sink = client.sink();
        ntex::rt::spawn(client.start_default());

        let l = log.clone();
        wait_until(move || l.lock().unwrap().len() > 1).await;
        assert!(
            log.lock()
                .unwrap()
                .iter()
                .all(|p| *p == Packet::PingRequest)
        );
        assert!(sink.is_open());
        sink.close();
    };

    // the client closes the connection if PINGRESP is not received in time
    let timed_out = async {
        let (srv, log) = ping_server(false);
        let client = keepalive_client(&srv).await;
        let sink = client.sink();
        ntex::rt::spawn(client.start_default());

        let l = log.clone();
        wait_until(move || l.lock().unwrap().len() > 1).await;
        assert!(!sink.is_open());
        assert_eq!(
            log.lock().unwrap()[1],
            Packet::Disconnect(Box::new(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::UnspecifiedError,
                reason_string: Some(ByteString::from_static("Keep Alive timeout")),
                ..Default::default()
            }))
        );
    };

    // the keep-alive task stops with the connection
    let closed = async {
        let (srv, log) = ping_server(true);
        let client = keepalive_client(&srv).await;
        let sink = client.sink();
        ntex::rt::spawn(client.start_default());
        sink.close();

        wait_until(move || !sink.is_open()).await;
        sleep(Millis(1250)).await;
        assert!(!log.lock().unwrap().contains(&Packet::PingRequest));
    };

    join(alive, join(timed_out, closed)).await;
}
