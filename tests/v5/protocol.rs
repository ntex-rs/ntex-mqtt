use super::*;

#[ntex::test]
async fn test_server_router() {
    let factory =
        Router::<St, TestError>::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build();
    assert!(format!("{factory:?}").contains("v5::RouterFactory"));

    let srv = server::test_server(async || {
        let router = Router::new(async |p: Publish| {
            // default resource
            Ok::<_, TestError>(
                p.ack()
                    .reason_code(codec::PublishAckReason::NoMatchingSubscribers)
                    .reason(ByteString::from_static("default")),
            )
        })
        .resource("one/{id}", async |p: Publish| {
            assert!(p.packet_size() > 0);
            let id = ByteString::from(p.topic().get("id").unwrap());
            Ok::<_, TestError>(p.ack().reason(id))
        })
        .resource("two", async |mut p: Publish| {
            let pl = p.take_payload().read_all().await.unwrap();
            Ok::<_, TestError>(
                PublishAck::new(codec::PublishAckReason::NotAuthorized)
                    .reason(ByteString::try_from(pl).unwrap())
                    .properties(|props| {
                        props.push((ByteString::from_static("h"), ByteString::from_static("two")))
                    }),
            )
        });
        assert!(format!("{router:?}").contains("v5::Router"));
        MqttServer::new(router).build(connect)
    });

    let (io, codec) = handshake(&srv).await;

    // routing to resources and to the default service
    for (idx, (topic, reason_code, reason)) in [
        ("one/5", codec::PublishAckReason::Success, "5"),
        ("one/abc", codec::PublishAckReason::Success, "abc"),
        ("two", codec::PublishAckReason::NotAuthorized, "body"),
        (
            "one",
            codec::PublishAckReason::NoMatchingSubscribers,
            "default",
        ),
        (
            "other/x",
            codec::PublishAckReason::NoMatchingSubscribers,
            "default",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let id = idx as u16 + 1;
        let payload = Bytes::from_static(b"body");
        let pkt = codec::Publish {
            payload_size: payload.len() as u32,
            ..pkt_publish_to(topic, QoS::AtLeastOnce, Some(id))
        };
        let res = send_recv(&io, &codec, Encoded::Publish(pkt, Some(payload))).await;
        match res {
            Packet::PublishAck(ack) => {
                assert_eq!(ack.packet_id, pid(id), "{topic}");
                assert_eq!(ack.reason_code, reason_code, "{topic}");
                assert_eq!(ack.reason_string.as_deref(), Some(reason), "{topic}");
            }
            pkt => panic!("unexpected packet for {topic}: {pkt:?}"),
        }
    }

    // topic alias is resolved by the server before the router is called
    let payload = Bytes::from_static(b"body");
    let mut pkt = codec::Publish {
        payload_size: payload.len() as u32,
        ..pkt_publish_to("two", QoS::AtLeastOnce, Some(10))
    };
    pkt.properties.topic_alias = Some(pid(1));
    let res = send_recv(&io, &codec, Encoded::Publish(pkt, Some(payload))).await;
    assert!(
        matches!(res, Packet::PublishAck(ref ack) if ack.reason_code
            == codec::PublishAckReason::NotAuthorized),
        "{res:?}"
    );

    let mut pkt = pkt_publish_to("", QoS::AtLeastOnce, Some(11));
    pkt.properties.topic_alias = Some(pid(1));
    let res = send_recv(&io, &codec, Encoded::Publish(pkt, None)).await;
    match res {
        Packet::PublishAck(ack) => {
            assert_eq!(ack.packet_id, pid(11));
            assert_eq!(ack.reason_code, codec::PublishAckReason::NotAuthorized);
            assert_eq!(ack.reason_string.as_deref(), Some(""));
        }
        pkt => panic!("unexpected packet: {pkt:?}"),
    }
}

fn user_props(props: &[(&str, &str)]) -> codec::UserProperties {
    props
        .iter()
        .map(|(k, v)| (ByteString::from(*k), ByteString::from(*v)))
        .collect()
}

#[ntex::test]
async fn test_server_protocol_message() {
    let sizes = Arc::new(Mutex::new(Vec::<(&'static str, u32)>::new()));
    let sizes2 = sizes.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let sizes = sizes2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| {
                let log = |name, size| sizes.lock().unwrap().push((name, size));
                Ok::<_, TestError>(match msg {
                    ProtocolMessage::Auth(msg) => {
                        log("auth", msg.packet_size());
                        assert_eq!(msg.packet().reason_code, codec::AuthReasonCode::ReAuth);
                        msg.ack(codec::Auth {
                            reason_code: codec::AuthReasonCode::ContinueAuth,
                            auth_method: Some(ByteString::from_static("m")),
                            ..Default::default()
                        })
                    }
                    ProtocolMessage::PublishRelease(msg) => {
                        log("pubrel", msg.packet_size());
                        assert_eq!(msg.packet().packet_id, pid(1));
                        msg.reason(ByteString::from_static("released"))
                            .properties(|p| p.extend(user_props(&[("r", "rel")])))
                            .ack()
                    }
                    ProtocolMessage::Subscribe(mut msg) => {
                        log("subscribe", msg.packet_size());
                        assert_eq!(msg.packet().packet_id, pid(2));
                        assert_eq!(format!("{:?}", msg.iter_mut()), "SubscribeIter");
                        for mut sub in &mut msg {
                            match sub.topic().as_str() {
                                "q0" => sub.confirm(QoS::AtMostOnce),
                                "q1" => {
                                    assert!(sub.options().no_local);
                                    sub.subscribe(QoS::AtLeastOnce);
                                }
                                "q2" => sub.confirm(QoS::ExactlyOnce),
                                _ => sub.fail(codec::SubscribeAckReason::NotAuthorized),
                            }
                        }
                        msg.ack_reason(ByteString::from_static("subscribed"))
                            .ack_properties(|p| p.extend(user_props(&[("s", "sub")])))
                            .ack()
                    }
                    ProtocolMessage::Unsubscribe(mut msg) => {
                        log("unsubscribe", msg.packet_size());
                        assert_eq!(msg.packet().packet_id, pid(3));
                        assert_eq!(msg.properties(), &user_props(&[("u", "unsub")]));
                        assert_eq!(msg.iter().count(), 2);
                        assert_eq!(format!("{:?}", msg.iter_mut()), "UnsubscribeIter");
                        for mut item in &mut msg {
                            if item.topic() == "keep" {
                                item.fail(codec::UnsubscribeAckReason::NotAuthorized);
                            } else {
                                item.success();
                            }
                        }
                        msg.ack_reason(ByteString::from_static("unsubscribed"))
                            .ack_properties(|p| p.extend(user_props(&[("u", "unsub")])))
                            .ack()
                    }
                    ProtocolMessage::Disconnect(msg) => {
                        log("disconnect", msg.packet_size());
                        assert_eq!(
                            msg.packet().reason_code,
                            codec::DisconnectReasonCode::DisconnectWithWillMessage
                        );
                        msg.ack()
                    }
                    ProtocolMessage::Ping(msg) => msg.ack(),
                })
            })
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let (io, codec) = handshake(&srv).await;

    // AUTH is answered with the `Auth` packet provided to `Auth::ack()`
    let auth = codec::Auth {
        reason_code: codec::AuthReasonCode::ReAuth,
        auth_method: Some(ByteString::from_static("m")),
        ..Default::default()
    };
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(auth.into())).await,
        Packet::Auth(Box::new(codec::Auth {
            reason_code: codec::AuthReasonCode::ContinueAuth,
            auth_method: Some(ByteString::from_static("m")),
            ..Default::default()
        }))
    );

    // PUBREL is answered with PUBCOMP carrying the configured reason/properties
    io.send(
        Encoded::Publish(pkt_publish_to("t", QoS::ExactlyOnce, Some(1)), None),
        &codec,
    )
    .await
    .unwrap();
    assert!(matches!(
        packet(io.recv(&codec).await.unwrap().unwrap()),
        Packet::PublishReceived(_)
    ));
    let pubrel = Packet::PublishRelease(codec::PublishAck2 {
        packet_id: pid(1),
        reason_code: codec::PublishAck2Reason::Success,
        properties: Default::default(),
        reason_string: None,
    });
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(pubrel)).await,
        Packet::PublishComplete(codec::PublishAck2 {
            packet_id: pid(1),
            reason_code: codec::PublishAck2Reason::Success,
            properties: user_props(&[("r", "rel")]),
            reason_string: Some(ByteString::from_static("released")),
        })
    );

    // every subscription is confirmed/failed individually
    let subscribe = Packet::Subscribe(codec::Subscribe {
        packet_id: pid(2),
        id: None,
        user_properties: Default::default(),
        topic_filters: vec![
            (
                ByteString::from_static("q0"),
                sub_opts(QoS::AtMostOnce, false),
            ),
            (
                ByteString::from_static("q1"),
                sub_opts(QoS::AtLeastOnce, true),
            ),
            (
                ByteString::from_static("q2"),
                sub_opts(QoS::ExactlyOnce, false),
            ),
            (
                ByteString::from_static("bad"),
                sub_opts(QoS::AtMostOnce, false),
            ),
        ],
    });
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(subscribe)).await,
        Packet::SubscribeAck(codec::SubscribeAck {
            packet_id: pid(2),
            status: vec![
                codec::SubscribeAckReason::GrantedQos0,
                codec::SubscribeAckReason::GrantedQos1,
                codec::SubscribeAckReason::GrantedQos2,
                codec::SubscribeAckReason::NotAuthorized,
            ],
            properties: user_props(&[("s", "sub")]),
            reason_string: Some(ByteString::from_static("subscribed")),
        })
    );

    // unsubscribe statuses default to `Success` and can be failed individually
    let unsubscribe = Packet::Unsubscribe(codec::Unsubscribe {
        packet_id: pid(3),
        user_properties: user_props(&[("u", "unsub")]),
        topic_filters: vec![
            ByteString::from_static("q0"),
            ByteString::from_static("keep"),
        ],
    });
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(unsubscribe)).await,
        Packet::UnsubscribeAck(codec::UnsubscribeAck {
            packet_id: pid(3),
            status: vec![
                codec::UnsubscribeAckReason::Success,
                codec::UnsubscribeAckReason::NotAuthorized,
            ],
            properties: user_props(&[("u", "unsub")]),
            reason_string: Some(ByteString::from_static("unsubscribed")),
        })
    );

    // PINGREQ is answered with PINGRESP
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(Packet::PingRequest)).await,
        Packet::PingResponse
    );

    // `Disconnect::ack()` closes the connection without sending a packet
    let disconnect = codec::Disconnect::new(codec::DisconnectReasonCode::DisconnectWithWillMessage);
    io.send(Encoded::Packet(disconnect.into()), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    let sizes = sizes.lock().unwrap().clone();
    assert_eq!(
        sizes.iter().map(|s| s.0).collect::<Vec<_>>(),
        ["auth", "pubrel", "subscribe", "unsubscribe", "disconnect"]
    );
    assert!(sizes.iter().all(|s| s.1 > 0), "{sizes:?}");
}

#[ntex::test]
async fn test_server_protocol_generic_ack() {
    let srv = server::TestServerBuilder::new(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async |msg: ProtocolMessage| Ok::<_, TestError>(msg.ack()))
            .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce)))
    .start();

    let (io, codec) = handshake(&srv).await;

    // default subscribe ack fails every filter, unsubscribe ack succeeds
    let subscribe = Packet::Subscribe(codec::Subscribe {
        packet_id: pid(1),
        id: None,
        user_properties: Default::default(),
        topic_filters: vec![(
            ByteString::from_static("t"),
            sub_opts(QoS::AtMostOnce, false),
        )],
    });
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(subscribe)).await,
        Packet::SubscribeAck(codec::SubscribeAck {
            packet_id: pid(1),
            status: vec![codec::SubscribeAckReason::UnspecifiedError],
            properties: Default::default(),
            reason_string: None,
        })
    );
    let unsubscribe = Packet::Unsubscribe(codec::Unsubscribe {
        packet_id: pid(2),
        user_properties: Default::default(),
        topic_filters: vec![ByteString::from_static("t")],
    });
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(unsubscribe)).await,
        Packet::UnsubscribeAck(codec::UnsubscribeAck {
            packet_id: pid(2),
            status: vec![codec::UnsubscribeAckReason::Success],
            properties: Default::default(),
            reason_string: None,
        })
    );
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(Packet::PingRequest)).await,
        Packet::PingResponse
    );

    // QoS2 release is acked
    io.send(
        Encoded::Publish(pkt_publish_to("t", QoS::ExactlyOnce, Some(3)), None),
        &codec,
    )
    .await
    .unwrap();
    io.recv(&codec).await.unwrap().unwrap();
    let pubrel = Packet::PublishRelease(codec::PublishAck2 {
        packet_id: pid(3),
        reason_code: codec::PublishAck2Reason::Success,
        properties: Default::default(),
        reason_string: None,
    });
    assert!(matches!(
        send_recv(&io, &codec, Encoded::Packet(pubrel)).await,
        Packet::PublishComplete(_)
    ));

    // AUTH is not supported by the generic ack, connection is terminated.
    // BUG: the `ImplementationSpecificError` DISCONNECT is dropped, see
    // `src/v5/dispatcher.rs:680` (the io is closed before the response is encoded)
    let auth = codec::Auth {
        reason_code: codec::AuthReasonCode::ReAuth,
        auth_method: Some(ByteString::from_static("m")),
        ..Default::default()
    };
    io.send(Encoded::Packet(auth.into()), &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    // DISCONNECT ack just closes the connection
    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());
}

#[ntex::test]
async fn test_server_default_protocol_service() {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
    });

    // the default protocol service answers PINGREQ
    let (io, codec) = handshake(&srv).await;
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(Packet::PingRequest)).await,
        Packet::PingResponse
    );

    // ... and terminates the connection for any other control packet.
    // BUG: the `UnspecifiedError` DISCONNECT is dropped, see
    // `src/v5/dispatcher.rs:680` (the io is closed before the response is encoded)
    let subscribe = Packet::Subscribe(codec::Subscribe {
        packet_id: pid(1),
        id: None,
        user_properties: Default::default(),
        topic_filters: vec![(
            ByteString::from_static("t"),
            sub_opts(QoS::AtMostOnce, false),
        )],
    });
    io.send(Encoded::Packet(subscribe), &codec).await.unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    // DISCONNECT is acked by closing the connection
    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());
}

/// Middleware that counts the number of dispatched packets
#[derive(Clone)]
struct CountMw(Arc<AtomicUsize>);

struct CountSvc<S> {
    svc: S,
    count: Arc<AtomicUsize>,
}

impl<S, St> ntex_service::Middleware<S, St> for CountMw {
    type Service = CountSvc<S>;

    fn create(&self, _: &St, svc: S) -> Self::Service {
        CountSvc {
            svc,
            count: self.0.clone(),
        }
    }
}

impl<S, St, R> ntex_service::Service<St, R> for CountSvc<S>
where
    S: ntex_service::Service<St, R>,
{
    type Res = S::Res;
    type Error = S::Error;

    async fn call(
        &self,
        req: R,
        ctx: ntex_service::Ctx<'_, Self, St>,
    ) -> Result<Self::Res, Self::Error> {
        self.count.fetch_add(1, Relaxed);
        ctx.call(&self.svc, req).await
    }

    ntex_service::forward_ready!(St, svc);
    ntex_service::forward_shutdown!(St, svc);
}

/// Checks that every dispatched packet is seen by the `CountMw` middleware
async fn assert_counted(srv: &server::TestServer, count: &AtomicUsize) {
    let (io, codec) = handshake(srv).await;
    assert_eq!(count.load(Relaxed), 0);
    for id in 1..3u16 {
        let pkt = pkt_publish_to("t", QoS::AtLeastOnce, Some(id));
        assert_eq!(
            send_recv(&io, &codec, Encoded::Publish(pkt, None)).await,
            Packet::PublishAck(codec::PublishAck {
                packet_id: pid(id),
                reason_code: codec::PublishAckReason::Success,
                properties: Default::default(),
                reason_string: None,
            })
        );
        assert_eq!(count.load(Relaxed), usize::from(id));
    }
}

#[ntex::test]
async fn test_server_middleware() {
    // `middleware()` stacks on top of the default one
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    let srv = server::test_server(async move || {
        let srv = MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()));
        assert_eq!(format!("{srv:?}"), "v5::MqttServer");
        srv.middleware(CountMw(count2.clone())).build(connect)
    });
    assert_counted(&srv, &count).await;

    // `replace_middlewares()` drops the default one
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    let srv = server::test_server(async move || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .replace_middlewares(CountMw(count2.clone()))
            .build(connect)
    });
    assert_counted(&srv, &count).await;
}
