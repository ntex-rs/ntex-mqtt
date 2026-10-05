use super::*;

/// Packet sent by the scripted server, raw bytes are used for packets
/// the encoder refuses to produce
enum Out {
    Pkt(Encoded),
    Raw(&'static [u8]),
    /// delay, the peer must process the previous packets separately
    Gap,
}

fn srv_publish(topic: &str, qos: QoS, packet_id: Option<u16>) -> Out {
    Out::Pkt(Encoded::Publish(
        pkt_publish_to(topic, qos, packet_id),
        None,
    ))
}

fn srv_pubrel(packet_id: u16) -> Out {
    Out::Pkt(Encoded::Packet(Packet::PublishRelease(
        codec::PublishAck2 {
            packet_id: pid(packet_id),
            ..Default::default()
        },
    )))
}

fn cl_ack(packet_id: u16) -> Packet {
    Packet::PublishAck(codec::PublishAck {
        packet_id: pid(packet_id),
        ..Default::default()
    })
}

fn cl_rec(packet_id: u16, reason_code: codec::PublishAckReason) -> Packet {
    Packet::PublishReceived(codec::PublishAck {
        packet_id: pid(packet_id),
        reason_code,
        ..Default::default()
    })
}

fn cl_disconnect(reason_code: codec::DisconnectReasonCode) -> Packet {
    Packet::Disconnect(Box::new(codec::Disconnect {
        reason_code,
        ..Default::default()
    }))
}

/// Number of the client dispatcher cases
const CASES: u8 = 28;

/// Packets sent by the scripted server and the expected client responses
#[allow(clippy::too_many_lines)]
fn client_case(case: u8) -> (Vec<Out>, Vec<Packet>) {
    use codec::{DisconnectReasonCode as D, PublishAckReason as R};

    let alias = |topic: &str, alias: u16, id: u16| {
        let mut pkt = pkt_publish_to(topic, QoS::AtLeastOnce, Some(id));
        pkt.properties.topic_alias = Some(pid(alias));
        Out::Pkt(Encoded::Publish(pkt, None))
    };

    match case {
        // the topic of a publish must not contain wildcards
        0 => (
            vec![Out::Raw(&[0x30, 0x06, 0x00, 0x03, b'a', b'/', b'#', 0x00])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // the response topic must not contain wildcards
        1 => (
            vec![Out::Raw(&[
                0x30, 0x0a, 0x00, 0x01, b'a', 0x06, 0x08, 0x00, 0x03, b'b', b'/', b'+',
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // a re-delivered QoS1 publish is acked once
        2 => {
            let mut dup = pkt_publish_to("slow", QoS::AtLeastOnce, Some(1));
            dup.dup = true;
            (
                vec![
                    srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                    Out::Pkt(Encoded::Publish(dup, None)),
                    srv_publish("a", QoS::AtLeastOnce, Some(2)),
                ],
                vec![cl_ack(1), cl_ack(2)],
            )
        }
        // a re-delivered QoS2 publish is acked with PUBREC until PUBREL is received
        3 => {
            let mut dup = pkt_publish_to("a", QoS::ExactlyOnce, Some(1));
            dup.dup = true;
            (
                vec![
                    srv_publish("a", QoS::ExactlyOnce, Some(1)),
                    Out::Pkt(Encoded::Publish(dup, None)),
                    srv_pubrel(1),
                ],
                vec![
                    cl_rec(1, R::Success),
                    cl_rec(1, R::Success),
                    Packet::PublishComplete(codec::PublishAck2 {
                        packet_id: pid(1),
                        ..Default::default()
                    }),
                ],
            )
        }
        // a packet id that is in use is rejected, acks keep the receive order
        4 => (
            vec![
                srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                srv_publish("a", QoS::AtLeastOnce, Some(1)),
            ],
            vec![cl_ack(1), {
                let mut ack = cl_ack(1);
                if let Packet::PublishAck(ref mut a) = ack {
                    a.reason_code = R::PacketIdentifierInUse;
                }
                ack
            }],
        ),
        // the topic alias must be known
        5 => {
            let mut pkt = pkt_publish_to("", QoS::AtMostOnce, None);
            pkt.properties.topic_alias = Some(pid(3));
            (
                vec![Out::Pkt(Encoded::Publish(pkt, None))],
                vec![cl_disconnect(D::TopicAliasInvalid)],
            )
        }
        // the topic alias must not exceed the advertised maximum
        6 => (
            vec![alias("a", 20, 1)],
            vec![cl_disconnect(D::TopicAliasInvalid)],
        ),
        // topic aliases are recorded, resolved and re-assigned
        7 => (
            vec![
                alias("a", 1, 1),
                alias("", 1, 2),
                alias("b", 1, 3),
                alias("b", 1, 4),
            ],
            vec![cl_ack(1), cl_ack(2), cl_ack(3), cl_ack(4)],
        ),
        // PUBREL before PUBREC is a protocol error
        8 => (
            vec![
                srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                srv_pubrel(1),
            ],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // DISCONNECT from a server must not carry a session expiry interval,
        // the packet is built by hand, a server codec removes the property
        9 => (
            vec![Out::Raw(&[
                0xe0, 0x07, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x01,
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // AUTH is not supported
        10 => (
            vec![Out::Pkt(Encoded::Packet(Packet::Auth(Box::default())))],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // a streamed payload is delivered in chunks
        16 => {
            let mut pkt = pkt_publish_to("a", QoS::AtLeastOnce, Some(1));
            pkt.payload_size = 9;
            (
                vec![
                    Out::Pkt(Encoded::Publish(pkt, Some(Bytes::from_static(b"abc")))),
                    Out::Gap,
                    Out::Pkt(Encoded::PayloadChunk(Bytes::from_static(b"def"))),
                    Out::Gap,
                    Out::Pkt(Encoded::PayloadChunk(Bytes::from_static(b"ghi"))),
                ],
                vec![cl_ack(1)],
            )
        }
        // the connection is closed if the payload is not read
        17 => {
            let mut pkt = pkt_publish_to("drop", QoS::AtMostOnce, None);
            pkt.payload_size = 6;
            (
                vec![Out::Pkt(Encoded::Publish(
                    pkt,
                    Some(Bytes::from_static(b"abc")),
                ))],
                vec![],
            )
        }
        // the payload of a re-delivered publish is discarded
        18 => {
            let mut dup = pkt_publish_to("slow", QoS::AtLeastOnce, Some(1));
            dup.dup = true;
            dup.payload_size = 6;
            (
                vec![
                    srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                    Out::Pkt(Encoded::Publish(dup, Some(Bytes::from_static(b"abc")))),
                    Out::Gap,
                    Out::Pkt(Encoded::PayloadChunk(Bytes::from_static(b"def"))),
                ],
                vec![cl_ack(1)],
            )
        }
        // only `max_receive` publishes may be in flight, the client uses
        // `max_receive` of 1 for this case
        19 => (
            vec![
                srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                srv_publish("slow", QoS::AtLeastOnce, Some(2)),
            ],
            vec![cl_disconnect(D::ReceiveMaximumExceeded)],
        ),
        // these packets are never sent by a server
        11..=15 => {
            let pkt = match case {
                11 => Packet::PingRequest,
                12 => Packet::ConnectAck(Box::default()),
                13 => Packet::Connect(Box::default()),
                14 => Packet::Subscribe(codec::Subscribe {
                    packet_id: pid(1),
                    id: None,
                    user_properties: Default::default(),
                    topic_filters: vec![(ByteString::from_static("a"), Default::default())],
                }),
                _ => Packet::Unsubscribe(codec::Unsubscribe {
                    packet_id: pid(1),
                    user_properties: Default::default(),
                    topic_filters: vec![ByteString::from_static("a")],
                }),
            };
            (
                vec![Out::Pkt(Encoded::Packet(pkt))],
                vec![cl_disconnect(D::ProtocolError)],
            )
        }
        // a packet id that is in use is rejected for QoS2 as well
        20 => (
            vec![
                srv_publish("slow", QoS::ExactlyOnce, Some(1)),
                srv_publish("a", QoS::ExactlyOnce, Some(1)),
            ],
            vec![cl_rec(1, R::Success), cl_rec(1, R::PacketIdentifierInUse)],
        ),
        // acks for packets that were never sent are protocol errors
        21..=25 => {
            let pkt = match case {
                21 => Packet::PublishAck(codec::PublishAck {
                    packet_id: pid(1),
                    ..Default::default()
                }),
                22 => Packet::PublishReceived(codec::PublishAck {
                    packet_id: pid(1),
                    ..Default::default()
                }),
                23 => Packet::PublishComplete(codec::PublishAck2 {
                    packet_id: pid(1),
                    ..Default::default()
                }),
                24 => Packet::SubscribeAck(codec::SubscribeAck {
                    packet_id: pid(1),
                    status: vec![codec::SubscribeAckReason::GrantedQos0],
                    properties: Default::default(),
                    reason_string: None,
                }),
                _ => Packet::UnsubscribeAck(codec::UnsubscribeAck {
                    packet_id: pid(1),
                    status: vec![codec::UnsubscribeAckReason::Success],
                    properties: Default::default(),
                    reason_string: None,
                }),
            };
            (
                vec![Out::Pkt(Encoded::Packet(pkt))],
                vec![cl_disconnect(D::ProtocolError)],
            )
        }
        // a QoS0 publish is not acked
        26 => (
            vec![
                srv_publish("a", QoS::AtMostOnce, None),
                srv_publish("a", QoS::AtLeastOnce, Some(1)),
            ],
            vec![cl_ack(1)],
        ),
        // the client closes the connection without a response to DISCONNECT
        _ => (
            vec![Out::Pkt(Encoded::Packet(
                Packet::Disconnect(Box::default()),
            ))],
            vec![],
        ),
    }
}

/// A case is terminal when the client closes the connection, either silently
/// or with a `Disconnect` packet
fn is_terminal(expected: &[Packet]) -> bool {
    expected.is_empty() || matches!(expected.last(), Some(Packet::Disconnect(_)))
}

/// Sentinel packet id, the response to it ends a non terminal script
const SENTINEL: u16 = 9;

/// Packets received by `scripted_server`, tagged with the case they belong to
type CaseLog = Arc<Mutex<Vec<(u8, Packet)>>>;

/// Raw server that runs `client_case` scripts requested by the client,
/// the log keeps the case a packet was received for
fn scripted_server() -> (server::TestServer, CaseLog) {
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

            // a connection runs a single case, requested by the "case" publish
            let mut current = u8::MAX;
            while let Ok(Some(res)) = io.recv(&codec).await {
                match res {
                    Decoded::Publish(pkt, payload, _) if pkt.topic == "case" => {
                        current = payload[0];
                        let (script, expected) = client_case(current);
                        for (idx, out) in script.into_iter().enumerate() {
                            match out {
                                Out::Pkt(pkt) => io.send(pkt, &codec).await.unwrap_or_else(|e| {
                                    panic!("case {current} packet {idx} send failed: {e:?}")
                                }),
                                Out::Raw(buf) => io.encode_slice(buf).unwrap(),
                                Out::Gap => sleep(Millis(50)).await,
                            }
                        }
                        // the sentinel marks the end of the non-terminal scripts
                        if !is_terminal(&expected)
                            && let Out::Pkt(pkt) = srv_pubrel(SENTINEL)
                        {
                            let _ = io.send(pkt, &codec).await;
                        }
                    }
                    // the normal disconnect of a previous case is not interesting
                    Decoded::Packet(Packet::Disconnect(ref pkt), _)
                        if pkt.reason_code == codec::DisconnectReasonCode::NormalDisconnection => {}
                    Decoded::Packet(pkt, _) => log.lock().unwrap().push((current, pkt)),
                    _ => {}
                }
            }
            Ok::<_, ()>(())
        })
    });
    (srv, log)
}

/// Every packet of a `client_case` script must be accepted by the encoder of
/// a server codec, packets the encoder refuses must use `Out::Raw`
#[ntex::test]
async fn test_client_case_scripts_encode() {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let done = Arc::new(AtomicBool::new(false));
    let (errors2, done2) = (errors.clone(), done.clone());

    let srv = server::test_server(async move || {
        let (errors, done) = (errors2.clone(), done2.clone());
        fn_service(async move |io: ntex::io::Io| {
            // a decoded CONNECT switches the codec to the server mode and
            // applies the limits the client asks for
            let codec = codec::Codec::default();
            io.recv(&codec).await.unwrap();
            let ack = codec::ConnectAck::default();
            io.send(Encoded::Packet(Packet::ConnectAck(Box::new(ack))), &codec)
                .await
                .unwrap();

            for case in 0..CASES {
                // the payload of a publish must be completed before the next packet
                let mut pending = 0;
                for (idx, out) in client_case(case).0.into_iter().enumerate() {
                    match out {
                        Out::Pkt(pkt) => {
                            match pkt {
                                Encoded::Publish(ref pkt, ref payload) => {
                                    pending = pkt.payload_size as usize
                                        - payload.as_ref().map_or(0, |p| p.len());
                                }
                                Encoded::PayloadChunk(ref chunk) => pending -= chunk.len(),
                                Encoded::Packet(_) => {}
                            }
                            if let Err(e) = io.encode(pkt, &codec) {
                                errors
                                    .lock()
                                    .unwrap()
                                    .push(format!("case {case}/{idx}: {e:?}"));
                            }
                        }
                        Out::Raw(buf) => io.encode_slice(buf).unwrap(),
                        Out::Gap => {}
                    }
                }
                if pending > 0 {
                    let chunk = Encoded::PayloadChunk(Bytes::from(vec![0; pending]));
                    io.encode(chunk, &codec).unwrap();
                }
            }
            done.store(true, Relaxed);
            Ok::<_, ()>(())
        })
    });

    // the real client connection, the codec of the server gets its limits
    let _client = connect_client(srv.addr()).await;
    wait_until(|| done.load(Relaxed)).await;
    assert_eq!(*errors.lock().unwrap(), Vec::<String>::new());
}

#[ntex::test]
async fn test_client_dispatcher() {
    let (srv, log) = scripted_server();

    let payloads = Arc::new(Mutex::new(Vec::<Bytes>::new()));

    for case in 0..CASES {
        let (_, mut expected) = client_case(case);
        let terminal = is_terminal(&expected);
        if !terminal {
            expected.push(Packet::PublishComplete(codec::PublishAck2 {
                packet_id: pid(SENTINEL),
                reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                ..Default::default()
            }));
        }

        let client = try_connect_client(
            client::Connect::new(srv.addr())
                .client_id("user")
                .max_receive(if case == 19 { 1 } else { 65535 }),
        )
        .await
        .unwrap();
        let sink = client.sink();
        // a publish of the "slow" topic stays in flight until the gate is open
        let gate = Arc::new(AtomicBool::new(false));
        let (payloads, gate2) = (payloads.clone(), gate.clone());
        ntex::rt::spawn(
            client.start(async move |msg: client::control::ProtocolMessage| {
                Ok::<_, ()>(match msg {
                    client::control::ProtocolMessage::Publish(p) => {
                        if p.packet().topic == "slow" {
                            wait_until(|| gate2.load(Relaxed)).await;
                        }
                        // the payload of the "drop" topic is never read
                        if p.packet().topic != "drop" {
                            let payload = p.read_all().await.unwrap();
                            payloads.lock().unwrap().push(payload);
                        }
                        p.ack(codec::PublishAckReason::Success)
                    }
                    msg => msg.ack(),
                })
            }),
        );

        sink.publish(ByteString::from_static("case"))
            .send_at_most_once(Bytes::from(vec![case]))
            .await
            .unwrap();

        // only the packets of this case, a previous one cannot interfere
        let received = || -> Vec<String> {
            let mut res: Vec<String> = log
                .lock()
                .unwrap()
                .iter()
                .filter(|(c, _)| *c == case)
                .map(|(_, p)| format!("{p:?}"))
                .collect();
            res.sort();
            res
        };
        // the whole script is processed by the client once it answers the
        // sentinel or closes the connection, in-flight publishes may complete now
        let answered = || {
            log.lock().unwrap().iter().any(|(c, p)| {
                *c == case
                    && matches!(p, Packet::PublishComplete(ack) if ack.packet_id == pid(SENTINEL))
            })
        };
        wait_until(|| if terminal { !sink.is_open() } else { answered() }).await;
        gate.store(true, Relaxed);

        // responses to slow publishes are not ordered, compare as a multiset
        let cnt = expected.len();
        poll_until(|| received().len() >= cnt).await;
        let mut expected: Vec<String> = expected.iter().map(|p| format!("{p:?}")).collect();
        expected.sort();
        assert_eq!(received(), expected, "case {case}");
        assert_eq!(sink.is_open(), !terminal, "case {case}");
        sink.close();
    }

    // streamed payloads are reassembled
    assert!(
        payloads
            .lock()
            .unwrap()
            .contains(&Bytes::from_static(b"abcdefghi"))
    );
}

fn srv_subscribe(packet_id: u16, filters: &[&str], id: Option<u32>) -> Out {
    Out::Pkt(Encoded::Packet(Packet::Subscribe(codec::Subscribe {
        packet_id: pid(packet_id),
        id: id.and_then(NonZeroU32::new),
        user_properties: Default::default(),
        topic_filters: filters
            .iter()
            .map(|tf| (ByteString::from(*tf), sub_opts(QoS::AtLeastOnce, false)))
            .collect(),
    })))
}

fn srv_unsubscribe(packet_id: u16, filters: &[&str]) -> Out {
    Out::Pkt(Encoded::Packet(Packet::Unsubscribe(codec::Unsubscribe {
        packet_id: pid(packet_id),
        user_properties: Default::default(),
        topic_filters: filters.iter().map(|tf| ByteString::from(*tf)).collect(),
    })))
}

/// Packets sent by a client and the expected server responses, `true` selects
/// the server with the restricted capabilities
#[allow(clippy::too_many_lines)]
fn server_case(case: u8) -> (bool, Vec<Out>, Vec<Packet>) {
    use codec::{DisconnectReasonCode as D, SubscribeAckReason as S, UnsubscribeAckReason as U};

    let alias = |topic: &str, alias: u16, id: u16| {
        let mut pkt = pkt_publish_to(topic, QoS::AtLeastOnce, Some(id));
        pkt.properties.topic_alias = Some(pid(alias));
        Out::Pkt(Encoded::Publish(pkt, None))
    };
    let suback = |id: u16, n: usize| {
        Packet::SubscribeAck(codec::SubscribeAck {
            packet_id: pid(id),
            status: vec![S::PacketIdentifierInUse; n],
            properties: Default::default(),
            reason_string: None,
        })
    };

    match case {
        // the topic of a publish must not contain wildcards
        0 => (
            false,
            vec![Out::Raw(&[0x30, 0x06, 0x00, 0x03, b'a', b'/', b'#', 0x00])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // the response topic must not contain wildcards
        1 => (
            false,
            vec![Out::Raw(&[
                0x30, 0x0a, 0x00, 0x01, b'a', 0x06, 0x08, 0x00, 0x03, b'b', b'/', b'+',
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // a client must not set subscription identifiers
        2 => {
            let mut pkt = pkt_publish_to("a", QoS::AtMostOnce, None);
            pkt.properties.subscription_ids = vec![NonZeroU32::new(1).unwrap()];
            (
                false,
                vec![Out::Pkt(Encoded::Publish(pkt, None))],
                vec![cl_disconnect(D::ProtocolError)],
            )
        }
        // retain is not available
        3 => {
            let mut pkt = pkt_publish_to("a", QoS::AtMostOnce, None);
            pkt.retain = true;
            (
                true,
                vec![Out::Pkt(Encoded::Publish(pkt, None))],
                vec![cl_disconnect(D::RetainNotSupported)],
            )
        }
        // a packet id that is in use is rejected
        4 => (
            false,
            vec![
                srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                srv_publish("a", QoS::AtLeastOnce, Some(1)),
            ],
            vec![cl_ack(1), {
                let mut ack = cl_ack(1);
                if let Packet::PublishAck(ref mut a) = ack {
                    a.reason_code = codec::PublishAckReason::PacketIdentifierInUse;
                }
                ack
            }],
        ),
        // the topic alias must be known
        5 => {
            let mut pkt = pkt_publish_to("", QoS::AtMostOnce, None);
            pkt.properties.topic_alias = Some(pid(3));
            (
                true,
                vec![Out::Pkt(Encoded::Publish(pkt, None))],
                vec![cl_disconnect(D::TopicAliasInvalid)],
            )
        }
        // topic aliases are recorded, resolved and re-assigned
        6 => (
            true,
            vec![
                alias("a", 1, 1),
                alias("", 1, 2),
                alias("b", 1, 3),
                alias("b", 1, 4),
            ],
            vec![cl_ack(1), cl_ack(2), cl_ack(3), cl_ack(4)],
        ),
        // the topic alias must not exceed the advertised maximum
        7 => (
            true,
            vec![alias("a", 6, 1)],
            vec![cl_disconnect(D::TopicAliasInvalid)],
        ),
        // PUBREL for an unknown packet id
        8 => (
            false,
            vec![srv_pubrel(7)],
            vec![Packet::PublishComplete(codec::PublishAck2 {
                packet_id: pid(7),
                reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                ..Default::default()
            })],
        ),
        // PUBREL before PUBREC is a protocol error
        9 => (
            false,
            vec![
                srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                srv_pubrel(1),
            ],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // the topic filter of a subscription must be valid
        10 => (
            false,
            vec![Out::Raw(&[
                0x82, 0x0b, 0x00, 0x01, 0x00, 0x00, 0x05, b'a', b'/', b'#', b'/', b'b', 0x01,
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // the shared topic filter of a subscription must be valid
        11 => (
            false,
            vec![Out::Raw(&[
                0x82, 0x0e, 0x00, 0x01, 0x00, 0x00, 0x08, b'$', b's', b'h', b'a', b'r', b'e', b'/',
                b'g', 0x01,
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // shared subscriptions are not available
        12 => (
            true,
            vec![srv_subscribe(1, &["$share/g/a"], None)],
            vec![cl_disconnect(D::SharedSubscriptionNotSupported)],
        ),
        // wildcard subscriptions are not available
        13 => (
            true,
            vec![srv_subscribe(1, &["a/+"], None)],
            vec![cl_disconnect(D::WildcardSubscriptionsNotSupported)],
        ),
        // a shared subscription must not set no local
        14 => (
            false,
            vec![Out::Raw(&[
                0x82, 0x10, 0x00, 0x01, 0x00, 0x00, 0x0a, b'$', b's', b'h', b'a', b'r', b'e', b'/',
                b'g', b'/', b'a', 0x05,
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // subscription identifiers are not available
        15 => (
            true,
            vec![srv_subscribe(1, &["a"], Some(1))],
            vec![cl_disconnect(D::SubscriptionIdentifiersNotSupported)],
        ),
        // a packet id that is in use is rejected
        16 => (
            false,
            vec![
                srv_subscribe(1, &["slow", "a"], None),
                srv_subscribe(1, &["b"], None),
            ],
            vec![
                Packet::SubscribeAck(codec::SubscribeAck {
                    packet_id: pid(1),
                    status: vec![S::UnspecifiedError; 2],
                    properties: Default::default(),
                    reason_string: None,
                }),
                suback(1, 1),
            ],
        ),
        // the topic filter of an unsubscription must be valid
        17 => (
            false,
            vec![Out::Raw(&[
                0xa2, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x05, b'a', b'/', b'#', b'/', b'b',
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // the shared topic filter of an unsubscription must be valid
        18 => (
            false,
            vec![Out::Raw(&[
                0xa2, 0x0d, 0x00, 0x01, 0x00, 0x00, 0x08, b'$', b's', b'h', b'a', b'r', b'e', b'/',
                b'g',
            ])],
            vec![cl_disconnect(D::ProtocolError)],
        ),
        // a packet id that is in use is rejected
        19 => (
            false,
            vec![
                srv_subscribe(1, &["slow"], None),
                srv_unsubscribe(1, &["a"]),
            ],
            vec![
                Packet::SubscribeAck(codec::SubscribeAck {
                    packet_id: pid(1),
                    status: vec![S::UnspecifiedError],
                    properties: Default::default(),
                    reason_string: None,
                }),
                Packet::UnsubscribeAck(codec::UnsubscribeAck {
                    packet_id: pid(1),
                    status: vec![U::PacketIdentifierInUse],
                    properties: Default::default(),
                    reason_string: None,
                }),
            ],
        ),
        // a streamed payload is delivered in chunks
        20 => {
            let mut pkt = pkt_publish_to("a", QoS::AtLeastOnce, Some(1));
            pkt.payload_size = 6;
            (
                false,
                vec![
                    Out::Pkt(Encoded::Publish(pkt, Some(Bytes::from_static(b"abc")))),
                    Out::Gap,
                    Out::Pkt(Encoded::PayloadChunk(Bytes::from_static(b"def"))),
                ],
                vec![cl_ack(1)],
            )
        }
        // the payload of a re-delivered publish is discarded
        21 => {
            let mut dup = pkt_publish_to("slow", QoS::AtLeastOnce, Some(1));
            dup.dup = true;
            dup.payload_size = 6;
            (
                false,
                vec![
                    srv_publish("slow", QoS::AtLeastOnce, Some(1)),
                    Out::Pkt(Encoded::Publish(dup, Some(Bytes::from_static(b"abc")))),
                    Out::Gap,
                    Out::Pkt(Encoded::PayloadChunk(Bytes::from_static(b"def"))),
                ],
                vec![cl_ack(1)],
            )
        }
        // a packet id that is in use is rejected for QoS2 as well
        22 => (
            false,
            vec![
                srv_publish("slow", QoS::ExactlyOnce, Some(1)),
                srv_publish("a", QoS::ExactlyOnce, Some(1)),
            ],
            vec![
                cl_rec(1, codec::PublishAckReason::Success),
                cl_rec(1, codec::PublishAckReason::PacketIdentifierInUse),
            ],
        ),
        // the session expiry interval must not be set in DISCONNECT if it was
        // zero in CONNECT
        _ => (
            false,
            vec![Out::Pkt(Encoded::Packet(Packet::Disconnect(Box::new(
                codec::Disconnect {
                    session_expiry_interval_secs: Some(1),
                    ..Default::default()
                },
            ))))],
            vec![cl_disconnect(D::ProtocolError)],
        ),
    }
}

#[ntex::test]
async fn test_server_dispatcher() {
    let build = |strict: bool| {
        server::TestServerBuilder::new(async move || {
            MqttServer::new(async |p: Publish| {
                if p.publish_topic() == "slow" {
                    sleep(Millis(100)).await;
                }
                p.read_all().await.unwrap();
                Ok::<_, TestError>(p.ack())
            })
            .protocol(async |msg: ProtocolMessage| {
                if let ProtocolMessage::Subscribe(ref sub) = msg
                    && sub.packet().topic_filters[0].0 == "slow"
                {
                    sleep(Millis(100)).await;
                }
                Ok::<_, TestError>(msg.ack())
            })
            .build(async move |msg: Connect| {
                Ok::<_, TestError>(msg.ack(St).with(|ack| {
                    ack.topic_alias_max = 5;
                    if strict {
                        ack.retain_available = false;
                        ack.shared_subscription_available = false;
                        ack.wildcard_subscription_available = false;
                        ack.subscription_identifiers_available = false;
                    }
                }))
            })
        })
        .config(
            SharedCfg::new("MQTT").add(
                MqttServiceConfig::new()
                    .set_max_qos(QoS::ExactlyOnce)
                    .set_check_subs_availability(true),
            ),
        )
        .start()
    };
    let (srv, strict_srv) = (build(false), build(true));

    for case in 0..24u8 {
        let (strict, script, expected) = server_case(case);
        let terminal = is_terminal(&expected);
        let (io, codec) = handshake(if strict { &strict_srv } else { &srv }).await;

        for out in script {
            let res = match out {
                Out::Pkt(pkt) => io.send(pkt, &codec).await.map_err(|e| format!("{e:?}")),
                Out::Raw(buf) => io.encode_slice(buf).map_err(|e| format!("{e:?}")),
                Out::Gap => {
                    sleep(Millis(50)).await;
                    Ok(())
                }
            };
            res.unwrap_or_else(|e| panic!("case {case}: {e}"));
        }

        let mut received = Vec::new();
        while received.len() < expected.len() {
            let res = timeout(Millis(1500), io.recv(&codec))
                .await
                .unwrap_or_else(|_| panic!("case {case}: timed out, got {received:?}"));
            match res.unwrap() {
                Some(res) => received.push(format!("{:?}", packet(res))),
                None => break,
            }
        }
        // responses to slow packets are not ordered, compare as a multiset
        let mut expected: Vec<String> = expected.iter().map(|p| format!("{p:?}")).collect();
        received.sort();
        expected.sort();
        assert_eq!(received, expected, "case {case}");

        // a terminal case closes the connection, otherwise it stays idle
        match timeout(Millis(250), io.recv(&codec)).await {
            Ok(res) => assert!(
                terminal && res.unwrap().is_none(),
                "case {case}: unexpected packet or close"
            ),
            Err(_) => assert!(!terminal, "case {case}: connection is not closed"),
        }
    }
}
