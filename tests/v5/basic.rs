use super::*;

/// STATEMENT: A connected client can publish to a valid topic but rejects a wildcard topic.
///
/// HOW:
/// - Start an acknowledging server and the default client dispatcher.
/// - Send QoS 1 messages to `test` and `#`; assert success for the former and an error for the
///   latter.
#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

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

/// STATEMENT: A rejected connection exposes the server's refusal reason to the client.
///
/// HOW:
/// - Reject the handshake with `NotAuthorized`.
/// - Connect with `try_connect_client` and assert that the returned acknowledgment error
///   carries that reason.
#[ntex::test]
async fn test_connect_failed() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |hnd: Connect| {
                Ok::<_, ()>(hnd.failed::<St>(codec::ConnectAckReason::NotAuthorized))
            },
        )
    });

    // connect to server
    let err = try_connect_client(client::Connect::new(srv.addr()).client_id("user"))
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

/// STATEMENT: A client publish fails with a server configured to close its sink before
/// acknowledging.
///
/// HOW:
/// - Use a publish handler that closes its sink before delaying its acknowledgment.
/// - Publish to `#` and assert that the send returns an error.
#[ntex::test]
async fn test_disconnect() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
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
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("#"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_err());

    Ok(())
}

/// STATEMENT: A client publish fails with a server configured to close its sink with
/// `ServerMoved`.
///
/// HOW:
/// - Configure a publish handler to call `close_with_reason` with `ServerMoved` before
///   acknowledging.
/// - Publish to `#` from a running client and assert that the send fails.
#[ntex::test]
async fn test_disconnect_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
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
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("#"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_err());

    Ok(())
}

/// STATEMENT: Errors from both the disconnect protocol handler and the stop handler still
/// close the connection.
///
/// HOW:
/// - Make the protocol handler return `TestError` for a disconnect and the control handler
///   return another error.
/// - Send a disconnect after the handshake and assert end of stream.
#[ntex::test]
async fn test_nested_errors_handling() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
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
    let (io, codec) = handshake(&srv).await;

    // disconnect
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    Ok(())
}

/// STATEMENT: A protocol-handler error while processing a disconnect closes the connection
/// without a reply.
///
/// HOW:
/// - Fail the disconnect handler and let the control handler propose an
///   `ImplementationSpecificError` disconnect.
/// - Send a disconnect and assert that the peer closes without sending a packet.
#[ntex::test]
async fn test_disconnect_on_error() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
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
    let (io, codec) = handshake(&srv).await;

    // disconnect
    io.send(Encoded::Packet(codec::Disconnect::default().into()), &codec)
        .await
        .unwrap();
    let res = io.recv(&codec).await.unwrap();
    assert!(res.is_none());

    Ok(())
}

/// STATEMENT: The default control service disconnects a client when subscription handling fails.
///
/// HOW:
/// - Return `TestError` from the subscribe protocol handler.
/// - Send a subscription after the handshake and assert that the next packet is a disconnect.
#[ntex::test]
async fn test_disconnect_after_control_error() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async move |msg| match msg {
                ProtocolMessage::Subscribe(_) => Err(TestError),
                _ => Ok(msg.disconnect()),
            })
            .build(connect)
    });

    let (io, codec) = handshake(&srv).await;

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

    let result = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(packet(result), Packet::Disconnect(_)));
    Ok(())
}

/// STATEMENT: A second connect or a client-sent ping response is reported as a protocol violation.
///
/// HOW:
/// - For each unexpected packet, complete a handshake and send that packet.
/// - Assert a `ProtocolError` disconnect, connection closure, and the corresponding violation
///   message captured by the control service.
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
        let srv = server::test_server(async move |_| {
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

/// STATEMENT: The advertised topic-alias maximum is inclusive and larger aliases are rejected.
///
/// HOW:
/// - Advertise a maximum alias of `2` and verify it in the connection acknowledgment.
/// - Publish with alias `2` and expect an acknowledgment; publish with alias `3` and expect
///   `TopicAliasInvalid`.
#[ntex::test]
async fn test_topic_alias_max() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async |_| {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_topic_alias(2)))
    .start();

    let (io, codec) = connect_raw(&srv).await;
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert!(
        matches!(ack, Decoded::Packet(Packet::ConnectAck(ref ack), _) if ack.topic_alias_max == 2),
        "{ack:?}"
    );

    // alias equal to the maximum is accepted
    let mut pkt = pkt_publish();
    pkt.properties.topic_alias = NonZeroU16::new(2);
    io.send(Encoded::Publish(pkt, None), &codec).await.unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert!(
        matches!(pkt, Decoded::Packet(Packet::PublishAck(_), _)),
        "{pkt:?}"
    );

    // alias greater than the maximum, DISCONNECT 0x94 (MQTT 5.0, 3.3.2.3.4)
    let mut pkt = codec::Publish {
        packet_id: NonZeroU16::new(2),
        ..pkt_publish()
    };
    pkt.properties.topic_alias = NonZeroU16::new(3);
    io.send(Encoded::Publish(pkt, None), &codec).await.unwrap();
    let pkt = io.recv(&codec).await;
    assert!(
        matches!(pkt, Ok(Some(Decoded::Packet(Packet::Disconnect(ref d), _)))
                 if d.reason_code == codec::DisconnectReasonCode::TopicAliasInvalid),
        "{pkt:?}"
    );
    Ok(())
}

/// STATEMENT: Acknowledging a QoS 1 publish with a QoS 2 acknowledgment terminates the
/// connection without panicking.
///
/// HOW:
/// - Trigger a server QoS 1 publish and answer it with either a publish-received or
///   publish-complete packet.
/// - Assert a `ProtocolError` disconnect, the expected-acknowledgment violation message, and a
///   pending-send result of `Disconnected`.
#[ntex::test]
async fn test_unexpected_ack_type() -> std::io::Result<()> {
    // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP (MQTT 5.0, 4.3.2, 4.3.3)
    for (ack, message) in [
        (
            Packet::PublishReceived(codec::PublishAck {
                packet_id: pid(1),
                ..Default::default()
            }),
            "Expected PUBACK packet",
        ),
        (
            Packet::PublishComplete(codec::PublishAck2 {
                packet_id: pid(1),
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
        let srv = server::test_server(async move |_| {
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

        let (io, codec) = handshake(&srv).await;

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
            matches!(pkt, Ok(Some(Decoded::Packet(Packet::Disconnect(ref d), _)))
                     if d.reason_code == codec::DisconnectReasonCode::ProtocolError),
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

/// STATEMENT: A ping request reaches the protocol service and receives a ping response.
///
/// HOW:
/// - Set a flag when the server handles `ProtocolMessage::Ping` and acknowledge it.
/// - Send a ping request and assert both the ping response and the handler flag.
#[ntex::test]
async fn test_ping() -> std::io::Result<()> {
    let ping = Arc::new(AtomicBool::new(false));
    let ping2 = ping.clone();

    let srv = server::test_server(async move |_| {
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

    let (io, codec) = handshake(&srv).await;

    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(packet(pkt), Packet::PingResponse);
    assert!(ping.load(Relaxed));

    Ok(())
}

/// STATEMENT: Disabling problem information omits acknowledgment properties and reason strings.
///
/// HOW:
/// - Connect with `request_problem_info` disabled and a 30-byte maximum packet size.
/// - Have the server attach large user properties and a reason to a QoS 1 acknowledgment;
///   assert both are absent in the received acknowledgment.
#[ntex::test]
async fn test_request_problem_info() {
    let srv = server::test_server(async move |_| {
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
    let client = try_connect_client(
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

/// STATEMENT: Subscription rejection acknowledgments preserve the configured status and reason
/// string.
///
/// HOW:
/// - Fail the requested filter with `ImplementationSpecificError` and set an acknowledgment
///   reason.
/// - Send a subscription and compare the complete acknowledgment, including its packet
///   identifier and reason string.
#[ntex::test]
async fn test_suback_with_reason() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
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

    let (io, codec) = handshake(&srv).await;

    io.send(
        Packet::Subscribe(codec::Subscribe {
            packet_id: pid(1),
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
            packet_id: pid(1),
            status: vec![codec::SubscribeAckReason::ImplementationSpecificError],
            properties: codec::UserProperties::default(),
            reason_string: Some("some reason".into()),
        })
    );

    Ok(())
}

/// STATEMENT: A buffered publish and following disconnect are both delivered to the configured
/// handlers.
///
/// HOW:
/// - Send a connect, then buffer a publish and disconnect before flushing and dropping the raw
///   connection.
/// - Assert that both the publish flag and disconnect flag were set.
#[ntex::test]
async fn test_handle_incoming() -> std::io::Result<()> {
    let publish = Arc::new(AtomicBool::new(false));
    let publish2 = publish.clone();
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::test_server(async move |_| {
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

    let (io, codec) = connect_raw(&srv).await;
    io.encode(Encoded::Publish(pkt_publish(), Some(Bytes::new())), &codec)
        .unwrap();
    io.encode(
        Packet::Disconnect(Box::new(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        }))
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

/// Binds the server QoS limits into a closure that checks publish handling after disconnect.
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

/// Buffers a publish followed by a disconnect, verifies disconnect handling, and reports
/// publish delivery.
async fn handle_or_drop_publish_after_disconnect(
    publish_qos: QoS,
    max_qos: QoS,
    handle_qos_after_disconnect: Option<QoS>,
) -> bool {
    let publish = Arc::new(AtomicBool::new(false));
    let publish2 = publish.clone();
    let disconnect = Arc::new(AtomicBool::new(false));
    let disconnect2 = disconnect.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
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
        _ => Some(pid(1)),
    };
    let (io, codec) = connect_raw(&srv).await;

    io.encode(
        Encoded::Publish(
            codec::Publish {
                packet_id,
                qos: publish_qos,
                ..pkt_publish()
            },
            None,
        ),
        &codec,
    )
    .unwrap();

    io.encode(
        Encoded::Packet(Packet::Disconnect(Box::new(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::ReceiveMaximumExceeded,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Default::default(),
        }))),
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

/// STATEMENT: Buffered publishes within the configured post-disconnect QoS threshold are handled.
///
/// HOW:
/// - Vary the server maximum QoS and `handle_qos_after_disconnect` across QoS 0, 1, and 2.
/// - For each permitted QoS, buffer a publish before a disconnect and assert that both
///   handlers run.
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

/// STATEMENT: Publishing above the server's maximum QoS produces a protocol violation and a
/// specific disconnect reason.
///
/// HOW:
/// - Limit the server to QoS 0 and capture protocol violations in its control service.
/// - Send a QoS 1 publish and assert `QosNotSupported` together with the violation flag.
#[ntex::test]
async fn test_max_qos() -> std::io::Result<()> {
    let violated = Arc::new(AtomicBool::new(false));
    let violated2 = violated.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
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

    let (io, codec) = handshake(&srv).await;

    io.encode(Encoded::Publish(pkt_publish(), None), &codec)
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(pkt),
        Packet::Disconnect(Box::new(codec::Disconnect {
            reason_code: codec::DisconnectReasonCode::QosNotSupported,
            ..Default::default()
        }))
    );
    assert!(violated.load(Relaxed));

    Ok(())
}

/// STATEMENT: A server that advertises no retained-message support rejects retained QoS 0 and
/// QoS 1 publishes.
///
/// HOW:
/// - Set `retain_available` to `false` and verify the advertised capability on each connection.
/// - Send retained messages at both QoS levels and assert `RetainNotSupported` disconnects.
#[ntex::test]
async fn test_retain_not_available() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async |msg: Connect| {
                Ok::<_, TestError>(msg.ack(St).with(|ack| ack.retain_available = false))
            },
        )
    });

    // RETAIN is not allowed for any QoS [MQTT-3.2.2-14]
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce] {
        let (io, codec) = connect_raw(&srv).await;
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
            Packet::Disconnect(Box::new(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::RetainNotSupported,
                ..Default::default()
            })),
            "{qos:?}"
        );
    }

    Ok(())
}

/// STATEMENT: Unavailable shared and wildcard subscription capabilities produce distinct
/// disconnect reasons.
///
/// HOW:
/// - Disable shared and wildcard subscriptions in the connection acknowledgment and verify
///   both flags.
/// - Subscribe to a shared filter and a wildcard filter on separate connections; assert the
///   matching unsupported-capability reason.
#[ntex::test]
async fn test_subscription_not_available() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
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
        let (io, codec) = connect_raw(&srv).await;
        let ack = io.recv(&codec).await.unwrap().unwrap();
        let Packet::ConnectAck(ack) = packet(ack) else {
            panic!()
        };
        assert!(!ack.shared_subscription_available);
        assert!(!ack.wildcard_subscription_available);

        io.send(
            Encoded::Packet(Packet::Subscribe(codec::Subscribe {
                packet_id: pid(1),
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

/// STATEMENT: Closing the sink from an error handler sends one disconnect before the
/// connection ends.
///
/// HOW:
/// - Fail a server publish handler and close its sink with `ServerMoved` from the control service.
/// - Send a QoS 0 publish over a framed connection; assert one matching disconnect followed by
///   end of stream.
#[ntex::test]
async fn test_disconnect_once() -> std::io::Result<()> {
    let srv = server::test_server(async move |_| {
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
                qos: QoS::AtMostOnce,
                packet_id: None,
                topic: "test/test".into(),
                ..pkt_publish()
            },
            None,
        ))
        .await
        .unwrap();

    // Receive DISCONNECT
    let res = client.recv().await.unwrap().unwrap();
    assert!(matches!(
        res,
        codec::Decoded::Packet(codec::Packet::Disconnect(ref pkt), _)
            if pkt.reason_code == codec::DisconnectReasonCode::ServerMoved
    ));
    // IO Close
    let res = client.recv().await.unwrap();
    assert_eq!(res, None);

    Ok(())
}

/// STATEMENT: A disconnect cannot increase an initially zero session-expiry interval.
///
/// HOW:
/// - Connect with zero session expiry, then send a disconnect requesting ten seconds.
/// - Assert a `ProtocolError` disconnect, notification of the control service, and subsequent
///   connection closure.
#[ntex::test]
async fn protocol_error_session_expiry() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move |_| {
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

/// STATEMENT: Unsupported protocol levels receive a version-appropriate refusal before the
/// connection closes.
///
/// HOW:
/// - Send hand-built connect packets using protocol levels `3`, `4`, `0`, `6`, and `0xff`.
/// - Assert v3 refusal packets for levels `3` and `4`, v5 `UnsupportedProtocolVersion` packets
///   for the others, and end of stream in every case.
#[ntex::test]
async fn test_handshake_unsupported_protocol_level() -> std::io::Result<()> {
    let srv = server::test_server(async |_| {
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

/// STATEMENT: Invalid will topics and will response topics are rejected before the handshake
/// service is called.
///
/// HOW:
/// - Send raw connect packets with empty or wildcard will topics and wildcard response topics.
/// - Assert `ProtocolError` acknowledgments containing the violated requirement, connection
///   closure, and an untouched handshake flag.
/// - Send a valid will and response topic; assert a successful acknowledgment and a set
///   handshake flag.
#[ntex::test]
async fn test_handshake_invalid_will_topic() -> std::io::Result<()> {
    let called = Arc::new(AtomicBool::new(false));
    let called2 = called.clone();
    let srv = server::test_server(async move |_| {
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

/// STATEMENT: Subscription and unsubscription sends are allowed on client sinks but not server
/// sinks.
///
/// HOW:
/// - Attempt both sends from the server session and record their results.
/// - Perform both operations from the client and assert successful acknowledgment statuses and
///   two server-side `NotAllowed` errors.
#[ntex::test]
async fn test_subscribe_client_only() {
    let server_res = Arc::new(Mutex::new(Vec::new()));
    let server_res2 = server_res.clone();
    let srv = server::test_server(async move |_| {
        let server_res = server_res2.clone();
        MqttServer::new(move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            let server_res = server_res.clone();
            async move {
                // MQTT 5.0, 3.8 and 3.10: a server does not send SUBSCRIBE or UNSUBSCRIBE
                let res = sink
                    .subscribe(None)
                    .topic_filter("a".into(), codec::SubscriptionOptions::default())
                    .send()
                    .await
                    .map(|_| ());
                server_res.lock().unwrap().push(res);
                let res = sink
                    .unsubscribe()
                    .topic_filter("a".into())
                    .send()
                    .await
                    .map(|_| ());
                server_res.lock().unwrap().push(res);
                Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                    Ok::<_, TestError>(p.ack())
                }))
            }
        })
        .protocol(async move |msg| match msg {
            ProtocolMessage::Subscribe(mut msg) => {
                for mut sub in &mut msg {
                    sub.subscribe(codec::QoS::AtLeastOnce);
                }
                Ok::<_, TestError>(msg.ack())
            }
            msg => Ok(msg.ack()),
        })
        .build(connect)
    });

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let ack = sink
        .subscribe(None)
        .topic_filter("a".into(), codec::SubscriptionOptions::default())
        .send()
        .await
        .unwrap();
    assert_eq!(ack.status, vec![codec::SubscribeAckReason::GrantedQos1]);
    let ack = sink
        .unsubscribe()
        .topic_filter("a".into())
        .send()
        .await
        .unwrap();
    assert_eq!(ack.status, vec![codec::UnsubscribeAckReason::Success]);

    assert_eq!(
        &server_res.lock().unwrap()[..],
        [
            Err(error::SendPacketError::NotAllowed),
            Err(error::SendPacketError::NotAllowed)
        ]
    );
}

/// STATEMENT: Invalid handshake traffic is rejected without invoking the handshake service or
/// preventing later connections.
///
/// HOW:
/// - Send non-connect first packets, malformed connects, an unsupported level, invalid wills,
///   and an early peer close.
/// - Check silent closure where appropriate, version or protocol refusal acknowledgments
///   otherwise, and no handshake-service calls.
/// - Complete a valid handshake, exchange a ping, and assert exactly one handshake-service call.
#[ntex::test]
async fn test_handshake_rejected() {
    let called = Arc::new(AtomicUsize::new(0));
    let called2 = called.clone();
    let srv = server::test_server(async move |_| {
        let called = called2.clone();
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |msg: Connect| {
                called.fetch_add(1, Relaxed);
                Ok::<_, TestError>(msg.ack(St))
            },
        )
    });

    // the first packet must be CONNECT, the connection is closed without a response
    for pkt in [
        Encoded::Packet(Packet::PingRequest),
        Encoded::Packet(Packet::PingResponse),
        Encoded::Publish(pkt_publish(), None),
    ] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.send(pkt, &codec).await.unwrap();
        assert!(io.recv(&codec).await.unwrap().is_none());
    }

    // a malformed CONNECT is not answered with a CONNACK, only an unsupported
    // protocol level is
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode_slice(&[
        0x10, 0x0c, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x01, 0x00, 0x3c, 0x00, 0x00,
    ])
    .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    // an unsupported protocol level is answered with a CONNACK
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.encode_slice(&[
        0x10, 0x0d, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x06, 0x02, 0x00, 0x3c, 0x00, 0x00, 0x00,
    ])
    .unwrap();
    let Some(Decoded::Packet(Packet::ConnectAck(ack), _)) = io.recv(&codec).await.unwrap() else {
        panic!("connect ack is expected")
    };
    assert_eq!(
        ack.reason_code,
        codec::ConnectAckReason::UnsupportedProtocolVersion
    );
    assert!(io.recv(&codec).await.unwrap().is_none());

    // a last will that violates the specification is rejected with a CONNACK,
    // such a CONNECT is built by hand, the encoder rejects it
    for connect in [
        // the will topic contains a wildcard
        &[
            0x10, 0x16, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, 0x00, 0x3c, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x03, b'a', b'/', b'#', 0x00, 0x01, b'x',
        ][..],
        // the response topic of the will contains a wildcard
        &[
            0x10, 0x1a, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, 0x00, 0x3c, 0x00, 0x00,
            0x00, 0x06, 0x08, 0x00, 0x03, b'b', b'/', b'+', 0x00, 0x01, b'a', 0x00, 0x01, b'x',
        ][..],
    ] {
        let io = srv.connect().await.unwrap();
        let codec = codec::Codec::default();
        io.encode_slice(connect).unwrap();
        let Some(Decoded::Packet(Packet::ConnectAck(ack), _)) = io.recv(&codec).await.unwrap()
        else {
            panic!("connect ack is expected")
        };
        assert_eq!(ack.reason_code, codec::ConnectAckReason::ProtocolError);
        assert!(
            ack.reason_string.unwrap().starts_with("[MQTT-"),
            "the violated requirement is reported"
        );
        assert!(io.recv(&codec).await.unwrap().is_none());
    }

    // the peer is gone before CONNECT is received
    let io = srv.connect().await.unwrap();
    io.shutdown().await.unwrap();

    // the handshake service was never called, the server is still usable
    assert_eq!(called.load(Relaxed), 0);
    let (io, codec) = handshake(&srv).await;
    assert_eq!(
        send_recv(&io, &codec, Encoded::Packet(Packet::PingRequest)).await,
        Packet::PingResponse
    );
    assert_eq!(called.load(Relaxed), 1);
}

/// STATEMENT: Packets following a disconnect in the same buffered write are ignored.
///
/// HOW:
/// - Buffer a disconnect followed by authentication, subscription, unsubscription, and publish
///   packets.
/// - Assert closure without acknowledgments, no publish-handler calls, and only the disconnect
///   in the protocol log.
#[ntex::test]
async fn test_packets_after_disconnect() {
    let published = Arc::new(AtomicUsize::new(0));
    let published2 = published.clone();
    let proto = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let proto2 = proto.clone();

    let srv = server::test_server(async move |_| {
        let (published, proto) = (published2.clone(), proto2.clone());
        MqttServer::new(move |p: Publish| {
            let published = published.clone();
            async move {
                published.fetch_add(1, Relaxed);
                Ok::<_, TestError>(p.ack())
            }
        })
        .protocol(move |msg: ProtocolMessage| {
            let proto = proto.clone();
            async move {
                proto.lock().unwrap().push(match msg {
                    ProtocolMessage::Auth(_) => "auth",
                    ProtocolMessage::Subscribe(_) => "subscribe",
                    ProtocolMessage::Unsubscribe(_) => "unsubscribe",
                    ProtocolMessage::Disconnect(_) => "disconnect",
                    ProtocolMessage::Ping(_) => "ping",
                    ProtocolMessage::PublishRelease(_) => "pubrel",
                });
                Ok::<_, TestError>(msg.ack())
            }
        })
        .build(async |msg: Connect| Ok::<_, TestError>(msg.ack(St)))
    });

    let (io, codec) = handshake(&srv).await;
    // the packets are written at once, the server decodes them all
    for pkt in [
        Encoded::Packet(Packet::Disconnect(Box::default())),
        Encoded::Packet(Packet::Auth(Box::new(codec::Auth {
            auth_method: Some(ByteString::from_static("m")),
            ..Default::default()
        }))),
        Encoded::Packet(Packet::Subscribe(codec::Subscribe {
            packet_id: pid(1),
            id: None,
            user_properties: Default::default(),
            topic_filters: vec![(
                ByteString::from_static("a"),
                sub_opts(QoS::AtLeastOnce, false),
            )],
        })),
        Encoded::Packet(Packet::Unsubscribe(codec::Unsubscribe {
            packet_id: pid(2),
            user_properties: Default::default(),
            topic_filters: vec![ByteString::from_static("a")],
        })),
        Encoded::Publish(pkt_publish_to("a", QoS::AtLeastOnce, Some(3)), None),
    ] {
        io.encode(pkt, &codec).unwrap();
    }

    // no ack is sent for any of them and the connection is closed
    assert!(
        timeout(Millis(500), io.recv(&codec))
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(published.load(Relaxed), 0);
    assert_eq!(*proto.lock().unwrap(), vec!["disconnect"]);
}

/// STATEMENT: Protocol handlers that request disconnection currently close the connection
/// without a response packet.
///
/// HOW:
/// - Return `disconnect` for a ping and `disconnect_with` for an incoming disconnect.
/// - Send each packet on a separate connection and assert end of stream without a reply.
#[ntex::test]
async fn test_protocol_disconnect() {
    let srv = server::test_server(async |_| {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .protocol(async |msg: ProtocolMessage| {
                Ok::<_, TestError>(match msg {
                    ProtocolMessage::Ping(_) => msg.disconnect(),
                    ProtocolMessage::Disconnect(_) => msg.disconnect_with(codec::Disconnect::new(
                        codec::DisconnectReasonCode::ServerBusy,
                    )),
                    _ => msg.ack(),
                })
            })
            .build(connect)
    });

    // BUG: the DISCONNECT packet is dropped, the io is closed before it is
    // encoded, see src/v5/dispatcher.rs:680
    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(Packet::PingRequest), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());

    // a DISCONNECT is never answered with a DISCONNECT
    let (io, codec) = handshake(&srv).await;
    io.send(Encoded::Packet(Packet::Disconnect(Box::default())), &codec)
        .await
        .unwrap();
    assert!(io.recv(&codec).await.unwrap().is_none());
}
