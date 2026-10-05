use super::*;

#[ntex::test]
async fn test_sink_encoder_error_pub_qos1() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(_)) = msg {
                Ok::<_, TestError>(None)
            } else {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(async move |con: Connect| {
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
                    Err(error::SendPacketError::Encode(
                        error::EncodeError::OverMaxPacketSize
                    ))
                );
            });
            Ok::<_, TestError>(con.ack(St))
        })
    });

    // connect to server
    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .max_packet_size(30),
    )
    .await
    .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
}

#[ntex::test]
async fn test_sink_encoder_error_pub_qos0() {
    let srv = server::test_server(async move || {
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(_)) = msg {
                Ok::<_, TestError>(None)
            } else {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(async move |con: Connect| {
            let builder = con.sink().publish("test").properties(|props| {
                props.user_properties.push((
                    "ssssssssssssssssssssssssssssssssssss".into(),
                    "ssssssssssssssssssssssssssssssssssss".into(),
                ));
            });
            let res = builder.send_at_most_once(Bytes::new()).await;
            assert_eq!(
                res,
                Err(error::SendPacketError::Encode(
                    error::EncodeError::OverMaxPacketSize
                ))
            );
            Ok::<_, TestError>(con.ack(St))
        })
    });

    // connect to server
    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .max_packet_size(30),
    )
    .await
    .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
}

/// Make sure we can publish message to client after local codec error
#[ntex::test]
async fn test_sink_success_after_encoder_error_qos1() {
    let success = Arc::new(AtomicBool::new(false));
    let success2 = success.clone();

    let srv = server::test_server(async move || {
        let success = success2.clone();
        MqttServer::new(async move |p: Publish| {
            sleep(Duration::from_millis(50)).await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(_)) = msg {
                Ok::<_, TestError>(None)
            } else {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                ))
            }
        })
        .build(async move |con: Connect| {
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
                    Err(error::SendPacketError::Encode(
                        error::EncodeError::OverMaxPacketSize
                    ))
                );

                let res = sink.publish("test").send_at_least_once(Bytes::new()).await;
                assert!(res.is_ok());
                success.store(true, Relaxed);
            });
            Ok::<_, TestError>(con.ack(St))
        })
    });

    // connect to server
    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .max_packet_size(30),
    )
    .await
    .unwrap();

    let sink = client.sink();

    async fn publish(pkt: Publish) -> Result<PublishAck, TestError> {
        Ok(pkt.ack())
    }

    let router = client.resource("test", publish);
    ntex::rt::spawn(router.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    assert!(success.load(Relaxed));
}

#[ntex::test]
async fn test_sink_ready() -> std::io::Result<()> {
    let srv = server::test_server(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(
            async move |packet: Connect| {
                let sink = packet.sink();
                let mut ready = Box::pin(sink.ready());
                let res = lazy(|cx| Pin::new(&mut ready).poll(cx)).await;
                assert!(res.is_pending());
                assert!(!sink.is_ready());

                let sink = sink.clone();
                ntex::rt::spawn(async move {
                    sink.ready().await;
                    assert!(sink.is_ready());
                    sink.publish("/test")
                        .send_at_most_once(Bytes::from_static(b"body"))
                        .await
                        .unwrap();
                });

                Ok::<_, TestError>(packet.ack(St))
            },
        )
    });

    // connect to server
    let (io, codec) = connect_raw(&srv).await;
    let ack = io.recv(&codec).await.unwrap().unwrap();
    assert_eq!(
        packet(ack),
        Packet::ConnectAck(Box::new(codec::ConnectAck {
            max_qos: QoS::AtLeastOnce,
            receive_max: pid(16),
            topic_alias_max: 32,
            server_keepalive_sec: Some(30),
            max_packet_size: Some(256 * 1024),
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
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack())).build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

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

    let res = sink
        .publish(ByteString::from_static("test3"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());

    assert_eq!(*results.borrow(), &[pid(1), pid(2)]);

    sink.close();
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
async fn test_publish_sink_disconnect() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = ::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let tx = tx.clone();
            let val = val.clone();
            let sink = con.sink().clone();

            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let _st = SetOnDrop(val.clone(), tx.lock().unwrap().take());
                sink.close();
                sleep(Seconds(999)).await;
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let _ = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once(Bytes::new())
        .await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_peergone_after_sink_disconnect() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let sink = con.sink().clone();
            rt::spawn(async move {
                sleep(Seconds(1)).await;
                sink.close();
            });
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                Ok::<_, TestError>(p.ack())
            }))
        })
        .control(async move |pkt: Control<_>| {
            if let Control::Stop(Reason::PeerGone(_)) = pkt {
                val.store(true, Relaxed);
                let _ = tx.lock().unwrap().take().unwrap().send(());
            }
            Ok::<_, TestError>(None)
        })
        .build(connect)
    });

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let _ = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once(Bytes::new())
        .await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

#[ntex::test]
async fn test_sink_close_with_no_reason() -> std::io::Result<()> {
    let val = Arc::new(AtomicBool::new(false));
    let val2 = val.clone();
    let (tx, rx) = ::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let srv = server::test_server(async move || {
        let tx = tx.clone();
        let val = val2.clone();
        MqttServer::new(async move |con: &Session<St>| {
            let tx = tx.clone();
            let val = val.clone();
            let sink = con.sink().clone();

            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let _st = SetOnDrop(val.clone(), tx.lock().unwrap().take());
                sink.close_with_no_reason();
                sleep(Seconds(999)).await;
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    });

    // connect to server
    let client = Pipeline::new(
        SharedCfg::new("client").build(),
        client::MqttConnector::new(),
    )
    .call(client::Connect::new(srv.addr()).client_id("user"))
    .await
    .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let _ = sink
        .publish(ByteString::from_static("test1"))
        .send_at_least_once(Bytes::new())
        .await;

    let _ = rx.await;
    assert!(val.load(Relaxed));

    Ok(())
}

/// Client side of the sink api: subscribe, unsubscribe and publish builders
#[ntex::test]
async fn test_client_sink_api() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let log2 = log.clone();

    let srv = server::test_server(async move || {
        let (log, log3) = (log2.clone(), log2.clone());
        MqttServer::new(move |p: Publish| {
            let log = log.clone();
            async move {
                let pkt = p.packet().clone();
                let payload = p.read_all().await.unwrap();
                log.lock().unwrap().push(format!(
                    "publish {} retain={} expiry={:?} props={:?} {payload:?}",
                    pkt.topic,
                    pkt.retain,
                    pkt.properties.message_expiry_interval,
                    pkt.properties.user_properties
                ));
                Ok::<_, TestError>(p.ack())
            }
        })
        .protocol(move |msg: ProtocolMessage| {
            let log = log3.clone();
            async move {
                Ok::<_, TestError>(match msg {
                    ProtocolMessage::Subscribe(mut sub) => {
                        log.lock().unwrap().push(format!(
                            "subscribe id={:?} props={:?}",
                            sub.packet().id,
                            sub.packet().user_properties
                        ));
                        for mut s in sub.iter_mut() {
                            let qos = s.options().qos;
                            s.confirm(qos);
                        }
                        sub.ack()
                    }
                    ProtocolMessage::Unsubscribe(unsub) => {
                        log.lock().unwrap().push(format!(
                            "unsubscribe props={:?}",
                            unsub.packet().user_properties
                        ));
                        unsub.ack()
                    }
                    msg => msg.ack(),
                })
            }
        })
        .build(connect)
    });

    let client = connect_client(srv.addr()).await;
    let sink = client.sink();
    rt::spawn(client.start(async |msg: client::ProtocolMessage| Ok::<_, ()>(msg.ack())));

    // the sink is ready for a new publish
    assert!(sink.is_ready());
    assert!(sink.ready().await);
    assert!(!sink.is_disconnect_recv());
    assert_eq!(sink.credit(), 16);
    assert!(format!("{sink:?}").contains("MqttSink"));

    // subscribe builder
    let sub = sink
        .subscribe(NonZeroU32::new(7))
        .packet_id(3)
        .topic_filter(
            ByteString::from_static("a"),
            sub_opts(QoS::AtLeastOnce, false),
        )
        .topic_filter(
            ByteString::from_static("b"),
            sub_opts(QoS::AtMostOnce, false),
        )
        .property(ByteString::from_static("k"), ByteString::from_static("v"));
    assert!(format!("{sub:?}").contains("SubscribeBuilder"));
    assert!(sub.size() > 0);
    let ack = sub.send().await.unwrap();
    assert_eq!(ack.packet_id, pid(3));
    assert_eq!(
        ack.status,
        vec![
            codec::SubscribeAckReason::GrantedQos1,
            codec::SubscribeAckReason::GrantedQos0
        ]
    );

    // unsubscribe builder
    let unsub = sink
        .unsubscribe()
        .packet_id(4)
        .topic_filter(ByteString::from_static("a"))
        .property(ByteString::from_static("k"), ByteString::from_static("v"));
    assert!(format!("{unsub:?}").contains("UnsubscribeBuilder"));
    assert!(unsub.size() > 0);
    let ack = unsub.send().await.unwrap();
    assert_eq!(ack.packet_id, pid(4));
    assert_eq!(ack.status, vec![codec::UnsubscribeAckReason::Success]);

    // publish builder, properties are set before and after it is built
    let mut builder = sink
        .publish("p")
        .dup(false)
        .retain(true)
        .properties(|p| p.message_expiry_interval = Some(30));
    builder.set_properties(|p| {
        p.user_properties
            .push((ByteString::from_static("k"), ByteString::from_static("v")))
    });
    assert!(format!("{builder:?}").contains("PublishBuilder"));
    assert!(builder.size(3) > 3);
    builder
        .send_at_least_once(Bytes::from_static(b"abc"))
        .await
        .unwrap();

    // streamed publish, the payload is sent in chunks
    let (fut, stream) = sink.publish("stream").packet_id(9).stream_at_least_once(6);
    assert!(format!("{stream:?}").contains("StreamingPayload"));
    let send = async {
        stream.send(Bytes::from_static(b"abc")).await.unwrap();
        stream.send(Bytes::from_static(b"def")).await.unwrap();
    };
    let (ack, ()) = join(fut, send).await;
    assert_eq!(ack.unwrap().packet_id, pid(9));

    // QoS0 streamed publish
    let stream = sink
        .publish("stream0")
        .stream_at_most_once(3)
        .await
        .unwrap();
    stream.send(Bytes::from_static(b"xyz")).await.unwrap();

    wait_until(|| log.lock().unwrap().len() == 5).await;
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "subscribe id=Some(7) props=[(\"k\", \"v\")]".to_owned(),
            "unsubscribe props=[(\"k\", \"v\")]".to_owned(),
            "publish p retain=true expiry=Some(30) props=[(\"k\", \"v\")] b\"abc\"".to_owned(),
            "publish stream retain=false expiry=None props=[] b\"abcdef\"".to_owned(),
            "publish stream0 retain=false expiry=None props=[] b\"xyz\"".to_owned(),
        ]
    );

    // the connection is aborted
    sink.force_close();
    wait_until(|| !sink.is_open()).await;
    assert!(!sink.is_ready());
    assert!(!sink.ready().await);
}
