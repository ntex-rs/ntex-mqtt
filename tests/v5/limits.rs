use super::*;

/// STATEMENT: The default 256 KiB packet limit accepts its exact boundary and rejects a packet
/// one byte larger.
///
/// HOW:
/// - Check the maximum packet size advertised by the server.
/// - Send a publish whose payload plus header totals 256 KiB and assert an acknowledgment.
/// - Send only the header declaring a one-byte-larger packet and assert a `PacketTooLarge`
///   disconnect.
#[ntex::test]
async fn test_default_max_size() {
    let srv = server::test_server(async move |_| {
        MqttServer::new(async move |p: Publish| {
            let _ = p.read_all().await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(err)) = msg {
                Ok(Some(
                    codec::Packet::from(codec::Disconnect::from_proto_error(err.get_ref())).into(),
                ))
            } else {
                Ok::<_, TestError>(None)
            }
        })
        .build(connect)
    });

    let (io, codec) = connect_raw(&srv).await;
    let ack = io.recv(&codec).await.unwrap().unwrap();
    let Packet::ConnectAck(ack) = packet(ack) else {
        panic!()
    };
    assert_eq!(ack.max_packet_size, Some(256 * 1024));

    // 13 bytes of fixed header, topic, packet id and properties length
    let publish = |payload_size| codec::Publish {
        payload_size,
        ..pkt_publish()
    };
    io.send(
        Encoded::Publish(
            publish(256 * 1024 - 13),
            Some(Bytes::from(vec![b'*'; 256 * 1024 - 13])),
        ),
        &codec,
    )
    .await
    .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    assert!(matches!(packet(pkt), Packet::PublishAck(_)));

    // The declared size is rejected before the payload is read. Sending the
    // payload can cause a TCP reset that hides DISCONNECT on Windows.
    io.send(Encoded::Publish(publish(256 * 1024 - 12), None), &codec)
        .await
        .unwrap();
    let pkt = io.recv(&codec).await.unwrap().unwrap();
    let Packet::Disconnect(pkt) = packet(pkt) else {
        panic!()
    };
    assert_eq!(pkt.reason_code, codec::DisconnectReasonCode::PacketTooLarge);
}

/// STATEMENT: A client uses the server's advertised keep-alive override to keep its connection
/// open.
///
/// HOW:
/// - Configure a one-second server keep-alive and connect a client that did not request
///   periodic pings.
/// - Assert the advertised value, run the client for 3.5 seconds, and check that its sink
///   remains open without a server timeout.
#[ntex::test]
async fn test_keepalive() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(async move |_| {
        let ka = ka2.clone();

        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| match msg {
                Control::Stop(Reason::Protocol(msg)) => {
                    if let &error::MqttProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ok::<_, TestError>(None)
                }
                _ => Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                )),
            })
            .build(async move |con: Connect| Ok::<_, TestError>(con.ack(St).keep_alive(1)))
    });

    // connect to server, client keep-alive is 0
    let client = connect_client(srv.addr()).await;
    // [MQTT-3.2.2-22] server advertises its keep-alive
    assert_eq!(client.packet().server_keepalive_sec, Some(1));

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    // client pings with the server keep-alive
    assert!(sink.is_open());
    sleep(Duration::from_millis(3500)).await;
    assert!(sink.is_open());
    assert!(!ka.load(Relaxed));
}

/// STATEMENT: Server keep-alive timeout follows the advertised override and is postponed by
/// incoming publishes.
///
/// HOW:
/// - Request a ten-second client keep-alive and assert the server advertises its two-second
///   override.
/// - Send two publishes half a second apart without automatic client pings.
/// - Check that the timeout flag is initially unset and becomes set after the longer idle wait.
#[ntex::test]
async fn test_keepalive2() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::test_server(async move |_| {
        let ka = ka2.clone();

        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| match msg {
                Control::Stop(Reason::Protocol(msg)) => {
                    if let &error::MqttProtocolError::KeepAliveTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ok::<_, TestError>(None)
                }
                _ => Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                )),
            })
            .build(async move |con: Connect| Ok::<_, TestError>(con.ack(St).keep_alive(2)))
    });

    // client that does not ping
    let io = srv.connect().await.unwrap();
    let codec = codec::Codec::default();
    io.send(
        Encoded::Packet(
            codec::Connect {
                keep_alive: 10,
                ..Default::default()
            }
            .client_id("user")
            .into(),
        ),
        &codec,
    )
    .await
    .unwrap();
    let ack = io.recv(&codec).await.unwrap().unwrap();
    let Decoded::Packet(codec::Packet::ConnectAck(ack), _) = ack else {
        panic!("{ack:?}")
    };
    assert_eq!(ack.server_keepalive_sec, Some(2));

    for id in 1..=2 {
        io.send(
            Encoded::Publish(
                codec::Publish {
                    packet_id: NonZeroU16::new(id),
                    ..pkt_publish()
                },
                None,
            ),
            &codec,
        )
        .await
        .unwrap();
        let _ = io.recv(&codec).await.unwrap().unwrap();
        sleep(Duration::from_millis(500)).await;
    }

    // closed after 1.5 times of the server keep-alive
    sleep(Duration::from_millis(1500)).await;
    assert!(!ka.load(Relaxed));
    sleep(Duration::from_millis(2500)).await;
    assert!(ka.load(Relaxed));
}

/// STATEMENT: An incomplete frame triggers the configured frame-read timeout.
///
/// HOW:
/// - Configure frame-read timing and a one-second server keep-alive, then complete a handshake
///   and send a publish.
/// - Send only five bytes of the next encoded publish, wait two seconds, and assert that the
///   control service observed `ReadTimeout`.
#[ntex::test]
async fn test_keepalive3() {
    let ka = Arc::new(AtomicBool::new(false));
    let ka2 = ka.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let ka = ka2.clone();

        MqttServer::new(async move |p: Publish| Ok::<_, TestError>(p.ack()))
            .control(async move |msg| match msg {
                Control::Stop(Reason::Protocol(msg)) => {
                    if let &error::MqttProtocolError::ReadTimeout = msg.get_ref() {
                        ka.store(true, Relaxed);
                    }
                    Ok::<_, TestError>(None)
                }
                _ => Ok(Some(
                    codec::Packet::from(codec::Disconnect::default()).into(),
                )),
            })
            .build(async move |con: Connect| Ok::<_, TestError>(con.ack(St).keep_alive(1)))
    })
    .config(
        SharedCfg::new("MQTT").add(IoConfig::new().set_frame_read_rate(
            Seconds(1),
            Seconds(5),
            256,
        )),
    )
    .start();

    // connect to server
    let (io, codec) = handshake(&srv).await;

    io.send(Encoded::Publish(pkt_publish(), None), &codec)
        .await
        .unwrap();
    sleep(Duration::from_millis(500)).await;

    let mut buf = BytePages::default();
    let pkt = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(2)),
            ..pkt_publish()
        },
        None,
    );
    codec.encode(pkt, &mut buf).unwrap();
    io.encode_slice(&buf.freeze()[..5]).unwrap();
    sleep(Duration::from_millis(2000)).await;

    assert!(ka.load(Relaxed));
}

/// STATEMENT: A frame exceeding the maximum read duration times out even when its byte rate is
/// sufficient.
///
/// HOW:
/// - Configure a ten-byte read rate, one-second intervals, and a two-second maximum while
///   disabling the packet-size limit.
/// - Send a large publish in small fragments; assert no early timeout after the initial fragments.
/// - Send another fragment that satisfies the rate and assert `ReadTimeout` after the maximum
///   duration.
#[ntex::test]
async fn test_frame_read_rate() -> std::io::Result<()> {
    let check = Arc::new(AtomicBool::new(false));
    let check2 = check.clone();

    let srv = server::TestServerBuilder::new(async move |_| {
        let check = check2.clone();

        MqttServer::new(async move |p: Publish| {
            let _ = p.read_all().await;
            Ok::<_, TestError>(p.ack())
        })
        .control(async move |msg| {
            if let Control::Stop(Reason::Protocol(msg)) = msg
                && msg.get_ref() == &error::MqttProtocolError::ReadTimeout
            {
                check.store(true, Relaxed);
            }
            Ok::<_, TestError>(None)
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT")
            .add(IoConfig::new().set_frame_read_rate(Seconds(1), Seconds(2), 10))
            .add(
                MqttServiceConfig::new()
                    .set_min_chunk_size(32 * 1024)
                    .set_max_size(0),
            ),
    )
    .start();

    let (io, codec) = handshake(&srv).await;

    let p = Encoded::Publish(
        codec::Publish {
            packet_id: Some(pid(3)),
            payload_size: 270 * 1024,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; 270 * 1024])),
    );

    let mut buf = BytePages::default();
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();

    io.encode_slice(&buf[..50]).unwrap();
    buf.advance_to(50);
    sleep(Millis(100)).await;
    io.encode_slice(&buf[..30]).unwrap();
    buf.advance_to(30);
    sleep(Millis(1500)).await;
    assert!(!check.load(Relaxed));

    // the read rate is satisfied, but the max timeout is reached
    io.encode_slice(&buf[..12]).unwrap();
    buf.advance_to(12);
    sleep(Millis(1000)).await;
    assert!(check.load(Relaxed));

    Ok(())
}

/// STATEMENT: A handshake-specific outbound limit overrides the configured server send limit.
///
/// HOW:
/// - Set the configured maximum send count to `10`, the handshake override to `15`, and the
///   client receive maximum to `100`.
/// - Assert server sink credit of `15` and verify that a client QoS 1 publish succeeds.
#[ntex::test]
async fn test_max_outbound() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move |_| {
        MqttServer::new(async |con: &Session<St>| {
            assert_eq!(con.sink().credit(), 15);
            Ok::<_, Infallible>(fn_service(async |p: Publish| Ok::<_, TestError>(p.ack())))
        })
        .build(async |hnd: Connect| Ok::<_, TestError>(hnd.ack(St).max_send(Some(15))))
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_send(10)))
    .start();

    // connect to server
    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .max_receive(100),
    )
    .await
    .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("test"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());

    sink.close();
    Ok(())
}

/// STATEMENT: Without a handshake override, configured send credit limits a larger client
/// receive maximum.
///
/// HOW:
/// - Set the server maximum send count to `10` and the client receive maximum to `100`.
/// - Assert server sink credit of `10` and verify that a client QoS 1 publish succeeds.
#[ntex::test]
async fn test_max_outbound2() -> std::io::Result<()> {
    let srv = server::TestServerBuilder::new(async move |_| {
        MqttServer::new(async |con: &Session<St>| {
            assert_eq!(con.sink().credit(), 10);
            Ok::<_, Infallible>(fn_service(async |p: Publish| Ok::<_, TestError>(p.ack())))
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_send(10)))
    .start();

    // connect to server
    let client = try_connect_client(
        client::Connect::new(srv.addr())
            .client_id("user")
            .max_receive(100),
    )
    .await
    .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("test"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());

    sink.close();
    Ok(())
}
