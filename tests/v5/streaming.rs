use super::*;

#[ntex::test]
async fn test_simple_streaming() -> std::io::Result<()> {
    let chunks = Arc::new(Mutex::new(Vec::new()));
    let chunks2 = chunks.clone();

    let srv = server::TestServerBuilder::new(async move || {
        let chunks = chunks2.clone();
        MqttServer::new(async move |p: Publish| {
            let chunks = chunks.clone();
            while let Ok(Some(chunk)) = p.read().await {
                chunks.lock().unwrap().push(chunk);
            }
            Ok::<_, TestError>(p.ack())
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"111111")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    // pkt 2
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(5);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"22")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"222")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    // pkt 3
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .stream_at_least_once(2);
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

    let (fut, _) = sink
        .publish(ByteString::from_static("#"))
        .stream_at_least_once(12);
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

    let srv = server::TestServerBuilder::new(async move || {
        let chunks = chunks2.clone();
        MqttServer::new(async move |mut p: Publish| {
            let chunks = chunks.clone();
            assert!(!p.dup());
            assert!(p.retain());
            assert_eq!(p.id(), Some(pid(1)));
            assert_eq!(p.qos(), QoS::AtLeastOnce);
            assert_eq!(p.topic().path(), "test");
            assert_eq!(p.topic_mut().path(), "test");
            assert_eq!(p.publish_topic(), "test");
            assert_eq!(p.packet_size(), 19);
            assert_eq!(p.payload_size(), 10);
            let chunk = p.read_all().await.unwrap();
            chunks.lock().unwrap().push(chunk);
            Ok::<_, TestError>(p.ack())
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_min_chunk_size(4)))
    .start();

    // connect to server
    let client = connect_client(srv.addr()).await;

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    // pkt 1
    let (fut, payload) = sink
        .publish(ByteString::from_static("test"))
        .retain(true)
        .stream_at_least_once(10);

    ntex_rt::spawn(async move {
        payload.send(Bytes::from_static(b"1111")).await.unwrap();
        sleep(Millis(50)).await;
        payload.send(Bytes::from_static(b"111111")).await.unwrap();
    });

    let res = fut.await;
    assert!(res.is_ok());

    assert_eq!(
        &chunks.lock().unwrap()[..],
        vec![Bytes::from_static(b"1111111111"),]
    );
}

/// Payload chunk waiting for write backpressure fails when the peer is gone
#[ntex::test]
async fn test_streaming_waiter_peer_gone() {
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = Arc::new(Mutex::new(None));
    let (sent2, result2) = (sent.clone(), result.clone());
    let srv = server::TestServerBuilder::new(async move || {
        let sent = sent2.clone();
        let result = result2.clone();
        MqttServer::new(async move |ses: &Session<St>| {
            let sink = ses.sink().clone();
            let sent = sent.clone();
            let result = result.clone();
            Ok::<_, Infallible>(fn_service(async move |p: Publish| {
                let sink = sink.clone();
                let sent = sent.clone();
                let result = result.clone();
                rt::spawn(async move {
                    let chunk = Bytes::from(vec![0u8; 65536]);
                    let stream = sink
                        .publish("test")
                        .stream_at_most_once(4000 * 65536)
                        .await
                        .unwrap();
                    let res = loop {
                        if let Err(e) = stream.send(chunk.clone()).await {
                            break e;
                        }
                        sent.fetch_add(1, Relaxed);
                        // let the dispatcher process write backpressure
                        sleep(Millis(1)).await;
                    };
                    *result.lock().unwrap() = Some(res);
                });
                Ok::<_, TestError>(p.ack())
            }))
        })
        .build(connect)
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_size(0)))
    .start();

    let (io, codec) = handshake(&srv).await;

    // trigger server streaming PUBLISH, the payload is not read
    let pkt = codec::Publish {
        qos: QoS::AtMostOnce,
        packet_id: None,
        ..pkt_publish()
    };
    io.send(Encoded::Publish(pkt, None), &codec).await.unwrap();

    // wait for write backpressure
    let mut last = usize::MAX;
    for _ in 0..50 {
        sleep(Millis(200)).await;
        let n = sent.load(Relaxed);
        if n > 0 && n == last {
            break;
        }
        last = n;
    }
    assert_eq!(sent.load(Relaxed), last);
    assert!(result.lock().unwrap().is_none());

    drop(io);
    let mut res = None;
    for _ in 0..50 {
        sleep(Millis(100)).await;
        res = result.lock().unwrap().take();
        if res.is_some() {
            break;
        }
    }
    assert_eq!(res, Some(error::SendPacketError::Disconnected));
}

/// The payload of a streaming publish is read while the publish fills the
/// response queue
#[ntex::test]
async fn test_streaming_publish_full_queue() {
    const SIZE: usize = 64 * 1024;

    let received = Arc::new(AtomicBool::new(false));
    let received2 = received.clone();
    let srv = server::TestServerBuilder::new(async move || {
        let received = received2.clone();
        MqttServer::new(async move |p: Publish| {
            if p.payload_size() != 1 && p.read_all().await.map_err(|_| TestError)?.len() == SIZE {
                received.store(true, Relaxed);
            }
            Ok::<_, TestError>(p.ack())
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_queue(1)
                .set_min_chunk_size(0),
        ),
    )
    .start();

    let (io, codec) = handshake(&srv).await;

    // the pending at most once publish fills the queue, the rest of its
    // payload arrives after it is dispatched
    let mut buf = BytePages::default();
    let p = Encoded::Publish(
        codec::Publish {
            qos: QoS::AtMostOnce,
            packet_id: None,
            payload_size: SIZE as u32,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; SIZE])),
    );
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();
    io.encode_slice(&buf[..1024]).unwrap();
    buf.advance_to(1024);
    io.flush(true).await.unwrap();
    sleep(Millis(100)).await;
    io.encode_slice(&buf).unwrap();
    io.encode(
        Encoded::Publish(
            codec::Publish {
                payload_size: 1,
                ..pkt_publish()
            },
            Some(Bytes::from_static(b"1")),
        ),
        &codec,
    )
    .unwrap();

    let res = ntex::time::timeout(Seconds(5), io.recv(&codec)).await;
    assert!(
        matches!(res, Ok(Ok(Some(Decoded::Packet(Packet::PublishAck(_), _))))),
        "{res:?}"
    );
    for _ in 0..100 {
        if received.load(Relaxed) {
            break;
        }
        sleep(Millis(20)).await;
    }
    assert!(received.load(Relaxed));
}

/// Payload chunks do not keep queue slots while the publish handler is
/// pending, packets after the publish are dispatched
#[ntex::test]
async fn test_streaming_publish_chunk_slots() {
    // below the receive size limit, reading continues after the payload
    const SIZE: usize = 16 * 1024;

    let gate = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicBool::new(false));
    let (gate2, received2) = (gate.clone(), received.clone());
    let srv = server::TestServerBuilder::new(async move || {
        let (gate, received) = (gate2.clone(), received2.clone());
        MqttServer::new(async move |p: Publish| {
            if p.payload_size() == 1 {
                received.store(true, Relaxed);
            } else if p.read_all().await.map_err(|_| TestError)?.len() == SIZE {
                while !gate.load(Relaxed) {
                    sleep(Millis(10)).await;
                }
            } else {
                return Err(TestError);
            }
            Ok::<_, TestError>(p.ack())
        })
        .build(connect)
    })
    .config(
        SharedCfg::new("MQTT").add(
            MqttServiceConfig::new()
                .set_max_queue(2)
                .set_min_chunk_size(0),
        ),
    )
    .start();

    let (io, codec) = handshake(&srv).await;

    // the payload arrives in chunks after the publish is dispatched
    let mut buf = BytePages::default();
    let p = Encoded::Publish(
        codec::Publish {
            payload_size: SIZE as u32,
            ..pkt_publish()
        },
        Some(Bytes::from(vec![b'*'; SIZE])),
    );
    codec.encode(p, &mut buf).unwrap();
    let mut buf = buf.freeze();
    io.encode_slice(&buf[..1024]).unwrap();
    buf.advance_to(1024);
    io.flush(true).await.unwrap();
    sleep(Millis(100)).await;
    io.encode_slice(&buf).unwrap();

    // the at most once publish is read, the queue is not full
    io.encode(
        Encoded::Publish(
            codec::Publish {
                qos: QoS::AtMostOnce,
                packet_id: None,
                payload_size: 1,
                ..pkt_publish()
            },
            Some(Bytes::from_static(b"1")),
        ),
        &codec,
    )
    .unwrap();
    io.flush(true).await.unwrap();

    for _ in 0..100 {
        if received.load(Relaxed) {
            break;
        }
        sleep(Millis(20)).await;
    }
    assert!(received.load(Relaxed));

    gate.store(true, Relaxed);
    let res = ntex::time::timeout(Seconds(5), io.recv(&codec)).await;
    assert!(
        matches!(res, Ok(Ok(Some(Decoded::Packet(Packet::PublishAck(_), _))))),
        "{res:?}"
    );
}

/// The connection is closed if the publish handler does not read a streamed
/// payload and the buffered part exceeds the configured limit
#[ntex::test]
async fn test_payload_not_read() {
    let srv = server::TestServerBuilder::new(async || {
        MqttServer::new(async |p: Publish| Ok::<_, TestError>(p.ack()))
            .build(async |msg: Connect| Ok::<_, TestError>(msg.ack(St)))
    })
    .config(SharedCfg::new("MQTT").add(MqttServiceConfig::new().set_max_payload_buffer_size(4)))
    .start();

    let (io, codec) = handshake(&srv).await;
    let mut pkt = pkt_publish_to("a", QoS::AtMostOnce, None);
    pkt.payload_size = 8;
    io.send(
        Encoded::Publish(pkt, Some(Bytes::from_static(b"abcd"))),
        &codec,
    )
    .await
    .unwrap();

    // the payload stream is dropped, the connection is reset
    let res = timeout(Millis(500), io.recv(&codec))
        .await
        .expect("the connection must be closed");
    assert!(res.is_err() || res.unwrap().is_none());
}
