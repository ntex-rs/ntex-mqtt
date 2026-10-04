use std::{cell::Cell, cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc};

use ntex_service::{Ctx, Service};
use ntex_util::HashMap;
use ntex_util::future::{Either, join};

use crate::error::{DecodeError, DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::inflight::InFlightServiceImpl;
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::types::packet_type;
use crate::v3::codec::{self, Decoded, Encoded, Packet};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{QoS, Session, control::ProtocolMessageKind, publish::Publish};

use super::control::{ProtocolMessage, ProtocolMessageAck};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<St, T, C, E>(
    sink: Rc<MqttShared>,
    inflight: u16,
    max_buffer_size: usize,
    publish: T,
    control: C,
) -> impl Service<Session<St>, Decoded, Res = Option<Encoded>, Error = DispatcherError<E>>
where
    St: 'static,
    E: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E> + 'static,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
{
    // limit number of in-flight publish messages
    InFlightServiceImpl::new(
        inflight,
        0,
        Dispatcher::new(
            sink,
            publish,
            control.map_err(DispatcherError::Service),
            max_buffer_size,
        ),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C, E> {
    publish: T,
    inner: Inner<C>,
    max_buffer_size: usize,
    st: PhantomData<(St, E)>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    payload: Cell<Option<PlSender>>,
    discard_payload: Cell<bool>,
    inflight: RefCell<HashMap<NonZeroU16, InFlight>>,
}

/// State of an incoming publish packet id
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InFlight {
    /// Publish is being handled
    Publish(QoS),
    /// `QoS 2` publish is handled and `PublishReceived` is sent, waiting for `PublishRelease`
    Received,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    St: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E>,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    E: 'static,
{
    pub(crate) fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        max_buffer_size: usize,
    ) -> Self {
        Self {
            publish,
            max_buffer_size,
            inner: Inner {
                sink,
                control,
                payload: Cell::new(None),
                discard_payload: Cell::new(false),
                inflight: RefCell::new(HashMap::default()),
            },
            st: PhantomData,
        }
    }
}

impl<C> Inner<C> {
    fn drop_payload<PErr>(&self, err: &PErr)
    where
        PErr: Clone,
        PayloadError: From<PErr>,
    {
        if let Some(pl) = self.payload.take() {
            pl.set_error(err.clone().into());
        }
    }

    /// Acknowledge handled publish packet
    ///
    /// `QoS 1` publish is acknowledged with `PublishAck` [MQTT-4.3.2-2], `QoS 2` publish
    /// with `PublishReceived`, the packet id stays in use until `PublishRelease` [MQTT-4.3.3-2]
    fn publish_ack(&self, packet_id: NonZeroU16) -> Encoded {
        let mut inflight = self.inflight.borrow_mut();
        if inflight.get(&packet_id) == Some(&InFlight::Publish(QoS::ExactlyOnce)) {
            inflight.insert(packet_id, InFlight::Received);
            Encoded::Packet(Packet::PublishReceived { packet_id })
        } else {
            inflight.remove(&packet_id);
            Encoded::Packet(Packet::PublishAck { packet_id })
        }
    }
}

impl<St, T, C, E> Service<Session<St>, Decoded> for Dispatcher<St, T, C, E>
where
    St: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E> + 'static,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    E: 'static,
{
    type Res = Option<Encoded>;
    type Error = DispatcherError<E>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, Session<St>>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), ctx.ready(&self.inner.control)).await;
        // payload backpressure, reading pauses while the streamed payload buffer
        // is above its high watermark, the connection is closed if the payload
        // receiver is dropped before the stream ends
        if res1.is_ok()
            && res2.is_ok()
            && let Some(pl) = self.inner.payload.take()
        {
            self.inner.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }

        res1.map_err(DispatcherError::Service)?;
        res2?;
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, Session<St>>) {
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();

        ctx.shutdown(&self.inner.control).await;
        ctx.shutdown(&self.publish).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        packet: Decoded,
        ctx: Ctx<'_, Self, Session<St>>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Dispatch packet: {packet:#?}");

        match packet {
            Decoded::Publish(publish, payload, size) => {
                // the Topic Name must not contain wildcards, [MQTT-3.3.2-2] (MQTT 3.1.1, 3.3.2)
                if publish.topic.contains(['#', '+']) {
                    return Err(SpecViolation::Pub_3_3_2_2.into());
                }

                let inner = &self.inner;
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    let state = inner.inflight.borrow().get(&pid).copied();
                    match state {
                        None => {
                            inner
                                .inflight
                                .borrow_mut()
                                .insert(pid, InFlight::Publish(publish.qos));
                        }
                        // a re-delivery keeps the packet id [MQTT-2.3.1-2], [MQTT-3.3.1-1]
                        // and is not a new publication until the ack is sent [MQTT-4.3.2-2],
                        // the ack of the first delivery acks both
                        Some(InFlight::Publish(qos)) if publish.dup && qos == publish.qos => {
                            log::trace!("Re-delivered publish packet is ignored: {pid:?}");
                            if publish.payload_size != payload.len() as u32 {
                                inner.discard_payload.set(true);
                            }
                            return Ok(None);
                        }
                        // until PUBREL, PUBLISH with the same packet id is acked by PUBREC
                        // and is not delivered [MQTT-4.3.3-2]
                        Some(InFlight::Received)
                            if publish.dup && publish.qos == QoS::ExactlyOnce =>
                        {
                            log::trace!("Re-delivered publish packet is received: {pid:?}");
                            if publish.payload_size != payload.len() as u32 {
                                inner.discard_payload.set(true);
                            }
                            return Ok(Some(Encoded::Packet(Packet::PublishReceived {
                                packet_id: pid,
                            })));
                        }
                        Some(_) => {
                            log::trace!("Duplicated packet id for publish packet: {pid:?}");
                            return Err(SpecViolation::PacketId_2_2_1_3_Pub.into());
                        }
                    }
                }

                let payload = if publish.payload_size == payload.len() as u32 {
                    Payload::from_bytes(payload)
                } else {
                    let (pl, sender) = Payload::from_stream(payload, self.max_buffer_size);
                    self.inner.payload.set(Some(sender));
                    pl
                };

                publish_fn(
                    &self.publish,
                    Publish::new(publish, payload, size),
                    packet_id,
                    inner,
                    ctx,
                )
                .await
            }
            Decoded::PayloadChunk(buf, eof) => {
                if self.inner.discard_payload.get() {
                    if eof {
                        self.inner.discard_payload.set(false);
                    }
                    Ok(None)
                } else if let Some(pl) = self.inner.payload.take() {
                    pl.feed_data(buf);
                    if eof {
                        pl.feed_eof();
                    } else {
                        self.inner.payload.set(Some(pl));
                    }
                    Ok(None)
                } else {
                    // the publish of the chunk failed, the connection is closing
                    Err(MqttProtocolError::Decode(DecodeError::UnexpectedPayload).into())
                }
            }
            Decoded::Packet(Packet::PublishAck { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishReceived { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishComplete { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishRelease { packet_id }, _) => {
                let state = self.inner.inflight.borrow().get(&packet_id).copied();
                match state {
                    Some(InFlight::Received) => {
                        self.inner
                            .control(ProtocolMessage::pubrel(packet_id), ctx)
                            .await
                    }
                    // PUBREL is a response to PUBREC [MQTT-4.3.3-1]
                    Some(InFlight::Publish(_)) => Err(MqttProtocolError::unexpected_packet(
                        packet_type::PUBREL,
                        "PublishRelease packet before PublishReceived",
                    )
                    .into()),
                    None => {
                        // PUBREL is re-sent after a session resumes [MQTT-4.4.0-1], the
                        // release could be already completed, PUBCOMP is required [MQTT-4.3.3-2]
                        log::trace!("Unknown packet-id in PublishRelease packet: {packet_id:?}");
                        Ok(Some(Encoded::Packet(Packet::PublishComplete { packet_id })))
                    }
                }
            }
            Decoded::Packet(Packet::SubscribeAck { packet_id, status }, _) => {
                if let Err(e) = self
                    .inner
                    .sink
                    .pkt_ack(Ack::Subscribe { packet_id, status })
                {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::UnsubscribeAck { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PingResponse, _) => Ok(None),
            Decoded::Packet(
                pkt @ (Packet::Connect(_)
                | Packet::ConnectAck(_)
                | Packet::PingRequest
                | Packet::Disconnect
                | Packet::Subscribe { .. }
                | Packet::Unsubscribe { .. }),
                _,
            ) => Err(MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            )
            .into()),
        }
    }
}

async fn publish_fn<'f, St, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<C>,
    ctx: Ctx<'f, Dispatcher<St, T, C, E>, Session<St>>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    E: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E>,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    let res = ctx.call(svc, pkt).await.map_err(DispatcherError::Service)?;
    match res {
        Either::Left(()) => {
            log::trace!("Publish result for packet {packet_id:?} is ready");

            Ok(packet_id.map(|packet_id| inner.publish_ack(packet_id)))
        }
        Either::Right(pkt) => {
            let (pkt, payload, size) = pkt.into_inner();
            inner
                .control(ProtocolMessage::publish(pkt, payload, size), ctx)
                .await
        }
    }
}

impl<C> Inner<C> {
    async fn control<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, Session<St>>,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<
                Session<St>,
                ProtocolMessage,
                Res = ProtocolMessageAck,
                Error = DispatcherError<E>,
            >,
    {
        let packet = match ctx
            .call(&self.control, pkt)
            .await
            .inspect_err(|_| {
                self.drop_payload(&PayloadError::Service);
                self.sink.close();
            })?
            .result
        {
            ProtocolMessageKind::Ping => Some(Encoded::Packet(codec::Packet::PingResponse)),
            ProtocolMessageKind::PublishAck(id) => Some(self.publish_ack(id)),
            ProtocolMessageKind::PublishRelease(id) => {
                self.inflight.borrow_mut().remove(&id);
                Some(Encoded::Packet(Packet::PublishComplete { packet_id: id }))
            }
            ProtocolMessageKind::Subscribe(_) | ProtocolMessageKind::Unsubscribe(_) => {
                unreachable!()
            }
            ProtocolMessageKind::Disconnect => {
                self.drop_payload(&PayloadError::Service);
                self.sink.close();
                None
            }
            ProtocolMessageKind::Nothing => None,
        };

        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_service};
    use ntex_util::future::lazy;
    use ntex_util::time::{Millis, Seconds, sleep, timeout};

    use super::*;
    use crate::v3::{MqttSink, QoS, codec::Decoded};

    #[ntex::test]
    async fn test_dup_packet_id() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));

        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(|_| async {
                    sleep(Seconds(10)).await;
                    Ok(Either::Left(()))
                }),
                fn_service(async |_| {
                    Ok(ProtocolMessageAck {
                        result: ProtocolMessageKind::Nothing,
                    })
                }),
                32 * 1024,
            ),
        );

        let mut f: Pin<Box<dyn Future<Output = Result<_, _>>>> =
            Box::pin(disp.call(Decoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtLeastOnce,
                    topic: ByteString::new(),
                    packet_id: NonZeroU16::new(1),
                    payload_size: 0,
                },
                Bytes::new(),
                999,
            )));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let f = Box::pin(disp.call(Decoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: ByteString::new(),
                packet_id: NonZeroU16::new(1),
                payload_size: 0,
            },
            Bytes::new(),
            999,
        )));
        let err = f.await.err().unwrap();
        match err {
            DispatcherError::Protocol(msg) => {
                assert!(
                    format!("{msg}")
                        .contains("PUBLISH received with packet id that is already in use")
                );
            }
            DispatcherError::Service(()) => panic!(),
        }
    }

    #[ntex::test]
    async fn test_publish_topic_wildcards() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(async |_| Ok::<_, ()>(Either::Left(()))),
                fn_service(async |_| {
                    Ok::<_, DispatcherError<()>>(ProtocolMessageAck {
                        result: ProtocolMessageKind::Nothing,
                    })
                }),
                32 * 1024,
            ),
        );
        let publish = |topic: &'static str| {
            Decoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic: ByteString::from_static(topic),
                    packet_id: None,
                    payload_size: 0,
                },
                Bytes::new(),
                999,
            )
        };

        // [MQTT-3.3.2-2] the Topic Name must not contain wildcards
        for topic in ["a/+", "a/#", "+", "a+b"] {
            let Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err))) =
                disp.call(publish(topic)).await
            else {
                panic!("expected protocol violation for {topic}")
            };
            assert_eq!(
                err.inner,
                crate::error::ViolationInner::Spec(SpecViolation::Pub_3_3_2_2)
            );
        }
        assert!(disp.call(publish("a/b")).await.is_ok());
    }

    #[ntex::test]
    async fn test_unknown_pubrel() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(async |_| Ok::<_, ()>(Either::Left(()))),
                fn_service(async |_| {
                    Ok::<_, DispatcherError<()>>(ProtocolMessageAck {
                        result: ProtocolMessageKind::Nothing,
                    })
                }),
                32 * 1024,
            ),
        );

        // PUBREL is re-sent after a session resumes [MQTT-4.4.0-1]
        let packet_id = NonZeroU16::new(100).unwrap();
        let pkt = disp
            .call(Decoded::Packet(Packet::PublishRelease { packet_id }, 999))
            .await
            .unwrap();
        assert_eq!(
            pkt,
            Some(Encoded::Packet(Packet::PublishComplete { packet_id }))
        );
        assert!(shared.is_active());
    }

    /// Publish service handles topic "publish", "publish/slow" waits 100ms,
    /// other topics are passed to the control service
    macro_rules! qos2_dispatcher {
        ($pubrel:expr, $published:expr) => {{
            let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                false,
                Rc::default(),
            ));
            let pubrel = $pubrel.clone();
            let published = $published.clone();
            let disp = Pipeline::new(
                Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
                Dispatcher::new(
                    shared.clone(),
                    fn_service(async move |pkt: Publish| {
                        published.set(published.get() + 1);
                        if pkt.topic().path() == "publish/slow" {
                            sleep(Millis(100)).await;
                        }
                        let _ = pkt.read_all().await;
                        if pkt.topic().path().starts_with("publish") {
                            Ok::<_, ()>(Either::Left(()))
                        } else {
                            Ok(Either::Right(pkt))
                        }
                    }),
                    fn_service(move |msg: ProtocolMessage| {
                        if let ProtocolMessage::PublishRelease(_) = msg {
                            pubrel.set(pubrel.get() + 1);
                        }
                        async move { Ok::<_, DispatcherError<()>>(msg.ack()) }
                    }),
                    32 * 1024,
                ),
            );
            (io, shared, disp)
        }};
    }

    fn publish(id: u16, qos: QoS, topic: &'static str) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos,
                topic: ByteString::from_static(topic),
                packet_id: NonZeroU16::new(id),
                payload_size: 0,
            },
            Bytes::new(),
            999,
        )
    }

    fn redelivery(id: u16, qos: QoS, chunked: bool) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup: true,
                retain: false,
                qos,
                topic: ByteString::from_static("publish"),
                packet_id: NonZeroU16::new(id),
                payload_size: if chunked { 6 } else { 3 },
            },
            Bytes::from_static(b"abc"),
            999,
        )
    }

    fn pubrel(id: u16) -> Decoded {
        Decoded::Packet(
            Packet::PublishRelease {
                packet_id: NonZeroU16::new(id).unwrap(),
            },
            999,
        )
    }

    fn encoded(pkt: Packet) -> Encoded {
        Encoded::Packet(pkt)
    }

    #[ntex::test]
    async fn test_publish_qos2() {
        let pid = |id| NonZeroU16::new(id).unwrap();
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, shared, disp) = qos2_dispatcher!(pubrel_calls, Rc::new(Cell::new(0)));

        for topic in ["publish", "control"] {
            pubrel_calls.set(0);

            // QoS 2 publish is acknowledged with PUBREC [MQTT-4.3.3-2]
            let res = disp.call(publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(
                res.unwrap(),
                Some(encoded(Packet::PublishReceived { packet_id: pid(1) }))
            );

            // packet id stays in use until PUBREL
            let res = disp.call(publish(1, QoS::ExactlyOnce, topic)).await;
            assert!(matches!(res, Err(DispatcherError::Protocol(_))));

            // PUBREL is passed to the control service, PUBCOMP is sent
            let res = disp.call(pubrel(1)).await;
            assert_eq!(
                res.unwrap(),
                Some(encoded(Packet::PublishComplete { packet_id: pid(1) }))
            );
            assert_eq!(pubrel_calls.get(), 1);

            // packet id is released
            let res = disp.call(publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(
                res.unwrap(),
                Some(encoded(Packet::PublishReceived { packet_id: pid(1) }))
            );
            let res = disp.call(pubrel(1)).await;
            assert_eq!(
                res.unwrap(),
                Some(encoded(Packet::PublishComplete { packet_id: pid(1) }))
            );
            assert_eq!(pubrel_calls.get(), 2);

            // QoS 1 publish is acknowledged with PUBACK, packet id is released
            let res = disp.call(publish(2, QoS::AtLeastOnce, topic)).await;
            assert_eq!(
                res.unwrap(),
                Some(encoded(Packet::PublishAck { packet_id: pid(2) }))
            );
            let res = disp.call(pubrel(2)).await;
            assert_eq!(
                res.unwrap(),
                Some(encoded(Packet::PublishComplete { packet_id: pid(2) }))
            );
            assert_eq!(pubrel_calls.get(), 2);
        }
        assert!(shared.is_active());
    }

    #[ntex::test]
    async fn test_pubrel_before_pubrec() {
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, Rc::new(Cell::new(0)));

        let mut f = Box::pin(disp.call(publish(1, QoS::ExactlyOnce, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        // PUBREL is a response to PUBREC [MQTT-4.3.3-1]
        let res = disp.call(pubrel(1)).await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if matches!(err.inner, crate::error::ViolationInner::UnexpectedPacket { .. })
        ));
        assert_eq!(pubrel_calls.get(), 0);
    }

    /// Chunks of a failed publish are a protocol error, not a panic
    #[ntex::test]
    async fn test_unexpected_payload_chunk() {
        let unexpected = |res: Result<Option<Encoded>, DispatcherError<()>>| {
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::Decode(
                    DecodeError::UnexpectedPayload
                )))
            )
        };
        let chunk = |eof| Decoded::PayloadChunk(Bytes::from_static(b"def"), eof);
        let (_io, _, disp) = qos2_dispatcher!(Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));

        // the streaming publish is rejected before its payload is set up
        let Decoded::Publish(mut pkt, payload, size) = redelivery(1, QoS::AtLeastOnce, true) else {
            unreachable!()
        };
        pkt.dup = false;
        pkt.topic = ByteString::from_static("a/+");
        let res = disp.call(Decoded::Publish(pkt, payload, size)).await;
        assert!(matches!(res, Err(DispatcherError::Protocol(_))), "{res:?}");
        assert!(unexpected(disp.call(chunk(true)).await));
        assert!(unexpected(disp.call(chunk(false)).await));
    }

    #[ntex::test]
    async fn test_publish_redelivery() {
        let pid = |id| NonZeroU16::new(id).unwrap();
        let is_err = |res: Result<Option<Encoded>, DispatcherError<()>>| {
            matches!(res, Err(DispatcherError::Protocol(_)))
        };
        let pubrel_calls = Rc::new(Cell::new(0));
        let published = Rc::new(Cell::new(0));
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, published);

        // re-delivery is ignored until PUBACK [MQTT-4.3.2-2]
        let mut f = Box::pin(disp.call(publish(1, QoS::AtLeastOnce, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp.call(redelivery(1, QoS::AtLeastOnce, true)).await;
        assert_eq!(res.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"def"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert!(is_err(
            disp.call(redelivery(1, QoS::ExactlyOnce, false)).await
        ));
        assert!(is_err(
            disp.call(publish(1, QoS::AtLeastOnce, "publish")).await
        ));
        assert_eq!(
            f.await.unwrap(),
            Some(encoded(Packet::PublishAck { packet_id: pid(1) }))
        );
        assert_eq!(published.get(), 1);

        // re-delivery is acked by PUBREC until PUBREL [MQTT-4.3.3-2]
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, published);
        let pubrec = Some(encoded(Packet::PublishReceived { packet_id: pid(2) }));
        let res = disp.call(publish(2, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), pubrec);
        let res = disp.call(redelivery(2, QoS::ExactlyOnce, true)).await;
        assert_eq!(res.unwrap(), pubrec);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"de"), false);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"f"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert_eq!(published.get(), 2);

        // after PUBCOMP re-delivery is a new message
        let res = disp.call(pubrel(2)).await;
        assert_eq!(
            res.unwrap(),
            Some(encoded(Packet::PublishComplete { packet_id: pid(2) }))
        );
        assert_eq!(pubrel_calls.get(), 1);
        let mut f = Box::pin(disp.call(redelivery(2, QoS::ExactlyOnce, true)));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        assert_eq!(published.get(), 3);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"def"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert_eq!(timeout(Millis(100), f).await.unwrap().unwrap(), pubrec);

        // a different QoS or a missing DUP flag is a packet id conflict
        assert!(is_err(
            disp.call(redelivery(2, QoS::AtLeastOnce, false)).await
        ));
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls, published);
        let res = disp.call(publish(2, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), pubrec);
        assert!(is_err(
            disp.call(publish(2, QoS::ExactlyOnce, "publish")).await
        ));
        assert_eq!(published.get(), 4);
    }

    fn assert_unexpected<E: std::fmt::Debug>(
        res: &Result<Option<Encoded>, DispatcherError<E>>,
        expected: u8,
    ) {
        assert!(
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)))
                    if matches!(
                        err.inner,
                        crate::error::ViolationInner::UnexpectedPacket { packet_type, .. }
                            if packet_type == expected
                    )
            ),
            "{res:?}"
        );
    }

    #[ntex::test]
    async fn test_unexpected_packets() {
        let (_io, shared, disp) = qos2_dispatcher!(Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));

        // PINGRESP is handled
        let res = disp.call(Decoded::Packet(Packet::PingResponse, 999)).await;
        assert_eq!(res.unwrap(), None);
        assert!(shared.is_active());

        // packets sent by the client only and a second CONNACK are not expected from server
        for (pkt, tp) in [
            (Packet::Connect(Box::default()), packet_type::CONNECT),
            (
                Packet::ConnectAck(codec::ConnectAck {
                    return_code: codec::ConnectAckReason::ConnectionAccepted,
                    session_present: false,
                }),
                packet_type::CONNACK,
            ),
            (Packet::PingRequest, packet_type::PINGREQ),
            (Packet::Disconnect, packet_type::DISCONNECT),
        ] {
            assert_unexpected(&disp.call(Decoded::Packet(pkt, 999)).await, tp);
        }
    }

    #[ntex::test]
    async fn test_payload_backpressure() {
        use std::{cell::RefCell, task::Poll};

        struct FailReady<E>(Rc<Cell<bool>>, fn() -> E);

        impl<St, E> Service<St, ProtocolMessage> for FailReady<E> {
            type Res = ProtocolMessageAck;
            type Error = E;

            async fn ready(&self, _: Ctx<'_, Self, St>) -> Result<(), E> {
                if self.0.get() { Err((self.1)()) } else { Ok(()) }
            }

            async fn call(
                &self,
                msg: ProtocolMessage,
                _: Ctx<'_, Self, St>,
            ) -> Result<Self::Res, E> {
                Ok(msg.ack())
            }
        }

        struct Hold<R, E>(
            Rc<RefCell<Option<Publish>>>,
            Rc<Cell<bool>>,
            fn() -> E,
            fn() -> R,
        );

        impl<St, R, E> Service<St, Publish> for Hold<R, E> {
            type Res = R;
            type Error = E;

            async fn ready(&self, _: Ctx<'_, Self, St>) -> Result<(), E> {
                if self.1.get() { Err((self.2)()) } else { Ok(()) }
            }

            async fn call(&self, pkt: Publish, _: Ctx<'_, Self, St>) -> Result<R, E> {
                *self.0.borrow_mut() = Some(pkt);
                Ok((self.3)())
            }
        }

        let fail = Rc::new(Cell::new(false));
        let pfail = Rc::new(Cell::new(false));
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(crate::MqttServiceConfig::new().set_max_payload_buffer_size(4))
            .into();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let held = Rc::new(RefCell::new(None));
        let h = held.clone();
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                Hold(h, pfail.clone(), || (), || Either::Left(())),
                FailReady(fail.clone(), || {
                    DispatcherError::Protocol(MqttProtocolError::ReadTimeout)
                }),
                4,
            ),
        );
        let chunk = |data: &'static [u8]| {
            disp.call_nowait(Decoded::PayloadChunk(Bytes::from_static(data), false))
        };

        // streamed payload below the high watermark
        let res = disp
            .call(Decoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic: ByteString::from_static("t"),
                    packet_id: None,
                    payload_size: 16,
                },
                Bytes::from_static(b"ab"),
                999,
            ))
            .await;
        assert_eq!(res.unwrap(), None);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Ok(()))
        ));

        // the payload buffer reached the high watermark, reading pauses
        assert_eq!(chunk(b"cd").await.unwrap(), None);
        assert!(lazy(|cx| disp.poll_ready(cx)).await.is_pending());

        // the payload buffer is drained to the low watermark
        let pkt = held.borrow_mut().take().unwrap();
        assert_eq!(pkt.read().await.unwrap(), Some(Bytes::from_static(b"ab")));
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Ok(()))
        ));
        assert!(io.is_active());

        // a readiness error is not delayed by the paused payload
        assert_eq!(chunk(b"efgh").await.unwrap(), None);
        fail.set(true);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Err(_))
        ));
        fail.set(false);
        pfail.set(true);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Err(_))
        ));
        pfail.set(false);

        // the payload receiver is dropped before the stream ends
        assert!(lazy(|cx| disp.poll_ready(cx)).await.is_pending());
        drop(pkt);
        assert!(disp.ready().await.is_ok());
        assert!(!io.is_active());
    }
}
