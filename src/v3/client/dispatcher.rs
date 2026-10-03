use std::{cell::Cell, cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc};

use ntex_service::{Ctx, Service};
use ntex_util::future::{Either, join};
use ntex_util::{HashMap, hash_map, services::inflight::InFlightService};

use crate::error::{DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::types::packet_type;
use crate::v3::codec::{self, Decoded, Encoded, Packet};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{QoS, Session, control::ProtocolMessageKind, publish::Publish};

use super::control::{ProtocolMessage, ProtocolMessageAck};

/// mqtt3 protocol dispatcher
pub(super) fn create_dispatcher<St, T, C, E>(
    sink: Rc<MqttShared>,
    inflight: usize,
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
    // limit number of in-flight messages
    InFlightService::new(
        inflight,
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
        if (res1.is_err() || res2.is_err())
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
                    match inner.inflight.borrow_mut().entry(pid) {
                        hash_map::Entry::Occupied(_) => {
                            log::trace!("Duplicated packet id for publish packet: {pid:?}");
                            return Err(SpecViolation::PacketId_2_2_1_3_Pub.into());
                        }
                        hash_map::Entry::Vacant(entry) => {
                            entry.insert(InFlight::Publish(publish.qos));
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
                let pl = self.inner.payload.take().unwrap();
                pl.feed_data(buf);
                if eof {
                    pl.feed_eof();
                } else {
                    self.inner.payload.set(Some(pl));
                }
                Ok(None)
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
            Decoded::Packet(
                pkt @ (Packet::PingRequest
                | Packet::Disconnect
                | Packet::Subscribe { .. }
                | Packet::Unsubscribe { .. }),
                _,
            ) => Err(MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            )
            .into()),
            Decoded::Packet(pkt, _) => {
                log::debug!("Unsupported packet: {pkt:?}");
                Ok(None)
            }
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
    use ntex_util::time::{Millis, Seconds, sleep};

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
        ($pubrel:expr) => {{
            let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                false,
                Rc::default(),
            ));
            let pubrel = $pubrel.clone();
            let disp = Pipeline::new(
                Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
                Dispatcher::new(
                    shared.clone(),
                    fn_service(async |pkt: Publish| {
                        if pkt.topic().path() == "publish/slow" {
                            sleep(Millis(100)).await;
                        }
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
        let (_io, shared, disp) = qos2_dispatcher!(pubrel_calls);

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
        let (_io, _, disp) = qos2_dispatcher!(pubrel_calls);

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
}
