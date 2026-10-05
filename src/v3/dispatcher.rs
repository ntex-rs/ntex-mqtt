use std::{cell::Cell, cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc};

use ntex_error::{Failure, IntoFailure};
use ntex_service::{Ctx, Service, ServiceFactory, cfg::Cfg, pipeline::PipelineState};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::{HashMap, future::join, hash_map, services::inflight::InFlightService};

use crate::error::{DecodeError, DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::payload::{Payload, PayloadStatus};
use crate::{MqttServiceConfig, types::QoS, types::packet_type};

use super::codec::{Decoded, Packet};
use super::control::{
    ProtocolMessage, ProtocolMessageAck, ProtocolMessageKind, Subscribe, Unsubscribe,
};
use super::{Session, publish::Publish, shared::Ack, shared::MqttShared};

/// mqtt3 protocol dispatcher
pub(super) fn factory<AppSt, Sf, Ctl>(
    publish: Sf,
    control: Ctl,
) -> impl ServiceFactory<
    Session<AppSt>,
    Decoded,
    Res = Option<Packet>,
    Error = DispatcherError<Sf::Error>,
    InitError = Failure,
>
where
    AppSt: 'static,
    Sf: ServiceFactory<Session<AppSt>, Publish, Res = ()> + 'static,
    Sf::InitError: IntoFailure,
    Ctl: ServiceFactory<Session<AppSt>, ProtocolMessage, Res = ProtocolMessageAck> + 'static,
    Ctl::Error: Into<Sf::Error>,
    Ctl::InitError: IntoFailure,
{
    ntex_service::factory(async move |st: &Session<AppSt>| {
        // create services
        let sink = st.sink().shared();
        let fut = join(publish.create(st), control.create(st));
        let (publish, control) = fut.await;

        let publish = publish.map_err(IntoFailure::fail)?;
        let control = control.map_err(IntoFailure::fail)?;

        let control = BufferService::new(
            16,
            // limit number of in-flight messages
            PipelineState::new(InFlightService::new(1, control)),
        )
        .map_err(|err| match err {
            BufferServiceError::Service(e) => DispatcherError::Service(e.into()),
            BufferServiceError::RequestCanceled => {
                DispatcherError::Protocol(MqttProtocolError::ReadTimeout)
            }
        });

        let cfg: Cfg<MqttServiceConfig> = st.cfg();
        Ok(Dispatcher::new(sink, publish, control, cfg))
    })
}

impl crate::inflight::SizedRequest for Decoded {
    fn size(&self) -> u32 {
        if let Decoded::Packet(_, size) | Decoded::Publish(_, _, size) = self {
            *size
        } else {
            0
        }
    }

    fn has_more_chunks(&self) -> bool {
        match self {
            Decoded::Publish(publish, payload, _) => publish.payload_size != payload.len() as u32,
            Decoded::PayloadChunk(_, eof) => !eof,
            Decoded::Packet(..) => false,
        }
    }

    /// `max_receive` limits incoming publish packets only, acks and pings
    /// are processed while publish handlers wait for them
    fn is_limited(&self) -> bool {
        matches!(self, Decoded::Publish(..))
    }

    /// Payload chunks follow their publish, publishes received before
    /// DISCONNECT are processed before it
    fn is_ordered(&self) -> bool {
        matches!(
            self,
            Decoded::PayloadChunk(..) | Decoded::Packet(Packet::Disconnect, _)
        )
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C, E> {
    publish: T,
    inner: Inner<C>,
    cfg: Cfg<MqttServiceConfig>,
    /// Payload chunks of an ignored PUBLISH are dropped
    discard_payload: Cell<bool>,
    st: PhantomData<(St, E)>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    inflight: RefCell<HashMap<NonZeroU16, InFlight>>,
}

/// State of a packet id used by the client
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InFlight {
    /// PUBLISH is processed by the publish service
    Publish(QoS),
    /// PUBREC is sent, PUBREL is expected
    Received,
    /// SUBSCRIBE or UNSUBSCRIBE is processed by the control service
    Subscribe,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    T: Service<St, Publish, Res = (), Error = E>,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>> + 'static,
{
    pub(crate) fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        cfg: Cfg<MqttServiceConfig>,
    ) -> Self {
        Self {
            cfg,
            publish,
            discard_payload: Cell::new(false),
            inner: Inner {
                sink,
                control,
                inflight: RefCell::new(HashMap::default()),
            },
            st: PhantomData,
        }
    }

    fn tag(&self) -> &'static str {
        self.inner.sink.tag()
    }
}

impl<St, T, C, E> Service<St, Decoded> for Dispatcher<St, T, C, E>
where
    E: 'static,
    T: Service<St, Publish, Res = (), Error = E> + 'static,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>> + 'static,
{
    type Res = Option<Packet>;
    type Error = DispatcherError<E>;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), ctx.ready(&self.inner.control)).await;
        // payload backpressure, reading pauses while the streamed payload buffer
        // is above its high watermark, the connection is closed if the payload
        // receiver is dropped before the stream ends
        if res1.is_ok()
            && res2.is_ok()
            && let Some(pl) = self.inner.sink.payload.take()
        {
            self.inner.sink.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }

        res1.map_err(DispatcherError::Service)?;
        res2?;
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.inner.sink.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();

        ctx.shutdown(&self.inner.control).await;
        ctx.shutdown(&self.publish).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(&self, req: Decoded, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        log::trace!("{}; Dispatch v3 packet: {:#?}", self.tag(), req);

        match req {
            Decoded::Publish(publish, payload, size) => {
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
                            log::trace!(
                                "{}: Re-delivered publish packet is ignored: {:?}",
                                self.tag(),
                                pid
                            );
                            if publish.payload_size != payload.len() as u32 {
                                self.discard_payload.set(true);
                            }
                            return Ok(None);
                        }
                        // until PUBREL, any subsequent PUBLISH with the same packet id is
                        // acked by PUBREC and is not delivered, irrespective of DUP [MQTT-4.3.3-2]
                        Some(InFlight::Received) if publish.qos == QoS::ExactlyOnce => {
                            log::trace!(
                                "{}: Re-delivered publish packet is received: {:?}",
                                self.tag(),
                                pid
                            );
                            if publish.payload_size != payload.len() as u32 {
                                self.discard_payload.set(true);
                            }
                            return Ok(Some(Packet::PublishReceived { packet_id: pid }));
                        }
                        Some(_) => {
                            log::trace!(
                                "{}: Duplicated packet id for publish packet: {:?}",
                                self.tag(),
                                pid
                            );
                            return Err(SpecViolation::PacketId_2_2_1_3_Pub.into());
                        }
                    }
                }

                // check max allowed qos
                if publish.qos > self.cfg.max_qos {
                    log::trace!(
                        "{}: Max allowed QoS is violated, max {:?} provided {:?}",
                        self.tag(),
                        self.cfg.max_qos,
                        publish.qos
                    );
                    return Err(SpecViolation::Connack_3_2_2_11.into());
                }

                if !inner.sink.is_active()
                    && self
                        .cfg
                        .handle_qos_after_disconnect
                        .is_none_or(|max_qos| publish.qos > max_qos)
                {
                    // payload chunks of the dropped publish are dropped as well
                    if publish.payload_size != payload.len() as u32 {
                        self.discard_payload.set(true);
                    }
                    return Ok(None);
                }

                let payload = if publish.payload_size == payload.len() as u32 {
                    Payload::from_bytes(payload)
                } else {
                    let (pl, sender) =
                        Payload::from_stream(payload, self.cfg.max_payload_buffer_size);
                    self.inner.sink.payload.set(Some(sender));
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
                if self.discard_payload.get() {
                    if eof {
                        self.discard_payload.set(false);
                    }
                    Ok(None)
                } else if let Some(pl) = self.inner.sink.payload.take() {
                    pl.feed_data(buf);
                    if eof {
                        pl.feed_eof();
                    } else {
                        self.inner.sink.payload.set(Some(pl));
                    }
                    Ok(None)
                } else {
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
            Decoded::Packet(Packet::PublishRelease { packet_id }, _) => {
                let state = self.inner.inflight.borrow().get(&packet_id).copied();
                if state == Some(InFlight::Received) {
                    self.inner
                        .control(ProtocolMessage::pubrel(packet_id), ctx)
                        .await
                } else if state.is_some() {
                    // PUBREL is sent in response to PUBREC [MQTT-4.3.3-1]
                    Err(MqttProtocolError::unexpected_packet(
                        packet_type::PUBREL,
                        "PublishRelease packet before PublishReceived",
                    )
                    .into())
                } else {
                    // PUBREL is re-sent after a session resumes [MQTT-4.4.0-1], the
                    // release could be already completed, PUBCOMP is required [MQTT-4.3.3-2]
                    log::trace!(
                        "{}: Unknown packet-id in PublishRelease packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    Ok(Some(Packet::PublishComplete { packet_id }))
                }
            }
            Decoded::Packet(Packet::PublishComplete { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PingRequest, _) => {
                self.inner.control(ProtocolMessage::ping(), ctx).await
            }
            Decoded::Packet(
                Packet::Subscribe {
                    packet_id,
                    topic_filters,
                },
                size,
            ) => {
                if !self.inner.sink.is_active() {
                    Ok(None)
                } else if topic_filters
                    .iter()
                    .any(|(tf, _)| !crate::topic::is_valid(tf))
                {
                    Err(SpecViolation::Subs_4_7_1.into())
                } else if !self.inner.insert_inflight(packet_id, InFlight::Subscribe) {
                    log::trace!(
                        "{}: Duplicated packet id for subscribe packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    Err(SpecViolation::PacketId_2_2_1_3_Sub.into())
                } else {
                    self.inner
                        .control(
                            ProtocolMessage::subscribe(Subscribe::new(
                                packet_id,
                                size,
                                topic_filters,
                            )),
                            ctx,
                        )
                        .await
                }
            }
            Decoded::Packet(
                Packet::Unsubscribe {
                    packet_id,
                    topic_filters,
                },
                size,
            ) => {
                if !self.inner.sink.is_active() {
                    Ok(None)
                } else if topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    Err(SpecViolation::Subs_4_7_1.into())
                } else if !self.inner.insert_inflight(packet_id, InFlight::Subscribe) {
                    log::trace!(
                        "{}: Duplicated packet id for unsubscribe packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    Err(SpecViolation::PacketId_2_2_1_3_Unsub.into())
                } else {
                    self.inner
                        .control(
                            ProtocolMessage::unsubscribe(Unsubscribe::new(
                                packet_id,
                                size,
                                topic_filters,
                            )),
                            ctx,
                        )
                        .await
                }
            }
            Decoded::Packet(Packet::Disconnect, _) => {
                self.inner.sink.is_disconnect_sent();
                self.inner
                    .control(ProtocolMessage::remote_disconnect(), ctx)
                    .await
            }
            // a second CONNECT is a protocol violation [MQTT-3.1.0-2], the connection is
            // closed on a protocol violation [MQTT-4.8.0-1]
            Decoded::Packet(Packet::Connect(_), _) => Err(MqttProtocolError::unexpected_packet(
                packet_type::CONNECT,
                "[MQTT-3.1.0-2] Second CONNECT packet is received",
            )
            .into()),
            Decoded::Packet(
                pkt @ (Packet::ConnectAck(_)
                | Packet::SubscribeAck { .. }
                | Packet::UnsubscribeAck { .. }
                | Packet::PingResponse),
                _,
            ) => Err(MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from client",
            )
            .into()),
        }
    }
}

/// Publish service response future
async fn publish_fn<'f, St, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<C>,
    ctx: Ctx<'f, Dispatcher<St, T, C, E>, St>,
) -> Result<Option<Packet>, DispatcherError<E>>
where
    T: Service<St, Publish, Res = (), Error = E>,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    let qos2 = pkt.qos() == QoS::ExactlyOnce;
    match ctx.call(svc, pkt).await {
        Ok(()) => {
            log::trace!(
                "{}: Publish result for packet {:?} is ready",
                inner.sink.tag(),
                packet_id
            );

            if let Some(packet_id) = packet_id {
                if qos2 {
                    inner
                        .inflight
                        .borrow_mut()
                        .insert(packet_id, InFlight::Received);
                    Ok(Some(Packet::PublishReceived { packet_id }))
                } else {
                    inner.inflight.borrow_mut().remove(&packet_id);
                    Ok(Some(Packet::PublishAck { packet_id }))
                }
            } else {
                Ok(None)
            }
        }
        Err(e) => Err(DispatcherError::Service(e)),
    }
}

impl<C> Inner<C> {
    /// Marks the packet id as used, returns `false` if it is already in use
    fn insert_inflight(&self, packet_id: NonZeroU16, state: InFlight) -> bool {
        match self.inflight.borrow_mut().entry(packet_id) {
            hash_map::Entry::Occupied(_) => false,
            hash_map::Entry::Vacant(entry) => {
                entry.insert(state);
                true
            }
        }
    }

    async fn control<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, St>,
    ) -> Result<Option<Packet>, DispatcherError<E>>
    where
        C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        match ctx.call(&self.control, pkt).await {
            Ok(item) => {
                let packet = match item.result {
                    ProtocolMessageKind::Ping => Some(Packet::PingResponse),
                    ProtocolMessageKind::Subscribe(res) => {
                        self.inflight.borrow_mut().remove(&res.packet_id);
                        Some(Packet::SubscribeAck {
                            status: res.codes,
                            packet_id: res.packet_id,
                        })
                    }
                    ProtocolMessageKind::Unsubscribe(res) => {
                        self.inflight.borrow_mut().remove(&res.packet_id);
                        Some(Packet::UnsubscribeAck {
                            packet_id: res.packet_id,
                        })
                    }
                    ProtocolMessageKind::Disconnect => {
                        self.sink.drop_payload(&PayloadError::Service);
                        self.sink.close();
                        None
                    }
                    ProtocolMessageKind::Nothing => None,
                    ProtocolMessageKind::PublishRelease(packet_id) => {
                        self.inflight.borrow_mut().remove(&packet_id);
                        Some(Packet::PublishComplete { packet_id })
                    }
                    ProtocolMessageKind::PublishAck(_) => unreachable!(),
                };
                Ok(packet)
            }
            Err(err) => {
                self.sink.drop_payload(&PayloadError::Service);
                self.sink.close();
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_service};
    use ntex_util::{future::lazy, time::Millis, time::Seconds, time::sleep};

    use super::*;
    use crate::{error, v3::MqttSink, v3::codec};

    /// The response queue keeps packets, not the larger encoder items
    #[test]
    fn test_queue_slot_size() {
        use std::mem::size_of;

        type Slot = crate::io::QueueSlot<MqttShared>;
        assert!(size_of::<Slot>() <= size_of::<Packet>());
        assert!(size_of::<Slot>() < size_of::<codec::Encoded>());
    }

    #[ntex::test]
    async fn test_dup_packet_id() {
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce))
            .into();

        let io = Io::new(IoTest::create().0, cfg.clone());
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));

        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(async |_| {
                    sleep(Seconds(10)).await;
                    Ok(())
                }),
                fn_service(async |msg: ProtocolMessage| Ok::<_, DispatcherError<()>>(msg.ack())),
                cfg.get(),
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

        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) =
            f.await.err().unwrap()
        else {
            panic!()
        };
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::PacketId_2_2_1_3_Pub)
        );
    }

    #[ntex::test]
    async fn test_spec_violations() {
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce))
            .into();

        let io = Io::new(IoTest::create().0, cfg.clone());
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));

        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(async |_: Publish| Ok::<_, ()>(())),
                fn_service(async |msg: ProtocolMessage| Ok::<_, DispatcherError<()>>(msg.ack())),
                cfg.get(),
            ),
        );

        // unknown PublishRelease, [MQTT-4.4.0-1]
        let pkt = disp
            .call(Decoded::Packet(
                Packet::PublishRelease {
                    packet_id: NonZeroU16::new(100).unwrap(),
                },
                999,
            ))
            .await
            .ok()
            .unwrap();
        assert_eq!(
            pkt,
            Some(Packet::PublishComplete {
                packet_id: NonZeroU16::new(100).unwrap()
            })
        );
        assert!(shared.is_active());

        // unknown PublishAck
        let err = disp
            .call(Decoded::Packet(
                Packet::PublishAck {
                    packet_id: NonZeroU16::new(100).unwrap(),
                },
                999,
            ))
            .await
            .err()
            .unwrap();
        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        let error::ViolationInner::Common { reason, .. } = err.inner else {
            panic!()
        };
        assert_eq!(
            reason,
            crate::v5::codec::DisconnectReasonCode::ProtocolError
        );

        // unknown PublishReceived
        let err = disp
            .call(Decoded::Packet(
                Packet::PublishReceived {
                    packet_id: NonZeroU16::new(100).unwrap(),
                },
                999,
            ))
            .await
            .err()
            .unwrap();
        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        let error::ViolationInner::Common { reason, .. } = err.inner else {
            panic!()
        };
        assert_eq!(
            reason,
            crate::v5::codec::DisconnectReasonCode::ProtocolError
        );

        // unknown PublishComplete
        let err = disp
            .call(Decoded::Packet(
                Packet::PublishComplete {
                    packet_id: NonZeroU16::new(100).unwrap(),
                },
                999,
            ))
            .await
            .err()
            .unwrap();
        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        let error::ViolationInner::Common { reason, .. } = err.inner else {
            panic!()
        };
        assert_eq!(
            reason,
            crate::v5::codec::DisconnectReasonCode::ProtocolError
        );

        // protocol violations close the connection, subscriptions are
        // ignored on a closed connection
        let io = Io::new(IoTest::create().0, cfg.clone());
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
                fn_service(async |_: Publish| Ok::<_, ()>(())),
                fn_service(async |msg: ProtocolMessage| Ok::<_, DispatcherError<()>>(msg.ack())),
                cfg.get(),
            ),
        );

        // subscribe invalid topic
        let err = disp
            .call(Decoded::Packet(
                Packet::Subscribe {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    topic_filters: vec![(ByteString::new(), QoS::AtLeastOnce)],
                },
                999,
            ))
            .await
            .err()
            .unwrap();
        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Subs_4_7_1)
        );

        // unsubscribe invalid topic
        let err = disp
            .call(Decoded::Packet(
                Packet::Unsubscribe {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    topic_filters: vec![ByteString::new()],
                },
                999,
            ))
            .await
            .err()
            .unwrap();

        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Subs_4_7_1)
        );
    }

    /// Dispatcher with a publish service that counts calls and sleeps for
    /// `publish/slow` topic, the control service sleeps for SUBSCRIBE
    macro_rules! redelivery_dispatcher {
        ($counter:ident) => {{
            let cfg: SharedCfg = SharedCfg::new("DBG")
                .add(MqttServiceConfig::new().set_max_qos(QoS::ExactlyOnce))
                .into();
            let io = Io::new(IoTest::create().0, cfg.clone());
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                false,
                Rc::default(),
            ));
            let counter = $counter.clone();
            let disp = Pipeline::new(
                Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
                Dispatcher::new(
                    shared.clone(),
                    fn_service(move |pkt: Publish| {
                        let counter = counter.clone();
                        async move {
                            counter.set(counter.get() + 1);
                            if pkt.topic().path() == "publish/slow" {
                                sleep(Millis(100)).await;
                            }
                            Ok::<_, ()>(())
                        }
                    }),
                    fn_service(async |msg: ProtocolMessage| {
                        if matches!(msg, ProtocolMessage::Subscribe(_)) {
                            sleep(Millis(100)).await;
                        }
                        Ok::<_, DispatcherError<()>>(msg.ack())
                    }),
                    cfg.get(),
                ),
            );
            (io, shared, disp)
        }};
    }

    fn publish(id: u16, qos: QoS, dup: bool, topic: &'static str) -> Decoded {
        publish_chunk(id, qos, dup, topic, 0, b"")
    }

    fn publish_chunk(
        id: u16,
        qos: QoS,
        dup: bool,
        topic: &'static str,
        payload_size: u32,
        payload: &'static [u8],
    ) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup,
                retain: false,
                qos,
                topic: ByteString::from_static(topic),
                packet_id: NonZeroU16::new(id),
                payload_size,
            },
            Bytes::from_static(payload),
            999,
        )
    }

    fn pid(id: u16) -> NonZeroU16 {
        NonZeroU16::new(id).unwrap()
    }

    fn assert_violation<T: std::fmt::Debug>(
        res: &Result<T, DispatcherError<()>>,
        expected: &error::ViolationInner,
    ) {
        let Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err))) = res else {
            panic!("expected protocol violation, got {res:?}")
        };
        assert_eq!(&err.inner, expected);
    }

    #[ntex::test]
    async fn test_redelivered_publish_qos1() {
        let counter = Rc::new(Cell::new(0));
        let (_io, shared, disp) = redelivery_dispatcher!(counter);

        let mut f = Box::pin(disp.call(publish(1, QoS::AtLeastOnce, false, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        assert_eq!(counter.get(), 1);

        // re-delivery before PUBACK is not delivered again [MQTT-4.3.2-2]
        let res = disp
            .call(publish(1, QoS::AtLeastOnce, true, "publish/slow"))
            .await;
        assert_eq!(res.unwrap(), None);
        assert_eq!(counter.get(), 1);

        // the first delivery acks both
        assert_eq!(
            f.await.unwrap(),
            Some(Packet::PublishAck { packet_id: pid(1) })
        );

        // after PUBACK the packet id is a new publication, irrespective of DUP
        let res = disp
            .call(publish(1, QoS::AtLeastOnce, true, "publish"))
            .await;
        assert_eq!(res.unwrap(), Some(Packet::PublishAck { packet_id: pid(1) }));
        assert_eq!(counter.get(), 2);
        assert!(shared.is_active());
    }

    #[ntex::test]
    async fn test_redelivered_publish_qos2() {
        let counter = Rc::new(Cell::new(0));
        let (_io, shared, disp) = redelivery_dispatcher!(counter);
        let pubrec = Some(Packet::PublishReceived { packet_id: pid(2) });

        let res = disp
            .call(publish(2, QoS::ExactlyOnce, false, "publish"))
            .await;
        assert_eq!(res.unwrap(), pubrec);
        assert_eq!(counter.get(), 1);

        // until PUBREL, re-delivery is acked by PUBREC and is not delivered [MQTT-4.3.3-2]
        let res = disp
            .call(publish(2, QoS::ExactlyOnce, true, "publish"))
            .await;
        assert_eq!(res.unwrap(), pubrec);
        assert_eq!(counter.get(), 1);

        // a subsequent PUBLISH without DUP as well
        let res = disp
            .call(publish(2, QoS::ExactlyOnce, false, "publish"))
            .await;
        assert_eq!(res.unwrap(), pubrec);
        assert_eq!(counter.get(), 1);
        assert!(shared.is_active());

        let res = disp
            .call(Decoded::Packet(
                Packet::PublishRelease { packet_id: pid(2) },
                999,
            ))
            .await;
        assert_eq!(
            res.unwrap(),
            Some(Packet::PublishComplete { packet_id: pid(2) })
        );

        // after PUBCOMP the packet id is a new publication
        let res = disp
            .call(publish(2, QoS::ExactlyOnce, true, "publish"))
            .await;
        assert_eq!(res.unwrap(), pubrec);
        assert_eq!(counter.get(), 2);
        assert!(shared.is_active());
    }

    #[ntex::test]
    async fn test_redelivered_publish_violations() {
        let in_use = error::ViolationInner::Spec(error::SpecViolation::PacketId_2_2_1_3_Pub);
        let counter = Rc::new(Cell::new(0));

        // re-delivery with a different QoS, QoS 1 is pending
        let (_io, _, disp) = redelivery_dispatcher!(counter);
        let mut f = Box::pin(disp.call(publish(1, QoS::AtLeastOnce, false, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp
            .call(publish(1, QoS::ExactlyOnce, true, "publish"))
            .await;
        assert_violation(&res, &in_use);

        // re-delivery with a different QoS, PUBREL is expected
        let (_io, _, disp) = redelivery_dispatcher!(counter);
        assert!(
            disp.call(publish(2, QoS::ExactlyOnce, false, "publish"))
                .await
                .is_ok()
        );
        let res = disp
            .call(publish(2, QoS::AtLeastOnce, true, "publish"))
            .await;
        assert_violation(&res, &in_use);

        // re-delivery with the packet id of a pending SUBSCRIBE
        let (_io, _, disp) = redelivery_dispatcher!(counter);
        let mut f = Box::pin(disp.call(Decoded::Packet(
            Packet::Subscribe {
                packet_id: pid(3),
                topic_filters: vec![(ByteString::from_static("topic"), QoS::AtLeastOnce)],
            },
            999,
        )));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp
            .call(publish(3, QoS::AtLeastOnce, true, "publish"))
            .await;
        assert_violation(&res, &in_use);
        // PUBREL with the packet id of a pending SUBSCRIBE
        let res = disp
            .call(Decoded::Packet(
                Packet::PublishRelease { packet_id: pid(3) },
                999,
            ))
            .await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if matches!(err.inner, error::ViolationInner::UnexpectedPacket { .. })
        ));

        // SUBSCRIBE and UNSUBSCRIBE with the packet id of a pending PUBLISH
        for (pkt, err) in [
            (
                Packet::Subscribe {
                    packet_id: pid(5),
                    topic_filters: vec![(ByteString::from_static("topic"), QoS::AtLeastOnce)],
                },
                error::SpecViolation::PacketId_2_2_1_3_Sub,
            ),
            (
                Packet::Unsubscribe {
                    packet_id: pid(5),
                    topic_filters: vec![ByteString::from_static("topic")],
                },
                error::SpecViolation::PacketId_2_2_1_3_Unsub,
            ),
        ] {
            let (_io, _, disp) = redelivery_dispatcher!(counter);
            let mut f = Box::pin(disp.call(publish(5, QoS::AtLeastOnce, false, "publish/slow")));
            let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
            let res = disp.call(Decoded::Packet(pkt, 999)).await;
            assert_violation(&res, &error::ViolationInner::Spec(err));
            // the PUBLISH state is kept
            let res = disp
                .call(publish(5, QoS::AtLeastOnce, true, "publish"))
                .await;
            assert_eq!(res.unwrap(), None);
        }

        // PUBREL before PUBREC [MQTT-4.3.3-1]
        let (_io, _, disp) = redelivery_dispatcher!(counter);
        let mut f = Box::pin(disp.call(publish(4, QoS::ExactlyOnce, false, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp
            .call(Decoded::Packet(
                Packet::PublishRelease { packet_id: pid(4) },
                999,
            ))
            .await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if matches!(err.inner, error::ViolationInner::UnexpectedPacket { .. })
        ));
    }

    #[ntex::test]
    async fn test_redelivered_publish_payload() {
        let counter = Rc::new(Cell::new(0));
        let (_io, shared, disp) = redelivery_dispatcher!(counter);
        let chunk = |data: &'static [u8], eof| {
            disp.call(Decoded::PayloadChunk(Bytes::from_static(data), eof))
        };

        // payload of an ignored re-delivery is dropped
        let mut f = Box::pin(disp.call(publish(1, QoS::AtLeastOnce, false, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp
            .call(publish_chunk(
                1,
                QoS::AtLeastOnce,
                true,
                "publish/slow",
                4,
                b"ab",
            ))
            .await;
        assert_eq!(res.unwrap(), None);
        assert_eq!(chunk(b"c", false).await.unwrap(), None);
        assert_eq!(chunk(b"d", true).await.unwrap(), None);
        assert!(f.await.is_ok());

        // payload of a re-delivery acked by PUBREC is dropped
        assert!(
            disp.call(publish(2, QoS::ExactlyOnce, false, "publish"))
                .await
                .is_ok()
        );
        let res = disp
            .call(publish_chunk(2, QoS::ExactlyOnce, true, "publish", 3, b"a"))
            .await;
        assert_eq!(
            res.unwrap(),
            Some(Packet::PublishReceived { packet_id: pid(2) })
        );
        assert_eq!(chunk(b"bc", true).await.unwrap(), None);
        assert_eq!(counter.get(), 2);
        assert!(shared.is_active());

        // the last chunk ends the dropped payload
        assert!(matches!(
            chunk(b"d", true).await,
            Err(DispatcherError::Protocol(MqttProtocolError::Decode(
                DecodeError::UnexpectedPayload
            )))
        ));
    }

    #[ntex::test]
    async fn test_inactive_publish_payload() {
        let counter = Rc::new(Cell::new(0));
        let (io, shared, disp) = redelivery_dispatcher!(counter);
        let chunk = |data: &'static [u8], eof| {
            disp.call(Decoded::PayloadChunk(Bytes::from_static(data), eof))
        };
        io.close();
        assert!(!shared.is_active());

        // payload of a publish dropped after disconnect is dropped
        let res = disp
            .call(publish_chunk(
                1,
                QoS::AtLeastOnce,
                false,
                "publish",
                4,
                b"a",
            ))
            .await;
        assert_eq!(res.unwrap(), None);
        assert_eq!(chunk(b"b", false).await.unwrap(), None);
        assert_eq!(chunk(b"cd", true).await.unwrap(), None);
        assert_eq!(counter.get(), 0);

        // the last chunk ends the dropped payload
        assert!(matches!(
            chunk(b"e", true).await,
            Err(DispatcherError::Protocol(MqttProtocolError::Decode(
                DecodeError::UnexpectedPayload
            )))
        ));
    }

    #[test]
    fn test_has_more_chunks() {
        use crate::inflight::SizedRequest;

        let publish = |size| {
            Decoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtLeastOnce,
                    topic: ByteString::new(),
                    packet_id: None,
                    payload_size: size,
                },
                Bytes::from_static(b"ab"),
                10,
            )
        };
        assert!(!publish(2).has_more_chunks());
        assert!(publish(5).has_more_chunks());
        assert!(Decoded::PayloadChunk(Bytes::from_static(b"c"), false).has_more_chunks());
        assert!(!Decoded::PayloadChunk(Bytes::from_static(b"de"), true).has_more_chunks());
        assert!(!Decoded::Packet(codec::Packet::PingRequest, 2).has_more_chunks());
    }

    #[test]
    fn test_inflight_kind() {
        use crate::inflight::SizedRequest;

        let id = NonZeroU16::new(1).unwrap();
        let publish = Decoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                topic: ByteString::new(),
                packet_id: None,
                payload_size: 0,
            },
            Bytes::new(),
            10,
        );
        assert!(publish.is_limited() && !publish.is_ordered());

        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"c"), false);
        assert!(!chunk.is_limited() && chunk.is_ordered());
        let disconnect = Decoded::Packet(Packet::Disconnect, 2);
        assert!(!disconnect.is_limited() && disconnect.is_ordered());

        for pkt in [
            Packet::PingRequest,
            Packet::PublishAck { packet_id: id },
            Packet::PublishReceived { packet_id: id },
            Packet::PublishRelease { packet_id: id },
            Packet::PublishComplete { packet_id: id },
        ] {
            let pkt = Decoded::Packet(pkt, 2);
            assert!(!pkt.is_limited() && !pkt.is_ordered(), "{pkt:?}");
        }
    }

    fn assert_unexpected<E: std::fmt::Debug>(
        res: &Result<Option<Packet>, DispatcherError<E>>,
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
        let pid = NonZeroU16::new(1).unwrap();
        let counter = Rc::new(Cell::new(0));
        let (_io, _, disp) = redelivery_dispatcher!(counter);

        // a second CONNECT is a protocol violation [MQTT-3.1.0-2], packets sent
        // by the server only are not expected from client [MQTT-4.8.0-1]
        for (pkt, tp) in [
            (Packet::Connect(Box::default()), packet_type::CONNECT),
            (
                Packet::ConnectAck(codec::ConnectAck {
                    return_code: codec::ConnectAckReason::ConnectionAccepted,
                    session_present: false,
                }),
                packet_type::CONNACK,
            ),
            (
                Packet::SubscribeAck {
                    packet_id: pid,
                    status: vec![],
                },
                packet_type::SUBACK,
            ),
            (
                Packet::UnsubscribeAck { packet_id: pid },
                packet_type::UNSUBACK,
            ),
            (Packet::PingResponse, packet_type::PINGRESP),
        ] {
            assert_unexpected(&disp.call(Decoded::Packet(pkt, 999)).await, tp);
        }
        assert_eq!(counter.get(), 0);
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
            .add(MqttServiceConfig::new().set_max_payload_buffer_size(4))
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
                Hold(h, pfail.clone(), || (), || ()),
                FailReady(fail.clone(), || {
                    DispatcherError::Protocol(MqttProtocolError::ReadTimeout)
                }),
                cfg.get(),
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
