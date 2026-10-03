use std::{cell::Cell, cell::RefCell, marker::PhantomData, num, rc::Rc};

use ntex_bytes::ByteString;
use ntex_error::{Failure, IntoFailure};
use ntex_service::pipeline::PipelineState;
use ntex_service::{Ctx, Service, ServiceFactory, cfg::Cfg};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::{HashMap, future::join, hash_map, services::inflight::InFlightService};

use crate::error::{DecodeError, DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::payload::{Payload, PayloadStatus};
use crate::{MqttServiceConfig, types::QoS, types::packet_type};

use super::codec::{self, Decoded, DisconnectReasonCode, Encoded, Packet};
use super::control::{Pkt, ProtocolMessage, ProtocolMessageAck};
use super::publish::{Publish, PublishAck};
use super::{Session, ToPublishAck, shared::Ack, shared::MqttShared};

/// MQTT 5 protocol dispatcher
pub(super) fn factory<AppSt, E, Pub, Ctl>(
    publish: Pub,
    control: Ctl,
) -> impl ServiceFactory<
    Session<AppSt>,
    Decoded,
    Res = Option<Encoded>,
    Error = DispatcherError<E>,
    InitError = Failure,
>
where
    AppSt: 'static,
    E: From<Ctl::Error> + 'static,
    Pub: ServiceFactory<Session<AppSt>, Publish, Res = PublishAck> + 'static,
    Pub::Error: ToPublishAck<Error = E>,
    Pub::InitError: IntoFailure,
    Ctl: ServiceFactory<Session<AppSt>, ProtocolMessage, Res = ProtocolMessageAck> + 'static,
    Ctl::InitError: IntoFailure,
{
    ntex_service::factory(async move |con: &Session<AppSt>| {
        let cfg: Cfg<MqttServiceConfig> = con.cfg();

        // create services
        let sink = con.sink().shared();
        let (publish, control) = join(publish.create(con), control.create(con)).await;

        let publish = publish.map_err(IntoFailure::fail)?;
        let control = control.map_err(IntoFailure::fail)?;

        let control = BufferService::new(
            16,
            // limit number of in-flight messages
            PipelineState::new(InFlightService::new(1, control)),
        )
        .map_err(|err| match err {
            BufferServiceError::Service(e) => DispatcherError::Service(E::from(e)),
            BufferServiceError::RequestCanceled => {
                DispatcherError::Protocol(MqttProtocolError::ReadTimeout)
            }
        });

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
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C, E> {
    publish: T,
    inner: Inner<C>,
    cfg: Cfg<MqttServiceConfig>,
    /// Payload chunks of an ignored PUBLISH are dropped
    discard_payload: Cell<bool>,
    e: PhantomData<(St, E)>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashMap<num::NonZeroU16, InFlight>,
    aliases: HashMap<num::NonZeroU16, ByteString>,
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

impl PublishInfo {
    /// Marks the packet id as used, returns `false` if it is already in use
    fn insert_inflight(&mut self, packet_id: num::NonZeroU16, state: InFlight) -> bool {
        match self.inflight.entry(packet_id) {
            hash_map::Entry::Occupied(_) => false,
            hash_map::Entry::Vacant(entry) => {
                entry.insert(state);
                true
            }
        }
    }
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    T: Service<St, Publish, Res = PublishAck>,
    T::Error: ToPublishAck<Error = E>,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    fn new(sink: Rc<MqttShared>, publish: T, control: C, cfg: Cfg<MqttServiceConfig>) -> Self {
        Self {
            cfg,
            publish,
            inner: Inner {
                sink,
                control,
                info: RefCell::new(PublishInfo {
                    aliases: HashMap::default(),
                    inflight: HashMap::default(),
                }),
            },
            discard_payload: Cell::new(false),
            e: PhantomData,
        }
    }

    fn tag(&self) -> &'static str {
        self.inner.sink.tag()
    }
}

impl<St, T, C, E> Service<St, Decoded> for Dispatcher<St, T, C, E>
where
    E: 'static,
    T: Service<St, Publish, Res = PublishAck> + 'static,
    T::Error: ToPublishAck<Error = E>,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    type Res = Option<Encoded>;
    type Error = DispatcherError<E>;

    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), ctx.ready(&self.inner.control)).await;
        if (res1.is_err() || res2.is_err())
            && let Some(pl) = self.inner.sink.payload.take()
        {
            self.inner.sink.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }

        res1.map_err(|e| DispatcherError::Service(e.into_error()))?;
        res2?;
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        log::trace!("{}: Shutdown v5 dispatcher", self.tag());
        self.inner.sink.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.drop_sink(true);

        ctx.shutdown(&self.publish).await;
        ctx.shutdown(&self.inner.control).await;
    }

    #[allow(clippy::too_many_lines, clippy::await_holding_refcell_ref)]
    async fn call(&self, req: Decoded, ctx: Ctx<'_, Self, St>) -> Result<Self::Res, Self::Error> {
        log::trace!("{}: Dispatch v5 packet: {:#?}", self.tag(), req);

        match req {
            Decoded::Publish(mut publish, payload, size) => {
                let info = &self.inner;
                let packet_id = publish.packet_id;

                if publish.topic.contains(['#', '+']) {
                    return Err(SpecViolation::Pub_3_3_2_2.into());
                }
                // (MQTT 5.0, 3.3.2.3.5)
                if publish
                    .properties
                    .response_topic
                    .as_ref()
                    .is_some_and(|t| t.contains(['#', '+']))
                {
                    return Err(SpecViolation::Pub_3_3_2_14.into());
                }
                // (MQTT 5.0, 3.3.4)
                if !publish.properties.subscription_ids.is_empty() {
                    return Err(SpecViolation::Pub_3_3_4_6.into());
                }

                // response to a re-delivered PUBLISH with a packet id in use
                let mut redelivered = None;
                {
                    let mut inner = info.info.borrow_mut();
                    let state = &self.inner.sink;

                    if let Some(pid) = packet_id {
                        redelivered = match inner.inflight.get(&pid).copied() {
                            // a re-delivery keeps the packet id [MQTT-3.3.1-1] and is not
                            // a new message until PUBACK is sent [MQTT-4.3.2-5],
                            // the ack of the first delivery acks both
                            Some(InFlight::Publish(qos)) if publish.dup && qos == publish.qos => {
                                log::trace!(
                                    "{}: Re-delivered publish packet is ignored: {pid:?}",
                                    self.tag()
                                );
                                Some(None)
                            }
                            // until PUBREL, PUBLISH with the same packet id is acked by PUBREC
                            // and is not delivered [MQTT-4.3.3-10]
                            Some(InFlight::Received)
                                if publish.dup && publish.qos == QoS::ExactlyOnce =>
                            {
                                log::trace!(
                                    "{}: Re-delivered publish packet is received: {pid:?}",
                                    self.tag()
                                );
                                Some(Some(Encoded::Packet(codec::Packet::PublishReceived(
                                    codec::PublishAck {
                                        packet_id: pid,
                                        ..Default::default()
                                    },
                                ))))
                            }
                            _ => None,
                        };

                        // check for receive maximum
                        let receive_max = state.receive_max();
                        if redelivered.is_none()
                            && receive_max != 0
                            && inner.inflight.len() >= receive_max as usize
                        {
                            log::trace!(
                                "{}: Receive maximum exceeded: max: {} in-flight: {}",
                                self.tag(),
                                receive_max,
                                inner.inflight.len()
                            );
                            return Err(SpecViolation::Pub_3_3_4_7.into());
                        }

                        // check max allowed qos
                        if publish.qos > state.max_qos() {
                            log::trace!(
                                "{}: Max allowed QoS is violated, max {:?} provided {:?}",
                                self.tag(),
                                state.max_qos(),
                                publish.qos
                            );
                            return Err(SpecViolation::Connack_3_2_2_11.into());
                        }
                        if publish.retain && !state.codec.retain_available() {
                            log::trace!("{}: Retain is not available but is set", self.tag());
                            return Err(SpecViolation::Connack_3_2_2_14.into());
                        }

                        // check for duplicated packet id
                        if redelivered.is_none()
                            && !inner.insert_inflight(pid, InFlight::Publish(publish.qos))
                        {
                            log::trace!(
                                "{}: Duplicated packet id for publish packet: {pid:?}",
                                self.tag()
                            );
                            // queued to keep acks in the order packets are received
                            let ack = codec::PublishAck {
                                packet_id: pid,
                                reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                ..Default::default()
                            };
                            redelivered =
                                Some(Some(Encoded::Packet(if publish.qos == QoS::ExactlyOnce {
                                    codec::Packet::PublishReceived(ack)
                                } else {
                                    codec::Packet::PublishAck(ack)
                                })));
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        if publish.topic.is_empty() {
                            // lookup topic by provided alias
                            if let Some(aliased_topic) = inner.aliases.get(&alias) {
                                publish.topic = aliased_topic.clone();
                            } else {
                                return Err(MqttProtocolError::violation(
                                    DisconnectReasonCode::TopicAliasInvalid,
                                    "Unknown topic alias",
                                )
                                .into());
                            }
                        } else {
                            // record new alias
                            match inner.aliases.entry(alias) {
                                hash_map::Entry::Occupied(mut entry) => {
                                    if entry.get().as_str() != publish.topic.as_str() {
                                        let mut topic = publish.topic.clone();
                                        topic.trimdown();
                                        entry.insert(topic);
                                    }
                                }
                                hash_map::Entry::Vacant(entry) => {
                                    if alias.get() > state.topic_alias_max() {
                                        return Err(SpecViolation::Connack_3_2_2_17.into());
                                    }
                                    let mut topic = publish.topic.clone();
                                    topic.trimdown();
                                    entry.insert(topic);
                                }
                            }
                        }
                    }

                    if let Some(res) = redelivered {
                        if publish.payload_size != payload.len() as u32 {
                            self.discard_payload.set(true);
                        }
                        return Ok(res);
                    }

                    if !state.is_active()
                        && self
                            .cfg
                            .handle_qos_after_disconnect
                            .is_none_or(|max_qos| publish.qos > max_qos)
                    {
                        return Ok(None);
                    }
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
                    packet_id.map_or(0, num::NonZero::get),
                    info,
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
            Decoded::Packet(Packet::PublishAck(packet), _) => {
                self.inner.sink.pkt_ack(Ack::Publish(packet))?;
                Ok(None)
            }
            Decoded::Packet(Packet::PublishReceived(pkt), _) => {
                self.inner.sink.pkt_ack(Ack::Receive(pkt))?;
                Ok(None)
            }
            Decoded::Packet(Packet::PublishRelease(ack), size) => {
                let packet_id = ack.packet_id;
                let state = self.inner.info.borrow().inflight.get(&packet_id).copied();
                if state == Some(InFlight::Received) {
                    // packet id is released after PUBCOMP [MQTT-4.3.3-12]
                    self.inner
                        .control_pkt(ProtocolMessage::pubrel(ack, size), packet_id.get(), ctx)
                        .await
                } else if state.is_some() {
                    // PUBREL is sent in response to PUBREC [MQTT-4.3.3-4]
                    Err(MqttProtocolError::unexpected_packet(
                        packet_type::PUBREL,
                        "PublishRelease packet before PublishReceived",
                    )
                    .into())
                } else {
                    Ok(Some(Encoded::Packet(codec::Packet::PublishComplete(
                        codec::PublishAck2 {
                            packet_id: ack.packet_id,
                            reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                            properties: codec::UserProperties::default(),
                            reason_string: None,
                        },
                    ))))
                }
            }
            Decoded::Packet(Packet::PublishComplete(pkt), _) => {
                self.inner.sink.pkt_ack(Ack::Complete(pkt))?;
                Ok(None)
            }
            Decoded::Packet(Packet::Auth(pkt), size) => {
                if self.inner.sink.is_active() {
                    self.inner
                        .control(ProtocolMessage::auth(pkt, size), ctx)
                        .await
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PingRequest, _) => {
                self.inner.control(ProtocolMessage::ping(), ctx).await
            }
            Decoded::Packet(Packet::Disconnect(pkt), size) => {
                self.inner.sink.set_disconnect_recv();

                // Check session expiry
                if let Some(val) = pkt.session_expiry_interval_secs
                    && val > 0
                    && self.inner.sink.is_zero_session_expiry()
                {
                    Err(SpecViolation::Disconnect_3_14_2_22.into())
                } else {
                    self.inner.sink.is_disconnect_sent();
                    self.inner.sink.close(None);
                    self.inner
                        .control(ProtocolMessage::remote_disconnect(pkt, size), ctx)
                        .await
                }
            }
            Decoded::Packet(Packet::Subscribe(pkt), size) => {
                if !self.inner.sink.is_active() {
                    Ok(None)
                } else if pkt
                    .topic_filters
                    .iter()
                    .any(|(tf, _)| !crate::topic::is_valid(tf))
                {
                    Err(SpecViolation::Subs_4_7_1.into())
                } else if pkt
                    .topic_filters
                    .iter()
                    .any(|(tf, _)| !crate::topic::is_valid_shared(tf))
                {
                    Err(SpecViolation::Subs_4_8_2.into())
                } else if pkt
                    .topic_filters
                    .iter()
                    .any(|(tf, opts)| opts.no_local && crate::topic::is_shared(tf))
                {
                    // (MQTT 5.0, 3.8.3.1)
                    Err(SpecViolation::Subs_3_8_3_4.into())
                } else if pkt.id.is_some() && !self.inner.sink.codec.sub_ids_available() {
                    log::trace!(
                        "{}: Subscription Identifiers are not supported but was set",
                        self.tag()
                    );
                    Err(SpecViolation::Connack_3_2_2_3_12.into())
                } else if !self
                    .inner
                    .info
                    .borrow_mut()
                    .insert_inflight(pkt.packet_id, InFlight::Subscribe)
                {
                    // duplicated packet id, queued to keep acks in the order packets are received
                    Ok(Some(Encoded::Packet(codec::Packet::SubscribeAck(
                        codec::SubscribeAck {
                            packet_id: pkt.packet_id,
                            status: pkt
                                .topic_filters
                                .iter()
                                .map(|_| codec::SubscribeAckReason::PacketIdentifierInUse)
                                .collect(),
                            properties: codec::UserProperties::new(),
                            reason_string: None,
                        },
                    ))))
                } else {
                    let id = pkt.packet_id;
                    self.inner
                        .control_pkt(ProtocolMessage::subscribe(pkt, size), id.get(), ctx)
                        .await
                }
            }
            Decoded::Packet(Packet::Unsubscribe(pkt), size) => {
                if !self.inner.sink.is_active() {
                    Ok(None)
                } else if pkt
                    .topic_filters
                    .iter()
                    .any(|tf| !crate::topic::is_valid(tf))
                {
                    Err(SpecViolation::Subs_4_7_1.into())
                } else if pkt
                    .topic_filters
                    .iter()
                    .any(|tf| !crate::topic::is_valid_shared(tf))
                {
                    Err(SpecViolation::Subs_4_8_2.into())
                } else if !self
                    .inner
                    .info
                    .borrow_mut()
                    .insert_inflight(pkt.packet_id, InFlight::Subscribe)
                {
                    // duplicated packet id, queued to keep acks in the order packets are received
                    Ok(Some(Encoded::Packet(codec::Packet::UnsubscribeAck(
                        codec::UnsubscribeAck {
                            packet_id: pkt.packet_id,
                            status: pkt
                                .topic_filters
                                .iter()
                                .map(|_| codec::UnsubscribeAckReason::PacketIdentifierInUse)
                                .collect(),
                            properties: codec::UserProperties::new(),
                            reason_string: None,
                        },
                    ))))
                } else {
                    let id = pkt.packet_id;
                    self.inner
                        .control_pkt(ProtocolMessage::unsubscribe(pkt, size), id.get(), ctx)
                        .await
                }
            }
            Decoded::Packet(_, _) => Ok(None),
        }
    }
}

impl<C> Inner<C> {
    async fn control<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, St>,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        self.control_pkt(pkt, 0, ctx).await
    }

    async fn control_pkt<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        packet_id: u16,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, St>,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        let result = match ctx.call(&self.control, pkt).await {
            Ok(result) => {
                if let Some(id) = num::NonZeroU16::new(packet_id) {
                    self.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                self.sink.drop_sink(false);
                self.sink.drop_payload(&PayloadError::Service);
                return Err(err);
            }
        };

        let response = match result.packet {
            Pkt::Packet(pkt) => Ok(Some(Encoded::Packet(pkt))),
            Pkt::Disconnect(pkt) => {
                if self.sink.is_disconnect_sent() {
                    Ok(None)
                } else {
                    Ok(Some(Encoded::Packet(codec::Packet::from(pkt))))
                }
            }
            Pkt::None => Ok(None),
        };

        if result.disconnect {
            self.sink.drop_sink(true);
            self.sink.drop_payload(&PayloadError::Service);
        }
        response
    }
}

/// Publish service response future
async fn publish_fn<'f, St, T, C, E>(
    publish: &T,
    pkt: Publish,
    packet_id: u16,
    inner: &'f Inner<C>,
    ctx: Ctx<'f, Dispatcher<St, T, C, E>, St>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    T: Service<St, Publish, Res = PublishAck>,
    T::Error: ToPublishAck<Error = E>,
    C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    let qos2 = pkt.qos() == QoS::ExactlyOnce;
    let ack = match ctx.call(publish, pkt).await {
        Ok(ack) => ack,
        Err(e) => {
            if packet_id != 0 {
                match e.try_ack() {
                    Ok(ack) => ack,
                    Err(e) => {
                        return Err(DispatcherError::Service(e));
                    }
                }
            } else {
                return Err(DispatcherError::Service(e.into_error()));
            }
        }
    };

    if let Some(id) = num::NonZeroU16::new(packet_id) {
        let ack = if qos2 {
            if u8::from(ack.reason_code) < 0x80 {
                // packet id stays in use until PUBREL [MQTT-4.3.3-10]
                inner
                    .info
                    .borrow_mut()
                    .inflight
                    .insert(id, InFlight::Received);
            } else {
                // PUBREC with error code completes the flow [MQTT-4.3.3-9]
                inner.info.borrow_mut().inflight.remove(&id);
            }
            codec::Packet::PublishReceived(codec::PublishAck {
                packet_id: id,
                reason_code: ack.reason_code,
                reason_string: ack.reason_string,
                properties: ack.properties,
            })
        } else {
            inner.info.borrow_mut().inflight.remove(&id);
            codec::Packet::PublishAck(codec::PublishAck {
                packet_id: id,
                reason_code: ack.reason_code,
                reason_string: ack.reason_string,
                properties: ack.properties,
            })
        };
        Ok(Some(Encoded::Packet(ack)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};
    use std::{cell::Cell, future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_service};

    use super::*;
    use crate::{error, v5::MqttSink, v5::codec};
    use ntex_util::{future::lazy, time::Millis, time::sleep};

    #[derive(Debug)]
    struct TestError;

    impl From<()> for TestError {
        fn from((): ()) -> Self {
            TestError
        }
    }

    impl TryFrom<TestError> for PublishAck {
        type Error = TestError;

        fn try_from(err: TestError) -> Result<Self, Self::Error> {
            Err(err)
        }
    }

    #[ntex::test]
    async fn test_spec_violations() {
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce))
            .into();

        let io = Io::new(IoTest::create().0, cfg.clone());
        let codec = codec::Codec::default();
        codec.set_retain_available(false);
        codec.set_sub_ids_available(false);
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Rc::default()));
        shared.set_topic_alias_max(1);

        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(async |msg: Publish| Ok::<_, TestError>(msg.ack())),
                fn_service(async |msg: ProtocolMessage| {
                    Ok::<_, DispatcherError<TestError>>(msg.ack())
                }),
                cfg.get(),
            ),
        );

        // retain not available
        let err = disp
            .call(Decoded::Publish(
                codec::Publish {
                    retain: true,
                    qos: QoS::AtLeastOnce,
                    packet_id: NonZeroU16::new(1),
                    ..Default::default()
                },
                Bytes::new(),
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
            error::ViolationInner::Spec(error::SpecViolation::Connack_3_2_2_14)
        );

        // topic aliases
        let mut pkt = codec::Publish::default();
        pkt.properties.topic_alias = NonZeroU16::new(1);

        let err = disp
            .call(Decoded::Publish(pkt, Bytes::new(), 999))
            .await
            .err()
            .unwrap();
        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        assert_eq!(
            err.inner,
            error::ViolationInner::Common {
                reason: DisconnectReasonCode::TopicAliasInvalid,
                message: "Unknown topic alias"
            }
        );

        // add topic aliase
        let mut pkt = codec::Publish {
            packet_id: NonZeroU16::new(1),
            topic: ByteString::from_static("test"),
            ..Default::default()
        };
        pkt.properties.topic_alias = NonZeroU16::new(1);
        let res = disp.call(Decoded::Publish(pkt, Bytes::new(), 999)).await;
        assert!(res.is_ok());

        // new topic alias
        let mut pkt = codec::Publish {
            packet_id: NonZeroU16::new(2),
            topic: ByteString::from_static("test2"),
            ..Default::default()
        };
        pkt.properties.topic_alias = NonZeroU16::new(2);

        let err = disp
            .call(Decoded::Publish(pkt, Bytes::new(), 999))
            .await
            .err()
            .unwrap();
        let DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)) = err else {
            panic!()
        };
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Connack_3_2_2_17)
        );

        // unknown PublishRelease
        let pkt = disp
            .call(Decoded::Packet(
                Packet::PublishRelease(codec::PublishAck2 {
                    packet_id: NonZeroU16::new(100).unwrap(),
                    reason_code: codec::PublishAck2Reason::Success,
                    properties: codec::UserProperties::default(),
                    reason_string: None,
                }),
                999,
            ))
            .await
            .ok()
            .unwrap()
            .unwrap();

        let Encoded::Packet(Packet::PublishComplete(pkt)) = pkt else {
            panic!()
        };
        assert_eq!(pkt.reason_code, codec::PublishAck2Reason::PacketIdNotFound);

        // subscribe invalid topic
        let err = disp
            .call(Decoded::Packet(
                Packet::Subscribe(codec::Subscribe {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    id: None,
                    user_properties: codec::UserProperties::default(),
                    topic_filters: vec![(ByteString::new(), codec::SubscriptionOptions::default())],
                }),
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

        // subscribe sub id not available
        let err = disp
            .call(Decoded::Packet(
                Packet::Subscribe(codec::Subscribe {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    id: NonZeroU32::new(1),
                    user_properties: codec::UserProperties::default(),
                    topic_filters: vec![(
                        ByteString::from_static("test"),
                        codec::SubscriptionOptions::default(),
                    )],
                }),
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
            error::ViolationInner::Spec(error::SpecViolation::Connack_3_2_2_3_12)
        );

        // unsubscribe invalid topic
        let err = disp
            .call(Decoded::Packet(
                Packet::Unsubscribe(codec::Unsubscribe {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    user_properties: codec::UserProperties::default(),
                    topic_filters: vec![ByteString::new()],
                }),
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

    #[ntex::test]
    async fn test_spec_violations_v5() {
        let cfg: SharedCfg = SharedCfg::new("DBG").add(MqttServiceConfig::new()).into();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            Rc::default(),
        ));
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared.clone(),
                fn_service(async |msg: Publish| Ok::<_, TestError>(msg.ack())),
                fn_service(async |msg: ProtocolMessage| {
                    Ok::<_, DispatcherError<TestError>>(msg.ack())
                }),
                cfg.get(),
            ),
        );
        let violation = |res: Result<Option<Encoded>, DispatcherError<TestError>>| {
            let Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err))) = res
            else {
                panic!("expected protocol violation")
            };
            assert_eq!(err.reason(), DisconnectReasonCode::ProtocolError);
            let error::ViolationInner::Spec(err) = err.inner else {
                panic!()
            };
            err
        };
        let publish = |response_topic: Option<&'static str>, sub_id: bool| {
            let mut pkt = codec::Publish {
                topic: ByteString::from_static("test"),
                ..Default::default()
            };
            pkt.properties.response_topic = response_topic.map(ByteString::from_static);
            if sub_id {
                pkt.properties
                    .subscription_ids
                    .push(NonZeroU32::new(1).unwrap());
            }
            Decoded::Publish(pkt, Bytes::new(), 999)
        };
        let subscribe = |filter: &'static str, no_local: bool| {
            Decoded::Packet(
                Packet::Subscribe(codec::Subscribe {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    id: None,
                    user_properties: codec::UserProperties::default(),
                    topic_filters: vec![(
                        ByteString::from_static(filter),
                        codec::SubscriptionOptions {
                            no_local,
                            ..Default::default()
                        },
                    )],
                }),
                999,
            )
        };
        let unsubscribe = |filter: &'static str| {
            Decoded::Packet(
                Packet::Unsubscribe(codec::Unsubscribe {
                    packet_id: NonZeroU16::new(2).unwrap(),
                    user_properties: codec::UserProperties::default(),
                    topic_filters: vec![ByteString::from_static(filter)],
                }),
                999,
            )
        };

        // [MQTT-3.3.2-14] Response Topic must not contain wildcards
        for topic in ["resp/+", "resp/#"] {
            let err = violation(disp.call(publish(Some(topic), false)).await);
            assert_eq!(err, error::SpecViolation::Pub_3_3_2_14);
        }
        assert!(disp.call(publish(Some("resp/a"), false)).await.is_ok());

        // [MQTT-3.3.4-6] Client must not send a Subscription Identifier
        let err = violation(disp.call(publish(None, true)).await);
        assert_eq!(err, error::SpecViolation::Pub_3_3_4_6);

        // [MQTT-4.8.2-1], [MQTT-4.8.2-2] ShareName format
        for filter in ["$share//a", "$share/g", "$share/+/a"] {
            let err = violation(disp.call(subscribe(filter, false)).await);
            assert_eq!(err, error::SpecViolation::Subs_4_8_2);
            let err = violation(disp.call(unsubscribe(filter)).await);
            assert_eq!(err, error::SpecViolation::Subs_4_8_2);
        }

        // [MQTT-3.8.3-4] No Local must not be set on a Shared Subscription
        let err = violation(disp.call(subscribe("$share/g/a", true)).await);
        assert_eq!(err, error::SpecViolation::Subs_3_8_3_4);

        let res = disp.call(subscribe("$share/g/a", false)).await.unwrap();
        assert!(matches!(
            res,
            Some(Encoded::Packet(Packet::SubscribeAck(_)))
        ));
        let res = disp.call(unsubscribe("$share/g/a")).await.unwrap();
        assert!(matches!(
            res,
            Some(Encoded::Packet(Packet::UnsubscribeAck(_)))
        ));
        let mut pkt = subscribe("a", true);
        if let Decoded::Packet(Packet::Subscribe(ref mut pkt), _) = pkt {
            pkt.packet_id = NonZeroU16::new(3).unwrap();
        }
        let res = disp.call(pkt).await.unwrap();
        assert!(matches!(
            res,
            Some(Encoded::Packet(Packet::SubscribeAck(_)))
        ));
    }

    #[test]
    fn test_has_more_chunks() {
        use crate::inflight::SizedRequest;

        let publish = |size| {
            Decoded::Publish(
                codec::Publish {
                    payload_size: size,
                    ..Default::default()
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

    fn qos2_dispatcher(
        pubrel_calls: Rc<Cell<usize>>,
        published: Rc<Cell<usize>>,
        receive_max: u16,
    ) -> (
        Io,
        Pipeline<Decoded, Option<Encoded>, DispatcherError<TestError>>,
    ) {
        let cfg: SharedCfg = SharedCfg::new("DBG").add(MqttServiceConfig::new()).into();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            Rc::default(),
        ));
        shared.set_max_qos(QoS::ExactlyOnce);
        shared.set_receive_max(receive_max);

        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(
                shared,
                fn_service(async move |msg: Publish| {
                    published.set(published.get() + 1);
                    if msg.topic().path() == "slow" {
                        sleep(Millis(100)).await;
                    }
                    if msg.topic().path() == "err" {
                        Ok(PublishAck::new(codec::PublishAckReason::UnspecifiedError))
                    } else {
                        Ok::<_, TestError>(msg.ack())
                    }
                }),
                fn_service(move |msg: ProtocolMessage| {
                    if matches!(msg, ProtocolMessage::PublishRelease(_)) {
                        pubrel_calls.set(pubrel_calls.get() + 1);
                    }
                    async move { Ok::<_, DispatcherError<TestError>>(msg.ack()) }
                }),
                cfg.get(),
            ),
        );
        (io, disp)
    }

    fn qos_publish(id: u16, qos: QoS, topic: &'static str) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                qos,
                packet_id: NonZeroU16::new(id),
                topic: ByteString::from_static(topic),
                ..Default::default()
            },
            Bytes::new(),
            999,
        )
    }

    fn pubrel(id: u16) -> Decoded {
        Decoded::Packet(
            Packet::PublishRelease(codec::PublishAck2 {
                packet_id: NonZeroU16::new(id).unwrap(),
                ..Default::default()
            }),
            999,
        )
    }

    fn ack(id: u16, qos2: bool, reason_code: codec::PublishAckReason) -> Encoded {
        let ack = codec::PublishAck {
            packet_id: NonZeroU16::new(id).unwrap(),
            reason_code,
            ..Default::default()
        };
        Encoded::Packet(if qos2 {
            Packet::PublishReceived(ack)
        } else {
            Packet::PublishAck(ack)
        })
    }

    fn pubcomp(id: u16, reason_code: codec::PublishAck2Reason) -> Encoded {
        Encoded::Packet(Packet::PublishComplete(codec::PublishAck2 {
            packet_id: NonZeroU16::new(id).unwrap(),
            reason_code,
            ..Default::default()
        }))
    }

    #[ntex::test]
    async fn test_publish_qos2() {
        use codec::{PublishAck2Reason as Ack2, PublishAckReason as Ack};

        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, disp) = qos2_dispatcher(pubrel_calls.clone(), Rc::default(), 1);
        let receive_max_exceeded = |res: Result<Option<Encoded>, DispatcherError<TestError>>| {
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                    if err.inner == error::ViolationInner::Spec(SpecViolation::Pub_3_3_4_7)
            )
        };

        let res = disp.call(qos_publish(1, QoS::ExactlyOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(1, true, Ack::Success)));

        // packet id stays in use until PUBREL [MQTT-4.3.3-10]
        let res = disp.call(qos_publish(2, QoS::AtLeastOnce, "test")).await;
        assert!(receive_max_exceeded(res));

        // packet id is released after PUBCOMP [MQTT-4.3.3-12]
        let res = disp.call(pubrel(1)).await;
        assert_eq!(res.unwrap(), Some(pubcomp(1, Ack2::Success)));
        assert_eq!(pubrel_calls.get(), 1);
        let res = disp.call(qos_publish(2, QoS::AtLeastOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(2, false, Ack::Success)));
        let res = disp.call(pubrel(1)).await;
        assert_eq!(res.unwrap(), Some(pubcomp(1, Ack2::PacketIdNotFound)));
        assert_eq!(pubrel_calls.get(), 1);

        // PUBREC with error code releases packet id [MQTT-4.3.3-9]
        let res = disp.call(qos_publish(3, QoS::ExactlyOnce, "err")).await;
        assert_eq!(res.unwrap(), Some(ack(3, true, Ack::UnspecifiedError)));
        let res = disp.call(qos_publish(4, QoS::AtLeastOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(4, false, Ack::Success)));
        let res = disp.call(pubrel(3)).await;
        assert_eq!(res.unwrap(), Some(pubcomp(3, Ack2::PacketIdNotFound)));
        assert_eq!(pubrel_calls.get(), 1);
    }

    #[ntex::test]
    async fn test_pubrel_before_pubrec() {
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, disp) = qos2_dispatcher(pubrel_calls.clone(), Rc::default(), 1);

        let mut f = Box::pin(disp.call(qos_publish(1, QoS::ExactlyOnce, "slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        // PUBREL is a response to PUBREC [MQTT-4.3.3-4]
        let res = disp.call(pubrel(1)).await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if matches!(err.inner, error::ViolationInner::UnexpectedPacket { .. })
        ));
        assert_eq!(pubrel_calls.get(), 0);
    }

    fn redelivery(id: u16, qos: QoS, chunked: bool) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup: true,
                qos,
                packet_id: NonZeroU16::new(id),
                topic: ByteString::from_static("test"),
                payload_size: if chunked { 6 } else { 3 },
                ..Default::default()
            },
            Bytes::from_static(b"abc"),
            999,
        )
    }

    #[ntex::test]
    async fn test_publish_redelivery() {
        use codec::{PublishAck2Reason as Ack2, PublishAckReason as Ack};

        let pubrel_calls = Rc::new(Cell::new(0));
        let published = Rc::new(Cell::new(0));
        let (_io, disp) = qos2_dispatcher(pubrel_calls.clone(), published.clone(), 16);

        // re-delivery is ignored until PUBACK [MQTT-4.3.2-5]
        let mut f = Box::pin(disp.call(qos_publish(1, QoS::AtLeastOnce, "slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp.call(redelivery(1, QoS::AtLeastOnce, false)).await;
        assert_eq!(res.unwrap(), None);
        let res = disp.call(redelivery(1, QoS::ExactlyOnce, false)).await;
        assert_eq!(res.unwrap(), Some(ack(1, true, Ack::PacketIdentifierInUse)));
        let res = disp.call(qos_publish(1, QoS::AtLeastOnce, "test")).await;
        assert_eq!(
            res.unwrap(),
            Some(ack(1, false, Ack::PacketIdentifierInUse))
        );
        assert_eq!(f.await.unwrap(), Some(ack(1, false, Ack::Success)));
        assert_eq!(published.get(), 1);

        // re-delivery is acked by PUBREC until PUBREL [MQTT-4.3.3-10]
        let res = disp.call(qos_publish(2, QoS::ExactlyOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(2, true, Ack::Success)));
        let res = disp.call(redelivery(2, QoS::ExactlyOnce, true)).await;
        assert_eq!(res.unwrap(), Some(ack(2, true, Ack::Success)));
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"de"), false);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"f"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let res = disp.call(redelivery(2, QoS::AtLeastOnce, false)).await;
        assert_eq!(
            res.unwrap(),
            Some(ack(2, false, Ack::PacketIdentifierInUse))
        );
        let res = disp.call(qos_publish(2, QoS::ExactlyOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(2, true, Ack::PacketIdentifierInUse)));
        assert_eq!(published.get(), 2);

        // after PUBCOMP re-delivery is a new message [MQTT-4.3.3-12]
        let res = disp.call(pubrel(2)).await;
        assert_eq!(res.unwrap(), Some(pubcomp(2, Ack2::Success)));
        assert_eq!(pubrel_calls.get(), 1);
        let res = disp.call(redelivery(2, QoS::ExactlyOnce, true)).await;
        assert_eq!(res.unwrap(), Some(ack(2, true, Ack::Success)));
        assert_eq!(published.get(), 3);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"def"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"g"), true);
        assert!(disp.call(chunk).await.is_err());

        // re-delivery does not use receive maximum quota
        let (_io, disp) = qos2_dispatcher(pubrel_calls.clone(), published.clone(), 1);
        let res = disp.call(qos_publish(1, QoS::ExactlyOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(1, true, Ack::Success)));
        let res = disp.call(redelivery(1, QoS::ExactlyOnce, false)).await;
        assert_eq!(res.unwrap(), Some(ack(1, true, Ack::Success)));
        assert_eq!(published.get(), 4);
    }
}
