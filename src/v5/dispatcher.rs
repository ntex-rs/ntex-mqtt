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

use super::codec::{self, Decoded, DisconnectReasonCode, Packet};
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
    Res = Option<Packet>,
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

    fn is_limited(&self) -> bool {
        matches!(self, Decoded::Publish(..))
    }

    fn is_ordered(&self) -> bool {
        matches!(
            self,
            Decoded::PayloadChunk(..) | Decoded::Packet(Packet::Disconnect(_), _)
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
    e: PhantomData<(St, E)>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashMap<num::NonZeroU16, InFlight>,
    // SUBSCRIBE and UNSUBSCRIBE packets in `inflight`
    subscribes: usize,
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
                if state == InFlight::Subscribe {
                    self.subscribes += 1;
                }
                true
            }
        }
    }

    /// Releases the packet id
    fn remove_inflight(&mut self, packet_id: num::NonZeroU16) {
        if self.inflight.remove(&packet_id) == Some(InFlight::Subscribe) {
            self.subscribes -= 1;
        }
    }

    /// In-flight `QoS 1` and `QoS 2` PUBLISH packets, Receive Maximum counts
    /// PUBLISH packets only [MQTT-4.9.0-2]
    fn publishes(&self) -> usize {
        self.inflight.len() - self.subscribes
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
                    subscribes: 0,
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
    type Res = Option<Packet>;
    type Error = DispatcherError<E>;

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
                // applies to PUBLISH of any QoS [MQTT-3.2.2-14]
                if publish.retain && !self.inner.sink.codec.retain_available() {
                    log::trace!("{}: Retain is not available but is set", self.tag());
                    return Err(SpecViolation::Connack_3_2_2_14.into());
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
                            // until PUBREL, any subsequent PUBLISH with the same packet id is
                            // acked by PUBREC and is not delivered, irrespective of DUP
                            // [MQTT-4.3.3-10]
                            Some(InFlight::Received) if publish.qos == QoS::ExactlyOnce => {
                                log::trace!(
                                    "{}: Re-delivered publish packet is received: {pid:?}",
                                    self.tag()
                                );
                                Some(Some(codec::Packet::PublishReceived(codec::PublishAck {
                                    packet_id: pid,
                                    ..Default::default()
                                })))
                            }
                            _ => None,
                        };

                        // check for receive maximum
                        let receive_max = state.receive_max();
                        if redelivered.is_none()
                            && receive_max != 0
                            && inner.publishes() >= receive_max as usize
                        {
                            log::trace!(
                                "{}: Receive maximum exceeded: max: {} in-flight: {}",
                                self.tag(),
                                receive_max,
                                inner.publishes()
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
                            redelivered = Some(Some(if publish.qos == QoS::ExactlyOnce {
                                codec::Packet::PublishReceived(ack)
                            } else {
                                codec::Packet::PublishAck(ack)
                            }));
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
                        // payload chunks of the dropped publish are dropped as well
                        if publish.payload_size != payload.len() as u32 {
                            self.discard_payload.set(true);
                        }
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
                    Ok(Some(codec::Packet::PublishComplete(codec::PublishAck2 {
                        packet_id: ack.packet_id,
                        reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                        properties: codec::UserProperties::default(),
                        reason_string: None,
                    })))
                }
            }
            Decoded::Packet(Packet::PublishComplete(pkt), _) => {
                self.inner.sink.pkt_ack(Ack::Complete(pkt))?;
                Ok(None)
            }
            Decoded::Packet(Packet::Auth(pkt), size) => {
                if self.inner.sink.is_active() {
                    self.inner
                        .control(ProtocolMessage::auth(*pkt, size), ctx)
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
                        .control(ProtocolMessage::remote_disconnect(*pkt, size), ctx)
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
                } else if self.cfg.check_subs_availability
                    && !self.inner.sink.codec.shared_subs_available()
                    && pkt
                        .topic_filters
                        .iter()
                        .any(|(tf, _)| crate::topic::is_shared(tf))
                {
                    // (MQTT 5.0, 3.2.2.3.13)
                    Err(MqttProtocolError::violation(
                        DisconnectReasonCode::SharedSubscriptionNotSupported,
                        "Shared Subscriptions are not supported",
                    )
                    .into())
                } else if self.cfg.check_subs_availability
                    && !self.inner.sink.codec.wildcard_subs_available()
                    && pkt
                        .topic_filters
                        .iter()
                        .any(|(tf, _)| tf.contains(['+', '#']))
                {
                    // (MQTT 5.0, 3.2.2.3.11)
                    Err(MqttProtocolError::violation(
                        DisconnectReasonCode::WildcardSubscriptionsNotSupported,
                        "Wildcard Subscriptions are not supported",
                    )
                    .into())
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
                    Ok(Some(codec::Packet::SubscribeAck(codec::SubscribeAck {
                        packet_id: pkt.packet_id,
                        status: pkt
                            .topic_filters
                            .iter()
                            .map(|_| codec::SubscribeAckReason::PacketIdentifierInUse)
                            .collect(),
                        properties: codec::UserProperties::new(),
                        reason_string: None,
                    })))
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
                    Ok(Some(codec::Packet::UnsubscribeAck(codec::UnsubscribeAck {
                        packet_id: pkt.packet_id,
                        status: pkt
                            .topic_filters
                            .iter()
                            .map(|_| codec::UnsubscribeAckReason::PacketIdentifierInUse)
                            .collect(),
                        properties: codec::UserProperties::new(),
                        reason_string: None,
                    })))
                } else {
                    let id = pkt.packet_id;
                    self.inner
                        .control_pkt(ProtocolMessage::unsubscribe(pkt, size), id.get(), ctx)
                        .await
                }
            }
            // a second CONNECT is a protocol error [MQTT-3.1.0-2]
            Decoded::Packet(Packet::Connect(_), _) => Err(MqttProtocolError::unexpected_packet(
                packet_type::CONNECT,
                "[MQTT-3.1.0-2] Second CONNECT packet is received",
            )
            .into()),
            Decoded::Packet(
                pkt @ (Packet::ConnectAck(_)
                | Packet::SubscribeAck(_)
                | Packet::UnsubscribeAck(_)
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

impl<C> Inner<C> {
    async fn control<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, St>,
    ) -> Result<Option<Packet>, DispatcherError<E>>
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
    ) -> Result<Option<Packet>, DispatcherError<E>>
    where
        C: Service<St, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        let result = match ctx.call(&self.control, pkt).await {
            Ok(result) => {
                if let Some(id) = num::NonZeroU16::new(packet_id) {
                    self.info.borrow_mut().remove_inflight(id);
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
            Pkt::Packet(pkt) => Ok(Some(pkt)),
            Pkt::Disconnect(pkt) => {
                if self.sink.is_disconnect_sent() {
                    Ok(None)
                } else {
                    Ok(Some(codec::Packet::from(pkt)))
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
) -> Result<Option<Packet>, DispatcherError<E>>
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
                inner.info.borrow_mut().remove_inflight(id);
            }
            codec::Packet::PublishReceived(codec::PublishAck {
                packet_id: id,
                reason_code: ack.reason_code,
                reason_string: ack.reason_string,
                properties: ack.properties,
            })
        } else {
            inner.info.borrow_mut().remove_inflight(id);
            codec::Packet::PublishAck(codec::PublishAck {
                packet_id: id,
                reason_code: ack.reason_code,
                reason_string: ack.reason_string,
                properties: ack.properties,
            })
        };
        Ok(Some(ack))
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

    /// The response queue keeps packets, not the larger encoder items
    #[test]
    fn test_queue_slot_size() {
        use std::mem::size_of;

        // ordered calls respond with publish acks, other responses are boxed
        type Slot = crate::io::QueueSlot<MqttShared>;
        assert!(size_of::<Slot>() <= size_of::<codec::PublishAck>() + 8);
        assert!(size_of::<Slot>() < size_of::<Packet>());
        // large and rare packets are boxed
        assert!(size_of::<Packet>() < size_of::<codec::Disconnect>() + 8);
        assert!(size_of::<Packet>() < size_of::<codec::Auth>());
    }

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

    type Disp = Pipeline<Decoded, Option<Packet>, DispatcherError<TestError>>;

    fn pid(id: u16) -> NonZeroU16 {
        NonZeroU16::new(id).unwrap()
    }

    fn dispatcher<T, C>(
        cfg: MqttServiceConfig,
        publish: T,
        control: C,
    ) -> (Io, Rc<MqttShared>, Disp)
    where
        T: Service<Session<()>, Publish, Res = PublishAck, Error = TestError> + 'static,
        C: Service<
                Session<()>,
                ProtocolMessage,
                Res = ProtocolMessageAck,
                Error = DispatcherError<TestError>,
            > + 'static,
    {
        let cfg: SharedCfg = SharedCfg::new("DBG").add(cfg).into();
        let io = Io::new(IoTest::create().0, cfg.clone());
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            Rc::default(),
        ));
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(shared.clone(), publish, control, cfg.get()),
        );
        (io, shared, disp)
    }

    /// Dispatcher that acks all packets
    fn ack_dispatcher(cfg: MqttServiceConfig) -> (Io, Rc<MqttShared>, Disp) {
        dispatcher(
            cfg,
            fn_service(async |msg: Publish| Ok::<_, TestError>(msg.ack())),
            fn_service(async |msg: ProtocolMessage| Ok::<_, DispatcherError<TestError>>(msg.ack())),
        )
    }

    fn violation(
        res: &Result<Option<Packet>, DispatcherError<TestError>>,
    ) -> error::ProtocolViolationError {
        let Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err))) = res else {
            panic!("expected protocol violation, got {res:?}")
        };
        *err
    }

    fn subscribe(id: u16, filters: &[&'static str]) -> Decoded {
        Decoded::Packet(
            Packet::Subscribe(codec::Subscribe {
                packet_id: pid(id),
                id: None,
                user_properties: codec::UserProperties::default(),
                topic_filters: filters
                    .iter()
                    .map(|f| {
                        (
                            ByteString::from_static(f),
                            codec::SubscriptionOptions::default(),
                        )
                    })
                    .collect(),
            }),
            999,
        )
    }

    fn unsubscribe(id: u16, filters: &[&'static str]) -> Decoded {
        Decoded::Packet(
            Packet::Unsubscribe(codec::Unsubscribe {
                packet_id: pid(id),
                user_properties: codec::UserProperties::default(),
                topic_filters: filters.iter().map(|f| ByteString::from_static(f)).collect(),
            }),
            999,
        )
    }

    #[ntex::test]
    async fn test_spec_violations() {
        let (_io, shared, disp) =
            ack_dispatcher(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce));
        shared.codec.set_retain_available(false);
        shared.codec.set_sub_ids_available(false);
        shared.set_topic_alias_max(1);

        // retain not available, for any QoS [MQTT-3.2.2-14]
        for (qos, packet_id) in [
            (QoS::AtMostOnce, None),
            (QoS::AtLeastOnce, NonZeroU16::new(1)),
        ] {
            let res = disp
                .call(Decoded::Publish(
                    codec::Publish {
                        retain: true,
                        qos,
                        packet_id,
                        ..Default::default()
                    },
                    Bytes::new(),
                    999,
                ))
                .await;
            assert_eq!(
                violation(&res).inner,
                error::ViolationInner::Spec(error::SpecViolation::Connack_3_2_2_14)
            );
        }

        // topic aliases
        let mut pkt = codec::Publish::default();
        pkt.properties.topic_alias = NonZeroU16::new(1);

        let err = violation(&disp.call(Decoded::Publish(pkt, Bytes::new(), 999)).await);
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

        let err = violation(&disp.call(Decoded::Publish(pkt, Bytes::new(), 999)).await);
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Connack_3_2_2_17)
        );
        assert_eq!(err.reason(), DisconnectReasonCode::TopicAliasInvalid);

        // unknown PublishRelease
        let pkt = disp
            .call(Decoded::Packet(
                Packet::PublishRelease(codec::PublishAck2 {
                    packet_id: pid(100),
                    ..Default::default()
                }),
                999,
            ))
            .await
            .ok()
            .unwrap()
            .unwrap();

        let Packet::PublishComplete(pkt) = pkt else {
            panic!()
        };
        assert_eq!(pkt.reason_code, codec::PublishAck2Reason::PacketIdNotFound);

        // subscribe invalid topic
        let err = violation(&disp.call(subscribe(1, &[""])).await);
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Subs_4_7_1)
        );

        // subscribe sub id not available
        let mut pkt = subscribe(1, &["test"]);
        if let Decoded::Packet(Packet::Subscribe(ref mut pkt), _) = pkt {
            pkt.id = NonZeroU32::new(1);
        }
        let err = violation(&disp.call(pkt).await);
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Connack_3_2_2_3_12)
        );

        // unsubscribe invalid topic
        let err = violation(&disp.call(unsubscribe(1, &[""])).await);
        assert_eq!(
            err.inner,
            error::ViolationInner::Spec(error::SpecViolation::Subs_4_7_1)
        );
    }

    #[ntex::test]
    async fn test_subscription_availability() {
        for available in [true, false] {
            let (_io, shared, disp) = ack_dispatcher(MqttServiceConfig::new());
            shared.codec.set_shared_subs_available(available);
            shared.codec.set_wildcard_subs_available(available);

            for (tf, reason, message) in [
                // (MQTT 5.0, 3.2.2.3.13)
                (
                    "$share/group/test",
                    DisconnectReasonCode::SharedSubscriptionNotSupported,
                    "Shared Subscriptions are not supported",
                ),
                (
                    "$share/group/a/+",
                    DisconnectReasonCode::SharedSubscriptionNotSupported,
                    "Shared Subscriptions are not supported",
                ),
                // (MQTT 5.0, 3.2.2.3.11)
                (
                    "a/+",
                    DisconnectReasonCode::WildcardSubscriptionsNotSupported,
                    "Wildcard Subscriptions are not supported",
                ),
                (
                    "a/#",
                    DisconnectReasonCode::WildcardSubscriptionsNotSupported,
                    "Wildcard Subscriptions are not supported",
                ),
            ] {
                let res = disp.call(subscribe(1, &["test", tf])).await;
                if available {
                    assert!(
                        matches!(res, Ok(Some(Packet::SubscribeAck(_)))),
                        "{tf}: {res:?}"
                    );
                } else {
                    let err = violation(&res);
                    assert_eq!(err.reason(), reason, "{tf}");
                    assert_eq!(err.message(), message, "{tf}");
                }

                // unsubscribe is not restricted
                let res = disp.call(unsubscribe(2, &[tf])).await;
                assert!(
                    matches!(res, Ok(Some(Packet::UnsubscribeAck(_)))),
                    "{tf}: {res:?}"
                );
            }

            // filters without wildcards are not restricted
            let res = disp.call(subscribe(1, &["test", "a/b"])).await;
            assert!(matches!(res, Ok(Some(Packet::SubscribeAck(_)))), "{res:?}");
        }
    }

    #[ntex::test]
    async fn test_subscription_availability_unchecked() {
        let (_io, shared, disp) =
            ack_dispatcher(MqttServiceConfig::new().set_check_subs_availability(false));
        shared.codec.set_shared_subs_available(false);
        shared.codec.set_wildcard_subs_available(false);

        for tf in ["$share/group/test", "a/+", "a/#"] {
            let res = disp.call(subscribe(1, &["test", tf])).await;
            assert!(
                matches!(res, Ok(Some(Packet::SubscribeAck(_)))),
                "{tf}: {res:?}"
            );
        }
    }

    #[ntex::test]
    async fn test_spec_violations_v5() {
        let (_io, _, disp) = ack_dispatcher(MqttServiceConfig::new());
        let violation = |res: &Result<_, _>| {
            let err = violation(res);
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
                    packet_id: pid(1),
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

        // [MQTT-3.3.2-14] Response Topic must not contain wildcards
        for topic in ["resp/+", "resp/#"] {
            let err = violation(&disp.call(publish(Some(topic), false)).await);
            assert_eq!(err, error::SpecViolation::Pub_3_3_2_14);
        }
        assert!(disp.call(publish(Some("resp/a"), false)).await.is_ok());

        // [MQTT-3.3.4-6] Client must not send a Subscription Identifier
        let err = violation(&disp.call(publish(None, true)).await);
        assert_eq!(err, error::SpecViolation::Pub_3_3_4_6);

        // [MQTT-4.8.2-1], [MQTT-4.8.2-2] ShareName format
        for filter in ["$share//a", "$share/g", "$share/+/a"] {
            let err = violation(&disp.call(subscribe(filter, false)).await);
            assert_eq!(err, error::SpecViolation::Subs_4_8_2);
            let err = violation(&disp.call(unsubscribe(2, &[filter])).await);
            assert_eq!(err, error::SpecViolation::Subs_4_8_2);
        }

        // [MQTT-3.8.3-4] No Local must not be set on a Shared Subscription
        let err = violation(&disp.call(subscribe("$share/g/a", true)).await);
        assert_eq!(err, error::SpecViolation::Subs_3_8_3_4);

        let res = disp.call(subscribe("$share/g/a", false)).await.unwrap();
        assert!(matches!(res, Some(Packet::SubscribeAck(_))));
        let res = disp.call(unsubscribe(2, &["$share/g/a"])).await.unwrap();
        assert!(matches!(res, Some(Packet::UnsubscribeAck(_))));
        let mut pkt = subscribe("a", true);
        if let Decoded::Packet(Packet::Subscribe(ref mut pkt), _) = pkt {
            pkt.packet_id = pid(3);
        }
        let res = disp.call(pkt).await.unwrap();
        assert!(matches!(res, Some(Packet::SubscribeAck(_))));
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

    #[test]
    fn test_inflight_kind() {
        use crate::inflight::SizedRequest;

        let publish = Decoded::Publish(codec::Publish::default(), Bytes::new(), 10);
        assert!(publish.is_limited() && !publish.is_ordered());
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"c"), true);
        assert!(!chunk.is_limited() && chunk.is_ordered());
        let disconnect = Decoded::Packet(Packet::Disconnect(Box::default()), 2);
        assert!(!disconnect.is_limited() && disconnect.is_ordered());
        let ping = Decoded::Packet(Packet::PingRequest, 2);
        assert!(!ping.is_limited() && !ping.is_ordered());
    }

    fn qos2_dispatcher(
        pubrel_calls: Rc<Cell<usize>>,
        published: Rc<Cell<usize>>,
        receive_max: u16,
    ) -> (Io, Disp) {
        let (io, shared, disp) = dispatcher(
            MqttServiceConfig::new(),
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
                async move {
                    if matches!(
                        msg,
                        ProtocolMessage::Subscribe(_) | ProtocolMessage::Unsubscribe(_)
                    ) {
                        sleep(Millis(100)).await;
                    }
                    Ok::<_, DispatcherError<TestError>>(msg.ack())
                }
            }),
        );
        shared.set_max_qos(QoS::ExactlyOnce);
        shared.set_receive_max(receive_max);
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
                packet_id: pid(id),
                ..Default::default()
            }),
            999,
        )
    }

    fn ack(id: u16, qos2: bool, reason_code: codec::PublishAckReason) -> Packet {
        let ack = codec::PublishAck {
            packet_id: pid(id),
            reason_code,
            ..Default::default()
        };
        if qos2 {
            Packet::PublishReceived(ack)
        } else {
            Packet::PublishAck(ack)
        }
    }

    fn pubcomp(id: u16, reason_code: codec::PublishAck2Reason) -> Packet {
        Packet::PublishComplete(codec::PublishAck2 {
            packet_id: pid(id),
            reason_code,
            ..Default::default()
        })
    }

    #[ntex::test]
    async fn test_publish_qos2() {
        use codec::{PublishAck2Reason as Ack2, PublishAckReason as Ack};

        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, disp) = qos2_dispatcher(pubrel_calls.clone(), Rc::default(), 1);
        let receive_max_exceeded = |res: Result<Option<Packet>, DispatcherError<TestError>>| {
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

    /// Receive Maximum counts `QoS 1` and `QoS 2` PUBLISH packets only [MQTT-4.9.0-2]
    #[ntex::test]
    async fn test_receive_max_subscribe() {
        let (_io, disp) = qos2_dispatcher(Rc::default(), Rc::default(), 1);
        let receive_max_exceeded = |res: Result<Option<Packet>, DispatcherError<TestError>>| {
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                    if err.inner == error::ViolationInner::Spec(SpecViolation::Pub_3_3_4_7)
            )
        };

        // SUBSCRIBE and UNSUBSCRIBE are processed by the control service
        let mut sub = Box::pin(disp.call(subscribe(1, &["a"])));
        let mut unsub = Box::pin(disp.call(unsubscribe(2, &["a"])));
        assert!(lazy(|cx| Pin::new(&mut sub).poll(cx)).await.is_pending());
        assert!(lazy(|cx| Pin::new(&mut unsub).poll(cx)).await.is_pending());

        let res = disp.call(qos_publish(3, QoS::AtLeastOnce, "test")).await;
        assert_eq!(
            res.unwrap(),
            Some(ack(3, false, codec::PublishAckReason::Success))
        );

        // PUBLISH packets are still limited
        let mut f = Box::pin(disp.call(qos_publish(4, QoS::AtLeastOnce, "slow")));
        assert!(lazy(|cx| Pin::new(&mut f).poll(cx)).await.is_pending());
        let res = disp.call(qos_publish(5, QoS::AtLeastOnce, "test")).await;
        assert!(receive_max_exceeded(res));

        assert!(matches!(sub.await.unwrap(), Some(Packet::SubscribeAck(_))));
        assert!(matches!(
            unsub.await.unwrap(),
            Some(Packet::UnsubscribeAck(_))
        ));
        assert_eq!(
            f.await.unwrap(),
            Some(ack(4, false, codec::PublishAckReason::Success))
        );
        let res = disp.call(qos_publish(5, QoS::AtLeastOnce, "test")).await;
        assert_eq!(
            res.unwrap(),
            Some(ack(5, false, codec::PublishAckReason::Success))
        );
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
        // a subsequent PUBLISH without DUP as well
        let res = disp.call(qos_publish(2, QoS::ExactlyOnce, "test")).await;
        assert_eq!(res.unwrap(), Some(ack(2, true, Ack::Success)));
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

    #[ntex::test]
    async fn test_inactive_publish_payload() {
        let published = Rc::new(Cell::new(0));
        let (io, disp) = qos2_dispatcher(Rc::default(), published.clone(), 16);
        let chunk = |data: &'static [u8], eof| {
            disp.call(Decoded::PayloadChunk(Bytes::from_static(data), eof))
        };
        io.close();
        assert!(!io.is_active());

        // payload of a publish dropped after disconnect is dropped
        let res = disp.call(redelivery(1, QoS::AtLeastOnce, true)).await;
        assert_eq!(res.unwrap(), None);
        assert_eq!(chunk(b"d", false).await.unwrap(), None);
        assert_eq!(chunk(b"ef", true).await.unwrap(), None);
        assert_eq!(published.get(), 0);

        // the last chunk ends the dropped payload
        assert!(matches!(
            chunk(b"g", true).await,
            Err(DispatcherError::Protocol(MqttProtocolError::Decode(
                DecodeError::UnexpectedPayload
            )))
        ));
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
        let pid = pid(1);
        let (_io, disp) = qos2_dispatcher(Rc::default(), Rc::default(), 1);

        // a second CONNECT is a protocol error [MQTT-3.1.0-2], packets sent
        // by the server only are not expected from client
        for (pkt, tp) in [
            (Packet::Connect(Box::default()), packet_type::CONNECT),
            (Packet::ConnectAck(Box::default()), packet_type::CONNACK),
            (
                Packet::SubscribeAck(codec::SubscribeAck {
                    packet_id: pid,
                    properties: Vec::default(),
                    reason_string: None,
                    status: vec![],
                }),
                packet_type::SUBACK,
            ),
            (
                Packet::UnsubscribeAck(codec::UnsubscribeAck {
                    packet_id: pid,
                    properties: Vec::default(),
                    reason_string: None,
                    status: vec![],
                }),
                packet_type::UNSUBACK,
            ),
            (Packet::PingResponse, packet_type::PINGRESP),
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
        let held = Rc::new(RefCell::new(None));
        let (io, _, disp) = dispatcher(
            MqttServiceConfig::new().set_max_payload_buffer_size(4),
            Hold(
                held.clone(),
                pfail.clone(),
                || TestError,
                || PublishAck::new(codec::PublishAckReason::Success),
            ),
            FailReady(fail.clone(), || {
                DispatcherError::Protocol(MqttProtocolError::ReadTimeout)
            }),
        );
        let chunk = |data: &'static [u8]| {
            disp.call_nowait(Decoded::PayloadChunk(Bytes::from_static(data), false))
        };

        // streamed payload below the high watermark
        let res = disp
            .call(Decoded::Publish(
                codec::Publish {
                    topic: ByteString::from_static("t"),
                    payload_size: 16,
                    ..Default::default()
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
