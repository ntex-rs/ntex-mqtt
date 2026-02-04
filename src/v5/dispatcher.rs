use std::{cell::Cell, cell::RefCell, marker, num, rc::Rc, task::Context};

use ntex_bytes::ByteString;
use ntex_dispatcher::{Control as DispControl, DispatchItem, Reason};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{self as service, Pipeline, Service, ServiceCtx, ServiceFactory};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::services::inflight::InFlightService;
use ntex_util::{HashMap, HashSet, future::join};

use crate::error::{DecodeError, HandshakeError, MqttError, PayloadError, ProtocolError};
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::{MqttServiceConfig, types::QoS};

use super::codec::{self, Decoded, DisconnectReasonCode, Encoded, Packet};
use super::control::{Control, ControlAck, Pkt};
use super::publish::{Publish, PublishAck};
use super::{Session, shared::Ack, shared::MqttShared};

/// MQTT 5 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
) -> impl ServiceFactory<
    DispatchItem<Rc<MqttShared>>,
    (SharedCfg, Session<St>),
    Response = Option<Encoded>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    St: 'static,
    E: From<T::Error> + From<T::InitError> + From<C::Error> + From<C::InitError> + 'static,
    T: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
    C: ServiceFactory<Control<E>, Session<St>, Response = ControlAck> + 'static,
    PublishAck: TryFrom<T::Error, Error = E>,
{
    let factories = Rc::new((publish, control));

    service::fn_factory_with_config(async move |(cfg, ses): (SharedCfg, Session<St>)| {
        let cfg: Cfg<MqttServiceConfig> = cfg.get();

        // create services
        let sink = ses.sink().shared();
        let (publish, control) =
            join(factories.0.create(ses.clone()), factories.1.create(ses)).await;

        let publish = publish.map_err(|e| MqttError::Service(e.into()))?;
        let control_unbuf = Pipeline::new(
            control
                .map_err(|e| MqttError::Service(e.into()))?
                .map_err(|e| MqttError::Service(E::from(e))),
        );

        let control = Pipeline::new(
            BufferService::new(
                16,
                // limit number of in-flight messages
                InFlightService::new(1, service::PipelineSvc::new(control_unbuf.clone())),
            )
            .map_err(|err| match err {
                BufferServiceError::Service(e) => e,
                BufferServiceError::RequestCanceled => {
                    MqttError::Handshake(HandshakeError::Disconnected(None))
                }
            }),
        );

        Ok(Dispatcher::<_, _, _, E>::new(sink, publish, control, control_unbuf, cfg))
    })
}

impl crate::inflight::SizedRequest for DispatchItem<Rc<MqttShared>> {
    fn size(&self) -> u32 {
        if let DispatchItem::Item(Decoded::Packet(_, size) | Decoded::Publish(_, _, size)) =
            self
        {
            *size
        } else {
            0
        }
    }

    fn is_publish(&self) -> bool {
        matches!(self, DispatchItem::Item(Decoded::Publish(..)))
    }

    fn is_chunk(&self) -> bool {
        matches!(self, DispatchItem::Item(Decoded::PayloadChunk(..)))
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<Control<E>>, C2: Service<Control<E>>, E> {
    publish: T,
    inner: Rc<Inner<C, C2>>,
    cfg: Cfg<MqttServiceConfig>,
    _t: marker::PhantomData<E>,
}

struct Inner<C, C2> {
    control: Pipeline<C>,
    control_unbuf: Pipeline<C2>,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
    payload: Cell<Option<PlSender>>,
}

struct PublishInfo {
    inflight: HashSet<num::NonZeroU16>,
    aliases: HashMap<num::NonZeroU16, ByteString>,
}

impl<T, C, C2, E> Dispatcher<T, C, C2, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: Pipeline<C>,
        control_unbuf: Pipeline<C2>,
        cfg: Cfg<MqttServiceConfig>,
    ) -> Self {
        Self {
            cfg,
            publish,
            inner: Rc::new(Inner {
                sink,
                control,
                control_unbuf,
                payload: Cell::new(None),
                info: RefCell::new(PublishInfo {
                    aliases: HashMap::default(),
                    inflight: HashSet::default(),
                }),
            }),
            _t: marker::PhantomData,
        }
    }

    fn tag(&self) -> &'static str {
        self.inner.sink.tag()
    }
}

impl<C, C2> Inner<C, C2> {
    fn drop_payload<PErr>(&self, err: &PErr)
    where
        PErr: Clone,
        PayloadError: From<PErr>,
    {
        if let Some(pl) = self.payload.take() {
            pl.set_error(err.clone().into());
        }
    }
}

impl<T, C, C2, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, C2, E>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = PublishAck> + 'static,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
{
    type Response = Option<Encoded>;
    type Error = MqttError<E>;

    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
        let result = if let Err(e) = res1 {
            if res2.is_err() {
                Err(MqttError::Service(e.into()))
            } else if !self.inner.sink.is_dispatcher_stopped() {
                match self.inner.control_unbuf.call(Control::error(e.into())).await {
                    Ok(res) => {
                        if res.disconnect {
                            self.inner.sink.drop_sink();
                        }
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            } else {
                res2
            }
        } else {
            res2
        };

        if result.is_ok()
            && let Some(pl) = self.inner.payload.take()
        {
            self.inner.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }
        result
    }

    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        if let Err(e) = self.publish.poll(cx)
            && !self.inner.sink.is_dispatcher_stopped()
        {
            let inner = self.inner.clone();
            ntex_rt::spawn(async move {
                if let Ok(res) = inner.control_unbuf.call(Control::error(e.into())).await
                    && res.disconnect
                {
                    inner.sink.drop_sink();
                }
            });
        }
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        log::trace!("{}: Shutdown v5 dispatcher", self.tag());
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.drop_sink();
        let _ = self.inner.control.call(Control::shutdown()).await;

        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::too_many_lines, clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        request: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("{}: Dispatch v5 packet: {:#?}", self.tag(), request);

        match request {
            DispatchItem::Item(Decoded::Publish(mut publish, payload, size)) => {
                let info = self.inner.as_ref();
                let packet_id = publish.packet_id;

                if publish.topic.contains(['#', '+']) {
                    return control(
                        Control::proto_error(
                            ProtocolError::generic_violation(
                                "PUBLISH packet's topic name contains wildcard character [MQTT-3.3.2-2]"
                            )
                        ),
                        &self.inner,
                        0,
                    ).await;
                }

                {
                    let mut inner = info.info.borrow_mut();
                    let state = &self.inner.sink;

                    if let Some(pid) = packet_id {
                        // check for receive maximum
                        let receive_max = state.receive_max();
                        if receive_max != 0 && inner.inflight.len() >= receive_max as usize {
                            log::trace!(
                                "{}: Receive maximum exceeded: max: {} in-flight: {}",
                                self.tag(),
                                receive_max,
                                inner.inflight.len()
                            );
                            drop(inner);
                            return control(
                                Control::proto_error(
                                    ProtocolError::violation(
                                        DisconnectReasonCode::ReceiveMaximumExceeded,
                                        "Number of in-flight messages exceeds set maximum [MQTT-3.3.4-7]"
                                    )
                                ),
                                &self.inner,
                                0,
                            ).await;
                        }

                        // check max allowed qos
                        if publish.qos > state.max_qos() {
                            log::trace!(
                                "{}: Max allowed QoS is violated, max {:?} provided {:?}",
                                self.tag(),
                                state.max_qos(),
                                publish.qos
                            );
                            drop(inner);
                            return control(
                                Control::proto_error(ProtocolError::violation(
                                    DisconnectReasonCode::QosNotSupported,
                                    "PUBLISH QoS is higher than supported [MQTT-3.2.2-11]",
                                )),
                                &self.inner,
                                0,
                            )
                            .await;
                        }
                        if publish.retain && !state.codec.retain_available() {
                            log::trace!("{}: Retain is not available but is set", self.tag());
                            drop(inner);
                            return control(
                                Control::proto_error(ProtocolError::violation(
                                    DisconnectReasonCode::RetainNotSupported,
                                    "RETAIN is not supported [MQTT-3.2.2-14]",
                                )),
                                &self.inner,
                                0,
                            )
                            .await;
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            let _ = self.inner.sink.encode_packet(codec::Packet::PublishAck(
                                codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                },
                            ));
                            return Ok(None);
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        if publish.topic.is_empty() {
                            // lookup topic by provided alias
                            if let Some(aliased_topic) = inner.aliases.get(&alias) {
                                publish.topic = aliased_topic.clone();
                            } else {
                                drop(inner);
                                return control(
                                    Control::proto_error(ProtocolError::violation(
                                        DisconnectReasonCode::TopicAliasInvalid,
                                        "Unknown topic alias",
                                    )),
                                    &self.inner,
                                    0,
                                )
                                .await;
                            }
                        } else {
                            // record new alias
                            match inner.aliases.entry(alias) {
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    if entry.get().as_str() != publish.topic.as_str() {
                                        let mut topic = publish.topic.clone();
                                        topic.trimdown();
                                        entry.insert(topic);
                                    }
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    if alias.get() > state.topic_alias_max() {
                                        drop(inner);
                                        return control(
                                                Control::proto_error(
                                                    ProtocolError::generic_violation(
                                                        "Topic alias is greater than max allowed [MQTT-3.2.2-17]",
                                                    )
                                                ),
                                                &self.inner,
                                            0,
                                            ).await;
                                    }
                                    let mut topic = publish.topic.clone();
                                    topic.trimdown();
                                    entry.insert(topic);
                                }
                            }
                        }
                    }

                    if state.is_closed()
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
                    self.inner.payload.set(Some(sender));
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
            DispatchItem::Item(Decoded::PayloadChunk(buf, eof)) => {
                if let Some(pl) = self.inner.payload.take() {
                    pl.feed_data(buf);
                    if eof {
                        pl.feed_eof();
                    } else {
                        self.inner.payload.set(Some(pl));
                    }
                    Ok(None)
                } else {
                    control(
                        Control::proto_error(ProtocolError::Decode(
                            DecodeError::UnexpectedPayload,
                        )),
                        &self.inner,
                        0,
                    )
                    .await
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishAck(packet), _)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Publish(packet)) {
                    control(Control::proto_error(err), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishReceived(pkt), _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(pkt)) {
                    control(Control::proto_error(e), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishRelease(ack), size)) => {
                if self.inner.info.borrow().inflight.contains(&ack.packet_id) {
                    control(Control::pubrel(ack, size), &self.inner, 0).await
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
            DispatchItem::Item(Decoded::Packet(Packet::PublishComplete(pkt), _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(pkt)) {
                    control(Control::proto_error(e), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::Auth(pkt), size)) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                control(Control::auth(pkt, size), &self.inner, 0).await
            }
            DispatchItem::Item(Decoded::Packet(Packet::PingRequest, _)) => {
                control(Control::ping(), &self.inner, 0).await
            }
            DispatchItem::Item(Decoded::Packet(Packet::Disconnect(pkt), size)) => {
                control(Control::remote_disconnect(pkt, size), &self.inner, 0).await
            }
            DispatchItem::Item(Decoded::Packet(Packet::Subscribe(pkt), size)) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if pkt.topic_filters.iter().any(|(tf, _)| !crate::topic::is_valid(tf)) {
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            "Topic filter is malformed [MQTT-4.7.1-*]",
                        )),
                        &self.inner,
                        0,
                    )
                    .await;
                }

                if pkt.id.is_some() && !self.inner.sink.codec.sub_ids_available() {
                    log::trace!(
                        "{}: Subscription Identifiers are not supported but was set",
                        self.tag()
                    );
                    return control(
                        Control::proto_error(ProtocolError::violation(
                            DisconnectReasonCode::SubscriptionIdentifiersNotSupported,
                            "Subscription Identifiers are not supported",
                        )),
                        &self.inner,
                        0,
                    )
                    .await;
                }

                // register inflight packet id
                if !self.inner.info.borrow_mut().inflight.insert(pkt.packet_id) {
                    // duplicated packet id
                    let _ = self.inner.sink.encode_packet(codec::Packet::SubscribeAck(
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
                    ));
                    return Ok(None);
                }
                let id = pkt.packet_id;
                control(Control::subscribe(pkt, size), &self.inner, id.get()).await
            }
            DispatchItem::Item(Decoded::Packet(Packet::Unsubscribe(pkt), size)) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if pkt.topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            "Topic filter is malformed [MQTT-4.7.1-*]",
                        )),
                        &self.inner,
                        0,
                    )
                    .await;
                }

                // register inflight packet id
                if !self.inner.info.borrow_mut().inflight.insert(pkt.packet_id) {
                    // duplicated packet id
                    let _ = self.inner.sink.encode_packet(codec::Packet::UnsubscribeAck(
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
                    ));
                    return Ok(None);
                }
                let id = pkt.packet_id;
                control(Control::unsubscribe(pkt, size), &self.inner, id.get()).await
            }
            DispatchItem::Item(Decoded::Packet(_, _)) => Ok(None),
            DispatchItem::Stop(Reason::Encoder(err)) => {
                let err = ProtocolError::Encode(err);
                self.inner.drop_payload(&err);
                control(Control::proto_error(err), &self.inner, 0).await
            }
            DispatchItem::Stop(Reason::KeepAliveTimeout) => {
                self.inner.drop_payload(&ProtocolError::KeepAliveTimeout);
                control(Control::proto_error(ProtocolError::KeepAliveTimeout), &self.inner, 0)
                    .await
            }
            DispatchItem::Stop(Reason::ReadTimeout) => {
                self.inner.drop_payload(&ProtocolError::ReadTimeout);
                control(Control::proto_error(ProtocolError::ReadTimeout), &self.inner, 0).await
            }
            DispatchItem::Stop(Reason::Decoder(err)) => {
                let err = ProtocolError::Decode(err);
                self.inner.drop_payload(&err);
                control(Control::proto_error(err), &self.inner, 0).await
            }
            DispatchItem::Stop(Reason::Io(err)) => {
                self.inner.drop_payload(&PayloadError::Disconnected);
                control(Control::peer_gone(err), &self.inner, 0).await
            }
            DispatchItem::Control(DispControl::WBackPressureEnabled) => {
                self.inner.sink.enable_wr_backpressure();
                control(Control::wr_backpressure(true), &self.inner, 0).await
            }
            DispatchItem::Control(DispControl::WBackPressureDisabled) => {
                self.inner.sink.disable_wr_backpressure();
                control(Control::wr_backpressure(false), &self.inner, 0).await
            }
        }
    }
}

/// Publish service response future
async fn publish_fn<'f, T, C, C2, E>(
    publish: &T,
    pkt: Publish,
    packet_id: u16,
    inner: &'f Inner<C, C2>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, C2, E>>,
) -> Result<Option<Encoded>, MqttError<E>>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let qos2 = pkt.qos() == QoS::ExactlyOnce;
    let ack = match ctx.call(publish, pkt).await {
        Ok(ack) => ack,
        Err(e) => {
            if packet_id != 0 {
                match PublishAck::try_from(e) {
                    Ok(ack) => ack,
                    Err(e) => return control(Control::error(e), inner, 0).await,
                }
            } else {
                return control(Control::error(e.into()), inner, 0).await;
            }
        }
    };
    if let Some(id) = num::NonZeroU16::new(packet_id) {
        let ack = if qos2 {
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

async fn control<C, C2, E>(
    pkt: Control<E>,
    inner: &Inner<C, C2>,
    packet_id: u16,
) -> Result<Option<Encoded>, MqttError<E>>
where
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let mut stop = matches!(pkt, Control::Stop(_));

    let result = if pkt.is_protocol() {
        inner.control.call(pkt).await
    } else {
        if stop && inner.sink.is_dispatcher_stopped() {
            inner.drop_payload(&PayloadError::Service);
            inner.sink.drop_sink();
            return Ok(None);
        }
        inner.control_unbuf.call(pkt).await
    };

    let result = match result {
        Ok(result) => {
            if let Some(id) = num::NonZeroU16::new(packet_id) {
                inner.info.borrow_mut().inflight.remove(&id);
            }
            result
        }
        Err(err) => {
            inner.drop_payload(&PayloadError::Service);

            // do not handle nested stop
            if stop {
                inner.sink.drop_sink();
                return Err(err);
            }
            // handle error from control service
            if inner.sink.is_dispatcher_stopped() {
                inner.sink.drop_sink();
                return Err(err);
            }
            if let MqttError::Service(err) = err {
                stop = true;
                inner
                    .control_unbuf
                    .call(Control::error(err))
                    .await
                    .inspect_err(|_| inner.sink.drop_sink())?
            } else {
                inner.sink.drop_sink();
                return Err(err);
            }
        }
    };

    let response = if stop {
        match result.packet {
            Pkt::Packet(pkt) => {
                let _ = inner.sink.encode_packet(pkt);
            }
            Pkt::Disconnect(pkt) => {
                if !inner.sink.is_disconnect_sent() {
                    let _ = inner.sink.encode_packet(pkt.into());
                }
            }
            Pkt::None => {}
        }
        Ok(None)
    } else {
        match result.packet {
            Pkt::Packet(pkt) => Ok(Some(Encoded::Packet(pkt))),
            Pkt::Disconnect(pkt) => {
                if inner.sink.is_disconnect_sent() {
                    Ok(None)
                } else {
                    Ok(Some(Encoded::Packet(codec::Packet::from(pkt))))
                }
            }
            Pkt::None => Ok(None),
        }
    };

    if result.disconnect {
        inner.drop_payload(&PayloadError::Service);
        inner.sink.drop_sink();
    }
    response
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use ntex_bytes::Bytes;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::fn_service;
    use ntex_util::future::{Ready, lazy};

    use super::*;
    use crate::v5::MqttSink;

    #[derive(Debug)]
    struct TestError;

    impl TryFrom<TestError> for PublishAck {
        type Error = TestError;

        fn try_from(err: TestError) -> Result<Self, Self::Error> {
            Err(err)
        }
    }

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Rc::default()));
        let control = Pipeline::new(fn_service(|_| {
            Ready::Ok::<_, MqttError<TestError>>(ControlAck {
                packet: Pkt::None,
                disconnect: false,
            })
        }));

        let disp = Pipeline::new(Dispatcher::<_, _, _, _>::new(
            shared.clone(),
            fn_service(|p: Publish| Ready::Ok::<_, TestError>(p.ack())),
            control.clone(),
            control,
            Cfg::default(),
        ));

        let sink = MqttSink::new(shared.clone());
        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        disp.call(DispatchItem::Control(DispControl::WBackPressureEnabled)).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        disp.call(DispatchItem::Control(DispControl::WBackPressureDisabled)).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }

    #[ntex::test]
    async fn control_stop_once_on_service_error() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Rc::default()));
        let counter = Rc::new(Cell::new(0));
        let counter2 = counter.clone();
        let control = Pipeline::new(fn_service(async move |msg| {
            if matches!(msg, Control::Stop(_)) {
                counter2.set(counter2.get() + 1);
            }
            Ok::<_, MqttError<TestError>>(ControlAck { packet: Pkt::None, disconnect: false })
        }));

        let disp = Pipeline::new(Dispatcher::<_, _, _, _>::new(
            shared.clone(),
            fn_service(async |_: Publish| Err(TestError)),
            control.clone(),
            control,
            Cfg::default(),
        ));

        disp.call(DispatchItem::Item(Decoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtLeastOnce,
                topic: ByteString::from("test"),
                packet_id: Some(NonZeroU16::new(1).unwrap()),
                payload_size: 0,
                properties: codec::PublishProperties::default(),
            },
            Bytes::new(),
            0,
        )))
        .await
        .unwrap();

        disp.call(DispatchItem::Stop(Reason::Io(None))).await.unwrap();

        assert_eq!(counter.get(), 1);
    }
}
