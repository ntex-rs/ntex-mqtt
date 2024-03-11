use std::{cell::RefCell, marker, num, rc::Rc, task::Context, task::Poll};

use ntex::io::DispatchItem;
use ntex::util::inflight::InFlightService;
use ntex::util::{buffer::BufferService, buffer::BufferServiceError};
use ntex::util::{join, BoxFuture, ByteString, HashMap, HashSet};
use ntex::{service, Pipeline, Service, ServiceCtx, ServiceFactory};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::types::QoS;

use super::control::{ControlMessage, ControlResult};
use super::publish::{Publish, PublishAck};
use super::shared::{Ack, MqttShared};
use super::{codec, codec::DisconnectReasonCode, Session};

/// MQTT 5 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    max_inflight_size: usize,
    handle_qos_after_disconnect: Option<QoS>,
) -> impl ServiceFactory<
    DispatchItem<Rc<MqttShared>>,
    Session<St>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    St: 'static,
    E: From<T::Error> + From<T::InitError> + From<C::Error> + From<C::InitError> + 'static,
    T: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
    C: ServiceFactory<ControlMessage<E>, Session<St>, Response = ControlResult> + 'static,
    PublishAck: TryFrom<T::Error, Error = E>,
{
    let factories = Rc::new((publish, control));

    service::fn_factory_with_config(move |ses: Session<St>| {
        let factories = factories.clone();

        async move {
            // create services
            let sink = ses.sink().shared();
            let (publish, control) =
                join(factories.0.create(ses.clone()), factories.1.create(ses)).await;

            let publish = publish.map_err(|e| MqttError::Service(e.into()))?;
            let control = control.map_err(|e| MqttError::Service(e.into()))?;

            let control = BufferService::new(
                16,
                // limit number of in-flight messages
                InFlightService::new(1, control),
            )
            .map_err(|err| match err {
                BufferServiceError::Service(e) => MqttError::Service(E::from(e)),
                BufferServiceError::RequestCanceled => {
                    MqttError::Handshake(HandshakeError::Disconnected(None))
                }
            });

            Ok(crate::inflight::InFlightService::new(
                0,
                max_inflight_size,
                Dispatcher::<_, _, E>::new(sink, publish, control, handle_qos_after_disconnect),
            ))
        }
    })
}

impl crate::inflight::SizedRequest for DispatchItem<Rc<MqttShared>> {
    fn size(&self) -> u32 {
        if let DispatchItem::Item((_, size)) = self {
            *size
        } else {
            0
        }
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<ControlMessage<E>>, E> {
    publish: T,
    handle_qos_after_disconnect: Option<QoS>,
    shutdown: RefCell<Option<BoxFuture<'static, ()>>>,
    inner: Rc<Inner<C>>,
    _t: marker::PhantomData<E>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<num::NonZeroU16>,
    aliases: HashMap<num::NonZeroU16, ByteString>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        handle_qos_after_disconnect: Option<QoS>,
    ) -> Self {
        Self {
            publish,
            handle_qos_after_disconnect,
            shutdown: RefCell::new(None),
            inner: Rc::new(Inner {
                sink,
                control,
                info: RefCell::new(PublishInfo {
                    aliases: HashMap::default(),
                    inflight: HashSet::default(),
                }),
            }),
            _t: marker::PhantomData,
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>> + 'static,
{
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(|e| MqttError::Service(e.into()))?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            self.inner.sink.drop_sink();
            let inner = self.inner.clone();
            *shutdown = Some(Box::pin(async move {
                let _ = Pipeline::new(&inner.control).call(ControlMessage::closed()).await;
            }));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.publish.poll_shutdown(cx);
        let res2 = self.inner.control.poll_shutdown(cx);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        request: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Dispatch v5 packet: {:#?}", request);

        match request {
            DispatchItem::Item((codec::Packet::Publish(mut publish), size)) => {
                let info = self.inner.as_ref();
                let packet_id = publish.packet_id;

                if publish.topic.contains(['#', '+']) {
                    return control(
                        ControlMessage::proto_error(
                            ProtocolError::generic_violation(
                                "PUBLISH packet's topic name contains wildcard character [MQTT-3.3.2-2]"
                            )
                        ),
                        &self.inner,
                        ctx,
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
                                "Receive maximum exceeded: max: {} in-flight: {}",
                                receive_max,
                                inner.inflight.len()
                            );
                            drop(inner);
                            return control(
                                ControlMessage::proto_error(
                                    ProtocolError::violation(
                                        DisconnectReasonCode::ReceiveMaximumExceeded,
                                        "Number of in-flight messages exceeds set maximum [MQTT-3.3.4-7]"
                                    )
                                ),
                                &self.inner,
                                ctx,
                                0,
                            ).await;
                        }

                        // check max allowed qos
                        if publish.qos > state.max_qos() {
                            log::trace!(
                                "Max allowed QoS is violated, max {:?} provided {:?}",
                                state.max_qos(),
                                publish.qos
                            );
                            drop(inner);
                            return control(
                                ControlMessage::proto_error(ProtocolError::violation(
                                    DisconnectReasonCode::QosNotSupported,
                                    "PUBLISH QoS is higher than supported [MQTT-3.2.2-11]",
                                )),
                                &self.inner,
                                ctx,
                                0,
                            )
                            .await;
                        }
                        if publish.retain && !state.codec.retain_available() {
                            log::trace!("Retain is not available but is set");
                            drop(inner);
                            return control(
                                ControlMessage::proto_error(ProtocolError::violation(
                                    DisconnectReasonCode::RetainNotSupported,
                                    "RETAIN is not supported [MQTT-3.2.2-14]",
                                )),
                                &self.inner,
                                ctx,
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
                            match inner.aliases.get(&alias) {
                                Some(aliased_topic) => publish.topic = aliased_topic.clone(),
                                None => {
                                    drop(inner);
                                    return control(
                                        ControlMessage::proto_error(ProtocolError::violation(
                                            DisconnectReasonCode::TopicAliasInvalid,
                                            "Unknown topic alias",
                                        )),
                                        &self.inner,
                                        ctx,
                                        0,
                                    )
                                    .await;
                                }
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
                                                ControlMessage::proto_error(
                                                    ProtocolError::generic_violation(
                                                        "Topic alias is greater than max allowed [MQTT-3.2.2-17]",
                                                    )
                                                ),
                                                &self.inner,
                                            ctx,
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
                        && !self
                            .handle_qos_after_disconnect
                            .map(|max_qos| publish.qos <= max_qos)
                            .unwrap_or_default()
                    {
                        return Ok(None);
                    }
                }

                publish_fn(
                    &self.publish,
                    Publish::new(publish, size),
                    packet_id.map(|v| v.get()).unwrap_or(0),
                    info,
                    ctx,
                )
                .await
            }
            DispatchItem::Item((codec::Packet::PublishAck(packet), _)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Publish(packet)) {
                    control(ControlMessage::proto_error(err), &self.inner, ctx, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item((codec::Packet::Auth(pkt), size)) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                control(ControlMessage::auth(pkt, size), &self.inner, ctx, 0).await
            }
            DispatchItem::Item((codec::Packet::PingRequest, _)) => {
                control(ControlMessage::ping(), &self.inner, ctx, 0).await
            }
            DispatchItem::Item((codec::Packet::Disconnect(pkt), size)) => {
                control(ControlMessage::remote_disconnect(pkt, size), &self.inner, ctx, 0).await
            }
            DispatchItem::Item((codec::Packet::Subscribe(pkt), size)) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if pkt.topic_filters.iter().any(|(tf, _)| !crate::topic::is_valid(tf)) {
                    return control(
                        ControlMessage::proto_error(ProtocolError::generic_violation(
                            "Topic filter is malformed [MQTT-4.7.1-*]",
                        )),
                        &self.inner,
                        ctx,
                        0,
                    )
                    .await;
                }

                if pkt.id.is_some() && !self.inner.sink.codec.sub_ids_available() {
                    log::trace!("Subscription Identifiers are not supported but was set");
                    return control(
                        ControlMessage::proto_error(ProtocolError::violation(
                            DisconnectReasonCode::SubscriptionIdentifiersNotSupported,
                            "Subscription Identifiers are not supported",
                        )),
                        &self.inner,
                        ctx,
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
                control(ControlMessage::subscribe(pkt, size), &self.inner, ctx, id.get()).await
            }
            DispatchItem::Item((codec::Packet::Unsubscribe(pkt), size)) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if pkt.topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    return control(
                        ControlMessage::proto_error(ProtocolError::generic_violation(
                            "Topic filter is malformed [MQTT-4.7.1-*]",
                        )),
                        &self.inner,
                        ctx,
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
                control(ControlMessage::unsubscribe(pkt, size), &self.inner, ctx, id.get())
                    .await
            }
            DispatchItem::Item((_, _)) => Ok(None),
            DispatchItem::EncoderError(err) => {
                control(
                    ControlMessage::proto_error(ProtocolError::Encode(err)),
                    &self.inner,
                    ctx,
                    0,
                )
                .await
            }
            DispatchItem::KeepAliveTimeout => {
                control(
                    ControlMessage::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                    ctx,
                    0,
                )
                .await
            }
            DispatchItem::ReadTimeout => {
                control(
                    ControlMessage::proto_error(ProtocolError::ReadTimeout),
                    &self.inner,
                    ctx,
                    0,
                )
                .await
            }
            DispatchItem::DecoderError(err) => {
                control(
                    ControlMessage::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                    ctx,
                    0,
                )
                .await
            }
            DispatchItem::Disconnect(err) => {
                control(ControlMessage::peer_gone(err), &self.inner, ctx, 0).await
            }
            DispatchItem::WBackPressureEnabled => {
                self.inner.sink.enable_wr_backpressure();
                Ok(None)
            }
            DispatchItem::WBackPressureDisabled => {
                self.inner.sink.disable_wr_backpressure();
                Ok(None)
            }
        }
    }
}

/// Publish service response future
async fn publish_fn<'f, T, C, E>(
    publish: &T,
    pkt: Publish,
    packet_id: u16,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    let ack = match ctx.call(publish, pkt).await {
        Ok(ack) => ack,
        Err(e) => {
            if packet_id != 0 {
                match PublishAck::try_from(e) {
                    Ok(ack) => ack,
                    Err(e) => return control(ControlMessage::error(e), inner, ctx, 0).await,
                }
            } else {
                return control(ControlMessage::error(e.into()), inner, ctx, 0).await;
            }
        }
    };
    if let Some(id) = num::NonZeroU16::new(packet_id) {
        inner.info.borrow_mut().inflight.remove(&id);
        let ack = codec::PublishAck {
            packet_id: id,
            reason_code: ack.reason_code,
            reason_string: ack.reason_string,
            properties: ack.properties,
        };
        Ok(Some(codec::Packet::PublishAck(ack)))
    } else {
        Ok(None)
    }
}

async fn control<'f, T, C, E>(
    pkt: ControlMessage<E>,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
    packet_id: u16,
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    let mut error = matches!(pkt, ControlMessage::Error(_) | ControlMessage::ProtocolError(_));

    let result = match ctx.call(&inner.control, pkt).await {
        Ok(result) => {
            if let Some(id) = num::NonZeroU16::new(packet_id) {
                inner.info.borrow_mut().inflight.remove(&id);
            }
            result
        }
        Err(err) => {
            // do not handle nested error
            if error {
                return Err(err);
            } else {
                // handle error from control service
                match err {
                    MqttError::Service(err) => {
                        error = true;
                        ctx.call(&inner.control, ControlMessage::error(err)).await?
                    }
                    _ => return Err(err),
                }
            }
        }
    };

    if error {
        if let Some(pkt) = result.packet {
            let _ = inner.sink.encode_packet(pkt);
        }
        if result.disconnect {
            inner.sink.drop_sink();
        }
        Ok(None)
    } else {
        if result.disconnect {
            inner.sink.drop_sink();
        }
        Ok(result.packet)
    }
}

#[cfg(test)]
mod tests {
    use ntex::{io::Io, service::fn_service, testing::IoTest, util::lazy, util::Ready};

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
        let io = Io::new(IoTest::create().0);
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Default::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, _>::new(
            shared.clone(),
            fn_service(|p: Publish| Ready::Ok::<_, TestError>(p.ack())),
            fn_service(|_| {
                Ready::Ok::<_, MqttError<TestError>>(ControlResult {
                    packet: None,
                    disconnect: false,
                })
            }),
            None,
        ));

        let sink = MqttSink::new(shared.clone());
        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        disp.call(DispatchItem::WBackPressureEnabled).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        disp.call(DispatchItem::WBackPressureDisabled).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }
}
