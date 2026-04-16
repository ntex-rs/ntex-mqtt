use std::{cell::RefCell, marker::PhantomData, num, rc::Rc, task::Context};

use ntex_bytes::ByteString;
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{self as service, Pipeline, Service, ServiceCtx, ServiceFactory};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::services::inflight::InFlightService;
use ntex_util::{HashMap, HashSet, future::join};

use crate::control::{Control, Reason};
use crate::error::{DecodeError, DispatcherError, PayloadError, ProtocolError, SpecViolation};
use crate::payload::{Payload, PayloadStatus};
use crate::{MqttServiceConfig, types::QoS};

use super::codec::{self, Decoded, DisconnectReasonCode, Encoded, Packet};
use super::control::{Pkt, ProtocolMessage, ProtocolMessageAck};
use super::publish::{Publish, PublishAck};
use super::{Session, ToPublishAck, shared::Ack, shared::MqttShared};

/// MQTT 5 protocol dispatcher
pub(super) fn factory<St, T, P, E>(
    publish: T,
    control: P,
) -> impl ServiceFactory<
    Decoded,
    (SharedCfg, Session<St>),
    Response = Option<Encoded>,
    Error = DispatcherError<E>,
    InitError = T::InitError,
>
where
    St: 'static,
    E: From<P::Error> + 'static,
    T: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
    T::Error: ToPublishAck<Error = E>,
    T::InitError: From<P::InitError>,
    P: ServiceFactory<ProtocolMessage, Session<St>, Response = ProtocolMessageAck> + 'static,
{
    let factories = Rc::new((publish, control));

    service::fn_factory_with_config(async move |(cfg, ses): (SharedCfg, Session<St>)| {
        let cfg: Cfg<MqttServiceConfig> = cfg.get();

        // create services
        let sink = ses.sink().shared();
        let (publish, control) =
            join(factories.0.create(ses.clone()), factories.1.create(ses)).await;

        let publish = publish?;
        let control = control
            .map_err(<T::InitError>::from)?
            .map_err(|e| DispatcherError::Service(e.into()));

        let control = Pipeline::new(
            BufferService::new(
                16,
                // limit number of in-flight messages
                InFlightService::new(1, control),
            )
            .map_err(|err| match err {
                BufferServiceError::Service(e) => e,
                BufferServiceError::RequestCanceled => {
                    DispatcherError::Protocol(ProtocolError::ReadTimeout)
                }
            }),
        );

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

    fn is_publish(&self) -> bool {
        matches!(self, Decoded::Publish(..))
    }

    fn is_chunk(&self) -> bool {
        matches!(self, Decoded::PayloadChunk(..))
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C, E> {
    publish: T,
    inner: Rc<Inner<C>>,
    cfg: Cfg<MqttServiceConfig>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: Pipeline<C>,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<num::NonZeroU16>,
    aliases: HashMap<num::NonZeroU16, ByteString>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Publish, Response = PublishAck>,
    T::Error: ToPublishAck<Error = E>,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: Pipeline<C>,
        cfg: Cfg<MqttServiceConfig>,
    ) -> Self {
        Self {
            cfg,
            publish,
            _t: PhantomData,
            inner: Rc::new(Inner {
                sink,
                control,
                info: RefCell::new(PublishInfo {
                    aliases: HashMap::default(),
                    inflight: HashSet::default(),
                }),
            }),
        }
    }

    fn tag(&self) -> &'static str {
        self.inner.sink.tag()
    }
}

impl<T, C, E> Service<Decoded> for Dispatcher<T, C, E>
where
    T: Service<Publish, Response = PublishAck> + 'static,
    T::Error: ToPublishAck<Error = E>,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>
        + 'static,
{
    type Response = Option<Encoded>;
    type Error = DispatcherError<E>;

    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
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

    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        self.publish.poll(cx).map_err(|e| DispatcherError::Service(e.into_error()))?;
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        log::trace!("{}: Shutdown v5 dispatcher", self.tag());
        self.inner.sink.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.drop_sink();

        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::too_many_lines, clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        request: Decoded,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("{}: Dispatch v5 packet: {:#?}", self.tag(), request);

        match request {
            Decoded::Publish(mut publish, payload, size) => {
                let info = self.inner.as_ref();
                let packet_id = publish.packet_id;

                if publish.topic.contains(['#', '+']) {
                    return Err(SpecViolation::Pub_3_3_2_2.into());
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
                                return Err(ProtocolError::violation(
                                    DisconnectReasonCode::TopicAliasInvalid,
                                    "Unknown topic alias",
                                )
                                .into());
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
                                        return Err(SpecViolation::Connack_3_2_2_17.into());
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
                if let Some(pl) = self.inner.sink.payload.take() {
                    pl.feed_data(buf);
                    if eof {
                        pl.feed_eof();
                    } else {
                        self.inner.sink.payload.set(Some(pl));
                    }
                    Ok(None)
                } else {
                    Err(ProtocolError::Decode(DecodeError::UnexpectedPayload).into())
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
                if self.inner.info.borrow().inflight.contains(&ack.packet_id) {
                    self.inner.control(ProtocolMessage::pubrel(ack, size)).await
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
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }
                self.inner.control(ProtocolMessage::auth(pkt, size)).await
            }
            Decoded::Packet(Packet::PingRequest, _) => {
                self.inner.control(ProtocolMessage::ping()).await
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
                    self.inner.control(ProtocolMessage::remote_disconnect(pkt, size)).await
                }
            }
            Decoded::Packet(Packet::Subscribe(pkt), size) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if pkt.topic_filters.iter().any(|(tf, _)| !crate::topic::is_valid(tf)) {
                    return Err(SpecViolation::Subs_4_7_1.into());
                }

                if pkt.id.is_some() && !self.inner.sink.codec.sub_ids_available() {
                    log::trace!(
                        "{}: Subscription Identifiers are not supported but was set",
                        self.tag()
                    );
                    return Err(SpecViolation::Connack_3_2_2_21.into());
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
                self.inner.control_pkt(ProtocolMessage::subscribe(pkt, size), id.get()).await
            }
            Decoded::Packet(Packet::Unsubscribe(pkt), size) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if pkt.topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    return Err(SpecViolation::Subs_4_7_1.into());
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
                self.inner.control_pkt(ProtocolMessage::unsubscribe(pkt, size), id.get()).await
            }
            Decoded::Packet(_, _) => Ok(None),
        }
    }
}

impl<C> Inner<C> {
    async fn control<E>(
        &self,
        pkt: ProtocolMessage,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        self.control_pkt(pkt, 0).await
    }

    async fn control_pkt<E>(
        &self,
        pkt: ProtocolMessage,
        packet_id: u16,
    ) -> Result<Option<Encoded>, DispatcherError<E>>
    where
        C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
    {
        let result = match self.control.call(pkt).await {
            Ok(result) => {
                if let Some(id) = num::NonZeroU16::new(packet_id) {
                    self.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                self.sink.drop_sink();
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
            self.sink.drop_sink();
            self.sink.drop_payload(&PayloadError::Service);
        }
        response
    }
}

/// Publish service response future
async fn publish_fn<'f, T, C, E>(
    publish: &T,
    pkt: Publish,
    packet_id: u16,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    T: Service<Publish, Response = PublishAck>,
    T::Error: ToPublishAck<Error = E>,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
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

#[derive(Clone, Debug)]
pub struct ControlService<S, E> {
    svc: S,
    shared: Rc<MqttShared>,
    _t: PhantomData<E>,
}

#[derive(Clone, Debug)]
pub struct ControlFactory<S, St, E> {
    svc: S,
    _t: PhantomData<(E, St)>,
}

impl<S, E> ControlService<S, E>
where
    S: Service<Control<E>>,
{
    pub(super) fn new(svc: S, shared: Rc<MqttShared>) -> Self {
        Self { svc, shared, _t: PhantomData }
    }
}

impl<S, St, E> ControlFactory<S, St, E>
where
    S: ServiceFactory<Control<E>, Session<St>>,
{
    pub(super) fn new(svc: S) -> Self {
        Self { svc, _t: PhantomData }
    }
}

impl<S, St, E> ServiceFactory<Control<E>, Session<St>> for ControlFactory<S, St, E>
where
    S: ServiceFactory<Control<E>, Session<St>, Response = Option<Encoded>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type InitError = S::InitError;
    type Service = ControlService<S::Service, E>;

    async fn create(&self, cfg: Session<St>) -> Result<Self::Service, Self::InitError> {
        Ok(ControlService {
            shared: cfg.sink().shared(),
            svc: self.svc.create(cfg).await?,
            _t: PhantomData,
        })
    }
}

impl<S, E> Service<Control<E>> for ControlService<S, E>
where
    S: Service<Control<E>, Response = Option<Encoded>>,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn call(
        &self,
        req: Control<E>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        let proto_err = match &req {
            Control::Stop(Reason::Error(_)) => {
                self.shared.drop_payload(&PayloadError::Service);
                false
            }
            Control::Stop(Reason::Protocol(err)) => {
                self.shared.drop_payload(err.get_ref());
                true
            }
            Control::Stop(Reason::PeerGone(_)) => {
                self.shared.drop_payload(&PayloadError::Disconnected);
                false
            }
            Control::WrBackpressure(status) => {
                if status.enabled() {
                    self.shared.enable_wr_backpressure();
                } else {
                    self.shared.disable_wr_backpressure();
                }
                false
            }
        };

        match ctx.call(&self.svc, req).await {
            Ok(Some(val)) => {
                if proto_err || !self.shared.is_disconnect_recv() {
                    Ok(Some(val))
                } else {
                    Ok(None)
                }
            }
            res => res,
        }
    }

    ntex_service::forward_ready!(svc);
    ntex_service::forward_poll!(svc);
    ntex_service::forward_shutdown!(svc);
}

#[cfg(test)]
mod tests {
    use ntex_io::{Io, testing::IoTest};
    use ntex_util::future::lazy;

    use super::*;
    use crate::{control, v5::MqttSink};

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
        let sink = MqttSink::new(shared.clone());
        let ses = Session::new((), sink.clone());

        let disp = ControlFactory::<_, (), ()>::new(control::DefaultControlService::<
            Session<()>,
            (),
            codec::Encoded,
        >::default());
        let svc = disp.pipeline(ses).await.unwrap();

        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        svc.call(Control::wr(true)).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        svc.call(Control::wr(false)).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }
}
