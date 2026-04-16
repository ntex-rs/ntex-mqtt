use std::cell::RefCell;
use std::{marker::PhantomData, num::NonZeroU16, rc::Rc, task::Context};

use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Pipeline, PipelineSvc, Service, ServiceCtx, ServiceFactory};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::{HashSet, future::join, services::inflight::InFlightService};

use crate::control::{Control, Reason};
use crate::error::{DecodeError, DispatcherError, PayloadError, ProtocolError, SpecViolation};
use crate::payload::{Payload, PayloadStatus};
use crate::{MqttServiceConfig, types::QoS, types::packet_type};

use super::codec::{Decoded, Encoded, Packet};
use super::control::{
    ProtocolMessage, ProtocolMessageAck, ProtocolMessageKind, Subscribe, Unsubscribe,
};
use super::{Session, publish::Publish, shared::Ack, shared::MqttShared};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
) -> impl ServiceFactory<
    Decoded,
    (SharedCfg, Session<St>),
    Response = Option<Encoded>,
    Error = DispatcherError<E>,
    InitError = T::InitError,
>
where
    St: 'static,
    T: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    C: ServiceFactory<ProtocolMessage, Session<St>, Response = ProtocolMessageAck> + 'static,
    E: From<C::Error> + From<T::Error> + From<T::InitError> + 'static,
    T::InitError: From<C::InitError>,
{
    let factories = Rc::new((publish, control));

    ntex_service::fn_factory_with_config(
        async move |(cfg, session): (SharedCfg, Session<St>)| {
            // create services
            let sink = session.sink().shared();
            let fut = join(factories.0.create(session.clone()), factories.1.create(session));
            let (publish, control) = fut.await;

            let publish = publish?;
            let control = Pipeline::new(
                control
                    .map_err(<T::InitError>::from)?
                    .map_err(|e| DispatcherError::Service(e.into())),
            );

            let control = Pipeline::new(
                BufferService::new(
                    16,
                    // limit number of in-flight messages
                    InFlightService::new(1, PipelineSvc::new(control.clone())),
                )
                .map_err(|err| match err {
                    BufferServiceError::Service(e) => e,
                    BufferServiceError::RequestCanceled => {
                        DispatcherError::Protocol(ProtocolError::ReadTimeout)
                    }
                }),
            );

            let cfg: Cfg<MqttServiceConfig> = cfg.get();
            Ok(Dispatcher::<_, _, E>::new(sink, publish, control, cfg))
        },
    )
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
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    pub(crate) fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: Pipeline<C>,
        cfg: Cfg<MqttServiceConfig>,
    ) -> Self {
        Self {
            cfg,
            publish,
            inner: Rc::new(Inner { sink, control, inflight: RefCell::new(HashSet::default()) }),
            _t: PhantomData,
        }
    }

    fn tag(&self) -> &'static str {
        self.inner.sink.tag()
    }
}

impl<T, C, E> Service<Decoded> for Dispatcher<T, C, E>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = ()> + 'static,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>
        + 'static,
{
    type Response = Option<Encoded>;
    type Error = DispatcherError<E>;

    #[inline]
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

        res1.map_err(|e| DispatcherError::Service(e.into()))?;
        res2?;
        Ok(())
    }

    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        self.publish.poll(cx).map_err(|e| DispatcherError::Service(e.into()))?;
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        self.inner.sink.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();
        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        req: Decoded,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("{}; Dispatch v3 packet: {:#?}", self.tag(), req);

        match req {
            Decoded::Publish(publish, payload, size) => {
                if publish.topic.contains(['#', '+']) {
                    return Err(SpecViolation::Pub_3_3_2_2.into());
                }

                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id
                    && !inner.inflight.borrow_mut().insert(pid)
                {
                    log::trace!(
                        "{}: Duplicated packet id for publish packet: {:?}",
                        self.tag(),
                        pid
                    );
                    return Err(SpecViolation::PacketId_2_2_1_3_Pub.into());
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

                if inner.sink.is_closed()
                    && self
                        .cfg
                        .handle_qos_after_disconnect
                        .is_none_or(|max_qos| publish.qos > max_qos)
                {
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
                if self.inner.inflight.borrow().contains(&packet_id) {
                    self.inner.control(ProtocolMessage::pubrel(packet_id)).await
                } else {
                    Err(ProtocolError::unexpected_packet(
                        packet_type::PUBREL,
                        "Unknown packet-id in PublishRelease packet",
                    )
                    .into())
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
                self.inner.control(ProtocolMessage::ping()).await
            }
            Decoded::Packet(Packet::Subscribe { packet_id, topic_filters }, size) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if topic_filters.iter().any(|(tf, _)| !crate::topic::is_valid(tf)) {
                    return Err(SpecViolation::Subs_4_7_1.into());
                }

                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!(
                        "{}: Duplicated packet id for subscribe packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    return Err(SpecViolation::PacketId_2_2_1_3_Sub.into());
                }

                self.inner
                    .control(ProtocolMessage::subscribe(Subscribe::new(
                        packet_id,
                        size,
                        topic_filters,
                    )))
                    .await
            }
            Decoded::Packet(Packet::Unsubscribe { packet_id, topic_filters }, size) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    return Err(SpecViolation::Subs_4_7_1.into());
                }

                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!(
                        "{}: Duplicated packet id for unsubscribe packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    return Err(SpecViolation::PacketId_2_2_1_3_Unsub.into());
                }

                self.inner
                    .control(ProtocolMessage::unsubscribe(Unsubscribe::new(
                        packet_id,
                        size,
                        topic_filters,
                    )))
                    .await
            }
            Decoded::Packet(Packet::Disconnect, _) => {
                self.inner.sink.is_disconnect_sent();
                self.inner.sink.close();
                self.inner.control(ProtocolMessage::remote_disconnect()).await
            }
            Decoded::Packet(..) => Ok(None),
        }
    }
}

/// Publish service response future
async fn publish_fn<'f, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<Encoded>, DispatcherError<E>>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<ProtocolMessage, Response = ProtocolMessageAck, Error = DispatcherError<E>>,
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
                    Ok(Some(Encoded::Packet(Packet::PublishReceived { packet_id })))
                } else {
                    inner.inflight.borrow_mut().remove(&packet_id);
                    Ok(Some(Encoded::Packet(Packet::PublishAck { packet_id })))
                }
            } else {
                Ok(None)
            }
        }
        Err(e) => Err(DispatcherError::Service(e.into())),
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
        match self.control.call(pkt).await {
            Ok(item) => {
                let packet = match item.result {
                    ProtocolMessageKind::Ping => Some(Encoded::Packet(Packet::PingResponse)),
                    ProtocolMessageKind::Subscribe(res) => {
                        self.inflight.borrow_mut().remove(&res.packet_id);
                        Some(Encoded::Packet(Packet::SubscribeAck {
                            status: res.codes,
                            packet_id: res.packet_id,
                        }))
                    }
                    ProtocolMessageKind::Unsubscribe(res) => {
                        self.inflight.borrow_mut().remove(&res.packet_id);
                        Some(Encoded::Packet(Packet::UnsubscribeAck {
                            packet_id: res.packet_id,
                        }))
                    }
                    ProtocolMessageKind::Disconnect => {
                        self.sink.drop_payload(&PayloadError::Service);
                        self.sink.close();
                        None
                    }
                    ProtocolMessageKind::Nothing => None,
                    ProtocolMessageKind::PublishRelease(packet_id) => {
                        self.inflight.borrow_mut().remove(&packet_id);
                        Some(Encoded::Packet(Packet::PublishComplete { packet_id }))
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
    S: ServiceFactory<Control<E>, Session<St>>,
{
    type Response = Option<Encoded>;
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
    S: Service<Control<E>>,
{
    type Response = Option<Encoded>;
    type Error = S::Error;

    async fn call(
        &self,
        req: Control<E>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        match &req {
            Control::Stop(Reason::Error(_)) => {
                self.shared.drop_payload(&PayloadError::Service);
            }
            Control::Stop(Reason::Protocol(err)) => {
                self.shared.drop_payload(err.get_ref());
            }
            Control::Stop(Reason::PeerGone(_)) => {
                self.shared.drop_payload(&PayloadError::Disconnected);
            }
            Control::WrBackpressure(status) => {
                if status.enabled() {
                    self.shared.enable_wr_backpressure();
                } else {
                    self.shared.disable_wr_backpressure();
                }
            }
        }

        ctx.call(&self.svc, req).await.map(|_| None)
    }

    ntex_service::forward_ready!(svc);
    ntex_service::forward_poll!(svc);
    ntex_service::forward_shutdown!(svc);
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{cfg::SharedCfg, fn_service};
    use ntex_util::{future::lazy, time::Seconds, time::sleep};

    use super::*;
    use crate::{control, error, v3::MqttSink, v3::codec};

    #[ntex::test]
    async fn test_dup_packet_id() {
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce))
            .into();

        let io = Io::new(IoTest::create().0, cfg.clone());
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));

        let disp = Pipeline::new(Dispatcher::new(
            shared.clone(),
            fn_service(async |_| {
                sleep(Seconds(10)).await;
                Ok(())
            }),
            Pipeline::new(fn_service(async |msg: ProtocolMessage| {
                Ok::<_, DispatcherError<()>>(msg.ack())
            })),
            cfg.get(),
        ));

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

        let DispatcherError::Protocol(ProtocolError::ProtocolViolation(err)) =
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
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let sink = MqttSink::new(shared.clone());
        let ses = Session::new((), sink.clone());

        let disp = ControlFactory::<_, (), ()>::new(control::DefaultControlService::<
            _,
            (),
            codec::Codec,
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
