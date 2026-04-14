use std::cell::{Cell, RefCell};
use std::{marker::PhantomData, num::NonZeroU16, rc::Rc, task::Context};

use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Pipeline, PipelineSvc, Service, ServiceCtx, ServiceFactory};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::{HashSet, future::join, services::inflight::InFlightService};

use crate::error::{
    DecodeError, DispatcherError, HandshakeError, PayloadError, ProtocolError, SpecViolation,
};
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::{MqttError, MqttServiceConfig, types::QoS, types::packet_type};

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
    InitError = MqttError<E>,
>
where
    St: 'static,
    T: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    C: ServiceFactory<ProtocolMessage, Session<St>, Response = ProtocolMessageAck> + 'static,
    E: From<C::Error> + From<C::InitError> + From<T::Error> + From<T::InitError> + 'static,
{
    let factories = Rc::new((publish, control));

    ntex_service::fn_factory_with_config(
        async move |(cfg, session): (SharedCfg, Session<St>)| {
            // create services
            let sink = session.sink().shared();
            let fut = join(factories.0.create(session.clone()), factories.1.create(session));
            let (publish, control) = fut.await;

            let publish = publish.map_err(|e| MqttError::Service(e.into()))?;
            let control = Pipeline::new(
                control
                    .map_err(|e| MqttError::Service(e.into()))?
                    .map_err(|e| DispatcherError::Service(E::from(e))),
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
    payload: Cell<Option<PlSender>>,
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
            inner: Rc::new(Inner {
                sink,
                control,
                payload: Cell::new(None),
                inflight: RefCell::new(HashSet::default()),
            }),
            _t: PhantomData,
        }
    }

    fn tag(&self) -> &'static str {
        self.inner.sink.tag()
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
            && let Some(pl) = self.inner.payload.take()
        {
            self.inner.payload.set(Some(pl.clone()));
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
        self.inner.drop_payload(&PayloadError::Disconnected);
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
                if let Some(pl) = self.inner.payload.take() {
                    pl.feed_data(buf);
                    if eof {
                        pl.feed_eof();
                    } else {
                        self.inner.payload.set(Some(pl));
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
            _ => Ok(None),
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
        mut pkt: ProtocolMessage,
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
                        self.drop_payload(&PayloadError::Service);
                        self.sink.close();
                        None
                    }
                    ProtocolMessageKind::Closed | ProtocolMessageKind::Nothing => None,
                    ProtocolMessageKind::PublishRelease(packet_id) => {
                        self.inflight.borrow_mut().remove(&packet_id);
                        Some(Encoded::Packet(Packet::PublishComplete { packet_id }))
                    }
                    ProtocolMessageKind::PublishAck(_) => unreachable!(),
                };
                Ok(packet)
            }
            Err(err) => {
                self.drop_payload(&PayloadError::Service);
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
    use ntex_service::{cfg::SharedCfg, fn_service};
    use ntex_util::future::{Ready, lazy};
    use ntex_util::time::{Seconds, sleep};

    use super::*;
    use crate::v3::{MqttSink, codec};

    #[ntex::test]
    async fn test_dup_packet_id() {
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce))
            .into();

        let io = Io::new(IoTest::create().0, cfg.clone());
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let err = Rc::new(RefCell::new(false));
        let err2 = err.clone();

        let control = Pipeline::new(fn_service(move |ctrl| {
            if let Control::ProtocolError(_) = ctrl {
                *err2.borrow_mut() = true;
            }
            Ready::Ok(None)
        }));

        let disp = Pipeline::new(Dispatcher::<_, _, _, ()>::new(
            shared.clone(),
            fn_service(|_| async {
                sleep(Seconds(10)).await;
                Ok(())
            }),
            control.clone(),
            control,
            cfg.get(),
        ));

        let mut f: Pin<Box<dyn Future<Output = Result<_, _>>>> =
            Box::pin(disp.call(DispatchItem::Item(Decoded::Publish(
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
            ))));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let f = Box::pin(disp.call(DispatchItem::Item(Decoded::Publish(
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
        ))));
        assert!(f.await.unwrap().is_none());
        assert!(*err.borrow());
    }

    #[ntex::test]
    async fn test_wr_backpressure() {
        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(MqttServiceConfig::new().set_max_qos(QoS::AtLeastOnce))
            .into();

        let io = Io::new(IoTest::create().0, cfg.clone());
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let control = Pipeline::new(fn_service(|_| {
            Ready::Ok(ControlAck { result: ControlAckKind::Nothing })
        }));

        let disp = Pipeline::new(Dispatcher::<_, _, _, ()>::new(
            shared.clone(),
            fn_service(|_| Ready::Ok(())),
            control.clone(),
            control,
            cfg.get(),
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
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let counter = Rc::new(Cell::new(0));
        let counter2 = counter.clone();
        let control = Pipeline::new(fn_service(async move |msg| {
            if matches!(msg, Control::Stop(_)) {
                counter2.set(counter2.get() + 1);
            }
            Ok::<_, MqttError<()>>(ControlAck { result: ControlAckKind::Nothing })
        }));

        let disp = Pipeline::new(Dispatcher::<_, _, _, _>::new(
            shared.clone(),
            fn_service(async |_: Publish| Err(())),
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
