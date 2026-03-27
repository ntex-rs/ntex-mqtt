use std::cell::{Cell, RefCell};
use std::{marker::PhantomData, num::NonZeroU16, rc::Rc, task::Context};

use ntex_dispatcher::{Control as DispControl, DispatchItem, Reason};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Pipeline, PipelineSvc, Service, ServiceCtx, ServiceFactory};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::{HashMap, future::join, services::inflight::InFlightService};

use crate::error::{DecodeError, HandshakeError, PayloadError, ProtocolError, SpecViolation};
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::{MqttError, MqttServiceConfig, types::QoS, types::packet_type};

use super::codec::{Decoded, Encoded, Packet};
use super::control::{Control, ControlAck, ControlAckKind, Subscribe, Unsubscribe};
use super::{Session, publish::Publish, shared::Ack, shared::MqttShared};

/// mqtt3 protocol dispatcher
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
    T: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    C: ServiceFactory<Control<E>, Session<St>, Response = ControlAck> + 'static,
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
            let control_unbuf = Pipeline::new(
                control
                    .map_err(|e| MqttError::Service(e.into()))?
                    .map_err(|e| MqttError::Service(E::from(e))),
            );

            let control = Pipeline::new(
                BufferService::new(
                    16,
                    // limit number of in-flight messages
                    InFlightService::new(1, PipelineSvc::new(control_unbuf.clone())),
                )
                .map_err(|err| match err {
                    BufferServiceError::Service(e) => e,
                    BufferServiceError::RequestCanceled => {
                        MqttError::Handshake(HandshakeError::Disconnected(None))
                    }
                }),
            );

            let cfg: Cfg<MqttServiceConfig> = cfg.get();
            Ok(Dispatcher::<_, _, _, E>::new(sink, publish, control, control_unbuf, cfg))
        },
    )
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
    _t: PhantomData<(E,)>,
}

struct Inner<C, C2> {
    control: Pipeline<C>,
    control_unbuf: Pipeline<C2>,
    sink: Rc<MqttShared>,
    payload: Cell<Option<PlSender>>,
    inflight: RefCell<HashMap<NonZeroU16, u8>>,
}

impl<T, C, C2, E> Dispatcher<T, C, C2, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    pub(crate) fn new(
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
                inflight: RefCell::new(HashMap::default()),
            }),
            _t: PhantomData,
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
    T: Service<Publish, Response = ()> + 'static,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
{
    type Response = Option<Encoded>;
    type Error = MqttError<E>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
        let result = if let Err(e) = res1 {
            if res2.is_err() {
                Err(MqttError::Service(e.into()))
            } else if !self.inner.sink.is_dispatcher_stopped() {
                match self.inner.control_unbuf.call(Control::error(e.into())).await {
                    Ok(_) => {
                        self.inner.sink.close();
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
                self.inner.sink.close();
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
                if inner.control_unbuf.call(Control::error(e.into())).await.is_ok() {
                    inner.sink.close();
                }
            });
        }
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();
        let _ = self.inner.control.call(Control::shutdown()).await;

        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        req: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("{}; Dispatch v3 packet: {:#?}", self.tag(), req);

        match req {
            DispatchItem::Item(Decoded::Publish(publish, payload, size)) => {
                if publish.topic.contains(['#', '+']) {
                    return self.inner.control(Control::spec(SpecViolation::Pub_3_3_2_2)).await;
                }

                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    let mut inflight = inner.inflight.borrow_mut();

                    // publish re-send with same packet-id and dup
                    if let Some(tp) = inflight.get(&pid) {
                        if *tp == packet_type::PUBLISH_START && publish.dup {
                            return Ok(None);
                        }

                        log::trace!(
                            "{}: Duplicated packet id for publish packet: {:?}",
                            self.tag(),
                            pid
                        );
                        return self
                            .inner
                            .control(Control::spec(SpecViolation::PacketId_2_2_1_3_Pub))
                            .await;
                    }
                    inflight.insert(pid, packet_type::PUBLISH_START);
                }

                // check max allowed qos
                if publish.qos > self.cfg.max_qos {
                    log::trace!(
                        "{}: Max allowed QoS is violated, max {:?} provided {:?}",
                        self.tag(),
                        self.cfg.max_qos,
                        publish.qos
                    );
                    return self
                        .inner
                        .control(Control::spec(SpecViolation::Connack_3_2_2_11))
                        .await;
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
                    self.inner
                        .control(Control::proto_error(ProtocolError::Decode(
                            DecodeError::UnexpectedPayload,
                        )))
                        .await
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishAck { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    self.inner.control(Control::proto_error(e)).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishReceived { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(packet_id)) {
                    self.inner.control(Control::proto_error(e)).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishRelease { packet_id }, _)) => {
                if let Some(tp) = self.inner.inflight.borrow().get(&packet_id)
                    && *tp == packet_type::PUBLISH_START
                {
                    self.inner.control(Control::pubrel(packet_id)).await
                } else {
                    self.inner
                        .control(Control::proto_error(ProtocolError::unexpected_packet(
                            packet_type::PUBREL,
                            "Unknown packet-id in PublishRelease packet",
                        )))
                        .await
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishComplete { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(packet_id)) {
                    self.inner.control(Control::proto_error(e)).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PingRequest, _)) => {
                self.inner.control(Control::ping()).await
            }
            DispatchItem::Item(Decoded::Packet(
                Packet::Subscribe { packet_id, topic_filters },
                size,
            )) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if topic_filters.iter().any(|(tf, _)| !crate::topic::is_valid(tf)) {
                    return self.inner.control(Control::spec(SpecViolation::Subs_4_7_1)).await;
                }

                let mut inflight = self.inner.inflight.borrow_mut();
                if let Some(tp) = inflight.get(&packet_id) {
                    // re-send packet
                    if *tp == packet_type::SUBSCRIBE {
                        return Ok(None);
                    }

                    log::trace!(
                        "{}: Duplicated packet id for subscribe packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    return self
                        .inner
                        .control(Control::spec(SpecViolation::PacketId_2_2_1_3_Sub))
                        .await;
                }
                inflight.insert(packet_id, packet_type::SUBSCRIBE);

                self.inner
                    .control(Control::subscribe(Subscribe::new(packet_id, size, topic_filters)))
                    .await
            }
            DispatchItem::Item(Decoded::Packet(
                Packet::Unsubscribe { packet_id, topic_filters },
                size,
            )) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    return self.inner.control(Control::spec(SpecViolation::Subs_4_7_1)).await;
                }

                let mut inflight = self.inner.inflight.borrow_mut();
                if let Some(tp) = inflight.get(&packet_id) {
                    // re-send packet
                    if *tp == packet_type::UNSUBSCRIBE {
                        return Ok(None);
                    }

                    log::trace!(
                        "{}: Duplicated packet id for unsubscribe packet: {:?}",
                        self.tag(),
                        packet_id
                    );
                    return self
                        .inner
                        .control(Control::spec(SpecViolation::PacketId_2_2_1_3_Unsub))
                        .await;
                }
                inflight.insert(packet_id, packet_type::UNSUBSCRIBE);

                self.inner
                    .control(Control::unsubscribe(Unsubscribe::new(
                        packet_id,
                        size,
                        topic_filters,
                    )))
                    .await
            }
            DispatchItem::Item(Decoded::Packet(Packet::Disconnect, _)) => {
                self.inner.sink.is_disconnect_sent();
                self.inner.sink.close();
                self.inner.control(Control::remote_disconnect()).await
            }
            DispatchItem::Item(_) => Ok(None),
            DispatchItem::Stop(Reason::Encoder(err)) => {
                let err = ProtocolError::Encode(err);
                self.inner.drop_payload(&err);
                self.inner.control(Control::proto_error(err)).await
            }
            DispatchItem::Stop(Reason::KeepAliveTimeout) => {
                self.inner.drop_payload(&ProtocolError::KeepAliveTimeout);
                self.inner.control(Control::proto_error(ProtocolError::KeepAliveTimeout)).await
            }
            DispatchItem::Stop(Reason::ReadTimeout) => {
                self.inner.drop_payload(&ProtocolError::ReadTimeout);
                self.inner.control(Control::proto_error(ProtocolError::ReadTimeout)).await
            }
            DispatchItem::Stop(Reason::Decoder(err)) => {
                let err = ProtocolError::Decode(err);
                self.inner.drop_payload(&err);
                self.inner.control(Control::proto_error(err)).await
            }
            DispatchItem::Stop(Reason::Io(err)) => {
                self.inner.drop_payload(&PayloadError::Disconnected);
                self.inner.control(Control::peer_gone(err)).await
            }
            DispatchItem::Control(DispControl::WBackPressureEnabled) => {
                self.inner.sink.enable_wr_backpressure();
                self.inner.control(Control::wr_backpressure(true)).await
            }
            DispatchItem::Control(DispControl::WBackPressureDisabled) => {
                self.inner.sink.disable_wr_backpressure();
                self.inner.control(Control::wr_backpressure(false)).await
            }
        }
    }
}

/// Publish service response future
async fn publish_fn<'f, T, C, C2, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<C, C2>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, C2, E>>,
) -> Result<Option<Encoded>, MqttError<E>>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
    C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
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
        Err(e) => inner.control(Control::error(e.into())).await,
    }
}

impl<C, C2> Inner<C, C2> {
    async fn control<E>(&self, mut pkt: Control<E>) -> Result<Option<Encoded>, MqttError<E>>
    where
        C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
        C2: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
    {
        loop {
            let error = matches!(pkt, Control::Stop(_));
            let result = if matches!(pkt, Control::Protocol(_)) {
                self.control.call(pkt).await
            } else {
                if error && self.sink.is_dispatcher_stopped() {
                    self.drop_payload(&PayloadError::Service);
                    self.sink.close();
                    return Ok(None);
                }
                self.control_unbuf.call(pkt).await
            };

            match result {
                Ok(item) => {
                    let packet = match item.result {
                        ControlAckKind::Ping => Some(Encoded::Packet(Packet::PingResponse)),
                        ControlAckKind::Subscribe(res) => {
                            self.inflight.borrow_mut().remove(&res.packet_id);
                            Some(Encoded::Packet(Packet::SubscribeAck {
                                status: res.codes,
                                packet_id: res.packet_id,
                            }))
                        }
                        ControlAckKind::Unsubscribe(res) => {
                            self.inflight.borrow_mut().remove(&res.packet_id);
                            Some(Encoded::Packet(Packet::UnsubscribeAck {
                                packet_id: res.packet_id,
                            }))
                        }
                        ControlAckKind::Disconnect => {
                            self.drop_payload(&PayloadError::Service);
                            self.sink.close();
                            None
                        }
                        ControlAckKind::Closed | ControlAckKind::Nothing => None,
                        ControlAckKind::PublishRelease(packet_id) => {
                            self.inflight.borrow_mut().remove(&packet_id);
                            Some(Encoded::Packet(Packet::PublishComplete { packet_id }))
                        }
                        ControlAckKind::PublishAck(_) => unreachable!(),
                    };
                    return Ok(packet);
                }
                Err(err) => {
                    // do not handle nested error
                    let result = if error {
                        Err(err)
                    } else {
                        // handle error from control service
                        if let MqttError::Service(err) = err {
                            pkt = Control::error(err);
                            continue;
                        }
                        Err(err)
                    };
                    self.drop_payload(&PayloadError::Service);
                    self.sink.close();
                    return result;
                }
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
    use crate::v3::{CtlReason, MqttSink, codec};

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
            if let Control::Stop(CtlReason::ProtocolError(_)) = ctrl {
                *err2.borrow_mut() = true;
            }
            Ready::Ok(ControlAck { result: ControlAckKind::Nothing })
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
