use std::{cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc, task::Context};

use ntex_io::DispatchItem;
use ntex_service::{Pipeline, Service, ServiceCtx, ServiceFactory};
use ntex_util::services::buffer::{BufferService, BufferServiceError};
use ntex_util::services::inflight::InFlightService;
use ntex_util::{future::join, HashSet};

use crate::error::{HandshakeError, MqttError, ProtocolError};
use crate::types::QoS;

use super::control::{Control, ControlAck, ControlAckKind, Subscribe, Unsubscribe};
use super::{codec, publish::Publish, shared::Ack, shared::MqttShared, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    max_qos: QoS,
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
    T: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
    C: ServiceFactory<Control<E>, Session<St>, Response = ControlAck> + 'static,
    E: From<C::Error> + From<C::InitError> + From<T::Error> + From<T::InitError> + 'static,
{
    let factories = Rc::new((publish, control));

    ntex_service::fn_factory_with_config(move |session: Session<St>| {
        let factories = factories.clone();

        async move {
            // create services
            let sink = session.sink().shared();
            let fut = join(factories.0.create(session.clone()), factories.1.create(session));
            let (publish, control) = fut.await;

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

            Ok(Dispatcher::<_, _, E>::new(
                sink,
                publish,
                control,
                max_qos,
                handle_qos_after_disconnect,
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
pub(crate) struct Dispatcher<T, C: Service<Control<E>>, E> {
    publish: T,
    max_qos: QoS,
    handle_qos_after_disconnect: Option<QoS>,
    inner: Rc<Inner<C>>,
    _t: PhantomData<(E,)>,
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
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    pub(crate) fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        max_qos: QoS,
        handle_qos_after_disconnect: Option<QoS>,
    ) -> Self {
        Self {
            publish,
            max_qos,
            handle_qos_after_disconnect,
            inner: Rc::new(Inner {
                sink,
                control: Pipeline::new(control),
                inflight: RefCell::new(HashSet::default()),
            }),
            _t: PhantomData,
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = ()> + 'static,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
{
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) =
            join(ctx.ready(&self.publish), ctx.ready(self.inner.control.get_ref())).await;
        if let Err(e) = res1 {
            if res2.is_err() {
                Err(MqttError::Service(e.into()))
            } else {
                match ctx
                    .call_nowait(self.inner.control.get_ref(), Control::error(e.into()))
                    .await
                {
                    Ok(_) => {
                        self.inner.sink.close();
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
        } else {
            res2
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        if let Err(e) = self.publish.poll(cx) {
            let inner = self.inner.clone();
            ntex_rt::spawn(async move {
                if inner.control.call_nowait(Control::error(e.into())).await.is_ok() {
                    inner.sink.close();
                }
            });
        }
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        self.inner.sink.close();
        let _ = self.inner.control.call(Control::closed()).await;

        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    async fn call(
        &self,
        req: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Dispatch v3 packet: {:#?}", req);

        match req {
            DispatchItem::Item((codec::Packet::Publish(publish), size)) => {
                if publish.topic.contains(['#', '+']) {
                    return control(
                        Control::proto_error(
                            ProtocolError::generic_violation(
                                "PUBLISH packet's topic name contains wildcard character [MQTT-3.3.2-2]"
                            )
                        ),
                        &self.inner,
                        ctx,
                    ).await;
                }

                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inner.inflight.borrow_mut().insert(pid) {
                        log::trace!("Duplicated packet id for publish packet: {:?}", pid);
                        return control(
                            Control::proto_error(
                                ProtocolError::generic_violation("PUBLISH received with packet id that is already in use [MQTT-2.2.1-3]")
                            ),
                            &self.inner,
                            ctx,
                        ).await;
                    }
                }

                // check max allowed qos
                if publish.qos > self.max_qos {
                    log::trace!(
                        "Max allowed QoS is violated, max {:?} provided {:?}",
                        self.max_qos,
                        publish.qos
                    );
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            match publish.qos {
                                QoS::AtLeastOnce => "PUBLISH with QoS 1 is not supported",
                                QoS::ExactlyOnce => "PUBLISH with QoS 2 is not supported",
                                QoS::AtMostOnce => unreachable!(), // max_qos cannot be lower than QoS 0
                            },
                        )),
                        &self.inner,
                        ctx,
                    )
                    .await;
                }

                if inner.sink.is_closed()
                    && !self
                        .handle_qos_after_disconnect
                        .map(|max_qos| publish.qos <= max_qos)
                        .unwrap_or_default()
                {
                    return Ok(None);
                }

                publish_fn(&self.publish, Publish::new(publish, size), packet_id, inner, ctx)
                    .await
            }
            DispatchItem::Item((codec::Packet::PublishAck { packet_id }, _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    control(Control::proto_error(e), &self.inner, ctx).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item((codec::Packet::PingRequest, _)) => {
                control(Control::ping(), &self.inner, ctx).await
            }
            DispatchItem::Item((
                codec::Packet::Subscribe { packet_id, topic_filters },
                size,
            )) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if topic_filters.iter().any(|(tf, _)| !crate::topic::is_valid(tf)) {
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            "Topic filter is malformed [MQTT-4.7.1-*]",
                        )),
                        &self.inner,
                        ctx,
                    )
                    .await;
                }

                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!("Duplicated packet id for subscribe packet: {:?}", packet_id);
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            "SUBSCRIBE received with packet id that is already in use [MQTT-2.2.1-3]"
                        )),
                        &self.inner,
                        ctx,
                    ).await;
                }

                control(
                    Control::subscribe(Subscribe::new(packet_id, size, topic_filters)),
                    &self.inner,
                    ctx,
                )
                .await
            }
            DispatchItem::Item((
                codec::Packet::Unsubscribe { packet_id, topic_filters },
                size,
            )) => {
                if self.inner.sink.is_closed() {
                    return Ok(None);
                }

                if topic_filters.iter().any(|tf| !crate::topic::is_valid(tf)) {
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            "Topic filter is malformed [MQTT-4.7.1-*]",
                        )),
                        &self.inner,
                        ctx,
                    )
                    .await;
                }

                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!("Duplicated packet id for unsubscribe packet: {:?}", packet_id);
                    return control(
                        Control::proto_error(ProtocolError::generic_violation(
                            "UNSUBSCRIBE received with packet id that is already in use [MQTT-2.2.1-3]"
                        )),
                        &self.inner,
                        ctx,
                    ).await;
                }

                control(
                    Control::unsubscribe(Unsubscribe::new(packet_id, size, topic_filters)),
                    &self.inner,
                    ctx,
                )
                .await
            }
            DispatchItem::Item((codec::Packet::Disconnect, _)) => {
                control(Control::remote_disconnect(), &self.inner, ctx).await
            }
            DispatchItem::Item(_) => Ok(None),
            DispatchItem::EncoderError(err) => {
                control(Control::proto_error(ProtocolError::Encode(err)), &self.inner, ctx)
                    .await
            }
            DispatchItem::KeepAliveTimeout => {
                control(Control::proto_error(ProtocolError::KeepAliveTimeout), &self.inner, ctx)
                    .await
            }
            DispatchItem::ReadTimeout => {
                control(Control::proto_error(ProtocolError::ReadTimeout), &self.inner, ctx)
                    .await
            }
            DispatchItem::DecoderError(err) => {
                control(Control::proto_error(ProtocolError::Decode(err)), &self.inner, ctx)
                    .await
            }
            DispatchItem::Disconnect(err) => {
                control(Control::peer_gone(err), &self.inner, ctx).await
            }
            DispatchItem::WBackPressureEnabled => {
                self.inner.sink.enable_wr_backpressure();
                control(Control::wr_backpressure(true), &self.inner, ctx).await
            }
            DispatchItem::WBackPressureDisabled => {
                self.inner.sink.disable_wr_backpressure();
                control(Control::wr_backpressure(false), &self.inner, ctx).await
            }
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
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    match ctx.call(svc, pkt).await {
        Ok(_) => {
            log::trace!("Publish result for packet {:?} is ready", packet_id);

            if let Some(packet_id) = packet_id {
                inner.inflight.borrow_mut().remove(&packet_id);
                Ok(Some(codec::Packet::PublishAck { packet_id }))
            } else {
                Ok(None)
            }
        }
        Err(e) => control(Control::error(e.into()), inner, ctx).await,
    }
}

async fn control<'f, T, C, E>(
    mut pkt: Control<E>,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<codec::Packet>, MqttError<E>>
where
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let mut error = matches!(pkt, Control::Error(_) | Control::ProtocolError(_));

    loop {
        match ctx.call(inner.control.get_ref(), pkt).await {
            Ok(item) => {
                let packet = match item.result {
                    ControlAckKind::Ping => Some(codec::Packet::PingResponse),
                    ControlAckKind::Subscribe(res) => {
                        inner.inflight.borrow_mut().remove(&res.packet_id);
                        Some(codec::Packet::SubscribeAck {
                            status: res.codes,
                            packet_id: res.packet_id,
                        })
                    }
                    ControlAckKind::Unsubscribe(res) => {
                        inner.inflight.borrow_mut().remove(&res.packet_id);
                        Some(codec::Packet::UnsubscribeAck { packet_id: res.packet_id })
                    }
                    ControlAckKind::Disconnect => {
                        inner.sink.close();
                        None
                    }
                    ControlAckKind::Closed | ControlAckKind::Nothing => None,
                    ControlAckKind::PublishAck(_) => unreachable!(),
                };
                return Ok(packet);
            }
            Err(err) => {
                // do not handle nested error
                return if error {
                    Err(err)
                } else {
                    // handle error from control service
                    match err {
                        MqttError::Service(err) => {
                            error = true;
                            pkt = Control::error(err);
                            continue;
                        }
                        _ => Err(err),
                    }
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{testing::IoTest, Io};
    use ntex_service::fn_service;
    use ntex_util::future::{lazy, Ready};
    use ntex_util::time::{sleep, Seconds};

    use super::*;
    use crate::v3::MqttSink;

    #[ntex_macros::rt_test]
    async fn test_dup_packet_id() {
        let io = Io::new(IoTest::create().0);
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Default::default()));
        let err = Rc::new(RefCell::new(false));
        let err2 = err.clone();

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| async {
                sleep(Seconds(10)).await;
                Ok(())
            }),
            fn_service(move |ctrl| {
                if let Control::ProtocolError(_) = ctrl {
                    *err2.borrow_mut() = true;
                }
                Ready::Ok(ControlAck { result: ControlAckKind::Nothing })
            }),
            QoS::AtLeastOnce,
            None,
        ));

        let mut f: Pin<Box<dyn Future<Output = Result<_, _>>>> =
            Box::pin(disp.call(DispatchItem::Item((
                codec::Packet::Publish(codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtLeastOnce,
                    topic: ByteString::new(),
                    packet_id: NonZeroU16::new(1),
                    payload: Bytes::new(),
                }),
                999,
            ))));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let f = Box::pin(disp.call(DispatchItem::Item((
            codec::Packet::Publish(codec::Publish {
                dup: false,
                retain: false,
                qos: QoS::AtLeastOnce,
                topic: ByteString::new(),
                packet_id: NonZeroU16::new(1),
                payload: Bytes::new(),
            }),
            999,
        ))));
        assert!(f.await.unwrap().is_none());
        assert!(*err.borrow());
    }

    #[ntex_macros::rt_test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0);
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Default::default()));

        let disp = Pipeline::new(Dispatcher::<_, _, ()>::new(
            shared.clone(),
            fn_service(|_| Ready::Ok(())),
            fn_service(|_| Ready::Ok(ControlAck { result: ControlAckKind::Nothing })),
            QoS::AtLeastOnce,
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
