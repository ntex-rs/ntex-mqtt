use std::cell::RefCell;
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::io::DispatchItem;
use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::{
    buffer::BufferService, inflight::InFlightService, join, Either, HashSet, Ready,
};

use crate::error::{MqttError, ProtocolError};
use crate::types::QoS;

use super::control::{
    ControlMessage, ControlResult, ControlResultKind, Subscribe, Unsubscribe,
};
use super::shared::MqttShared;
use super::{codec, publish::Publish, shared::Ack, sink::MqttSink, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    inflight: u16,
    inflight_size: usize,
    max_qos: QoS,
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
    C: ServiceFactory<ControlMessage<E>, Session<St>, Response = ControlResult> + 'static,
    E: From<C::Error> + From<C::InitError> + From<T::Error> + From<T::InitError> + 'static,
{
    let factories = Rc::new((publish, control));

    fn_factory_with_config(move |cfg: Session<St>| {
        let factories = factories.clone();

        async move {
            // create services
            let fut = join(factories.0.create(cfg.clone()), factories.1.create(cfg.clone()));
            let (publish, control) = fut.await;

            let publish = publish.map_err(|e| MqttError::Service(e.into()))?;
            let control = control
                .map_err(|e| MqttError::Service(e.into()))?
                .map_err(|e| MqttError::Service(E::from(e)));

            let control = BufferService::new(
                16,
                || MqttError::<E>::Disconnected(None),
                // limit number of in-flight messages
                InFlightService::new(1, control),
            );

            Ok(
                // limit number of in-flight messages
                crate::inflight::InFlightService::new(
                    inflight,
                    inflight_size,
                    Dispatcher::<_, _, _, E>::new(cfg, publish, control, max_qos),
                ),
            )
        }
    })
}

impl crate::inflight::SizedRequest for DispatchItem<Rc<MqttShared>> {
    fn size(&self) -> u32 {
        if let DispatchItem::Item(ref item) = self {
            codec::encode::get_encoded_size(item) as u32
        } else {
            0
        }
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C: Service<ControlMessage<E>>, E> {
    session: Session<St>,
    publish: T,
    max_qos: QoS,
    shutdown: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,
    inner: Rc<Inner<C>>,
    _t: PhantomData<(E,)>,
}

struct Inner<C> {
    control: C,
    sink: MqttSink,
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(session: Session<St>, publish: T, control: C, max_qos: QoS) -> Self {
        let sink = session.sink().clone();

        Self {
            session,
            publish,
            max_qos,
            shutdown: RefCell::new(None),
            inner: Rc::new(Inner { sink, control, inflight: RefCell::new(HashSet::default()) }),
            _t: PhantomData,
        }
    }
}

impl<St, T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<St, T, C, E>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = ()>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>> + 'static,
{
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future<'f> = Either<
        PublishResponse<'f, T, C, E>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<'f, C, E>>,
    > where Self: 'f;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(|e| MqttError::Service(e.into()))?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, cx: &mut Context<'_>, is_error: bool) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            self.inner.sink.close();
            let inner = self.inner.clone();
            *shutdown = Some(Box::pin(async move {
                let _ = inner.control.call(ControlMessage::closed(is_error)).await;
            }));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.publish.poll_shutdown(cx, is_error);
        let res2 = self.inner.control.poll_shutdown(cx, is_error);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, req: DispatchItem<Rc<MqttShared>>) -> Self::Future<'_> {
        log::trace!("Dispatch v3 packet: {:#?}", req);

        match req {
            DispatchItem::Item(codec::Packet::Publish(publish)) => {
                let inner = self.inner.as_ref();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inner.inflight.borrow_mut().insert(pid) {
                        log::trace!("Duplicated packet id for publish packet: {:?}", pid);
                        return Either::Right(Either::Right(ControlResponse::new(
                            ControlMessage::proto_error(ProtocolError::ReceiveMaximumExceeded),
                            &self.inner,
                        )));
                    }
                }

                // check max allowed qos
                if publish.qos > self.max_qos {
                    log::trace!(
                        "Max allowed QoS is violated, max {:?} provided {:?}",
                        self.max_qos,
                        publish.qos
                    );
                    return Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(ProtocolError::MaxQoSViolated(publish.qos)),
                        &self.inner,
                    )));
                }

                Either::Left(PublishResponse {
                    packet_id,
                    inner,
                    state: PublishResponseState::Publish {
                        fut: self.publish.call(Publish::new(publish)),
                    },
                })
            }
            DispatchItem::Item(codec::Packet::PublishAck { packet_id }) => {
                if let Err(e) = self.session.sink().pkt_ack(Ack::Publish(packet_id)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(e),
                        &self.inner,
                    )))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::PingRequest) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::ping(), &self.inner),
            )),
            DispatchItem::Item(codec::Packet::Subscribe { packet_id, topic_filters }) => {
                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!("Duplicated packet id for unsubscribe packet: {:?}", packet_id);
                    return Either::Right(Either::Left(Ready::Err(MqttError::ServerError(
                        "Duplicated packet id for unsubscribe packet",
                    ))));
                }

                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::subscribe(Subscribe::new(packet_id, topic_filters)),
                    &self.inner,
                )))
            }
            DispatchItem::Item(codec::Packet::Unsubscribe { packet_id, topic_filters }) => {
                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!("Duplicated packet id for unsubscribe packet: {:?}", packet_id);
                    return Either::Right(Either::Left(Ready::Err(MqttError::ServerError(
                        "Duplicated packet id for unsubscribe packet",
                    ))));
                }

                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::unsubscribe(Unsubscribe::new(packet_id, topic_filters)),
                    &self.inner,
                )))
            }
            DispatchItem::Item(codec::Packet::Disconnect) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::remote_disconnect(), &self.inner),
            )),
            DispatchItem::Item(_) => Either::Right(Either::Left(Ready::Ok(None))),
            DispatchItem::EncoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Encode(err)),
                    &self.inner,
                )))
            }
            DispatchItem::KeepAliveTimeout => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                )))
            }
            DispatchItem::DecoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                )))
            }
            DispatchItem::Disconnect(err) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::peer_gone(err), &self.inner),
            )),
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                Either::Right(Either::Left(Ready::Ok(None)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<'f, T: Service<Publish>, C: Service<ControlMessage<E>>, E>
    where T: 'f, C: 'f, E: 'f
    {
        #[pin]
        state: PublishResponseState<'f, T, C, E>,
        packet_id: Option<NonZeroU16>,
        inner: &'f Inner<C>,
    }
}

pin_project_lite::pin_project! {
    #[project = PublishResponseStateProject]
    enum PublishResponseState<'f, T: Service<Publish>, C: Service<ControlMessage<E>>, E>
    where T: 'f, C: 'f, E: 'f
    {
        Publish { #[pin] fut: T::Future<'f> },
        Control { #[pin] fut: ControlResponse<'f, C, E> },
    }
}

impl<'f, T, C, E> Future for PublishResponse<'f, T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = ()>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish { fut } => match fut.poll(cx) {
                Poll::Ready(Ok(_)) => {
                    log::trace!("Publish result for packet {:?} is ready", this.packet_id);

                    if let Some(packet_id) = this.packet_id {
                        this.inner.inflight.borrow_mut().remove(packet_id);
                        Poll::Ready(Ok(Some(codec::Packet::PublishAck {
                            packet_id: *packet_id,
                        })))
                    } else {
                        Poll::Ready(Ok(None))
                    }
                }
                Poll::Ready(Err(e)) => {
                    this.state.set(PublishResponseState::Control {
                        fut: ControlResponse::new(ControlMessage::error(e.into()), this.inner),
                    });
                    self.poll(cx)
                }
                Poll::Pending => Poll::Pending,
            },
            PublishResponseStateProject::Control { fut } => fut.poll(cx),
        }
    }
}

pin_project_lite::pin_project! {
    /// Control service response future
    pub(crate) struct ControlResponse<'f, C: Service<ControlMessage<E>>, E>
    where C: 'f, E: 'f
    {
        #[pin]
        fut: C::Future<'f>,
        inner: &'f Inner<C>,
        error: bool,
        _t: PhantomData<E>,
    }
}

impl<'f, C, E> ControlResponse<'f, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(pkt: ControlMessage<E>, inner: &'f Inner<C>) -> Self {
        let error = matches!(pkt, ControlMessage::Error(_) | ControlMessage::ProtocolError(_));
        Self { error, inner, fut: inner.control.call(pkt), _t: PhantomData }
    }
}

impl<'f, C, E> Future for ControlResponse<'f, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        match this.fut.poll(cx) {
            Poll::Ready(Ok(item)) => {
                let packet = match item.result {
                    ControlResultKind::Ping => Some(codec::Packet::PingResponse),
                    ControlResultKind::Subscribe(res) => {
                        this.inner.inflight.borrow_mut().remove(&res.packet_id);
                        Some(codec::Packet::SubscribeAck {
                            status: res.codes,
                            packet_id: res.packet_id,
                        })
                    }
                    ControlResultKind::Unsubscribe(res) => {
                        this.inner.inflight.borrow_mut().remove(&res.packet_id);
                        Some(codec::Packet::UnsubscribeAck { packet_id: res.packet_id })
                    }
                    ControlResultKind::Disconnect
                    | ControlResultKind::Closed
                    | ControlResultKind::Nothing => {
                        this.inner.sink.close();
                        None
                    }
                    ControlResultKind::PublishAck(_) => unreachable!(),
                };
                Poll::Ready(Ok(packet))
            }
            Poll::Ready(Err(err)) => {
                // do not handle nested error
                if *this.error {
                    Poll::Ready(Err(err))
                } else {
                    // handle error from control service
                    match err {
                        MqttError::Service(err) => {
                            *this.error = true;
                            let fut = this.inner.control.call(ControlMessage::error(err));
                            self.as_mut().project().fut.set(fut);
                            self.poll(cx)
                        }
                        _ => Poll::Ready(Err(err)),
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
