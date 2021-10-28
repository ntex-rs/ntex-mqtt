use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::{
    buffer::BufferService, inflight::InFlightService, join, Either, HashSet, Ready,
};

use crate::error::{MqttError, ProtocolError};
use crate::io::DispatchItem;

use super::control::{
    ControlMessage, ControlResult, ControlResultKind, Subscribe, Unsubscribe,
};
use super::shared::MqttShared;
use super::{codec, publish::Publish, shared::Ack, sink::MqttSink, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    inflight: usize,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = DispatchItem<Rc<MqttShared>>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    E: 'static,
    St: 'static,
    T: ServiceFactory<
            Config = Session<St>,
            Request = Publish,
            Response = (),
            Error = E,
            InitError = MqttError<E>,
        > + 'static,
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage<E>,
            Response = ControlResult,
            Error = E,
            InitError = MqttError<E>,
        > + 'static,
{
    fn_factory_with_config(move |cfg: Session<St>| {
        // create services
        let fut = join(publish.new_service(cfg.clone()), control.new_service(cfg.clone()));

        async move {
            let (publish, control) = fut.await;

            let control = BufferService::new(
                16,
                || MqttError::<C::Error>::Disconnected,
                // limit number of in-flight messages
                InFlightService::new(1, control?.map_err(MqttError::Service)),
            );

            Ok(
                // limit number of in-flight messages
                InFlightService::new(
                    inflight,
                    Dispatcher::<_, _, _, E>::new(cfg, publish?, control),
                ),
            )
        }
    })
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C, E> {
    session: Session<St>,
    publish: T,
    shutdown: Cell<bool>,
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
    T: Service<Request = Publish, Response = (), Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(session: Session<St>, publish: T, control: C) -> Self {
        let sink = session.sink().clone();

        Self {
            session,
            publish,
            shutdown: Cell::new(false),
            inner: Rc::new(Inner { sink, control, inflight: RefCell::new(HashSet::default()) }),
            _t: PhantomData,
        }
    }
}

impl<St, T, C, E> Service for Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = (), Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
    C::Future: 'static,
    E: 'static,
{
    type Request = DispatchItem<Rc<MqttShared>>;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(MqttError::Service)?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.inner.sink.close();
            self.shutdown.set(true);
            let fut = self.inner.control.call(ControlMessage::closed(is_error));
            ntex::rt::spawn(async move {
                let _ = fut.await;
            });
        }
        Poll::Ready(())
    }

    fn call(&self, req: DispatchItem<Rc<MqttShared>>) -> Self::Future {
        log::trace!("Dispatch v3 packet: {:#?}", req);

        match req {
            DispatchItem::Item(codec::Packet::Publish(publish)) => {
                let inner = self.inner.clone();
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
            DispatchItem::IoError(err) => Either::Right(Either::Right(ControlResponse::new(
                ControlMessage::proto_error(ProtocolError::Io(err)),
                &self.inner,
            ))),
            DispatchItem::WBackPressureEnabled | DispatchItem::WBackPressureDisabled => {
                Either::Right(Either::Left(Ready::Ok(None)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T: Service, C: Service, E> {
        #[pin]
        state: PublishResponseState<T, C, E>,
        packet_id: Option<NonZeroU16>,
        inner: Rc<Inner<C>>,
    }
}

pin_project_lite::pin_project! {
    #[project = PublishResponseStateProject]
    enum PublishResponseState<T: Service, C: Service, E> {
        Publish { #[pin] fut: T::Future },
        Control { #[pin] fut: ControlResponse<C, E> },
    }
}

impl<T, C, E> Future for PublishResponse<T, C, E>
where
    T: Service<Request = Publish, Response = (), Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
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
                        fut: ControlResponse::new(ControlMessage::error(e), this.inner),
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
    pub(crate) struct ControlResponse<C: Service, E>
    {
        #[pin]
        fut: C::Future,
        inner: Rc<Inner<C>>,
        error: bool,
        _t: PhantomData<E>,
    }
}

impl<C: Service, E> ControlResponse<C, E>
where
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    #[allow(clippy::match_like_matches_macro)]
    fn new(pkt: ControlMessage<E>, inner: &Rc<Inner<C>>) -> Self {
        let error = match pkt {
            ControlMessage::Error(_) | ControlMessage::ProtocolError(_) => true,
            _ => false,
        };

        Self { error, fut: inner.control.call(pkt), inner: inner.clone(), _t: PhantomData }
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
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
