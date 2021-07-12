use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::{inflight::InFlightService, join, Either, HashSet, Ready};

use crate::error::MqttError;

use super::control::{
    ControlMessage, ControlResult, ControlResultKind, Subscribe, Unsubscribe,
};
use super::{codec, publish::Publish, shared::Ack, sink::MqttSink, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    inflight: usize,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = codec::Packet,
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
            Error = MqttError<E>,
            InitError = MqttError<E>,
        > + 'static,
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage,
            Response = ControlResult,
            Error = MqttError<E>,
            InitError = MqttError<E>,
        > + 'static,
{
    fn_factory_with_config(move |cfg: Session<St>| {
        // create services
        let fut = join(publish.new_service(cfg.clone()), control.new_service(cfg.clone()));

        async move {
            let (publish, control) = fut.await;

            Ok(
                // limit number of in-flight messages
                InFlightService::new(
                    inflight,
                    Dispatcher::<_, _, _, E>::new(cfg, publish?, control?),
                ),
            )
        }
    })
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T: Service<Error = MqttError<E>>, C, E> {
    session: Session<St>,
    publish: T,
    control: C,
    shutdown: Cell<bool>,
    inner: Rc<Inner>,
}

struct Inner {
    sink: MqttSink,
    inflight: RefCell<HashSet<NonZeroU16>>,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = (), Error = MqttError<E>>,
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(session: Session<St>, publish: T, control: C) -> Self {
        let sink = session.sink().clone();

        Self {
            session,
            publish,
            control,
            shutdown: Cell::new(false),
            inner: Rc::new(Inner { sink, inflight: RefCell::new(HashSet::default()) }),
        }
    }
}

impl<St, T, C, E> Service for Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = (), Error = MqttError<E>>,
    C: Service<Request = ControlMessage, Response = ControlResult, Error = MqttError<E>>,
    C::Future: 'static,
    E: 'static,
{
    type Request = codec::Packet;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T::Future, MqttError<E>>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<C::Future, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx)?;
        let res2 = self.control.poll_ready(cx)?;

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
            let fut = self.control.call(ControlMessage::closed(is_error));
            ntex::rt::spawn(async move {
                let _ = fut.await;
            });
        }
        Poll::Ready(())
    }

    fn call(&self, packet: codec::Packet) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", packet);
        match packet {
            codec::Packet::Publish(publish) => {
                let inner = self.inner.clone();
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    if !inner.inflight.borrow_mut().insert(pid) {
                        log::trace!("Duplicated packet id for publish packet: {:?}", pid);
                        return Either::Right(Either::Left(Ready::Err(
                            MqttError::ServerError("Duplicated packet id for publish packet"),
                        )));
                    }
                }
                Either::Left(PublishResponse {
                    packet_id,
                    inner,
                    fut: self.publish.call(Publish::new(publish)),
                    _t: PhantomData,
                })
            }
            codec::Packet::PublishAck { packet_id } => {
                if let Err(e) = self.session.sink().pkt_ack(Ack::Publish(packet_id)) {
                    Either::Right(Either::Left(Ready::Err(MqttError::Protocol(e))))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            codec::Packet::PingRequest => Either::Right(Either::Right(ControlResponse::new(
                self.control.call(ControlMessage::ping()),
                &self.inner,
            ))),
            codec::Packet::Disconnect => Either::Right(Either::Right(ControlResponse::new(
                self.control.call(ControlMessage::pkt_disconnect()),
                &self.inner,
            ))),
            codec::Packet::Subscribe { packet_id, topic_filters } => {
                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!("Duplicated packet id for unsubscribe packet: {:?}", packet_id);
                    return Either::Right(Either::Left(Ready::Err(MqttError::ServerError(
                        "Duplicated packet id for unsubscribe packet",
                    ))));
                }

                Either::Right(Either::Right(ControlResponse::new(
                    self.control.call(ControlMessage::Subscribe(Subscribe::new(
                        packet_id,
                        topic_filters,
                    ))),
                    &self.inner,
                )))
            }
            codec::Packet::Unsubscribe { packet_id, topic_filters } => {
                if !self.inner.inflight.borrow_mut().insert(packet_id) {
                    log::trace!("Duplicated packet id for unsubscribe packet: {:?}", packet_id);
                    return Either::Right(Either::Left(Ready::Err(MqttError::ServerError(
                        "Duplicated packet id for unsubscribe packet",
                    ))));
                }

                Either::Right(Either::Right(ControlResponse::new(
                    self.control.call(ControlMessage::Unsubscribe(Unsubscribe::new(
                        packet_id,
                        topic_filters,
                    ))),
                    &self.inner,
                )))
            }
            _ => Either::Right(Either::Left(Ready::Ok(None))),
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T, E> {
        #[pin]
        fut: T,
        packet_id: Option<NonZeroU16>,
        inner: Rc<Inner>,
        _t: PhantomData<E>,
    }
}

impl<T, E> Future for PublishResponse<T, E>
where
    T: Future<Output = Result<(), E>>,
{
    type Output = Result<Option<codec::Packet>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.fut.poll(cx) {
            Poll::Ready(result) => result?,
            Poll::Pending => return Poll::Pending,
        };

        log::trace!("Publish result for packet {:?} is ready", this.packet_id);

        if let Some(packet_id) = this.packet_id {
            this.inner.inflight.borrow_mut().remove(packet_id);
            Poll::Ready(Ok(Some(codec::Packet::PublishAck { packet_id: *packet_id })))
        } else {
            Poll::Ready(Ok(None))
        }
    }
}

pin_project_lite::pin_project! {
    /// Control service response future
    pub(crate) struct ControlResponse<T, E>
    where
        T: Future<Output = Result<ControlResult, MqttError<E>>>,
    {
        #[pin]
        fut: T,
        inner: Rc<Inner>,
    }
}

impl<T, E> ControlResponse<T, E>
where
    T: Future<Output = Result<ControlResult, MqttError<E>>>,
{
    fn new(fut: T, inner: &Rc<Inner>) -> Self {
        Self { fut, inner: inner.clone() }
    }
}

impl<T, E> Future for ControlResponse<T, E>
where
    T: Future<Output = Result<ControlResult, MqttError<E>>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let packet = match this.fut.poll(cx)? {
            Poll::Ready(item) => match item.result {
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
            },
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(Ok(packet))
    }
}
