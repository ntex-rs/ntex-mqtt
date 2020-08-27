use std::cell::{Cell, RefCell};
use std::convert::TryFrom;
use std::task::{Context, Poll};
use std::{future::Future, io, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use futures::future::{join, ok, Either, FutureExt, Ready};
use futures::ready;
use fxhash::FxHashSet;
use ntex::service::Service;
use ntex::util::inflight::InFlightService;
use ntex::util::order::{InOrder, InOrderError};

use crate::error::{MqttError, ProtocolError};
use crate::framed::DispatcherError;
use crate::types::packet_type;
use crate::v5::codec;
use crate::v5::publish::{Publish, PublishAck};

use super::control::{self, ControlMessage, ControlResult};
use super::sink::MqttSink;

/// mqtt5 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: MqttSink,
    max_receive: usize,
    max_topic_alias: u16,
    publish: T,
    control: C,
) -> impl Service<
    Request = Result<codec::Packet, DispatcherError<codec::Codec>>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
>
where
    E: From<T::Error> + 'static,
    T: Service<Request = Publish, Response = either::Either<Publish, PublishAck>, Error = E>
        + 'static,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E> + 'static,
{
    // mqtt dispatcher.
    // mqtt spec requires ack ordering, so enforce response ordering
    Dispatcher::<_, _, E>::new(
        sink,
        max_receive as usize,
        max_topic_alias,
        InOrder::service(publish).map_err(|e| match e {
            InOrderError::Service(e) => either::Either::Left(e),
            InOrderError::Disconnected => either::Either::Right(ProtocolError::Io(
                io::Error::new(io::ErrorKind::Other, "Service dropped"),
            )),
        }),
        // limit number of in-flight control messages
        InFlightService::new(
            16,
            InOrder::service(control).map_err(|e| match e {
                InOrderError::Service(e) => either::Either::Left(e),
                InOrderError::Disconnected => either::Either::Right(ProtocolError::Io(
                    io::Error::new(io::ErrorKind::Other, "Service dropped"),
                )),
            }),
        ),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C, E> {
    publish: T,
    shutdown: Cell<bool>,
    max_receive: usize,
    max_topic_alias: u16,
    inner: Rc<Inner<C>>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: C,
    sink: MqttSink,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: FxHashSet<NonZeroU16>,
    aliases: FxHashSet<NonZeroU16>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<
        Request = Publish,
        Response = either::Either<Publish, PublishAck>,
        Error = either::Either<E, ProtocolError>,
    >,
    C: Service<
        Request = ControlMessage<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
{
    fn new(
        sink: MqttSink,
        max_receive: usize,
        max_topic_alias: u16,
        publish: T,
        control: C,
    ) -> Self {
        Self {
            publish,
            max_receive,
            max_topic_alias,
            shutdown: Cell::new(false),
            inner: Rc::new(Inner {
                control,
                sink,
                info: RefCell::new(PublishInfo {
                    aliases: FxHashSet::default(),
                    inflight: FxHashSet::default(),
                }),
            }),
            _t: PhantomData,
        }
    }
}

impl<T, C, E> Service for Dispatcher<T, C, E>
where
    T: Service<
        Request = Publish,
        Response = either::Either<Publish, PublishAck>,
        Error = either::Either<E, ProtocolError>,
    >,
    C: Service<
        Request = ControlMessage<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
    C::Future: 'static,
{
    type Request = Result<codec::Packet, DispatcherError<codec::Codec>>;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E>,
        Either<Ready<Result<Self::Response, MqttError<E>>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(|e| match e {
            either::Either::Left(e) => MqttError::Service(e.into()),
            either::Either::Right(e) => MqttError::Protocol(e),
        })?;
        let res2 = self.inner.control.poll_ready(cx).map_err(MqttError::from)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.shutdown.set(true);
            ntex::rt::spawn(
                self.inner.control.call(ControlMessage::closed(is_error)).map(|_| ()),
            );
        }
        Poll::Ready(())
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", request);

        match request {
            Ok(codec::Packet::Publish(publish)) => {
                let info = self.inner.clone();
                let packet_id = publish.packet_id;

                {
                    let mut inner = info.info.borrow_mut();

                    if let Some(pid) = packet_id {
                        // check for receive maximum
                        if self.max_receive != 0 && inner.inflight.len() >= self.max_receive {
                            log::trace!(
                                "Receive maximum exceeded: max: {} inflight: {}",
                                self.max_receive,
                                inner.inflight.len()
                            );
                            return Either::Right(Either::Right(
                                ControlResponse::new(
                                    ControlMessage::proto_error(
                                        ProtocolError::ReceiveMaximumExceeded,
                                    ),
                                    &self.inner,
                                )
                                .error(),
                            ));
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            return Either::Right(Either::Left(ok(Some(
                                codec::Packet::PublishAck(codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                }),
                            ))));
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        // check existing topic
                        if publish.topic.is_empty() {
                            if !inner.aliases.contains(&alias) {
                                return Either::Right(Either::Right(
                                    ControlResponse::new(
                                        ControlMessage::proto_error(
                                            ProtocolError::UnknownTopicAlias,
                                        ),
                                        &self.inner,
                                    )
                                    .error(),
                                ));
                            }
                        } else {
                            if alias.get() > self.max_topic_alias {
                                return Either::Right(Either::Right(
                                    ControlResponse::new(
                                        ControlMessage::proto_error(
                                            ProtocolError::MaxTopicAlias,
                                        ),
                                        &self.inner,
                                    )
                                    .error(),
                                ));
                            }

                            // record new alias
                            inner.aliases.insert(alias);
                        }
                    }
                }

                Either::Left(PublishResponse {
                    packet_id: packet_id.map(|v| v.get()).unwrap_or(0),
                    inner: info,
                    state: PublishResponseState::Publish(
                        self.publish.call(Publish::new(publish)),
                    ),
                    _t: PhantomData,
                })
            }
            Ok(codec::Packet::PublishAck(packet)) => {
                if !self.inner.sink.complete_publish_qos1(packet) {
                    Either::Right(Either::Right(
                        ControlResponse::new(
                            ControlMessage::proto_error(ProtocolError::PacketIdMismatch),
                            &self.inner,
                        )
                        .error(),
                    ))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            Ok(codec::Packet::PingRequest) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::ping(), &self.inner),
            )),
            Ok(codec::Packet::Disconnect(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::dis(pkt), &self.inner),
            )),
            Ok(codec::Packet::Auth(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Unexpected(
                        packet_type::AUTH,
                        "Auth packet is not supported",
                    )),
                    &self.inner,
                )
                .error(),
            )),
            Ok(codec::Packet::Subscribe(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Unexpected(
                        packet_type::SUBSCRIBE,
                        "Subscribe packet is not supported",
                    )),
                    &self.inner,
                )
                .error(),
            )),
            Ok(codec::Packet::Unsubscribe(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Unexpected(
                        packet_type::UNSUBSCRIBE,
                        "Unsubscribe packet is not supported",
                    )),
                    &self.inner,
                )
                .error(),
            )),
            Ok(_) => Either::Right(Either::Left(ok(None))),
            Err(e) => Either::Right(Either::Right(ControlResponse::new(
                ControlMessage::proto_error(e.into()),
                &self.inner,
            ))),
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T: Service, C: Service, E> {
        #[pin]
        state: PublishResponseState<T, C, E>,
        packet_id: u16,
        inner: Rc<Inner<C>>,
        _t: PhantomData<E>,
    }
}

#[pin_project::pin_project(project = PublishResponseStateProject)]
enum PublishResponseState<T: Service, C: Service, E> {
    Publish(#[pin] T::Future),
    Control(#[pin] ControlResponse<C, E>),
}

impl<T, C, E> Future for PublishResponse<T, C, E>
where
    T: Service<
        Request = Publish,
        Response = either::Either<Publish, PublishAck>,
        Error = either::Either<E, ProtocolError>,
    >,
    C: Service<
        Request = ControlMessage<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish(fut) => {
                let ack = match ready!(fut.poll(cx)) {
                    Ok(res) => match res {
                        either::Either::Right(ack) => ack,
                        either::Either::Left(pkt) => {
                            this.state.set(PublishResponseState::Control(
                                ControlResponse::new(
                                    ControlMessage::publish(pkt.into_inner()),
                                    this.inner,
                                )
                                .set_packet_id(*this.packet_id),
                            ));
                            return self.poll(cx);
                        }
                    },
                    Err(e) => match e {
                        either::Either::Left(e) => {
                            if *this.packet_id != 0 {
                                this.state.set(PublishResponseState::Control(
                                    ControlResponse::new(ControlMessage::error(e), this.inner)
                                        .error(),
                                ));
                                return self.poll(cx);
                            } else {
                                this.state.set(PublishResponseState::Control(
                                    ControlResponse::new(
                                        ControlMessage::error(e.into()),
                                        this.inner,
                                    )
                                    .error(),
                                ));
                                return self.poll(cx);
                            }
                        }
                        either::Either::Right(e) => {
                            this.state.set(PublishResponseState::Control(
                                ControlResponse::new(
                                    ControlMessage::proto_error(e),
                                    this.inner,
                                )
                                .error(),
                            ));
                            return self.poll(cx);
                        }
                    },
                };
                if let Some(id) = NonZeroU16::new(*this.packet_id) {
                    this.inner.info.borrow_mut().inflight.remove(&id);
                    let ack = codec::PublishAck {
                        packet_id: id,
                        reason_code: ack.reason_code,
                        reason_string: ack.reason_string,
                        properties: ack.properties,
                    };
                    Poll::Ready(Ok(Some(codec::Packet::PublishAck(ack))))
                } else {
                    Poll::Ready(Ok(None))
                }
            }
            PublishResponseStateProject::Control(fut) => fut.poll(cx),
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
        packet_id: u16,
        _t: PhantomData<E>,
    }
}

impl<C: Service, E> ControlResponse<C, E>
where
    C: Service<
        Request = ControlMessage<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
{
    fn new(pkt: ControlMessage<E>, inner: &Rc<Inner<C>>) -> Self {
        Self {
            fut: inner.control.call(pkt),
            inner: inner.clone(),
            packet_id: 0,
            error: false,
            _t: PhantomData,
        }
    }

    fn error(mut self) -> Self {
        self.error = true;
        self
    }

    fn packet_id(mut self, id: NonZeroU16) -> Self {
        self.packet_id = id.get();
        self
    }

    fn set_packet_id(mut self, id: u16) -> Self {
        self.packet_id = id;
        self
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<
        Request = ControlMessage<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        let result = match ready!(this.fut.poll(cx)) {
            Ok(result) => {
                if let Some(id) = NonZeroU16::new(self.packet_id) {
                    self.inner.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                // do not handle nested error
                if *this.error {
                    return Poll::Ready(Err(MqttError::from(err)));
                } else {
                    // handle error from control service
                    *this.error = true;
                    let fut = match err {
                        either::Either::Left(err) => {
                            this.inner.control.call(ControlMessage::error(err))
                        }
                        either::Either::Right(err) => {
                            this.inner.control.call(ControlMessage::proto_error(err))
                        }
                    };
                    self.as_mut().project().fut.set(fut);
                    return self.poll(cx);
                }
            }
        };

        if result.disconnect {
            self.inner.sink.drop_sink();
        }

        Poll::Ready(Ok(result.packet))
    }
}
