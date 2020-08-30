use std::cell::{Cell, RefCell};
use std::convert::TryFrom;
use std::task::{Context, Poll};
use std::{future::Future, io, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use futures::future::{join, ok, Either, FutureExt, Ready};
use futures::ready;
use fxhash::FxHashSet;
use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::inflight::InFlightService;
use ntex::util::order::{InOrder, InOrderError};

use crate::error::{MqttError, ProtocolError};
use crate::framed::DispatcherItem;

use super::control::{self, ControlPacket, ControlResult};
use super::publish::{Publish, PublishAck};
use super::sink::{Ack, MqttSink};
use super::{codec, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    max_receive: u16,
    max_topic_alias: u16,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = DispatcherItem<codec::Codec>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    E: From<T::Error> + 'static,
    St: 'static,
    T: ServiceFactory<
            Config = Session<St>,
            Request = Publish,
            Response = PublishAck,
            InitError = MqttError<E>,
        > + 'static,
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlPacket<E>,
            Response = ControlResult,
            Error = E,
            InitError = MqttError<E>,
        > + 'static,
    PublishAck: TryFrom<T::Error, Error = E>,
{
    fn_factory_with_config(move |cfg: Session<St>| {
        // create services
        let fut = join(publish.new_service(cfg.clone()), control.new_service(cfg.clone()));

        async move {
            let (publish, control) = fut.await;

            // mqtt dispatcher.
            // mqtt spec requires ack ordering, so enforce response ordering
            Ok(Dispatcher::<_, _, _, E, T::Error>::new(
                cfg,
                max_receive as usize,
                max_topic_alias,
                InOrder::service(publish?).map_err(|e| match e {
                    InOrderError::Service(e) => either::Either::Left(e),
                    InOrderError::Disconnected => either::Either::Right(ProtocolError::Io(
                        io::Error::new(io::ErrorKind::Other, "Service dropped"),
                    )),
                }),
                // limit number of in-flight control messages
                InFlightService::new(
                    16,
                    InOrder::service(control?).map_err(|e| match e {
                        InOrderError::Service(e) => either::Either::Left(e),
                        InOrderError::Disconnected => either::Either::Right(ProtocolError::Io(
                            io::Error::new(io::ErrorKind::Other, "Service dropped"),
                        )),
                    }),
                ),
            ))
        }
    })
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T, C, E, E2> {
    session: Session<St>,
    publish: T,
    shutdown: Cell<bool>,
    max_receive: usize,
    max_topic_alias: u16,
    inner: Rc<Inner<C>>,
    _t: PhantomData<(E, E2)>,
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

impl<St, T, C, E, E2> Dispatcher<St, T, C, E, E2>
where
    T: Service<
        Request = Publish,
        Response = PublishAck,
        Error = either::Either<E2, ProtocolError>,
    >,
    PublishAck: TryFrom<E2, Error = E>,
    C: Service<
        Request = ControlPacket<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
{
    fn new(
        session: Session<St>,
        max_receive: usize,
        max_topic_alias: u16,
        publish: T,
        control: C,
    ) -> Self {
        let sink = session.sink().clone();
        Self {
            session,
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

impl<St, T, C, E, E2> Service for Dispatcher<St, T, C, E, E2>
where
    T: Service<
        Request = Publish,
        Response = PublishAck,
        Error = either::Either<E2, ProtocolError>,
    >,
    PublishAck: TryFrom<E2, Error = E>,
    C: Service<
        Request = ControlPacket<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
    C::Future: 'static,
    E: From<E2> + 'static,
{
    type Request = DispatcherItem<codec::Codec>;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E, E2>,
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
                self.inner.control.call(ControlPacket::closed(is_error)).map(|_| ()),
            );
        }
        Poll::Ready(())
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", request);

        match request {
            DispatcherItem::Item(codec::Packet::Publish(publish)) => {
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
                                    ControlPacket::proto_error(
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
                                        ControlPacket::proto_error(
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
                                        ControlPacket::proto_error(
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
            DispatcherItem::Item(codec::Packet::PublishAck(packet)) => {
                if let Err(err) = self.session.sink().pkt_ack(Ack::Publish(packet)) {
                    Either::Right(Either::Right(
                        ControlResponse::new(ControlPacket::proto_error(err), &self.inner)
                            .error(),
                    ))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            DispatcherItem::Item(codec::Packet::Auth(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(ControlPacket::auth(pkt), &self.inner),
            )),
            DispatcherItem::Item(codec::Packet::PingRequest) => Either::Right(Either::Right(
                ControlResponse::new(ControlPacket::ping(), &self.inner),
            )),
            DispatcherItem::Item(codec::Packet::Disconnect(pkt)) => Either::Right(
                Either::Right(ControlResponse::new(ControlPacket::dis(pkt), &self.inner)),
            ),
            DispatcherItem::Item(codec::Packet::Subscribe(pkt)) => {
                // register inflight packet id
                if !self.inner.info.borrow_mut().inflight.insert(pkt.packet_id) {
                    // duplicated packet id
                    return Either::Right(Either::Left(ok(Some(codec::Packet::SubscribeAck(
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
                    )))));
                }
                let id = pkt.packet_id;
                Either::Right(Either::Right(
                    ControlResponse::new(control::Subscribe::create(pkt), &self.inner)
                        .packet_id(id),
                ))
            }
            DispatcherItem::Item(codec::Packet::Unsubscribe(pkt)) => {
                // register inflight packet id
                if !self.inner.info.borrow_mut().inflight.insert(pkt.packet_id) {
                    // duplicated packet id
                    return Either::Right(Either::Left(ok(Some(
                        codec::Packet::UnsubscribeAck(codec::UnsubscribeAck {
                            packet_id: pkt.packet_id,
                            status: pkt
                                .topic_filters
                                .iter()
                                .map(|_| codec::UnsubscribeAckReason::PacketIdentifierInUse)
                                .collect(),
                            properties: codec::UserProperties::new(),
                            reason_string: None,
                        }),
                    ))));
                }
                let id = pkt.packet_id;
                Either::Right(Either::Right(
                    ControlResponse::new(control::Unsubscribe::create(pkt), &self.inner)
                        .packet_id(id),
                ))
            }
            DispatcherItem::Item(_) => Either::Right(Either::Left(ok(None))),
            DispatcherItem::EncoderError(idx, err) => {
                if idx == 0 {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlPacket::proto_error(ProtocolError::Encode(err)),
                        &self.inner,
                    )))
                } else {
                    self.session.sink().pkt_encode_err(idx, err);
                    Either::Right(Either::Left(ok(None)))
                }
            }
            DispatcherItem::ItemEncoded(idx) => {
                self.session.sink().pkt_written(idx);
                Either::Right(Either::Left(ok(None)))
            }
            DispatcherItem::KeepAliveTimeout => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlPacket::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                )))
            }
            DispatcherItem::DecoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlPacket::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                )))
            }
            DispatcherItem::IoError(err) => Either::Right(Either::Right(ControlResponse::new(
                ControlPacket::proto_error(ProtocolError::Io(err)),
                &self.inner,
            ))),
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T: Service, C: Service, E, E2> {
        #[pin]
        state: PublishResponseState<T, C, E>,
        packet_id: u16,
        inner: Rc<Inner<C>>,
        _t: PhantomData<(E, E2)>,
    }
}

#[pin_project::pin_project(project = PublishResponseStateProject)]
enum PublishResponseState<T: Service, C: Service, E> {
    Publish(#[pin] T::Future),
    Control(#[pin] ControlResponse<C, E>),
}

impl<T, C, E, E2> Future for PublishResponse<T, C, E, E2>
where
    E: From<E2>,
    T: Service<
        Request = Publish,
        Response = PublishAck,
        Error = either::Either<E2, ProtocolError>,
    >,
    PublishAck: TryFrom<E2, Error = E>,
    C: Service<
        Request = ControlPacket<E>,
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
                    Ok(ack) => ack,
                    Err(e) => match e {
                        either::Either::Left(e) => {
                            if *this.packet_id != 0 {
                                match PublishAck::try_from(e) {
                                    Ok(ack) => ack,
                                    Err(e) => {
                                        this.state.set(PublishResponseState::Control(
                                            ControlResponse::new(
                                                ControlPacket::error(e),
                                                this.inner,
                                            )
                                            .error(),
                                        ));
                                        return self.poll(cx);
                                    }
                                }
                            } else {
                                this.state.set(PublishResponseState::Control(
                                    ControlResponse::new(
                                        ControlPacket::error(e.into()),
                                        this.inner,
                                    )
                                    .error(),
                                ));
                                return self.poll(cx);
                            }
                        }
                        either::Either::Right(e) => {
                            this.state.set(PublishResponseState::Control(
                                ControlResponse::new(ControlPacket::proto_error(e), this.inner)
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
        Request = ControlPacket<E>,
        Response = ControlResult,
        Error = either::Either<E, ProtocolError>,
    >,
{
    fn new(pkt: ControlPacket<E>, inner: &Rc<Inner<C>>) -> Self {
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
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<
        Request = ControlPacket<E>,
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
                            this.inner.control.call(ControlPacket::error(err))
                        }
                        either::Either::Right(err) => {
                            this.inner.control.call(ControlPacket::proto_error(err))
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
