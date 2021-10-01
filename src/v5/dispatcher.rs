use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};
use std::{convert::TryFrom, future::Future, marker, num, pin::Pin, rc::Rc};

use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::{
    buffer::BufferService, inflight::InFlightService, join, Either, HashSet, Ready,
};

use crate::error::{MqttError, ProtocolError};
use crate::io::DispatchItem;

use super::control::{self, ControlMessage, ControlResult};
use super::publish::{Publish, PublishAck};
use super::shared::{Ack, MqttShared};
use super::sink::MqttSink;
use super::{codec, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = DispatchItem<Rc<MqttShared>>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    St: 'static,
    E: From<T::Error> + 'static,
    T: ServiceFactory<
            Config = Session<St>,
            Request = Publish,
            Response = PublishAck,
            InitError = MqttError<E>,
        > + 'static,
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage<E>,
            Response = ControlResult,
            Error = E,
            InitError = MqttError<E>,
        > + 'static,
    PublishAck: TryFrom<T::Error, Error = E>,
{
    fn_factory_with_config(move |cfg: Session<St>| {
        // create services
        let fut = join(publish.new_service(cfg.clone()), control.new_service(cfg.clone()));

        let (max_receive, max_topic_alias) = cfg.params();

        async move {
            let (publish, control) = fut.await;

            let control = BufferService::new(
                16,
                || MqttError::<C::Error>::Disconnected,
                // limit number of in-flight messages
                InFlightService::new(1, control?.map_err(MqttError::Service)),
            );

            Ok(Dispatcher::<_, _, E, T::Error>::new(
                cfg.sink().clone(),
                max_receive as usize,
                max_topic_alias,
                publish?,
                control,
            ))
        }
    })
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C, E, E2> {
    sink: MqttSink,
    publish: T,
    shutdown: Cell<bool>,
    max_receive: usize,
    max_topic_alias: u16,
    inner: Rc<Inner<C>>,
    _t: marker::PhantomData<(E, E2)>,
}

struct Inner<C> {
    control: C,
    sink: MqttSink,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<num::NonZeroU16>,
    aliases: HashSet<num::NonZeroU16>,
}

impl<T, C, E, E2> Dispatcher<T, C, E, E2>
where
    T: Service<Request = Publish, Response = PublishAck, Error = E2>,
    PublishAck: TryFrom<E2, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
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
            sink: sink.clone(),
            shutdown: Cell::new(false),
            inner: Rc::new(Inner {
                control,
                sink,
                info: RefCell::new(PublishInfo {
                    aliases: HashSet::default(),
                    inflight: HashSet::default(),
                }),
            }),
            _t: marker::PhantomData,
        }
    }
}

impl<T, C, E, E2> Service for Dispatcher<T, C, E, E2>
where
    T: Service<Request = Publish, Response = PublishAck, Error = E2>,
    PublishAck: TryFrom<E2, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
    C::Future: 'static,
    E: From<E2> + 'static,
{
    type Request = DispatchItem<Rc<MqttShared>>;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E, E2>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(|e| MqttError::Service(e.into()))?;
        let res2 = self.inner.control.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.inner.sink.drop_sink();
            self.shutdown.set(true);
            let fut = self.inner.control.call(ControlMessage::closed(is_error));
            ntex::rt::spawn(async move {
                let _ = fut.await;
            });
        }
        Poll::Ready(())
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        log::trace!("Dispatch v5 packet: {:#?}", request);

        match request {
            DispatchItem::Item(codec::Packet::Publish(publish)) => {
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
                            return Either::Right(Either::Right(ControlResponse::new(
                                ControlMessage::proto_error(
                                    ProtocolError::ReceiveMaximumExceeded,
                                ),
                                &self.inner,
                            )));
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            self.sink.send(codec::Packet::PublishAck(codec::PublishAck {
                                packet_id: pid,
                                reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                ..Default::default()
                            }));
                            return Either::Right(Either::Left(Ready::Ok(None)));
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        // check existing topic
                        if publish.topic.is_empty() {
                            if !inner.aliases.contains(&alias) {
                                return Either::Right(Either::Right(ControlResponse::new(
                                    ControlMessage::proto_error(
                                        ProtocolError::UnknownTopicAlias,
                                    ),
                                    &self.inner,
                                )));
                            }
                        } else {
                            if alias.get() > self.max_topic_alias {
                                return Either::Right(Either::Right(ControlResponse::new(
                                    ControlMessage::proto_error(ProtocolError::MaxTopicAlias),
                                    &self.inner,
                                )));
                            }

                            // record new alias
                            inner.aliases.insert(alias);
                        }
                    }
                }

                Either::Left(PublishResponse {
                    packet_id: packet_id.map(|v| v.get()).unwrap_or(0),
                    inner: info,
                    state: PublishResponseState::Publish {
                        fut: self.publish.call(Publish::new(publish)),
                    },
                    _t: marker::PhantomData,
                })
            }
            DispatchItem::Item(codec::Packet::PublishAck(packet)) => {
                if let Err(err) = self.sink.pkt_ack(Ack::Publish(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                    )))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item(codec::Packet::Auth(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::auth(pkt), &self.inner),
            )),
            DispatchItem::Item(codec::Packet::PingRequest) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::ping(), &self.inner),
            )),
            DispatchItem::Item(codec::Packet::Disconnect(pkt)) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::dis(pkt), &self.inner),
            )),
            DispatchItem::Item(codec::Packet::Subscribe(pkt)) => {
                // register inflight packet id
                if !self.inner.info.borrow_mut().inflight.insert(pkt.packet_id) {
                    // duplicated packet id
                    self.sink.send(codec::Packet::SubscribeAck(codec::SubscribeAck {
                        packet_id: pkt.packet_id,
                        status: pkt
                            .topic_filters
                            .iter()
                            .map(|_| codec::SubscribeAckReason::PacketIdentifierInUse)
                            .collect(),
                        properties: codec::UserProperties::new(),
                        reason_string: None,
                    }));
                    return Either::Right(Either::Left(Ready::Ok(None)));
                }
                let id = pkt.packet_id;
                Either::Right(Either::Right(
                    ControlResponse::new(control::Subscribe::create(pkt), &self.inner)
                        .packet_id(id),
                ))
            }
            DispatchItem::Item(codec::Packet::Unsubscribe(pkt)) => {
                // register inflight packet id
                if !self.inner.info.borrow_mut().inflight.insert(pkt.packet_id) {
                    // duplicated packet id
                    self.sink.send(codec::Packet::UnsubscribeAck(codec::UnsubscribeAck {
                        packet_id: pkt.packet_id,
                        status: pkt
                            .topic_filters
                            .iter()
                            .map(|_| codec::UnsubscribeAckReason::PacketIdentifierInUse)
                            .collect(),
                        properties: codec::UserProperties::new(),
                        reason_string: None,
                    }));
                    return Either::Right(Either::Left(Ready::Ok(None)));
                }
                let id = pkt.packet_id;
                Either::Right(Either::Right(
                    ControlResponse::new(control::Unsubscribe::create(pkt), &self.inner)
                        .packet_id(id),
                ))
            }
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
    pub(crate) struct PublishResponse<T: Service, C: Service, E, E2> {
        #[pin]
        state: PublishResponseState<T, C, E>,
        packet_id: u16,
        inner: Rc<Inner<C>>,
        _t: marker::PhantomData<(E, E2)>,
    }
}

pin_project_lite::pin_project! {
    #[project = PublishResponseStateProject]
    enum PublishResponseState<T: Service, C: Service, E> {
        Publish { #[pin] fut: T::Future },
        Control { #[pin] fut: ControlResponse<C, E> },
    }
}

impl<T, C, E, E2> Future for PublishResponse<T, C, E, E2>
where
    E: From<E2>,
    T: Service<Request = Publish, Response = PublishAck, Error = E2>,
    PublishAck: TryFrom<E2, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish { fut } => {
                let ack = match fut.poll(cx) {
                    Poll::Ready(Ok(ack)) => ack,
                    Poll::Ready(Err(e)) => {
                        if *this.packet_id != 0 {
                            match PublishAck::try_from(e) {
                                Ok(ack) => ack,
                                Err(e) => {
                                    this.state.set(PublishResponseState::Control {
                                        fut: ControlResponse::new(
                                            ControlMessage::error(e),
                                            this.inner,
                                        ),
                                    });
                                    return self.poll(cx);
                                }
                            }
                        } else {
                            this.state.set(PublishResponseState::Control {
                                fut: ControlResponse::new(
                                    ControlMessage::error(e.into()),
                                    this.inner,
                                ),
                            });
                            return self.poll(cx);
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                };
                if let Some(id) = num::NonZeroU16::new(*this.packet_id) {
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
        packet_id: u16,
        _t: marker::PhantomData<E>,
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

        Self {
            error,
            fut: inner.control.call(pkt),
            inner: inner.clone(),
            packet_id: 0,
            _t: marker::PhantomData,
        }
    }

    fn packet_id(mut self, id: num::NonZeroU16) -> Self {
        self.packet_id = id.get();
        self
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        let result = match this.fut.poll(cx) {
            Poll::Ready(Ok(result)) => {
                if let Some(id) = num::NonZeroU16::new(self.packet_id) {
                    self.inner.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Poll::Ready(Err(err)) => {
                // do not handle nested error
                return if *this.error {
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
                };
            }
            Poll::Pending => return Poll::Pending,
        };

        if self.error {
            if let Some(pkt) = result.packet {
                self.inner.sink.send(pkt)
            }
            if result.disconnect {
                self.inner.sink.drop_sink();
            }
            Poll::Ready(Ok(None))
        } else {
            if result.disconnect {
                self.inner.sink.drop_sink();
            }
            Poll::Ready(Ok(result.packet))
        }
    }
}
