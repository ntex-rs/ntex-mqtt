use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use futures::future::{ok, Either, FutureExt, Ready};
use ntex::service::Service;

use crate::error::{MqttError, ProtocolError};
use crate::v5::{codec, publish::Publish, publish::PublishAck, sink::Ack, sink::MqttSink};
use crate::{io::DispatcherItem, types::packet_type, AHashSet};

use super::control::{ControlMessage, ControlResult};

/// mqtt5 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: MqttSink,
    max_receive: usize,
    max_topic_alias: u16,
    publish: T,
    control: C,
) -> impl Service<
    Request = DispatcherItem<codec::Codec>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
>
where
    E: From<T::Error> + 'static,
    T: Service<Request = Publish, Response = either::Either<Publish, PublishAck>, Error = E>
        + 'static,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E> + 'static,
{
    Dispatcher::<_, _, E>::new(sink, max_receive as usize, max_topic_alias, publish, control)
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
    inflight: AHashSet<NonZeroU16>,
    aliases: AHashSet<NonZeroU16>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Request = Publish, Response = either::Either<Publish, PublishAck>, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
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
                    aliases: AHashSet::default(),
                    inflight: AHashSet::default(),
                }),
            }),
            _t: PhantomData,
        }
    }
}

impl<T, C, E> Service for Dispatcher<T, C, E>
where
    T: Service<Request = Publish, Response = either::Either<Publish, PublishAck>, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
    C::Future: 'static,
{
    type Request = DispatcherItem<codec::Codec>;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T, C, E>,
        Either<Ready<Result<Self::Response, MqttError<E>>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(MqttError::Service)?;
        let res2 = self.inner.control.poll_ready(cx).map_err(MqttError::Service)?;

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
            ntex::rt::spawn(
                self.inner.control.call(ControlMessage::closed(is_error)).map(|_| ()),
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
                            return Either::Right(Either::Right(ControlResponse::new(
                                ControlMessage::proto_error(
                                    ProtocolError::ReceiveMaximumExceeded,
                                ),
                                &self.inner,
                            )));
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            self.inner.sink.send(codec::Packet::PublishAck(
                                codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                },
                            ));
                            return Either::Right(Either::Left(ok(None)));
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
                    state: PublishResponseState::Publish(
                        self.publish.call(Publish::new(publish)),
                    ),
                    _t: PhantomData,
                })
            }
            DispatcherItem::Item(codec::Packet::PublishAck(packet)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Publish(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                    )))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            DispatcherItem::Item(codec::Packet::SubscribeAck(packet)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Subscribe(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                    )))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            DispatcherItem::Item(codec::Packet::UnsubscribeAck(packet)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                    )))
                } else {
                    Either::Right(Either::Left(ok(None)))
                }
            }
            DispatcherItem::Item(codec::Packet::PingRequest) => {
                Either::Right(Either::Left(ok(Some(codec::Packet::PingResponse))))
            }
            DispatcherItem::Item(codec::Packet::Disconnect(pkt)) => Either::Right(
                Either::Right(ControlResponse::new(ControlMessage::dis(pkt), &self.inner)),
            ),
            DispatcherItem::Item(codec::Packet::Auth(_)) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Unexpected(
                        packet_type::AUTH,
                        "Auth packet is not supported",
                    )),
                    &self.inner,
                )))
            }
            DispatcherItem::Item(codec::Packet::Subscribe(_)) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Unexpected(
                        packet_type::SUBSCRIBE,
                        "Subscribe packet is not supported",
                    )),
                    &self.inner,
                )))
            }
            DispatcherItem::Item(codec::Packet::Unsubscribe(_)) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Unexpected(
                        packet_type::UNSUBSCRIBE,
                        "Unsubscribe packet is not supported",
                    )),
                    &self.inner,
                )))
            }
            DispatcherItem::Item(codec::Packet::PingResponse) => {
                Either::Right(Either::Left(ok(None)))
            }
            DispatcherItem::Item(pkt) => {
                log::debug!("Unsupported packet: {:?}", pkt);
                Either::Right(Either::Left(ok(None)))
            }
            DispatcherItem::EncoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Encode(err)),
                    &self.inner,
                )))
            }
            DispatcherItem::DecoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                )))
            }
            DispatcherItem::IoError(err) => Either::Right(Either::Right(ControlResponse::new(
                ControlMessage::proto_error(ProtocolError::Io(err)),
                &self.inner,
            ))),
            DispatcherItem::KeepAliveTimeout => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                )))
            }
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
    T: Service<Request = Publish, Response = either::Either<Publish, PublishAck>, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish(fut) => {
                let ack = match futures::ready!(fut.poll(cx)) {
                    Ok(res) => match res {
                        either::Either::Right(ack) => ack,
                        either::Either::Left(pkt) => {
                            this.state.set(PublishResponseState::Control(
                                ControlResponse::new(
                                    ControlMessage::publish(pkt.into_inner()),
                                    this.inner,
                                )
                                .packet_id(*this.packet_id),
                            ));
                            return self.poll(cx);
                        }
                    },
                    Err(e) => {
                        this.state.set(PublishResponseState::Control(ControlResponse::new(
                            ControlMessage::error(e),
                            this.inner,
                        )));
                        return self.poll(cx);
                    }
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
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
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
            _t: PhantomData,
        }
    }

    fn packet_id(mut self, id: u16) -> Self {
        self.packet_id = id;
        self
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        let result = match futures::ready!(this.fut.poll(cx)) {
            Ok(result) => {
                if let Some(id) = NonZeroU16::new(self.packet_id) {
                    self.inner.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                // do not handle nested error
                if *this.error {
                    return Poll::Ready(Err(MqttError::Service(err)));
                } else {
                    // handle error from control service
                    *this.error = true;
                    let fut = this.inner.control.call(ControlMessage::error(err));
                    self.as_mut().project().fut.set(fut);
                    return self.poll(cx);
                }
            }
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
