use std::cell::{Cell, RefCell};
use std::task::{Context, Poll};
use std::time::Duration;
use std::{convert::TryFrom, future::Future, marker, num, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::{join, Either, HashSet, Ready};

use crate::error::{MqttError, ProtocolError};
use crate::io::DispatchItem;
use crate::types::AwaitingRelSet;

use super::control::{self, ControlMessage, ControlResult};
use super::publish::{Publish, PublishMessage, PublishResult};
use super::shared::{Ack, MqttShared};
use super::sink::MqttSink;
use super::{codec, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    max_awaiting_rel: usize,
    await_rel_timeout: Duration,
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
            Request = PublishMessage,
            Response = PublishResult,
            InitError = MqttError<E>,
        > + 'static,
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlMessage<E>,
            Response = ControlResult,
            Error = E,
            InitError = MqttError<E>,
        > + 'static,
    PublishResult: TryFrom<T::Error, Error = E>,
{
    fn_factory_with_config(move |cfg: Session<St>| {
        // create services
        let fut = join(publish.new_service(cfg.clone()), control.new_service(cfg.clone()));

        let (max_receive, max_topic_alias) = cfg.params();

        async move {
            let (publish, control) = fut.await;

            Ok(Dispatcher::<_, _, E, T::Error>::new(
                cfg.sink().clone(),
                max_receive as usize,
                max_topic_alias,
                publish?,
                control?,
                max_awaiting_rel,
                await_rel_timeout,
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
    awaiting_rels: RefCell<AwaitingRelSet>,
}

struct PublishInfo {
    inflight: HashSet<num::NonZeroU16>,
    aliases: HashSet<num::NonZeroU16>,
}

impl<T, C, E, E2> Dispatcher<T, C, E, E2>
where
    T: Service<Request = PublishMessage, Response = PublishResult, Error = E2>,
    PublishResult: TryFrom<E2, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
{
    fn new(
        sink: MqttSink,
        max_receive: usize,
        max_topic_alias: u16,
        publish: T,
        control: C,
        max_awaiting_rel: usize,
        await_rel_timeout: Duration,
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
                awaiting_rels: RefCell::new(AwaitingRelSet::new(
                    max_awaiting_rel,
                    await_rel_timeout,
                )),
            }),
            _t: marker::PhantomData,
        }
    }
}

impl<T, C, E, E2> Service for Dispatcher<T, C, E, E2>
where
    T: Service<Request = PublishMessage, Response = PublishResult, Error = E2>,
    PublishResult: TryFrom<E2, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
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
            let fut = self.inner.control.call(ControlMessage::closed(is_error));
            ntex::rt::spawn(async move {
                let _ = fut.await;
            });
        }
        Poll::Ready(())
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", request);
        match request {
            DispatchItem::Item(codec::Packet::Publish(publish)) => {
                let inner = self.inner.clone();
                let packet_id = publish.packet_id;
                let qos = publish.qos;
                {
                    let mut info = inner.info.borrow_mut();

                    if let Some(pid) = packet_id {
                        // check for receive maximum
                        if self.max_receive != 0 && info.inflight.len() >= self.max_receive {
                            log::trace!(
                                "Receive maximum exceeded: max: {} inflight: {}",
                                self.max_receive,
                                info.inflight.len()
                            );
                            return Either::Right(Either::Right(ControlResponse::new(
                                ControlMessage::proto_error(
                                    ProtocolError::ReceiveMaximumExceeded,
                                ),
                                &self.inner,
                            )));
                        }

                        // check for duplicated packet id
                        if !info.inflight.insert(pid) {
                            let _ =
                                self.sink.send(codec::Packet::PublishAck(codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                }));
                            return Either::Right(Either::Left(Ready::Ok(None)));
                        }

                        //qos == 2
                        if codec::QoS::ExactlyOnce == qos {
                            let mut awaiting_rels = inner.awaiting_rels.borrow_mut();
                            if awaiting_rels.contains(&pid) {
                                log::warn!(
                                    "Duplicated sending of QoS2 message, packet id is {:?}",
                                    pid
                                );
                                return Either::Right(Either::Left(Ready::Ok(None)));
                            }
                            //Remove the timeout awating release QoS2 messages, if it exists
                            awaiting_rels.remove_timeouts();
                            if awaiting_rels.is_full() {
                                // Too many awating release QoS2 messages, the earliest ones will be removed
                                if let Some(packet_id) = awaiting_rels.pop() {
                                    log::warn!("Too many awating release QoS2 messages, remove the earliest, packet id is {}", packet_id);
                                }
                            }
                            //Stored message identifier
                            awaiting_rels.push(pid)
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        // check existing topic
                        if publish.topic.is_empty() {
                            if !info.aliases.contains(&alias) {
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
                            info.aliases.insert(alias);
                        }
                    }
                }

                Either::Left(PublishResponse {
                    state: PublishResponseState::Publish {
                        packet_id: packet_id.map(|id| id.get()).unwrap_or_default(),
                        qos,
                        fut: self.publish.call(PublishMessage::Publish(Publish::new(publish))),
                    },
                    inner,
                    _t: marker::PhantomData,
                })
            }
            DispatchItem::Item(codec::Packet::PublishAck(ack)) => {
                let packet_id = ack.packet_id;
                if let Err(e) = self.sink.pkt_ack(Ack::Publish(ack.clone())) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(e),
                        &self.inner,
                    )))
                } else {
                    Either::Left(PublishResponse {
                        state: PublishResponseState::PublishAck {
                            packet_id: packet_id.get(),
                            fut: self.publish.call(PublishMessage::PublishAck(ack)),
                        },
                        inner: self.inner.clone(),
                        _t: marker::PhantomData,
                    })
                }
            }

            DispatchItem::Item(codec::Packet::PublishRelease(ack2)) => {
                self.inner.awaiting_rels.borrow_mut().remove(&ack2.packet_id);
                Either::Right(Either::Left(Ready::Ok(Some(codec::Packet::PublishComplete(
                    ack2, //@TODO ...
                )))))
            }

            //fut: self.publish.call(PublishMessage::PublishReceived(packet_id)),
            DispatchItem::Item(codec::Packet::PublishReceived(ack)) => {
                Either::Left(PublishResponse {
                    // state: PublishResponseState::Publish {
                    //     fut: self.publish.call(PublishMessage::PublishReceived(ack.packet_id)),
                    // },
                    //state: PublishResponseState::Release{packet_id: ack.packet_id},
                    state: PublishResponseState::Release {
                        packet_id: ack.packet_id,
                        fut: self.publish.call(PublishMessage::PublishReceived(ack)),
                    },
                    inner: self.inner.clone(),
                    _t: marker::PhantomData,
                })
            }

            DispatchItem::Item(codec::Packet::PublishComplete(ack2)) => {
                Either::Left(PublishResponse {
                    state: PublishResponseState::Complete {
                        packet_id: ack2.packet_id,
                        fut: self.publish.call(PublishMessage::PublishComplete(ack2)),
                    },
                    // state: PublishResponseState::Nothing,
                    inner: self.inner.clone(),
                    _t: marker::PhantomData,
                })
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
                    let _ = self.sink.send(codec::Packet::SubscribeAck(codec::SubscribeAck {
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
                    let _ =
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
        inner: Rc<Inner<C>>,
        _t: marker::PhantomData<(E, E2)>,
    }
}

pin_project_lite::pin_project! {
    #[project = PublishResponseStateProject]
    enum PublishResponseState<T: Service, C: Service, E> {
        Publish { packet_id: u16, qos: codec::QoS, #[pin] fut: T::Future },
        PublishAck { packet_id: u16, #[pin] fut: T::Future },
        Release { packet_id: NonZeroU16, #[pin] fut: T::Future },
        Complete { packet_id: NonZeroU16, #[pin] fut: T::Future },
        Control { #[pin] fut: ControlResponse<C, E> },
    }
}

impl<T, C, E, E2> Future for PublishResponse<T, C, E, E2>
where
    E: From<E2>,
    T: Service<Request = PublishMessage, Response = PublishResult, Error = E2>,
    PublishResult: TryFrom<E2, Error = E>,
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish { packet_id, qos, fut } => {
                let ack = match fut.poll(cx) {
                    Poll::Ready(Ok(ack)) => ack,
                    Poll::Ready(Err(e)) => {
                        if *packet_id != 0 {
                            match PublishResult::try_from(e) {
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

                if let Some(id) = num::NonZeroU16::new(*packet_id) {
                    this.inner.info.borrow_mut().inflight.remove(&id);
                    if let PublishResult::PublishAck(ack) = ack {
                        let ack = codec::PublishAck {
                            packet_id: id,
                            reason_code: ack.reason_code,
                            reason_string: ack.reason_string,
                            properties: ack.properties,
                        };
                        match qos {
                            codec::QoS::AtLeastOnce => {
                                Poll::Ready(Ok(Some(codec::Packet::PublishAck(ack))))
                            }
                            codec::QoS::ExactlyOnce => {
                                Poll::Ready(Ok(Some(codec::Packet::PublishReceived(ack))))
                            }
                            _ => Poll::Ready(Ok(None)),
                        }
                    } else {
                        Poll::Ready(Ok(None))
                    }
                } else {
                    Poll::Ready(Ok(None))
                }
            }

            PublishResponseStateProject::PublishAck { packet_id: _, fut } => {
                let _ = match fut.poll(cx) {
                    Poll::Ready(result) => result,
                    Poll::Pending => return Poll::Pending,
                };
                Poll::Ready(Ok(None))
            }
            PublishResponseStateProject::Control { fut } => fut.poll(cx),
            PublishResponseStateProject::Release { packet_id, fut } => {
                let _ = match fut.poll(cx) {
                    Poll::Ready(result) => result,
                    Poll::Pending => return Poll::Pending,
                };

                let ack2 = codec::PublishAck2 {
                    packet_id: *packet_id,
                    reason_code: codec::PublishAck2Reason::Success,
                    properties: codec::UserProperties::default(),
                    reason_string: None,
                };
                Poll::Ready(Ok(Some(codec::Packet::PublishRelease(ack2))))
            }
            PublishResponseStateProject::Complete { packet_id: _, fut } => {
                let _ = match fut.poll(cx) {
                    Poll::Ready(result) => result,
                    Poll::Pending => return Poll::Pending,
                };
                Poll::Ready(Ok(None))
            }
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
    C: Service<Request = ControlMessage<E>, Response = ControlResult, Error = E>,
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
            Poll::Pending => return Poll::Pending,
        };

        if self.error {
            if let Some(pkt) = result.packet {
                let _ = self.inner.sink.send(pkt);
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
