use std::cell::RefCell;
use std::task::{Context, Poll};
use std::{convert::TryFrom, future::Future, marker, num, pin::Pin, rc::Rc};

use ntex::io::DispatchItem;
use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::{buffer::BufferService, inflight::InFlightService, join};
use ntex::util::{BoxFuture, ByteString, Either, HashMap, HashSet, Ready};

use crate::error::{MqttError, ProtocolError};

use super::control::{ControlMessage, ControlResult};
use super::publish::{Publish, PublishAck};
use super::shared::{Ack, MqttShared};
use super::sink::MqttSink;
use super::{codec, codec::EncodeLtd, QoS, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    max_qos: QoS,
    max_inflight_size: usize,
) -> impl ServiceFactory<
    DispatchItem<Rc<MqttShared>>,
    Session<St>,
    Response = Option<codec::Packet>,
    Error = MqttError<E>,
    InitError = MqttError<E>,
>
where
    St: 'static,
    E: From<T::Error> + From<T::InitError> + From<C::Error> + From<C::InitError> + 'static,
    T: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
    C: ServiceFactory<ControlMessage<E>, Session<St>, Response = ControlResult> + 'static,
    PublishAck: TryFrom<T::Error, Error = E>,
{
    let factories = Rc::new((publish, control));

    fn_factory_with_config(move |cfg: Session<St>| {
        let factories = factories.clone();
        let (max_receive, max_topic_alias) = cfg.params();

        async move {
            // create services
            let (publish, control) =
                join(factories.0.create(cfg.clone()), factories.1.create(cfg.clone())).await;

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

            Ok(crate::inflight::InFlightService::new(
                0,
                max_inflight_size,
                Dispatcher::<_, _, E>::new(
                    cfg.sink().clone(),
                    max_qos,
                    max_receive as usize,
                    max_topic_alias,
                    publish,
                    control,
                ),
            ))
        }
    })
}

impl crate::inflight::SizedRequest for DispatchItem<Rc<MqttShared>> {
    fn size(&self) -> u32 {
        if let DispatchItem::Item(ref item) = self {
            item.encoded_size(u32::MAX) as u32
        } else {
            0
        }
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<ControlMessage<E>>, E> {
    sink: MqttSink,
    publish: T,
    shutdown: RefCell<Option<BoxFuture<'static, ()>>>,
    max_qos: QoS,
    max_receive: usize,
    max_topic_alias: u16,
    inner: Rc<Inner<C>>,
    _t: marker::PhantomData<E>,
}

struct Inner<C> {
    control: C,
    sink: MqttSink,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<num::NonZeroU16>,
    aliases: HashMap<num::NonZeroU16, ByteString>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(
        sink: MqttSink,
        max_qos: QoS,
        max_receive: usize,
        max_topic_alias: u16,
        publish: T,
        control: C,
    ) -> Self {
        Self {
            publish,
            max_qos,
            max_receive,
            max_topic_alias,
            sink: sink.clone(),
            shutdown: RefCell::new(None),
            inner: Rc::new(Inner {
                control,
                sink,
                info: RefCell::new(PublishInfo {
                    aliases: HashMap::default(),
                    inflight: HashSet::default(),
                }),
            }),
            _t: marker::PhantomData,
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    E: From<T::Error>,
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
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

    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut shutdown = self.shutdown.borrow_mut();
        if !shutdown.is_some() {
            self.inner.sink.drop_sink();
            let inner = self.inner.clone();
            *shutdown = Some(Box::pin(async move {
                let _ = inner.control.call(ControlMessage::closed()).await;
            }));
        }

        let res0 = shutdown.as_mut().expect("guard above").as_mut().poll(cx);
        let res1 = self.publish.poll_shutdown(cx);
        let res2 = self.inner.control.poll_shutdown(cx);
        if res0.is_pending() || res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn call(&self, request: DispatchItem<Rc<MqttShared>>) -> Self::Future<'_> {
        log::trace!("Dispatch v5 packet: {:#?}", request);

        match request {
            DispatchItem::Item(codec::Packet::Publish(mut publish)) => {
                let info = self.inner.as_ref();
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

                        // check max allowed qos
                        if publish.qos > self.max_qos {
                            log::trace!(
                                "Max allowed QoS is violated, max {:?} provided {:?}",
                                self.max_qos,
                                publish.qos
                            );
                            return Either::Right(Either::Right(ControlResponse::new(
                                ControlMessage::proto_error(ProtocolError::MaxQoSViolated(
                                    publish.qos,
                                )),
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
                        if publish.topic.is_empty() {
                            // lookup topic by provided alias
                            match inner.aliases.get(&alias) {
                                Some(aliased_topic) => publish.topic = aliased_topic.clone(),
                                None => {
                                    return Either::Right(Either::Right(ControlResponse::new(
                                        ControlMessage::proto_error(
                                            ProtocolError::UnknownTopicAlias,
                                        ),
                                        &self.inner,
                                    )));
                                }
                            }
                        } else {
                            // record new alias
                            match inner.aliases.entry(alias) {
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    if entry.get().as_str() != publish.topic.as_str() {
                                        let mut topic = publish.topic.clone();
                                        topic.trimdown();
                                        entry.insert(topic);
                                    }
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    if alias.get() > self.max_topic_alias {
                                        return Either::Right(Either::Right(
                                            ControlResponse::new(
                                                ControlMessage::proto_error(
                                                    ProtocolError::MaxTopicAlias,
                                                ),
                                                &self.inner,
                                            ),
                                        ));
                                    }
                                    let mut topic = publish.topic.clone();
                                    topic.trimdown();
                                    entry.insert(topic);
                                }
                            }
                        }
                    }
                }

                Either::Left(PublishResponse {
                    packet_id: packet_id.map(|v| v.get()).unwrap_or(0),
                    inner: info,
                    state: PublishResponseState::Publish {
                        fut: self.publish.call(Publish::new(publish)),
                    },
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
                ControlResponse::new(ControlMessage::remote_disconnect(pkt), &self.inner),
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
                    ControlResponse::new(ControlMessage::subscribe(pkt), &self.inner)
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
                    ControlResponse::new(ControlMessage::unsubscribe(pkt), &self.inner)
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
        packet_id: u16,
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
    T: Service<Publish, Response = PublishAck>,
    PublishAck: TryFrom<T::Error, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
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
    pub(crate) struct ControlResponse<'f, C: Service<ControlMessage<E>>, E>
    where C: 'f, E: 'f
    {
        #[pin]
        fut: C::Future<'f>,
        inner: &'f Inner<C>,
        error: bool,
        packet_id: u16,
        _t: marker::PhantomData<E>,
    }
}

impl<'f, C, E> ControlResponse<'f, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(pkt: ControlMessage<E>, inner: &'f Inner<C>) -> Self {
        let error = matches!(pkt, ControlMessage::Error(_) | ControlMessage::ProtocolError(_));

        Self {
            error,
            inner,
            fut: inner.control.call(pkt),
            packet_id: 0,
            _t: marker::PhantomData,
        }
    }

    fn packet_id(mut self, id: num::NonZeroU16) -> Self {
        self.packet_id = id.get();
        self
    }
}

impl<'f, C, E> Future for ControlResponse<'f, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
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
