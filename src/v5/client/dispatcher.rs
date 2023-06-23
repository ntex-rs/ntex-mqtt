use std::cell::RefCell;
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, num::NonZeroU16, pin::Pin, rc::Rc};

use ntex::io::DispatchItem;
use ntex::service::{Pipeline, Service, ServiceCall, ServiceCtx};
use ntex::util::{BoxFuture, ByteString, Either, HashMap, HashSet, Ready};

use crate::error::{MqttError, ProtocolError};
use crate::types::packet_type;
use crate::v5::codec::DisconnectReasonCode;
use crate::v5::shared::{Ack, MqttShared};
use crate::v5::{codec, publish::Publish, publish::PublishAck, sink::MqttSink};

use super::control::{ControlMessage, ControlResult};

/// mqtt5 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: MqttSink,
    max_receive: usize,
    max_topic_alias: u16,
    publish: T,
    control: C,
) -> impl Service<DispatchItem<Rc<MqttShared>>, Response = Option<codec::Packet>, Error = MqttError<E>>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = E> + 'static,
{
    Dispatcher::<_, _, E>::new(
        sink,
        max_receive,
        max_topic_alias,
        publish,
        control.map_err(MqttError::Service),
    )
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<ControlMessage<E>>, E> {
    publish: T,
    shutdown: RefCell<Option<BoxFuture<'static, ()>>>,
    max_receive: usize,
    max_topic_alias: u16,
    inner: Rc<Inner<C>>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: C,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
}

struct PublishInfo {
    inflight: HashSet<NonZeroU16>,
    aliases: HashMap<NonZeroU16, ByteString>,
}

impl<T, C, E> Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
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
            shutdown: RefCell::new(None),
            inner: Rc::new(Inner {
                control,
                sink: sink.shared(),
                info: RefCell::new(PublishInfo {
                    aliases: HashMap::default(),
                    inflight: HashSet::default(),
                }),
            }),
            _t: PhantomData,
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>> + 'static,
{
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future<'f> = Either<
        PublishResponse<'f, T, C, E>,
        Either<Ready<Self::Response, MqttError<E>>, ControlResponse<'f, T, C, E>>,
    > where Self: 'f;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx).map_err(MqttError::Service)?;
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
                let _ =
                    Pipeline::new(&inner.control).service_call(ControlMessage::closed()).await;
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

    fn call<'a>(
        &'a self,
        request: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'a, Self>,
    ) -> Self::Future<'a> {
        log::trace!("Dispatch packet: {:#?}", request);

        match request {
            DispatchItem::Item((codec::Packet::Publish(mut publish), size)) => {
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
                                    ProtocolError::violation(
                                        codec::DisconnectReasonCode::ReceiveMaximumExceeded,
                                        "Number of in-flight messages exceeds set maximum, [MQTT-3.3.4-9]"
                                    )
                                ),
                                &self.inner,
                                ctx,
                            )));
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            let _ = self.inner.sink.encode_packet(codec::Packet::PublishAck(
                                codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                },
                            ));
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
                                        ControlMessage::proto_error(ProtocolError::violation(
                                            DisconnectReasonCode::TopicAliasInvalid,
                                            "Unknown topic alias",
                                        )),
                                        &self.inner,
                                        ctx,
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
                                                    ProtocolError::generic_violation(
                                                        "Topic alias is greater than max allowed [MQTT-3.1.2-26]",
                                                    )
                                                ),
                                                &self.inner,
                                                ctx
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
                    packet_size: size,
                    inner: info,
                    state: PublishResponseState::Publish {
                        fut: ctx.call(&self.publish, Publish::new(publish, size)),
                    },
                    ctx,
                    _t: PhantomData,
                })
            }
            DispatchItem::Item((codec::Packet::PublishAck(packet), _)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Publish(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                        ctx,
                    )))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item((codec::Packet::SubscribeAck(packet), _)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Subscribe(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                        ctx,
                    )))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item((codec::Packet::UnsubscribeAck(packet), _)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet)) {
                    Either::Right(Either::Right(ControlResponse::new(
                        ControlMessage::proto_error(err),
                        &self.inner,
                        ctx,
                    )))
                } else {
                    Either::Right(Either::Left(Ready::Ok(None)))
                }
            }
            DispatchItem::Item((codec::Packet::Disconnect(pkt), size)) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::dis(pkt, size),
                    &self.inner,
                    ctx,
                )))
            }
            DispatchItem::Item((codec::Packet::Auth(_), _)) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::unexpected_packet(
                        packet_type::AUTH,
                        "AUTH packet is not supported at this time",
                    )),
                    &self.inner,
                    ctx,
                )))
            }
            DispatchItem::Item((
                pkt @ (codec::Packet::PingRequest
                | codec::Packet::Subscribe(_)
                | codec::Packet::Unsubscribe(_)),
                _,
            )) => Either::Right(Either::Left(Ready::Err(
                ProtocolError::unexpected_packet(
                    pkt.packet_type(),
                    "Packet of the type is not expected from server",
                )
                .into(),
            ))),
            DispatchItem::Item((codec::Packet::PingResponse, _)) => {
                Either::Right(Either::Left(Ready::Ok(None)))
            }
            DispatchItem::Item((pkt, _)) => {
                log::debug!("Unsupported packet: {:?}", pkt);
                Either::Right(Either::Left(Ready::Ok(None)))
            }
            DispatchItem::EncoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Encode(err)),
                    &self.inner,
                    ctx,
                )))
            }
            DispatchItem::DecoderError(err) => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::Decode(err)),
                    &self.inner,
                    ctx,
                )))
            }
            DispatchItem::Disconnect(err) => Either::Right(Either::Right(
                ControlResponse::new(ControlMessage::peer_gone(err), &self.inner, ctx),
            )),
            DispatchItem::KeepAliveTimeout => {
                Either::Right(Either::Right(ControlResponse::new(
                    ControlMessage::proto_error(ProtocolError::KeepAliveTimeout),
                    &self.inner,
                    ctx,
                )))
            }
            DispatchItem::WBackPressureEnabled => {
                self.inner.sink.enable_wr_backpressure();
                Either::Right(Either::Left(Ready::Ok(None)))
            }
            DispatchItem::WBackPressureDisabled => {
                self.inner.sink.disable_wr_backpressure();
                Either::Right(Either::Left(Ready::Ok(None)))
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<'f, T: Service<Publish>, C: Service<ControlMessage<E>>, E> {
        #[pin]
        state: PublishResponseState<'f, T, C, E>,
        packet_id: u16,
        packet_size: u32,
        inner: &'f Inner<C>,
        ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
        _t: PhantomData<E>,
    }
}

pin_project_lite::pin_project! {
    #[project = PublishResponseStateProject]
    enum PublishResponseState<'f, T: Service<Publish>, C: Service<ControlMessage<E>>, E>
    where T: 'f, C: 'f, E: 'f
    {
        Publish { #[pin] fut: ServiceCall<'f, T, Publish> },
        Control { #[pin] fut: ControlResponse<'f, T, C, E> },
    }
}

impl<'f, T, C, E> Future for PublishResponse<'f, T, C, E>
where
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E>,
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish { fut } => {
                let ack = match fut.poll(cx) {
                    Poll::Ready(Ok(res)) => match res {
                        Either::Right(ack) => ack,
                        Either::Left(pkt) => {
                            this.state.set(PublishResponseState::Control {
                                fut: ControlResponse::new(
                                    ControlMessage::publish(
                                        pkt.into_inner(),
                                        *this.packet_size,
                                    ),
                                    this.inner,
                                    *this.ctx,
                                )
                                .packet_id(*this.packet_id),
                            });
                            return self.poll(cx);
                        }
                    },
                    Poll::Ready(Err(e)) => {
                        this.state.set(PublishResponseState::Control {
                            fut: ControlResponse::new(
                                ControlMessage::error(e),
                                this.inner,
                                *this.ctx,
                            ),
                        });
                        return self.poll(cx);
                    }
                    Poll::Pending => return Poll::Pending,
                };
                if let Some(id) = NonZeroU16::new(*this.packet_id) {
                    log::trace!("Sending publish ack for {} id", this.packet_id);
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
    pub(crate) struct ControlResponse<'f, T, C: Service<ControlMessage<E>>, E>
    where T: 'f, C: 'f, E: 'f
    {
        #[pin]
        fut: ServiceCall<'f, C, ControlMessage<E>>,
        inner: &'f Inner<C>,
        error: bool,
        packet_id: u16,
        ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
        _t: PhantomData<E>,
    }
}

impl<'f, T, C, E> ControlResponse<'f, T, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    fn new(
        pkt: ControlMessage<E>,
        inner: &'f Inner<C>,
        ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
    ) -> Self {
        let error = matches!(pkt, ControlMessage::Error(_) | ControlMessage::ProtocolError(_));
        let fut = ctx.call(&inner.control, pkt);

        Self { error, inner, fut, ctx, packet_id: 0, _t: PhantomData }
    }

    fn packet_id(mut self, id: u16) -> Self {
        self.packet_id = id;
        self
    }
}

impl<'f, T, C, E> Future for ControlResponse<'f, T, C, E>
where
    C: Service<ControlMessage<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        let result = match this.fut.poll(cx) {
            Poll::Ready(Ok(result)) => {
                if let Some(id) = NonZeroU16::new(self.packet_id) {
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
                            let fut =
                                this.ctx.call(&this.inner.control, ControlMessage::error(err));
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
                let _ = self.inner.sink.encode_packet(pkt);
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

#[cfg(test)]
mod tests {
    use ntex::{io::Io, service::fn_service, testing::IoTest, util::lazy};
    use std::rc::Rc;

    use super::*;
    use crate::v5::{codec, MqttSink};

    #[derive(Debug)]
    struct TestError;

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0);
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Default::default()));
        let sink = MqttSink::new(shared.clone());

        let disp = Pipeline::new(Dispatcher::<_, _, _>::new(
            sink.clone(),
            16,
            16,
            fn_service(|p: Publish| Ready::Ok::<_, TestError>(Either::Right(p.ack()))),
            fn_service(|_| {
                Ready::Ok::<_, MqttError<TestError>>(ControlResult {
                    packet: None,
                    disconnect: false,
                })
            }),
        ));

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
