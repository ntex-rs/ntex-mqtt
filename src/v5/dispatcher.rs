use std::cell::{Cell, RefCell};
use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::future::{join, ok, Either, FutureExt, Ready};
use futures::ready;
use fxhash::FxHashSet;
use ntex::service::{fn_factory_with_config, Service, ServiceFactory};
use ntex::util::order::{InOrder, InOrderError};
use ntex::util::{buffer::BufferService, inflight::InFlightService};

use crate::error::MqttError;
use crate::framed::DispatcherError;

use super::control::{self, ControlPacket, ControlResult};
use super::publish::{Publish, PublishAck};
use super::sink::MqttSink;
use super::{codec, Session};

/// mqtt3 protocol dispatcher
pub(super) fn factory<St, T, C, E>(
    publish: T,
    control: C,
    max_topic_alias: u16,
) -> impl ServiceFactory<
    Config = Session<St>,
    Request = Result<codec::Packet, DispatcherError<codec::Codec>>,
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
            Response = PublishAck,
            Error = MqttError<E>,
            InitError = MqttError<E>,
        > + 'static,
    C: ServiceFactory<
            Config = Session<St>,
            Request = ControlPacket<E>,
            Response = ControlResult,
            Error = MqttError<E>,
            InitError = MqttError<E>,
        > + 'static,
{
    fn_factory_with_config(move |cfg: Session<St>| {
        let inflight = cfg.max_inflight();

        // create services
        let fut = join(publish.new_service(cfg.clone()), control.new_service(cfg.clone()));

        async move {
            let (publish, control) = fut.await;

            // mqtt dispatcher
            Ok(Dispatcher::<_, _, _, E>::new(
                cfg,
                max_topic_alias,
                // limit number of in-flight messages
                InFlightService::new(
                    inflight,
                    // mqtt spec requires ack ordering, so enforce response ordering
                    InOrder::service(publish?).map_err(|e| match e {
                        InOrderError::Service(e) => e,
                        InOrderError::Disconnected => MqttError::Disconnected,
                    }),
                ),
                BufferService::new(
                    16,
                    || MqttError::Disconnected,
                    // limit number of in-flight messages
                    InFlightService::new(1, control?),
                ),
            ))
        }
    })
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<St, T: Service<Error = MqttError<E>>, C, E> {
    session: Session<St>,
    publish: T,
    shutdown: Cell<bool>,
    max_topic_alias: u16,
    info: Rc<(RefCell<PublishInfo>, C, MqttSink)>,
}

struct PublishInfo {
    inflight: FxHashSet<NonZeroU16>,
    aliases: FxHashSet<NonZeroU16>,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = PublishAck, Error = MqttError<E>>,
    C: Service<Request = ControlPacket<E>, Response = ControlResult, Error = MqttError<E>>,
{
    pub(crate) fn new(
        session: Session<St>,
        max_topic_alias: u16,
        publish: T,
        control: C,
    ) -> Self {
        let sink = session.sink().clone();
        Self {
            session,
            publish,
            max_topic_alias,
            shutdown: Cell::new(false),
            info: Rc::new((
                RefCell::new(PublishInfo {
                    aliases: FxHashSet::default(),
                    inflight: FxHashSet::default(),
                }),
                control,
                sink,
            )),
        }
    }
}

impl<St, T, C, E> Service for Dispatcher<St, T, C, E>
where
    T: Service<Request = Publish, Response = PublishAck, Error = MqttError<E>>,
    C: Service<Request = ControlPacket<E>, Response = ControlResult, Error = MqttError<E>>,
    C::Future: 'static,
    E: 'static,
{
    type Request = Result<codec::Packet, DispatcherError<codec::Codec>>;
    type Response = Option<codec::Packet>;
    type Error = MqttError<E>;
    type Future = Either<
        PublishResponse<T::Future, E, C>,
        Either<Ready<Result<Self::Response, MqttError<E>>>, ControlResponse<C, E>>,
    >;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let res1 = self.publish.poll_ready(cx)?;
        let res2 = self.info.1.poll_ready(cx)?;

        if res1.is_pending() || res2.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(&self, _: &mut Context<'_>, is_error: bool) -> Poll<()> {
        if !self.shutdown.get() {
            self.shutdown.set(true);
            ntex::rt::spawn(self.info.1.call(ControlPacket::ctl_closed(is_error)).map(|_| ()));
        }
        Poll::Ready(())
    }

    fn call(&self, request: Self::Request) -> Self::Future {
        log::trace!("Dispatch packet: {:#?}", request);

        match request {
            Ok(codec::Packet::Publish(publish)) => {
                let info = self.info.clone();
                let packet_id = publish.packet_id;

                {
                    let mut inner = info.0.borrow_mut();

                    // check for duplicated packet id
                    if let Some(pid) = packet_id {
                        if !inner.inflight.insert(pid) {
                            return Either::Right(Either::Right(ControlResponse {
                                fut: self.info.1.call(ControlPacket::ctl_error(
                                    MqttError::DuplicatedPacketId,
                                )),
                                error: true,
                                info: self.info.clone(),
                            }));
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        // check existing topic
                        if publish.topic.is_empty() {
                            if !inner.aliases.contains(&alias) {
                                return Either::Right(Either::Right(ControlResponse {
                                    fut: self.info.1.call(ControlPacket::ctl_error(
                                        MqttError::UnknownTopicAlias,
                                    )),
                                    error: true,
                                    info: self.info.clone(),
                                }));
                            }
                        } else {
                            if alias.get() > self.max_topic_alias {
                                return Either::Right(Either::Right(ControlResponse {
                                    fut: self.info.1.call(ControlPacket::ctl_error(
                                        MqttError::MaxTopicAlias,
                                    )),
                                    error: true,
                                    info: self.info.clone(),
                                }));
                            }

                            // record new alias
                            inner.aliases.insert(alias);
                        }
                    }
                }

                Either::Left(PublishResponse {
                    packet_id,
                    info,
                    state: PublishResponseState::Publish(
                        self.publish.call(Publish::new(publish)),
                    ),
                    _t: PhantomData,
                })
            }
            Ok(codec::Packet::PublishAck(packet)) => {
                self.session.sink().complete_publish_qos1(packet.packet_id);
                Either::Right(Either::Left(ok(None)))
            }
            Ok(codec::Packet::Auth(pkt)) => Either::Right(Either::Right(ControlResponse {
                fut: self.info.1.call(ControlPacket::ctl_auth(pkt)),
                info: self.info.clone(),
                error: false,
            })),
            Ok(codec::Packet::PingRequest) => Either::Right(Either::Right(ControlResponse {
                fut: self.info.1.call(ControlPacket::ctl_ping()),
                info: self.info.clone(),
                error: false,
            })),
            Ok(codec::Packet::Disconnect(pkt)) => {
                Either::Right(Either::Right(ControlResponse {
                    fut: self.info.1.call(ControlPacket::ctl_disconnect(pkt)),
                    info: self.info.clone(),
                    error: false,
                }))
            }
            Ok(codec::Packet::Subscribe(pkt)) => {
                Either::Right(Either::Right(ControlResponse {
                    fut: self.info.1.call(control::Subscribe::create(pkt)),
                    info: self.info.clone(),
                    error: false,
                }))
            }
            Ok(codec::Packet::Unsubscribe(pkt)) => {
                Either::Right(Either::Right(ControlResponse {
                    fut: self.info.1.call(control::Unsubscribe::create(pkt)),
                    info: self.info.clone(),
                    error: false,
                }))
            }
            Ok(_) => Either::Right(Either::Left(ok(None))),
            Err(e) => Either::Right(Either::Right(ControlResponse {
                fut: self.info.1.call(ControlPacket::ctl_error(e.into())),
                info: self.info.clone(),
                error: false,
            })),
        }
    }
}

pin_project_lite::pin_project! {
    /// Publish service response future
    pub(crate) struct PublishResponse<T, E, C: Service<Error = MqttError<E>>> {
        #[pin]
        state: PublishResponseState<T, C, E>,
        packet_id: Option<NonZeroU16>,
        info: Rc<(RefCell<PublishInfo>, C, MqttSink)>,
        _t: PhantomData<E>,
    }
}

#[pin_project::pin_project(project = PublishResponseStateProject)]
enum PublishResponseState<T, C: Service<Error = MqttError<E>>, E> {
    Publish(#[pin] T),
    Control(#[pin] ControlResponse<C, E>),
}

impl<T, E, C> Future for PublishResponse<T, E, C>
where
    T: Future<Output = Result<PublishAck, MqttError<E>>>,
    C: Service<Request = ControlPacket<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        match this.state.as_mut().project() {
            PublishResponseStateProject::Publish(fut) => {
                let ack = match ready!(fut.poll(cx)) {
                    Ok(ack) => ack,
                    Err(e) => {
                        this.state.set(PublishResponseState::Control(ControlResponse {
                            fut: this.info.1.call(ControlPacket::ctl_error(e)),
                            info: this.info.clone(),
                            error: true,
                        }));
                        return self.poll(cx);
                    }
                };
                if let Some(packet_id) = this.packet_id {
                    this.info.0.borrow_mut().inflight.remove(&packet_id);
                    let ack = codec::PublishAck {
                        packet_id: *packet_id,
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
    pub(crate) struct ControlResponse<C: Service<Error = MqttError<E>>, E>
    {
        #[pin]
        fut: C::Future,
        info: Rc<(RefCell<PublishInfo>, C, MqttSink)>,
        error: bool,
    }
}

impl<C, E> Future for ControlResponse<C, E>
where
    C: Service<Request = ControlPacket<E>, Response = ControlResult, Error = MqttError<E>>,
{
    type Output = Result<Option<codec::Packet>, MqttError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        let result = match ready!(this.fut.poll(cx)) {
            Ok(result) => result,
            Err(err) => {
                if *this.error {
                    return Poll::Ready(Err(err));
                } else {
                    // handle error from control service
                    *this.error = true;
                    let fut = this.info.1.call(ControlPacket::ctl_error(err));
                    self.as_mut().project().fut.set(fut);
                    return self.poll(cx);
                }
            }
        };

        if result.disconnect {
            self.info.2.drop_sink();
        }

        Poll::Ready(Ok(result.packet))
    }
}
