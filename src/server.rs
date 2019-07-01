use std::marker::PhantomData;
use std::rc::Rc;

use actix_codec::{AsyncRead, AsyncWrite};
use actix_ioframe as ioframe;
use actix_server_config::{Io as ServerIo, ServerConfig};
use actix_service::{boxed, new_apply_fn, new_service_cfg};
use actix_service::{IntoNewService, IntoService, NewService, Service, ServiceExt};
use futures::future::{err, Either};
use futures::{Future, Sink, Stream};
use mqtt_codec as mqtt;

use crate::cell::Cell;
use crate::connect::{Connect, ConnectAck};
use crate::default::{SubsNotImplemented, UnsubsNotImplemented};
use crate::dispatcher::{dispatcher, MqttState};
use crate::error::MqttError;
use crate::publish::Publish;
use crate::sink::MqttSink;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};
use crate::State;

/// Mqtt Server
pub struct MqttServer<St, C: NewService> {
    connect: C,
    subscribe: boxed::BoxedNewService<
        St,
        Subscribe<St>,
        SubscribeResult,
        MqttError<C::Error>,
        MqttError<C::Error>,
    >,
    unsubscribe: boxed::BoxedNewService<
        St,
        Unsubscribe<St>,
        (),
        MqttError<C::Error>,
        MqttError<C::Error>,
    >,
    disconnect: Option<Cell<boxed::BoxedService<State<St>, (), MqttError<C::Error>>>>,
    keep_alive: u64,
    inflight: usize,
    _t: PhantomData<(St,)>,
}

impl<St, C> MqttServer<St, C>
where
    St: 'static,
    C: NewService<Config = (), Request = Connect<()>, Response = ConnectAck<St>> + 'static,
{
    /// Create server factory and provide connect service
    pub fn new<F>(connect: F) -> MqttServer<St, C>
    where
        F: IntoNewService<C>,
    {
        MqttServer {
            connect: connect.into_new_service(),
            subscribe: boxed::new_service(
                NewService::map_err(SubsNotImplemented::default(), |e| MqttError::Service(e))
                    .map_init_err(|e| MqttError::Service(e)),
            ),
            unsubscribe: boxed::new_service(
                NewService::map_err(UnsubsNotImplemented::default(), |e| MqttError::Service(e))
                    .map_init_err(|e| MqttError::Service(e)),
            ),
            disconnect: None,
            keep_alive: 30,
            inflight: 15,
            _t: PhantomData,
        }
    }

    /// A time interval measured in seconds.
    ///
    /// keep-alive is set to 30 seconds by default.
    pub fn keep_alive(mut self, val: u16) -> Self {
        self.keep_alive = val.into();
        self
    }

    /// Number of in-flight concurrent messages.
    ///
    /// in-flight is set to 15 messages
    pub fn inflight(mut self, val: usize) -> Self {
        self.inflight = val;
        self
    }

    /// Service to execute for subscribe packet
    pub fn subscribe<F, Srv>(mut self, subscribe: F) -> Self
    where
        F: IntoNewService<Srv>,
        Srv: NewService<Config = St, Request = Subscribe<St>, Response = SubscribeResult>
            + 'static,
        Srv::Service: 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError> + 'static,
    {
        self.subscribe = boxed::new_service(
            subscribe
                .into_new_service()
                .map_err(|e| MqttError::Service(e.into()))
                .map_init_err(|e| MqttError::Service(e.into())),
        );
        self
    }

    /// Service to execute for unsubscribe packet
    pub fn unsubscribe<F, Srv>(mut self, unsubscribe: F) -> Self
    where
        F: IntoNewService<Srv>,
        Srv: NewService<Config = St, Request = Unsubscribe<St>, Response = ()> + 'static,
        Srv::Service: 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError> + 'static,
    {
        self.unsubscribe = boxed::new_service(
            unsubscribe
                .into_new_service()
                .map_err(|e| MqttError::Service(e.into()))
                .map_init_err(|e| MqttError::Service(e.into())),
        );
        self
    }

    /// Service to execute on disconnect
    pub fn disconnect<UF, U>(mut self, srv: UF) -> Self
    where
        UF: IntoService<U>,
        U: Service<Request = State<St>, Response = ()> + 'static,
        C::Error: From<U::Error> + 'static,
    {
        self.disconnect = Some(Cell::new(boxed::service(
            srv.into_service().map_err(|e| MqttError::Service(e.into())),
        )));
        self
    }

    /// Set service to execute for publish packet and create service factory
    pub fn finish<Io, F, P, I>(
        self,
        publish: F,
    ) -> impl NewService<
        Config = ServerConfig,
        Request = ServerIo<Io, I>,
        Response = (),
        Error = MqttError<C::Error>,
    >
    where
        I: 'static,
        Io: AsyncRead + AsyncWrite + 'static,
        F: IntoNewService<P>,
        P: NewService<Config = St, Request = Publish<St>, Response = ()> + 'static,
        C::Error: From<P::Error> + From<P::InitError> + 'static,
    {
        new_apply_fn(
            ioframe::Builder::new()
                .factory(connect_service(self.connect))
                .finish(dispatcher(
                    publish
                        .into_new_service()
                        .map_err(|e| MqttError::Service(e.into()))
                        .map_init_err(|e| MqttError::Service(e.into())),
                    Rc::new(self.subscribe),
                    Rc::new(self.unsubscribe),
                    self.keep_alive,
                    self.inflight,
                ))
                .map_err(|e| match e {
                    ioframe::ServiceError::Service(e) => e,
                    ioframe::ServiceError::Encoder(e) => MqttError::Protocol(e),
                    ioframe::ServiceError::Decoder(e) => MqttError::Protocol(e),
                }),
            |io: ServerIo<Io, I>, srv| srv.call(io.into_parts().0),
        )
        .map_config(|_| actix_service::MappedConfig::Owned(()))
    }
}

fn connect_service<Io, St, C>(
    service: C,
) -> impl NewService<
    Config = (),
    Request = ioframe::Connect<Io>,
    Response = ioframe::ConnectResult<Io, MqttState<St>, mqtt::Codec>,
    Error = MqttError<C::Error>,
>
where
    Io: AsyncRead + AsyncWrite,
    C: NewService<Config = (), Request = Connect<()>, Response = ConnectAck<St>> + 'static,
{
    new_service_cfg(move |_cfg: &()| {
        service.new_service(&()).map(|service| {
            let service = Cell::new(service);

            (move |conn: ioframe::Connect<Io>| {
                let mut srv = service.clone();
                let conn = conn.codec(mqtt::Codec::new());

                conn.into_future()
                    .map_err(|(e, _)| MqttError::Protocol(e.into()))
                    .and_then(move |(packet, framed)| {
                        match packet {
                            Some(mqtt::Packet::Connect(connect)) => {
                                let sink = MqttSink::new(framed.sink().clone());

                                Either::A(
                                    // authenticate mqtt connection
                                    srv.get_mut()
                                        .call(Connect::new(connect, (), sink.clone()))
                                        .map_err(|e| MqttError::Service(e.into()))
                                        .and_then(move |result| {
                                            match result.into_inner() {
                                                either::Either::Left((session, session_present)) => {
                                                    Either::A(
                                                        framed
                                                            .send(mqtt::Packet::ConnectAck {
                                                                session_present,
                                                                return_code:
                                                                mqtt::ConnectCode::ConnectionAccepted,
                                                            })
                                                            .map_err(|e| MqttError::Protocol(e.into()))
                                                            .map(move |framed| framed.state(MqttState::new(session, sink))),
                                                    )
                                                }
                                                either::Either::Right(code) => Either::B(
                                                    framed
                                                        .send(mqtt::Packet::ConnectAck {
                                                            session_present: false,
                                                            return_code: code,
                                                        })
                                                        .map_err(|e| MqttError::Protocol(e.into()))
                                                        .and_then(|_| err(MqttError::Disconnected)),
                                                ),
                                            }
                                        }),
                                )
                            }
                            Some(packet) => {
                                log::info!(
                                    "MQTT-3.1.0-1: Expected CONNECT packet, received {}",
                                    packet.packet_type()
                                );
                                Either::B(err(MqttError::Unexpected(
                                    packet,
                                    "MQTT-3.1.0-1: Expected CONNECT packet",
                                )))
                            }
                            None => {
                                log::trace!("mqtt client disconnected",);
                                Either::B(err(MqttError::Disconnected))
                            }
                        }
                        .from_err()
                    })
            })
            .into_service()
        })
    })
}
