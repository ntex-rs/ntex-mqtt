use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use futures::{FutureExt, SinkExt, StreamExt};
use ntex::channel::mpsc;
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::framed;
use ntex::service::{apply, apply_fn, boxed, fn_factory, pipeline_factory, unit_config};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::util::timeout::{Timeout, TimeoutError};

use crate::codec3 as mqtt;
use crate::connect::{Connect, ConnectAck};
use crate::default::{SubsNotImplemented, UnsubsNotImplemented};
use crate::dispatcher::factory;
use crate::error::MqttError;
use crate::publish::Publish;
use crate::session::Session;
use crate::sink::MqttSink;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

/// Mqtt Server
pub struct MqttServer<Io, St, C: ServiceFactory> {
    connect: C,
    subscribe: boxed::BoxServiceFactory<
        Session<St>,
        Subscribe,
        SubscribeResult,
        MqttError<C::Error>,
        MqttError<C::Error>,
    >,
    unsubscribe: boxed::BoxServiceFactory<
        Session<St>,
        Unsubscribe,
        (),
        MqttError<C::Error>,
        MqttError<C::Error>,
    >,
    disconnect: Option<Rc<dyn Fn(&Session<St>, bool)>>,
    max_size: usize,
    inflight: usize,
    handshake_timeout: usize,
    disconnect_timeout: usize,
    _t: PhantomData<(Io, St)>,
}

impl<Io, St, C> MqttServer<Io, St, C>
where
    St: 'static,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>
        + 'static,
    C::Error: fmt::Debug,
{
    /// Create server factory and provide connect service
    pub fn new<F>(connect: F) -> MqttServer<Io, St, C>
    where
        F: IntoServiceFactory<C>,
    {
        MqttServer {
            connect: connect.into_factory(),
            subscribe: boxed::factory(
                pipeline_factory(SubsNotImplemented::default())
                    .map_err(MqttError::Service)
                    .map_init_err(MqttError::Service),
            ),
            unsubscribe: boxed::factory(
                pipeline_factory(UnsubsNotImplemented::default())
                    .map_err(MqttError::Service)
                    .map_init_err(MqttError::Service),
            ),
            max_size: 0,
            inflight: 15,
            disconnect: None,
            handshake_timeout: 0,
            disconnect_timeout: 3000,
            _t: PhantomData,
        }
    }
}

impl<Io, St, C> MqttServer<Io, St, C>
where
    St: 'static,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>
        + 'static,
    C::Error: fmt::Debug,
{
    /// Set handshake timeout in millis.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: usize) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set server connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(mut self, val: usize) -> Self {
        self.disconnect_timeout = val;
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
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
        F: IntoServiceFactory<Srv>,
        Srv: ServiceFactory<
                Config = Session<St>,
                Request = Subscribe,
                Response = SubscribeResult,
            > + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        self.subscribe = boxed::factory(
            subscribe
                .into_factory()
                .map_err(|e| MqttError::Service(e.into()))
                .map_init_err(|e| MqttError::Service(e.into())),
        );
        self
    }

    /// Service to execute for unsubscribe packet
    pub fn unsubscribe<F, Srv>(mut self, unsubscribe: F) -> Self
    where
        F: IntoServiceFactory<Srv>,
        Srv: ServiceFactory<Config = Session<St>, Request = Unsubscribe, Response = ()>
            + 'static,
        C::Error: From<Srv::Error> + From<Srv::InitError>,
    {
        self.unsubscribe = boxed::factory(
            unsubscribe
                .into_factory()
                .map_err(|e| MqttError::Service(e.into()))
                .map_init_err(|e| MqttError::Service(e.into())),
        );
        self
    }

    /// Callback to execute on disconnect
    ///
    /// Second parameter indicates error occured during disconnect.
    pub fn disconnect<F, Out>(mut self, disconnect: F) -> Self
    where
        F: Fn(&Session<St>, bool) -> Out + 'static,
        Out: Future + 'static,
    {
        self.disconnect = Some(Rc::new(move |st: &Session<St>, err| {
            let fut = disconnect(st, err);
            ntex::rt::spawn(fut.map(|_| ()));
        }));
        self
    }

    /// Set service to execute for publish packet and create service factory
    pub fn finish<F, P>(
        self,
        publish: F,
    ) -> impl ServiceFactory<Config = (), Request = Io, Response = (), Error = MqttError<C::Error>>
    where
        Io: AsyncRead + AsyncWrite + Unpin + 'static,
        F: IntoServiceFactory<P> + 'static,
        P: ServiceFactory<Config = Session<St>, Request = Publish, Response = ()> + 'static,
        C::Error: From<P::Error> + From<P::InitError> + fmt::Debug,
    {
        let connect = self.connect;
        let max_size = self.max_size;
        let handshake_timeout = self.handshake_timeout;
        let disconnect = self.disconnect;
        let disconnect_timeout = self.disconnect_timeout;
        let publish = publish
            .into_factory()
            .map_err(|e| MqttError::Service(e.into()))
            .map_init_err(|e| MqttError::Service(e.into()));

        unit_config(
            framed::FactoryBuilder::new(connect_service_factory(
                connect,
                max_size,
                self.inflight,
                handshake_timeout,
            ))
            .disconnect_timeout(disconnect_timeout)
            .build(factory(
                publish,
                self.subscribe,
                self.unsubscribe,
                disconnect,
            ))
            .map_err(|e| match e {
                framed::ServiceError::Service(e) => e,
                framed::ServiceError::Encoder(e) => MqttError::Protocol(e),
                framed::ServiceError::Decoder(e) => MqttError::Protocol(e),
            }),
        )
    }
}

fn connect_service_factory<Io, St, C>(
    factory: C,
    max_size: usize,
    inflight: usize,
    handshake_timeout: usize,
) -> impl ServiceFactory<
    Config = (),
    Request = framed::Connect<Io, mqtt::Codec>,
    Response = framed::ConnectResult<
        Io,
        Session<St>,
        mqtt::Codec,
        mpsc::Receiver<mqtt::Packet>,
    >,
    Error = MqttError<C::Error>,
>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    C: ServiceFactory<Config = (), Request = Connect<Io>, Response = ConnectAck<Io, St>>,
    C::Error: fmt::Debug,
{
    apply(
        Timeout::new(Duration::from_millis(handshake_timeout as u64)),
        fn_factory(move || {
            let fut = factory.new_service(());

            async move {
                let service = Rc::new(fut.await?);

                Ok::<_, C::InitError>(apply_fn(
                    service.map_err(MqttError::Service),
                    move |conn: framed::Connect<Io, mqtt::Codec>, service| {
                        log::trace!("Starting mqtt handshake");
                        let srv = service.clone();
                        let mut framed = conn.codec(mqtt::Codec::new().max_size(max_size));

                        async move {
                            // read first packet
                            let packet = framed
                                .next()
                                .await
                                .ok_or_else(|| {
                                    log::trace!("Server mqtt is disconnected during handshake");
                                    MqttError::Disconnected
                                })
                                .and_then(|res| {
                                    res.map_err(|e| {
                                        log::trace!(
                                            "Error is received during mqtt handshake: {:?}",
                                            e
                                        );
                                        MqttError::Protocol(e)
                                    })
                                })?;

                            match packet {
                                mqtt::Packet::Connect(connect) => {
                                    let (tx, rx) = mpsc::channel();
                                    let sink = MqttSink::new(tx);

                                    // authenticate mqtt connection
                                    let mut ack = srv
                                        .call(Connect::new(connect, framed, sink, inflight))
                                        .await?;

                                    match ack.session {
                                        Some(session) => {
                                            log::trace!(
                                                "Sending: {:#?}",
                                                mqtt::Packet::ConnectAck {
                                                    session_present: ack.session_present,
                                                    return_code:
                                                        mqtt::ConnectCode::ConnectionAccepted,
                                                }
                                            );
                                            let sink = ack.sink;
                                            ack.io
                                                .send(mqtt::Packet::ConnectAck {
                                                    session_present: ack.session_present,
                                                    return_code:
                                                        mqtt::ConnectCode::ConnectionAccepted,
                                                })
                                                .await?;

                                            Ok(ack.io.out(rx).state(Session::new(
                                                session,
                                                sink,
                                                ack.keep_alive,
                                                ack.inflight,
                                            )))
                                        }
                                        None => {
                                            log::trace!(
                                                "Sending: {:#?}",
                                                mqtt::Packet::ConnectAck {
                                                    session_present: false,
                                                    return_code: ack.return_code,
                                                }
                                            );

                                            ack.io
                                                .send(mqtt::Packet::ConnectAck {
                                                    session_present: false,
                                                    return_code: ack.return_code,
                                                })
                                                .await?;
                                            Err(MqttError::Disconnected)
                                        }
                                    }
                                }
                                packet => {
                                    log::info!(
                                        "MQTT-3.1.0-1: Expected CONNECT packet, received {}",
                                        packet.packet_type()
                                    );
                                    Err(MqttError::Unexpected(
                                        packet,
                                        "MQTT-3.1.0-1: Expected CONNECT packet",
                                    ))
                                }
                            }
                        }
                    },
                ))
            }
        }),
    )
    .map_err(|e| match e {
        TimeoutError::Service(e) => e,
        TimeoutError::Timeout => MqttError::HandshakeTimeout,
    })
}
