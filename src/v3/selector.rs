use std::{fmt, future::Future, marker, rc::Rc, task::Context, task::Poll};

use ntex::io::{Filter, Io, IoBoxed};
use ntex::service::{boxed, Service, ServiceFactory};
use ntex::time::{Deadline, Millis, Seconds};
use ntex::util::{select, BoxFuture, Either};

use crate::error::{MqttError, ProtocolError};

use super::control::{ControlMessage, ControlResult};
use super::handshake::{Handshake, HandshakeAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, MqttServer, Publish, Session};

pub(crate) type SelectItem = (Handshake, Deadline);

type ServerFactory<Err, InitErr> =
    boxed::BoxServiceFactory<(), SelectItem, Either<SelectItem, ()>, MqttError<Err>, InitErr>;

type Server<Err> = boxed::BoxService<SelectItem, Either<SelectItem, ()>, MqttError<Err>>;

/// Mqtt server selector
///
/// Selector allows to choose different mqtt server impls depends on
/// connectt packet.
pub struct Selector<Err, InitErr> {
    servers: Vec<ServerFactory<Err, InitErr>>,
    max_size: u32,
    handshake_timeout: Millis,
    pool: Rc<MqttSinkPool>,
    _t: marker::PhantomData<(Err, InitErr)>,
}

impl<Err, InitErr> Selector<Err, InitErr> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Selector {
            servers: Vec::new(),
            max_size: 0,
            handshake_timeout: Millis(10000),
            pool: Default::default(),
            _t: marker::PhantomData,
        }
    }
}

impl<Err, InitErr> Selector<Err, InitErr>
where
    Err: 'static,
    InitErr: 'static,
{
    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is 10 seconds.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout.into();
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: u32) -> Self {
        self.max_size = size;
        self
    }

    /// Add server variant
    pub fn variant<F, R, St, C, Cn, P>(
        mut self,
        check: F,
        mut server: MqttServer<St, C, Cn, P>,
    ) -> Self
    where
        F: Fn(&Handshake) -> R + 'static,
        R: Future<Output = Result<bool, Err>> + 'static,
        St: 'static,
        C: ServiceFactory<
                Handshake,
                Response = HandshakeAck<St>,
                Error = Err,
                InitError = InitErr,
            > + 'static,
        Cn: ServiceFactory<ControlMessage<Err>, Session<St>, Response = ControlResult>
            + 'static,
        P: ServiceFactory<Publish, Session<St>, Response = ()> + 'static,
        C::Error: From<Cn::Error>
            + From<Cn::InitError>
            + From<P::Error>
            + From<P::InitError>
            + fmt::Debug,
    {
        server.pool = self.pool.clone();
        self.servers.push(boxed::factory(server.finish_selector(check)));
        self
    }
}

impl<Err, InitErr> Selector<Err, InitErr>
where
    Err: 'static,
    InitErr: 'static,
{
    async fn create_service(&self) -> Result<SelectorService<Err>, InitErr> {
        let mut servers = Vec::new();
        for fut in self.servers.iter().map(|srv| srv.create(())) {
            servers.push(fut.await?);
        }
        Ok(SelectorService {
            max_size: self.max_size,
            handshake_timeout: self.handshake_timeout,
            pool: self.pool.clone(),
            servers: Rc::new(servers),
        })
    }
}

impl<F, Err, InitErr> ServiceFactory<Io<F>> for Selector<Err, InitErr>
where
    F: Filter,
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type InitError = InitErr;
    type Service = SelectorService<Err>;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>>;

    fn create(&self, _: ()) -> Self::Future<'_> {
        Box::pin(self.create_service())
    }
}

impl<Err, InitErr> ServiceFactory<IoBoxed> for Selector<Err, InitErr>
where
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type InitError = InitErr;
    type Service = SelectorService<Err>;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>>;

    fn create(&self, _: ()) -> Self::Future<'_> {
        Box::pin(self.create_service())
    }
}

impl<Err, InitErr> ServiceFactory<(IoBoxed, Deadline)> for Selector<Err, InitErr>
where
    Err: 'static,
    InitErr: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type InitError = InitErr;
    type Service = SelectorService<Err>;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Self::InitError>>;

    fn create(&self, _: ()) -> Self::Future<'_> {
        Box::pin(self.create_service())
    }
}

pub struct SelectorService<Err> {
    servers: Rc<Vec<Server<Err>>>,
    max_size: u32,
    handshake_timeout: Millis,
    pool: Rc<MqttSinkPool>,
}

impl<F, Err> Service<Io<F>> for SelectorService<Err>
where
    F: Filter,
    Err: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Future<'f> = BoxFuture<'f, Result<(), MqttError<Err>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<IoBoxed>::poll_ready(self, cx)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        Service::<IoBoxed>::poll_shutdown(self, cx)
    }

    #[inline]
    fn call(&self, io: Io<F>) -> Self::Future<'_> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io))
    }
}

impl<Err> Service<IoBoxed> for SelectorService<Err>
where
    Err: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Future<'f> = BoxFuture<'f, Result<(), MqttError<Err>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut ready = true;
        for srv in self.servers.iter() {
            ready &= srv.poll_ready(cx)?.is_ready();
        }
        if ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut ready = true;
        for srv in self.servers.iter() {
            ready &= srv.poll_shutdown(cx).is_ready()
        }
        if ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn call(&self, io: IoBoxed) -> Self::Future<'_> {
        let servers = self.servers.clone();
        let codec = mqtt::Codec::default();
        codec.set_max_size(self.max_size);
        let shared = Rc::new(MqttShared::new(io.clone(), codec, false, self.pool.clone()));
        let mut timeout = Deadline::new(self.handshake_timeout);

        Box::pin(async move {
            // read first packet
            let result = select(&mut timeout, async {
                io.recv(&shared.codec)
                    .await
                    .map_err(|err| {
                        log::trace!("Error is received during mqtt handshake: {:?}", err);
                        MqttError::from(err)
                    })?
                    .ok_or_else(|| {
                        log::trace!("Server mqtt is disconnected during handshake");
                        MqttError::Disconnected(None)
                    })
            })
            .await;

            let packet = match result {
                Either::Left(_) => Err(MqttError::HandshakeTimeout),
                Either::Right(item) => item,
            }?;

            let connect = match packet {
                mqtt::Packet::Connect(connect) => connect,
                packet => {
                    log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {:?}", packet);
                    return Err(MqttError::Protocol(ProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "Expected CONNECT packet [MQTT-3.1.0-1]",
                    )));
                }
            };

            // call servers
            let mut item = (Handshake::new(connect, io, shared), timeout);
            for srv in servers.iter() {
                match srv.call(item).await? {
                    Either::Left(result) => {
                        item = result;
                    }
                    Either::Right(_) => return Ok(()),
                }
            }
            log::error!("Cannot handle CONNECT packet {:?}", item.0);
            Err(MqttError::ServerError("Cannot handle CONNECT packet"))
        })
    }
}

impl<Err> Service<(IoBoxed, Deadline)> for SelectorService<Err>
where
    Err: 'static,
{
    type Response = ();
    type Error = MqttError<Err>;
    type Future<'f> = BoxFuture<'f, Result<(), MqttError<Err>>>;

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::<IoBoxed>::poll_ready(self, cx)
    }

    #[inline]
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        Service::<IoBoxed>::poll_shutdown(self, cx)
    }

    #[inline]
    fn call(&self, (io, mut timeout): (IoBoxed, Deadline)) -> Self::Future<'_> {
        let servers = self.servers.clone();
        let codec = mqtt::Codec::default();
        codec.set_max_size(self.max_size);
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, self.pool.clone()));

        Box::pin(async move {
            // read first packet
            let result = select(&mut timeout, async {
                io.recv(&shared.codec)
                    .await
                    .map_err(|err| {
                        log::trace!("Error is received during mqtt handshake: {:?}", err);
                        MqttError::from(err)
                    })?
                    .ok_or_else(|| {
                        log::trace!("Server mqtt is disconnected during handshake");
                        MqttError::Disconnected(None)
                    })
            })
            .await;

            let packet = match result {
                Either::Left(_) => Err(MqttError::HandshakeTimeout),
                Either::Right(item) => item,
            }?;

            let connect = match packet {
                mqtt::Packet::Connect(connect) => connect,
                packet => {
                    log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {:?}", packet);
                    return Err(MqttError::Protocol(ProtocolError::unexpected_packet(
                        packet.packet_type(),
                        "MQTT-3.1.0-1: Expected CONNECT packet",
                    )));
                }
            };

            // call servers
            let mut item = (Handshake::new(connect, io, shared), timeout);
            for srv in servers.iter() {
                match srv.call(item).await? {
                    Either::Left(result) => {
                        item = result;
                    }
                    Either::Right(_) => return Ok(()),
                }
            }
            log::error!("Cannot handle CONNECT packet {:?}", item.0.packet());
            Err(MqttError::ServerError("Cannot handle CONNECT packet"))
        })
    }
}
