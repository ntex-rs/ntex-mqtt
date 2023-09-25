use std::{
    convert::TryFrom, fmt, future::Future, io, marker, rc::Rc, task::Context, task::Poll,
};

use ntex::io::{Filter, Io, IoBoxed};
use ntex::service::{boxed, Service, ServiceCtx, ServiceFactory};
use ntex::time::{Deadline, Millis, Seconds};
use ntex::util::{select, BoxFuture, Either};

use crate::error::{HandshakeError, MqttError, ProtocolError};

use super::control::{ControlMessage, ControlResult};
use super::handshake::{Handshake, HandshakeAck};
use super::publish::{Publish, PublishAck};
use super::shared::{MqttShared, MqttSinkPool};
use super::{codec as mqtt, MqttServer, Session};

type ServerFactory<Err, InitErr> =
    boxed::BoxServiceFactory<(), Handshake, Either<Handshake, ()>, MqttError<Err>, InitErr>;

type Server<Err> = boxed::BoxService<Handshake, Either<Handshake, ()>, MqttError<Err>>;

/// Mqtt server selector
///
/// Selector allows to choose different mqtt server impls depends on
/// connectt packet.
pub struct Selector<Err, InitErr> {
    servers: Vec<ServerFactory<Err, InitErr>>,
    max_size: u32,
    connect_timeout: Millis,
    pool: Rc<MqttSinkPool>,
    _t: marker::PhantomData<(Err, InitErr)>,
}

impl<Err, InitErr> Selector<Err, InitErr> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Selector {
            servers: Vec::new(),
            max_size: 0,
            connect_timeout: Millis(10000),
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
    /// Set client timeout for first `Connect` frame.
    ///
    /// Defines a timeout for reading `Connect` frame. If a client does not transmit
    /// the entire frame within this time, the connection is terminated with
    /// Mqtt::Handshake(HandshakeError::Timeout) error.
    ///
    /// By default, connect timeout is disabled.
    pub fn conenct_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout.into();
        self
    }

    #[deprecated(since = "0.12.1")]
    #[doc(hidden)]
    /// Set handshake timeout.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout.into();
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
        C::Error: From<Cn::Error>
            + From<Cn::InitError>
            + From<P::Error>
            + From<P::InitError>
            + fmt::Debug,
        Cn: ServiceFactory<ControlMessage<Err>, Session<St>, Response = ControlResult>
            + 'static,

        P: ServiceFactory<Publish, Session<St>, Response = PublishAck> + 'static,
        P::Error: fmt::Debug,
        PublishAck: TryFrom<P::Error, Error = C::Error>,
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
            servers,
            max_size: self.max_size,
            connect_timeout: self.connect_timeout,
            pool: self.pool.clone(),
        })
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
    servers: Vec<Server<Err>>,
    max_size: u32,
    connect_timeout: Millis,
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
    fn call<'a>(&'a self, io: Io<F>, ctx: ServiceCtx<'a, Self>) -> Self::Future<'a> {
        Service::<IoBoxed>::call(self, IoBoxed::from(io), ctx)
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
    fn call<'a>(&'a self, io: IoBoxed, ctx: ServiceCtx<'a, Self>) -> Self::Future<'a> {
        Service::<(IoBoxed, Deadline)>::call(
            self,
            (io, Deadline::new(self.connect_timeout)),
            ctx,
        )
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
    fn call<'a>(
        &'a self,
        (io, mut timeout): (IoBoxed, Deadline),
        ctx: ServiceCtx<'a, Self>,
    ) -> Self::Future<'a> {
        Box::pin(async move {
            let codec = mqtt::Codec::default();
            codec.set_max_inbound_size(self.max_size);
            let shared = Rc::new(MqttShared::new(io.get_ref(), codec, self.pool.clone()));

            // read first packet
            let result = select(&mut timeout, async {
                io.recv(&shared.codec)
                    .await
                    .map_err(|err| {
                        // log::trace!("Error is received during mqtt handshake: {:?}", err);
                        MqttError::Handshake(HandshakeError::from(err))
                    })?
                    .ok_or_else(|| {
                        log::trace!("Server mqtt is disconnected during handshake");
                        MqttError::Handshake(HandshakeError::Disconnected(None))
                    })
            })
            .await;

            let (packet, size) = match result {
                Either::Left(_) => Err(MqttError::Handshake(HandshakeError::Timeout)),
                Either::Right(item) => item,
            }?;

            let connect = match packet {
                mqtt::Packet::Connect(connect) => connect,
                packet => {
                    log::info!("MQTT-3.1.0-1: Expected CONNECT packet, received {:?}", packet);
                    return Err(MqttError::Handshake(HandshakeError::Protocol(
                        ProtocolError::unexpected_packet(
                            packet.packet_type(),
                            "Expected CONNECT packet [MQTT-3.1.0-1]",
                        ),
                    )));
                }
            };

            // call servers
            let mut item = Handshake::new(connect, size, io, shared);
            for srv in self.servers.iter() {
                match ctx.call(srv, item).await? {
                    Either::Left(result) => {
                        item = result;
                    }
                    Either::Right(_) => return Ok(()),
                }
            }
            log::error!("Cannot handle CONNECT packet {:?}", item);
            Err(MqttError::Handshake(HandshakeError::Disconnected(Some(io::Error::new(
                io::ErrorKind::Other,
                "Cannot handle CONNECT packet",
            )))))
        })
    }
}
