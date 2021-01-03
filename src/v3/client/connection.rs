use std::marker::PhantomData;
use std::time::{Duration, Instant};

use futures::future::{err, ok, Either, Future, TryFutureExt};
use ntex::codec::{AsyncRead, AsyncWrite};
use ntex::router::{IntoPattern, Router, RouterBuilder};
use ntex::rt::time::{delay_until, Instant as RtInstant};
use ntex::service::{apply_fn, boxed::BoxService, into_service, IntoService, Service};

use crate::error::{MqttError, ProtocolError};
use crate::io::{DispatcherItem, IoDispatcher, IoState, Timer};
use crate::v3::{codec, sink::MqttSink, ControlResult, Publish};

use super::control::ControlMessage;
use super::dispatcher::create_dispatcher;

/// Mqtt client
pub struct Client<Io> {
    io: Io,
    sink: MqttSink,
    state: IoState<codec::Codec>,
    keepalive: u16,
    disconnect_timeout: u16,
    session_present: bool,
    max_receive: usize,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: T,
        state: IoState<codec::Codec>,
        session_present: bool,
        keepalive_timeout: u16,
        disconnect_timeout: u16,
        max_send: usize,
        max_receive: usize,
    ) -> Self {
        let sink = MqttSink::new(state.clone(), max_send, Default::default());

        Client {
            io,
            sink,
            state,
            session_present,
            disconnect_timeout,
            max_receive,
            keepalive: keepalive_timeout,
        }
    }
}

impl<Io> Client<Io>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> MqttSink {
        self.sink.clone()
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.session_present
    }

    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, U, E>(self, address: T, service: F) -> ClientRouter<Io, E, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U>,
        U: Service<Request = Publish, Response = ()> + 'static,
        E: From<U::Error>,
    {
        let mut builder = Router::build();
        builder.path(address, 0);
        let handlers = vec![ntex::boxed::service(service.into_service())];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            sink: self.sink,
            state: self.state,
            keepalive: self.keepalive,
            disconnect_timeout: self.disconnect_timeout,
            max_receive: self.max_receive,
            _t: PhantomData,
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(self.sink.clone(), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.sink,
            self.max_receive,
            into_service(|pkt| ok(either::Right(pkt))),
            into_service(|msg: ControlMessage| ok::<_, MqttError<()>>(msg.disconnect())),
        );

        let _ = IoDispatcher::with(
            self.io,
            self.state,
            apply_fn(dispatcher, |req: DispatcherItem<codec::Codec>, srv| match req {
                DispatcherItem::Item(req) => Either::Left(srv.call(req)),
                DispatcherItem::KeepAliveTimeout => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::KeepAliveTimeout)))
                }
                DispatcherItem::EncoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Encode(e))))
                }
                DispatcherItem::DecoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Decode(e))))
                }
                DispatcherItem::IoError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Io(e))))
                }
            }),
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }

    /// Run client with provided control messages handler
    pub async fn start<F, S, E>(self, service: F) -> Result<(), MqttError<E>>
    where
        E: 'static,
        F: IntoService<S> + 'static,
        S: Service<Request = ControlMessage, Response = ControlResult, Error = E> + 'static,
    {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(self.sink.clone(), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.sink,
            self.max_receive,
            into_service(|pkt| ok(either::Right(pkt))),
            service.into_service().map_err(MqttError::Service),
        );

        IoDispatcher::with(
            self.io,
            self.state,
            apply_fn(dispatcher, |req: DispatcherItem<codec::Codec>, srv| match req {
                DispatcherItem::Item(req) => Either::Left(srv.call(req)),
                DispatcherItem::KeepAliveTimeout => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::KeepAliveTimeout)))
                }
                DispatcherItem::EncoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Encode(e))))
                }
                DispatcherItem::DecoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Decode(e))))
                }
                DispatcherItem::IoError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Io(e))))
                }
            }),
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await
    }
}

type Handler<E> = BoxService<Publish, (), E>;

/// Mqtt client with routing capabilities
pub struct ClientRouter<Io, Err, PErr> {
    builder: RouterBuilder<usize>,
    handlers: Vec<Handler<PErr>>,
    io: Io,
    state: IoState<codec::Codec>,
    sink: MqttSink,
    keepalive: u16,
    disconnect_timeout: u16,
    max_receive: usize,
    _t: PhantomData<Err>,
}

impl<Io, Err, PErr> ClientRouter<Io, Err, PErr>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
    Err: From<PErr> + 'static,
    PErr: 'static,
{
    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S>,
        S: Service<Request = Publish, Response = (), Error = PErr> + 'static,
    {
        self.builder.path(address, self.handlers.len());
        self.handlers.push(ntex::boxed::service(service.into_service()));
        self
    }

    /// Run client with default control messages handler
    pub async fn start_default(self) {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(self.sink.clone(), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.sink,
            self.max_receive,
            dispatch(self.builder.finish(), self.handlers),
            into_service(|msg: ControlMessage| ok::<_, MqttError<Err>>(msg.disconnect())),
        );

        let _ = IoDispatcher::with(
            self.io,
            self.state,
            apply_fn(dispatcher, |req: DispatcherItem<codec::Codec>, srv| match req {
                DispatcherItem::Item(req) => Either::Left(srv.call(req)),
                DispatcherItem::KeepAliveTimeout => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::KeepAliveTimeout)))
                }
                DispatcherItem::EncoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Encode(e))))
                }
                DispatcherItem::DecoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Decode(e))))
                }
                DispatcherItem::IoError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Io(e))))
                }
            }),
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await;
    }

    /// Run client and handle control messages
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S> + 'static,
        S: Service<Request = ControlMessage, Response = ControlResult, Error = Err> + 'static,
    {
        if self.keepalive > 0 {
            ntex::rt::spawn(keepalive(self.sink.clone(), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.sink,
            self.max_receive,
            dispatch(self.builder.finish(), self.handlers),
            service.into_service().map_err(MqttError::Service),
        );

        IoDispatcher::with(
            self.io,
            self.state,
            apply_fn(dispatcher, |req: DispatcherItem<codec::Codec>, srv| match req {
                DispatcherItem::Item(req) => Either::Left(srv.call(req)),
                DispatcherItem::KeepAliveTimeout => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::KeepAliveTimeout)))
                }
                DispatcherItem::EncoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Encode(e))))
                }
                DispatcherItem::DecoderError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Decode(e))))
                }
                DispatcherItem::IoError(e) => {
                    Either::Right(err(MqttError::Protocol(ProtocolError::Io(e))))
                }
            }),
            Timer::with(Duration::from_secs(1)),
        )
        .keepalive_timeout(0)
        .disconnect_timeout(self.disconnect_timeout)
        .await
    }
}

fn dispatch<Err, PErr>(
    router: Router<usize>,
    handlers: Vec<Handler<PErr>>,
) -> impl Service<Request = Publish, Response = either::Either<(), Publish>, Error = MqttError<Err>>
where
    PErr: 'static,
    Err: From<PErr>,
{
    into_service(move |mut req: Publish| {
        if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
            // exec handler
            return Either::Left(call(req, &handlers[*idx]).map_err(MqttError::Service));
        }
        Either::Right(ok::<_, MqttError<Err>>(either::Right(req)))
    })
}

fn call<S, Err, PErr>(
    req: Publish,
    srv: &S,
) -> impl Future<Output = Result<either::Either<(), Publish>, Err>>
where
    S: Service<Request = Publish, Response = (), Error = PErr>,
    Err: From<PErr>,
{
    let fut = srv.call(req);

    async move {
        match fut.await {
            Ok(_) => Ok(either::Left(())),
            Err(err) => Err(err.into()),
        }
    }
}

async fn keepalive(sink: MqttSink, timeout: u16) {
    log::debug!("start mqtt client keep-alive task");

    let keepalive = Duration::from_secs(timeout as u64);
    loop {
        let expire = RtInstant::from_std(Instant::now() + keepalive);
        delay_until(expire).await;

        if !sink.ping() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }
    }
}
