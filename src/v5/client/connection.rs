use std::{cell::RefCell, fmt, marker, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_io::IoBoxed;
use ntex_router::{IntoPattern, Path, Router, RouterBuilder};
use ntex_service::pipeline::{Pipeline, PipelineState};
use ntex_service::{BindCtx, Ctx, IntoService, Service, cfg::Cfg, fn_service};
use ntex_util::time::{Millis, Seconds, sleep};
use ntex_util::{HashMap, future::Either};

use crate::v5::default::ControlService;
use crate::v5::publish::{Publish, PublishAck};
use crate::v5::{ProtocolMessageAck, Session, codec, shared::MqttShared, sink::MqttSink};
use crate::{MqttServiceConfig, control, error::MqttError, io::Dispatcher};

use super::{control::ProtocolMessage, dispatcher::create_dispatcher};

/// Established MQTT 5 connection awaiting a client dispatch loop.
///
/// Obtain a sink before starting the client; acknowledgements and inbound packets
/// are processed only while a `start` method is being polled.
pub struct Client {
    /// Negotiated transport, moved into the dispatch loop when started.
    io: IoBoxed,
    /// Connection state shared with sink handles and the packet dispatcher.
    shared: Rc<MqttShared>,
    /// Ping interval in seconds; zero disables the keep-alive task.
    keepalive: Seconds,
    /// Advertised limit on incoming `QoS` 1 and `QoS` 2 exchanges.
    max_receive: usize,
    /// Configuration retained for dispatch and streamed payload buffering.
    cfg: Cfg<MqttServiceConfig>,
    /// Broker acknowledgement retained for inspection after the handshake.
    pkt: Box<codec::ConnectAck>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::Client")
            .field("keepalive", &self.keepalive)
            .field("max_receive", &self.max_receive)
            .field("cfg", &self.cfg)
            .field("connect", &self.pkt)
            .finish()
    }
}

impl Client {
    /// Retain the completed handshake, transport, and limits until the client is started.
    pub(super) fn new(
        io: IoBoxed,
        shared: Rc<MqttShared>,
        pkt: Box<codec::ConnectAck>,
        max_receive: u16,
        keepalive: Seconds,
        cfg: Cfg<MqttServiceConfig>,
    ) -> Self {
        Client {
            io,
            pkt,
            shared,
            cfg,
            keepalive,
            max_receive: max_receive as usize,
        }
    }
}

impl Client {
    #[inline]
    /// Clone a sending handle sharing this connection and its flow-control state.
    ///
    /// The client dispatch loop must run to process acknowledgements and release
    /// outbound `QoS` credit.
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Return whether the broker reported an existing session in `CONNACK`.
    pub fn session_present(&self) -> bool {
        self.pkt.session_present
    }

    #[inline]
    /// Borrow the broker's `CONNACK` packet retained after the handshake.
    pub fn packet(&self) -> &codec::ConnectAck {
        &self.pkt
    }

    #[inline]
    /// Mutably borrow the retained `CONNACK` packet.
    ///
    /// Changes affect this stored packet only, not limits already applied to the
    /// sink, codec, or keep-alive task.
    pub fn packet_mut(&mut self) -> &mut codec::ConnectAck {
        &mut self.pkt
    }

    /// Register a publish handler for a topic path pattern.
    ///
    /// Patterns use `ntex_router` syntax, not MQTT subscription-filter syntax.
    /// This does not send a subscription to the broker.
    ///
    /// # Panics
    ///
    /// Panics if a supplied pattern is invalid for `ntex_router`.
    pub fn resource<T, F, U, E>(self, address: T, service: F) -> ClientRouter<E, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U, Session<()>, Publish>,
        U: Service<Session<()>, Publish, Res = PublishAck> + 'static,
        E: From<U::Error>,
        PublishAck: TryFrom<U::Error, Error = E>,
    {
        let mut builder = Router::builder();
        builder.path(address, 0);
        let handlers = vec![PipelineState::new(service.into_service())];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            shared: self.shared,
            keepalive: self.keepalive,
            max_receive: self.max_receive,
            cfg: self.cfg,
            _t: marker::PhantomData,
        }
    }

    /// Run the connection until it closes, rejecting incoming publications.
    ///
    /// Default handlers close the connection on any incoming publication or
    /// protocol message delivered to them. Dispatcher errors are discarded.
    pub async fn start_default(self) {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                fn_service(async |pkt| Ok(Either::Left(pkt))),
                fn_service(async |msg: ProtocolMessage| {
                    Ok::<_, ()>(msg.disconnect(codec::Disconnect::default()))
                }),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(
                control::DefaultControlService::<(), codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        let _ = Dispatcher::new(self.io, self.shared, dispatcher, control).await;
    }

    /// Run the connection with a service for unrouted publications and protocol messages.
    ///
    /// Returned acknowledgements are applied by the dispatcher. The default control
    /// service handles connection events and errors; the future completes when
    /// the connection closes.
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<()>>
    where
        F: IntoService<S, Session<()>, ProtocolMessage> + 'static,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = ()> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                fn_service(async |pkt| Ok(Either::Left(pkt))),
                service.into_service(),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(
                control::DefaultControlService::<(), codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Run the connection with custom protocol-message and connection-control services.
    ///
    /// Publications are delivered to the protocol-message service. The control
    /// service handles connection events and errors, and its error type determines
    /// the returned connection error.
    pub async fn start_with_control<F, S, C, E>(
        self,
        service: F,
        control: C,
    ) -> Result<(), MqttError<C::Error>>
    where
        E: fmt::Debug + 'static,
        F: IntoService<S, Session<()>, ProtocolMessage> + 'static,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
        C: Service<Session<()>, control::Control<E>, Res = Option<codec::Encoded>> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                fn_service(async |pkt| Ok(Either::Left(pkt))),
                service.into_service(),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(control, self.shared.clone()),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Consume the client and return its negotiated transport and a codec clone.
    ///
    /// No client dispatch loop or keep-alive task is started.
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

/// Established client connection with topic-pattern handlers for incoming publications.
pub struct ClientRouter<Err, PErr> {
    /// Negotiated transport, moved into the dispatch loop when started.
    io: IoBoxed,
    /// Topic patterns mapped to indices in `handlers`.
    builder: RouterBuilder<usize>,
    /// Owned publish services returning acknowledgement metadata.
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, PErr>>,
    /// Connection state shared with sink handles and the packet dispatcher.
    shared: Rc<MqttShared>,
    /// Ping interval in seconds; zero disables the keep-alive task.
    keepalive: Seconds,
    /// Advertised limit on incoming `QoS` 1 and `QoS` 2 exchanges.
    max_receive: usize,
    /// Configuration retained for dispatch and streamed payload buffering.
    cfg: Cfg<MqttServiceConfig>,
    /// Error types used by the dispatcher and publish handlers.
    _t: marker::PhantomData<(Err, PErr)>,
}

impl<Err, PErr> fmt::Debug for ClientRouter<Err, PErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::ClientRouter")
            .field("keepalive", &self.keepalive)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl<Err, PErr> ClientRouter<Err, PErr>
where
    Err: From<PErr> + fmt::Debug + 'static,
    PublishAck: TryFrom<PErr, Error = PErr>,
    PErr: fmt::Debug + 'static,
{
    #[must_use]
    /// Register a publish handler for a topic path pattern.
    ///
    /// Patterns use `ntex_router` syntax, not MQTT subscription-filter syntax.
    /// This does not send a subscription to the broker.
    ///
    /// # Panics
    ///
    /// Panics if a supplied pattern is invalid for `ntex_router`.
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S, Session<()>, Publish>,
        S: Service<Session<()>, Publish, Res = PublishAck, Error = PErr> + 'static,
    {
        self.builder.path(address, self.handlers.len());
        self.handlers
            .push(PipelineState::new(service.into_service()));
        self
    }

    /// Run the routed connection until it closes, discarding dispatcher errors.
    ///
    /// Default handlers acknowledge `PublishRelease` for routed `QoS` 2 publications
    /// and close the connection on any unrouted publication or other protocol
    /// message delivered to them.
    pub async fn start_default(self) {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                dispatch(self.builder.build(), self.handlers),
                fn_service(async |msg: ProtocolMessage| {
                    Ok(match msg {
                        ProtocolMessage::PublishRelease(msg) => msg.ack(),
                        msg => msg.disconnect(codec::Disconnect::default()),
                    })
                }),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(
                control::DefaultControlService::<Err, codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        let _ = Dispatcher::new(self.io, self.shared, dispatcher, control).await;
    }

    /// Run the connection with a service for unrouted publications and protocol messages.
    ///
    /// Returned acknowledgements are applied by the dispatcher. The default control
    /// service handles connection events and errors; the future completes when
    /// the connection closes.
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S, Session<()>, ProtocolMessage>,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = PErr> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                dispatch(self.builder.build(), self.handlers),
                service.into_service(),
                self.max_receive,
                16,
                self.cfg,
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(
                control::DefaultControlService::<Err, codec::Encoded>::default(),
                self.shared.clone(),
            ),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Consume the client and return its negotiated transport and a codec clone.
    ///
    /// No client dispatch loop or keep-alive task is started.
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

/// Build a publish-routing service with the required error conversion.
fn dispatch<PErr>(
    router: Router<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, PErr>>,
) -> impl Service<Session<()>, Publish, Res = Either<Publish, PublishAck>, Error = PErr>
where
    PErr: 'static,
    PublishAck: TryFrom<PErr, Error = PErr>,
{
    RouteDispatch {
        router,
        handlers,
        aliases: RefCell::new(HashMap::default()),
    }
}

/// Route incoming publications and propagate handler readiness and shutdown.
struct RouteDispatch<E> {
    /// Compiled topic patterns whose values index `handlers`.
    router: Router<usize>,
    /// Publish services whose errors may be converted into acknowledgements.
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, E>>,
    /// Per-connection aliases retaining the matched handler and captured topic path.
    aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>>,
}

impl<E: 'static> Service<Session<()>, Publish> for RouteDispatch<E>
where
    PublishAck: TryFrom<E, Error = E>,
{
    type Res = Either<Publish, PublishAck>;
    type Error = E;

    fn bind(&self, ctx: BindCtx<'_>) {
        for handler in &self.handlers {
            handler.bind_to(&ctx);
        }
    }

    async fn ready(&self, ctx: Ctx<'_, Self, Session<()>>) -> Result<(), E> {
        for handler in &self.handlers {
            handler.ready(ctx.st()).await?;
        }
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, Session<()>>) {
        for handler in &self.handlers {
            handler.shutdown(ctx.st()).await;
        }
    }

    async fn call(
        &self,
        mut req: Publish,
        ctx: Ctx<'_, Self, Session<()>>,
    ) -> Result<Self::Res, E> {
        let idx = if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    self.aliases
                        .borrow_mut()
                        .insert(alias, (*idx, req.topic().clone()));
                }
                *idx
            } else {
                return Ok(Either::Left(req));
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = self.aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                *req.topic_mut() = item.1.clone();
                item.0
            } else {
                log::error!("Unknown topic alias: {alias:?}");
                return Ok(Either::Left(req));
            }
        } else {
            return Ok(Either::Left(req));
        };

        // exec handler
        match self.handlers[idx].call(req, ctx.st()).await {
            Ok(ack) => Ok(Either::Right(ack)),
            Err(err) => match PublishAck::try_from(err) {
                Ok(ack) => Ok(Either::Right(ack)),
                Err(err) => Err(err),
            },
        }
    }
}

/// Run periodic pings using the interval negotiated in seconds.
async fn keepalive(sink: MqttSink, timeout: Seconds) {
    keepalive_interval(sink, Millis::from(timeout)).await;
}

/// Ping periodically and close on a missing response, timing only after writes unblock.
async fn keepalive_interval(sink: MqttSink, interval: Millis) {
    log::debug!("start mqtt client keep-alive task");

    loop {
        sleep(interval).await;

        if !sink.is_open() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }

        if sink.is_ping_pending() {
            // PINGRESP is not received within the keep-alive interval,
            // the client closes the connection (MQTT 5.0, 3.1.2.10),
            // Keep Alive timeout reason code is sent by the server only
            log::debug!("PINGRESP is not received within {interval:?}, closing connection");
            sink.close_with_reason(codec::Disconnect {
                reason_code: codec::DisconnectReasonCode::UnspecifiedError,
                reason_string: Some(ByteString::from_static("Keep Alive timeout")),
                ..Default::default()
            });
            break;
        }

        if !sink.ping() {
            // connection is closed
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }

        // PINGREQ may wait behind a streaming payload or a full write buffer,
        // the timeout starts once it can be written
        if !sink.wait_write_unblocked().await {
            log::debug!("mqtt client connection is closed, stopping keep-alive task");
            break;
        }
    }
}

/// Unit tests for routing lifecycle, keep-alive timing, and sink backpressure.
#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ntex_bytes::Bytes;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;

    /// Fixed-header byte identifying an MQTT ping request.
    const PINGREQ: u8 = 0b1100_0000;
    /// Shortened keep-alive interval used by timing tests.
    const INTERVAL: Millis = Millis(200);

    /// Create a paired test transport, shared client state, and a sending handle.
    fn setup() -> (IoTest, Io, Rc<MqttShared>, MqttSink) {
        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_client();
        let sink = MqttSink::new(shared.clone());
        (client, io, shared, sink)
    }

    /// Wait until a ping is encoded, failing if the bounded retry budget expires.
    async fn wait_ping_pending(sink: &MqttSink) {
        let mut n = 0;
        while !sink.is_ping_pending() {
            n += 1;
            assert!(n < 1000, "PINGREQ is not encoded");
            sleep(Millis(5)).await;
        }
    }

    /// Handler failure deliberately not convertible into a publish acknowledgement.
    #[derive(Debug)]
    struct TestError;

    impl TryFrom<TestError> for PublishAck {
        type Error = TestError;

        fn try_from(err: TestError) -> Result<Self, TestError> {
            Err(err)
        }
    }

    /// Reject readiness and count shutdown calls through shared state.
    struct Handler(Rc<std::cell::Cell<usize>>);

    impl Service<Session<()>, Publish> for Handler {
        type Res = PublishAck;
        type Error = TestError;

        async fn ready(&self, _: Ctx<'_, Self, Session<()>>) -> Result<(), TestError> {
            Err(TestError)
        }

        async fn call(
            &self,
            _: Publish,
            _: Ctx<'_, Self, Session<()>>,
        ) -> Result<PublishAck, TestError> {
            Err(TestError)
        }

        async fn shutdown(&self, _: Ctx<'_, Self, Session<()>>) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// STATEMENT: Topic routing propagates handler readiness failures and shuts handlers down.
    ///
    /// HOW:
    /// - Build an empty router with a handler that rejects readiness and counts shutdowns.
    /// - Assert readiness fails, then shut down the routing service and observe one shutdown call.
    #[ntex::test]
    async fn test_route_dispatch_ready_shutdown() {
        let (_client, io, _shared, sink) = setup();
        let count = Rc::new(std::cell::Cell::new(0));
        let svc = Pipeline::new(
            Session::new((), sink, io.shared()),
            dispatch(
                Router::<usize>::builder().build(),
                vec![PipelineState::new(Handler(count.clone()))],
            ),
        );

        assert!(svc.ready().await.is_err());
        svc.shutdown().await;
        assert_eq!(count.get(), 1);
    }

    /// STATEMENT: A ping response deadline starts only after the ping can follow a streamed payload.
    ///
    /// HOW:
    /// - Observe an initial `PINGREQ` and clear its pending flag to simulate a response.
    /// - Hold a streaming publication open across the next ping and verify the sink stays open.
    /// - Finish the stream, withhold `PINGRESP`, and check the elapsed interval and disconnect packet.
    #[ntex::test]
    async fn test_pingresp_timeout() {
        let (client, _io, shared, sink) = setup();
        ntex_util::spawn(keepalive_interval(sink.clone(), INTERVAL));

        // PINGRESP clears pending ping
        assert_eq!(client.read().await.unwrap()[0], PINGREQ);
        assert!(sink.is_ping_pending());
        shared.set_ping_pending(false);

        // PINGREQ waits behind a streaming payload, the timeout does not start
        let stream = sink.publish("a").stream_at_most_once(2).await.unwrap();
        stream.send(Bytes::from_static(b"a")).await.unwrap();
        let _ = client.read().await.unwrap();
        wait_ping_pending(&sink).await;

        // the payload completes within the interval after PINGREQ is encoded
        sleep(Millis(INTERVAL.0 / 2)).await;
        assert!(sink.is_open());

        // the timeout starts once PINGREQ can be written
        let start = Instant::now();
        stream.send(Bytes::from_static(b"b")).await.unwrap();
        let res = client.read().await.unwrap();
        assert_eq!(res[res.len() - 2], PINGREQ);

        // PINGRESP is not received, DISCONNECT is sent and the connection is closed
        let res = client.read().await.unwrap();
        assert!(start.elapsed() >= Duration::from(INTERVAL));
        assert!(!sink.is_open());
        assert_eq!(res[0], 0b1110_0000);
        // Unspecified error reason code
        assert_eq!(res[2], 0x80);
    }

    /// STATEMENT: Write backpressure postpones the ping response deadline until writes unblock.
    ///
    /// HOW:
    /// - Fill a blocked test transport and wait for a pending ping.
    /// - Wait two keep-alive intervals and verify the ping remains pending without closing the sink.
    /// - Drain the transport, withhold `PINGRESP`, and check the interval before the disconnect.
    #[ntex::test]
    async fn test_pingresp_timeout_write_backpressure() {
        use ntex_util::future::lazy;

        let (client, io, shared, sink) = setup();
        ntex_util::spawn(keepalive_interval(sink.clone(), INTERVAL));

        assert_eq!(client.read().await.unwrap()[0], PINGREQ);
        shared.set_ping_pending(false);

        // PINGREQ waits behind a full write buffer, the timeout does not start
        client.remote_buffer_cap(0);
        sink.publish("a")
            .send_at_most_once(Bytes::from(vec![0u8; 128 * 1024]))
            .await
            .unwrap();
        assert!(lazy(|cx| io.poll_flush(cx, false).is_pending()).await);
        assert!(io.is_wr_backpressure());
        wait_ping_pending(&sink).await;
        sleep(Millis(INTERVAL.0 * 2)).await;
        assert!(sink.is_ping_pending());
        assert!(sink.is_open());

        // the timeout starts once PINGREQ can be written
        let start = Instant::now();
        client.remote_buffer_cap(1024 * 1024);
        let mut buf = Vec::new();
        while !buf.ends_with(&[PINGREQ, 0]) {
            buf.extend_from_slice(&client.read().await.unwrap());
        }

        // PINGRESP is not received, DISCONNECT is sent and the connection is closed
        let res = client.read().await.unwrap();
        assert!(start.elapsed() >= Duration::from(INTERVAL));
        assert!(!sink.is_open());
        assert_eq!(res[0], 0b1110_0000);
        // Unspecified error reason code
        assert_eq!(res[2], 0x80);
    }

    /// STATEMENT: Sink write readiness recovers even while a backpressure control call is blocked.
    ///
    /// HOW:
    /// - Block transport writes and gate the control service after its enabled-backpressure event.
    /// - Assert the sink is not ready, then drain the transport while keeping the control call blocked.
    /// - Send another publication and observe its encoded bytes before releasing the control gate.
    #[ntex::test]
    async fn test_sink_wr_backpressure_slow_control() {
        use std::cell::Cell;

        use ntex_util::{channel::condition::Condition, time::timeout};

        let (client, io, shared, sink) = setup();
        shared.set_cap(16);
        let cfg = io.shared().get::<MqttServiceConfig>();
        let conn = Client::new(io.into(), shared, Box::default(), 16, Seconds::ZERO, cfg);

        let gate = Condition::new();
        let wr = Rc::new(Cell::new(None));
        let (gate2, wr2) = (gate.clone(), wr.clone());
        ntex_util::spawn(conn.start_with_control(
            fn_service(async |_: ProtocolMessage| Err::<ProtocolMessageAck, _>(())),
            fn_service(move |msg: control::Control<()>| {
                let (gate, wr) = (gate2.clone(), wr2.clone());
                async move {
                    if let control::Control::WrBackpressure(st) = msg {
                        wr.set(Some(st.enabled()));
                        // control service is busy until the gate is open
                        if st.enabled() {
                            gate.wait().await;
                        }
                    }
                    Ok::<_, ()>(None)
                }
            }),
        ));

        // fill the write buffer
        client.remote_buffer_cap(0);
        sink.publish("a")
            .send_at_most_once(Bytes::from(vec![0u8; 128 * 1024]))
            .await
            .unwrap();
        let mut n = 0;
        while wr.get() != Some(true) {
            n += 1;
            assert!(n < 1000, "write backpressure is not enabled");
            sleep(Millis(5)).await;
        }
        assert!(!sink.is_ready());

        // backpressure is released, wr(false) waits for the control service
        client.remote_buffer_cap(1024 * 1024);
        let mut len = 0;
        while len < 128 * 1024 {
            len += client.read().await.unwrap().len();
        }
        let sink2 = sink.clone();
        ntex_util::spawn(async move {
            let _ = sink2.publish("b").send_at_least_once(Bytes::new()).await;
        });
        let mut buf = Vec::new();
        while !buf.ends_with(b"\x32\x06\x00\x01b\x00\x01\x00") {
            buf.extend_from_slice(&timeout(Millis(1000), client.read()).await.unwrap().unwrap());
        }
        assert_eq!(wr.get(), Some(true));
        gate.notify(());
    }
}
