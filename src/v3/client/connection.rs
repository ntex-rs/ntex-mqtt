#![allow(clippy::let_underscore_future)]
use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_io::IoBoxed;
use ntex_router::{IntoPattern, Router, RouterBuilder};
use ntex_service::pipeline::{Pipeline, PipelineState};
use ntex_service::{IntoService, Service, fn_service, fn_service_st};
use ntex_util::future::Either;
use ntex_util::time::{Millis, Seconds, sleep};

use crate::v3::default::ControlService;
use crate::v3::{ProtocolMessageAck, Publish, Session, codec, shared::MqttShared, sink::MqttSink};
use crate::{control, error::MqttError, io::Dispatcher};

use super::{control::ProtocolMessage, dispatcher::create_dispatcher};

/// Mqtt client
pub struct Client {
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    session_present: bool,
    max_receive: u16,
    max_buffer_size: usize,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::Client")
            .field("keepalive", &self.keepalive)
            .field("session_present", &self.session_present)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl Client {
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: IoBoxed,
        shared: Rc<MqttShared>,
        session_present: bool,
        keepalive: Seconds,
        max_receive: u16,
        max_buffer_size: usize,
    ) -> Self {
        Client {
            io,
            shared,
            keepalive,
            session_present,
            max_receive,
            max_buffer_size,
        }
    }
}

impl Client {
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.session_present
    }

    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, U>(self, address: T, service: F) -> ClientRouter<U::Error, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U, Session<()>, Publish>,
        U: Service<Session<()>, Publish, Res = ()> + 'static,
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
            max_buffer_size: self.max_buffer_size,
            _t: PhantomData,
        }
    }

    /// Run client with default handlers.
    ///
    /// Default handlers close connection on any incoming publish or
    /// protocol message.
    pub async fn start_default(self) {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            let _ = ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                self.max_receive,
                self.max_buffer_size,
                fn_service(async |pkt| Ok(Either::Right(pkt))),
                fn_service(async |_: ProtocolMessage| Ok::<_, ()>(ProtocolMessage::disconnect())),
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

    /// Run client with provided protocol-message service.
    ///
    /// Default control service is used.
    pub async fn start<F, S, E>(self, service: F) -> Result<(), MqttError<E>>
    where
        E: fmt::Debug + 'static,
        F: IntoService<S, Session<()>, ProtocolMessage> + 'static,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            let _ = ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                self.max_receive,
                self.max_buffer_size,
                fn_service(async |pkt| Ok(Either::Right(pkt))),
                service.into_service(),
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(
                control::DefaultControlService::<E, codec::Encoded>::default(),
                self.shared.clone(),
            )
            .map_err(MqttError::Service),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Run client with provided protocol-message and control services.
    pub async fn start_with_control<F, S, E, C>(
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
            let _ = ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                self.max_receive,
                self.max_buffer_size,
                fn_service(async |pkt| Ok(Either::Right(pkt))),
                service.into_service(),
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(control, self.shared.clone()).map_err(MqttError::Service),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

/// Mqtt client with routing capabilities
pub struct ClientRouter<Err, PErr> {
    builder: RouterBuilder<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, (), PErr>>,
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    max_receive: u16,
    max_buffer_size: usize,
    _t: PhantomData<Err>,
}

impl<Err, PErr> fmt::Debug for ClientRouter<Err, PErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::ClientRouter")
            .field("keepalive", &self.keepalive)
            .field("max_receive", &self.max_receive)
            .finish()
    }
}

impl<Err, PErr> ClientRouter<Err, PErr>
where
    Err: From<PErr> + fmt::Debug + 'static,
    PErr: 'static,
{
    #[must_use]
    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, S>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoService<S, Session<()>, Publish>,
        S: Service<Session<()>, Publish, Res = (), Error = PErr> + 'static,
    {
        self.builder.path(address, self.handlers.len());
        self.handlers
            .push(PipelineState::new(service.into_service()));
        self
    }

    /// Run client with default handlers.
    ///
    /// Default handlers acknowledge `PublishRelease` of routed `QoS 2` publishes and
    /// close connection on any unrouted publish or other protocol message.
    pub async fn start_default(self) {
        let sink = MqttSink::new(self.shared.clone());
        if self.keepalive.non_zero() {
            let _ = ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                self.max_receive,
                self.max_buffer_size,
                dispatch(self.builder.build(), self.handlers),
                fn_service(async |msg: ProtocolMessage| {
                    Ok::<_, Err>(match msg {
                        ProtocolMessage::PublishRelease(msg) => msg.ack(),
                        _ => ProtocolMessage::disconnect(),
                    })
                }),
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

    /// Run client with provided protocol-message service.
    ///
    /// Default control service is used.
    pub async fn start<F, S>(self, service: F) -> Result<(), MqttError<Err>>
    where
        F: IntoService<S, Session<()>, ProtocolMessage>,
        S: Service<Session<()>, ProtocolMessage, Res = ProtocolMessageAck, Error = Err> + 'static,
    {
        let sink = MqttSink::new(self.shared.clone());

        if self.keepalive.non_zero() {
            let _ = ntex_util::spawn(keepalive(sink.clone(), self.keepalive));
        }

        let dispatcher = Pipeline::new(
            Session::new((), sink.clone(), self.io.shared()),
            create_dispatcher(
                self.shared.clone(),
                self.max_receive,
                self.max_buffer_size,
                dispatch(self.builder.build(), self.handlers),
                service.into_service(),
            ),
        );
        let control = Pipeline::new(
            Session::new((), sink, self.io.shared()),
            ControlService::new(
                control::DefaultControlService::<Err, codec::Encoded>::default(),
                self.shared.clone(),
            )
            .map_err(MqttError::Service),
        );

        Dispatcher::new(self.io, self.shared, dispatcher, control).await
    }
}

fn dispatch<Err, PErr>(
    router: Router<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, (), PErr>>,
) -> impl Service<Session<()>, Publish, Res = Either<(), Publish>, Error = Err>
where
    PErr: 'static,
    Err: From<PErr>,
{
    let handlers = Rc::new(handlers);

    fn_service_st(async move |st: &Session<()>, mut req: Publish| {
        if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
            // exec handler
            let idx = *idx;
            match handlers[idx].call(req, st).await {
                Ok(()) => Ok(Either::Left(())),
                Err(err) => Err(err.into()),
            }
        } else {
            Ok::<_, Err>(Either::Right(req))
        }
    })
}

async fn keepalive(sink: MqttSink, timeout: Seconds) {
    keepalive_interval(sink, Millis::from(timeout)).await;
}

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
            // the client closes the connection (MQTT 3.1.1, 3.1.2.10)
            log::debug!("PINGRESP is not received within {interval:?}, closing connection");
            sink.close();
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ntex_bytes::Bytes;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;

    const PINGREQ: u8 = 0b1100_0000;
    const INTERVAL: Millis = Millis(200);

    async fn wait_ping_pending(sink: &MqttSink) {
        let mut n = 0;
        while !sink.is_ping_pending() {
            n += 1;
            assert!(n < 1000, "PINGREQ is not encoded");
            sleep(Millis(5)).await;
        }
    }

    #[ntex::test]
    async fn test_pingresp_timeout() {
        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        let sink = MqttSink::new(shared.clone());
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
        assert_eq!(&res[..], &[0b1110_0000, 0][..]);
    }

    #[ntex::test]
    async fn test_pingresp_timeout_write_backpressure() {
        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        let sink = MqttSink::new(shared.clone());
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
        assert_eq!(&res[..], &[0b1110_0000, 0][..]);
    }

    /// Sink follows io write backpressure while the control service is busy,
    /// `Control::wr` messages wait behind pending control calls
    #[ntex::test]
    async fn test_sink_wr_backpressure_slow_control() {
        use std::cell::Cell;

        use ntex_util::{channel::condition::Condition, time::timeout};

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());
        let conn = Client::new(io.into(), shared, false, Seconds::ZERO, 16, 0);

        let gate = Condition::new();
        let wr = Rc::new(Cell::new(None));
        let (gate2, wr2) = (gate.clone(), wr.clone());
        ntex_util::spawn(conn.start_with_control(
            fn_service(async |_: ProtocolMessage| Ok::<_, ()>(ProtocolMessage::disconnect())),
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
        while !buf.ends_with(b"\x32\x05\x00\x01b\x00\x01") {
            buf.extend_from_slice(&timeout(Millis(1000), client.read()).await.unwrap().unwrap());
        }
        assert_eq!(wr.get(), Some(true));
        gate.notify(());
    }
}
