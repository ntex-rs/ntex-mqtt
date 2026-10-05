use std::{cell::RefCell, fmt, marker, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_io::IoBoxed;
use ntex_router::{IntoPattern, Path, Router, RouterBuilder};
use ntex_service::pipeline::{Pipeline, PipelineState};
use ntex_service::{IntoService, Service, cfg::Cfg, fn_service, fn_service_st};
use ntex_util::time::{Millis, Seconds, sleep};
use ntex_util::{HashMap, future::Either};

use crate::v5::default::ControlService;
use crate::v5::publish::{Publish, PublishAck};
use crate::v5::{ProtocolMessageAck, Session, codec, shared::MqttShared, sink::MqttSink};
use crate::{MqttServiceConfig, control, error::MqttError, io::Dispatcher};

use super::{control::ProtocolMessage, dispatcher::create_dispatcher};

/// Mqtt client
pub struct Client {
    io: IoBoxed,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    max_receive: usize,
    cfg: Cfg<MqttServiceConfig>,
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
    /// Construct new `Dispatcher` instance with outgoing messages stream.
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
    /// Get client sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.pkt.session_present
    }

    #[inline]
    /// Get reference to `ConnectAck` packet
    pub fn packet(&self) -> &codec::ConnectAck {
        &self.pkt
    }

    #[inline]
    /// Get mutable reference to `ConnectAck` packet
    pub fn packet_mut(&mut self) -> &mut codec::ConnectAck {
        &mut self.pkt
    }

    /// Configure mqtt resource for a specific topic
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

    /// Run client with default handlers.
    ///
    /// Default handlers close connection on any incoming publish or
    /// protocol message.
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

    /// Run client with provided protocol-message service.
    ///
    /// Default control service is used.
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

    /// Run client with provided protocol-message and control services.
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

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

/// Mqtt client with routing capabilities
pub struct ClientRouter<Err, PErr> {
    io: IoBoxed,
    builder: RouterBuilder<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, PErr>>,
    shared: Rc<MqttShared>,
    keepalive: Seconds,
    max_receive: usize,
    cfg: Cfg<MqttServiceConfig>,
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
    /// Configure mqtt resource for a specific topic
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

    /// Run client with default handlers.
    ///
    /// Default handlers acknowledge `PublishRelease` of routed `QoS 2` publishes and
    /// close connection on any unrouted publish or other protocol message.
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

    /// Run client with provided protocol-message service.
    ///
    /// Default control service is used.
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

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}

fn dispatch<PErr>(
    router: Router<usize>,
    handlers: Vec<PipelineState<Session<()>, Publish, PublishAck, PErr>>,
) -> impl Service<Session<()>, Publish, Res = Either<Publish, PublishAck>, Error = PErr>
where
    PErr: 'static,
    PublishAck: TryFrom<PErr, Error = PErr>,
{
    // let handlers =
    let aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>> =
        RefCell::new(HashMap::default());
    let handlers = Rc::new(handlers);

    fn_service_st(async move |st: &Session<()>, mut req: Publish| {
        let idx = if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    aliases
                        .borrow_mut()
                        .insert(alias, (*idx, req.topic().clone()));
                }
                *idx
            } else {
                return Ok::<_, PErr>(Either::Left(req));
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = aliases.borrow();
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
        match handlers[idx].call(req, st).await {
            Ok(ack) => Ok(Either::Right(ack)),
            Err(err) => match PublishAck::try_from(err) {
                Ok(ack) => Ok(Either::Right(ack)),
                Err(err) => Err(err),
            },
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
            codec::Codec::new(),
            Rc::default(),
        ));
        shared.set_client();
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
        assert_eq!(res[0], 0b1110_0000);
        // Unspecified error reason code
        assert_eq!(res[2], 0x80);
    }

    #[ntex::test]
    async fn test_pingresp_timeout_write_backpressure() {
        use ntex_util::future::lazy;

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
}
