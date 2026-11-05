use std::{marker::PhantomData, rc::Rc};

use ntex_error::Failure;
use ntex_service::{BindCtx, Ctx, Middleware, Service, ServiceFactory, cfg::Cfg};

use crate::error::{MqttError, PayloadError};
use crate::{Control, MqttServiceConfig, Reason, inflight::InFlightServiceImpl};

use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::{Session, codec, codec::Encoded, shared::MqttShared};

/// Default protocol handler for pings, disconnects, and `QoS 2` release messages.
///
/// Responds to `Ping`, `Disconnect` and `PublishRelease` messages, any other message
/// closes the connection with `UnspecifiedError` disconnect reason.
#[derive(Debug)]
pub struct DefaultProtoSrv<E>(PhantomData<E>);

impl<E> Default for DefaultProtoSrv<E> {
    fn default() -> Self {
        DefaultProtoSrv(PhantomData)
    }
}

impl<St, Err> ServiceFactory<St, ProtocolMessage> for DefaultProtoSrv<Err> {
    type Res = ProtocolMessageAck;
    type Error = Err;

    type Service = DefaultProtoSrv<Err>;
    type InitError = Failure;

    async fn create(&self, _: &St) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultProtoSrv(PhantomData))
    }
}

impl<St, E> Service<St, ProtocolMessage> for DefaultProtoSrv<E> {
    type Res = ProtocolMessageAck;
    type Error = E;

    async fn call(
        &self,
        pkt: ProtocolMessage,
        _: Ctx<'_, Self, St>,
    ) -> Result<Self::Res, Self::Error> {
        match pkt {
            ProtocolMessage::Ping(pkt) => Ok(pkt.ack()),
            ProtocolMessage::Disconnect(pkt) => Ok(pkt.ack()),
            // `QoS 2` publish is already handled by publish service [MQTT-4.3.3-11]
            ProtocolMessage::PublishRelease(pkt) => Ok(pkt.ack()),
            _ => {
                log::warn!("MQTT5 Control service is not configured, pkt: {pkt:?}");
                Ok(pkt.disconnect_with(super::codec::Disconnect::new(
                    super::codec::DisconnectReasonCode::UnspecifiedError,
                )))
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
/// Middleware limiting the total size of in-flight inbound publishes.
///
/// Uses `MqttServiceConfig::max_receive_size` (65,535 bytes by default), without
/// a request-count limit. Payload chunks and disconnects retain ordered handling.
pub struct InFlightService;

impl<S, AppSt> Middleware<S, Session<AppSt>> for InFlightService {
    type Service = InFlightServiceImpl<S>;

    #[inline]
    fn create(&self, st: &Session<AppSt>, service: S) -> Self::Service {
        let cfg: Cfg<MqttServiceConfig> = st.cfg();
        InFlightServiceImpl::new(0, cfg.max_receive_size, service)
    }
}

/// Connection-control adapter that fails payloads and supplies disconnect responses.
#[derive(Clone, Debug)]
pub struct ControlService<S, E> {
    /// Application connection-control service.
    svc: S,
    /// Connection payload and disconnect state used while handling stop events.
    shared: Rc<MqttShared>,
    /// Carries the reported publish/protocol error type without storing a value.
    _t: PhantomData<E>,
}

/// Factory wrapping application connection-control services with MQTT error handling.
#[derive(Clone, Debug)]
pub struct ControlFactory<AppSt, Sf, E> {
    /// Application factory instantiated once for each accepted session.
    svc: Sf,
    /// Carries the connection error and application state types.
    _t: PhantomData<(E, AppSt)>,
}

impl<S, E> ControlService<S, E> {
    /// Wraps an application control service with the connection's shared state.
    pub(super) fn new(svc: S, shared: Rc<MqttShared>) -> Self {
        Self {
            svc,
            shared,
            _t: PhantomData,
        }
    }
}

impl<AppSt, Sf, E> ControlFactory<AppSt, Sf, E>
where
    Sf: ServiceFactory<Session<AppSt>, Control<E>>,
{
    /// Wraps a factory whose service returns optional encoded control responses.
    pub(super) fn new(svc: Sf) -> Self {
        Self {
            svc,
            _t: PhantomData,
        }
    }
}

impl<AppSt, Sf, E> ServiceFactory<Session<AppSt>, Control<E>> for ControlFactory<AppSt, Sf, E>
where
    Sf: ServiceFactory<Session<AppSt>, Control<E>, Res = Option<Encoded>>,
{
    type Res = Sf::Res;
    type Error = MqttError<Sf::Error>;

    type Service = ControlService<Sf::Service, E>;
    type InitError = Sf::InitError;

    async fn create(&self, cfg: &Session<AppSt>) -> Result<Self::Service, Self::InitError> {
        Ok(ControlService {
            shared: cfg.sink().shared(),
            svc: self.svc.create(cfg).await?,
            _t: PhantomData,
        })
    }
}

impl<St, S, E> Service<St, Control<E>> for ControlService<S, E>
where
    S: Service<St, Control<E>, Res = Option<Encoded>>,
{
    type Res = S::Res;
    type Error = MqttError<S::Error>;

    async fn call(
        &self,
        req: Control<E>,
        ctx: Ctx<'_, Self, St>,
    ) -> Result<Self::Res, Self::Error> {
        let mut proto_error = false;
        let disconnect = match &req {
            Control::Stop(Reason::Error(_)) => {
                self.shared.drop_payload(&PayloadError::Service);
                Some(
                    codec::Packet::from(codec::Disconnect::new(
                        codec::DisconnectReasonCode::ImplementationSpecificError,
                    ))
                    .into(),
                )
            }
            Control::Stop(Reason::Protocol(err)) => {
                self.shared.drop_payload(err.get_ref());
                proto_error = true;
                Some(codec::Packet::from(codec::Disconnect::from_proto_error(err.get_ref())).into())
            }
            Control::Stop(Reason::PeerGone(_)) => {
                self.shared.drop_payload(&PayloadError::Disconnected);
                None
            }
            // the sink follows the io write backpressure state directly
            Control::WrBackpressure(_) => None,
        };

        match ctx.call(&self.svc, req).await {
            Ok(Some(val)) => {
                if (proto_error || !self.shared.is_disconnect_recv())
                    && !self.shared.is_disconnect_sent()
                {
                    Ok(Some(val))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => {
                if disconnect.is_some() && !self.shared.is_disconnect_sent() {
                    Ok(disconnect)
                } else {
                    Ok(None)
                }
            }
            Err(err) => Err(MqttError::Service(err)),
        }
    }

    fn bind(&self, ctx: BindCtx<'_>) {
        ctx.bind(&self.svc);
    }

    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        ctx.ready(&self.svc).await.map_err(MqttError::Service)
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        ctx.shutdown(&self.svc).await;
    }
}

/// Unit tests for connection-control integration with sink backpressure.
#[cfg(test)]
mod tests {
    use std::{pin::pin, task::Poll};

    use ntex_bytes::Bytes;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg};
    use ntex_util::future::lazy;

    use super::*;
    use crate::{control, v5::MqttSink, v5::codec::PublishAck};

    /// Unconvertible publish error used by the test service types.
    #[derive(Debug)]
    struct TestError;

    impl TryFrom<TestError> for PublishAck {
        type Error = TestError;

        fn try_from(err: TestError) -> Result<Self, Self::Error> {
            Err(err)
        }
    }

    /// STATEMENT: Sink readiness follows transport backpressure rather than control notifications.
    ///
    /// HOW:
    /// - Grant a send credit, block the peer buffer, and send enough `QoS 0` data to cause backpressure.
    /// - Poll two readiness waiters and send a false backpressure notification; check they remain blocked.
    /// - Drain the buffer and check both waiters complete and credit returns, then clear the transport flag.
    /// - Send a true backpressure notification and verify the drained sink stays ready.
    #[ntex::test]
    async fn test_wr_backpressure() {
        let (client, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Rc::default()));
        let sink = MqttSink::new(shared.clone());
        let ses = Session::new((), sink.clone(), io.shared());

        let disp =
            ControlFactory::new(control::DefaultControlService::<(), codec::Encoded>::default());
        let svc = Pipeline::new(ses.clone(), disp.create(&ses).await.unwrap());

        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());

        client.remote_buffer_cap(0);
        sink.publish("a")
            .send_at_most_once(Bytes::from(vec![0u8; 128 * 1024]))
            .await
            .unwrap();
        assert!(io.is_wr_backpressure());
        assert!(!sink.is_ready());
        let mut rx = pin!(sink.ready());
        let mut rx2 = pin!(sink.ready());
        assert!(lazy(|cx| rx.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| rx2.as_mut().poll(cx).is_pending()).await);

        // the send credit is granted to the first waiter, it waits for the
        // write buffer, a control message does not release it
        svc.call(Control::<bool>::wr(false)).await.unwrap();
        assert_eq!(sink.credit(), 0);
        assert!(lazy(|cx| rx.as_mut().poll(cx).is_pending()).await);

        // the write buffer drains
        client.remote_buffer_cap(1024 * 1024);
        io.write_ready().await.unwrap();
        assert_eq!(lazy(|cx| rx.as_mut().poll(cx)).await, Poll::Ready(true));
        assert_eq!(lazy(|cx| rx2.as_mut().poll(cx)).await, Poll::Ready(true));
        assert_eq!(sink.credit(), 1);

        // the dispatcher clears the flag
        assert!(lazy(|cx| io.poll_flush(cx, false).is_ready()).await);
        assert!(!io.is_wr_backpressure());
        assert!(sink.is_ready());
        svc.call(Control::<bool>::wr(true)).await.unwrap();
        assert!(sink.is_ready());
    }
}
