use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory};

use crate::error::{MqttError, PayloadError};
use crate::{Control, MqttServiceConfig, Reason, inflight::InFlightServiceImpl};

use super::shared::MqttShared;
use super::{Session, codec::Encoded, control::ProtocolMessage, control::ProtocolMessageAck};

/// Default control service
#[derive(Debug)]
pub struct DefaultProtocolService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultProtocolService<S, E> {
    fn default() -> Self {
        DefaultProtocolService(PhantomData)
    }
}

impl<S, E: fmt::Debug> ServiceFactory<ProtocolMessage, S> for DefaultProtocolService<S, E> {
    type Response = ProtocolMessageAck;
    type Error = E;
    type InitError = E;
    type Service = DefaultProtocolService<S, E>;

    async fn create(&self, _: S) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultProtocolService(PhantomData))
    }
}

impl<S, E: fmt::Debug> Service<ProtocolMessage> for DefaultProtocolService<S, E> {
    type Response = ProtocolMessageAck;
    type Error = E;

    async fn call(
        &self,
        pkt: ProtocolMessage,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::warn!("MQTT3 Subscribe is not supported");

        Ok(match pkt {
            ProtocolMessage::Ping(ping) => ping.ack(),
            ProtocolMessage::Disconnect(disc) => disc.ack(),
            pkt => {
                log::warn!("MQTT3 Control service is not configured, pkt: {pkt:?}");
                pkt.disconnect()
            }
        })
    }
}

#[derive(Copy, Clone, Debug)]
/// Service that can limit number of in-flight async requests.
///
/// Default is 16 in-flight messages and 64kb size
pub struct InFlightService;

impl<S, St> Middleware<S, (SharedCfg, Session<St>)> for InFlightService {
    type Service = InFlightServiceImpl<S>;

    #[inline]
    fn create(&self, service: S, cfg: (SharedCfg, Session<St>)) -> Self::Service {
        let cfg: Cfg<MqttServiceConfig> = cfg.0.get();
        InFlightServiceImpl::new(cfg.max_receive, cfg.max_receive_size, service)
    }
}

#[derive(Clone, Debug)]
pub struct ControlService<S, E> {
    svc: S,
    shared: Rc<MqttShared>,
    _t: PhantomData<E>,
}

#[derive(Clone, Debug)]
pub struct ControlFactory<S, St, E> {
    svc: S,
    _t: PhantomData<(E, St)>,
}

impl<S, E> ControlService<S, E>
where
    S: Service<Control<E>>,
{
    pub(super) fn new(svc: S, shared: Rc<MqttShared>) -> Self {
        Self { svc, shared, _t: PhantomData }
    }
}

impl<S, St, E> ControlFactory<S, St, E>
where
    S: ServiceFactory<Control<E>, Session<St>>,
{
    pub(super) fn new(svc: S) -> Self {
        Self { svc, _t: PhantomData }
    }
}

impl<S, St, E> ServiceFactory<Control<E>, Session<St>> for ControlFactory<S, St, E>
where
    S: ServiceFactory<Control<E>, Session<St>>,
{
    type Response = Option<Encoded>;
    type Error = MqttError<S::Error>;
    type InitError = MqttError<S::InitError>;
    type Service = ControlService<S::Service, E>;

    async fn create(&self, cfg: Session<St>) -> Result<Self::Service, Self::InitError> {
        Ok(ControlService {
            shared: cfg.sink().shared(),
            svc: self.svc.create(cfg).await.map_err(MqttError::Service)?,
            _t: PhantomData,
        })
    }
}

impl<S, E> Service<Control<E>> for ControlService<S, E>
where
    S: Service<Control<E>>,
{
    type Response = Option<Encoded>;
    type Error = MqttError<S::Error>;

    async fn call(
        &self,
        req: Control<E>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        match &req {
            Control::Stop(Reason::Error(_)) => {
                self.shared.drop_payload(&PayloadError::Service);
            }
            Control::Stop(Reason::Protocol(err)) => {
                self.shared.drop_payload(err.get_ref());
            }
            Control::Stop(Reason::PeerGone(_)) => {
                self.shared.drop_payload(&PayloadError::Disconnected);
            }
            Control::WrBackpressure(status) => {
                if status.enabled() {
                    self.shared.enable_wr_backpressure();
                } else {
                    self.shared.disable_wr_backpressure();
                }
            }
        }

        ctx.call(&self.svc, req).await.map(|_| None).map_err(MqttError::Service)
    }

    ntex_service::forward_ready!(svc, MqttError::Service);
    ntex_service::forward_poll!(svc, MqttError::Service);
    ntex_service::forward_shutdown!(svc);
}

#[cfg(test)]
mod tests {
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;
    use ntex_util::future::lazy;

    use super::*;
    use crate::{control, v3::MqttSink, v3::codec};

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, false, Rc::default()));
        let sink = MqttSink::new(shared.clone());
        let ses = Session::new((), sink.clone());

        let disp = ControlFactory::<_, (), ()>::new(control::DefaultControlService::<
            _,
            (),
            codec::Codec,
        >::default());
        let svc = disp.pipeline(ses).await.unwrap();

        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        svc.call(Control::wr(true)).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        svc.call(Control::wr(false)).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }
}
