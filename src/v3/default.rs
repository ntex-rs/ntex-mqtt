use std::{convert::Infallible, error::Error, marker::PhantomData, rc::Rc};

use ntex_service::{Ctx, Middleware, Service, ServiceFactory, cfg::Cfg};
use ntex_util::dyn_err;

use crate::error::PayloadError;
use crate::{Control, MqttServiceConfig, Reason, inflight::InFlightServiceImpl};

use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::{Connection, Session, codec::Encoded, shared::MqttShared};

/// Default control service
#[derive(Debug)]
pub struct DefaultProtocolService<E>(PhantomData<E>);

impl<E> Default for DefaultProtocolService<E> {
    fn default() -> Self {
        DefaultProtocolService(PhantomData)
    }
}

impl<St, E, Cfg> ServiceFactory<St, ProtocolMessage, Cfg> for DefaultProtocolService<E> {
    type Res = ProtocolMessageAck;
    type Error = E;

    type Service = DefaultProtocolService<E>;
    type InitError = Infallible;

    async fn create(&self, _: &Cfg) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultProtocolService(PhantomData))
    }
}

impl<St, E> Service<St, ProtocolMessage> for DefaultProtocolService<E> {
    type Res = ProtocolMessageAck;
    type Error = E;

    async fn call(
        &self,
        pkt: ProtocolMessage,
        _: Ctx<'_, Self, St>,
    ) -> Result<Self::Res, Self::Error> {
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

impl<S, St, AppSt> Middleware<S, St, Connection<AppSt>> for InFlightService {
    type Service = InFlightServiceImpl<S>;

    #[inline]
    fn create(&self, service: S, con: &Connection<AppSt>) -> Self::Service {
        let cfg: Cfg<MqttServiceConfig> = con.cfg();
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
pub struct ControlFactory<AppSt, S, E> {
    svc: S,
    _t: PhantomData<(AppSt, E)>,
}

impl<S, E> ControlService<S, E> {
    pub(super) fn new(svc: S, shared: Rc<MqttShared>) -> Self {
        Self {
            svc,
            shared,
            _t: PhantomData,
        }
    }
}

impl<AppSt, S, E> ControlFactory<AppSt, S, E> {
    pub(super) fn new(svc: S) -> Self {
        Self {
            svc,
            _t: PhantomData,
        }
    }
}

impl<AppSt, S, E> ServiceFactory<Session<AppSt>, Control<E>, Connection<AppSt>>
    for ControlFactory<AppSt, S, E>
where
    S: ServiceFactory<Session<AppSt>, Control<E>, Connection<AppSt>, Res = Option<Encoded>>,
    S::InitError: Error + 'static,
{
    type Res = Option<Encoded>;
    type Error = S::Error;

    type Service = ControlService<S::Service, E>;
    type InitError = Box<dyn Error>;

    async fn create(&self, cfg: &Connection<AppSt>) -> Result<Self::Service, Self::InitError> {
        Ok(ControlService {
            shared: cfg.st().sink().shared(),
            svc: self.svc.create(cfg).await.map_err(dyn_err)?,
            _t: PhantomData,
        })
    }
}

impl<AppSt, S, E> Service<Session<AppSt>, Control<E>> for ControlService<S, E>
where
    S: Service<Session<AppSt>, Control<E>>,
{
    type Res = Option<Encoded>;
    type Error = S::Error;

    async fn call(
        &self,
        req: Control<E>,
        ctx: Ctx<'_, Self, Session<AppSt>>,
    ) -> Result<Self::Res, Self::Error> {
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

        ctx.call(&self.svc, req).await.map(|_| None)
    }

    ntex_service::forward_ready!(Session<AppSt>, svc);
    ntex_service::forward_shutdown!(Session<AppSt>, svc);
}

#[cfg(test)]
mod tests {
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg};
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
        let con = Connection::new((), sink.clone(), SharedCfg::new("DBG").build());

        let disp = ControlFactory::<(), (), _, ()>::new(control::DefaultControlService::<
            (),
            codec::Encoded,
        >::default())
        .create(&con)
        .await
        .unwrap();

        let svc = Pipeline::with(ses, disp);

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
