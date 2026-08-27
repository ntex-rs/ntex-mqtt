use std::{convert::Infallible, error::Error, marker::PhantomData, rc::Rc};

use ntex_service::{Ctx, Middleware, Service, ServiceFactory, cfg::Cfg};

use crate::error::{MqttError, PayloadError};
use crate::{Control, MqttServiceConfig, Reason, inflight::InFlightServiceImpl};

use super::control::{ProtocolMessage, ProtocolMessageAck};
use super::{Connection, Session, codec, codec::Encoded, shared::MqttShared};

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
        match pkt {
            ProtocolMessage::Ping(pkt) => Ok(pkt.ack()),
            ProtocolMessage::Disconnect(pkt) => Ok(pkt.ack()),
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
/// Service that can limit size of in-flight async requests.
///
/// Default is 64kb size
pub struct InFlightService;

impl<S, St> Middleware<S, Connection<St>> for InFlightService {
    type Service = InFlightServiceImpl<S>;

    #[inline]
    fn create(&self, service: S, cfg: &Connection<St>) -> Self::Service {
        let cfg: Cfg<MqttServiceConfig> = cfg.cfg();
        InFlightServiceImpl::new(0, cfg.max_receive_size, service)
    }
}

#[derive(Clone, Debug)]
pub struct ControlService<S, E> {
    svc: S,
    shared: Rc<MqttShared>,
    _t: PhantomData<E>,
}

#[derive(Clone, Debug)]
pub struct ControlFactory<St, AppSt, Sf, E> {
    svc: Sf,
    _t: PhantomData<(E, St, AppSt)>,
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

impl<St, AppSt, Sf, E> ControlFactory<St, AppSt, Sf, E>
where
    Sf: ServiceFactory<Session<AppSt>, Control<E>, Connection<St>>,
{
    pub(super) fn new(svc: Sf) -> Self {
        Self {
            svc,
            _t: PhantomData,
        }
    }
}

impl<St, AppSt, Sf, E> ServiceFactory<Session<AppSt>, Control<E>, Connection<St>>
    for ControlFactory<St, AppSt, Sf, E>
where
    Sf: ServiceFactory<Session<AppSt>, Control<E>, Connection<St>, Res = Option<Encoded>>,
    Sf::InitError: Error + 'static,
{
    type Res = Sf::Res;
    type Error = MqttError<Sf::Error>;

    type Service = ControlService<Sf::Service, E>;
    type InitError = Box<dyn Error>;

    async fn create(&self, cfg: &Connection<St>) -> Result<Self::Service, Self::InitError> {
        Ok(ControlService {
            shared: cfg.sink().shared(),
            svc: self.svc.create(cfg).await.map_err(Box::new)?,
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
            Control::WrBackpressure(status) => {
                if status.enabled() {
                    self.shared.enable_wr_backpressure();
                } else {
                    self.shared.disable_wr_backpressure();
                }
                None
            }
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

    ntex_service::forward_ready!(St, svc, MqttError::Service);
    ntex_service::forward_shutdown!(St, svc);
}

#[cfg(test)]
mod tests {
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg};
    use ntex_util::future::lazy;

    use super::*;
    use crate::{control, v5::MqttSink, v5::codec::PublishAck};

    #[derive(Debug)]
    struct TestError;

    impl TryFrom<TestError> for PublishAck {
        type Error = TestError;

        fn try_from(err: TestError) -> Result<Self, Self::Error> {
            Err(err)
        }
    }

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Rc::default()));
        let sink = MqttSink::new(shared.clone());
        let ses = Session::new((), sink.clone());
        let con = Connection::new((), sink.clone(), io.shared());

        let disp = ControlFactory::<(), (), _, ()>::new(control::DefaultControlService::<
            (),
            codec::Encoded,
        >::default());
        let svc = Pipeline::with(ses, disp.create(&con).await.unwrap());

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
