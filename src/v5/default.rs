use std::{fmt, marker::PhantomData};

use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory};

use crate::{MqttServiceConfig, inflight::InFlightServiceImpl};

use super::Session;
use super::control::{Control, ControlAck, CtlFlow, CtlFrame};

/// Default control service
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E: fmt::Debug> Default for DefaultControlService<S, E> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E: fmt::Debug> ServiceFactory<Control<E>, Session<S>> for DefaultControlService<S, E> {
    type Response = ControlAck;
    type Error = E;
    type InitError = E;
    type Service = DefaultControlService<S, E>;

    async fn create(&self, _: Session<S>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E: fmt::Debug> Service<Control<E>> for DefaultControlService<S, E> {
    type Response = ControlAck;
    type Error = E;

    async fn call(
        &self,
        pkt: Control<E>,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        match pkt {
            Control::Flow(CtlFlow::Ping(pkt)) => Ok(pkt.ack()),
            Control::Protocol(CtlFrame::Disconnect(pkt)) => Ok(pkt.ack()),
            _ => {
                log::warn!("MQTT5 Control service is not configured, pkt: {:?}", pkt);
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

impl<S> Middleware<S, SharedCfg> for InFlightService {
    type Service = InFlightServiceImpl<S>;

    #[inline]
    fn create(&self, service: S, cfg: SharedCfg) -> Self::Service {
        let cfg: Cfg<MqttServiceConfig> = cfg.get();
        InFlightServiceImpl::new(0, cfg.max_receive_size, service)
    }
}
