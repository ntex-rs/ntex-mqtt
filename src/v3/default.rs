use std::{fmt, marker::PhantomData};

use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory};

use crate::{MqttServiceConfig, inflight::InFlightServiceImpl};

use super::Session;
use super::control::{ProtocolMessage, ProtocolMessageAck};

/// Default control service
#[derive(Debug)]
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultControlService<S, E> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E: fmt::Debug> ServiceFactory<ProtocolMessage, Session<S>>
    for DefaultControlService<S, E>
{
    type Response = ProtocolMessageAck;
    type Error = E;
    type InitError = E;
    type Service = DefaultControlService<S, E>;

    async fn create(&self, _: Session<S>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E: fmt::Debug> Service<ProtocolMessage> for DefaultControlService<S, E> {
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
