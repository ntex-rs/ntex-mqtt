use std::{fmt, marker::PhantomData};

use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{Middleware, Service, ServiceCtx, ServiceFactory};

use crate::{MqttServiceConfig, inflight::InFlightServiceImpl};

use super::{Session, control::ProtocolMessage, control::ProtocolMessageAck};

/// Default control service
#[derive(Debug)]
pub struct DefaultProtocolService<S, E>(PhantomData<(S, E)>);

impl<S, E: fmt::Debug> Default for DefaultProtocolService<S, E> {
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

impl<S, St> Middleware<S, (SharedCfg, Session<St>)> for InFlightService {
    type Service = InFlightServiceImpl<S>;

    #[inline]
    fn create(&self, service: S, cfg: (SharedCfg, Session<St>)) -> Self::Service {
        let cfg: Cfg<MqttServiceConfig> = cfg.0.get();
        InFlightServiceImpl::new(0, cfg.max_receive_size, service)
    }
}
