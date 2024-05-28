use std::{fmt, marker::PhantomData};

use ntex_service::{Service, ServiceCtx, ServiceFactory};

use super::control::{Control, ControlAck};
use super::publish::{Publish, PublishAck};
use super::Session;

/// Default publish service
pub struct DefaultPublishService<St, Err> {
    _t: PhantomData<(St, Err)>,
}

impl<St, Err> Default for DefaultPublishService<St, Err> {
    fn default() -> Self {
        Self { _t: PhantomData }
    }
}

impl<St, Err> ServiceFactory<Publish, Session<St>> for DefaultPublishService<St, Err> {
    type Response = PublishAck;
    type Error = Err;
    type Service = DefaultPublishService<St, Err>;
    type InitError = Err;

    async fn create(&self, _: Session<St>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultPublishService { _t: PhantomData })
    }
}

impl<St, Err> Service<Publish> for DefaultPublishService<St, Err> {
    type Response = PublishAck;
    type Error = Err;

    async fn call(
        &self,
        req: Publish,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::warn!("Publish service is disabled");
        Ok(req.ack())
    }
}

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
            Control::Ping(pkt) => Ok(pkt.ack()),
            Control::Disconnect(pkt) => Ok(pkt.ack()),
            _ => {
                log::warn!("MQTT5 Control service is not configured, pkt: {:?}", pkt);
                Ok(pkt.disconnect_with(super::codec::Disconnect::new(
                    super::codec::DisconnectReasonCode::UnspecifiedError,
                )))
            }
        }
    }
}
