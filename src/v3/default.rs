use std::{fmt, marker::PhantomData};

use ntex::service::{Service, ServiceCtx, ServiceFactory};

use super::control::{ControlMessage, ControlResult, ControlResultKind};
use super::publish::Publish;
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
    type Response = ();
    type Error = Err;
    type Service = DefaultPublishService<St, Err>;
    type InitError = Err;

    async fn create(&self, _: Session<St>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultPublishService { _t: PhantomData })
    }
}

impl<St, Err> Service<Publish> for DefaultPublishService<St, Err> {
    type Response = ();
    type Error = Err;

    async fn call(
        &self,
        _: Publish,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::warn!("Publish service is disabled");
        Ok(())
    }
}

/// Default control service
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for DefaultControlService<S, E> {
    fn default() -> Self {
        DefaultControlService(PhantomData)
    }
}

impl<S, E: fmt::Debug> ServiceFactory<ControlMessage<E>, Session<S>>
    for DefaultControlService<S, E>
{
    type Response = ControlResult;
    type Error = E;
    type InitError = E;
    type Service = DefaultControlService<S, E>;

    async fn create(&self, _: Session<S>) -> Result<Self::Service, Self::InitError> {
        Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E: fmt::Debug> Service<ControlMessage<E>> for DefaultControlService<S, E> {
    type Response = ControlResult;
    type Error = E;

    async fn call(
        &self,
        pkt: ControlMessage<E>,
        _: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::warn!("MQTT3 Subscribe is not supported");

        Ok(match pkt {
            ControlMessage::Ping(ping) => ping.ack(),
            ControlMessage::Disconnect(disc) => disc.ack(),
            ControlMessage::Closed(msg) => msg.ack(),
            _ => {
                log::warn!("MQTT3 Control service is not configured, pkt: {:?}", pkt);
                ControlResult { result: ControlResultKind::Disconnect }
            }
        })
    }
}
