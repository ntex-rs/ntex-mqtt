use std::{fmt, marker::PhantomData};

use ntex::service::{Service, ServiceFactory};
use ntex::util::Ready;

use super::control::{ControlMessage, ControlResult};
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
    type Future<'f> = Ready<Self::Service, Self::InitError> where Self: 'f;

    fn create(&self, _: Session<St>) -> Self::Future<'_> {
        Ready::Ok(DefaultPublishService { _t: PhantomData })
    }
}

impl<St, Err> Service<Publish> for DefaultPublishService<St, Err> {
    type Response = PublishAck;
    type Error = Err;
    type Future<'f> = Ready<Self::Response, Self::Error> where Self: 'f;

    fn call(&self, req: Publish) -> Self::Future<'_> {
        log::warn!("Publish service is disabled");
        Ready::Ok(req.ack())
    }
}

/// Default control service
pub struct DefaultControlService<S, E>(PhantomData<(S, E)>);

impl<S, E: fmt::Debug> Default for DefaultControlService<S, E> {
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
    type Future<'f> = Ready<Self::Service, Self::InitError> where Self: 'f;

    fn create(&self, _: Session<S>) -> Self::Future<'_> {
        Ready::Ok(DefaultControlService(PhantomData))
    }
}

impl<S, E: fmt::Debug> Service<ControlMessage<E>> for DefaultControlService<S, E> {
    type Response = ControlResult;
    type Error = E;
    type Future<'f> = Ready<Self::Response, Self::Error> where Self: 'f;

    #[inline]
    fn call(&self, pkt: ControlMessage<E>) -> Self::Future<'_> {
        match pkt {
            ControlMessage::Ping(pkt) => Ready::Ok(pkt.ack()),
            ControlMessage::Disconnect(pkt) => Ready::Ok(pkt.ack()),
            _ => {
                log::warn!("MQTT5 Control service is not configured, pkt: {:?}", pkt);
                Ready::Ok(pkt.disconnect_with(super::codec::Disconnect::new(
                    super::codec::DisconnectReasonCode::UnspecifiedError,
                )))
            }
        }
    }
}
