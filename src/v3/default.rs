use std::{fmt, marker::PhantomData};

use ntex::service::{Ctx, Service, ServiceFactory};
use ntex::util::Ready;

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
    type Future<'f> = Ready<Self::Service, Self::InitError> where Self: 'f;

    fn create(&self, _: Session<St>) -> Self::Future<'_> {
        Ready::Ok(DefaultPublishService { _t: PhantomData })
    }
}

impl<St, Err> Service<Publish> for DefaultPublishService<St, Err> {
    type Response = ();
    type Error = Err;
    type Future<'f> = Ready<Self::Response, Self::Error> where Self: 'f;

    fn call<'a>(&'a self, _: Publish, _: Ctx<'a, Self>) -> Self::Future<'a> {
        log::warn!("Publish service is disabled");
        Ready::Ok(())
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
    fn call<'a>(&'a self, pkt: ControlMessage<E>, _: Ctx<'a, Self>) -> Self::Future<'a> {
        log::warn!("MQTT3 Subscribe is not supported");

        Ready::Ok(match pkt {
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
