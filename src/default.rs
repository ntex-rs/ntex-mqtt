use std::marker::PhantomData;
use std::task::{Context, Poll};

use futures::future::{ok, Ready};
use ntex::service::{Service, ServiceFactory};

use crate::publish::Publish;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

/// Not implemented publish service
pub struct NotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for NotImplemented<S, E> {
    fn default() -> Self {
        NotImplemented(PhantomData)
    }
}

impl<S, E> ServiceFactory for NotImplemented<S, E> {
    type Config = S;
    type Request = Publish;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = NotImplemented<S, E>;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: S) -> Self::Future {
        ok(NotImplemented(PhantomData))
    }
}

impl<S, E> Service for NotImplemented<S, E> {
    type Request = Publish;
    type Response = ();
    type Error = E;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Publish) -> Self::Future {
        log::warn!("MQTT Publish is not supported");
        ok(())
    }
}

/// Not implemented subscribe service
pub struct SubsNotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for SubsNotImplemented<S, E> {
    fn default() -> Self {
        SubsNotImplemented(PhantomData)
    }
}

impl<S, E> ServiceFactory for SubsNotImplemented<S, E> {
    type Config = S;
    type Request = Subscribe;
    type Response = SubscribeResult;
    type Error = E;
    type InitError = E;
    type Service = SubsNotImplemented<S, E>;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: S) -> Self::Future {
        ok(SubsNotImplemented(PhantomData))
    }
}

impl<S, E> Service for SubsNotImplemented<S, E> {
    type Request = Subscribe;
    type Response = SubscribeResult;
    type Error = E;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, subs: Subscribe) -> Self::Future {
        log::warn!("MQTT Subscribe is not supported");
        ok(subs.into_result())
    }
}

/// Not implemented unsubscribe service
pub struct UnsubsNotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for UnsubsNotImplemented<S, E> {
    fn default() -> Self {
        UnsubsNotImplemented(PhantomData)
    }
}

impl<S, E> ServiceFactory for UnsubsNotImplemented<S, E> {
    type Config = S;
    type Request = Unsubscribe;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = UnsubsNotImplemented<S, E>;
    type Future = Ready<Result<Self::Service, Self::InitError>>;

    fn new_service(&self, _: S) -> Self::Future {
        ok(UnsubsNotImplemented(PhantomData))
    }
}

impl<S, E> Service for UnsubsNotImplemented<S, E> {
    type Request = Unsubscribe;
    type Response = ();
    type Error = E;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Unsubscribe) -> Self::Future {
        log::warn!("MQTT Unsubscribe is not supported");
        ok(())
    }
}
