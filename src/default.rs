use std::marker::PhantomData;

use actix_service::{NewService, Service};
use futures::future::{ok, FutureResult};
use futures::{Async, Poll};

use crate::publish::Publish;
use crate::subs::{Subscribe, SubscribeResult, Unsubscribe};

/// Not implemented publish service
pub struct NotImplemented<S, E>(PhantomData<(S, E)>);

impl<S, E> Default for NotImplemented<S, E> {
    fn default() -> Self {
        NotImplemented(PhantomData)
    }
}

impl<S, E> NewService for NotImplemented<S, E> {
    type Config = S;
    type Request = Publish<S>;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = NotImplemented<S, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(NotImplemented(PhantomData))
    }
}

impl<S, E> Service for NotImplemented<S, E> {
    type Request = Publish<S>;
    type Response = ();
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: Publish<S>) -> Self::Future {
        log::warn!("MQTT Publish is not implemented");
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

impl<S, E> NewService for SubsNotImplemented<S, E> {
    type Config = S;
    type Request = Subscribe<S>;
    type Response = SubscribeResult;
    type Error = E;
    type InitError = E;
    type Service = SubsNotImplemented<S, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(SubsNotImplemented(PhantomData))
    }
}

impl<S, E> Service for SubsNotImplemented<S, E> {
    type Request = Subscribe<S>;
    type Response = SubscribeResult;
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, subs: Subscribe<S>) -> Self::Future {
        log::warn!("MQTT Subscribe is not implemented");
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

impl<S, E> NewService for UnsubsNotImplemented<S, E> {
    type Config = S;
    type Request = Unsubscribe<S>;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = UnsubsNotImplemented<S, E>;
    type Future = FutureResult<Self::Service, Self::InitError>;

    fn new_service(&self, _: &S) -> Self::Future {
        ok(UnsubsNotImplemented(PhantomData))
    }
}

impl<S, E> Service for UnsubsNotImplemented<S, E> {
    type Request = Unsubscribe<S>;
    type Response = ();
    type Error = E;
    type Future = FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: Unsubscribe<S>) -> Self::Future {
        log::warn!("MQTT Unsubscribe is not implemented");
        ok(())
    }
}
