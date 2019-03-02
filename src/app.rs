use std::rc::Rc;

use actix_router::{Router, RouterBuilder};
use actix_service::boxed::{self, BoxedNewService, BoxedService};
use actix_service::{IntoConfigurableNewService, IntoNewService, NewService, Service};
use futures::future::{err, join_all, Either, FutureResult, JoinAll};
use futures::{Async, Future, Poll};

use crate::publish::Publish;

type Handler<S, E1, E2> = BoxedNewService<S, Publish<S>, (), E1, E2>;
type HandlerService<S, E> = BoxedService<Publish<S>, (), E>;

pub struct App<S, E1, E2> {
    router: RouterBuilder<usize>,
    handlers: Vec<Handler<S, E1, E2>>,
    not_found: Rc<Fn(Publish<S>) -> E1>,
}

impl<S, E1, E2> App<S, E1, E2>
where
    S: 'static,
    E1: 'static,
    E2: 'static,
{
    /// Create mqtt application and provide default topic handler.
    pub fn new<F>(not_found: F) -> Self
    where
        F: Fn(Publish<S>) -> E1 + 'static,
    {
        App {
            router: Router::build(),
            handlers: Vec::new(),
            not_found: Rc::new(not_found),
        }
    }

    pub fn resource<F, U: 'static>(mut self, address: &str, service: F) -> Self
    where
        F: IntoNewService<U, S>,
        U: NewService<S, Request = Publish<S>, Response = (), Error = E1, InitError = E2>,
    {
        self.router.path(address, self.handlers.len());
        self.handlers
            .push(boxed::new_service(service.into_new_service()));
        self
    }
}

impl<S, E1, E2> IntoConfigurableNewService<AppFactory<S, E1, E2>, S> for App<S, E1, E2>
where
    S: 'static,
    E1: 'static,
    E2: 'static,
{
    fn into_new_service(self) -> AppFactory<S, E1, E2> {
        AppFactory {
            router: Rc::new(self.router.finish()),
            handlers: self.handlers,
            not_found: self.not_found,
        }
    }
}

pub struct AppFactory<S, E1, E2> {
    router: Rc<Router<usize>>,
    handlers: Vec<Handler<S, E1, E2>>,
    not_found: Rc<Fn(Publish<S>) -> E1>,
}

impl<S, E1, E2> NewService<S> for AppFactory<S, E1, E2>
where
    S: 'static,
    E1: 'static,
    E2: 'static,
{
    type Request = Publish<S>;
    type Response = ();
    type Error = E1;
    type InitError = E2;
    type Service = AppService<S, E1>;
    type Future = AppFactoryFut<S, E1, E2>;

    fn new_service(&self, session: &S) -> Self::Future {
        let fut: Vec<_> = self
            .handlers
            .iter()
            .map(|h| h.new_service(session))
            .collect();

        AppFactoryFut {
            router: self.router.clone(),
            handlers: join_all(fut),
            not_found: self.not_found.clone(),
        }
    }
}

pub struct AppFactoryFut<S, E1, E2> {
    router: Rc<Router<usize>>,
    handlers: JoinAll<Vec<Box<Future<Item = HandlerService<S, E1>, Error = E2>>>>,
    not_found: Rc<Fn(Publish<S>) -> E1>,
}

impl<S, E1, E2> Future for AppFactoryFut<S, E1, E2> {
    type Item = AppService<S, E1>;
    type Error = E2;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let handlers = futures::try_ready!(self.handlers.poll());
        Ok(Async::Ready(AppService {
            handlers,
            router: self.router.clone(),
            not_found: self.not_found.clone(),
        }))
    }
}

pub struct AppService<S, E> {
    router: Rc<Router<usize>>,
    handlers: Vec<BoxedService<Publish<S>, (), E>>,
    not_found: Rc<Fn(Publish<S>) -> E>,
}

impl<S, E> Service for AppService<S, E>
where
    S: 'static,
    E: 'static,
{
    type Request = Publish<S>;
    type Response = ();
    type Error = E;
    type Future = Either<
        Box<Future<Item = Self::Response, Error = Self::Error>>,
        FutureResult<Self::Response, Self::Error>,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let mut not_ready = false;
        for hnd in &mut self.handlers {
            if let Async::NotReady = hnd.poll_ready()? {
                not_ready = true;
            }
        }

        if not_ready {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(()))
        }
    }

    fn call(&mut self, mut req: Self::Request) -> Self::Future {
        if let Some((idx, _info)) = self.router.recognize(req.path_mut()) {
            Either::A(self.handlers[*idx].call(req))
        } else {
            Either::B(err((*self.not_found.as_ref())(req)))
        }
    }
}
