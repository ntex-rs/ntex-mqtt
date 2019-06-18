use std::rc::Rc;

use actix_router::{Router, RouterBuilder};
use actix_service::boxed::{self, BoxedNewService, BoxedService};
use actix_service::{service_fn, IntoNewService, NewService, Service};
use futures::future::{join_all, Either, FutureResult, JoinAll};
use futures::{Async, Future, Poll};

use crate::publish::Publish;

type Handler<S, E> = BoxedNewService<S, Publish<S>, (), E, E>;
type HandlerService<S, E> = BoxedService<Publish<S>, (), E>;

/// Application builder - structure that follows the builder pattern
/// for building application instances for mqtt server.
pub struct App<S, E> {
    router: RouterBuilder<usize>,
    handlers: Vec<Handler<S, E>>,
    default: Handler<S, E>,
}

impl<S, E> App<S, E>
where
    S: 'static,
    E: 'static,
{
    /// Create mqtt application.
    ///
    /// **Note** Default service acks all publish packets
    pub fn new() -> Self {
        App {
            router: Router::build(),
            handlers: Vec::new(),
            default: boxed::new_service(
                service_fn(|p: Publish<S>| {
                    log::warn!("Unknown topic {:?}", p.publish_topic());
                    Ok::<_, E>(())
                })
                .map_init_err(|_| panic!()),
            ),
        }
    }

    /// Configure mqtt resource for a specific topic.
    pub fn resource<F, U: 'static>(mut self, address: &str, service: F) -> Self
    where
        F: IntoNewService<U>,
        U: NewService<Config = S, Request = Publish<S>, Response = ()>,
        E: From<U::Error> + From<U::InitError>,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::new_service(
            service
                .into_new_service()
                .map_err(|e| e.into())
                .map_init_err(|e| e.into()),
        ));
        self
    }

    /// Default service to be used if no matching resource could be found.
    pub fn default_resource<F, U: 'static>(mut self, service: F) -> Self
    where
        F: IntoNewService<U>,
        U: NewService<Config = S, Request = Publish<S>, Response = ()>,
        E: From<U::Error> + From<U::InitError>,
    {
        self.default = boxed::new_service(
            service
                .into_new_service()
                .map_err(|e| e.into())
                .map_init_err(|e| e.into()),
        );
        self
    }
}

impl<S, E> IntoNewService<AppFactory<S, E>> for App<S, E>
where
    S: 'static,
    E: 'static,
{
    fn into_new_service(self) -> AppFactory<S, E> {
        AppFactory {
            router: Rc::new(self.router.finish()),
            handlers: self.handlers,
            default: self.default,
        }
    }
}

pub struct AppFactory<S, E> {
    router: Rc<Router<usize>>,
    handlers: Vec<Handler<S, E>>,
    default: Handler<S, E>,
}

impl<S, E> NewService for AppFactory<S, E>
where
    S: 'static,
    E: 'static,
{
    type Config = S;
    type Request = Publish<S>;
    type Response = ();
    type Error = E;
    type InitError = E;
    type Service = AppService<S, E>;
    type Future = AppFactoryFut<S, E>;

    fn new_service(&self, session: &S) -> Self::Future {
        let fut: Vec<_> = self
            .handlers
            .iter()
            .map(|h| h.new_service(session))
            .collect();

        AppFactoryFut {
            router: self.router.clone(),
            handlers: join_all(fut),
            default: Some(either::Either::Left(self.default.new_service(session))),
        }
    }
}

pub struct AppFactoryFut<S, E> {
    router: Rc<Router<usize>>,
    handlers: JoinAll<Vec<Box<Future<Item = HandlerService<S, E>, Error = E>>>>,
    default: Option<
        either::Either<
            Box<Future<Item = HandlerService<S, E>, Error = E>>,
            HandlerService<S, E>,
        >,
    >,
}

impl<S, E> Future for AppFactoryFut<S, E> {
    type Item = AppService<S, E>;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let handlers = match self.default.as_mut().unwrap() {
            either::Either::Left(ref mut fut) => {
                let default = futures::try_ready!(fut.poll());
                self.default = Some(either::Either::Right(default));
                return self.poll();
            }
            either::Either::Right(_) => futures::try_ready!(self.handlers.poll()),
        };

        Ok(Async::Ready(AppService {
            handlers,
            router: self.router.clone(),
            default: self.default.take().unwrap().right().unwrap(),
        }))
    }
}

pub struct AppService<S, E> {
    router: Rc<Router<usize>>,
    handlers: Vec<BoxedService<Publish<S>, (), E>>,
    default: BoxedService<Publish<S>, (), E>,
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
        FutureResult<Self::Response, Self::Error>,
        Box<Future<Item = Self::Response, Error = Self::Error>>,
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

    fn call(&mut self, mut req: Publish<S>) -> Self::Future {
        if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
            self.handlers[*idx].call(req)
        } else {
            self.default.call(req)
        }
    }
}
