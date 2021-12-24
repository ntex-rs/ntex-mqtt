use std::task::{Context, Poll};
use std::{future::Future, pin::Pin, rc::Rc};

use ntex::router::{IntoPattern, RouterBuilder};
use ntex::service::boxed::{self, BoxService, BoxServiceFactory};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};

use super::publish::Publish;

type Handler<S, E> = BoxServiceFactory<S, Publish, (), E, E>;
type HandlerService<E> = BoxService<Publish, (), E>;

/// Router - structure that follows the builder pattern
/// for building publish packet router instances for mqtt server.
pub struct Router<S, Err> {
    router: RouterBuilder<usize>,
    handlers: Vec<Handler<S, Err>>,
    default: Handler<S, Err>,
}

impl<S, Err> Router<S, Err>
where
    S: Clone + 'static,
    Err: 'static,
{
    /// Create mqtt application router.
    ///
    /// Default service to be used if no matching resource could be found.
    pub fn new<F, U: 'static>(default_service: F) -> Self
    where
        F: IntoServiceFactory<U, Publish, S>,
        U: ServiceFactory<Publish, S, Response = (), Error = Err, InitError = Err>,
    {
        Router {
            router: ntex::router::Router::build(),
            handlers: Vec::new(),
            default: boxed::factory(default_service.into_factory()),
        }
    }

    /// Configure mqtt resource for a specific topic.
    pub fn resource<T, F, U: 'static>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Publish, S>,
        U: ServiceFactory<Publish, S, Response = (), Error = Err>,
        Err: From<U::InitError>,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::factory(service.into_factory().map_init_err(Err::from)));
        self
    }
}

impl<S, Err> IntoServiceFactory<RouterFactory<S, Err>, Publish, S> for Router<S, Err>
where
    S: Clone + 'static,
    Err: 'static,
{
    fn into_factory(self) -> RouterFactory<S, Err> {
        RouterFactory {
            router: Rc::new(self.router.finish()),
            handlers: self.handlers,
            default: self.default,
        }
    }
}

pub struct RouterFactory<S, Err> {
    router: Rc<ntex::router::Router<usize>>,
    handlers: Vec<Handler<S, Err>>,
    default: Handler<S, Err>,
}

impl<S, Err> ServiceFactory<Publish, S> for RouterFactory<S, Err>
where
    S: Clone + 'static,
    Err: 'static,
{
    type Response = ();
    type Error = Err;
    type InitError = Err;
    type Service = RouterService<Err>;
    type Future = Pin<Box<dyn Future<Output = Result<RouterService<Err>, Err>>>>;

    fn new_service(&self, session: S) -> Self::Future {
        let fut: Vec<_> =
            self.handlers.iter().map(|h| h.new_service(session.clone())).collect();
        let default_fut = self.default.new_service(session);
        let router = self.router.clone();

        Box::pin(async move {
            let mut handlers = Vec::new();
            for handler in fut {
                handlers.push(handler.await?);
            }

            Ok(RouterService { router, handlers, default: default_fut.await? })
        })
    }
}

pub struct RouterService<Err> {
    router: Rc<ntex::router::Router<usize>>,
    handlers: Vec<HandlerService<Err>>,
    default: HandlerService<Err>,
}

impl<Err> Service<Publish> for RouterService<Err> {
    type Response = ();
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut not_ready = false;
        for hnd in &self.handlers {
            if hnd.poll_ready(cx)?.is_pending() {
                not_ready = true;
            }
        }

        if self.default.poll_ready(cx)?.is_pending() {
            not_ready = true;
        }

        if not_ready {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&self, mut req: Publish) -> Self::Future {
        if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
            self.handlers[*idx].call(req)
        } else {
            self.default.call(req)
        }
    }
}
