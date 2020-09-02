use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::future::{join_all, JoinAll, LocalBoxFuture};
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
        F: IntoServiceFactory<U>,
        U: ServiceFactory<
            Config = S,
            Request = Publish,
            Response = (),
            Error = Err,
            InitError = Err,
        >,
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
        F: IntoServiceFactory<U>,
        U: ServiceFactory<Config = S, Request = Publish, Response = (), Error = Err>,
        Err: From<U::InitError>,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::factory(service.into_factory().map_init_err(Err::from)));
        self
    }
}

impl<S, Err> IntoServiceFactory<RouterFactory<S, Err>> for Router<S, Err>
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

impl<S, Err> ServiceFactory for RouterFactory<S, Err>
where
    S: Clone + 'static,
    Err: 'static,
{
    type Config = S;
    type Request = Publish;
    type Response = ();
    type Error = Err;
    type InitError = Err;
    type Service = RouterService<Err>;
    type Future = RouterFactoryFut<Err>;

    fn new_service(&self, session: S) -> Self::Future {
        let fut: Vec<_> =
            self.handlers.iter().map(|h| h.new_service(session.clone())).collect();

        RouterFactoryFut {
            router: self.router.clone(),
            handlers: join_all(fut),
            default: Some(either::Either::Left(self.default.new_service(session))),
        }
    }
}

pub struct RouterFactoryFut<Err> {
    router: Rc<ntex::router::Router<usize>>,
    handlers: JoinAll<LocalBoxFuture<'static, Result<HandlerService<Err>, Err>>>,
    default: Option<
        either::Either<
            LocalBoxFuture<'static, Result<HandlerService<Err>, Err>>,
            HandlerService<Err>,
        >,
    >,
}

impl<Err> Future for RouterFactoryFut<Err> {
    type Output = Result<RouterService<Err>, Err>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let res = match self.default.as_mut().unwrap() {
            either::Either::Left(ref mut fut) => {
                let default = match futures::ready!(Pin::new(fut).poll(cx)) {
                    Ok(default) => default,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                self.default = Some(either::Either::Right(default));
                return self.poll(cx);
            }
            either::Either::Right(_) => futures::ready!(Pin::new(&mut self.handlers).poll(cx)),
        };

        let mut handlers = Vec::new();
        for handler in res {
            match handler {
                Ok(h) => handlers.push(h),
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        Poll::Ready(Ok(RouterService {
            handlers,
            router: self.router.clone(),
            default: self.default.take().unwrap().right().unwrap(),
        }))
    }
}

pub struct RouterService<Err> {
    router: Rc<ntex::router::Router<usize>>,
    handlers: Vec<HandlerService<Err>>,
    default: HandlerService<Err>,
}

impl<Err> Service for RouterService<Err> {
    type Request = Publish;
    type Response = ();
    type Error = Err;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let mut not_ready = false;
        for hnd in &self.handlers {
            if let Poll::Pending = hnd.poll_ready(cx)? {
                not_ready = true;
            }
        }

        if let Poll::Pending = self.default.poll_ready(cx)? {
            not_ready = true;
        }

        if not_ready {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&self, mut req: Self::Request) -> Self::Future {
        if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
            self.handlers[*idx].call(req)
        } else {
            self.default.call(req)
        }
    }
}
