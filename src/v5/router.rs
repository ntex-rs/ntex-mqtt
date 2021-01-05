use std::task::{Context, Poll};
use std::{cell::Cell, cell::RefCell, num::NonZeroU16, rc::Rc};

use bytestring::ByteString;
use futures::future::{FutureExt, LocalBoxFuture};
use ntex::router::{IntoPattern, Path, RouterBuilder};
use ntex::service::boxed::{self, BoxService, BoxServiceFactory};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::task::LocalWaker;

use super::publish::{Publish, PublishAck};
use crate::AHashMap;

type Handler<S, E> = BoxServiceFactory<S, Publish, PublishAck, E, E>;
type HandlerService<E> = BoxService<Publish, PublishAck, E>;

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
            Response = PublishAck,
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
        U: ServiceFactory<Config = S, Request = Publish, Response = PublishAck, Error = Err>,
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
            router: self.router.finish(),
            handlers: Rc::new(self.handlers),
            default: self.default,
        }
    }
}

pub struct RouterFactory<S, Err> {
    router: ntex::router::Router<usize>,
    handlers: Rc<Vec<Handler<S, Err>>>,
    default: Handler<S, Err>,
}

impl<S, Err> ServiceFactory for RouterFactory<S, Err>
where
    S: Clone + 'static,
    Err: 'static,
{
    type Config = S;
    type Request = Publish;
    type Response = PublishAck;
    type Error = Err;
    type InitError = Err;
    type Service = RouterService<S, Err>;
    type Future = LocalBoxFuture<'static, Result<Self::Service, Err>>;

    fn new_service(&self, session: S) -> Self::Future {
        let router = self.router.clone();
        let factories = self.handlers.clone();
        let default_fut = self.default.new_service(session.clone());

        async move {
            let default = default_fut.await?;
            let handlers = (0..factories.len()).map(|_| None).collect();

            Ok(RouterService {
                router,
                default,
                inner: Rc::new(Inner {
                    session,
                    factories,
                    handlers: RefCell::new(handlers),
                    creating: Cell::new(false),
                    aliases: RefCell::new(AHashMap::default()),
                    waker: LocalWaker::new(),
                }),
            })
        }
        .boxed_local()
    }
}

pub struct RouterService<S, Err> {
    inner: Rc<Inner<S, Err>>,
    router: ntex::router::Router<usize>,
    default: HandlerService<Err>,
}

struct Inner<S, Err> {
    session: S,
    handlers: RefCell<Vec<Option<HandlerService<Err>>>>,
    factories: Rc<Vec<Handler<S, Err>>>,
    aliases: RefCell<AHashMap<NonZeroU16, (usize, Path<ByteString>)>>,
    waker: LocalWaker,
    creating: Cell<bool>,
}

impl<S: Clone + 'static, Err: 'static> RouterService<S, Err> {
    fn create_handler(
        &self,
        idx: usize,
        req: Publish,
    ) -> LocalBoxFuture<'static, Result<PublishAck, Err>> {
        let inner = self.inner.clone();
        inner.creating.set(true);

        async move {
            let handler = inner.factories[idx].new_service(inner.session.clone()).await?;
            if let Err(e) = crate::utils::ready(&handler).await {
                inner.waker.wake();
                inner.creating.set(false);
                return Err(e);
            }

            let fut = handler.call(req);
            inner.waker.wake();
            inner.creating.set(false);
            inner.handlers.borrow_mut()[idx] = Some(handler);
            fut.await
        }
        .boxed_local()
    }
}

impl<S: Clone + 'static, Err: 'static> Service for RouterService<S, Err> {
    type Request = Publish;
    type Response = PublishAck;
    type Error = Err;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut not_ready = false;
        for hnd in &*self.inner.handlers.borrow() {
            if let Some(hnd) = hnd {
                if let Poll::Pending = hnd.poll_ready(cx)? {
                    not_ready = true;
                }
            }
        }

        if let Poll::Pending = self.default.poll_ready(cx)? {
            not_ready = true;
        }

        // new handler get created at the moment
        if self.inner.creating.get() {
            self.inner.waker.register(cx.waker());
            return Poll::Pending;
        }

        if not_ready {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&self, mut req: Self::Request) -> Self::Future {
        if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    self.inner.aliases.borrow_mut().insert(alias, (*idx, req.topic().clone()));
                }
                if let Some(hnd) = &self.inner.handlers.borrow()[*idx] {
                    return hnd.call(req);
                } else {
                    return self.create_handler(*idx, req);
                }
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = self.inner.aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                *req.topic_mut() = item.1.clone();
                if let Some(hnd) = &self.inner.handlers.borrow()[item.0] {
                    return hnd.call(req);
                } else {
                    return self.create_handler(item.0, req);
                }
            } else {
                log::error!("Unknown topic alias: {:?}", alias);
            }
        }
        self.default.call(req)
    }
}
