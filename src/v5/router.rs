use std::{cell::RefCell, num::NonZeroU16, rc::Rc, task::Context, task::Poll};

use ntex::router::{IntoPattern, Path, RouterBuilder};
use ntex::service::boxed::{self, BoxService, BoxServiceFactory};
use ntex::service::{IntoServiceFactory, Service, ServiceFactory};
use ntex::util::{BoxFuture, ByteString, HashMap};

use super::publish::{Publish, PublishAck};
use super::Session;

type Handler<S, E> = BoxServiceFactory<Session<S>, Publish, PublishAck, E, E>;
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
    S: 'static,
    Err: 'static,
{
    /// Create mqtt application router.
    ///
    /// Default service to be used if no matching resource could be found.
    pub fn new<F, U: 'static>(default_service: F) -> Self
    where
        F: IntoServiceFactory<U, Publish, Session<S>>,
        U: ServiceFactory<
            Publish,
            Session<S>,
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
        F: IntoServiceFactory<U, Publish, Session<S>>,
        U: ServiceFactory<Publish, Session<S>, Response = PublishAck, Error = Err>,
        Err: From<U::InitError>,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::factory(service.into_factory().map_init_err(Err::from)));
        self
    }

    /// Finish router configuration and create router service factory
    pub fn finish(self) -> RouterFactory<S, Err> {
        RouterFactory {
            router: self.router.finish(),
            handlers: Rc::new(self.handlers),
            default: self.default,
        }
    }
}

impl<S, Err> IntoServiceFactory<RouterFactory<S, Err>, Publish, Session<S>> for Router<S, Err>
where
    S: 'static,
    Err: 'static,
{
    fn into_factory(self) -> RouterFactory<S, Err> {
        self.finish()
    }
}

pub struct RouterFactory<S, Err> {
    router: ntex::router::Router<usize>,
    handlers: Rc<Vec<Handler<S, Err>>>,
    default: Handler<S, Err>,
}

impl<S, Err> ServiceFactory<Publish, Session<S>> for RouterFactory<S, Err>
where
    S: 'static,
    Err: 'static,
{
    type Response = PublishAck;
    type Error = Err;
    type InitError = Err;
    type Service = RouterService<Err>;
    type Future<'f> = BoxFuture<'f, Result<Self::Service, Err>>;

    fn create(&self, session: Session<S>) -> Self::Future<'_> {
        Box::pin(async move {
            let default = self.default.create(session.clone()).await?;

            let mut handlers = Vec::with_capacity(self.handlers.len());
            for f in self.handlers.as_ref() {
                handlers.push(f.create(session.clone()).await?);
            }

            Ok(RouterService {
                default,
                handlers,
                router: self.router.clone(),
                aliases: RefCell::new(HashMap::default()),
            })
        })
    }
}

pub struct RouterService<Err> {
    router: ntex::router::Router<usize>,
    default: HandlerService<Err>,
    handlers: Vec<HandlerService<Err>>,
    aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>>,
}

impl<Err: 'static> Service<Publish> for RouterService<Err> {
    type Response = PublishAck;
    type Error = Err;
    type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut not_ready = false;
        for hnd in self.handlers.iter() {
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

    fn call(&self, mut req: Publish) -> Self::Future<'_> {
        if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    self.aliases.borrow_mut().insert(alias, (*idx, req.topic().clone()));
                }
                return self.handlers[*idx].call(req);
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = self.aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                let idx = item.0;
                *req.topic_mut() = item.1.clone();
                drop(aliases);
                return self.handlers[idx].call(req);
            } else {
                log::error!("Unknown topic alias: {:?}", alias);
            }
        }
        self.default.call(req)
    }
}
