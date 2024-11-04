use std::future::{poll_fn, Future};
use std::{cell::RefCell, num::NonZeroU16, pin::Pin, rc::Rc, task::Poll};

use ntex_bytes::ByteString;
use ntex_router::{IntoPattern, Path, RouterBuilder};
use ntex_service::boxed::{self, BoxService, BoxServiceFactory};
use ntex_service::{IntoServiceFactory, Service, ServiceCtx, ServiceFactory};
use ntex_util::HashMap;

use super::{publish::Publish, publish::PublishAck, Session};

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
    pub fn new<F, U>(default_service: F) -> Self
    where
        F: IntoServiceFactory<U, Publish, Session<S>>,
        U: ServiceFactory<
                Publish,
                Session<S>,
                Response = PublishAck,
                Error = Err,
                InitError = Err,
            > + 'static,
    {
        Router {
            router: ntex_router::Router::build(),
            handlers: Vec::new(),
            default: boxed::factory(default_service.into_factory()),
        }
    }

    /// Configure mqtt resource for a specific topic.
    pub fn resource<T, F, U>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Publish, Session<S>>,
        U: ServiceFactory<Publish, Session<S>, Response = PublishAck, Error = Err> + 'static,
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
    router: ntex_router::Router<usize>,
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

    async fn create(&self, session: Session<S>) -> Result<Self::Service, Err> {
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
    }
}

pub struct RouterService<Err> {
    router: ntex_router::Router<usize>,
    default: HandlerService<Err>,
    handlers: Vec<HandlerService<Err>>,
    aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>>,
}

impl<Err: 'static> Service<Publish> for RouterService<Err> {
    type Response = PublishAck;
    type Error = Err;

    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        for hnd in self.handlers.iter() {
            ctx.ready(hnd).await?;
        }

        ctx.ready(&self.default).await
    }

    #[inline]
    async fn not_ready(&self) {
        let mut futs = Vec::with_capacity(self.handlers.len() + 1);
        for hnd in &self.handlers {
            futs.push(Box::pin(hnd.not_ready()));
        }
        futs.push(Box::pin(self.default.not_ready()));

        poll_fn(|cx| {
            for hnd in &mut futs {
                if Pin::new(hnd).poll(cx).is_ready() {
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        })
        .await;
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        mut req: Publish,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    self.aliases.borrow_mut().insert(alias, (*idx, req.topic().clone()));
                }
                return ctx.call(&self.handlers[*idx], req).await;
            }
        }
        // handle publish with topic alias
        else if let Some(ref alias) = req.packet().properties.topic_alias {
            let aliases = self.aliases.borrow();
            if let Some(item) = aliases.get(alias) {
                let idx = item.0;
                *req.topic_mut() = item.1.clone();
                drop(aliases);
                return ctx.call(&self.handlers[idx], req).await;
            } else {
                log::error!("Unknown topic alias: {:?}", alias);
            }
        }
        ctx.call(&self.default, req).await
    }
}
