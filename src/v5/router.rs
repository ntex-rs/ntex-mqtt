use std::{cell::RefCell, error::Error, fmt, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_router::{IntoPattern, Path, RouterBuilder};
use ntex_service::boxed::{self, BoxService, BoxServiceFactory};
use ntex_service::{Ctx, IntoServiceFactory, Service, ServiceFactory};
use ntex_util::HashMap;

use super::{Connection, Session, publish::Publish, publish::PublishAck};

type Handler<St, AppSt, E> =
    BoxServiceFactory<Session<AppSt>, Publish, PublishAck, E, Connection<St>, Box<dyn Error>>;
type HandlerService<AppSt, E> = BoxService<Session<AppSt>, Publish, PublishAck, E>;

/// Router - structure that follows the builder pattern
/// for building publish packet router instances for mqtt server.
pub struct Router<St, AppSt, Err> {
    router: RouterBuilder<usize>,
    handlers: Vec<Handler<St, AppSt, Err>>,
    default: Handler<St, AppSt, Err>,
}

impl<St, AppSt, Err> fmt::Debug for Router<St, AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::Router").finish()
    }
}

impl<St, AppSt, Err> Router<St, AppSt, Err>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
{
    /// Create mqtt application router.
    ///
    /// Default service to be used if no matching resource could be found.
    pub fn new<U>(
        default: impl IntoServiceFactory<U, Session<AppSt>, Publish, Connection<St>>,
    ) -> Self
    where
        U: ServiceFactory<Session<AppSt>, Publish, Connection<St>, Res = PublishAck, Error = Err>
            + 'static,
        U::InitError: Error + 'static,
    {
        Router {
            router: ntex_router::Router::build(),
            handlers: Vec::new(),
            default: boxed::factory(
                default
                    .into_factory()
                    .map_init_err(|e| Box::new(e) as Box<dyn Error>),
            ),
        }
    }

    #[must_use]
    /// Configure mqtt resource for a specific topic.
    pub fn resource<T, F, U>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Session<AppSt>, Publish, Connection<St>>,
        U: ServiceFactory<Session<AppSt>, Publish, Connection<St>, Res = PublishAck, Error = Err>
            + 'static,
        U::InitError: Error + 'static,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::factory(
            service
                .into_factory()
                .map_init_err(|e| Box::new(e) as Box<dyn Error>),
        ));
        self
    }

    /// Finish router configuration and create router service factory
    pub fn build(self) -> RouterFactory<St, AppSt, Err> {
        RouterFactory {
            router: self.router.finish(),
            handlers: Rc::new(self.handlers),
            default: self.default,
        }
    }
}

impl<St, AppSt, Err>
    IntoServiceFactory<RouterFactory<St, AppSt, Err>, Session<AppSt>, Publish, Connection<St>>
    for Router<St, AppSt, Err>
where
    St: 'static,
    AppSt: 'static,
    Err: 'static,
{
    fn into_factory(self) -> RouterFactory<St, AppSt, Err> {
        self.build()
    }
}

pub struct RouterFactory<St, AppSt, Err> {
    router: ntex_router::Router<usize>,
    handlers: Rc<Vec<Handler<St, AppSt, Err>>>,
    default: Handler<St, AppSt, Err>,
}

impl<St, AppSt, Err> fmt::Debug for RouterFactory<St, AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::RouterFactory").finish()
    }
}

impl<St, AppSt, Err> ServiceFactory<Session<AppSt>, Publish, Connection<St>>
    for RouterFactory<St, AppSt, Err>
where
    St: 'static,
    Err: 'static,
{
    type Res = PublishAck;
    type Error = Err;

    type Service = RouterService<AppSt, Err>;
    type InitError = Box<dyn Error>;

    async fn create(&self, cfg: &Connection<St>) -> Result<Self::Service, Self::InitError> {
        let default = self.default.create(cfg).await?;

        let mut handlers = Vec::with_capacity(self.handlers.len());
        for f in self.handlers.as_ref() {
            handlers.push(f.create(cfg).await?);
        }

        Ok(RouterService {
            default,
            handlers,
            router: self.router.clone(),
            aliases: RefCell::new(HashMap::default()),
        })
    }
}

pub struct RouterService<AppSt, Err> {
    router: ntex_router::Router<usize>,
    default: HandlerService<AppSt, Err>,
    handlers: Vec<HandlerService<AppSt, Err>>,
    aliases: RefCell<HashMap<NonZeroU16, (usize, Path<ByteString>)>>,
}

impl<AppSt, Err> fmt::Debug for RouterService<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::RouterService").finish()
    }
}

impl<AppSt, Err: 'static> Service<Session<AppSt>, Publish> for RouterService<AppSt, Err> {
    type Res = PublishAck;
    type Error = Err;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, Session<AppSt>>) -> Result<(), Self::Error> {
        for hnd in &self.handlers {
            ctx.ready(hnd).await?;
        }
        ctx.ready(&self.default).await
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        mut req: Publish,
        ctx: Ctx<'_, Self, Session<AppSt>>,
    ) -> Result<Self::Res, Self::Error> {
        if !req.publish_topic().is_empty() {
            if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
                // save info for topic alias
                if let Some(alias) = req.packet().properties.topic_alias {
                    self.aliases
                        .borrow_mut()
                        .insert(alias, (*idx, req.topic().clone()));
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
            }
            log::error!("Unknown topic alias: {alias:?}");
        }
        ctx.call(&self.default, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v5::codec::PublishAckReason;

    #[test]
    fn test_debug() {
        let router: Router<(), (), ()> =
            Router::new(async |_: Publish| Ok::<_, ()>(PublishAck::new(PublishAckReason::Success)));
        assert!(format!("{router:?}").contains("v5::Router"));
    }
}
