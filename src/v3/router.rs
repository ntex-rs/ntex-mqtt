use std::{error::Error, fmt, rc::Rc};

use ntex_router::{IntoPattern, RouterBuilder};
use ntex_service::boxed::{self, BoxService, BoxServiceFactory};
use ntex_service::{Ctx, IntoServiceFactory, Service, ServiceFactory};

use super::{Session, publish::Publish};

type Handler<AppSt, E> =
    BoxServiceFactory<Session<AppSt>, Publish, (), E, Session<AppSt>, Box<dyn Error>>;
type HandlerService<AppSt, E> = BoxService<Session<AppSt>, Publish, (), E>;

/// Router - structure that follows the builder pattern
/// for building publish packet router instances for mqtt server.
pub struct Router<AppSt, Err> {
    router: RouterBuilder<usize>,
    handlers: Vec<Handler<AppSt, Err>>,
    default: Handler<AppSt, Err>,
}

impl<AppSt, Err> fmt::Debug for Router<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::Router").finish()
    }
}

impl<AppSt, Err> Router<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    /// Create mqtt application router.
    ///
    /// Default service to be used if no matching resource could be found.
    pub fn new<U>(f: impl IntoServiceFactory<U, Session<AppSt>, Publish, Session<AppSt>>) -> Self
    where
        U: ServiceFactory<Session<AppSt>, Publish, Session<AppSt>, Res = (), Error = Err> + 'static,
        U::InitError: Error + 'static,
    {
        Router {
            router: ntex_router::Router::build(),
            handlers: Vec::new(),
            default: boxed::factory(
                f.into_factory()
                    .map_init_err(|e| Box::new(e) as Box<dyn Error>),
            ),
        }
    }

    #[must_use]
    /// Configure mqtt resource for a specific topic.
    pub fn resource<T, F, U>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Session<AppSt>, Publish, Session<AppSt>>,
        U: ServiceFactory<Session<AppSt>, Publish, Session<AppSt>, Res = (), Error = Err> + 'static,
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
}

impl<AppSt, Err>
    IntoServiceFactory<RouterFactory<AppSt, Err>, Session<AppSt>, Publish, Session<AppSt>>
    for Router<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    fn into_factory(self) -> RouterFactory<AppSt, Err> {
        RouterFactory {
            router: Rc::new(self.router.finish()),
            handlers: self.handlers,
            default: self.default,
        }
    }
}

pub struct RouterFactory<AppSt, Err> {
    router: Rc<ntex_router::Router<usize>>,
    handlers: Vec<Handler<AppSt, Err>>,
    default: Handler<AppSt, Err>,
}

impl<AppSt, Err> fmt::Debug for RouterFactory<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::RouterFactory").finish()
    }
}

impl<AppSt, Err> ServiceFactory<Session<AppSt>, Publish, Session<AppSt>>
    for RouterFactory<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    type Res = ();
    type Error = Err;

    type Service = RouterService<AppSt, Err>;
    type InitError = Box<dyn Error>;

    async fn create(&self, con: &Session<AppSt>) -> Result<Self::Service, Self::InitError> {
        let fut: Vec<_> = self.handlers.iter().map(|h| h.create(con)).collect();

        let mut handlers = Vec::new();
        for handler in fut {
            handlers.push(handler.await?);
        }

        Ok(RouterService {
            handlers,
            router: self.router.clone(),
            default: self.default.create(con).await?,
        })
    }
}

pub struct RouterService<AppSt, Err> {
    router: Rc<ntex_router::Router<usize>>,
    handlers: Vec<HandlerService<AppSt, Err>>,
    default: HandlerService<AppSt, Err>,
}

impl<AppSt, Err> fmt::Debug for RouterService<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::RouterService").finish()
    }
}

impl<AppSt, Err> Service<Session<AppSt>, Publish> for RouterService<AppSt, Err> {
    type Res = ();
    type Error = Err;

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, Session<AppSt>>) -> Result<(), Self::Error> {
        for hnd in &self.handlers {
            ctx.ready(hnd).await?;
        }
        ctx.ready(&self.default).await
    }

    #[inline]
    async fn call(
        &self,
        mut req: Publish,
        ctx: Ctx<'_, Self, Session<AppSt>>,
    ) -> Result<Self::Res, Self::Error> {
        if let Some((idx, _info)) = self.router.recognize(req.topic_mut()) {
            ctx.call(&self.handlers[*idx], req).await
        } else {
            ctx.call(&self.default, req).await
        }
    }
}

#[cfg(test)]
mod tests {
    use ntex_service::fn_service;

    use super::*;

    #[test]
    fn test_debug() {
        let router: Router<(), (), ()> =
            Router::new(fn_service(async |_: Publish| Ok::<_, ()>(())));
        assert!(format!("{router:?}").contains("v3::Router"));
    }
}
