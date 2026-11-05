use std::{fmt, rc::Rc};

use ntex_error::{Failure, IntoFailure};
use ntex_router::{IntoPattern, RouterBuilder};
use ntex_service::boxed::{self, BoxService, BoxServiceFactory};
use ntex_service::{Ctx, IntoServiceFactory, Service, ServiceFactory};

use super::{Session, publish::Publish};

/// Type-erased factory for a session-scoped publish handler.
type Handler<AppSt, E> = BoxServiceFactory<Session<AppSt>, Publish, (), E, Failure>;
/// Type-erased publish handler instantiated for one session.
type HandlerService<AppSt, E> = BoxService<Session<AppSt>, Publish, (), E>;

/// Builder for routing incoming publish topics with `ntex_router` path patterns.
///
/// Matched path captures are available through `Publish::topic`. MQTT subscription
/// wildcards are not routing patterns; use path captures such as `sensors/{id}`.
pub struct Router<AppSt, Err> {
    /// Topic patterns mapped to indices in `handlers`.
    router: RouterBuilder<usize>,
    /// Resource factories in the same order as their router indices.
    handlers: Vec<Handler<AppSt, Err>>,
    /// Fallback factory used when no topic pattern matches.
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
    /// Create a topic router with `f` as the fallback publish-service factory.
    ///
    /// Every resource service and the fallback are created for each session, and all
    /// participate in router readiness and shutdown, even if they handle no messages.
    pub fn new<U>(f: impl IntoServiceFactory<U, Session<AppSt>, Publish>) -> Self
    where
        U: ServiceFactory<Session<AppSt>, Publish, Res = (), Error = Err> + 'static,
        U::InitError: IntoFailure,
    {
        Router {
            router: ntex_router::Router::builder(),
            handlers: Vec::new(),
            default: boxed::factory(f.into_factory().map_init_err(IntoFailure::fail)),
        }
    }

    #[must_use]
    /// Register a publish-service factory for a topic path pattern.
    ///
    /// Matching uses `ntex_router` syntax, not MQTT subscription wildcards.
    ///
    /// # Panics
    ///
    /// Panics if `address` contains a malformed path pattern.
    pub fn resource<T, F, U>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Session<AppSt>, Publish>,
        U: ServiceFactory<Session<AppSt>, Publish, Res = (), Error = Err> + 'static,
        U::InitError: IntoFailure,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::factory(
            service.into_factory().map_init_err(IntoFailure::fail),
        ));
        self
    }
}

impl<AppSt, Err> IntoServiceFactory<RouterFactory<AppSt, Err>, Session<AppSt>, Publish>
    for Router<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    fn into_factory(self) -> RouterFactory<AppSt, Err> {
        RouterFactory {
            router: Rc::new(self.router.build()),
            handlers: self.handlers,
            default: self.default,
        }
    }
}

/// Factory that instantiates every resource handler and fallback for a session.
pub struct RouterFactory<AppSt, Err> {
    /// Compiled topic matcher shared by session-specific router services.
    router: Rc<ntex_router::Router<usize>>,
    /// Resource factories indexed by the compiled matcher.
    handlers: Vec<Handler<AppSt, Err>>,
    /// Fallback factory instantiated for each session.
    default: Handler<AppSt, Err>,
}

impl<AppSt, Err> fmt::Debug for RouterFactory<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::RouterFactory").finish()
    }
}

impl<AppSt, Err> ServiceFactory<Session<AppSt>, Publish> for RouterFactory<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    type Res = ();
    type Error = Err;

    type Service = RouterService<AppSt, Err>;
    type InitError = Failure;

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

/// Session-scoped topic router forwarding readiness and shutdown to every handler.
pub struct RouterService<AppSt, Err> {
    /// Shared matcher that populates topic-path captures during recognition.
    router: Rc<ntex_router::Router<usize>>,
    /// Session-specific handlers indexed by the matcher.
    handlers: Vec<HandlerService<AppSt, Err>>,
    /// Session-specific fallback for unmatched publish topics.
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

    fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
        for handler in &self.handlers {
            ctx.bind(handler);
        }
        ctx.bind(&self.default);
    }

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, Session<AppSt>>) -> Result<(), Self::Error> {
        for hnd in &self.handlers {
            ctx.ready(hnd).await?;
        }
        ctx.ready(&self.default).await
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, Session<AppSt>>) {
        for hnd in &self.handlers {
            ctx.shutdown(hnd).await;
        }
        ctx.shutdown(&self.default).await;
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

/// Unit tests for router formatting and handler shutdown.
#[cfg(test)]
mod tests {
    use ntex_service::fn_service;

    use super::*;

    /// STATEMENT: The publish router has an identifiable debug representation.
    ///
    /// HOW:
    /// - Build a router with a successful default publish handler.
    /// - Format it and assert the output contains `v3::Router`.
    #[test]
    fn test_debug() {
        let router: Router<(), ()> = Router::new(fn_service(async |_: Publish| Ok::<_, ()>(())));
        assert!(format!("{router:?}").contains("v3::Router"));
    }

    /// Test publish service incrementing a shared counter when shut down.
    struct Tracked(Rc<std::cell::Cell<usize>>);

    impl Service<Session<()>, Publish> for Tracked {
        type Res = ();
        type Error = ();

        async fn call(&self, _: Publish, _: Ctx<'_, Self, Session<()>>) -> Result<(), ()> {
            Ok(())
        }

        async fn shutdown(&self, _: Ctx<'_, Self, Session<()>>) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// STATEMENT: Router shutdown reaches both resource handlers and the default handler.
    ///
    /// HOW:
    /// - Create a session router with two handlers sharing a shutdown counter.
    /// - Shut down the pipeline and assert the counter records two shutdown calls.
    #[ntex::test]
    async fn test_shutdown() {
        use ntex_io::{Io, testing::IoTest};
        use ntex_service::{Pipeline, cfg::SharedCfg, fn_factory};

        use crate::v3::{MqttSink, codec, shared::MqttShared};

        let (_client, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("DBG"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let ses = Session::new((), MqttSink::new(shared), io.shared());

        let count = Rc::new(std::cell::Cell::new(0));
        let c1 = count.clone();
        let c2 = count.clone();
        let factory = Router::new(fn_factory(async move |_: &Session<()>| {
            Ok::<_, Failure>(Tracked(c1.clone()))
        }))
        .resource(
            "topic",
            fn_factory(async move |_: &Session<()>| Ok::<_, Failure>(Tracked(c2.clone()))),
        )
        .into_factory();

        let svc = Pipeline::new(ses.clone(), factory.create(&ses).await.unwrap());
        svc.shutdown().await;
        assert_eq!(count.get(), 2);
    }
}
