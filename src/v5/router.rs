use std::{cell::RefCell, fmt, num::NonZeroU16, rc::Rc};

use ntex_bytes::ByteString;
use ntex_error::{Failure, IntoFailure};
use ntex_router::{IntoPattern, Path, RouterBuilder};
use ntex_service::boxed::{self, BoxService, BoxServiceFactory};
use ntex_service::{Ctx, IntoServiceFactory, Service, ServiceFactory};
use ntex_util::HashMap;

use super::{Session, publish::Publish, publish::PublishAck};

/// Type-erased publish handler factory with normalized initialization failures.
type Handler<AppSt, E> = BoxServiceFactory<Session<AppSt>, Publish, PublishAck, E, Failure>;
/// Type-erased publish handler instantiated for one application session.
type HandlerService<AppSt, E> = BoxService<Session<AppSt>, Publish, PublishAck, E>;

/// Builder for routing inbound publishes to topic-pattern handlers.
///
/// Unmatched topics use a required default handler. Matching updates the publish
/// routing path with captured parameters; handler readiness gates the entire router.
pub struct Router<AppSt, Err> {
    /// Topic patterns mapped to indices in the handler factory list.
    router: RouterBuilder<usize>,
    /// Resource factories in the same order as their route indices.
    handlers: Vec<Handler<AppSt, Err>>,
    /// Fallback factory for publishes without a matching resource.
    default: Handler<AppSt, Err>,
}

impl<AppSt, Err> fmt::Debug for Router<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::Router").finish()
    }
}

impl<AppSt, Err> Router<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    /// Creates a topic router with a fallback publish handler.
    ///
    /// The default handler is used when no resource pattern matches.
    pub fn new<U>(default: impl IntoServiceFactory<U, Session<AppSt>, Publish>) -> Self
    where
        U: ServiceFactory<Session<AppSt>, Publish, Res = PublishAck, Error = Err> + 'static,
        U::InitError: IntoFailure,
    {
        Router {
            router: ntex_router::Router::builder(),
            handlers: Vec::new(),
            default: boxed::factory(default.into_factory().map_init_err(IntoFailure::fail)),
        }
    }

    #[must_use]
    /// Registers a publish handler for a topic path pattern.
    ///
    /// Patterns use `ntex_router` path syntax, not MQTT subscription wildcards.
    ///
    /// # Panics
    ///
    /// Panics if a pattern has malformed dynamic segments or invalid regular expressions.
    pub fn resource<T, F, U>(mut self, address: T, service: F) -> Self
    where
        T: IntoPattern,
        F: IntoServiceFactory<U, Session<AppSt>, Publish>,
        U: ServiceFactory<Session<AppSt>, Publish, Res = PublishAck, Error = Err> + 'static,
        U::InitError: IntoFailure,
    {
        self.router.path(address, self.handlers.len());
        self.handlers.push(boxed::factory(
            service.into_factory().map_init_err(IntoFailure::fail),
        ));
        self
    }

    /// Finishes route configuration and returns a per-session service factory.
    pub fn build(self) -> RouterFactory<AppSt, Err> {
        RouterFactory {
            router: self.router.build(),
            handlers: Rc::new(self.handlers),
            default: self.default,
        }
    }
}

impl<AppSt, Err> IntoServiceFactory<RouterFactory<AppSt, Err>, Session<AppSt>, Publish>
    for Router<AppSt, Err>
where
    AppSt: 'static,
    Err: 'static,
{
    fn into_factory(self) -> RouterFactory<AppSt, Err> {
        self.build()
    }
}

/// Compiled topic routes and factories used to create per-session handlers.
pub struct RouterFactory<AppSt, Err> {
    /// Compiled patterns returning indices into the handler list.
    router: ntex_router::Router<usize>,
    /// Shared resource factories instantiated for each new session.
    handlers: Rc<Vec<Handler<AppSt, Err>>>,
    /// Fallback factory instantiated alongside the resource handlers.
    default: Handler<AppSt, Err>,
}

impl<AppSt, Err> fmt::Debug for RouterFactory<AppSt, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v5::RouterFactory").finish()
    }
}

impl<AppSt, Err> ServiceFactory<Session<AppSt>, Publish> for RouterFactory<AppSt, Err>
where
    Err: 'static,
{
    type Res = PublishAck;
    type Error = Err;

    type Service = RouterService<AppSt, Err>;
    type InitError = Failure;

    async fn create(&self, cfg: &Session<AppSt>) -> Result<Self::Service, Self::InitError> {
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

/// Per-session topic router with handler services and cached alias routing paths.
pub struct RouterService<AppSt, Err> {
    /// Compiled patterns selecting resource handler indices.
    router: ntex_router::Router<usize>,
    /// Fallback handler for unmatched topics and aliases without cached routes.
    default: HandlerService<AppSt, Err>,
    /// Instantiated resource services indexed by the compiled routes.
    handlers: Vec<HandlerService<AppSt, Err>>,
    /// Session-local topic aliases caching a matched handler and captured routing path.
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

/// Unit tests for router formatting and handler shutdown propagation.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::v5::codec::PublishAckReason;

    /// STATEMENT: Router debug output identifies the MQTT 5 router type.
    ///
    /// HOW:
    /// - Create a router with a successful fallback publish handler.
    /// - Format it and check that the output contains `v5::Router`.
    #[test]
    fn test_debug() {
        let router: Router<(), ()> =
            Router::new(async |_: Publish| Ok::<_, ()>(PublishAck::new(PublishAckReason::Success)));
        assert!(format!("{router:?}").contains("v5::Router"));
    }

    /// Publish service incrementing a shared counter each time it is shut down.
    struct Tracked(Rc<std::cell::Cell<usize>>);

    impl Service<Session<()>, Publish> for Tracked {
        type Res = PublishAck;
        type Error = ();

        async fn call(&self, _: Publish, _: Ctx<'_, Self, Session<()>>) -> Result<PublishAck, ()> {
            Ok(PublishAck::new(PublishAckReason::Success))
        }

        async fn shutdown(&self, _: Ctx<'_, Self, Session<()>>) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// STATEMENT: Router shutdown reaches resource handlers and the default handler.
    ///
    /// HOW:
    /// - Create a session and a router with one tracked resource and one tracked fallback.
    /// - Shut down its service pipeline and assert the shared shutdown counter is two.
    #[ntex::test]
    async fn test_shutdown() {
        use ntex_io::{Io, testing::IoTest};
        use ntex_service::{Pipeline, cfg::SharedCfg, fn_factory};

        use crate::v5::{MqttSink, codec, shared::MqttShared};

        let (_client, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("DBG"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
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

    /// STATEMENT: Topic aliases preserve captured routing parameters, can be rebound, and unknown aliases use the fallback.
    ///
    /// HOW:
    /// - Register an alias on a parameterized route and send an empty-topic publish using it.
    /// - Rebind the alias to another topic and verify both handler selection and captured values.
    /// - Send an unknown alias, an unmatched topic, and an empty topic without an alias; check fallback responses.
    /// - Check the instantiated service identifies its type in debug output.
    #[ntex::test]
    async fn test_topic_alias_routing() {
        use crate::v5::{MqttSink, codec, shared::MqttShared};
        use ntex_io::{Io, testing::IoTest};
        use ntex_service::{Pipeline, cfg::SharedCfg};

        let (_client, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("alias"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::new(),
            Rc::default(),
        ));
        let ses = Session::new((), MqttSink::new(shared), io.shared());
        let factory = Router::new(async |p: Publish| {
            Ok::<_, ()>(p.ack().reason(ByteString::from_static("fallback")))
        })
        .resource("sensor/{id}", async |p: Publish| {
            let id = ByteString::from(p.topic().get("id").unwrap());
            Ok::<_, ()>(p.ack().reason(id))
        })
        .resource("status", async |p: Publish| {
            Ok::<_, ()>(p.ack().reason(ByteString::from_static("status")))
        })
        .build();
        let service = factory.create(&ses).await.unwrap();
        assert!(format!("{service:?}").contains("v5::RouterService"));
        let svc = Pipeline::new(ses.clone(), service);
        for (topic, alias, expected) in [
            ("sensor/42", 1, "42"),
            ("", 1, "42"),
            ("status", 1, "status"),
            ("", 1, "status"),
            ("", 2, "fallback"),
            ("unmatched", 0, "fallback"),
            ("", 0, "fallback"),
        ] {
            let pkt = codec::Publish {
                dup: false,
                retain: false,
                qos: codec::QoS::AtMostOnce,
                topic: topic.into(),
                packet_id: None,
                payload_size: 0,
                properties: codec::PublishProperties {
                    topic_alias: NonZeroU16::new(alias),
                    ..Default::default()
                },
            };
            let ack = svc
                .call(Publish::new(pkt, crate::payload::Payload::default(), 0))
                .await
                .unwrap();
            assert_eq!(ack.reason_string.as_deref(), Some(expected));
        }
    }
}
