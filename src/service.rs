use std::{fmt, marker::PhantomData};

use ntex_codec::{Decoder, Encoder};
use ntex_error::Failure;
use ntex_io::IoBoxed;
use ntex_service::pipeline::{Pipeline, PipelineFactory};
use ntex_service::{BindCtx, Ctx, Middleware, RequestState, Service, ServiceFactory};

use crate::error::{DecodeError, DispatcherError, EncodeError, MqttError};
use crate::{ConnectPipeline, Session, control::Control, io::Dispatcher, io::FrameState};

/// Decoded packet or payload chunk delivered to the protocol handler.
type Request<U> = <U as Decoder>::Item;
/// Optional dispatcher response associated with a decoded frame.
type Response<U> = Option<<U as FrameState>::Response>;
/// Optional wire packet returned by the connection-control service.
type ControlResponse<U> = Option<<U as Encoder>::Item>;

/// Factory for a per-session connection-control pipeline.
type ControlPipeline<AppSt, Codec, Cfg, Err, E> = PipelineFactory<
    Session<Cfg, AppSt>,
    Control<E>,
    ControlResponse<Codec>,
    MqttError<Err>,
    Failure,
>;

/// Protocol-specific service that handshakes a connection and runs its dispatcher.
pub struct MqttServer<St, Im, AppSt, Codec: Encoder, Cfg, Err, E, T, M> {
    /// Handshake pipeline producing the negotiated codec and session.
    connect: ConnectPipeline<St, Im, AppSt, Codec, Cfg, MqttError<Err>>,
    /// Factory for the per-connection protocol packet handler.
    handler: T,
    /// Middleware applied to each newly created packet handler.
    middleware: M,
    /// Factory for lifecycle and transport-error handling after the handshake.
    control: ControlPipeline<AppSt, Codec, Cfg, Err, E>,
    /// Retains the application error and configuration type parameters.
    _t: PhantomData<(E, Cfg)>,
}

impl<St, Im, AppSt, Codec, Cfg, Err, E, T, M> fmt::Debug
    for MqttServer<St, Im, AppSt, Codec, Cfg, Err, E, T, M>
where
    Codec: Encoder,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttServer").finish()
    }
}

impl<St, Im, AppSt, Codec, Cfg, Err, E, T, M> MqttServer<St, Im, AppSt, Codec, Cfg, Err, E, T, M>
where
    St: 'static,
    Im: 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    Codec: Encoder,
{
    /// Combine the handshake, handler factory, middleware, and connection-control factory.
    pub(crate) fn new(
        connect: ConnectPipeline<St, Im, AppSt, Codec, Cfg, MqttError<Err>>,
        handler: T,
        middleware: M,
        control: ControlPipeline<AppSt, Codec, Cfg, Err, E>,
    ) -> Self {
        MqttServer {
            connect,
            handler,
            middleware,
            control,
            _t: PhantomData,
        }
    }
}

impl<St, Im, AppSt, Codec, Cfg, Err, E, T, M, Req> Service<St, Req>
    for MqttServer<St, Im, AppSt, Codec, Cfg, Err, E, T, M>
where
    St: 'static,
    Im: 'static,
    AppSt: 'static,
    Cfg: 'static,
    Err: 'static,
    E: 'static,
    T: ServiceFactory<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
            InitError = Failure,
        > + 'static,
    M: Middleware<T::Service, Session<Cfg, AppSt>>,
    M::Service: Service<
            Session<Cfg, AppSt>,
            Request<Codec>,
            Res = Response<Codec>,
            Error = DispatcherError<E>,
        > + 'static,
    Codec:
        Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + FrameState + Clone + 'static,
    Req: RequestState<IoBoxed, State = Im>,
{
    type Res = ();
    type Error = MqttError<Err>;

    fn bind(&self, ctx: BindCtx<'_>) {
        self.connect.bind_to(&ctx);
    }

    async fn call(&self, req: Req, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        let (st, io) = req.unpack();
        let tag = io.tag();

        let (io, codec, session, keepalive) = self.connect.call((io, st), ctx.st()).await?;
        log::trace!("{tag}: Connection handshake succeeded");

        let control = self
            .control
            .create(session.clone())
            .await
            .map_err(MqttError::HandlerInit)?;
        let handler = self
            .handler
            .create(&session)
            .await
            .map_err(MqttError::HandlerInit)?;
        let hnd = self.middleware.create(&session, handler);
        log::trace!("{tag}: Connection handler is created, starting dispatcher");

        Dispatcher::new(io, codec, Pipeline::new(session, hnd), control)
            .keepalive_timeout(keepalive)
            .await
    }

    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), Self::Error> {
        self.connect.ready(ctx.st()).await
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        self.connect.shutdown(ctx.st()).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_factory, fn_service};

    use super::*;

    /// STATEMENT: A handshake-service failure is returned without constructing per-session handlers.
    ///
    /// HOW:
    /// - Build a server with a counted publish factory and a handshake handler that rejects CONNECT.
    /// - Verify debug formatting works with closure services, send CONNECT, and assert the original service error.
    /// - Assert the publish factory was never initialized.
    #[ntex::test]
    async fn handshake_failure_skips_handler_initialization() {
        let initialized = Rc::new(Cell::new(0));
        let count = initialized.clone();
        let publish = fn_factory(async move |_: &crate::v3::Session<()>| {
            count.set(count.get() + 1);
            Ok::<_, std::io::Error>(fn_service(async |_: crate::v3::Publish| Ok::<_, ()>(())))
        });
        let server = crate::v3::MqttServer::new(publish).build(async |_: crate::v3::Connect| {
            Err::<crate::v3::ConnectAck<()>, _>("handshake rejected")
        });
        assert_eq!(format!("{server:?}"), "MqttServer");
        let pipeline = Pipeline::new((), server);
        let (client, peer) = IoTest::create();
        let io = IoBoxed::from(Io::new(peer, SharedCfg::new("test")));
        client.write(b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00");
        assert!(matches!(
            pipeline.call(((), io)).await,
            Err(MqttError::Service("handshake rejected"))
        ));
        assert_eq!(initialized.get(), 0);
    }

    /// STATEMENT: Session factories initialize in order, and failures retain their handler-initialization context.
    ///
    /// HOW:
    /// - Accept a CONNECT with session state `42` and exercise successful, control-failing, and publish-failing factories.
    /// - Verify a control failure skips the publish factory and each initialization error retains its diagnostic.
    /// - On success, verify the wire CONNACK, close the peer, and assert clean dispatcher completion and one initialization each.
    #[ntex::test]
    async fn session_factory_initialization_and_failures() {
        use ntex_util::future::join;

        for failure in [None, Some("control"), Some("publish")] {
            let publish_count = Rc::new(Cell::new(0));
            let control_count = Rc::new(Cell::new(0));
            let count = publish_count.clone();
            let publish = fn_factory(async move |session: &crate::v3::Session<u8>| {
                assert_eq!(**session, 42);
                count.set(count.get() + 1);
                if failure == Some("publish") {
                    Err(std::io::Error::other("publish initialization failed"))
                } else {
                    Ok(fn_service(async |_: crate::v3::Publish| Ok::<_, ()>(())))
                }
            });
            let count = control_count.clone();
            let control = fn_factory(async move |session: &crate::v3::Session<u8>| {
                assert_eq!(**session, 42);
                count.set(count.get() + 1);
                if failure == Some("control") {
                    Err(std::io::Error::other("control initialization failed"))
                } else {
                    Ok(fn_service(async |_: Control<()>| {
                        Ok::<_, ()>(None::<crate::v3::codec::Encoded>)
                    }))
                }
            });
            let server = crate::v3::MqttServer::new(publish)
                .control(control)
                .build(async |connect: crate::v3::Connect| Ok::<_, ()>(connect.ack(42, false)));
            let pipeline = Pipeline::new((), server);
            let (client, peer) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = IoBoxed::from(Io::new(peer, SharedCfg::new("test")));
            client.write(b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00");
            let call = pipeline.call(((), io));
            if let Some(stage) = failure {
                let Err(MqttError::HandlerInit(error)) = call.await else {
                    panic!("expected a handler initialization error");
                };
                assert!(
                    error
                        .to_string()
                        .contains(&format!("{stage} initialization failed"))
                );
            } else {
                let (result, ()) = join(call, async {
                    assert_eq!(client.read().await.unwrap(), b"\x20\x02\x00\x00".as_slice());
                    client.close().await;
                })
                .await;
                assert!(result.is_ok());
            }
            assert_eq!(control_count.get(), 1);
            assert_eq!(publish_count.get(), usize::from(failure != Some("control")));
        }
    }
}
