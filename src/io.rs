//! Framed transport dispatcher
use std::task::{Context, Poll};
use std::{cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc, time};

use ntex::codec::{Decoder, Encoder};
use ntex::io::{DispatchItem, IoBoxed, IoRef, IoStatusUpdate, RecvError};
use ntex::service::{IntoService, Pipeline, PipelineCall, Service};
use ntex::time::Seconds;
use ntex::util::{ready, Pool};

type Response<U> = <U as Encoder>::Item;

pin_project_lite::pin_project! {
    /// Dispatcher for mqtt protocol
    pub(crate) struct Dispatcher<S, U>
    where
        S: Service<DispatchItem<U>, Response = Option<Response<U>>>,
        S: 'static,
        U: Encoder,
    U: Decoder,
    U: 'static,
    {
        codec: U,
        service: Pipeline<S>,
        inner: DispatcherInner<S, U>,
        pool: Pool,
        #[pin]
        response: Option<PipelineCall<S, DispatchItem<U>>>,
        response_idx: usize,
    }
}

bitflags::bitflags! {
    struct Flags: u8  {
        const READY_ERR  = 0b0001;
        const IO_ERR     = 0b0010;
    }
}

struct DispatcherInner<S: Service<DispatchItem<U>>, U: Encoder + Decoder> {
    io: IoBoxed,
    flags: Flags,
    st: IoDispatcherState,
    state: Rc<RefCell<DispatcherState<S, U>>>,
    keepalive_timeout: time::Duration,
}

struct DispatcherState<S: Service<DispatchItem<U>>, U: Encoder + Decoder> {
    error: Option<IoDispatcherError<S::Error, <U as Encoder>::Error>>,
    base: usize,
    queue: VecDeque<ServiceResult<Result<S::Response, S::Error>>>,
}

enum ServiceResult<T> {
    Pending,
    Ready(T),
}

impl<T> ServiceResult<T> {
    fn take(&mut self) -> Option<T> {
        let slf = std::mem::replace(self, ServiceResult::Pending);
        match slf {
            ServiceResult::Pending => None,
            ServiceResult::Ready(result) => Some(result),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum IoDispatcherState {
    Processing,
    Stop,
    Shutdown,
}

pub(crate) enum IoDispatcherError<S, U> {
    KeepAlive,
    Encoder(U),
    Service(S),
}

enum PollService<U: Encoder + Decoder> {
    Item(DispatchItem<U>),
    Continue,
    Ready,
}

impl<S, U> From<S> for IoDispatcherError<S, U> {
    fn from(err: S) -> Self {
        IoDispatcherError::Service(err)
    }
}

impl<S, U> Dispatcher<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + Clone + 'static,
    <U as Encoder>::Item: 'static,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(crate) fn new<F: IntoService<S, DispatchItem<U>>>(
        io: IoBoxed,
        codec: U,
        service: F,
    ) -> Self {
        let keepalive_timeout = Seconds(30).into();

        // register keepalive timer
        io.start_keepalive_timer(keepalive_timeout);

        let state = Rc::new(RefCell::new(DispatcherState {
            error: None,
            base: 0,
            queue: VecDeque::new(),
        }));
        let pool = io.memory_pool().pool();

        Dispatcher {
            codec,
            pool,
            service: Pipeline::new(service.into_service()),
            response: None,
            response_idx: 0,
            inner: DispatcherInner {
                io,
                state,
                keepalive_timeout,
                flags: Flags::empty(),
                st: IoDispatcherState::Processing,
            },
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub(crate) fn keepalive_timeout(mut self, timeout: Seconds) -> Self {
        let timeout = timeout.into();
        self.inner.io.start_keepalive_timer(timeout);
        self.inner.keepalive_timeout = timeout;
        self
    }

    /// Set connection disconnect timeout.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 1 seconds.
    pub(crate) fn disconnect_timeout(self, val: Seconds) -> Self {
        self.inner.io.set_disconnect_timeout(val.into());
        self
    }
}

impl<S, U> DispatcherInner<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + Clone + 'static,
    <U as Encoder>::Item: 'static,
{
    fn update_keepalive(&self) {
        // update keep-alive timer
        self.io.start_keepalive_timer(self.keepalive_timeout);
    }

    fn unregister_keepalive(&mut self) {
        // unregister keep-alive timer
        self.io.stop_keepalive_timer();
        self.keepalive_timeout = time::Duration::ZERO;
    }
}

impl<S, U> DispatcherState<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    fn handle_result(
        &mut self,
        item: Result<S::Response, S::Error>,
        response_idx: usize,
        io: &IoRef,
        codec: &U,
        wake: bool,
    ) {
        let idx = response_idx.wrapping_sub(self.base);

        // handle first response
        if idx == 0 {
            let _ = self.queue.pop_front();
            self.base = self.base.wrapping_add(1);
            match item {
                Err(err) => {
                    self.error = Some(err.into());
                }
                Ok(Some(item)) => {
                    if let Err(err) = io.encode(item, codec) {
                        self.error = Some(IoDispatcherError::Encoder(err));
                    }
                }
                Ok(None) => (),
            }

            // check remaining response
            while let Some(item) = self.queue.front_mut().and_then(|v| v.take()) {
                let _ = self.queue.pop_front();
                self.base = self.base.wrapping_add(1);
                match item {
                    Err(err) => {
                        self.error = Some(err.into());
                    }
                    Ok(Some(item)) => {
                        if let Err(err) = io.encode(item, codec) {
                            self.error = Some(IoDispatcherError::Encoder(err));
                        }
                    }
                    Ok(None) => (),
                }
            }

            if wake && self.queue.is_empty() {
                io.wake()
            }
        } else {
            self.queue[idx] = ServiceResult::Ready(item);
        }
    }
}

impl<S, U> Future for Dispatcher<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + Clone + 'static,
    <U as Encoder>::Item: 'static,
{
    type Output = Result<(), S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        let inner = &mut this.inner;

        // println!("IO-DISP poll :{:?}:", this.st);

        // handle service response future
        if let Some(fut) = this.response.as_mut().as_pin_mut() {
            if let Poll::Ready(item) = fut.poll(cx) {
                inner.state.borrow_mut().handle_result(
                    item,
                    *this.response_idx,
                    inner.io.as_ref(),
                    this.codec,
                    false,
                );
                this.response.set(None);
            }
        }

        // handle memory pool pressure
        if this.pool.poll_ready(cx).is_pending() {
            inner.io.pause();
            return Poll::Pending;
        }

        loop {
            match inner.st {
                IoDispatcherState::Processing => {
                    // println!("IO-DISP state :{:?}:", io.flags());
                    let item = match ready!(inner.poll_service(this.service, cx,)) {
                        PollService::Ready => {
                            // decode incoming bytes stream
                            match ready!(inner.io.poll_recv(this.codec, cx)) {
                                Ok(el) => {
                                    // update keep-alive timer
                                    inner.update_keepalive();
                                    Some(DispatchItem::Item(el))
                                }
                                Err(RecvError::Stop) => {
                                    log::trace!("dispatcher is instructed to stop");
                                    inner.st = IoDispatcherState::Stop;
                                    None
                                }
                                Err(RecvError::KeepAlive) => {
                                    // check keepalive timeout
                                    log::trace!("keepalive timeout");
                                    inner.st = IoDispatcherState::Stop;
                                    let mut state = inner.state.borrow_mut();
                                    if state.error.is_none() {
                                        state.error = Some(IoDispatcherError::KeepAlive);
                                    }
                                    Some(DispatchItem::KeepAliveTimeout)
                                }
                                Err(RecvError::WriteBackpressure) => {
                                    if let Err(err) = ready!(inner.io.poll_flush(cx, false)) {
                                        inner.st = IoDispatcherState::Stop;
                                        Some(DispatchItem::Disconnect(Some(err)))
                                    } else {
                                        continue;
                                    }
                                }
                                Err(RecvError::Decoder(err)) => {
                                    inner.st = IoDispatcherState::Stop;
                                    Some(DispatchItem::DecoderError(err))
                                }
                                Err(RecvError::PeerGone(err)) => {
                                    inner.st = IoDispatcherState::Stop;
                                    Some(DispatchItem::Disconnect(err))
                                }
                            }
                        }
                        PollService::Item(item) => Some(item),
                        PollService::Continue => continue,
                    };

                    // call service
                    if let Some(item) = item {
                        // optimize first call
                        if this.response.is_none() {
                            this.response.set(Some(this.service.call(item)));
                            let res = this.response.as_mut().as_pin_mut().unwrap().poll(cx);

                            let mut state = inner.state.borrow_mut();
                            let response_idx = state.base.wrapping_add(state.queue.len());

                            if let Poll::Ready(res) = res {
                                // check if current result is only response
                                if state.queue.is_empty() {
                                    match res {
                                        Err(err) => {
                                            state.error = Some(err.into());
                                        }
                                        Ok(Some(item)) => {
                                            if let Err(err) = inner.io.encode(item, this.codec)
                                            {
                                                state.error =
                                                    Some(IoDispatcherError::Encoder(err));
                                            }
                                        }
                                        Ok(None) => (),
                                    }
                                } else {
                                    *this.response_idx = response_idx;
                                    state.queue.push_back(ServiceResult::Ready(res));
                                }
                                this.response.set(None);
                            } else {
                                *this.response_idx = response_idx;
                                state.queue.push_back(ServiceResult::Pending);
                            }
                        } else {
                            let mut state = inner.state.borrow_mut();
                            let response_idx = state.base.wrapping_add(state.queue.len());
                            state.queue.push_back(ServiceResult::Pending);

                            let st = inner.io.get_ref();
                            let codec = this.codec.clone();
                            let state = inner.state.clone();
                            let fut = this.service.call(item);
                            ntex::rt::spawn(async move {
                                let item = fut.await;
                                state.borrow_mut().handle_result(
                                    item,
                                    response_idx,
                                    &st,
                                    &codec,
                                    true,
                                );
                            });
                        }
                    }
                }
                // drain service responses and shutdown io
                IoDispatcherState::Stop => {
                    inner.unregister_keepalive();

                    // service may relay on poll_ready for response results
                    if !inner.flags.contains(Flags::READY_ERR) {
                        let _ = this.service.poll_ready(cx);
                    }

                    if inner.state.borrow().queue.is_empty() {
                        if inner.io.poll_shutdown(cx).is_ready() {
                            log::trace!("io shutdown completed");
                            inner.st = IoDispatcherState::Shutdown;
                            continue;
                        }
                    } else if !inner.flags.contains(Flags::IO_ERR) {
                        match ready!(inner.io.poll_status_update(cx)) {
                            IoStatusUpdate::PeerGone(_)
                            | IoStatusUpdate::Stop
                            | IoStatusUpdate::KeepAlive => {
                                inner.flags.insert(Flags::IO_ERR);
                                continue;
                            }
                            IoStatusUpdate::WriteBackpressure => {
                                if ready!(inner.io.poll_flush(cx, true)).is_err() {
                                    inner.flags.insert(Flags::IO_ERR);
                                }
                                continue;
                            }
                        }
                    } else {
                        inner.io.poll_dispatch(cx);
                    }
                    return Poll::Pending;
                }
                // shutdown service
                IoDispatcherState::Shutdown => {
                    return if this.service.poll_shutdown(cx).is_ready() {
                        log::trace!("service shutdown is completed, stop");

                        Poll::Ready(
                            if let Some(IoDispatcherError::Service(err)) =
                                inner.state.borrow_mut().error.take()
                            {
                                Err(err)
                            } else {
                                Ok(())
                            },
                        )
                    } else {
                        Poll::Pending
                    };
                }
            }
        }
    }
}

impl<S, U> DispatcherInner<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + Clone + 'static,
    <U as Encoder>::Item: 'static,
{
    fn poll_service(
        &mut self,
        srv: &Pipeline<S>,
        cx: &mut Context<'_>,
    ) -> Poll<PollService<U>> {
        match srv.poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                // check for errors
                let mut state = self.state.borrow_mut();
                Poll::Ready(if let Some(err) = state.error.take() {
                    log::trace!("error occured, stopping dispatcher");
                    match err {
                        IoDispatcherError::Encoder(err) => {
                            self.st = IoDispatcherState::Stop;
                            PollService::Item(DispatchItem::EncoderError(err))
                        }
                        IoDispatcherError::Service(err) => {
                            state.error = Some(IoDispatcherError::Service(err));
                            PollService::Ready
                        }
                        IoDispatcherError::KeepAlive => {
                            self.st = IoDispatcherState::Stop;
                            PollService::Item(DispatchItem::KeepAliveTimeout)
                        }
                    }
                } else {
                    PollService::Ready
                })
            }
            // pause io read task
            Poll::Pending => {
                log::trace!("service is not ready, pause read task");
                match ready!(self.io.poll_read_pause(cx)) {
                    IoStatusUpdate::KeepAlive => {
                        log::trace!("keep-alive error, stopping dispatcher during pause");
                        self.st = IoDispatcherState::Stop;
                        Poll::Ready(PollService::Item(DispatchItem::KeepAliveTimeout))
                    }
                    IoStatusUpdate::Stop => {
                        log::trace!("dispatcher is instructed to stop during pause");
                        self.st = IoDispatcherState::Stop;
                        Poll::Ready(PollService::Continue)
                    }
                    IoStatusUpdate::PeerGone(err) => {
                        log::trace!(
                            "peer is gone during pause, stopping dispatcher: {:?}",
                            err
                        );
                        self.st = IoDispatcherState::Stop;
                        Poll::Ready(PollService::Item(DispatchItem::Disconnect(err)))
                    }
                    IoStatusUpdate::WriteBackpressure => Poll::Pending,
                }
            }
            // handle service readiness error
            Poll::Ready(Err(err)) => {
                log::trace!("service readiness check failed, stopping");
                self.st = IoDispatcherState::Stop;
                self.state.borrow_mut().error = Some(IoDispatcherError::Service(err));
                self.flags.insert(Flags::READY_ERR);
                Poll::Ready(PollService::Continue)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use ntex::channel::condition::Condition;
    use ntex::time::{sleep, Millis};
    use ntex::util::{Bytes, Ready};
    use ntex::{codec::BytesCodec, io as nio, service::ServiceCtx, testing::Io};

    use super::*;

    impl<S, U> Dispatcher<S, U>
    where
        S: Service<DispatchItem<U>, Response = Option<Response<U>>>,
        S::Error: 'static,
        U: Decoder + Encoder + 'static,
        <U as Encoder>::Item: 'static,
    {
        /// Construct new `Dispatcher` instance
        pub(crate) fn new_debug<F: IntoService<S, DispatchItem<U>>>(
            io: nio::Io,
            codec: U,
            service: F,
        ) -> (Self, nio::IoRef) {
            let keepalive_timeout = Seconds(30).into();
            io.start_keepalive_timer(keepalive_timeout);
            let rio = io.get_ref();

            let state = Rc::new(RefCell::new(DispatcherState {
                error: None,
                base: 0,
                queue: VecDeque::new(),
            }));

            (
                Dispatcher {
                    codec,
                    service: Pipeline::new(service.into_service()),
                    response: None,
                    response_idx: 0,
                    pool: io.memory_pool().pool(),
                    inner: DispatcherInner {
                        state,
                        keepalive_timeout,
                        io: IoBoxed::from(io),
                        st: IoDispatcherState::Processing,
                        flags: Flags::empty(),
                    },
                },
                rio,
            )
        }
    }

    #[ntex::test]
    async fn test_basic() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex::service::fn_service(|msg: DispatchItem<BytesCodec>| async move {
                sleep(Millis(50)).await;
                if let DispatchItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex::rt::spawn(async move {
            let _ = disp.await;
        });
        sleep(Millis(25)).await;
        client.write("GET /test HTTP/1\r\n\r\n");

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        client.close().await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_ordering() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("test");

        let condition = Condition::new();
        let waiter = condition.wait();

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex::service::fn_service(move |msg: DispatchItem<BytesCodec>| {
                let waiter = waiter.clone();
                async move {
                    waiter.await;
                    if let DispatchItem::Item(msg) = msg {
                        Ok::<_, ()>(Some(msg.freeze()))
                    } else {
                        panic!()
                    }
                }
            }),
        );
        ntex::rt::spawn(async move {
            let _ = disp.await;
        });
        sleep(Millis(50)).await;

        client.write("test");
        sleep(Millis(50)).await;
        client.write("test");
        sleep(Millis(50)).await;
        condition.notify();

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"testtesttest"));

        client.close().await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_sink() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex::service::fn_service(|msg: DispatchItem<BytesCodec>| async move {
                if let DispatchItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex::rt::spawn(async move {
            let _ = disp.disconnect_timeout(Seconds(1)).await;
        });

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        assert!(io.encode(Bytes::from_static(b"test"), &BytesCodec).is_ok());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"test"));

        io.close();
        sleep(Millis(1200)).await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex::service::fn_service(|_: DispatchItem<BytesCodec>| async move {
                Err::<Option<Bytes>, _>(())
            }),
        );
        ntex::rt::spawn(async move {
            let _ = disp.await;
        });

        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &BytesCodec).unwrap();

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        assert!(!client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service_ready() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let counter = Rc::new(Cell::new(0));

        struct Srv(Rc<Cell<usize>>);

        impl Service<DispatchItem<BytesCodec>> for Srv {
            type Response = Option<Response<BytesCodec>>;
            type Error = ();
            type Future<'f> = Ready<Option<Response<BytesCodec>>, ()>;

            fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
                self.0.set(self.0.get() + 1);
                Poll::Ready(Err(()))
            }

            fn call<'a>(
                &'a self,
                _: DispatchItem<BytesCodec>,
                _: ServiceCtx<'a, Self>,
            ) -> Self::Future<'a> {
                Ready::Ok(None)
            }
        }

        let (disp, io) =
            Dispatcher::new_debug(nio::Io::new(server), BytesCodec, Srv(counter.clone()));
        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &mut BytesCodec).unwrap();
        ntex::rt::spawn(async move {
            let _ = disp.await;
        });

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        assert!(client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());

        // service must be checked for readiness only once
        assert_eq!(counter.get(), 1);
    }

    #[ntex::test]
    async fn test_shutdown_dispatcher_waker() {
        let (client, server) = Io::create();
        let server = nio::Io::new(server);
        client.remote_buffer_cap(1024);

        let flag = Rc::new(Cell::new(true));
        let flag2 = flag.clone();
        let server_ref = server.get_ref();

        let (disp, _io) = Dispatcher::new_debug(
            server,
            BytesCodec,
            ntex::service::fn_service(move |item: DispatchItem<BytesCodec>| {
                let first = flag2.get();
                flag2.set(false);
                let io = server_ref.clone();
                async move {
                    match item {
                        DispatchItem::Item(b) => {
                            if !first {
                                sleep(Millis(500)).await;
                            }
                            Ok(Some(b.freeze()))
                        }
                        _ => {
                            io.close();
                            Ok::<_, ()>(None)
                        }
                    }
                }
            }),
        );
        let (tx, rx) = ntex::channel::oneshot::channel();
        ntex::rt::spawn(async move {
            let _ = disp.await;
            let _ = tx.send(());
        });

        // send first message
        client.write(b"msg1");
        sleep(Millis(25)).await;

        // send second message
        client.write(b"msg2");

        // receive response to first message
        sleep(Millis(150)).await;
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"msg1"));

        // close read side
        client.close().await;
        let _ = rx.recv().await;
    }
}
