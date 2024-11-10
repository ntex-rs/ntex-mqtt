//! Framed transport dispatcher
use std::future::{poll_fn, Future};
use std::task::{ready, Context, Poll};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, pin::Pin, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{
    Decoded, DispatchItem, DispatcherConfig, IoBoxed, IoRef, IoStatusUpdate, RecvError,
};
use ntex_service::{IntoService, Pipeline, PipelineBinding, PipelineCall, Service};
use ntex_util::{task::LocalWaker, time::Seconds};

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
        inner: DispatcherInner<S, U>
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    struct Flags: u8  {
        const READY_ERR     = 0b0000001;
        const IO_ERR        = 0b0000010;
        const KA_ENABLED    = 0b0000100;
        const KA_TIMEOUT    = 0b0001000;
        const READ_TIMEOUT  = 0b0010000;
        const READY         = 0b0100000;
        const READY_TASK    = 0b1000000;
    }
}

struct DispatcherInner<S: Service<DispatchItem<U>>, U: Encoder + Decoder + 'static> {
    io: IoBoxed,
    flags: Flags,
    codec: U,
    service: PipelineBinding<S, DispatchItem<U>>,
    st: IoDispatcherState,
    state: Rc<DispatcherState<S, U>>,
    config: DispatcherConfig,
    read_remains: u32,
    read_remains_prev: u32,
    read_max_timeout: Seconds,
    keepalive_timeout: Seconds,

    response: Option<PipelineCall<S, DispatchItem<U>>>,
    response_idx: usize,
}

struct DispatcherState<S: Service<DispatchItem<U>>, U: Encoder + Decoder> {
    error: Cell<Option<IoDispatcherError<S::Error, <U as Encoder>::Error>>>,
    base: Cell<usize>,
    ready: Cell<bool>,
    queue: RefCell<VecDeque<ServiceResult<Result<S::Response, S::Error>>>>,
    waker: LocalWaker,
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
    Backpressure,
    Stop,
    Shutdown,
}

pub(crate) enum IoDispatcherError<S, U> {
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
        config: &DispatcherConfig,
    ) -> Self {
        // register keepalive timer
        io.set_disconnect_timeout(config.disconnect_timeout());

        let state = Rc::new(DispatcherState {
            error: Cell::new(None),
            base: Cell::new(0),
            ready: Cell::new(false),
            queue: RefCell::new(VecDeque::new()),
            waker: LocalWaker::default(),
        });
        let keepalive_timeout = config.keepalive_timeout();

        Dispatcher {
            inner: DispatcherInner {
                io,
                codec,
                state,
                keepalive_timeout,
                flags: if keepalive_timeout.is_zero() {
                    Flags::KA_ENABLED
                } else {
                    Flags::empty()
                },
                service: Pipeline::new(service.into_service()).bind(),
                config: config.clone(),
                st: IoDispatcherState::Processing,
                response: None,
                response_idx: 0,
                read_remains: 0,
                read_remains_prev: 0,
                read_max_timeout: Seconds::ZERO,
            },
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub(crate) fn keepalive_timeout(mut self, timeout: Seconds) -> Self {
        self.inner.keepalive_timeout = timeout;
        if timeout.is_zero() {
            self.inner.flags.remove(Flags::KA_ENABLED);
        } else {
            self.inner.flags.insert(Flags::KA_ENABLED);
        }
        self
    }
}

impl<S, U> DispatcherState<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    fn handle_result(
        &self,
        item: Result<S::Response, S::Error>,
        response_idx: usize,
        io: &IoRef,
        codec: &U,
        wake: bool,
    ) {
        let mut queue = self.queue.borrow_mut();
        let idx = response_idx.wrapping_sub(self.base.get());

        // handle first response
        if idx == 0 {
            let _ = queue.pop_front();
            self.base.set(self.base.get().wrapping_add(1));
            match item {
                Err(err) => {
                    self.error.set(Some(err.into()));
                }
                Ok(Some(item)) => {
                    if let Err(err) = io.encode(item, codec) {
                        self.error.set(Some(IoDispatcherError::Encoder(err)));
                    }
                }
                Ok(None) => (),
            }

            // check remaining response
            while let Some(item) = queue.front_mut().and_then(|v| v.take()) {
                let _ = queue.pop_front();
                self.base.set(self.base.get().wrapping_add(1));
                match item {
                    Err(err) => {
                        self.error.set(Some(err.into()));
                    }
                    Ok(Some(item)) => {
                        if let Err(err) = io.encode(item, codec) {
                            self.error.set(Some(IoDispatcherError::Encoder(err)));
                        }
                    }
                    Ok(None) => (),
                }
            }

            if wake && queue.is_empty() {
                io.wake()
            }
        } else {
            queue[idx] = ServiceResult::Ready(item);
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

        inner.state.waker.register(cx.waker());

        // handle service response future
        if let Some(fut) = inner.response.as_mut() {
            if let Poll::Ready(item) = Pin::new(fut).poll(cx) {
                inner.state.handle_result(
                    item,
                    inner.response_idx,
                    inner.io.as_ref(),
                    &inner.codec,
                    false,
                );
                inner.response = None;
            }
        }

        // start ready task
        if inner.flags.contains(Flags::READY_TASK) {
            inner.flags.insert(Flags::READY_TASK);
            ntex_rt::spawn(not_ready(inner.state.clone(), inner.service.clone()));
        }

        loop {
            match inner.st {
                IoDispatcherState::Processing => {
                    let item = match ready!(inner.poll_service(cx)) {
                        PollService::Ready => {
                            // decode incoming bytes stream
                            match inner.io.poll_recv_decode(&inner.codec, cx) {
                                Ok(decoded) => {
                                    inner.update_timer(&decoded);
                                    if let Some(el) = decoded.item {
                                        DispatchItem::Item(el)
                                    } else {
                                        return Poll::Pending;
                                    }
                                }
                                Err(RecvError::Stop) => {
                                    log::trace!(
                                        "{}: Dispatcher is instructed to stop",
                                        inner.io.tag()
                                    );
                                    inner.st = IoDispatcherState::Stop;
                                    continue;
                                }
                                Err(RecvError::KeepAlive) => {
                                    if let Err(err) = inner.handle_timeout() {
                                        inner.st = IoDispatcherState::Stop;
                                        err
                                    } else {
                                        continue;
                                    }
                                }
                                Err(RecvError::WriteBackpressure) => {
                                    inner.st = IoDispatcherState::Backpressure;
                                    DispatchItem::WBackPressureEnabled
                                }
                                Err(RecvError::Decoder(err)) => {
                                    inner.st = IoDispatcherState::Stop;
                                    DispatchItem::DecoderError(err)
                                }
                                Err(RecvError::PeerGone(err)) => {
                                    inner.st = IoDispatcherState::Stop;
                                    DispatchItem::Disconnect(err)
                                }
                            }
                        }
                        PollService::Item(item) => item,
                        PollService::Continue => continue,
                    };

                    inner.state.ready.set(false);
                    inner.call_service(cx, item);
                }
                // handle write back-pressure
                IoDispatcherState::Backpressure => {
                    match ready!(inner.poll_service(cx)) {
                        PollService::Ready => (),
                        PollService::Item(item) => inner.call_service(cx, item),
                        PollService::Continue => continue,
                    };

                    let item = if let Err(err) = ready!(inner.io.poll_flush(cx, false)) {
                        inner.st = IoDispatcherState::Stop;
                        DispatchItem::Disconnect(Some(err))
                    } else {
                        inner.st = IoDispatcherState::Processing;
                        DispatchItem::WBackPressureDisabled
                    };
                    inner.call_service(cx, item);
                }

                // drain service responses and shutdown io
                IoDispatcherState::Stop => {
                    inner.io.stop_timer();

                    // service may relay on poll_ready for response results
                    if !inner.flags.contains(Flags::READY_ERR) {
                        if let Poll::Ready(res) = inner.service.poll_ready(cx) {
                            if res.is_err() {
                                inner.flags.insert(Flags::READY_ERR);
                            }
                        }
                    }

                    if inner.state.queue.borrow().is_empty() {
                        if inner.io.poll_shutdown(cx).is_ready() {
                            log::trace!("{}: io shutdown completed", inner.io.tag());
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
                    return if inner.service.poll_shutdown(cx).is_ready() {
                        log::trace!("{}: Service shutdown is completed, stop", inner.io.tag());

                        Poll::Ready(
                            if let Some(IoDispatcherError::Service(err)) =
                                inner.state.error.take()
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
    fn call_service(&mut self, cx: &mut Context<'_>, item: DispatchItem<U>) {
        let mut fut = self.service.call_nowait(item);
        let mut queue = self.state.queue.borrow_mut();

        // optimize first call
        if self.response.is_none() {
            if let Poll::Ready(res) = Pin::new(&mut fut).poll(cx) {
                // check if current result is only response
                if queue.is_empty() {
                    match res {
                        Err(err) => {
                            self.state.error.set(Some(err.into()));
                        }
                        Ok(Some(item)) => {
                            if let Err(err) = self.io.encode(item, &self.codec) {
                                self.state.error.set(Some(IoDispatcherError::Encoder(err)));
                            }
                        }
                        Ok(None) => (),
                    }
                } else {
                    queue.push_back(ServiceResult::Ready(res));
                    self.response_idx = self.state.base.get().wrapping_add(queue.len());
                }
            } else {
                self.response = Some(fut);
                self.response_idx = self.state.base.get().wrapping_add(queue.len());
                queue.push_back(ServiceResult::Pending);
            }
        } else {
            let response_idx = self.state.base.get().wrapping_add(queue.len());
            queue.push_back(ServiceResult::Pending);

            let st = self.io.get_ref();
            let codec = self.codec.clone();
            let state = self.state.clone();

            ntex_util::spawn(async move {
                let item = fut.await;
                state.handle_result(item, response_idx, &st, &codec, true);
            });
        }
    }

    fn check_error(&mut self) -> PollService<U> {
        // check for errors
        if let Some(err) = self.state.error.take() {
            log::trace!("{}: Error occured, stopping dispatcher", self.io.tag());
            self.st = IoDispatcherState::Stop;
            match err {
                IoDispatcherError::Encoder(err) => {
                    PollService::Item(DispatchItem::EncoderError(err))
                }
                IoDispatcherError::Service(err) => {
                    self.state.error.set(Some(IoDispatcherError::Service(err)));
                    PollService::Continue
                }
            }
        } else {
            PollService::Ready
        }
    }

    fn poll_service(&mut self, cx: &mut Context<'_>) -> Poll<PollService<U>> {
        if self.state.ready.get() {
            return Poll::Ready(self.check_error());
        }

        match self.service.poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                self.state.ready.set(true);
                Poll::Ready(self.check_error())
            }
            // pause io read task
            Poll::Pending => {
                log::trace!("{}: Service is not ready, pause read task", self.io.tag());

                // remove timers
                self.flags.remove(Flags::KA_TIMEOUT | Flags::READ_TIMEOUT);
                self.io.stop_timer();

                match ready!(self.io.poll_read_pause(cx)) {
                    IoStatusUpdate::KeepAlive => {
                        log::trace!(
                            "{}: Keep-alive error, stopping dispatcher during pause",
                            self.io.tag()
                        );
                        self.st = IoDispatcherState::Stop;
                        Poll::Ready(PollService::Item(DispatchItem::KeepAliveTimeout))
                    }
                    IoStatusUpdate::Stop => {
                        log::trace!(
                            "{}: Dispatcher is instructed to stop during pause",
                            self.io.tag()
                        );
                        self.st = IoDispatcherState::Stop;
                        Poll::Ready(PollService::Continue)
                    }
                    IoStatusUpdate::PeerGone(err) => {
                        log::trace!(
                            "{}: Peer is gone during pause, stopping dispatcher: {:?}",
                            self.io.tag(),
                            err
                        );
                        self.st = IoDispatcherState::Stop;
                        Poll::Ready(PollService::Item(DispatchItem::Disconnect(err)))
                    }
                    IoStatusUpdate::WriteBackpressure => {
                        self.st = IoDispatcherState::Backpressure;
                        Poll::Ready(PollService::Item(DispatchItem::WBackPressureEnabled))
                    }
                }
            }
            // handle service readiness error
            Poll::Ready(Err(err)) => {
                log::error!("{}: Service readiness check failed, stopping", self.io.tag());
                self.st = IoDispatcherState::Stop;
                self.flags.insert(Flags::READY_ERR);
                self.state.error.set(Some(IoDispatcherError::Service(err)));
                Poll::Ready(PollService::Item(DispatchItem::Disconnect(None)))
            }
        }
    }

    fn update_timer(&mut self, decoded: &Decoded<<U as Decoder>::Item>) {
        // got parsed frame
        if decoded.item.is_some() {
            self.read_remains = 0;
            self.flags.remove(Flags::KA_TIMEOUT | Flags::READ_TIMEOUT);
        } else if self.flags.contains(Flags::READ_TIMEOUT) {
            // received new data but not enough for parsing complete frame
            self.read_remains = decoded.remains as u32;
        } else if self.read_remains == 0 && decoded.remains == 0 {
            // no new data, start keep-alive timer
            if self.flags.contains(Flags::KA_ENABLED) && !self.flags.contains(Flags::KA_TIMEOUT)
            {
                log::debug!(
                    "{}: Start keep-alive timer {:?}",
                    self.io.tag(),
                    self.keepalive_timeout
                );
                self.flags.insert(Flags::KA_TIMEOUT);
                self.io.start_timer(self.keepalive_timeout);
            }
        } else if let Some((timeout, max, _)) = self.config.frame_read_rate() {
            // we got new data but not enough to parse single frame
            // start read timer
            self.flags.insert(Flags::READ_TIMEOUT);

            self.read_remains = decoded.remains as u32;
            self.read_remains_prev = 0;
            self.read_max_timeout = max;
            self.io.start_timer(timeout);

            log::debug!("{}: Start frame read timer {:?}", self.io.tag(), timeout);
        }
    }

    fn handle_timeout(&mut self) -> Result<(), DispatchItem<U>> {
        // check read timer
        if self.flags.contains(Flags::READ_TIMEOUT) {
            if let Some((timeout, max, rate)) = self.config.frame_read_rate() {
                let total =
                    (self.read_remains - self.read_remains_prev).try_into().unwrap_or(u16::MAX);

                // read rate, start timer for next period
                if total > rate {
                    self.read_remains_prev = self.read_remains;
                    self.read_remains = 0;

                    if !max.is_zero() {
                        self.read_max_timeout =
                            Seconds(self.read_max_timeout.0.saturating_sub(timeout.0));
                    }

                    if max.is_zero() || !self.read_max_timeout.is_zero() {
                        log::trace!(
                            "{}: Frame read rate {:?}, extend timer",
                            self.io.tag(),
                            total
                        );
                        self.io.start_timer(timeout);
                        return Ok(());
                    }
                }
                log::trace!("{}: Max payload timeout has been reached", self.io.tag());
                return Err(DispatchItem::ReadTimeout);
            }
        } else if self.flags.contains(Flags::KA_TIMEOUT) {
            log::trace!("{}: Keep-alive error, stopping dispatcher", self.io.tag());
            return Err(DispatchItem::KeepAliveTimeout);
        }
        Ok(())
    }
}

async fn not_ready<S, U>(
    slf: Rc<DispatcherState<S, U>>,
    pl: PipelineBinding<S, DispatchItem<U>>,
) where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Encoder + Decoder + 'static,
{
    loop {
        if !pl.is_shutdown() {
            if let Err(err) = poll_fn(|cx| pl.poll_ready(cx)).await {
                slf.error.set(Some(IoDispatcherError::Service(err)));
                break;
            }
            if !pl.is_shutdown() {
                poll_fn(|cx| pl.poll_not_ready(cx)).await;
                slf.ready.set(false);
                slf.waker.wake();
                continue;
            }
        }
        break;
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, io, sync::Arc, sync::Mutex};

    use ntex_bytes::{Bytes, BytesMut};
    use ntex_codec::BytesCodec;
    use ntex_io::{self as nio, testing::IoTest as Io};
    use ntex_service::ServiceCtx;
    use ntex_util::channel::condition::Condition;
    use ntex_util::time::{sleep, Millis};
    use rand::Rng;

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
            Self::new_debug_cfg(io, codec, DispatcherConfig::default(), service)
        }

        /// Construct new `Dispatcher` instance
        pub(crate) fn new_debug_cfg<F: IntoService<S, DispatchItem<U>>>(
            io: nio::Io,
            codec: U,
            config: DispatcherConfig,
            service: F,
        ) -> (Self, nio::IoRef) {
            let keepalive_timeout = config.keepalive_timeout();
            let rio = io.get_ref();

            let state = Rc::new(DispatcherState {
                error: Cell::new(None),
                base: Cell::new(0),
                ready: Cell::new(false),
                waker: LocalWaker::default(),
                queue: RefCell::new(VecDeque::new()),
            });

            (
                Dispatcher {
                    inner: DispatcherInner {
                        codec,
                        state,
                        config,
                        keepalive_timeout,
                        service: Pipeline::new(service.into_service()).bind(),
                        response: None,
                        response_idx: 0,
                        io: IoBoxed::from(io),
                        st: IoDispatcherState::Processing,
                        flags: if keepalive_timeout.is_zero() {
                            Flags::empty()
                        } else {
                            Flags::KA_ENABLED
                        },
                        read_remains: 0,
                        read_remains_prev: 0,
                        read_max_timeout: Seconds::ZERO,
                    },
                },
                rio,
            )
        }
    }

    #[ntex_macros::rt_test]
    async fn test_basic() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex_service::fn_service(|msg: DispatchItem<BytesCodec>| async move {
                sleep(Millis(50)).await;
                if let DispatchItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex_util::spawn(async move {
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

    #[ntex_macros::rt_test]
    async fn test_ordering() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("test");

        let condition = Condition::new();
        let waiter = condition.wait();

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex_service::fn_service(move |msg: DispatchItem<BytesCodec>| {
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
        ntex_util::spawn(async move {
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

    #[ntex_macros::rt_test]
    async fn test_sink() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex_service::fn_service(|msg: DispatchItem<BytesCodec>| async move {
                if let DispatchItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
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

    #[ntex_macros::rt_test]
    async fn test_err_in_service() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex_service::fn_service(|_: DispatchItem<BytesCodec>| async move {
                Err::<Option<Bytes>, _>(())
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &BytesCodec).unwrap();

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        sleep(Millis(50)).await;
        assert!(client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());
    }

    #[ntex_macros::rt_test]
    async fn test_err_in_service_ready() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let counter = Rc::new(Cell::new(0));

        struct Srv(Rc<Cell<usize>>);

        impl Service<DispatchItem<BytesCodec>> for Srv {
            type Response = Option<Response<BytesCodec>>;
            type Error = ();

            async fn ready(&self, _: ServiceCtx<'_, Self>) -> Result<(), ()> {
                self.0.set(self.0.get() + 1);
                Err(())
            }

            async fn call(
                &self,
                _: DispatchItem<BytesCodec>,
                _: ServiceCtx<'_, Self>,
            ) -> Result<Option<Response<BytesCodec>>, ()> {
                Ok(None)
            }
        }

        let (disp, io) =
            Dispatcher::new_debug(nio::Io::new(server), BytesCodec, Srv(counter.clone()));
        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &BytesCodec).unwrap();
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        sleep(Millis(50)).await;
        assert!(client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());

        // service must be checked for readiness only once
        assert_eq!(counter.get(), 1);
    }

    #[ntex_macros::rt_test]
    async fn test_write_backpressure() {
        let (client, server) = Io::create();
        // do not allow to write to socket
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex_service::fn_service(move |msg: DispatchItem<BytesCodec>| {
                let data = data2.clone();
                async move {
                    match msg {
                        DispatchItem::Item(_) => {
                            data.lock().unwrap().borrow_mut().push(0);
                            let bytes = rand::thread_rng()
                                .sample_iter(&rand::distributions::Alphanumeric)
                                .take(65_536)
                                .map(char::from)
                                .collect::<String>();
                            return Ok::<_, ()>(Some(Bytes::from(bytes)));
                        }
                        DispatchItem::WBackPressureEnabled => {
                            data.lock().unwrap().borrow_mut().push(1);
                        }
                        DispatchItem::WBackPressureDisabled => {
                            data.lock().unwrap().borrow_mut().push(2);
                        }
                        _ => (),
                    }
                    Ok(None)
                }
            }),
        );
        let pool = io.memory_pool().pool().pool_ref();
        pool.set_read_params(8 * 1024, 1024);
        pool.set_write_params(16 * 1024, 1024);

        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        let buf = client.read_any();
        assert_eq!(buf, Bytes::from_static(b""));
        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(25)).await;

        // buf must be consumed
        assert_eq!(client.remote_buffer(|buf| buf.len()), 0);

        // response message
        assert_eq!(io.with_write_buf(|buf| buf.len()).unwrap(), 65536);

        client.remote_buffer_cap(10240);
        sleep(Millis(50)).await;
        assert_eq!(io.with_write_buf(|buf| buf.len()).unwrap(), 55296);

        client.remote_buffer_cap(45056);
        sleep(Millis(50)).await;
        assert_eq!(io.with_write_buf(|buf| buf.len()).unwrap(), 10240);

        // backpressure disabled
        assert_eq!(&data.lock().unwrap().borrow()[..], &[0, 1, 2]);
    }

    #[ntex_macros::rt_test]
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
            ntex_service::fn_service(move |item: DispatchItem<BytesCodec>| {
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
        let (tx, rx) = ntex_util::channel::oneshot::channel();
        ntex_util::spawn(async move {
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

    /// Update keep-alive timer after receiving frame
    #[ntex_macros::rt_test]
    async fn test_keepalive() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server),
            BytesCodec,
            ntex_service::fn_service(move |msg: DispatchItem<BytesCodec>| {
                let data = data2.clone();
                async move {
                    match msg {
                        DispatchItem::Item(bytes) => {
                            data.lock().unwrap().borrow_mut().push(0);
                            return Ok::<_, ()>(Some(bytes.freeze()));
                        }
                        DispatchItem::KeepAliveTimeout => {
                            data.lock().unwrap().borrow_mut().push(1);
                        }
                        _ => (),
                    }
                    Ok(None)
                }
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.keepalive_timeout(Seconds(2)).await;
        });

        client.write("1");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"1"));
        sleep(Millis(750)).await;

        client.write("2");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"2"));

        sleep(Millis(750)).await;
        client.write("3");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"3"));

        sleep(Millis(750)).await;
        assert!(!client.is_closed());
        assert_eq!(&data.lock().unwrap().borrow()[..], &[0, 0, 0]);
    }

    #[derive(Debug, Copy, Clone)]
    struct BytesLenCodec(usize);

    impl Encoder for BytesLenCodec {
        type Item = Bytes;
        type Error = io::Error;

        #[inline]
        fn encode(&self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.extend_from_slice(&item[..]);
            Ok(())
        }
    }

    impl Decoder for BytesLenCodec {
        type Item = BytesMut;
        type Error = io::Error;

        fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.len() >= self.0 {
                Ok(Some(src.split_to(self.0)))
            } else {
                Ok(None)
            }
        }
    }

    /// Do not use keep-alive timer if not configured
    #[ntex_macros::rt_test]
    async fn test_no_keepalive_err_after_frame_timeout() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();

        let config = DispatcherConfig::default();
        config.set_keepalive_timeout(Seconds(0)).set_frame_read_rate(Seconds(1), Seconds(2), 2);

        let (disp, _) = Dispatcher::new_debug_cfg(
            nio::Io::new(server),
            BytesLenCodec(2),
            config,
            ntex_service::fn_service(move |msg: DispatchItem<BytesLenCodec>| {
                let data = data2.clone();
                async move {
                    match msg {
                        DispatchItem::Item(bytes) => {
                            data.lock().unwrap().borrow_mut().push(0);
                            return Ok::<_, ()>(Some(bytes.freeze()));
                        }
                        DispatchItem::KeepAliveTimeout => {
                            data.lock().unwrap().borrow_mut().push(1);
                        }
                        _ => (),
                    }
                    Ok(None)
                }
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("1");
        sleep(Millis(250)).await;
        client.write("2");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"12"));
        sleep(Millis(2000)).await;

        assert_eq!(&data.lock().unwrap().borrow()[..], &[0]);
    }

    #[ntex_macros::rt_test]
    async fn test_read_timeout() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();

        let config = DispatcherConfig::default();
        config.set_keepalive_timeout(Seconds::ZERO).set_frame_read_rate(
            Seconds(1),
            Seconds(2),
            2,
        );

        let (disp, state) = Dispatcher::new_debug_cfg(
            nio::Io::new(server),
            BytesLenCodec(8),
            config,
            ntex_service::fn_service(move |msg: DispatchItem<BytesLenCodec>| {
                let data = data2.clone();
                async move {
                    match msg {
                        DispatchItem::Item(bytes) => {
                            data.lock().unwrap().borrow_mut().push(0);
                            return Ok::<_, ()>(Some(bytes.freeze()));
                        }
                        DispatchItem::ReadTimeout => {
                            data.lock().unwrap().borrow_mut().push(1);
                        }
                        _ => (),
                    }
                    Ok(None)
                }
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("12345678");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"12345678"));

        client.write("1");
        sleep(Millis(1000)).await;
        assert!(!state.flags().contains(nio::Flags::IO_STOPPING));
        client.write("23");
        sleep(Millis(1000)).await;
        assert!(!state.flags().contains(nio::Flags::IO_STOPPING));
        client.write("4");
        sleep(Millis(2000)).await;

        // write side must be closed, dispatcher should fail with keep-alive
        assert!(state.flags().contains(nio::Flags::IO_STOPPING));
        assert!(client.is_closed());
        assert_eq!(&data.lock().unwrap().borrow()[..], &[0, 1]);
    }
}
