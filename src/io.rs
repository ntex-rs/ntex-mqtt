//! Framed transport dispatcher
use std::task::{Context, Poll, ready};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Decoded, IoBoxed, IoRef, IoStatusUpdate, RecvError};
use ntex_service::{Pipeline, PipelineBinding, PipelineCall, Service};
use ntex_util::channel::condition::Condition;
use ntex_util::{future::Either, future::select, spawn, task::LocalWaker, time::Seconds};

use crate::control::Control;
use crate::error::{DecodeError, DispatcherError, EncodeError, ProtocolError};

type Request<U> = <U as Decoder>::Item;
type Response<U> = <U as Encoder>::Item;
type Queue<T, E> = RefCell<VecDeque<ServiceResult<Result<T, E>>>>;

pin_project_lite::pin_project! {
    /// Dispatcher for mqtt protocol
    pub(crate) struct Dispatcher<P, C, U, E>
    where
        P: Service<Request<U>, Response = Option<Response<U>>, Error = DispatcherError<E>>,
        P: 'static,
        C: Service<Control<E>, Response = Option<Response<U>>>,
        C: 'static,
        U: Encoder,
        U: Decoder,
        U: 'static,
        E: 'static
    {
        inner: DispatcherInner<P, C, U, E>
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    struct Flags: u8  {
        const READY_ERR     = 0b0000_0001;
        const IO_ERR        = 0b0000_0010;
        const KA_ENABLED    = 0b0000_0100;
        const KA_TIMEOUT    = 0b0000_1000;
        const READ_TIMEOUT  = 0b0001_0000;
        const READY         = 0b0010_0000;
        const READY_TASK    = 0b0100_0000;
    }
}

struct DispatcherInner<P, C, U, E>
where
    P: Service<Request<U>>,
    C: Service<Control<E>>,
    U: Encoder + Decoder + 'static,
    E: 'static,
{
    io: IoBoxed,
    flags: Flags,
    codec: U,
    service: PipelineBinding<P, Request<U>>,
    control: PipelineBinding<C, Control<E>>,
    st: IoDispatcherState<C, E>,
    state: Rc<DispatcherState<P, U>>,
    stopping: Condition,
    read_remains: u32,
    read_remains_prev: u32,
    read_max_timeout: Seconds,
    keepalive_timeout: Seconds,
}

struct DispatcherState<P, U>
where
    P: Service<Request<U>>,
    U: Encoder + Decoder + 'static,
{
    error: Cell<Option<IoDispatcherError<P::Error>>>,
    base: Cell<usize>,
    queue: Queue<P::Response, P::Error>,
    waker: LocalWaker,
    response: Cell<Option<PipelineCall<P, Request<U>>>>,
    response_idx: Cell<usize>,
}

enum ServiceResult<T> {
    Pending,
    Ready(T),
}

impl<T> ServiceResult<T> {
    fn take(&mut self) -> Option<T> {
        let this = std::mem::replace(self, ServiceResult::Pending);
        match this {
            ServiceResult::Pending => None,
            ServiceResult::Ready(result) => Some(result),
        }
    }
}

#[derive(Debug)]
enum IoDispatcherState<C: Service<Control<E>>, E: 'static> {
    Processing,
    Backpressure,
    Stop(Option<PipelineCall<C, Control<E>>>),
    Shutdown(Option<Result<(), C::Error>>),
    ShutdownIo(Option<Result<(), C::Error>>),
}

pub(crate) enum IoDispatcherError<S> {
    Encoder(EncodeError),
    Service(S),
}

enum PollService {
    Continue,
    Ready,
}

impl<P, C, U, E> Dispatcher<P, C, U, E>
where
    P: Service<Request<U>, Response = Option<Response<U>>, Error = DispatcherError<E>>
        + 'static,
    C: Service<Control<E>, Response = Option<Response<U>>> + 'static,
    U: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
    <U as Encoder>::Item: 'static,
    E: 'static,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(crate) fn new(io: IoBoxed, codec: U, service: P, control: C) -> Self {
        let state = Rc::new(DispatcherState {
            error: Cell::new(None),
            base: Cell::new(0),
            queue: RefCell::new(VecDeque::new()),
            waker: LocalWaker::default(),
            response: Cell::new(None),
            response_idx: Cell::new(0),
        });
        let keepalive_timeout = io.cfg().keepalive_timeout();

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
                service: Pipeline::new(service).bind(),
                control: Pipeline::new(control).bind(),
                st: IoDispatcherState::Processing,
                stopping: Condition::new(),
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

impl<P, U> DispatcherState<P, U>
where
    P: Service<Request<U>, Response = Option<Response<U>>> + 'static,
    U: Encoder<Error = EncodeError> + Decoder<Error = DecodeError>,
    <U as Encoder>::Item: 'static,
{
    fn handle_result(
        &self,
        item: Result<P::Response, P::Error>,
        response_idx: usize,
        io: &IoRef,
        codec: &U,
    ) -> bool {
        let err = item.is_err();
        let mut queue = self.queue.borrow_mut();
        let idx = response_idx.wrapping_sub(self.base.get());

        // handle first response
        if idx == 0 {
            let _ = queue.pop_front();
            self.base.set(self.base.get().wrapping_add(1));
            match item {
                Err(err) => {
                    self.error.set(Some(IoDispatcherError::Service(err)));
                }
                Ok(Some(item)) => {
                    if let Err(err) = io.encode(item, codec) {
                        self.error.set(Some(IoDispatcherError::Encoder(err)));
                    }
                }
                Ok(None) => (),
            }

            // check remaining response
            while let Some(item) = queue.front_mut().and_then(ServiceResult::take) {
                let _ = queue.pop_front();
                self.base.set(self.base.get().wrapping_add(1));
                match item {
                    Err(err) => {
                        self.error.set(Some(IoDispatcherError::Service(err)));
                    }
                    Ok(Some(item)) => {
                        if let Err(err) = io.encode(item, codec) {
                            self.error.set(Some(IoDispatcherError::Encoder(err)));
                        }
                    }
                    Ok(None) => (),
                }
            }

            err || queue.is_empty()
        } else {
            if let Err(err) = item {
                self.error.set(Some(IoDispatcherError::Service(err)));
            } else {
                queue[idx] = ServiceResult::Ready(item);
            }
            err
        }
    }
}

impl<P, C, U, E> Future for Dispatcher<P, C, U, E>
where
    P: Service<Request<U>, Response = Option<Response<U>>, Error = DispatcherError<E>>
        + 'static,
    C: Service<Control<E>, Response = Option<Response<U>>> + 'static,
    U: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
    <U as Encoder>::Item: 'static,
    E: 'static,
{
    type Output = Result<(), C::Error>;

    #[allow(clippy::too_many_lines)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        let inner = this.inner;
        inner.state.waker.register(cx.waker());

        // check control service readiness
        ready!(inner.control.poll_ready(cx))?;

        // handle service response future
        if let Some(mut fut) = inner.state.response.take() {
            if let Poll::Ready(item) = Pin::new(&mut fut).poll(cx) {
                inner.state.handle_result(
                    item,
                    inner.state.response_idx.get(),
                    inner.io.as_ref(),
                    &inner.codec,
                );
            } else {
                inner.state.response.set(Some(fut));
            }
        }

        loop {
            match inner.st {
                IoDispatcherState::Processing => {
                    match ready!(inner.poll_service(cx)) {
                        PollService::Ready => {
                            // decode incoming bytes stream
                            match inner.io.poll_recv_decode(&inner.codec, cx) {
                                Ok(decoded) => {
                                    inner.update_timer(&decoded);
                                    if let Some(el) = decoded.item {
                                        inner.call_service(cx, el);
                                    } else {
                                        return Poll::Pending;
                                    }
                                }
                                Err(RecvError::KeepAlive) => {
                                    if let Err(err) = inner.handle_timeout() {
                                        inner.stop(inner.control.call(Control::proto(err)));
                                    }
                                }
                                Err(RecvError::WriteBackpressure) => {
                                    inner.st = IoDispatcherState::Backpressure;
                                    spawn(inner.control.call(Control::wr(true)));
                                }
                                Err(RecvError::Decoder(err)) => {
                                    inner.stop(
                                        inner
                                            .control
                                            .call(Control::proto(ProtocolError::Decode(err))),
                                    );
                                }
                                Err(RecvError::PeerGone(err)) => {
                                    inner.stop(inner.control.call(Control::peer_gone(err)));
                                }
                            }
                        }
                        PollService::Continue => (),
                    }
                }
                // handle write back-pressure
                IoDispatcherState::Backpressure => {
                    match ready!(inner.poll_service(cx)) {
                        PollService::Ready => (),
                        PollService::Continue => continue,
                    }

                    if let Err(err) = ready!(inner.io.poll_flush(cx, false)) {
                        inner.stop(inner.control.call(Control::peer_gone(Some(err))));
                    } else {
                        inner.st = IoDispatcherState::Processing;
                        spawn(inner.control.call(Control::wr(false)));
                    }
                }

                // drain service responses and shutdown io
                IoDispatcherState::Stop(ref mut stop) => {
                    // service may relay on poll_ready for response results
                    if !inner.flags.contains(Flags::READY_ERR)
                        && let Poll::Ready(res) = inner.service.poll_ready(cx)
                        && res.is_err()
                    {
                        inner.flags.insert(Flags::READY_ERR);
                    }

                    let mut fut = stop.take().unwrap();
                    match Pin::new(&mut fut).poll(cx) {
                        Poll::Ready(Ok(item)) => {
                            if let Some(item) = item {
                                let _ = inner.io.encode(item, &inner.codec);
                            }
                            inner.st = IoDispatcherState::Shutdown(Some(Ok(())));
                        }
                        Poll::Ready(Err(err)) => {
                            inner.st = IoDispatcherState::Shutdown(Some(Err(err)));
                        }
                        Poll::Pending => {
                            *stop = Some(fut);
                            return Poll::Pending;
                        }
                    }
                }
                // shutdown service
                IoDispatcherState::Shutdown(ref mut res) => {
                    if inner.service.poll_shutdown(cx).is_ready() {
                        log::trace!("{}: Service shutdown is completed, stop", inner.io.tag());
                        inner.stopping.notify();
                        inner.st = IoDispatcherState::ShutdownIo(res.take());
                    } else {
                        return Poll::Pending;
                    }
                }

                IoDispatcherState::ShutdownIo(ref mut res) => {
                    return if inner.flags.contains(Flags::IO_ERR) {
                        Poll::Ready(res.take().unwrap_or(Ok(())))
                    } else if inner.io.poll_shutdown(cx).is_ready() {
                        log::trace!("{}: io shutdown completed", inner.io.tag());
                        Poll::Ready(res.take().unwrap_or(Ok(())))
                    } else {
                        Poll::Pending
                    };
                }
            }
        }
    }
}

impl<P, C, U, E> DispatcherInner<P, C, U, E>
where
    P: Service<Request<U>, Response = Option<Response<U>>, Error = DispatcherError<E>>
        + 'static,
    C: Service<Control<E>, Response = Option<Response<U>>> + 'static,
    U: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
    <U as Encoder>::Item: 'static,
    E: 'static,
{
    fn stop(&mut self, fut: PipelineCall<C, Control<E>>) {
        self.io.stop_timer();
        self.st = IoDispatcherState::Stop(Some(fut));
    }

    fn call_service(&mut self, cx: &mut Context<'_>, item: Request<U>) {
        let mut fut = self.service.call_nowait(item);
        let mut queue = self.state.queue.borrow_mut();

        // optimize first call
        if let Some(resp) = self.state.response.take() {
            // first call is running
            self.state.response.set(Some(resp));

            let response_idx = self.state.base.get().wrapping_add(queue.len());
            queue.push_back(ServiceResult::Pending);

            let st = self.io.get_ref();
            let codec = self.codec.clone();
            let state = self.state.clone();
            let stopping = self.stopping.wait();

            spawn(async move {
                let empty_q = match select(fut, stopping).await {
                    Either::Left(item) => state.handle_result(item, response_idx, &st, &codec),
                    Either::Right(()) => {
                        state.handle_result(Ok(None), response_idx, &st, &codec)
                    }
                };
                if empty_q {
                    st.wake();
                }
            });
        } else if let Poll::Ready(res) = Pin::new(&mut fut).poll(cx) {
            // check if current result is only response
            if queue.is_empty() {
                match res {
                    Err(err) => {
                        self.state.error.set(Some(IoDispatcherError::Service(err)));
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
                self.state.response_idx.set(self.state.base.get().wrapping_add(queue.len()));
            }
        } else {
            self.state.response.set(Some(fut));
            self.state.response_idx.set(self.state.base.get().wrapping_add(queue.len()));
            queue.push_back(ServiceResult::Pending);
        }
    }

    fn poll_service(&mut self, cx: &mut Context<'_>) -> Poll<PollService> {
        // check for errors
        if let Some(err) = self.state.error.take() {
            log::trace!("{}: Error occured, stopping dispatcher", self.io.tag());
            let item = match err {
                IoDispatcherError::Encoder(err) => Control::proto(ProtocolError::Encode(err)),
                IoDispatcherError::Service(DispatcherError::Service(err)) => Control::err(err),
                IoDispatcherError::Service(DispatcherError::Protocol(err)) => {
                    Control::proto(err)
                }
            };
            self.stop(self.control.call(item));
            return Poll::Ready(PollService::Continue);
        }

        // check readiness
        match self.service.poll_ready(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(PollService::Ready),
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
                        self.stop(
                            self.control.call(Control::proto(ProtocolError::KeepAliveTimeout)),
                        );
                        Poll::Ready(PollService::Continue)
                    }
                    IoStatusUpdate::PeerGone(err) => {
                        log::trace!(
                            "{}: Peer is gone during pause, stopping dispatcher: {:?}",
                            self.io.tag(),
                            err
                        );
                        self.stop(self.control.call(Control::peer_gone(err)));
                        Poll::Ready(PollService::Continue)
                    }
                    IoStatusUpdate::WriteBackpressure => {
                        self.st = IoDispatcherState::Backpressure;
                        spawn(self.control.call(Control::wr(true)));
                        Poll::Ready(PollService::Continue)
                    }
                }
            }
            // handle service readiness error
            Poll::Ready(Err(DispatcherError::Service(err))) => {
                log::error!("{}: Service readiness check failed, stopping", self.io.tag());
                self.flags.insert(Flags::READY_ERR);
                self.stop(self.control.call(Control::err(err)));
                Poll::Ready(PollService::Continue)
            }
            // handle protocol violations
            Poll::Ready(Err(DispatcherError::Protocol(err))) => {
                self.stop(self.control.call(Control::proto(err)));
                Poll::Ready(PollService::Continue)
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
                log::trace!(
                    "{}: Start keep-alive timer {:?}",
                    self.io.tag(),
                    self.keepalive_timeout
                );
                self.flags.insert(Flags::KA_TIMEOUT);
                self.io.start_timer(self.keepalive_timeout);
            }
        } else if let Some(params) = self.io.cfg().frame_read_rate() {
            // we got new data but not enough to parse single frame
            // start read timer
            self.flags.insert(Flags::READ_TIMEOUT);

            self.read_remains = decoded.remains as u32;
            self.read_remains_prev = 0;
            self.read_max_timeout = params.max_timeout;
            self.io.start_timer(params.timeout);

            log::trace!("{}: Start frame read timer {:?}", self.io.tag(), params.timeout);
        }
    }

    fn handle_timeout(&mut self) -> Result<(), ProtocolError> {
        // check read timer
        if self.flags.contains(Flags::READ_TIMEOUT) {
            if let Some(params) = self.io.cfg().frame_read_rate() {
                let total = self.read_remains - self.read_remains_prev;

                // read rate, start timer for next period
                if total > params.rate {
                    self.read_remains_prev = self.read_remains;
                    self.read_remains = 0;

                    if !params.max_timeout.is_zero() {
                        self.read_max_timeout =
                            Seconds(self.read_max_timeout.0.saturating_sub(params.timeout.0));
                    }

                    if params.max_timeout.is_zero() || !self.read_max_timeout.is_zero() {
                        log::trace!(
                            "{}: Frame read rate {:?}, extend timer",
                            self.io.tag(),
                            total
                        );
                        self.io.start_timer(params.timeout);
                        return Ok(());
                    }
                }
                log::trace!("{}: Max payload timeout has been reached", self.io.tag());
                return Err(ProtocolError::ReadTimeout);
            }
        } else if self.flags.contains(Flags::KA_TIMEOUT) {
            log::trace!("{}: Keep-alive error, stopping dispatcher", self.io.tag());
            return Err(ProtocolError::KeepAliveTimeout);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::items_after_statements)]
mod tests {
    use std::cell::Cell;
    use std::sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering};

    use ntex_bytes::{Bytes, BytesMut};
    use ntex_io::{self as nio, IoConfig, testing::IoTest as Io};
    use ntex_service::{IntoService, ServiceCtx, cfg::SharedCfg, fn_service};
    use ntex_util::channel::condition::Condition;
    use ntex_util::time::{Millis, sleep};
    use rand::Rng;

    use super::*;
    use crate::{control::Reason, error::DecodeError, error::EncodeError};

    #[derive(Debug, Copy, Clone)]
    struct BytesCodec;

    impl Encoder for BytesCodec {
        type Item = Bytes;
        type Error = EncodeError;

        #[inline]
        fn encode(&self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.extend_from_slice(&item[..]);
            Ok(())
        }
    }

    impl Decoder for BytesCodec {
        type Item = Bytes;
        type Error = DecodeError;

        fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.is_empty() {
                Ok(None)
            } else {
                Ok(Some(src.split_to(src.len())))
            }
        }
    }

    impl<P, C, U, E> Dispatcher<P, C, U, E>
    where
        P: Service<Request<U>, Response = Option<Response<U>>, Error = DispatcherError<E>>
            + 'static,
        C: Service<Control<E>, Response = Option<Response<U>>> + 'static,
        U: Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + Clone + 'static,
        E: 'static,
    {
        /// Construct new `Dispatcher` instance
        pub(crate) fn new_debug<F: IntoService<P, Request<U>>>(
            io: nio::Io,
            codec: U,
            service: F,
            control: C,
        ) -> (Self, nio::IoRef) {
            let keepalive_timeout = io.cfg().keepalive_timeout();
            let rio = io.get_ref();

            let state = Rc::new(DispatcherState {
                error: Cell::new(None),
                base: Cell::new(0),
                waker: LocalWaker::default(),
                queue: RefCell::new(VecDeque::new()),
                response: Cell::new(None),
                response_idx: Cell::new(0),
            });

            (
                Dispatcher {
                    inner: DispatcherInner {
                        codec,
                        state,
                        keepalive_timeout,
                        stopping: Condition::new(),
                        service: Pipeline::new(service.into_service()).bind(),
                        control: Pipeline::new(control).bind(),
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

    #[ntex::test]
    async fn test_basic() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                sleep(Millis(50)).await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
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

    #[ntex::test]
    async fn test_drop_connection() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("test");

        #[derive(Clone)]
        struct OnDrop(Rc<Cell<bool>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if Rc::strong_count(&self.0) == 2 {
                    self.0.set(true);
                }
            }
        }
        let ops = Rc::new(Cell::new(false));
        let on_drop = OnDrop(ops.clone());

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                let _on_drop = on_drop.clone();
                if msg == "test" {
                    sleep(Millis(500)).await;
                }
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });
        sleep(Millis(25)).await;
        client.write("pl1");
        client.close().await;
        assert!(client.is_server_dropped());
        // service dropped?
        assert!(ops.get());
    }

    #[ntex::test]
    async fn test_ordering() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("test");

        let condition = Condition::new();
        let waiter = condition.wait();

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                waiter.clone().await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
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

    /// On disconnect, call control service and after call completion
    /// drop in-flight publish handlers
    #[ntex::test]
    async fn test_disconnect_ordering() {
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        enum Info {
            Publish,
            PublishDrop,
            Disconnect,
        }

        struct OnDrop(Rc<RefCell<Vec<Info>>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.borrow_mut().push(Info::PublishDrop);
            }
        }

        let condition = Condition::new();
        let waiter = condition.wait();
        let ops = Rc::new(RefCell::new(Vec::new()));
        let ops2 = ops.clone();
        let ops3 = ops.clone();

        let run_server = async || -> Io {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, SharedCfg::new("DBG")),
                BytesCodec,
                fn_service(async move |msg: Bytes| {
                    if msg == b"1" {
                        sleep(Millis(75)).await;
                    } else {
                        ops2.borrow_mut().push(Info::Publish);
                        let on_drop = OnDrop(ops2.clone());
                        waiter.clone().await;
                        drop(on_drop);
                    }
                    Ok::<_, DispatcherError<()>>(Some(msg))
                }),
                fn_service(async move |msg: Control<()>| {
                    if matches!(msg, Control::Stop(Reason::PeerGone(_))) {
                        sleep(Millis(25)).await;
                        ops3.borrow_mut().push(Info::Disconnect);
                    } else {
                        panic!()
                    }
                    Ok::<_, ()>(None)
                }),
            );
            ntex_util::spawn(async move {
                let _ = disp.await;
            });
            sleep(Millis(50)).await;

            client
        };
        let client = run_server.clone()().await;

        client.write("test");
        sleep(Millis(50)).await;
        client.write("test");
        sleep(Millis(50)).await;
        client.close().await;
        assert!(client.is_server_dropped());
        sleep(Millis(150)).await;

        assert_eq!(
            &[
                Info::Publish,
                Info::Publish,
                Info::Disconnect,
                Info::PublishDrop,
                Info::PublishDrop
            ][..],
            &*ops.borrow()
        );

        // different options
        ops.borrow_mut().clear();
        let client = run_server().await;

        client.write("1");
        sleep(Millis(50)).await;

        client.write("test");
        sleep(Millis(50)).await;
        client.write("test");
        sleep(Millis(50)).await;
        client.close().await;
        assert!(client.is_server_dropped());
        sleep(Millis(150)).await;

        assert_eq!(
            &[
                Info::Publish,
                Info::Publish,
                Info::Disconnect,
                Info::PublishDrop,
                Info::PublishDrop
            ][..],
            &*ops.borrow()
        );
    }

    #[ntex::test]
    async fn test_sink() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
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
        sleep(Millis(150)).await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |_: Bytes| {
                Err::<Option<Bytes>, _>(DispatcherError::Service(()))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
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

    #[ntex::test]
    async fn test_err_in_service_ready() {
        struct Srv(Rc<Cell<usize>>);

        impl Service<Bytes> for Srv {
            type Response = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
                self.0.set(self.0.get() + 1);
                Err(DispatcherError::Service(()))
            }

            async fn call(
                &self,
                _: Bytes,
                _: ServiceCtx<'_, Self>,
            ) -> Result<Option<Bytes>, Self::Error> {
                Ok(None)
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let counter = Rc::new(Cell::new(0));

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            Srv(counter.clone()),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
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

    #[ntex::test]
    async fn test_write_backpressure() {
        let (client, server) = Io::create();
        // do not allow to write to socket
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();
        let data3 = data.clone();

        let config = SharedCfg::new("DBG").add(
            IoConfig::new().set_read_buf(8 * 1024, 1024, 16).set_write_buf(32 * 1024, 1024, 16),
        );

        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server, config),
            BytesCodec,
            fn_service(async move |_: Bytes| {
                data2.lock().unwrap().borrow_mut().push(0);
                let bytes = rand::rng()
                    .sample_iter(&rand::distr::Alphanumeric)
                    .take(65_536)
                    .map(char::from)
                    .collect::<String>();
                Ok::<_, DispatcherError<()>>(Some(Bytes::from(bytes)))
            }),
            fn_service(async move |msg: Control<()>| {
                if let Control::WrBackpressure(st) = msg {
                    if st.enabled() {
                        data3.lock().unwrap().borrow_mut().push(1);
                    } else {
                        data3.lock().unwrap().borrow_mut().push(2);
                    }
                }
                Ok::<_, ()>(None)
            }),
        );

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

    #[ntex::test]
    async fn test_shutdown_dispatcher_waker() {
        let (client, server) = Io::create();
        let server = nio::Io::new(server, SharedCfg::new("DBG"));
        client.remote_buffer_cap(1024);

        let flag = Rc::new(Cell::new(true));
        let flag2 = flag.clone();
        let _server_ref = server.get_ref();

        let (disp, _io) = Dispatcher::new_debug(
            server,
            BytesCodec,
            fn_service(async move |item: Bytes| {
                let first = flag2.get();
                flag2.set(false);
                if !first {
                    sleep(Millis(500)).await;
                }
                Ok(Some(item))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
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
    #[ntex::test]
    async fn test_keepalive() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();
        let data3 = data.clone();

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                data2.lock().unwrap().borrow_mut().push(0);
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg
                    && matches!(err.get_ref(), &ProtocolError::KeepAliveTimeout)
                {
                    data3.lock().unwrap().borrow_mut().push(1);
                }
                Ok::<_, ()>(None)
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
        type Error = EncodeError;

        #[inline]
        fn encode(&self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.extend_from_slice(&item[..]);
            Ok(())
        }
    }

    impl Decoder for BytesLenCodec {
        type Item = Bytes;
        type Error = DecodeError;

        fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.len() >= self.0 {
                Ok(Some(src.split_to(self.0)))
            } else {
                Ok(None)
            }
        }
    }

    /// Do not use keep-alive timer if not configured
    #[ntex::test]
    async fn test_no_keepalive_err_after_frame_timeout() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();
        let data3 = data.clone();

        let config = SharedCfg::new("BDG").add(
            IoConfig::new().set_keepalive_timeout(Seconds(0)).set_frame_read_rate(
                Seconds(1),
                Seconds(2),
                2,
            ),
        );

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, config),
            BytesLenCodec(2),
            fn_service(async move |msg: Bytes| {
                data2.lock().unwrap().borrow_mut().push(0);
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg
                    && matches!(err.get_ref(), &ProtocolError::KeepAliveTimeout)
                {
                    data3.lock().unwrap().borrow_mut().push(1);
                }
                Ok::<_, ()>(None)
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

    #[ntex::test]
    async fn test_read_timeout() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();
        let data3 = data.clone();

        let config = SharedCfg::new("DBG").add(
            IoConfig::new().set_keepalive_timeout(Seconds::ZERO).set_frame_read_rate(
                Seconds(1),
                Seconds(2),
                2,
            ),
        );

        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, config),
            BytesLenCodec(8),
            fn_service(async move |msg: Bytes| {
                data2.lock().unwrap().borrow_mut().push(0);
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg
                    && matches!(err.get_ref(), &ProtocolError::ReadTimeout)
                {
                    data3.lock().unwrap().borrow_mut().push(1);
                }
                Ok::<_, ()>(None)
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

    /// Do not use keep-alive timer if not configured
    #[ntex::test]
    async fn cancel_on_stop() {
        #[derive(Clone)]
        struct OnDrop(Arc<AtomicBool>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let data = Arc::new(AtomicBool::new(false));
        let data2 = OnDrop(data.clone());

        let config = SharedCfg::new("DBG").add(
            IoConfig::new().set_keepalive_timeout(Seconds(0)).set_frame_read_rate(
                Seconds(1),
                Seconds(2),
                2,
            ),
        );

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, config),
            BytesLenCodec(2),
            fn_service(async move |msg: Bytes| {
                let data = data2.clone();
                sleep(Millis(99_9999)).await;
                drop(data);
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("1");
        client.close().await;
        sleep(Millis(250)).await;

        assert!(&data.load(Ordering::Relaxed));
    }
}
