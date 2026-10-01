//! Framed transport dispatcher
mod timer;

use std::task::{Context, Poll, ready};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Decoded, IoBoxed, IoRef, IoStatusUpdate, RecvError};
use ntex_service::{
    cfg::Configuration,
    pipeline::{Pipeline, PipelineCall},
};
use ntex_util::{future::Either, future::select, spawn, task::LocalWaker, time::Seconds};

use self::timer::{Timer, Timers};
use crate::config::MqttServiceConfig;
use crate::control::Control;
use crate::error::{DecodeError, DispatcherError, EncodeError, MqttProtocolError};

/// Io waiter tag, in-flight service calls are cancelled once it is woken.
const STOP_TAG: usize = 0x6d71_7474;

/// Decoder state the dispatcher needs for read timers and response ordering.
pub trait FrameState: Decoder {
    /// Returns `true` while the last decoded item is followed by more parts
    /// of the same packet, such as the payload chunks of a streamed publish.
    ///
    /// The parts of a packet are read as one frame, keep-alive and frame
    /// read rate timers continue across them.
    fn is_partial(&self) -> bool {
        false
    }

    /// Returns `false` if the service never responds to the item, such as
    /// an at most once publish.
    ///
    /// These calls do not keep a slot in the response queue, a response
    /// returned anyway is written once the call completes.
    fn has_response(&self, _: &<Self as Decoder>::Item) -> bool {
        true
    }
}

impl<T: FrameState> FrameState for Rc<T> {
    #[inline]
    fn is_partial(&self) -> bool {
        (**self).is_partial()
    }

    #[inline]
    fn has_response(&self, item: &T::Item) -> bool {
        (**self).has_response(item)
    }
}

type Request<U> = <U as Decoder>::Item;
type Response<U> = <U as Encoder>::Item;
type ServiceResult<Codec, E> = Result<Option<Response<Codec>>, DispatcherError<E>>;

type ServiceCall<Codec, E> =
    PipelineCall<Request<Codec>, Option<Response<Codec>>, DispatcherError<E>>;
type ServicePipeline<Codec, E> =
    Pipeline<Request<Codec>, Option<Response<Codec>>, DispatcherError<E>>;

type ControlCall<Codec, E, Err> = PipelineCall<Control<E>, Option<Response<Codec>>, Err>;
type ControlPipeline<Codec, E, Err> = Pipeline<Control<E>, Option<Response<Codec>>, Err>;

pin_project_lite::pin_project! {
    /// Dispatcher for mqtt protocol
    pub(crate) struct Dispatcher<U, E, Err>
    where
        U: Encoder,
        U: Decoder,
        U: 'static,
        E: 'static,
        Err: 'static,
    {
        inner: DispatcherInner<U, E, Err>
    }
}

struct DispatcherInner<Codec, E, Err>
where
    Codec: Encoder + Decoder + 'static,
    E: 'static,
    Err: 'static,
{
    io: IoBoxed,
    codec: Codec,
    service: ServicePipeline<Codec, E>,
    control: ControlPipeline<Codec, E, Err>,
    st: IoDispatcherState<Codec, E, Err>,
    state: Rc<DispatcherState<Codec, E>>,
    timers: Timers,
    keepalive_timeout: Seconds,
    /// Service readiness failed, it is not polled during stop
    ready_err: bool,
}

struct DispatcherState<Codec, E>
where
    Codec: Encoder + Decoder + 'static,
    E: 'static,
{
    /// Stop message for the first error
    error: Cell<Option<Control<E>>>,
    /// Index of the queue head
    base: Cell<usize>,
    /// Service results in request order, `None` is a pending call
    queue: RefCell<VecDeque<Option<ServiceResult<Codec, E>>>>,
    waker: LocalWaker,
    /// Pending call polled by the dispatcher, other pending calls are spawned
    response: Cell<Option<ServiceCall<Codec, E>>>,
    /// Queue index of the polled call, `None` for a call without response
    response_idx: Cell<Option<usize>>,
    max_queue: usize,
}

#[derive(Debug)]
enum IoDispatcherState<Codec: Encoder + Decoder, E: 'static, Err: 'static> {
    Processing,
    Backpressure,
    Stop(ControlCall<Codec, E, Err>),
    Shutdown(Option<Result<(), Err>>),
    ShutdownIo(Option<Result<(), Err>>),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PollService {
    Continue,
    Ready,
}

impl<Codec, E, Err> Dispatcher<Codec, E, Err>
where
    Codec:
        Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + FrameState + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
    E: 'static,
{
    /// Construct new `Dispatcher` instance.
    pub(crate) fn new(
        io: IoBoxed,
        codec: Codec,
        service: ServicePipeline<Codec, E>,
        control: ControlPipeline<Codec, E, Err>,
    ) -> Self {
        let state = Rc::new(DispatcherState {
            error: Cell::new(None),
            base: Cell::new(0),
            queue: RefCell::new(VecDeque::new()),
            waker: LocalWaker::default(),
            response: Cell::new(None),
            response_idx: Cell::new(None),
            max_queue: io.cfg().ctx().get::<MqttServiceConfig>().max_queue,
        });
        let keepalive_timeout = io.cfg().keepalive_timeout();

        Dispatcher {
            inner: DispatcherInner {
                timers: Timers::new(&io),
                io,
                codec,
                state,
                control,
                service,
                keepalive_timeout,
                ready_err: false,
                st: IoDispatcherState::Processing,
            },
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is taken from the io configuration.
    pub(crate) fn keepalive_timeout(mut self, timeout: Seconds) -> Self {
        self.inner.keepalive_timeout = timeout;
        self
    }
}

impl<Codec, E> DispatcherState<Codec, E>
where
    Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError>,
    <Codec as Encoder>::Item: 'static,
{
    fn is_full(&self, len: usize) -> bool {
        self.max_queue != 0 && len >= self.max_queue
    }

    fn set_error(&self, err: DispatcherError<E>) {
        self.error.set(Some(match err {
            DispatcherError::Service(err) => Control::err(err),
            DispatcherError::Protocol(err) => Control::proto(err),
        }));
    }

    /// Encodes the response of a completed call, returns `true` on error.
    fn write_result(&self, item: ServiceResult<Codec, E>, io: &IoRef, codec: &Codec) -> bool {
        match item {
            Ok(Some(item)) => {
                if let Err(err) = io.encode(item, codec) {
                    self.error
                        .set(Some(Control::proto(MqttProtocolError::Encode(err))));
                    return true;
                }
                false
            }
            Ok(None) => false,
            Err(err) => {
                self.set_error(err);
                true
            }
        }
    }

    /// Handles the result of a call, returns `true` if the dispatcher must be
    /// woken up.
    fn complete(
        &self,
        item: ServiceResult<Codec, E>,
        response_idx: Option<usize>,
        io: &IoRef,
        codec: &Codec,
    ) -> bool {
        if let Some(idx) = response_idx {
            self.handle_result(item, idx, io, codec)
        } else {
            self.write_result(item, io, codec)
        }
    }

    /// Handles the result of a queued call, returns `true` if the dispatcher
    /// must be woken up.
    fn handle_result(
        &self,
        item: ServiceResult<Codec, E>,
        response_idx: usize,
        io: &IoRef,
        codec: &Codec,
    ) -> bool {
        let mut queue = self.queue.borrow_mut();
        let idx = response_idx.wrapping_sub(self.base.get());

        if idx == 0 {
            // write the head response and the completed responses after it
            let was_full = self.is_full(queue.len());
            let mut err = false;
            let mut item = Some(item);
            while let Some(res) = item {
                let _ = queue.pop_front();
                self.base.set(self.base.get().wrapping_add(1));
                err |= self.write_result(res, io, codec);
                item = queue.front_mut().and_then(Option::take);
            }
            // the dispatcher waits for results only on errors and a full queue
            err || (was_full && !self.is_full(queue.len()))
        } else if let Err(err) = item {
            self.set_error(err);
            true
        } else {
            queue[idx] = Some(item);
            false
        }
    }
}

impl<Codec, E, Err> Future for Dispatcher<Codec, E, Err>
where
    Codec:
        Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + FrameState + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
    E: 'static,
    Err: 'static,
{
    type Output = Result<(), Err>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.as_mut().project().inner;
        inner.state.waker.register(cx.waker());

        // check control service readiness
        ready!(inner.control.poll_ready(cx))?;

        // handle service response future
        if let Some(mut fut) = inner.state.response.take() {
            if let Poll::Ready(item) = Pin::new(&mut fut).poll(cx) {
                inner.state.complete(
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
                    if ready!(inner.poll_service(cx)) == PollService::Continue {
                        continue;
                    }
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
                        Err(RecvError::Timeout) => {
                            if let Err(err) = inner.handle_timeout() {
                                inner.stop(Control::proto(err));
                            }
                        }
                        Err(RecvError::WriteBackpressure) => inner.enter_backpressure(),
                        Err(RecvError::Decoder(err)) => {
                            inner.stop(Control::proto(MqttProtocolError::Decode(err)));
                        }
                        Err(RecvError::PeerGone(err)) => inner.stop(Control::peer_gone(err)),
                    }
                }
                // handle write back-pressure
                IoDispatcherState::Backpressure => {
                    // check write timeout
                    if let Poll::Ready(IoStatusUpdate::Timeout) = inner.io.poll_status_update(cx)
                        && let Err(err) = inner.handle_timeout()
                    {
                        inner.stop(Control::proto(err));
                    } else if let Err(err) = ready!(inner.io.poll_flush(cx, false)) {
                        inner.stop(Control::peer_gone(Some(err)));
                    } else if ready!(inner.poll_service(cx)) == PollService::Ready {
                        inner.stop_timer();
                        inner.st = IoDispatcherState::Processing;
                        spawn(inner.control.call_static(Control::wr(false)));
                    }
                }
                // wait for the control service to handle the stop message
                IoDispatcherState::Stop(ref mut fut) => {
                    // service may rely on poll_ready for response results
                    if !inner.ready_err
                        && let Poll::Ready(Err(_)) = inner.service.poll_ready(cx)
                    {
                        inner.ready_err = true;
                    }

                    let res = ready!(Pin::new(fut).poll(cx)).map(|item| {
                        if let Some(item) = item {
                            let _ = inner.io.encode(item, &inner.codec);
                        }
                    });
                    inner.st = IoDispatcherState::Shutdown(Some(res));
                }
                // shutdown service
                IoDispatcherState::Shutdown(ref mut res) => {
                    ready!(inner.service.poll_shutdown(cx));
                    log::trace!("{}: Service shutdown is completed, stop", inner.io.tag());
                    inner.io.wake(STOP_TAG);
                    inner.st = IoDispatcherState::ShutdownIo(res.take());
                }
                IoDispatcherState::ShutdownIo(ref mut res) => {
                    let _ = ready!(inner.io.poll_shutdown(cx));
                    log::trace!("{}: io shutdown completed", inner.io.tag());
                    return Poll::Ready(res.take().unwrap_or(Ok(())));
                }
            }
        }
    }
}

impl<Codec, E, Err> Drop for DispatcherInner<Codec, E, Err>
where
    Codec: Encoder + Decoder + 'static,
    E: 'static,
    Err: 'static,
{
    fn drop(&mut self) {
        // io waiters are woken on disconnect only after the transport is torn
        // down, cancel in-flight calls unless the shutdown already did
        if !matches!(self.st, IoDispatcherState::ShutdownIo(_)) {
            self.io.wake(STOP_TAG);
        }
    }
}

impl<Codec, E, Err> DispatcherInner<Codec, E, Err>
where
    Codec:
        Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + FrameState + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
    E: 'static,
    Err: 'static,
{
    /// Stops timers and sends the stop message to the control service.
    fn stop(&mut self, msg: Control<E>) {
        self.timers.active = Timer::Stopped;
        self.io.stop_timer();
        self.st = IoDispatcherState::Stop(self.control.call_static(msg));
    }

    fn enter_backpressure(&mut self) {
        if !matches!(self.st, IoDispatcherState::Backpressure) {
            self.start_write_timer();
            self.st = IoDispatcherState::Backpressure;
            spawn(self.control.call_static(Control::wr(true)));
        }
    }

    fn call_service(&mut self, cx: &mut Context<'_>, item: Request<Codec>) {
        let ordered = self.codec.has_response(&item);
        let mut fut = self.service.call_nowait(item);
        let mut queue = self.state.queue.borrow_mut();

        // calls without response do not keep a slot in the queue
        let mut push_pending = || {
            ordered.then(|| {
                queue.push_back(None);
                self.state.base.get().wrapping_add(queue.len() - 1)
            })
        };

        // only one pending call is polled by the dispatcher, spawn the rest
        if let Some(resp) = self.state.response.take() {
            self.state.response.set(Some(resp));
            let response_idx = push_pending();

            let st = self.io.get_ref();
            let codec = self.codec.clone();
            let state = self.state.clone();
            // the dispatcher is not stopped yet, register the waiter now
            // so a wake before the first poll of the task is not missed
            let stopping = st.waiter(STOP_TAG).into_static();
            let _ = stopping.poll_ready(cx);

            spawn(async move {
                let item = match select(fut, stopping).await {
                    Either::Left(item) => item,
                    Either::Right(()) => Ok(None),
                };
                if state.complete(item, response_idx, &st, &codec) {
                    st.notify_dispatcher();
                }
            });
        } else if let Poll::Ready(res) = Pin::new(&mut fut).poll(cx) {
            // only responses wait for the calls before them
            if queue.is_empty() || !matches!(res, Ok(Some(_))) {
                self.state.write_result(res, self.io.as_ref(), &self.codec);
            } else {
                queue.push_back(Some(res));
            }
        } else {
            let response_idx = push_pending();
            self.state.response_idx.set(response_idx);
            self.state.response.set(Some(fut));
        }
    }

    fn poll_service(&mut self, cx: &mut Context<'_>) -> Poll<PollService> {
        // check for errors
        if let Some(msg) = self.state.error.take() {
            log::trace!("{}: Error occurred, stopping dispatcher", self.io.tag());
            self.stop(msg);
            return Poll::Ready(PollService::Continue);
        }

        // check readiness, pause reading while the response queue is full
        let ready = if self.state.is_full(self.state.queue.borrow().len()) {
            Poll::Pending
        } else {
            self.service.poll_ready(cx)
        };
        let msg = match ready {
            Poll::Ready(Ok(())) => return Poll::Ready(PollService::Ready),
            Poll::Pending => match ready!(self.poll_read_pause(cx)) {
                Some(msg) => msg,
                None => return Poll::Ready(PollService::Continue),
            },
            Poll::Ready(Err(DispatcherError::Service(err))) => {
                log::error!(
                    "{}: Service readiness check failed, stopping",
                    self.io.tag()
                );
                self.ready_err = true;
                Control::err(err)
            }
            Poll::Ready(Err(DispatcherError::Protocol(err))) => Control::proto(err),
        };
        self.stop(msg);
        Poll::Ready(PollService::Continue)
    }

    /// Pauses reading while the service is not ready, returns the stop
    /// message if the dispatcher must stop.
    fn poll_read_pause(&mut self, cx: &mut Context<'_>) -> Poll<Option<Control<E>>> {
        log::trace!(
            "{}: Service is not ready, pause read task {:?}",
            self.io.tag(),
            self.io.flags()
        );

        // the write timeout keeps running while the service is paused
        if self.timers.active != Timer::Write {
            self.stop_timer();
        }
        self.timers.reset_read(self.io.cfg());

        let status = match self.io.poll_read_pause(cx) {
            Poll::Ready(status) => status,
            // clean read eof does not close the connection, but a peer that
            // stopped sending cannot make progress while service is not ready
            Poll::Pending if self.io.is_read_eof() && self.io.with_read_dst(|b| b.is_empty()) => {
                IoStatusUpdate::PeerGone(None)
            }
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(match status {
            IoStatusUpdate::Timeout if self.timers.active == Timer::Write => {
                self.handle_timeout().err().map(Control::proto)
            }
            IoStatusUpdate::Timeout => {
                log::trace!(
                    "{}: Keep-alive error, stopping dispatcher during pause",
                    self.io.tag()
                );
                Some(Control::proto(MqttProtocolError::KeepAliveTimeout))
            }
            IoStatusUpdate::PeerGone(err) => {
                log::trace!(
                    "{}: Peer is gone during pause, stopping dispatcher: {:?}",
                    self.io.tag(),
                    err
                );
                Some(Control::peer_gone(err))
            }
            IoStatusUpdate::WriteBackpressure => {
                self.enter_backpressure();
                None
            }
        })
    }

    fn update_timer(&mut self, decoded: &Decoded<<Codec as Decoder>::Item>) {
        // parts of a streamed publish are read as one frame
        let item = decoded.item.is_some() && !self.codec.is_partial();
        self.timers.update_read(
            self.io.cfg(),
            item,
            decoded.remains as u32,
            decoded.consumed as u32,
        );

        // keep-alive and frame read timers do not apply while a complete frame is handled
        let timer = self
            .timers
            .select(self.io.cfg(), !self.keepalive_timeout.is_zero(), item);
        self.set_timer(timer);
    }

    /// Starts the write timeout when write backpressure is enabled.
    ///
    /// Frames are not decoded during backpressure, so read-side timers are
    /// stopped when no write timeout is configured.
    fn start_write_timer(&mut self) {
        let timeout = self.io.cfg().write_timeout();
        if timeout.is_zero() {
            self.stop_timer();
        } else if self.timers.active != Timer::Write {
            log::trace!("{}: Start write timer {:?}", self.io.tag(), timeout);
            self.timers.active = Timer::Write;
            self.io.start_timer(timeout);
        }
    }

    /// Stops the dispatcher timer, if it is armed.
    fn stop_timer(&mut self) {
        if self.timers.active != Timer::Stopped {
            self.timers.active = Timer::Stopped;
            self.io.stop_timer();
        }
    }

    /// Arms the dispatcher timer for a read-side purpose, an armed timer
    /// with the same purpose keeps running.
    fn set_timer(&mut self, timer: Timer) {
        if self.timers.active == timer {
            return;
        }
        self.timers.active = match timer {
            Timer::KeepAlive => {
                log::trace!(
                    "{}: Start keep-alive timer {:?}",
                    self.io.tag(),
                    self.keepalive_timeout
                );
                self.io.start_timer(self.keepalive_timeout);
                Timer::KeepAlive
            }
            Timer::FrameRead if let Some(params) = self.io.cfg().frame_read_rate() => {
                log::trace!(
                    "{}: Start frame read timer {:?}",
                    self.io.tag(),
                    params.timeout
                );
                self.io.start_timer(params.timeout);
                Timer::FrameRead
            }
            _ => {
                self.io.stop_timer();
                Timer::Stopped
            }
        };
    }

    fn handle_timeout(&mut self) -> Result<(), MqttProtocolError> {
        match self.timers.active {
            Timer::FrameRead => {
                let (Some(params), Some(p)) =
                    (self.io.cfg().frame_read_rate(), self.timers.read.progress())
                else {
                    self.timers.active = Timer::Stopped;
                    return Ok(());
                };

                // read rate, start timer for next period
                if p.consumed > params.rate {
                    let total = p.consumed;
                    p.consumed = 0;

                    if !params.max_timeout.is_zero() {
                        p.max_timeout = Seconds(p.max_timeout.0.saturating_sub(params.timeout.0));
                    }

                    if params.max_timeout.is_zero() || !p.max_timeout.is_zero() {
                        log::trace!(
                            "{}: Frame read rate {:?}, extend timer",
                            self.io.tag(),
                            total
                        );
                        self.io.start_timer(params.timeout);
                        return Ok(());
                    }
                    log::trace!("{}: Max payload timeout has been reached", self.io.tag());
                }
                Err(MqttProtocolError::ReadTimeout)
            }
            // backpressure can be released unnoticed while the service is paused
            Timer::Write if !self.io.is_wr_backpressure() => {
                self.timers.active = Timer::Stopped;
                Ok(())
            }
            Timer::Write => {
                log::trace!("{}: Write backpressure timeout", self.io.tag());
                Err(MqttProtocolError::WriteTimeout)
            }
            Timer::KeepAlive => {
                log::trace!("{}: Keep-alive error, stopping dispatcher", self.io.tag());
                Err(MqttProtocolError::KeepAliveTimeout)
            }
            Timer::Stopped => Ok(()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::items_after_statements)]
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use ntex_bytes::{BytePages, Bytes, BytesMut};
    use ntex_io::{self as nio, IoConfig, testing::IoTest as Io};
    use ntex_service::{Ctx, IntoService, Service, cfg::SharedCfg, fn_service};
    use ntex_util::channel::{condition::Condition, oneshot};
    use ntex_util::time::{Millis, sleep};
    use rand::RngExt;

    use super::*;
    use crate::{control::Reason, error::DecodeError, error::EncodeError};

    #[derive(Debug, Copy, Clone)]
    struct BytesCodec;

    impl Encoder for BytesCodec {
        type Item = Bytes;
        type Error = EncodeError;

        #[inline]
        fn encode(&self, item: Bytes, dst: &mut BytePages) -> Result<(), Self::Error> {
            dst.append(item);
            Ok(())
        }
    }

    impl FrameState for BytesCodec {}

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

    impl<U, E, Err> Dispatcher<U, E, Err>
    where
        U: Decoder<Error = DecodeError>
            + Encoder<Error = EncodeError>
            + FrameState
            + Clone
            + 'static,
        E: 'static,
        Err: 'static,
    {
        /// Construct new `Dispatcher` instance
        pub(crate) fn new_debug<P, C, F: IntoService<P, (), Request<U>>>(
            io: nio::Io,
            codec: U,
            service: F,
            control: C,
        ) -> (Self, nio::IoRef)
        where
            P: Service<(), Request<U>, Res = Option<Response<U>>, Error = DispatcherError<E>>
                + 'static,
            C: Service<(), Control<E>, Res = Option<Response<U>>, Error = Err> + 'static,
        {
            let rio = io.get_ref();
            let disp = Dispatcher::new(
                IoBoxed::from(io),
                codec,
                Pipeline::new((), service.into_service()),
                Pipeline::new((), control),
            );
            (disp, rio)
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
        condition.notify(());

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

        let condition = Condition::default();
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
            fn_service(async move |_: Bytes| Err::<Option<Bytes>, _>(DispatcherError::Service(()))),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &BytesCodec)
            .unwrap();

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

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                self.0.set(self.0.get() + 1);
                Err(DispatcherError::Service(()))
            }

            async fn call(
                &self,
                _: Bytes,
                _: Ctx<'_, Self, ()>,
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
        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &BytesCodec)
            .unwrap();
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
            IoConfig::new()
                .set_read_buf(8 * 1024, 1024)
                .set_write_buf(32 * 1024),
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
        assert_eq!(io.with_write_src(|buf| buf.len()).unwrap(), 65536);

        client.remote_buffer_cap(10240);
        sleep(Millis(50)).await;
        assert_eq!(io.with_write_src(|buf| buf.len()).unwrap(), 55296);

        client.remote_buffer_cap(45056);
        sleep(Millis(50)).await;
        assert_eq!(io.with_write_src(|buf| buf.len()).unwrap(), 10240);

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
                    && matches!(err.get_ref(), &MqttProtocolError::KeepAliveTimeout)
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
        fn encode(&self, item: Bytes, dst: &mut BytePages) -> Result<(), Self::Error> {
            dst.append(item);
            Ok(())
        }
    }

    impl FrameState for BytesLenCodec {}

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

    fn timeout_dispatcher(cfg: IoConfig) -> (Io, nio::IoRef, Rc<RefCell<Vec<MqttProtocolError>>>) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(cfg)),
            BytesLenCodec(64),
            fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    errs2.borrow_mut().push(*err.get_ref());
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });
        (client, state, errs)
    }

    /// Decodes one byte per item, `p` is a part of a streamed packet
    #[derive(Clone, Default)]
    struct ChunkCodec(Rc<Cell<bool>>);

    impl Encoder for ChunkCodec {
        type Item = Bytes;
        type Error = EncodeError;

        fn encode(&self, item: Bytes, dst: &mut BytePages) -> Result<(), Self::Error> {
            dst.append(item);
            Ok(())
        }
    }

    impl FrameState for ChunkCodec {
        fn is_partial(&self) -> bool {
            self.0.get()
        }
    }

    impl Decoder for ChunkCodec {
        type Item = Bytes;
        type Error = DecodeError;

        fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.is_empty() {
                Ok(None)
            } else {
                self.0.set(src[0] == b'p');
                Ok(Some(src.split_to(1)))
            }
        }
    }

    fn chunk_dispatcher(cfg: IoConfig) -> (Io, nio::IoRef, Rc<RefCell<Vec<MqttProtocolError>>>) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(cfg)),
            ChunkCodec::default(),
            fn_service(async move |_: Bytes| Ok::<_, DispatcherError<()>>(None)),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    errs2.borrow_mut().push(*err.get_ref());
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });
        (client, state, errs)
    }

    /// Keep-alive bounds the whole streamed packet, not each part
    #[ntex::test]
    async fn test_keepalive_partial_items() {
        let (client, state, errs) =
            chunk_dispatcher(IoConfig::new().set_keepalive_timeout(Seconds(1)));

        for _ in 0..3 {
            client.write("p");
            sleep(Millis(300)).await;
        }
        assert!(state.is_active());
        for _ in 0..4 {
            client.write("p");
            sleep(Millis(300)).await;
        }
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::KeepAliveTimeout]
        ));
    }

    /// Keep-alive restarts after the last part of a streamed packet
    #[ntex::test]
    async fn test_keepalive_after_partial_items() {
        let (client, state, errs) =
            chunk_dispatcher(IoConfig::new().set_keepalive_timeout(Seconds(1)));

        for _ in 0..2 {
            client.write("p");
            sleep(Millis(300)).await;
        }
        client.write("e");
        for _ in 0..5 {
            sleep(Millis(300)).await;
            client.write("c");
        }
        assert!(state.is_active());
        assert!(errs.borrow().is_empty());
    }

    /// Frame read rate max timeout bounds the whole streamed packet
    #[ntex::test]
    async fn test_read_rate_partial_items() {
        let (client, state, errs) = chunk_dispatcher(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
        );

        for _ in 0..4 {
            client.write("ppp");
            sleep(Millis(400)).await;
        }
        assert!(state.is_active());
        for _ in 0..4 {
            client.write("ppp");
            sleep(Millis(400)).await;
        }
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::ReadTimeout]
        ));
    }

    /// Streamed packet parts below the frame read rate time out
    #[ntex::test]
    async fn test_read_rate_slow_partial_items() {
        let (client, state, errs) = chunk_dispatcher(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds::ZERO, 2),
        );

        client.write("p");
        sleep(Millis(1500)).await;
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::ReadTimeout]
        ));
    }

    /// Frame read rate is satisfied, but the cumulative max timeout is reached
    #[ntex::test]
    async fn test_read_rate_max_timeout() {
        let (client, state, errs) = timeout_dispatcher(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
        );

        for _ in 0..4 {
            client.write("123");
            sleep(Millis(400)).await;
        }
        assert!(state.is_active());
        for _ in 0..4 {
            client.write("123");
            sleep(Millis(400)).await;
        }
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::ReadTimeout]
        ));
    }

    /// Bytes the codec consumes without producing a frame count towards the read rate
    #[ntex::test]
    async fn test_read_rate_codec_consumed() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        #[derive(Clone)]
        struct ConsumingCodec;

        impl Encoder for ConsumingCodec {
            type Item = Bytes;
            type Error = EncodeError;

            fn encode(&self, item: Bytes, dst: &mut BytePages) -> Result<(), Self::Error> {
                dst.append(item);
                Ok(())
            }
        }

        impl FrameState for ConsumingCodec {}

        impl Decoder for ConsumingCodec {
            type Item = Bytes;
            type Error = DecodeError;

            fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
                // consume input, frames are never complete
                src.clear();
                Ok(None)
            }
        }

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let cfg = IoConfig::new()
            .set_keepalive_timeout(Seconds::ZERO)
            .set_frame_read_rate(Seconds(1), Seconds::ZERO, 2);
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(cfg)),
            ConsumingCodec,
            fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    errs2.borrow_mut().push(*err.get_ref());
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        for _ in 0..6 {
            client.write("123");
            sleep(Millis(400)).await;
        }
        assert!(state.is_active());

        // data stops, read rate is not satisfied
        sleep(Millis(2500)).await;
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::ReadTimeout]
        ));
    }

    /// Without frame read rate, keep-alive bounds a slowly received frame
    #[ntex::test]
    async fn test_keepalive_partial_frame() {
        let (client, state, errs) =
            timeout_dispatcher(IoConfig::new().set_keepalive_timeout(Seconds(1)));

        for _ in 0..3 {
            client.write("1");
            sleep(Millis(300)).await;
        }
        assert!(state.is_active());
        for _ in 0..4 {
            client.write("1");
            sleep(Millis(300)).await;
        }
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::KeepAliveTimeout]
        ));
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
            IoConfig::new()
                .set_keepalive_timeout(Seconds(0))
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
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
                    && matches!(err.get_ref(), &MqttProtocolError::KeepAliveTimeout)
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
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
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
                    && matches!(err.get_ref(), &MqttProtocolError::ReadTimeout)
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
        sleep(Millis(500)).await;
        assert!(state.is_active());
        client.write("23");
        sleep(Millis(1000)).await;
        assert!(state.is_active());
        client.write("4");
        sleep(Millis(2000)).await;

        // write side must be closed, dispatcher should fail with keep-alive
        assert!(!state.is_active());
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
            IoConfig::new()
                .set_keepalive_timeout(Seconds(0))
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
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

    #[derive(Clone)]
    struct ByteCodec;

    impl Encoder for ByteCodec {
        type Item = Bytes;
        type Error = EncodeError;

        fn encode(&self, item: Bytes, dst: &mut BytePages) -> Result<(), Self::Error> {
            if item == "X" {
                return Err(EncodeError::MalformedPacket);
            }
            dst.append(item);
            Ok(())
        }
    }

    impl FrameState for ByteCodec {
        fn has_response(&self, item: &Bytes) -> bool {
            item != "q"
        }
    }

    impl Decoder for ByteCodec {
        type Item = Bytes;
        type Error = DecodeError;

        fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            match src.first() {
                None => Ok(None),
                Some(b'E') => Err(DecodeError::MalformedPacket),
                Some(_) => Ok(Some(src.split_to(1))),
            }
        }
    }

    struct OnDropFlag(Rc<Cell<bool>>);
    impl Drop for OnDropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    /// Service that responds to `w` immediately and never completes other calls
    fn pending_service(
        dropped: Rc<Cell<bool>>,
    ) -> impl Service<(), Bytes, Res = Option<Bytes>, Error = DispatcherError<()>> {
        fn_service(move |msg: Bytes| {
            let dropped = dropped.clone();
            async move {
                if msg == Bytes::from_static(b"w") {
                    return Ok(Some(Bytes::from_static(b"response")));
                }
                // the first call is polled by the dispatcher, others are spawned
                let _guard = (msg == Bytes::from_static(b"2")).then(|| OnDropFlag(dropped));
                std::future::pending::<()>().await;
                Ok::<_, DispatcherError<()>>(None)
            }
        })
    }

    /// A spawned call that fails to encode its response stops the dispatcher
    /// while later calls are still pending
    #[ntex::test]
    async fn encode_error_in_spawned_call() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
            ByteCodec,
            fn_service(async move |msg: Bytes| {
                let delay = match &msg[..] {
                    b"a" => 50,
                    b"X" => 100,
                    _ => 10_000,
                };
                sleep(Millis(delay)).await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    errs2.borrow_mut().push(*err.get_ref());
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("aXc");
        sleep(Millis(300)).await;
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::Encode(EncodeError::MalformedPacket)]
        ));
        assert_eq!(client.read().await.unwrap(), Bytes::from_static(b"a"));
    }

    /// Calls spawned in the same poll as the stop are cancelled on service shutdown
    #[ntex::test]
    async fn cancel_spawned_before_first_poll() {
        let (client, server) = Io::create();
        // pending write data delays io shutdown
        client.remote_buffer_cap(0);

        let dropped = Rc::new(Cell::new(false));
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
            ByteCodec,
            pending_service(dropped.clone()),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );

        // decode, spawn and stop within a single dispatcher poll
        client.write("w12E");
        ntex_util::spawn(async move {
            let _ = disp.await;
        });
        sleep(Millis(50)).await;
        assert!(!client.is_closed());
        assert!(dropped.get());
    }

    /// Spawned calls are cancelled when the dispatcher is dropped
    #[ntex::test]
    async fn cancel_spawned_on_drop() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let dropped = Rc::new(Cell::new(false));
        let (mut disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
            ByteCodec,
            pending_service(dropped.clone()),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );

        client.write("w12");
        sleep(Millis(25)).await;
        let _ = ntex_util::future::lazy(|cx| Pin::new(&mut disp).poll(cx)).await;
        sleep(Millis(25)).await;
        assert!(!dropped.get());

        drop(disp);
        sleep(Millis(25)).await;
        assert!(dropped.get());
    }

    /// Handle peer gone while publish service is not ready
    #[ntex::test]
    async fn peer_gone_while_service_is_not_ready() {
        #[derive(Clone)]
        struct OnDrop(Arc<AtomicUsize>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                let cnt = self.0.load(Ordering::Relaxed) + 1;
                self.0.store(cnt, Ordering::Relaxed);
            }
        }

        let data = Arc::new(AtomicUsize::new(0));
        let data2 = OnDrop(data.clone());

        struct Srv(Cell<bool>, OnDrop);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                if self.0.get() {
                    sleep(Millis(999_999)).await;
                }
                Ok(())
            }

            async fn call(
                &self,
                _: Bytes,
                _: Ctx<'_, Self, ()>,
            ) -> Result<Option<Bytes>, Self::Error> {
                let _data = self.1.clone();
                self.0.set(true);
                sleep(Millis(999_999)).await;
                Ok(None)
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            Srv(Cell::new(false), data2),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        let (tx, rx) = ntex::channel::oneshot::channel();
        ntex_util::spawn(async move {
            let _ = disp.await;
            let _ = tx.send(());
        });

        client.write("1");
        client.close().await;
        let _ = rx.await;

        let cnt = data.load(Ordering::Relaxed);
        assert_eq!(cnt, 2);
    }

    fn write_timeout_srv(
        ready: bool,
    ) -> (
        impl Service<(), Bytes, Res = Option<Bytes>, Error = DispatcherError<()>>,
        impl Service<(), Control<()>, Res = Option<Bytes>, Error = ()>,
        Rc<RefCell<Vec<u8>>>,
    ) {
        struct Srv(bool, Cell<bool>);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                if !self.0 && self.1.get() {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }

            async fn call(&self, _: Bytes, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
                self.1.set(true);
                Ok(Some(Bytes::from(vec![b'x'; 65_536])))
            }
        }

        let data = Rc::new(RefCell::new(Vec::new()));
        let data2 = data.clone();
        let control = fn_service(async move |msg: Control<()>| {
            match msg {
                Control::WrBackpressure(st) => {
                    data2.borrow_mut().push(if st.enabled() { 1 } else { 2 });
                }
                Control::Stop(Reason::Protocol(err))
                    if matches!(err.get_ref(), &MqttProtocolError::WriteTimeout) =>
                {
                    data2.borrow_mut().push(3);
                }
                Control::Stop(_) => (),
            }
            Ok::<_, ()>(None)
        });
        (Srv(ready, Cell::new(false)), control, data)
    }

    fn write_timeout_cfg() -> SharedCfg {
        SharedCfg::new("DBG")
            .add(
                IoConfig::new()
                    .set_keepalive_timeout(Seconds::ZERO)
                    .set_write_buf(32 * 1024)
                    .set_write_timeout(Seconds(1)),
            )
            .into()
    }

    /// Peer does not read, dispatcher stops with write timeout
    #[ntex::test]
    async fn test_write_timeout() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let (srv, control, data) = write_timeout_srv(true);
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, write_timeout_cfg()),
            BytesCodec,
            srv,
            control,
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(500)).await;
        assert!(state.is_active());
        assert_eq!(&data.borrow()[..], &[1]);

        sleep(Millis(2000)).await;
        assert!(!state.is_active());
        assert_eq!(&data.borrow()[..], &[1, 3]);
    }

    /// Backpressure is released before write timeout
    #[ntex::test]
    async fn test_write_timeout_released() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let (srv, control, data) = write_timeout_srv(true);
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, write_timeout_cfg()),
            BytesCodec,
            srv,
            control,
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(500)).await;
        assert_eq!(&data.borrow()[..], &[1]);

        client.remote_buffer_cap(1024 * 1024);
        sleep(Millis(2000)).await;
        assert!(state.is_active());
        assert_eq!(&data.borrow()[..], &[1, 2]);
        assert_eq!(client.read_any().len(), 65_536);
    }

    /// Write timeout keeps running while service is not ready
    #[ntex::test]
    async fn test_write_timeout_service_not_ready() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let (srv, control, data) = write_timeout_srv(false);
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, write_timeout_cfg()),
            BytesCodec,
            srv,
            control,
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(500)).await;
        assert!(state.is_active());

        sleep(Millis(2000)).await;
        assert!(!state.is_active());
        assert!(data.borrow().contains(&3));
    }

    /// Reading pauses while the response queue is full
    #[ntex::test]
    async fn test_max_queue() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let (tx, rx) = oneshot::channel::<()>();
        let rx = Cell::new(Some(rx));
        let calls = Rc::new(Cell::new(0));
        let calls2 = calls.clone();

        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(IoConfig::new())
            .add(MqttServiceConfig::new().set_max_queue(4))
            .into();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, cfg),
            BytesLenCodec(1),
            fn_service(async move |msg: Bytes| {
                calls2.set(calls2.get() + 1);
                // first call blocks the head of the queue
                if let Some(rx) = rx.take() {
                    let _ = rx.await;
                }
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("0123456789");
        sleep(Millis(50)).await;
        assert_eq!(calls.get(), 4);
        assert!(client.read_any().is_empty());

        let _ = tx.send(());
        sleep(Millis(50)).await;
        assert_eq!(calls.get(), 10);
        assert_eq!(client.read_any(), Bytes::from_static(b"0123456789"));
    }

    /// Calls without response do not keep a slot in the response queue
    #[ntex::test]
    async fn no_response_calls_skip_queue() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let (tx, rx) = oneshot::channel::<()>();
        let rx = Cell::new(Some(rx));
        let calls = Rc::new(RefCell::new(Vec::new()));
        let calls2 = calls.clone();

        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(IoConfig::new())
            .add(MqttServiceConfig::new().set_max_queue(2))
            .into();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, cfg),
            ByteCodec,
            fn_service(async move |msg: Bytes| {
                calls2.borrow_mut().push(msg.clone());
                if msg == "q" {
                    sleep(Millis(10)).await;
                    return Ok(None);
                }
                // first ordered call blocks the head of the queue
                if let Some(rx) = rx.take() {
                    let _ = rx.await;
                }
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        // the first call is polled by the dispatcher, others are spawned
        client.write("qaqqbc");
        sleep(Millis(50)).await;
        assert_eq!(&calls.borrow()[..], ["q", "a", "q", "q", "b"]);
        assert!(client.read_any().is_empty());

        let _ = tx.send(());
        sleep(Millis(50)).await;
        assert_eq!(calls.borrow().len(), 6);
        assert_eq!(client.read_any(), Bytes::from_static(b"abc"));
    }

    /// Read is paused while the service is not ready, the queue is full or
    /// on write backpressure. The transport does not read the socket then, so
    /// a peer half-close or a reset is observed only once the backend reports
    /// it (epoll HUP/ERR, failed write), which is what `Terminate` simulates.
    #[derive(Copy, Clone, Debug)]
    enum Disconnect {
        /// Peer closes the connection, clean read eof
        Close,
        /// Connection reset, reported on the next read
        Reset,
        /// Transport failure detected by the backend, e.g. epoll HUP/ERR
        Terminate,
    }

    impl Disconnect {
        async fn apply(self, client: &Io, io: &nio::IoRef) {
            match self {
                Disconnect::Close => client.close().await,
                Disconnect::Reset => {
                    client.read_error(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
                }
                Disconnect::Terminate => io.terminate(),
            }
        }
    }

    fn spawn_disp<F: Future + 'static>(disp: F) -> Rc<Cell<bool>> {
        let done = Rc::new(Cell::new(false));
        let done2 = done.clone();
        ntex_util::spawn(async move {
            let _ = disp.await;
            done2.set(true);
        });
        done
    }

    fn ctl_srv() -> impl Service<(), Control<()>, Res = Option<Bytes>, Error = ()> {
        fn_service(async move |_: Control<()>| Ok::<_, ()>(None))
    }

    async fn assert_stops(case: &str, done: &Cell<bool>) {
        for _ in 0..40 {
            if done.get() {
                return;
            }
            sleep(Millis(50)).await;
        }
        panic!("{case}: dispatcher did not stop after disconnect");
    }

    /// Dispatcher stops on disconnect while idle or reading a frame
    #[ntex::test]
    async fn disconnect_while_reading() {
        for d in [Disconnect::Close, Disconnect::Reset, Disconnect::Terminate] {
            for data in ["", "1", "12"] {
                let (client, server) = Io::create();
                client.remote_buffer_cap(1024);
                let (disp, io) = Dispatcher::new_debug(
                    nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
                    BytesLenCodec(4),
                    fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
                    ctl_srv(),
                );
                let done = spawn_disp(disp);
                client.write(data);
                sleep(Millis(25)).await;
                assert!(!done.get());

                d.apply(&client, &io).await;
                assert_stops(&format!("{d:?}, data {data:?}"), &done).await;
            }
        }
    }

    /// Dispatcher stops on disconnect while service calls are in flight
    #[ntex::test]
    async fn disconnect_with_inflight_calls() {
        for d in [Disconnect::Close, Disconnect::Reset, Disconnect::Terminate] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);
            let dropped = Rc::new(Cell::new(false));
            let (disp, io) = Dispatcher::new_debug(
                nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
                ByteCodec,
                pending_service(dropped.clone()),
                ctl_srv(),
            );
            let done = spawn_disp(disp);
            client.write("123");
            sleep(Millis(25)).await;

            d.apply(&client, &io).await;
            assert_stops(&format!("{d:?}"), &done).await;
            assert!(dropped.get());
        }
    }

    /// Dispatcher stops on disconnect while the response queue is full
    #[ntex::test]
    async fn disconnect_with_full_queue() {
        for d in [Disconnect::Terminate] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);
            let cfg: SharedCfg = SharedCfg::new("DBG")
                .add(IoConfig::new())
                .add(MqttServiceConfig::new().set_max_queue(2))
                .into();
            let (disp, io) = Dispatcher::new_debug(
                nio::Io::new(server, cfg),
                ByteCodec,
                pending_service(Rc::default()),
                ctl_srv(),
            );
            let done = spawn_disp(disp);
            client.write("12345");
            sleep(Millis(25)).await;

            d.apply(&client, &io).await;
            assert_stops(&format!("{d:?}"), &done).await;
        }
    }

    /// Dispatcher stops on disconnect while the service is not ready
    #[ntex::test]
    async fn disconnect_service_not_ready() {
        for d in [Disconnect::Terminate] {
            for data in ["1", "12"] {
                let (client, server) = Io::create();
                client.remote_buffer_cap(1024 * 1024);
                let (srv, control, _) = write_timeout_srv(false);
                let (disp, io) = Dispatcher::new_debug(
                    nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
                    ByteCodec,
                    srv,
                    control,
                );
                let done = spawn_disp(disp);
                client.write(data);
                sleep(Millis(25)).await;
                assert!(!done.get());

                d.apply(&client, &io).await;
                assert_stops(&format!("{d:?}, data {data:?}"), &done).await;
            }
        }
    }

    /// Dispatcher stops on disconnect during write backpressure
    #[ntex::test]
    async fn disconnect_write_backpressure() {
        for d in [Disconnect::Terminate] {
            for ready in [true, false] {
                let (client, server) = Io::create();
                client.remote_buffer_cap(0);
                let (srv, control, data) = write_timeout_srv(ready);
                let (disp, io) = Dispatcher::new_debug(
                    nio::Io::new(
                        server,
                        SharedCfg::new("DBG").add(IoConfig::new().set_write_buf(32 * 1024)),
                    ),
                    ByteCodec,
                    srv,
                    control,
                );
                let done = spawn_disp(disp);
                client.write("1");
                sleep(Millis(25)).await;
                assert_eq!(&data.borrow()[..], &[1]);

                d.apply(&client, &io).await;
                assert_stops(&format!("{d:?}, service ready {ready}"), &done).await;
            }
        }
    }

    /// Service becomes not ready and write backpressure is enabled
    #[ntex::test]
    async fn service_is_not_ready_and_backpressure() {
        let (ctx, rx) = oneshot::channel();

        struct Srv(Cell<bool>, Cell<Option<oneshot::Receiver<()>>>);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                if self.0.get()
                    && let Some(rx) = self.1.take()
                {
                    let _ = rx.await;
                }
                Ok(())
            }

            async fn call(
                &self,
                msg: Bytes,
                _: Ctx<'_, Self, ()>,
            ) -> Result<Option<Bytes>, Self::Error> {
                self.0.set(true);
                Ok(Some(msg))
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(
                server,
                SharedCfg::new("DBG").add(IoConfig::new().set_write_buf(2)),
            ),
            BytesCodec,
            Srv(Cell::new(false), Cell::new(Some(rx))),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        let (tx, rx) = ntex::channel::oneshot::channel();
        ntex_util::spawn(async move {
            let _ = disp.await;
            let _ = tx.send(());
        });

        client.write("123456789");
        client.remote_buffer_cap(16);
        let res = client.read().await;
        assert_eq!(res.unwrap(), Bytes::from_static(b"123456789"));
        client.close().await;
        let _ = ctx.send(());
        let _ = rx.await;
    }
}
