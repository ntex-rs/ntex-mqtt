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

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    struct Flags: u8 {
        /// Service readiness failed, it is not polled during stop
        const READY_ERR  = 0b0000_0001;
        /// Write backpressure state of the last `Control::wr` message
        const WR_ENABLED = 0b0000_0010;
    }
}

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

    /// Returns `false` if the response to the item does not have to follow
    /// the responses to earlier items, such as a ping or an at most once
    /// publish.
    ///
    /// These calls do not keep a slot in the response queue, the response is
    /// written once the call completes.
    fn is_ordered(&self, _: &<Self as Decoder>::Item) -> bool {
        true
    }

    /// Returns how the item is dispatched while the response queue is full.
    ///
    /// Held back items are kept in read order, reading pauses while an item
    /// is held back.
    fn queue_limit(&self, _: &<Self as Decoder>::Item) -> QueueLimit {
        QueueLimit::Hold
    }

    /// Extra response queue slots for [`QueueLimit::Bounded`] items, the
    /// flow control limit of the peer.
    fn bounded_slots(&self) -> usize {
        0
    }
}

/// Dispatch of an item while the response queue is full.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum QueueLimit {
    /// Held back in read order until the queue has room.
    Hold,
    /// Dispatched, such as an ack that pending calls may wait for.
    Bypass,
    /// Dispatched unless earlier items are held back, up to `max_queue` plus
    /// [`FrameState::bounded_slots()`] queued responses. For items the peer's
    /// flow control bounds, such as v5 `QoS 1` and `QoS 2` publishes.
    Bounded,
}

impl<T: FrameState> FrameState for Rc<T> {
    #[inline]
    fn is_partial(&self) -> bool {
        (**self).is_partial()
    }

    #[inline]
    fn is_ordered(&self, item: &T::Item) -> bool {
        (**self).is_ordered(item)
    }

    #[inline]
    fn queue_limit(&self, item: &T::Item) -> QueueLimit {
        (**self).queue_limit(item)
    }

    #[inline]
    fn bounded_slots(&self) -> usize {
        (**self).bounded_slots()
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
    flags: Flags,
    /// Pending `Control::wr` call, messages are delivered one at a time
    wr_call: Option<ControlCall<Codec, E, Err>>,
    /// Limited items read while the response queue is full, in read order
    held: VecDeque<Request<Codec>>,
    /// Hold decision of the frame whose remaining parts are not read yet,
    /// the parts of a frame follow its first item
    frame_hold: Option<bool>,
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
    /// Queue index of the polled call, `None` for an unordered call
    response_idx: Cell<Option<usize>>,
    /// Pending unordered calls, they count towards `max_queue`
    unordered: Cell<usize>,
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
        let cfg = io.cfg().ctx().get::<MqttServiceConfig>();
        let state = Rc::new(DispatcherState {
            error: Cell::new(None),
            base: Cell::new(0),
            queue: RefCell::new(VecDeque::new()),
            waker: LocalWaker::default(),
            response: Cell::new(None),
            response_idx: Cell::new(None),
            unordered: Cell::new(0),
            max_queue: cfg.max_queue,
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
                flags: Flags::empty(),
                wr_call: None,
                held: VecDeque::new(),
                frame_hold: None,
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
        self.max_queue != 0 && len + self.unordered.get() >= self.max_queue
    }

    fn set_error(&self, err: DispatcherError<E>) {
        self.set_stop(match err {
            DispatcherError::Service(err) => Control::err(err),
            DispatcherError::Protocol(err) => Control::proto(err),
        });
    }

    /// Keeps the stop message for the first error, later errors are dropped.
    fn set_stop(&self, msg: Control<E>) {
        let first = self.error.take().unwrap_or(msg);
        self.error.set(Some(first));
    }

    /// Encodes the response of a completed call, returns `true` on error.
    fn write_result(&self, item: ServiceResult<Codec, E>, io: &IoRef, codec: &Codec) -> bool {
        match item {
            Ok(Some(item)) => {
                if let Err(err) = io.encode(item, codec) {
                    self.set_stop(Control::proto(MqttProtocolError::Encode(err)));
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
            let len = self.queue.borrow().len();
            let was_full = self.is_full(len);
            self.unordered.set(self.unordered.get() - 1);
            self.write_result(item, io, codec) || (was_full && !self.is_full(len))
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

        if matches!(
            inner.st,
            IoDispatcherState::Processing | IoDispatcherState::Backpressure
        ) {
            inner.poll_wr(cx);
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
                                inner.dispatch(cx, el);
                            } else {
                                return Poll::Pending;
                            }
                        }
                        Err(RecvError::Timeout) => {
                            if let Err(err) = inner.handle_timeout() {
                                inner.stop(Control::proto(err));
                            }
                        }
                        Err(RecvError::WriteBackpressure) => inner.enter_backpressure(cx),
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
                    } else {
                        // backpressure is released, service readiness is checked
                        // by the processing state
                        inner.stop_timer();
                        inner.st = IoDispatcherState::Processing;
                        inner.poll_wr(cx);
                    }
                }
                // wait for the control service to handle the stop message
                IoDispatcherState::Stop(ref mut fut) => {
                    // service may rely on poll_ready for response results
                    if !inner.flags.contains(Flags::READY_ERR)
                        && let Poll::Ready(Err(_)) = inner.service.poll_ready(cx)
                    {
                        inner.flags.insert(Flags::READY_ERR);
                    }

                    // the stop message is delivered after the pending wr message
                    if inner.wr_call.is_some() {
                        inner.poll_wr(cx);
                        if inner.wr_call.is_some() {
                            return Poll::Pending;
                        }
                        continue;
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
                    // cancel in-flight calls, the polled one is dropped as well,
                    // otherwise it runs until the io is closed
                    inner.io.wake(STOP_TAG);
                    drop(inner.state.response.take());
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

    /// Shuts down the service and io without the stop message.
    fn shutdown_err(&mut self, err: Err) {
        self.timers.active = Timer::Stopped;
        self.io.stop_timer();
        self.st = IoDispatcherState::Shutdown(Some(Err(err)));
    }

    fn enter_backpressure(&mut self, cx: &mut Context<'_>) {
        if !matches!(self.st, IoDispatcherState::Backpressure) {
            self.start_write_timer();
            self.st = IoDispatcherState::Backpressure;
            self.poll_wr(cx);
        }
    }

    /// Delivers write backpressure changes to the control service.
    ///
    /// Messages are delivered one at a time and in order, state changes
    /// during a pending call are coalesced into the latest state. No new
    /// messages are sent after the dispatcher is stopped.
    fn poll_wr(&mut self, cx: &mut Context<'_>) {
        loop {
            if let Some(mut fut) = self.wr_call.take() {
                match Pin::new(&mut fut).poll(cx) {
                    Poll::Pending => {
                        self.wr_call = Some(fut);
                        return;
                    }
                    Poll::Ready(Ok(Some(item))) => {
                        if let Err(err) = self.io.encode(item, &self.codec) {
                            self.state
                                .set_stop(Control::proto(MqttProtocolError::Encode(err)));
                        }
                    }
                    Poll::Ready(Ok(None)) => (),
                    Poll::Ready(Err(err)) => {
                        log::error!(
                            "{}: Control service failed to handle write backpressure, shutdown",
                            self.io.tag()
                        );
                        self.shutdown_err(err);
                        return;
                    }
                }
            }

            let enabled = match self.st {
                IoDispatcherState::Processing => false,
                IoDispatcherState::Backpressure => true,
                _ => return,
            };
            if enabled == self.flags.contains(Flags::WR_ENABLED) {
                return;
            }
            self.flags.set(Flags::WR_ENABLED, enabled);
            self.wr_call = Some(self.control.call_static(Control::wr(enabled)));
        }
    }

    /// Calls the service, limited items are held back while the response
    /// queue is full.
    ///
    /// Pending calls can wait for acks of outgoing packets, reading continues
    /// while the queue is full so that acks are still dispatched, until an
    /// item is held back. Held items are dispatched before new items are
    /// read, so items are held back only while the queue is full.
    fn dispatch(&mut self, cx: &mut Context<'_>, item: Request<Codec>) {
        let hold = if let Some(hold) = self.frame_hold {
            hold
        } else {
            let len = self.state.queue.borrow().len();
            let full = self.state.is_full(len);
            match self.codec.queue_limit(&item) {
                QueueLimit::Hold => full,
                QueueLimit::Bypass => false,
                // not reordered ahead of held items, the extra slots bound
                // items the peer's flow control does not count, such as
                // re-delivered publishes
                QueueLimit::Bounded => {
                    full && (!self.held.is_empty()
                        || len + self.state.unordered.get()
                            >= self.state.max_queue + self.codec.bounded_slots())
                }
            }
        };
        self.frame_hold = self.codec.is_partial().then_some(hold);

        if hold {
            self.held.push_back(item);
        } else {
            self.call_service(cx, item);
        }
    }

    fn call_service(&mut self, cx: &mut Context<'_>, item: Request<Codec>) {
        let ordered = self.codec.is_ordered(&item);
        let mut fut = self.service.call_nowait(item);
        let mut queue = self.state.queue.borrow_mut();

        // unordered calls do not keep a slot in the queue
        let mut push_pending = || {
            if ordered {
                queue.push_back(None);
                Some(self.state.base.get().wrapping_add(queue.len() - 1))
            } else {
                self.state.unordered.set(self.state.unordered.get() + 1);
                None
            }
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

        // control service cannot handle the stop message, shutdown
        let control = self.control.poll_ready(cx);
        if let Poll::Ready(Err(err)) = control {
            log::error!(
                "{}: Control service readiness check failed, shutdown",
                self.io.tag()
            );
            self.shutdown_err(err);
            return Poll::Ready(PollService::Continue);
        }

        // check readiness, pause reading while the service or the control
        // service is not ready, or the response queue is full and an item
        // is held back
        let full = self.state.is_full(self.state.queue.borrow().len());
        let ready = if full && !self.held.is_empty() {
            Poll::Pending
        } else {
            match self.service.poll_ready(cx) {
                Poll::Ready(Ok(())) if control.is_pending() => Poll::Pending,
                ready => ready,
            }
        };
        let msg = match ready {
            Poll::Ready(Ok(())) => {
                // held items are dispatched before new items are read
                if !full && let Some(item) = self.held.pop_front() {
                    // the unread parts of the frame follow the dispatched
                    // item, such as the payload of a streaming publish
                    // that the publish handler waits for
                    if self.frame_hold.is_some() {
                        self.frame_hold = Some(false);
                    }
                    self.call_service(cx, item);
                    return Poll::Ready(PollService::Continue);
                }
                return Poll::Ready(PollService::Ready);
            }
            Poll::Pending => match ready!(self.poll_read_pause(cx)) {
                Some(msg) => msg,
                None => return Poll::Ready(PollService::Continue),
            },
            Poll::Ready(Err(DispatcherError::Service(err))) => {
                log::error!(
                    "{}: Service readiness check failed, stopping",
                    self.io.tag()
                );
                self.flags.insert(Flags::READY_ERR);
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
            // stopped sending cannot make progress while service is not ready,
            // unless read or held back items are waiting
            Poll::Pending
                if self.io.is_read_eof()
                    && self.held.is_empty()
                    && self.io.with_read_dst(|b| b.is_empty()) =>
            {
                IoStatusUpdate::PeerGone(None)
            }
            Poll::Pending => return Poll::Pending,
        };

        Poll::Ready(match status {
            // only the write timer can be armed during pause
            IoStatusUpdate::Timeout => self.handle_timeout().err().map(Control::proto),
            IoStatusUpdate::PeerGone(err) => {
                log::trace!(
                    "{}: Peer is gone during pause, stopping dispatcher: {:?}",
                    self.io.tag(),
                    err
                );
                Some(Control::peer_gone(err))
            }
            IoStatusUpdate::WriteBackpressure => {
                self.enter_backpressure(cx);
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
    use ntex_util::time::{Millis, sleep, timeout};
    use rand::RngExt;

    use super::*;
    use crate::{control::Reason, error::DecodeError, error::EncodeError};

    /// Waits up to 1 second for the condition, returns its last value
    async fn wait_until(f: impl Fn() -> bool) -> bool {
        for _ in 0..100 {
            if f() {
                return true;
            }
            sleep(Millis(10)).await;
        }
        f()
    }

    /// Writes `data` every `step` until the dispatcher stops, up to 10 times
    async fn write_until_stopped(
        client: &Io,
        state: &nio::IoRef,
        data: &'static str,
        step: Millis,
    ) -> bool {
        for _ in 0..10 {
            if !state.is_active() {
                return true;
            }
            client.write(data);
            sleep(step).await;
        }
        !state.is_active()
    }

    /// Reads from the peer until `len` bytes are received
    async fn read_exact(client: &Io, len: usize) -> BytesMut {
        let mut buf = BytesMut::new();
        while buf.len() < len {
            let data = timeout(Millis(1000), client.read()).await.unwrap().unwrap();
            buf.extend_from_slice(&data);
        }
        buf
    }

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

        // both responses are in flight, a late wakeup can read them at once
        let expected = b"GET /test HTTP/1\r\n\r\nGET /test HTTP/1\r\n\r\n";
        let mut buf = BytesMut::new();
        while buf.len() < expected.len() {
            let chunk = client.read().await.unwrap();
            assert!(!chunk.is_empty(), "connection is closed");
            buf.extend_from_slice(&chunk);
        }
        assert_eq!(buf, &expected[..]);

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
        assert!(wait_until(|| client.is_server_dropped()).await);
        assert!(wait_until(|| ops.borrow().len() == 5).await);

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
        assert!(wait_until(|| client.is_server_dropped()).await);
        assert!(wait_until(|| ops.borrow().len() == 5).await);

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

    /// Control service, `busy` makes it not ready until `gate` is opened
    struct Ctl {
        busy: Rc<Cell<bool>>,
        gate: Rc<Cell<bool>>,
        fail: bool,
    }

    impl Service<(), Control<()>> for Ctl {
        type Res = Option<Bytes>;
        type Error = ();

        async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            if self.fail {
                return Err(());
            }
            while self.busy.get() {
                if self.gate.get() {
                    self.busy.set(false);
                } else {
                    sleep(Millis(5)).await;
                }
            }
            Ok(())
        }

        async fn call(&self, _: Control<()>, _: Ctx<'_, Self, ()>) -> Result<Option<Bytes>, ()> {
            Ok(None)
        }
    }

    /// Responses are written and reading is paused while control is not ready
    #[ntex::test]
    async fn control_not_ready() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("1");

        let busy = Rc::new(Cell::new(false));
        let gate = Rc::new(Cell::new(false));
        let calls = Rc::new(Cell::new(0));
        let (busy2, calls2) = (busy.clone(), calls.clone());
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                busy2.set(true);
                calls2.set(calls2.get() + 1);
                sleep(Millis(50)).await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            Ctl {
                busy: busy.clone(),
                gate: gate.clone(),
                fail: false,
            },
        );
        let (tx, rx) = oneshot::channel();
        ntex_util::spawn(async move {
            let _ = tx.send(disp.await);
        });

        // response is written while control is not ready
        let buf = timeout(Millis(5_000), client.read())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buf, Bytes::from_static(b"1"));
        assert!(busy.get());

        // reading is paused until control is ready
        client.write("2");
        sleep(Millis(50)).await;
        assert_eq!(client.read_any(), Bytes::new());
        assert_eq!(calls.get(), 1);

        gate.set(true);
        let buf = timeout(Millis(5_000), client.read())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(buf, Bytes::from_static(b"2"));
        assert_eq!(calls.get(), 2);

        client.close().await;
        assert_eq!(rx.await.unwrap(), Ok(()));
    }

    /// Control readiness error shuts down the dispatcher
    #[ntex::test]
    async fn control_ready_err() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("1");

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
            Ctl {
                busy: Rc::default(),
                gate: Rc::default(),
                fail: true,
            },
        );
        let res = timeout(Millis(500), disp).await.unwrap();
        assert_eq!(res, Err(()));
        assert_eq!(client.read_any(), Bytes::new());
        assert!(client.is_closed());
    }

    /// Protocol error from service readiness stops the dispatcher
    #[ntex::test]
    async fn test_protocol_err_in_service_ready() {
        struct Srv;

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                Err(DispatcherError::Protocol(MqttProtocolError::ReadTimeout))
            }

            async fn call(&self, _: Bytes, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
                Ok(None)
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            BytesCodec,
            Srv,
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    errs2.borrow_mut().push(*err.get_ref());
                }
                Ok::<_, ()>(None)
            }),
        );
        let res = timeout(Millis(500), disp).await.unwrap();
        assert_eq!(res, Ok(()));
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::ReadTimeout]
        ));
        assert!(client.is_closed());
    }

    /// Service readiness error while the control service handles the stop message
    #[ntex::test]
    async fn test_err_in_service_ready_during_stop() {
        struct Srv(Rc<Cell<bool>>, Rc<Cell<usize>>);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                if self.0.get() {
                    self.1.set(self.1.get() + 1);
                    Err(DispatcherError::Service(()))
                } else {
                    Ok(())
                }
            }

            async fn call(&self, _: Bytes, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
                Ok(None)
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let stopped = Rc::new(Cell::new(false));
        let stopped2 = stopped.clone();
        let counter = Rc::new(Cell::new(0));
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG")),
            ByteCodec,
            Srv(stopped.clone(), counter.clone()),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(_)) = msg {
                    stopped2.set(true);
                    sleep(Millis(100)).await;
                }
                Ok::<_, ()>(None)
            }),
        );
        client.write("E");
        let res = timeout(Millis(500), disp).await.unwrap();
        assert_eq!(res, Ok(()));
        assert!(stopped.get());
        assert_eq!(counter.get(), 1);
        assert!(client.is_closed());
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

    /// Decodes one byte per item, `p` and `P` are followed by more parts of a
    /// streamed packet
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

        // a frame that starts with `P` is dispatched while the queue is full
        fn queue_limit(&self, item: &Bytes) -> QueueLimit {
            if item == "P" || item == "k" {
                QueueLimit::Bypass
            } else {
                QueueLimit::Hold
            }
        }
    }

    impl Decoder for ChunkCodec {
        type Item = Bytes;
        type Error = DecodeError;

        fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.is_empty() {
                Ok(None)
            } else {
                self.0.set(src[0] == b'p' || src[0] == b'P');
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

        for _ in 0..2 {
            client.write("p");
            sleep(Millis(300)).await;
        }
        assert!(state.is_active());

        // parts of a streamed packet do not reset keep-alive
        assert!(write_until_stopped(&client, &state, "p", Millis(300)).await);
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

        for _ in 0..2 {
            client.write("ppp");
            sleep(Millis(400)).await;
        }
        assert!(state.is_active());

        // the frame read rate is satisfied until the max timeout
        assert!(write_until_stopped(&client, &state, "ppp", Millis(400)).await);
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
        sleep(Millis(1000)).await;
        assert!(wait_until(|| !state.is_active()).await);
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

        for _ in 0..2 {
            client.write("123");
            sleep(Millis(400)).await;
        }
        assert!(state.is_active());

        // the frame read rate is satisfied until the max timeout
        assert!(write_until_stopped(&client, &state, "123", Millis(400)).await);
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

        for _ in 0..2 {
            client.write("1");
            sleep(Millis(300)).await;
        }
        assert!(state.is_active());

        // received bytes of an incomplete frame do not reset keep-alive
        assert!(write_until_stopped(&client, &state, "1", Millis(300)).await);
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

    /// Frame read budget restarts after the service pauses reading
    #[ntex::test]
    async fn test_read_rate_reset_on_pause() {
        struct Srv(Rc<Cell<bool>>, Condition<()>);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                if self.0.get() {
                    self.1.wait().await;
                }
                Ok(())
            }

            async fn call(
                &self,
                msg: Bytes,
                _: Ctx<'_, Self, ()>,
            ) -> Result<Self::Res, Self::Error> {
                Ok(Some(msg))
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let paused = Rc::new(Cell::new(false));
        let cond = Condition::new();
        let config = SharedCfg::new("DBG").add(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
        );
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, config),
            BytesLenCodec(16),
            Srv(paused.clone(), cond.clone()),
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

        // first period is extended, one second of the budget is left
        client.write("123");
        sleep(Millis(1200)).await;
        client.write("456");
        sleep(Millis(100)).await;

        // pause restarts the budget
        paused.set(true);
        client.write("7");
        sleep(Millis(300)).await;
        paused.set(false);
        cond.notify(());

        // two more periods fit into the restarted budget
        sleep(Millis(200)).await;
        client.write("abc");
        sleep(Millis(1000)).await;
        client.write("def");
        sleep(Millis(200)).await;
        assert!(state.is_active(), "{:?}", errs.borrow());
        assert!(errs.borrow().is_empty());

        client.write("ghi");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"1234567abcdefghi"));
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
        fn is_ordered(&self, item: &Bytes) -> bool {
            item != "q"
        }

        // "k" is an ack, it is dispatched while the queue is full, "p" is
        // bounded by 2 extra slots
        fn queue_limit(&self, item: &Bytes) -> QueueLimit {
            match &item[..] {
                b"k" => QueueLimit::Bypass,
                b"p" => QueueLimit::Bounded,
                _ => QueueLimit::Hold,
            }
        }

        fn bounded_slots(&self) -> usize {
            2
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

    /// The first error is reported to the control service
    #[ntex::test]
    async fn first_error_is_kept() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let msgs = Rc::new(RefCell::new(Vec::new()));
        let msgs2 = msgs.clone();
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, SharedCfg::new("DBG").add(IoConfig::new())),
            ByteCodec,
            fn_service(async move |msg: Bytes| match &msg[..] {
                b"b" => {
                    sleep(Millis(50)).await;
                    Err(DispatcherError::Service(()))
                }
                b"c" => {
                    sleep(Millis(50)).await;
                    Err(DispatcherError::Protocol(
                        MqttProtocolError::KeepAliveTimeout,
                    ))
                }
                _ => {
                    sleep(Millis(10_000)).await;
                    Ok(None)
                }
            }),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(reason) = msg {
                    msgs2.borrow_mut().push(matches!(reason, Reason::Error(_)));
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("abc");
        sleep(Millis(300)).await;
        assert!(!state.is_active());
        assert_eq!(&msgs.borrow()[..], &[true]);
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

    /// Backpressure is released while service is not ready, write timer is stopped
    #[ntex::test]
    async fn test_write_timeout_released_service_not_ready() {
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
        sleep(Millis(300)).await;
        assert_eq!(&data.borrow()[..], &[1]);

        client.remote_buffer_cap(1024 * 1024);
        sleep(Millis(200)).await;
        assert_eq!(&data.borrow()[..], &[1, 2]);

        sleep(Millis(2000)).await;
        assert!(state.is_active());
        assert_eq!(&data.borrow()[..], &[1, 2]);
        assert_eq!(client.read_any().len(), 65_536);
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

    /// Unordered calls do not keep a slot in the response queue
    #[ntex::test]
    async fn unordered_calls_skip_queue() {
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

        // the first call is polled by the dispatcher, others are spawned,
        // unordered calls complete one after another
        client.write("qaqqbc");
        wait_until(|| calls.borrow().len() == 5).await;
        // the queue is full with "a" and "b", "c" is not read
        sleep(Millis(50)).await;
        assert_eq!(&calls.borrow()[..], ["q", "a", "q", "q", "b"]);
        assert!(client.read_any().is_empty());

        let _ = tx.send(());
        assert_eq!(read_exact(&client, 3).await, b"abc"[..]);
        assert_eq!(calls.borrow().len(), 6);
    }

    /// Pending unordered calls count towards the queue limit
    #[ntex::test]
    async fn unordered_calls_limit() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let calls = Rc::new(Cell::new(0));
        let calls2 = calls.clone();
        let gate = Condition::new();
        let gate2 = gate.clone();

        let cfg: SharedCfg = SharedCfg::new("DBG")
            .add(IoConfig::new())
            .add(MqttServiceConfig::new().set_max_queue(3))
            .into();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, cfg),
            ByteCodec,
            fn_service(async move |msg: Bytes| {
                calls2.set(calls2.get() + 1);
                // the polled call never completes, spawned calls wake the dispatcher
                if msg == "a" {
                    std::future::pending::<()>().await;
                }
                let _ = gate2.wait().await;
                Ok::<_, DispatcherError<()>>(None)
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(async move {
            let _ = disp.await;
        });

        client.write("aqqqq");
        assert!(wait_until(|| calls.get() == 3).await);
        sleep(Millis(100)).await;
        assert_eq!(calls.get(), 3);

        // completed calls release the limit
        gate.notify_and_lock(());
        assert!(wait_until(|| calls.get() == 5).await);
    }

    /// Records calls, "k" opens the gate the other calls wait for
    fn ack_srv(
        calls: Rc<RefCell<Vec<Bytes>>>,
        gate: Condition,
    ) -> impl Service<(), Bytes, Res = Option<Bytes>, Error = DispatcherError<()>> {
        fn_service(move |msg: Bytes| {
            let calls = calls.clone();
            let gate = gate.clone();
            async move {
                calls.borrow_mut().push(msg.clone());
                if msg == "k" {
                    gate.notify_and_lock(());
                    return Ok(None);
                }
                let _ = gate.wait().await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }
        })
    }

    fn max_queue_cfg(max_queue: usize) -> SharedCfg {
        SharedCfg::new("DBG")
            .add(IoConfig::new())
            .add(MqttServiceConfig::new().set_max_queue(max_queue))
            .into()
    }

    /// Bounded items are dispatched while nothing is held back, up to
    /// `max_queue` plus the bounded slots
    #[ntex::test]
    async fn bounded_items() {
        // data, dispatched items
        for (data, dispatched) in [
            ("abppp", "abpp"),
            ("abcp", "ab"),
            ("abpcp", "abp"),
            ("pp", "pp"),
        ] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let calls = Rc::new(RefCell::new(Vec::new()));
            let gate = Condition::new();
            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, max_queue_cfg(2)),
                ByteCodec,
                ack_srv(calls.clone(), gate.clone()),
                ctl_srv(),
            );
            let _done = spawn_disp(disp);

            client.write(data);
            sleep(Millis(50)).await;
            let called: Vec<u8> = calls.borrow().iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(&called[..], dispatched.as_bytes(), "{data}");

            // responses follow the read order
            gate.notify_and_lock(());
            assert_eq!(
                read_exact(&client, data.len()).await,
                data.as_bytes(),
                "{data}"
            );
        }
    }

    /// Parts of a frame follow the hold decision of its first item
    #[ntex::test]
    async fn frame_parts_follow_first_item() {
        // data, dispatched items
        for (data, dispatched) in [("abPpc", "abPpc"), ("abpck", "ab")] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let calls = Rc::new(RefCell::new(Vec::new()));
            let gate = Condition::new();
            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, max_queue_cfg(2)),
                ChunkCodec::default(),
                ack_srv(calls.clone(), gate.clone()),
                ctl_srv(),
            );
            let _done = spawn_disp(disp);

            client.write(data);
            sleep(Millis(50)).await;
            let called: Vec<u8> = calls.borrow().iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(&called[..], dispatched.as_bytes(), "{data}");

            // the rest of a held frame is dispatched in order once the queue
            // has room
            gate.notify_and_lock(());
            assert!(wait_until(|| calls.borrow().len() == data.len()).await);
            let called: Vec<u8> = calls.borrow().iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(&called[..], data.as_bytes(), "{data}");
        }
    }

    /// The rest of a held frame is dispatched while the queue is full once
    /// its first item is dispatched, the first item waits for it
    #[ntex::test]
    async fn held_frame_rest_follows_dispatched_item() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let calls = Rc::new(RefCell::new(Vec::new()));
        let calls2 = calls.clone();
        let gate = Condition::new();
        let gate2 = gate.clone();
        let rest = Condition::new();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, max_queue_cfg(1)),
            ChunkCodec::default(),
            fn_service(move |msg: Bytes| {
                let calls = calls2.clone();
                let gate = gate2.clone();
                let rest = rest.clone();
                async move {
                    calls.borrow_mut().push(msg.clone());
                    match &msg[..] {
                        b"a" => {
                            let _ = gate.wait().await;
                        }
                        b"p" => {
                            let _ = rest.wait().await;
                        }
                        _ => {
                            rest.notify_and_lock(());
                            return Ok(None);
                        }
                    }
                    Ok::<_, DispatcherError<()>>(Some(msg))
                }
            }),
            ctl_srv(),
        );
        let _done = spawn_disp(disp);

        // "p" is held, its rest "c" is not read
        client.write("apc");
        sleep(Millis(50)).await;
        assert_eq!(&calls.borrow()[..], ["a"]);

        // "p" fills the queue, "c" is dispatched
        gate.notify_and_lock(());
        assert_eq!(read_exact(&client, 2).await, b"ap"[..]);
        assert_eq!(&calls.borrow()[..], ["a", "p", "c"]);
    }

    /// Acks are dispatched while the queue is full, limited items are held
    /// back and dispatched once the queue has room
    #[ntex::test]
    async fn acks_dispatched_while_queue_is_full() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let calls = Rc::new(RefCell::new(Vec::new()));
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, max_queue_cfg(2)),
            ByteCodec,
            ack_srv(calls.clone(), Condition::new()),
            ctl_srv(),
        );
        let _done = spawn_disp(disp);

        client.write("ab");
        sleep(Millis(50)).await;
        assert_eq!(&calls.borrow()[..], ["a", "b"]);
        assert!(client.read_any().is_empty());

        // the ack is read while the queue is full and completes the pending
        // calls
        client.write("kc");
        assert_eq!(read_exact(&client, 3).await, b"abc"[..]);
        assert_eq!(&calls.borrow()[..], ["a", "b", "k", "c"]);
    }

    /// Reading pauses at the first held item, held items are dispatched
    /// before the items read after them
    #[ntex::test]
    async fn held_item_pauses_reading() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let calls = Rc::new(RefCell::new(Vec::new()));
        let gate = Condition::new();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, max_queue_cfg(2)),
            ByteCodec,
            ack_srv(calls.clone(), gate.clone()),
            ctl_srv(),
        );
        let _done = spawn_disp(disp);

        // "c" is held back, "d" and the ack are not read
        client.write("abcdk");
        sleep(Millis(50)).await;
        assert_eq!(&calls.borrow()[..], ["a", "b"]);

        gate.notify_and_lock(());
        assert_eq!(read_exact(&client, 4).await, b"abcd"[..]);
        assert!(wait_until(|| calls.borrow().len() == 5).await);
        // "k" can be dispatched before "d" if the queue is full again
        let calls = calls.borrow();
        let pos = |item| calls.iter().position(|c| c == item).unwrap();
        assert!(pos("c") < pos("d"));
        assert!(pos("k") > pos("c"));
    }

    /// Held items are dispatched after a clean read eof
    #[ntex::test]
    async fn held_items_after_read_eof() {
        // one or two items after the held item, eof with the data or later
        for (data, delay) in [("abc", 0), ("abcd", 0), ("abc", 20), ("abcd", 20)] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let calls = Rc::new(RefCell::new(Vec::new()));
            let gate = Condition::new();
            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, max_queue_cfg(2)),
                ByteCodec,
                ack_srv(calls.clone(), gate.clone()),
                ctl_srv(),
            );
            let done = spawn_disp(disp);

            client.write(data);
            if delay > 0 {
                sleep(Millis(delay)).await;
                assert_eq!(&calls.borrow()[..], ["a", "b"], "{data}");
            }
            client.close().await;
            sleep(Millis(50)).await;
            assert_eq!(&calls.borrow()[..], ["a", "b"], "{data}");
            assert!(!done.get(), "{data}");

            gate.notify_and_lock(());
            assert_stops(data, &done).await;
            assert_eq!(calls.borrow().len(), data.len(), "{data}");
        }
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

    /// Control service that records `wr` and stop messages
    #[derive(Clone)]
    struct WrCtl {
        log: Rc<RefCell<Vec<&'static str>>>,
        /// `wr(true)` handling waits for the gate
        gate: Option<Condition<()>>,
        /// response for `wr(false)`
        resp: Option<Bytes>,
        /// `wr(false)` fails
        fail: bool,
    }

    impl WrCtl {
        fn new(gate: Option<Condition<()>>) -> Self {
            WrCtl {
                log: Rc::default(),
                gate,
                resp: None,
                fail: false,
            }
        }
    }

    impl Service<(), Control<()>> for WrCtl {
        type Res = Option<Bytes>;
        type Error = ();

        async fn call(&self, msg: Control<()>, _: Ctx<'_, Self, ()>) -> Result<Option<Bytes>, ()> {
            match msg {
                Control::WrBackpressure(st) if st.enabled() => {
                    self.log.borrow_mut().push("wr1");
                    if let Some(gate) = &self.gate {
                        gate.wait().await;
                    }
                    self.log.borrow_mut().push("wr1-done");
                    Ok(None)
                }
                Control::WrBackpressure(_) => {
                    self.log.borrow_mut().push("wr0");
                    if self.fail { Err(()) } else { Ok(self.resp.clone()) }
                }
                Control::Stop(_) => {
                    self.log.borrow_mut().push("stop");
                    Ok(None)
                }
            }
        }
    }

    fn wr_dispatcher(
        ctl: WrCtl,
        write_timeout: Seconds,
    ) -> (Io, nio::IoRef, oneshot::Receiver<Result<(), ()>>) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let cfg = SharedCfg::new("DBG").add(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_write_buf(32 * 1024)
                .set_write_timeout(write_timeout),
        );
        let (disp, io) = Dispatcher::new_debug(
            nio::Io::new(server, cfg),
            BytesCodec,
            fn_service(async |_: Bytes| {
                Ok::<_, DispatcherError<()>>(Some(Bytes::from(vec![b'x'; 65_536])))
            }),
            ctl,
        );
        let (tx, rx) = oneshot::channel();
        ntex_util::spawn(async move {
            let _ = tx.send(disp.await);
        });
        client.write("1");
        (client, io, rx)
    }

    /// `wr(false)` is delivered after `wr(true)` is handled
    #[ntex::test]
    async fn test_wr_in_order() {
        let gate = Condition::new();
        let ctl = WrCtl::new(Some(gate.clone()));
        let (client, _io, _rx) = wr_dispatcher(ctl.clone(), Seconds::ZERO);
        assert!(wait_until(|| ctl.log.borrow().len() == 1).await);

        // backpressure is released while wr(true) is pending
        client.remote_buffer_cap(1024 * 1024);
        let _ = read_exact(&client, 65_536).await;
        sleep(Millis(50)).await;
        assert_eq!(&ctl.log.borrow()[..], &["wr1"]);

        gate.notify(());
        assert!(wait_until(|| ctl.log.borrow().len() == 3).await);
        assert_eq!(&ctl.log.borrow()[..], &["wr1", "wr1-done", "wr0"]);
    }

    /// Stop is delivered after the pending wr message
    #[ntex::test]
    async fn test_wr_before_stop() {
        let gate = Condition::new();
        let ctl = WrCtl::new(Some(gate.clone()));
        let (_client, _io, _rx) = wr_dispatcher(ctl.clone(), Seconds(1));

        // write timeout stops the dispatcher while wr(true) is pending
        sleep(Millis(2500)).await;
        assert_eq!(&ctl.log.borrow()[..], &["wr1"]);

        gate.notify(());
        assert!(wait_until(|| ctl.log.borrow().len() == 3).await);
        assert_eq!(&ctl.log.borrow()[..], &["wr1", "wr1-done", "stop"]);
    }

    /// Response of the wr message is written
    #[ntex::test]
    async fn test_wr_response() {
        let mut ctl = WrCtl::new(None);
        ctl.resp = Some(Bytes::from_static(b"wr"));
        let (client, _io, _rx) = wr_dispatcher(ctl.clone(), Seconds::ZERO);
        assert!(wait_until(|| ctl.log.borrow().len() == 2).await);

        client.remote_buffer_cap(1024 * 1024);
        let buf = read_exact(&client, 65_538).await;
        assert_eq!(buf.len(), 65_538);
        assert!(buf.ends_with(b"wr"));
        assert_eq!(&ctl.log.borrow()[..], &["wr1", "wr1-done", "wr0"]);
    }

    /// Error of the wr message shuts down the dispatcher without stop message
    #[ntex::test]
    async fn test_wr_error() {
        let mut ctl = WrCtl::new(None);
        ctl.fail = true;
        let (client, _io, rx) = wr_dispatcher(ctl.clone(), Seconds::ZERO);
        assert!(wait_until(|| ctl.log.borrow().len() == 2).await);

        client.remote_buffer_cap(1024 * 1024);
        assert_eq!(timeout(Millis(1000), rx).await.unwrap().unwrap(), Err(()));
        assert_eq!(&ctl.log.borrow()[..], &["wr1", "wr1-done", "wr0"]);
    }
}
