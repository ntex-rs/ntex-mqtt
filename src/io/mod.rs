//! Concurrent framed transport dispatcher with ordered responses and staged shutdown.
/// Timer selection and byte-rate accounting for incomplete frames.
mod timer;

use std::task::{Context, Poll, ready};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc};

use ntex_codec::{Decoder, Encoder};
use ntex_io::{Decoded, IoBoxed, IoRef, IoStatusUpdate, RecvError};
use ntex_service::{cfg::Configuration, pipeline::Pipeline, pipeline::PipelineCall};
use ntex_util::{future::Either, future::select, spawn_detached, time::Seconds};

use self::timer::{Timer, Timers};
use crate::config::MqttServiceConfig;
use crate::control::Control;
use crate::error::{DecodeError, DispatcherError, EncodeError, MqttProtocolError};

/// The response queue is released once it is empty and its capacity is above
/// this number of slots.
const QUEUE_SHRINK_CAP: usize = 16;

/// Io waiter tag, in-flight service calls are cancelled once it is woken.
const STOP_TAG: usize = 0x6d71_7474;

/// Io waiter tag, woken once an outgoing streaming publish payload is complete
/// or the connection is closed.
pub(crate) const STREAM_TAG: usize = 0x6d71_7475;

bitflags::bitflags! {
    /// Readiness-failure and last-delivered write-backpressure state.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    struct Flags: u8 {
        /// Service readiness failed, it is not polled during stop
        const READY_ERR  = 0b0000_0001;
        /// Write backpressure state of the last `Control::wr` message
        const WR_ENABLED = 0b0000_0010;
    }
}

/// Decoder state the dispatcher needs for read timers and response ordering.
pub trait FrameState: Decoder + Encoder {
    /// Response of the service, it is converted to the encoder item when it
    /// is written.
    type Response: Into<<Self as Encoder>::Item> + 'static;

    /// Response of an ordered call kept in the response queue, it can be
    /// smaller than the service response if ordered calls respond with a
    /// subset of packets.
    type Queued: From<Self::Response> + Into<<Self as Encoder>::Item> + 'static;

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
    /// These calls do not keep an ordered response slot; their responses are
    /// written as soon as they complete. Pending unordered calls still count
    /// toward the dispatcher's queue limit. Ordering constrains responses, not
    /// whether request handlers run concurrently.
    fn is_ordered(&self, _: &<Self as Decoder>::Item) -> bool {
        true
    }
}

impl<T: FrameState> FrameState for Rc<T> {
    type Response = T::Response;
    type Queued = T::Queued;

    #[inline]
    fn is_partial(&self) -> bool {
        (**self).is_partial()
    }

    #[inline]
    fn is_ordered(&self, item: &<T as Decoder>::Item) -> bool {
        (**self).is_ordered(item)
    }
}

/// Decoded item passed to the request service.
type Request<U> = <U as Decoder>::Item;
/// Request-service response before conversion to an encoder item.
type Response<U> = <U as FrameState>::Response;
/// Control-service response written directly through the codec.
type ControlResponse<U> = <U as Encoder>::Item;
/// Compact response retained until earlier ordered calls complete.
type Queued<U> = <U as FrameState>::Queued;
/// Slot for an ordered call: `None` while pending, `Some(None)` when completed
/// without a response, or `Some(Some(response))` when ready to write.
pub(crate) type QueueSlot<U> = Option<Option<Queued<U>>>;
/// Request-call completion, optionally containing a response or stop-triggering error.
type ServiceResult<Codec, E> = Result<Option<Response<Codec>>, DispatcherError<E>>;

/// Owned request-call future that the dispatcher can poll or spawn.
type ServiceCall<Codec, E> =
    PipelineCall<Request<Codec>, Option<Response<Codec>>, DispatcherError<E>>;
/// Request pipeline sharing cached readiness across dispatched items.
type ServicePipeline<Codec, E> =
    Pipeline<Request<Codec>, Option<Response<Codec>>, DispatcherError<E>>;

/// Owned control-call future for backpressure changes or the stop message.
type ControlCall<Codec, E, Err> = PipelineCall<Control<E>, Option<ControlResponse<Codec>>, Err>;
/// Control pipeline handling connection state changes and optional protocol responses.
type ControlPipeline<Codec, E, Err> = Pipeline<Control<E>, Option<ControlResponse<Codec>>, Err>;

pin_project_lite::pin_project! {
    /// Drives MQTT request handling, ordered output, connection deadlines, and shutdown.
    pub(crate) struct Dispatcher<U, E, Err>
    where
        U: FrameState,
        U: 'static,
        E: 'static,
        Err: 'static,
    {
        inner: DispatcherInner<U, E, Err>
    }
}

/// State owned by the dispatcher until transport shutdown finishes or it is dropped.
struct DispatcherInner<Codec, E, Err>
where
    Codec: FrameState + 'static,
    E: 'static,
    Err: 'static,
{
    /// Owned transport carrying input, encoded output, and the single dispatcher timer.
    io: IoBoxed,
    /// Packet codec reporting streaming-frame state and response-order requirements.
    codec: Codec,
    /// Request handler pipeline whose readiness gates further decoding.
    service: ServicePipeline<Codec, E>,
    /// Connection-control pipeline for backpressure transitions and the first stop reason.
    control: ControlPipeline<Codec, E, Err>,
    /// Current processing, flushing, or staged shutdown phase.
    st: IoDispatcherState<Codec, E, Err>,
    /// Response accounting shared with independently spawned request calls.
    state: Rc<DispatcherState<Codec, E>>,
    /// Armed timer purpose and incomplete-frame byte-rate progress.
    timers: Timers,
    /// Idle or incomplete-packet deadline in seconds; zero disables keep-alive.
    keepalive_timeout: Seconds,
    /// Readiness-failure latch and last backpressure state delivered to control.
    flags: Flags,
    /// Pending `Control::wr` call, messages are delivered one at a time
    wr_call: Option<ControlCall<Codec, E, Err>>,
}

/// Shared result queue and stop reason retained by pending request calls.
struct DispatcherState<Codec, E>
where
    Codec: FrameState + 'static,
    E: 'static,
{
    /// Stop message for the first error
    error: Cell<Option<Control<E>>>,
    /// Index of the queue head
    base: Cell<usize>,
    /// Service results in request order, `None` is a pending call
    queue: RefCell<VecDeque<QueueSlot<Codec>>>,
    /// Pending call polled by the dispatcher, other pending calls are spawned
    response: Cell<Option<ServiceCall<Codec, E>>>,
    /// Queue index of the polled call, `None` for an unordered call
    response_idx: Cell<Option<usize>>,
    /// Pending unordered calls, they count towards `max_queue`
    unordered: Cell<usize>,
    /// Limit on queued ordered responses plus pending unordered calls; zero is unlimited.
    max_queue: usize,
}

/// Dispatcher phases from normal decoding through service and transport shutdown.
#[derive(Debug)]
enum IoDispatcherState<Codec: FrameState, E: 'static, Err: 'static> {
    /// Check readiness, decode input, and dispatch requests.
    Processing,
    /// Flush buffered output while monitoring write timeout and request failures.
    Backpressure,
    /// Wait for pending backpressure handling, then deliver the stop control call.
    Stop(ControlCall<Codec, E, Err>),
    /// Shut down the request service, retaining the eventual dispatcher result.
    Shutdown(Option<Result<(), Err>>),
    /// Cancel pending calls and await transport shutdown before returning the result.
    ShutdownIo(Option<Result<(), Err>>),
}

/// Readiness outcome telling the dispatcher whether its current phase can decode input.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PollService {
    /// The phase changed; loop again without attempting to decode.
    Continue,
    /// Both pipelines and response accounting permit another decode attempt.
    Ready,
}

impl<Codec, E, Err> Dispatcher<Codec, E, Err>
where
    Codec:
        Decoder<Error = DecodeError> + Encoder<Error = EncodeError> + FrameState + Clone + 'static,
    <Codec as Encoder>::Item: 'static,
    E: 'static,
{
    /// Creates a dispatcher using transport-configured queue limits and keep-alive.
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
    Codec: Encoder<Error = EncodeError> + Decoder<Error = DecodeError> + FrameState,
    <Codec as Encoder>::Item: 'static,
{
    /// Checks the combined ordered-queue and pending-unordered count against the limit.
    fn is_full(&self, len: usize) -> bool {
        self.max_queue != 0 && len + self.unordered.get() >= self.max_queue
    }

    /// Converts a request error to a control stop reason, preserving any earlier error.
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
            Ok(item) => self.write_response(item, io, codec),
            Err(err) => {
                self.set_error(err);
                true
            }
        }
    }

    /// Encodes a response, returns `true` on error.
    fn write_response<T>(&self, item: Option<T>, io: &IoRef, codec: &Codec) -> bool
    where
        T: Into<<Codec as Encoder>::Item>,
    {
        if let Some(item) = item
            && let Err(err) = io.encode(item.into(), codec)
        {
            self.set_stop(Control::proto(MqttProtocolError::Encode(err)));
            return true;
        }
        false
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
            let _ = queue.pop_front();
            self.base.set(self.base.get().wrapping_add(1));
            let mut err = self.write_result(item, io, codec);
            while let Some(res) = queue.front_mut().and_then(Option::take) {
                let _ = queue.pop_front();
                self.base.set(self.base.get().wrapping_add(1));
                err |= self.write_response(res, io, codec);
            }
            // release the memory of a burst
            if queue.is_empty() && queue.capacity() > QUEUE_SHRINK_CAP {
                queue.shrink_to_fit();
            }
            // the dispatcher waits for results only on errors and a full queue
            err || (was_full && !self.is_full(queue.len()))
        } else {
            match item {
                Ok(res) => {
                    queue[idx] = Some(res.map(Queued::<Codec>::from));
                    false
                }
                Err(err) => {
                    self.set_error(err);
                    true
                }
            }
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
                        Err(RecvError::WriteBackpressure) => inner.enter_backpressure(cx),
                        Err(RecvError::Decoder(err)) => {
                            inner.stop(Control::proto(MqttProtocolError::Decode(err)));
                        }
                        Err(RecvError::PeerGone(err)) => inner.stop(Control::peer_gone(err)),
                    }
                }
                // handle write back-pressure
                IoDispatcherState::Backpressure => {
                    // a failed call stops the dispatcher, the queued output is
                    // flushed by the io shutdown
                    if let Some(msg) = inner.state.error.take() {
                        log::trace!("{}: Error occurred, stopping dispatcher", inner.io.tag());
                        inner.stop(msg);
                        continue;
                    }
                    if let Err(err) = inner.poll_write_timeout(cx) {
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
    Codec: FrameState + 'static,
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

    /// Enters output flushing, starts its deadline, and reports the backpressure transition.
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

    /// Starts a request, queues ordered responses, and spawns calls beyond the one polled locally.
    ///
    /// Spawned calls register their stop waiter before spawning so shutdown cannot
    /// miss cancelling a task that has not yet been polled.
    fn call_service(&mut self, cx: &mut Context<'_>, item: Request<Codec>) {
        let ordered = self.codec.is_ordered(&item);
        // Reuse cached readiness unless the service has notified a change.
        let mut fut = self.service.call_static(item);
        let result = Pin::new(&mut fut).poll(cx);
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

        if let Poll::Ready(res) = result {
            // only responses of ordered calls wait for the calls before them
            match res {
                Ok(Some(res)) if ordered && !queue.is_empty() => {
                    queue.push_back(Some(Some(Queued::<Codec>::from(res))));
                }
                res => {
                    self.state.write_result(res, self.io.as_ref(), &self.codec);
                }
            }
        } else if let Some(resp) = self.state.response.take() {
            // only one pending call is polled by the dispatcher, spawn the rest
            self.state.response.set(Some(resp));
            let response_idx = push_pending();

            let st = self.io.get_ref();
            let codec = self.codec.clone();
            let state = self.state.clone();
            // the dispatcher is not stopped yet, register the waiter now
            // so a wake before the first poll of the task is not missed
            let stopping = st.waiter(STOP_TAG).into_static();
            let _ = stopping.poll_ready(cx);

            spawn_detached(async move {
                let item = match select(fut, stopping).await {
                    Either::Left(item) => item,
                    Either::Right(()) => Ok(None),
                };
                if state.complete(item, response_idx, &st, &codec) {
                    st.notify_dispatcher();
                }
            });
        } else {
            let response_idx = push_pending();
            self.state.response_idx.set(response_idx);
            self.state.response.set(Some(fut));
        }
    }

    /// Checks stop reasons and pipeline readiness, pausing reads when capacity is unavailable.
    ///
    /// A full response queue still permits decoding the remainder of a partial packet.
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
        // service is not ready, or the response queue is full. The rest of
        // a partially read frame is read while the queue is full, such as
        // the payload of a streaming publish that its handler waits for
        let ready =
            if !self.codec.is_partial() && self.state.is_full(self.state.queue.borrow().len()) {
                Poll::Pending
            } else {
                match self.service.poll_ready(cx) {
                    Poll::Ready(Ok(())) if control.is_pending() => Poll::Pending,
                    ready => ready,
                }
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
            // unless read items are waiting
            Poll::Pending if self.io.is_read_eof() && self.io.with_read_dst(|b| b.is_empty()) => {
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

    /// Updates incomplete-frame accounting and selects a read deadline after decoding.
    ///
    /// Streaming chunks retain frame progress; filter pauses suspend read deadlines
    /// and reset the rate budget so local delays are not charged to the peer.
    fn update_timer(&mut self, decoded: &Decoded<<Codec as Decoder>::Item>) {
        // parts of a streamed publish are read as one frame
        let item = decoded.item.is_some() && !self.codec.is_partial();
        self.timers.update_read(
            self.io.cfg(),
            item,
            decoded.remains as u32,
            decoded.consumed as u32,
        );

        // the filter chain pauses reading, the peer is not charged for it
        if decoded.item.is_none() && self.io.is_filter_paused() {
            if self.timers.active != Timer::Write {
                self.stop_timer();
            }
            self.timers.reset_read(self.io.cfg());
            return;
        }

        // keep-alive and frame read timers do not apply while a complete frame is handled
        let timer = self
            .timers
            .select(self.io.cfg(), !self.keepalive_timeout.is_zero(), item);
        self.set_timer(timer);
    }

    /// Starts the write timeout when write backpressure is enabled.
    ///
    /// Frames are not decoded during backpressure, so read-side timers are
    /// stopped when no write timeout is configured. The timeout does not run
    /// while the filter chain pauses writes, the peer is not charged for it,
    /// and starts over once writes resume.
    fn start_write_timer(&mut self) {
        let timeout = self.io.cfg().write_timeout();
        if timeout.is_zero() || self.io.is_filter_paused() {
            self.stop_timer();
        } else if self.timers.active != Timer::Write {
            log::trace!("{}: Start write timer {:?}", self.io.tag(), timeout);
            self.timers.active = Timer::Write;
            self.io.start_timer(timeout);
        }
    }

    /// Checks the write timeout during write backpressure, the timeout is
    /// suspended or resumed on a filter pause.
    fn poll_write_timeout(&mut self, cx: &mut Context<'_>) -> Result<(), MqttProtocolError> {
        if let Poll::Ready(IoStatusUpdate::Timeout) = self.io.poll_status_update(cx) {
            self.handle_timeout()?;
        }
        self.start_write_timer();
        Ok(())
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

    /// Handles an elapsed timer, extending a sufficient frame rate or returning a timeout error.
    ///
    /// Frame progress must exceed the configured byte rate to earn another period;
    /// a nonzero cumulative budget bounds those extensions. Stale write expirations
    /// after backpressure release or a filter pause do not fail the connection.
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
            // the filter chain paused writes before the dispatcher noticed
            Timer::Write if self.io.is_filter_paused() => {
                self.stop_timer();
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

/// Unit tests for response ordering, readiness, deadlines, backpressure, and shutdown.
#[cfg(test)]
#[allow(clippy::items_after_statements)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::{cell::Cell, io};

    use ntex_bytes::{BytePages, Bytes, BytesMut};
    use ntex_io::{self as nio, IoConfig, testing::IoTest as Io};
    use ntex_service::{Ctx, IntoService, Service, cfg::SharedCfg, fn_service};
    use ntex_util::channel::{condition::Condition, oneshot};
    use ntex_util::time::{Millis, sleep, timeout};
    use rand::RngExt;

    use super::*;
    use crate::{control::Reason, error::DecodeError, error::EncodeError};

    /// Test io pair, the peer accepts up to 1024 bytes
    fn create() -> (Io, nio::Io) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        (client, nio::Io::new(server, SharedCfg::new("DBG")))
    }

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

    /// Pass-through codec treating all currently buffered bytes as one complete frame.
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

    impl FrameState for BytesCodec {
        type Response = Bytes;
        type Queued = Bytes;
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
        /// Creates test pipelines and returns the dispatcher with a shared transport handle.
        pub(crate) fn new_debug<P, C, F: IntoService<P, (), Request<U>>>(
            io: nio::Io,
            codec: U,
            service: F,
            control: C,
        ) -> (Self, nio::IoRef)
        where
            P: Service<(), Request<U>, Res = Option<Response<U>>, Error = DispatcherError<E>>
                + 'static,
            C: Service<(), Control<E>, Res = Option<ControlResponse<U>>, Error = Err> + 'static,
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

    /// STATEMENT: Delayed echo responses preserve all bytes and peer closure tears down the dispatcher.
    ///
    /// HOW:
    /// - Send two requests while their echo handlers are delayed and collect every response chunk.
    /// - Verify the concatenated responses match both requests, then close the peer and verify the server is dropped.
    #[ntex::test]
    async fn test_basic() {
        let (client, server) = create();
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                sleep(Millis(50)).await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(disp);
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

    /// STATEMENT: Disconnect releases service-owned state even while response handlers are pending.
    ///
    /// HOW:
    /// - Start a delayed echo request, send another frame, and close the peer before the first call completes.
    /// - Verify the server is dropped and the shared lifetime probe records service-state release.
    #[ntex::test]
    async fn test_drop_connection() {
        let (client, server) = create();
        client.write("test");

        /// Signals when the service-owned probe clones have been released.
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
            server,
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
        ntex_util::spawn(disp);
        sleep(Millis(25)).await;
        client.write("pl1");
        client.close().await;
        assert!(client.is_server_dropped());
        // service dropped?
        assert!(ops.get());
    }

    /// STATEMENT: Responses from concurrently pending ordered requests are emitted in request order.
    ///
    /// HOW:
    /// - Send three echo requests whose handlers await a shared condition.
    /// - Open the condition, verify `testtesttest` is received, then close the peer and verify server teardown.
    #[ntex::test]
    async fn test_ordering() {
        let (client, server) = create();
        client.write("test");

        let condition = Condition::new();
        let waiter = condition.wait();

        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                waiter.clone().await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(disp);
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

    /// STATEMENT: Disconnect control handling completes before pending publish handlers are dropped.
    ///
    /// HOW:
    /// - Record handler starts, delayed disconnect control completion, and handler-drop events for two pending publishes.
    /// - Repeat with an earlier delayed response occupying the locally polled call, and verify both cases record disconnect before the two drops.
    #[ntex::test]
    async fn test_disconnect_ordering() {
        /// Recorded handler, disconnect-control, and handler-drop events.
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        enum Info {
            /// A gated publish handler has started.
            Publish,
            /// A publish handler's lifetime probe has been dropped.
            PublishDrop,
            /// The delayed disconnect control handler has completed.
            Disconnect,
        }

        /// Records a handler-drop event when a publish call is cancelled or completes.
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

    /// STATEMENT: The shared transport handle can write independently and close the dispatcher connection.
    ///
    /// HOW:
    /// - Echo one request, then encode `test` directly through the transport handle and verify both responses.
    /// - Close the shared handle and verify the peer observes server teardown.
    #[ntex::test]
    async fn test_sink() {
        let (client, server) = create();
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            server,
            BytesCodec,
            fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
            fn_service(async move |_: Control<()>| Ok::<_, ()>(None)),
        );
        ntex_util::spawn(disp);

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        assert!(io.encode(Bytes::from_static(b"test"), &BytesCodec).is_ok());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"test"));

        io.close();
        sleep(Millis(150)).await;
        assert!(client.is_server_dropped());
    }

    /// STATEMENT: A request-service error flushes buffered output before closing the write side.
    ///
    /// HOW:
    /// - Queue an output frame while the peer accepts no bytes and make the request handler fail.
    /// - Allow writes, verify the queued frame is received and the write side closes, then close the peer and verify teardown.
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
        ntex_util::spawn(disp);

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

    /// STATEMENT: A failing service readiness check is not retried during shutdown and buffered output is flushed.
    ///
    /// HOW:
    /// - Use a counting service that fails readiness and prequeue output while peer writes are blocked.
    /// - Allow writes, verify the output and write-side closure, close the peer, and verify readiness failed exactly once.
    #[ntex::test]
    async fn test_err_in_service_ready() {
        /// Service whose failing readiness check increments a shared counter.
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
        ntex_util::spawn(disp);

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

    /// STATEMENT: Request and control readiness are cached independently across decoded frames.
    ///
    /// HOW:
    /// - Wrap both pipelines with readiness counters, echo three frames, and verify each readiness check ran once.
    /// - Notify only the request pipeline, echo another frame, and verify only its readiness counter increments.
    #[ntex::test]
    async fn test_cached_readiness_across_frames() {
        /// Counts wrapped readiness checks and exposes an explicit invalidation notifier.
        struct CountReady<S>(S, Rc<Cell<usize>>, ntex_service::ReadyNotifier);

        impl<S: Service<(), R>, R> Service<(), R> for CountReady<S> {
            type Res = S::Res;
            type Error = S::Error;

            fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
                ctx.subscribe(&self.2);
                ctx.bind(&self.0);
            }

            async fn ready(&self, ctx: Ctx<'_, Self>) -> Result<(), Self::Error> {
                self.1.set(self.1.get() + 1);
                ctx.ready(&self.0).await
            }

            async fn call(&self, req: R, ctx: Ctx<'_, Self>) -> Result<Self::Res, Self::Error> {
                ctx.call(&self.0, req).await
            }
        }

        let checks = Rc::new(Cell::new(0));
        let control_checks = Rc::new(Cell::new(0));
        let changed = ntex_service::ReadyNotifier::new();
        let (client, server) = create();
        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            CountReady(
                fn_service(async |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
                checks.clone(),
                changed.clone(),
            ),
            CountReady(
                fn_service(async |_: Control<()>| Ok::<_, ()>(None)),
                control_checks.clone(),
                ntex_service::ReadyNotifier::new(),
            ),
        );
        ntex_util::spawn(disp);

        for _ in 0..3 {
            client.write("1");
            assert_eq!(client.read().await.unwrap(), Bytes::from_static(b"1"));
        }
        assert_eq!((checks.get(), control_checks.get()), (1, 1));
        changed.notify();
        client.write("2");
        assert_eq!(client.read().await.unwrap(), Bytes::from_static(b"2"));
        assert_eq!((checks.get(), control_checks.get()), (2, 1));
        client.close().await;
    }

    /// Control service, `busy` makes it not ready until `gate` is opened
    struct Ctl {
        /// Whether control readiness is blocked pending the test gate.
        busy: Rc<Cell<bool>>,
        /// Shared switch releasing blocked control readiness checks.
        gate: Rc<Cell<bool>>,
        /// Whether control readiness returns an error instead of waiting.
        fail: bool,
        /// Notifier invalidating cached control readiness when the test makes it busy.
        changed: ntex_service::ReadyNotifier,
    }

    impl Service<(), Control<()>> for Ctl {
        type Res = Option<Bytes>;
        type Error = ();

        fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
            ctx.subscribe(&self.changed);
        }

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

    /// STATEMENT: Control backpressure pauses new requests without blocking pending responses.
    ///
    /// HOW:
    /// - Make control readiness busy during the first delayed echo and verify its response is still written.
    /// - Send a second frame while control is busy and verify no handler starts or response arrives.
    /// - Open the control gate, verify the second echo is dispatched, and close the peer with a successful dispatcher result.
    #[ntex::test]
    async fn control_not_ready() {
        let (client, server) = create();
        client.write("1");

        let busy = Rc::new(Cell::new(false));
        let gate = Rc::new(Cell::new(false));
        let calls = Rc::new(Cell::new(0));
        let (busy2, calls2) = (busy.clone(), calls.clone());
        let changed = ntex_service::ReadyNotifier::new();
        let changed2 = changed.clone();
        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            fn_service(async move |msg: Bytes| {
                busy2.set(true);
                changed2.notify();
                calls2.set(calls2.get() + 1);
                sleep(Millis(50)).await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }),
            Ctl {
                busy: busy.clone(),
                gate: gate.clone(),
                fail: false,
                changed,
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

    /// STATEMENT: Control readiness failure shuts down the dispatcher without dispatching buffered input.
    ///
    /// HOW:
    /// - Send one frame to a dispatcher whose control service immediately fails readiness.
    /// - Verify the dispatcher returns `Err(())`, writes no response, and closes the connection.
    #[ntex::test]
    async fn control_ready_err() {
        let (client, server) = create();
        client.write("1");

        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg))),
            Ctl {
                busy: Rc::default(),
                gate: Rc::default(),
                fail: true,
                changed: ntex_service::ReadyNotifier::new(),
            },
        );
        let res = timeout(Millis(500), disp).await.unwrap();
        assert_eq!(res, Err(()));
        assert_eq!(client.read_any(), Bytes::new());
        assert!(client.is_closed());
    }

    /// STATEMENT: A protocol error from request readiness is delivered to the control service as a stop reason.
    ///
    /// HOW:
    /// - Use a request service whose readiness returns `MqttProtocolError::ReadTimeout` and record protocol control messages.
    /// - Verify the dispatcher completes successfully, records exactly that timeout, and closes the peer.
    #[ntex::test]
    async fn test_protocol_err_in_service_ready() {
        /// Request service reporting a protocol error from readiness.
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

        let (client, server) = create();

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let (disp, _) = Dispatcher::new_debug(
            server,
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

    /// STATEMENT: Readiness failure during stop handling is observed once without replacing the stop result.
    ///
    /// HOW:
    /// - Trigger a decode failure, then have the delayed stop control handler invalidate request readiness and make it fail.
    /// - Verify the dispatcher completes successfully, the stop handler ran, readiness failed once, and the peer is closed.
    #[ntex::test]
    async fn test_err_in_service_ready_during_stop() {
        /// Counts readiness failures after the control handler signals that stopping has begun.
        struct Srv(Rc<Cell<bool>>, Rc<Cell<usize>>, ntex_service::ReadyNotifier);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
                ctx.subscribe(&self.2);
            }

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

        let (client, server) = create();

        let stopped = Rc::new(Cell::new(false));
        let stopped2 = stopped.clone();
        let changed = ntex_service::ReadyNotifier::new();
        let counter = Rc::new(Cell::new(0));
        let (disp, _) = Dispatcher::new_debug(
            server,
            ByteCodec,
            Srv(stopped.clone(), counter.clone(), changed.clone()),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(_)) = msg {
                    stopped2.set(true);
                    changed.notify();
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

    /// STATEMENT: Large responses enable write backpressure and flushing below the watermark disables it.
    ///
    /// HOW:
    /// - Block peer writes, send requests, and produce a 65,536-byte response with a 32 KiB write watermark.
    /// - Verify input consumption and output buffer lengths while progressively allowing peer writes.
    /// - Verify the event log contains one request call followed by backpressure enable and disable notifications.
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
                .set_read_backpressure(8 * 1024)
                .set_write_backpressure(32 * 1024),
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

        ntex_util::spawn(disp);

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

    /// STATEMENT: Peer closure wakes shutdown while a later response handler remains pending.
    ///
    /// HOW:
    /// - Echo a first request immediately, delay the second handler, and verify only `msg1` is received.
    /// - Close the peer and await the dispatcher completion notification.
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

    /// STATEMENT: Complete frames received before the keep-alive deadline keep the connection active.
    ///
    /// HOW:
    /// - Configure a two-second keep-alive and send three echo frames 750 milliseconds apart.
    /// - Verify every response, then verify the connection stays open and no timeout event follows the three handler events.
    #[ntex::test]
    async fn test_keepalive() {
        let (client, server) = create();

        let data = Arc::new(Mutex::new(RefCell::new(Vec::new())));
        let data2 = data.clone();
        let data3 = data.clone();

        let (disp, _) = Dispatcher::new_debug(
            server,
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

    /// Pass-through encoder whose decoder waits for the specified frame length in bytes.
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

    impl FrameState for BytesLenCodec {
        type Response = Bytes;
        type Queued = Bytes;
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

    /// Spawns a 64-byte-frame echo dispatcher and returns its peer, transport, and timeout log.
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
        ntex_util::spawn(disp);
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
        type Response = Bytes;
        type Queued = Bytes;

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
                self.0.set(src[0] == b'p' || src[0] == b'P');
                Ok(Some(src.split_to(1)))
            }
        }
    }

    /// Spawns a chunk-aware dispatcher with no responses and records protocol stop reasons.
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
        ntex_util::spawn(disp);
        (client, state, errs)
    }

    /// STATEMENT: Payload parts do not restart the keep-alive deadline for an incomplete packet.
    ///
    /// HOW:
    /// - Send `p` chunks through `ChunkCodec` with a one-second keep-alive and verify the connection initially remains active.
    /// - Keep sending partial chunks until the dispatcher stops and verify the sole protocol error is `MqttProtocolError::KeepAliveTimeout`.
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

    /// STATEMENT: Finishing a streamed packet lets subsequent complete packets refresh keep-alive.
    ///
    /// HOW:
    /// - Send two partial `p` chunks, finish with `e`, and then send complete `c` packets at 300-millisecond intervals.
    /// - Verify the connection remains active beyond the original deadline with no protocol errors.
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

    /// STATEMENT: The cumulative frame-read deadline bounds a streamed packet even when its byte rate is sufficient.
    ///
    /// HOW:
    /// - Configure one-second rate periods and a two-second budget, then send `ppp` chunks every 400 milliseconds.
    /// - Verify the connection initially remains active but eventually stops with exactly `MqttProtocolError::ReadTimeout`.
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

    /// STATEMENT: A streamed packet below the required byte rate times out without a cumulative budget.
    ///
    /// HOW:
    /// - Configure a rate of two bytes per one-second period and send only one partial `p` chunk.
    /// - Wait for shutdown and verify the only protocol error is `MqttProtocolError::ReadTimeout`.
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

    /// STATEMENT: An incomplete fixed-length frame cannot extend reading beyond its cumulative deadline.
    ///
    /// HOW:
    /// - Use a 64-byte decoder with one-second rate periods and a two-second budget, sending three bytes every 400 milliseconds.
    /// - Verify the connection initially remains active, then stops with `MqttProtocolError::ReadTimeout` despite sufficient byte progress.
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

    /// STATEMENT: Bytes consumed without producing a decoded item still satisfy the frame-read rate.
    ///
    /// HOW:
    /// - Use a codec that clears every input buffer and send three bytes every 400 milliseconds with no cumulative budget.
    /// - Verify the connection stays active while bytes arrive, then stop sending and verify a single `MqttProtocolError::ReadTimeout`.
    #[ntex::test]
    async fn test_read_rate_codec_consumed() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        /// Consumes every input byte without completing a frame, exercising rate accounting.
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

        impl FrameState for ConsumingCodec {
            type Response = Bytes;
            type Queued = Bytes;
        }

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
        ntex_util::spawn(disp);

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

    /// STATEMENT: Receiving bytes of an incomplete frame does not restart keep-alive.
    ///
    /// HOW:
    /// - Use a 64-byte decoder with a one-second keep-alive and send one byte every 300 milliseconds.
    /// - Verify the connection initially remains active, then stops with exactly `MqttProtocolError::KeepAliveTimeout`.
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

    /// STATEMENT: Disabled keep-alive is not armed after a partially received frame completes.
    ///
    /// HOW:
    /// - Disable keep-alive, enable frame-rate tracking, and complete a two-byte frame in two writes.
    /// - Verify the echoed bytes and, after two idle seconds, verify the log contains only the successful handler event.
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
        ntex_util::spawn(disp);

        client.write("1");
        sleep(Millis(250)).await;
        client.write("2");
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"12"));
        sleep(Millis(2000)).await;

        assert_eq!(&data.lock().unwrap().borrow()[..], &[0]);
    }

    /// STATEMENT: A service-induced read pause restores the cumulative frame-read budget.
    ///
    /// HOW:
    /// - Begin a 16-byte frame, consume one rate period, then invalidate service readiness and pause it with a condition.
    /// - Resume readiness, send the remaining bytes across further periods, and verify the connection remains active without errors.
    /// - Complete the frame and verify the full 16-byte echo.
    #[ntex::test]
    async fn test_read_rate_reset_on_pause() {
        /// Echo service whose readiness can be paused through a shared flag and condition.
        struct Srv(Rc<Cell<bool>>, Condition<()>, ntex_service::ReadyNotifier);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
                ctx.subscribe(&self.2);
            }

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
        let changed = ntex_service::ReadyNotifier::new();
        let cond = Condition::new();
        let config = SharedCfg::new("DBG").add(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
        );
        let (disp, state) = Dispatcher::new_debug(
            nio::Io::new(server, config),
            BytesLenCodec(16),
            Srv(paused.clone(), cond.clone(), changed.clone()),
            fn_service(async move |msg: Control<()>| {
                if let Control::Stop(Reason::Protocol(err)) = msg {
                    errs2.borrow_mut().push(*err.get_ref());
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(disp);

        // first period is extended, one second of the budget is left
        client.write("123");
        sleep(Millis(1200)).await;
        client.write("456");
        sleep(Millis(100)).await;

        // pause restarts the budget
        paused.set(true);
        changed.notify();
        client.write("7");
        sleep(Millis(300)).await;
        paused.set(false);
        changed.notify();
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

    /// STATEMENT: A complete frame succeeds before a later slow partial frame triggers read timeout.
    ///
    /// HOW:
    /// - Echo one complete eight-byte frame, then send an incomplete frame in progressively delayed pieces under a frame-rate deadline.
    /// - Verify the connection is active during initial progress, then closes with one handler event followed by one read-timeout event.
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
        ntex_util::spawn(disp);

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

    /// STATEMENT: Stopping during an incomplete frame releases dispatcher-owned service state.
    ///
    /// HOW:
    /// - Disable keep-alive, use a two-byte decoder, send only one byte, and close the peer.
    /// - Wait briefly and verify the service lifetime probe was dropped.
    #[ntex::test]
    async fn cancel_on_stop() {
        /// Marks a shared flag when dispatcher-owned service state is released.
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
        ntex_util::spawn(disp);

        client.write("1");
        client.close().await;
        sleep(Millis(250)).await;

        assert!(&data.load(Ordering::Relaxed));
    }

    /// One-byte decoder with decode failure on `E`, encode failure on `X`, and unordered `q`.
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
        type Response = Bytes;
        type Queued = Bytes;

        fn is_ordered(&self, item: &Bytes) -> bool {
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

    /// Marks cancellation or completion of a monitored pending service call.
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

    /// STATEMENT: A spawned response encoding failure stops the dispatcher while later calls are still pending.
    ///
    /// HOW:
    /// - Send `aXc` with delayed handlers and a codec that cannot encode `X`.
    /// - Verify shutdown reports `EncodeError::MalformedPacket` and the earlier `a` response was still delivered.
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
        ntex_util::spawn(disp);

        client.write("aXc");
        sleep(Millis(300)).await;
        assert!(!state.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::Encode(EncodeError::MalformedPacket)]
        ));
        assert_eq!(client.read().await.unwrap(), Bytes::from_static(b"a"));
    }

    /// STATEMENT: The first completed request error remains the sole reported stop reason.
    ///
    /// HOW:
    /// - Keep the first request pending, then make a service error and a protocol error complete after equal delays.
    /// - Verify the dispatcher stops and the control log records only the first service-error reason.
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
        ntex_util::spawn(disp);

        client.write("abc");
        sleep(Millis(300)).await;
        assert!(!state.is_active());
        assert_eq!(&msgs.borrow()[..], &[true]);
    }

    /// STATEMENT: A pending request error is reported without waiting for blocked output to flush.
    ///
    /// HOW:
    /// - Keep an ordered request pending and let an unordered response fill the write buffer while the peer accepts no bytes.
    /// - Verify backpressure is enabled, then release the failing call and verify the stop-error message follows immediately with no peer output.
    #[ntex::test]
    async fn error_during_write_backpressure() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let msgs = Rc::new(RefCell::new(Vec::new()));
        let msgs2 = msgs.clone();
        let condition = Condition::new();
        let waiter = condition.wait();
        let (disp, _io) = Dispatcher::new_debug(
            nio::Io::new(
                server,
                SharedCfg::new("DBG").add(IoConfig::new().set_write_backpressure(1024)),
            ),
            ByteCodec,
            fn_service(async move |msg: Bytes| match &msg[..] {
                b"b" => {
                    waiter.clone().await;
                    Err(DispatcherError::Service(()))
                }
                _ => Ok(Some(Bytes::from(vec![b'x'; 64 * 1024]))),
            }),
            fn_service(async move |msg: Control<()>| {
                match msg {
                    Control::WrBackpressure(st) => msgs2.borrow_mut().push(if st.enabled() {
                        "wr-enabled"
                    } else {
                        "wr-disabled"
                    }),
                    Control::Stop(Reason::Error(_)) => msgs2.borrow_mut().push("stop-error"),
                    Control::Stop(_) => msgs2.borrow_mut().push("stop"),
                }
                Ok::<_, ()>(None)
            }),
        );
        ntex_util::spawn(disp);

        // the unordered response of "q" is written while "b" is pending
        client.write("bq");
        for _ in 0..100 {
            if !msgs.borrow().is_empty() {
                break;
            }
            sleep(Millis(10)).await;
        }
        assert_eq!(&msgs.borrow()[..], &["wr-enabled"]);

        // the peer does not read, the error is not delayed by backpressure
        condition.notify(());
        for _ in 0..100 {
            if msgs.borrow().len() > 1 {
                break;
            }
            sleep(Millis(10)).await;
        }
        assert_eq!(&msgs.borrow()[..], &["wr-enabled", "stop-error"]);
        assert!(client.read_any().is_empty());
    }

    /// STATEMENT: Shutdown cancels spawned calls even when stopping precedes their first task poll.
    ///
    /// HOW:
    /// - Block peer writes and send `w12E` so decoding, spawning pending handlers, and the decode error occur in one dispatcher poll.
    /// - Verify the monitored spawned handler is dropped while queued output still delays transport closure.
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
        ntex_util::spawn(disp);
        sleep(Millis(50)).await;
        assert!(!client.is_closed());
        assert!(dropped.get());
    }

    /// STATEMENT: Dropping the dispatcher cancels its independently spawned request calls.
    ///
    /// HOW:
    /// - Block peer writes, send `w12`, and manually poll the dispatcher once to spawn pending handlers.
    /// - Verify the monitored handler is initially alive, then drop the dispatcher and verify its drop probe is set.
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

    /// STATEMENT: Peer closure during a service read pause releases both active-call and service state.
    ///
    /// HOW:
    /// - Start a pending handler that invalidates readiness and makes further readiness wait indefinitely.
    /// - Close the peer, await dispatcher completion, and verify both lifetime probe clones were dropped.
    #[ntex::test]
    async fn peer_gone_while_service_is_not_ready() {
        /// Counts drops of the service and its active-call probe clones.
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

        /// Starts a pending call and invalidates readiness so disconnect occurs during a read pause.
        struct Srv(Cell<bool>, OnDrop, ntex_service::ReadyNotifier);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
                ctx.subscribe(&self.2);
            }

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
                self.2.notify();
                sleep(Millis(999_999)).await;
                Ok(None)
            }
        }

        let (client, server) = create();

        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            Srv(Cell::new(false), data2, ntex_service::ReadyNotifier::new()),
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

    /// Creates a 65,536-byte response service and a log of backpressure and write-timeout events.
    ///
    /// If `ready` is false, the first request also makes service readiness pending.
    fn write_timeout_srv(
        ready: bool,
    ) -> (
        impl Service<(), Bytes, Res = Option<Bytes>, Error = DispatcherError<()>>,
        impl Service<(), Control<()>, Res = Option<Bytes>, Error = ()>,
        Rc<RefCell<Vec<u8>>>,
    ) {
        /// Emits a large response and optionally blocks readiness after its first call.
        struct Srv(bool, Cell<bool>, ntex_service::ReadyNotifier);

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
                ctx.subscribe(&self.2);
            }

            async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), Self::Error> {
                if !self.0 && self.1.get() {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }

            async fn call(&self, _: Bytes, _: Ctx<'_, Self, ()>) -> Result<Self::Res, Self::Error> {
                self.1.set(true);
                self.2.notify();
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
        (
            Srv(ready, Cell::new(false), ntex_service::ReadyNotifier::new()),
            control,
            data,
        )
    }

    /// Configures 32 KiB write backpressure and a one-second write deadline with no keep-alive.
    fn write_timeout_cfg() -> SharedCfg {
        SharedCfg::new("DBG")
            .add(
                IoConfig::new()
                    .set_keepalive_timeout(Seconds::ZERO)
                    .set_write_backpressure(32 * 1024)
                    .set_write_timeout(Seconds(1)),
            )
            .into()
    }

    /// STATEMENT: A peer that does not accept a backpressured response triggers write timeout.
    ///
    /// HOW:
    /// - Block peer writes, configure a one-second write deadline, and emit a 65,536-byte response.
    /// - Verify the connection starts active with backpressure enabled, then stops with a write-timeout event and no disable event.
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
        ntex_util::spawn(disp);

        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(500)).await;
        assert!(state.is_active());
        assert_eq!(&data.borrow()[..], &[1]);

        sleep(Millis(2000)).await;
        assert!(!state.is_active());
        assert_eq!(&data.borrow()[..], &[1, 3]);
    }

    /// STATEMENT: Releasing write backpressure before its deadline cancels the write timer.
    ///
    /// HOW:
    /// - Block a large response until backpressure is enabled, then allow the peer to accept the entire response.
    /// - Wait beyond the original deadline and verify the connection remains active, all 65,536 bytes arrive, and only enable/disable events are recorded.
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
        ntex_util::spawn(disp);

        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(500)).await;
        assert_eq!(&data.borrow()[..], &[1]);

        client.remote_buffer_cap(1024 * 1024);
        sleep(Millis(2000)).await;
        assert!(state.is_active());
        assert_eq!(&data.borrow()[..], &[1, 2]);
        assert_eq!(client.read_any().len(), 65_536);
    }

    /// STATEMENT: Write timeout remains effective while request readiness is blocked.
    ///
    /// HOW:
    /// - Use `write_timeout_srv(false)` to emit a large response and make request readiness pending while peer writes are blocked.
    /// - Verify the connection initially remains active, then stops and records a write-timeout event.
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
        ntex_util::spawn(disp);

        client.write("GET /test HTTP/1\r\n\r\n");
        sleep(Millis(500)).await;
        assert!(state.is_active());

        sleep(Millis(2000)).await;
        assert!(!state.is_active());
        assert!(data.borrow().contains(&3));
    }

    /// STATEMENT: Backpressure release cancels write timeout even while request readiness remains pending.
    ///
    /// HOW:
    /// - Produce a blocked large response with a service that becomes not ready and verify backpressure is enabled.
    /// - Allow all bytes to flush, wait beyond the deadline, and verify the connection stays active with only enable/disable events.
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
        ntex_util::spawn(disp);

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

    /// STATEMENT: A full ordered response queue pauses decoding until its head completes.
    ///
    /// HOW:
    /// - Set a four-entry limit, block the first handler, and send ten one-byte requests.
    /// - Verify exactly four handlers start and no response is written, then release the head and verify all ten ordered responses arrive.
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
        ntex_util::spawn(disp);

        client.write("0123456789");
        sleep(Millis(50)).await;
        assert_eq!(calls.get(), 4);
        assert!(client.read_any().is_empty());

        let _ = tx.send(());
        sleep(Millis(50)).await;
        assert_eq!(calls.get(), 10);
        assert_eq!(client.read_any(), Bytes::from_static(b"0123456789"));
    }

    /// STATEMENT: Completed unordered calls leave no response-queue slots behind.
    ///
    /// HOW:
    /// - Use a two-entry limit, send `qaqqbc`, delay unordered calls briefly, and block the first ordered response.
    /// - Verify unordered calls finish while `a` and `b` fill the queue, leaving `c` unread.
    /// - Release the ordered head and verify `abc` arrives with all six requests dispatched.
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
        ntex_util::spawn(disp);

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

    /// STATEMENT: An immediately completed unordered response is written ahead of pending ordered responses.
    ///
    /// HOW:
    /// - Dispatch `a` and gated `b`, read `a`, and verify one ordered response slot remains.
    /// - Dispatch unordered `q` and verify its response arrives without changing queue length; release the gate and read `b`.
    #[ntex::test]
    async fn unordered_ready_response_skips_queue() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let gate = Condition::new();
        let gate2 = gate.clone();
        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(server, max_queue_cfg(0)),
            ByteCodec,
            fn_service(move |msg: Bytes| {
                let gate = gate2.clone();
                async move {
                    if msg == "b" {
                        let _ = gate.wait().await;
                    } else if msg == "a" {
                        sleep(Millis(10)).await;
                    }
                    Ok::<_, DispatcherError<()>>(Some(msg))
                }
            }),
            ctl_srv(),
        );
        let state = disp.inner.state.clone();
        let _done = spawn_disp(disp);

        // "a" is polled by the dispatcher, "b" is spawned and keeps the queue
        client.write("ab");
        assert_eq!(read_exact(&client, 1).await, b"a"[..]);
        assert_eq!(state.queue.borrow().len(), 1);

        // "q" completes when it is called
        client.write("q");
        assert_eq!(read_exact(&client, 1).await, b"q"[..]);
        assert_eq!(state.queue.borrow().len(), 1);

        gate.notify_and_lock(());
        assert_eq!(read_exact(&client, 1).await, b"b"[..]);
    }

    /// STATEMENT: Pending unordered calls count toward the dispatcher's combined queue limit.
    ///
    /// HOW:
    /// - Set a three-entry limit, keep ordered `a` pending, and send four gated unordered `q` requests.
    /// - Verify only three calls initially start, then release the gate and verify the remaining two calls are dispatched.
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
        ntex_util::spawn(disp);

        client.write("aqqqq");
        assert!(wait_until(|| calls.get() == 3).await);
        sleep(Millis(100)).await;
        assert_eq!(calls.get(), 3);

        // completed calls release the limit
        gate.notify_and_lock(());
        assert!(wait_until(|| calls.get() == 5).await);
    }

    /// Records calls, the calls wait for the gate
    fn gate_srv(
        calls: Rc<RefCell<Vec<Bytes>>>,
        gate: Condition,
    ) -> impl Service<(), Bytes, Res = Option<Bytes>, Error = DispatcherError<()>> {
        fn_service(move |msg: Bytes| {
            let calls = calls.clone();
            let gate = gate.clone();
            async move {
                calls.borrow_mut().push(msg.clone());
                let _ = gate.wait().await;
                Ok::<_, DispatcherError<()>>(Some(msg))
            }
        })
    }

    /// Creates transport configuration with the requested pending-response limit.
    fn max_queue_cfg(max_queue: usize) -> SharedCfg {
        SharedCfg::new("DBG")
            .add(IoConfig::new())
            .add(MqttServiceConfig::new().set_max_queue(max_queue))
            .into()
    }

    /// STATEMENT: A full queue permits the remainder of a streamed frame but not the next frame.
    ///
    /// HOW:
    /// - With a two-entry limit and gated handlers, compare `apcd` with `abpc` using `ChunkCodec`.
    /// - Verify decoding continues through the partial frame only when it has already started.
    /// - Release the gate and verify all remaining requests and responses follow input order.
    #[ntex::test]
    async fn frame_rest_read_while_queue_is_full() {
        // data, dispatched items
        for (data, dispatched) in [("apcd", "apc"), ("abpc", "ab")] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let calls = Rc::new(RefCell::new(Vec::new()));
            let gate = Condition::new();
            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, max_queue_cfg(2)),
                ChunkCodec::default(),
                gate_srv(calls.clone(), gate.clone()),
                ctl_srv(),
            );
            let _done = spawn_disp(disp);

            client.write(data);
            sleep(Millis(50)).await;
            let called: Vec<u8> = calls.borrow().iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(&called[..], dispatched.as_bytes(), "{data}");

            // the rest is dispatched in order once the queue has room
            gate.notify_and_lock(());
            assert_eq!(
                read_exact(&client, data.len()).await,
                data.as_bytes(),
                "{data}"
            );
            let called: Vec<u8> = calls.borrow().iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(&called[..], data.as_bytes(), "{data}");
        }
    }

    /// STATEMENT: An emptied response queue releases large burst capacity but preserves small allocations.
    ///
    /// HOW:
    /// - Queue gated responses in bursts above `QUEUE_SHRINK_CAP` and below it, recording the allocated capacity.
    /// - Release all handlers, verify every response and an empty queue, then verify capacity becomes zero only for the large burst.
    #[ntex::test]
    async fn queue_shrinks_after_burst() {
        for (count, shrinks) in [(QUEUE_SHRINK_CAP + 8, true), (8, false)] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let calls = Rc::new(RefCell::new(Vec::new()));
            let gate = Condition::new();
            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, max_queue_cfg(0)),
                ByteCodec,
                gate_srv(calls.clone(), gate.clone()),
                ctl_srv(),
            );
            let state = disp.inner.state.clone();
            let _done = spawn_disp(disp);

            let data = "a".repeat(count);
            client.write(data.as_str());
            sleep(Millis(50)).await;
            assert_eq!(state.queue.borrow().len(), count);
            let cap = state.queue.borrow().capacity();
            assert!(cap >= count, "{count}");

            gate.notify_and_lock(());
            assert_eq!(read_exact(&client, count).await, data.as_bytes());
            assert!(state.queue.borrow().is_empty());
            let expected = if shrinks { 0 } else { cap };
            assert_eq!(state.queue.borrow().capacity(), expected, "{count}");
        }
    }

    /// STATEMENT: Buffered requests survive clean read EOF while a full queue waits for capacity.
    ///
    /// HOW:
    /// - Send three or four one-byte requests with a two-entry limit and gated handlers, closing the peer immediately or after a delay.
    /// - Verify only two handlers initially start and shutdown remains pending.
    /// - Release the gate, await shutdown, and verify every buffered request was dispatched.
    #[ntex::test]
    async fn queue_full_after_read_eof() {
        // one or two items after the full queue, eof with the data or later
        for (data, delay) in [("abc", 0), ("abcd", 0), ("abc", 20), ("abcd", 20)] {
            let (client, server) = Io::create();
            client.remote_buffer_cap(1024);

            let calls = Rc::new(RefCell::new(Vec::new()));
            let gate = Condition::new();
            let (disp, _) = Dispatcher::new_debug(
                nio::Io::new(server, max_queue_cfg(2)),
                ByteCodec,
                gate_srv(calls.clone(), gate.clone()),
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
        /// Applies a peer half-close, read reset, or immediate transport termination.
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

    /// Spawns the dispatcher and returns a flag set after its future completes.
    fn spawn_disp<F: Future + 'static>(disp: F) -> Rc<Cell<bool>> {
        let done = Rc::new(Cell::new(false));
        let done2 = done.clone();
        ntex_util::spawn(async move {
            let _ = disp.await;
            done2.set(true);
        });
        done
    }

    /// Control service accepting every message without emitting a response.
    fn ctl_srv() -> impl Service<(), Control<()>, Res = Option<Bytes>, Error = ()> {
        fn_service(async move |_: Control<()>| Ok::<_, ()>(None))
    }

    /// Waits up to two seconds for dispatcher completion, panicking with the case on timeout.
    async fn assert_stops(case: &str, done: &Cell<bool>) {
        for _ in 0..40 {
            if done.get() {
                return;
            }
            sleep(Millis(50)).await;
        }
        panic!("{case}: dispatcher did not stop after disconnect");
    }

    /// STATEMENT: Idle and partially reading dispatchers stop on clean closure, read reset, or transport termination.
    ///
    /// HOW:
    /// - For each `Disconnect` mode, send zero, one, or two bytes to a four-byte decoder and verify the dispatcher is initially pending.
    /// - Apply the disconnection and verify dispatcher completion within the helper's timeout.
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

    /// STATEMENT: Disconnect cancels pending request handlers before the dispatcher completes.
    ///
    /// HOW:
    /// - Start three pending one-byte handlers with a monitored spawned call for each `Disconnect` mode.
    /// - Apply disconnection, await completion, and verify the handler-drop probe is set.
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

    /// STATEMENT: A backend-reported transport failure stops a dispatcher paused by a full response queue.
    ///
    /// HOW:
    /// - Set a two-entry queue limit, send five bytes to pending handlers, and allow the queue to fill.
    /// - Apply `Disconnect::Terminate` and verify dispatcher completion within the helper's timeout.
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

    /// STATEMENT: A backend-reported transport failure stops a dispatcher whose request service is not ready.
    ///
    /// HOW:
    /// - Use a service that becomes not ready after its first response, testing one- and two-byte inputs.
    /// - Verify the dispatcher is still pending, apply `Disconnect::Terminate`, and await completion.
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

    /// STATEMENT: Backend-reported transport failure stops write-backpressured dispatchers regardless of request readiness.
    ///
    /// HOW:
    /// - Block peer writes and emit a large response with services that either remain ready or become not ready.
    /// - Verify backpressure is enabled, apply `Disconnect::Terminate`, and await dispatcher completion in both cases.
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
                        SharedCfg::new("DBG")
                            .add(IoConfig::new().set_write_backpressure(32 * 1024)),
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

    /// STATEMENT: Output flush and shutdown can progress when a request simultaneously invalidates readiness and enables backpressure.
    ///
    /// HOW:
    /// - Use a one-shot-gated readiness service, a two-byte write watermark, and a peer initially accepting no bytes.
    /// - Send nine bytes, enable peer writes, and verify the entire echo arrives.
    /// - Close the peer, release readiness, and await dispatcher completion.
    #[ntex::test]
    async fn service_is_not_ready_and_backpressure() {
        let (ctx, rx) = oneshot::channel();

        /// Echo service whose first call blocks readiness until a one-shot gate is released.
        struct Srv(
            Cell<bool>,
            Cell<Option<oneshot::Receiver<()>>>,
            ntex_service::ReadyNotifier,
        );

        impl Service<(), Bytes> for Srv {
            type Res = Option<Bytes>;
            type Error = DispatcherError<()>;

            fn bind(&self, ctx: ntex_service::BindCtx<'_>) {
                ctx.subscribe(&self.2);
            }

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
                self.2.notify();
                Ok(Some(msg))
            }
        }

        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let (disp, _) = Dispatcher::new_debug(
            nio::Io::new(
                server,
                SharedCfg::new("DBG").add(IoConfig::new().set_write_backpressure(2)),
            ),
            BytesCodec,
            Srv(
                Cell::new(false),
                Cell::new(Some(rx)),
                ntex_service::ReadyNotifier::new(),
            ),
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
        /// Ordered record of started and completed backpressure calls and stop messages.
        log: Rc<RefCell<Vec<&'static str>>>,
        /// `wr(true)` handling waits for the gate
        gate: Option<Condition<()>>,
        /// response for `wr(false)`
        resp: Option<Bytes>,
        /// `wr(false)` fails
        fail: bool,
    }

    impl WrCtl {
        /// Creates a logging control service with optional gating of enabled-backpressure handling.
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

    /// Starts a large-response dispatcher with a blocked peer and configurable write deadline.
    fn wr_dispatcher(
        ctl: WrCtl,
        write_timeout: Seconds,
    ) -> (Io, nio::IoRef, oneshot::Receiver<Result<(), ()>>) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);

        let cfg = SharedCfg::new("DBG").add(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_write_backpressure(32 * 1024)
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

    /// STATEMENT: Disabled-backpressure control messages wait for enabled-backpressure handling to complete.
    ///
    /// HOW:
    /// - Gate the `wr(true)` control call, then flush the response to release backpressure.
    /// - Verify only the enabled message is logged while its handler waits, then open the gate and verify enable completion precedes `wr(false)`.
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

    /// STATEMENT: Stop control handling waits for a pending write-backpressure control call.
    ///
    /// HOW:
    /// - Gate `wr(true)` and let a one-second write timeout expire while peer writes remain blocked.
    /// - Verify no stop message is logged until the gate opens, then verify enabled handling completes before the stop message.
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

    /// STATEMENT: Responses returned by write-backpressure control handlers are encoded onto the connection.
    ///
    /// HOW:
    /// - Configure a `wr(false)` response containing `wr`, then unblock the peer after backpressure is enabled.
    /// - Read 65,538 bytes, verify the output ends with `wr`, and verify enabled handling completed before the disabled message.
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

    /// STATEMENT: A write-backpressure control error returns directly without sending a stop message.
    ///
    /// HOW:
    /// - Make `wr(false)` fail after a large response enables backpressure, then allow the peer to drain output.
    /// - Verify the dispatcher returns `Err(())` and the control log contains enable, enable completion, and disable but no stop.
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

    /// Readiness gate for one filter direction, retaining the latest task to wake on changes.
    #[derive(Debug, Default)]
    struct GateDir {
        /// Whether successful underlying readiness is converted to pending.
        blocked: Cell<bool>,
        /// Latest task polling the gate, consumed when its state changes.
        waker: RefCell<Option<std::task::Waker>>,
    }

    impl GateDir {
        /// Registers the task and delays successful readiness while blocked, preserving errors.
        fn poll(&self, res: Poll<io::Result<()>>, cx: &Context<'_>) -> Poll<io::Result<()>> {
            *self.waker.borrow_mut() = Some(cx.waker().clone());
            match res {
                Poll::Ready(Ok(())) if self.blocked.get() => Poll::Pending,
                res => res,
            }
        }

        /// Changes the gate and wakes its most recently registered polling task.
        fn set(&self, blocked: bool) {
            self.blocked.set(blocked);
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    /// Gate control: pause input or hold output in the filter.
    #[derive(Debug, Clone, Default)]
    struct GateCtl(Rc<(GateDir, GateDir)>);

    impl GateCtl {
        /// Changes filter readiness, pausing or resuming inbound delivery.
        fn block_read(&self, blocked: bool) {
            self.0.0.set(blocked);
        }

        /// Holds or releases outbound bytes in the filter layer and wakes its waiter.
        fn block_write(&self, blocked: bool) {
            self.0.1.set(blocked);
        }
    }

    /// Filter wrapper applying the read gate to the underlying filter's readiness.
    struct GateFilter<F>(F, GateCtl);

    /// Pass-through filter layer that retains outbound bytes while its write gate is blocked.
    #[derive(Debug)]
    struct GateLayer(GateCtl);

    impl nio::FilterLayer for GateLayer {
        fn process_read_buf(&self, buf: &nio::FilterBuf<'_>) -> io::Result<()> {
            buf.with_read_buffers(|src, dst| {
                if let Some(src) = src {
                    dst.extend_from_slice(src);
                    src.clear();
                }
            });
            Ok(())
        }

        fn process_write_buf(&self, buf: &nio::FilterBuf<'_>) -> io::Result<()> {
            if !self.0.0.1.blocked.get() {
                buf.with_write_buffers(ntex_bytes::BytePages::move_to);
            }
            Ok(())
        }

        fn shutdown(&self, _: &nio::FilterBuf<'_>) -> io::Result<Poll<()>> {
            Ok(Poll::Ready(()))
        }
    }

    impl<F: nio::Filter> nio::Filter for GateFilter<F> {
        fn query(&self, id: std::any::TypeId) -> Option<Box<dyn std::any::Any>> {
            self.0.query(id)
        }

        fn process_read_buf(&self, ctx: &mut nio::FilterCtx<'_>) -> std::io::Result<()> {
            self.0.process_read_buf(ctx)
        }

        fn process_write_buf(&self, ctx: &mut nio::FilterCtx<'_>) -> std::io::Result<()> {
            self.0.process_write_buf(ctx)
        }

        fn shutdown(&self, ctx: &mut nio::FilterCtx<'_>) -> std::io::Result<Poll<()>> {
            self.0.shutdown(ctx)
        }

        fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.1.0.0.poll(self.0.poll_ready(cx), cx)
        }
    }

    /// Spawns a dispatcher over a gated filter chain
    fn spawn_gated<U, S, C>(
        server: Io,
        cfg: SharedCfg,
        codec: U,
        srv: S,
        control: C,
    ) -> (GateCtl, nio::IoRef)
    where
        U: Decoder<Error = DecodeError>
            + Encoder<Error = EncodeError>
            + FrameState
            + Clone
            + 'static,
        S: Service<(), Request<U>, Res = Option<Response<U>>, Error = DispatcherError<()>>
            + 'static,
        C: Service<(), Control<()>, Res = Option<ControlResponse<U>>, Error = ()> + 'static,
    {
        let gate = GateCtl::default();
        let g = gate.clone();
        let io = nio::Io::new(server, cfg)
            .add_filter(GateLayer(gate.clone()))
            .map_filter(move |f| GateFilter(f, g));
        let ioref = io.get_ref();
        let disp = Dispatcher::new(
            IoBoxed::from(io),
            codec,
            Pipeline::new((), srv),
            Pipeline::new((), control),
        );
        ntex_util::spawn(disp);
        (gate, ioref)
    }

    /// Echo dispatcher of 8 byte frames over a gated filter chain
    fn gated_dispatcher(
        cfg: IoConfig,
    ) -> (Io, GateCtl, nio::IoRef, Rc<RefCell<Vec<MqttProtocolError>>>) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);

        let errs = Rc::new(RefCell::new(Vec::new()));
        let errs2 = errs.clone();
        let srv = fn_service(async move |msg: Bytes| Ok::<_, DispatcherError<()>>(Some(msg)));
        let control = fn_service(async move |msg: Control<()>| {
            if let Control::Stop(Reason::Protocol(err)) = msg {
                errs2.borrow_mut().push(*err.get_ref());
            }
            Ok::<_, ()>(None)
        });
        let cfg = SharedCfg::new("DBG").add(cfg).into();
        let (gate, io) = spawn_gated(server, cfg, BytesLenCodec(8), srv, control);
        (client, gate, io, errs)
    }

    /// STATEMENT: Filter-induced read pauses suspend the frame-read deadline without charging the peer.
    ///
    /// HOW:
    /// - Begin an eight-byte frame under a frame-rate deadline, block filter readiness, and send its remaining bytes.
    /// - Wait beyond the cumulative budget and verify the connection is active with no errors.
    /// - Unblock the filter and verify the full echo arrives and the filter is no longer paused.
    #[ntex::test]
    async fn test_filter_pause_suspends_frame_read_timer() {
        let (client, gate, io, errs) = gated_dispatcher(
            IoConfig::new()
                .set_keepalive_timeout(Seconds::ZERO)
                .set_frame_read_rate(Seconds(1), Seconds(2), 2),
        );

        // a partial frame arms the frame read timer
        client.write("1234");
        sleep(Millis(100)).await;

        // the peer sends the rest of the frame while the filter is not ready
        gate.block_read(true);
        client.write("5678");
        sleep(Millis(100)).await;
        assert!(io.is_filter_paused());

        sleep(Millis(3000)).await;
        assert!(io.is_active());
        assert!(errs.borrow().is_empty());

        gate.block_read(false);
        assert_eq!(&read_exact(&client, 8).await[..], b"12345678");
        assert!(!io.is_filter_paused());
        assert!(errs.borrow().is_empty());
    }

    /// STATEMENT: Keep-alive is suspended during filter pauses and resumes after input delivery.
    ///
    /// HOW:
    /// - Block filter readiness before sending a complete frame and wait beyond a one-second keep-alive.
    /// - Verify the connection remains active without errors, then unblock and verify the echo.
    /// - Leave the resumed connection idle and verify it stops with exactly `MqttProtocolError::KeepAliveTimeout`.
    #[ntex::test]
    async fn test_filter_pause_suspends_keepalive() {
        let (client, gate, io, errs) =
            gated_dispatcher(IoConfig::new().set_keepalive_timeout(Seconds(1)));

        // the peer sends a frame while the filter is not ready
        sleep(Millis(100)).await;
        gate.block_read(true);
        client.write("12345678");
        sleep(Millis(100)).await;
        assert!(io.is_filter_paused());

        sleep(Millis(2000)).await;
        assert!(io.is_active());
        assert!(errs.borrow().is_empty());

        gate.block_read(false);
        assert_eq!(&read_exact(&client, 8).await[..], b"12345678");

        // keep-alive is armed again once reads resume
        sleep(Millis(2000)).await;
        assert!(!io.is_active());
        assert!(matches!(
            &errs.borrow()[..],
            [MqttProtocolError::KeepAliveTimeout]
        ));
    }

    /// `write_timeout_srv` over a gated filter chain
    fn gated_write_dispatcher(
        client_cap: usize,
    ) -> (Io, GateCtl, nio::IoRef, Rc<RefCell<Vec<u8>>>) {
        let (client, server) = Io::create();
        client.remote_buffer_cap(client_cap);
        let (srv, control, data) = write_timeout_srv(true);
        let (gate, io) = spawn_gated(server, write_timeout_cfg(), BytesCodec, srv, control);
        (client, gate, io, data)
    }

    /// STATEMENT: Filter-induced pauses suspend write timeout until retained output can be released.
    ///
    /// HOW:
    /// - Hold a large response in the filter, then block filter readiness and verify backpressure is enabled.
    /// - Wait beyond the write deadline and verify no timeout, then release both gates.
    /// - Verify all 65,536 bytes flush, the filter resumes, and the log contains only enable/disable events.
    #[ntex::test]
    async fn test_filter_pause_suspends_write_timeout() {
        let (client, gate, io, data) = gated_write_dispatcher(1024 * 1024);

        // the response waits for the filter, write backpressure is enabled
        gate.block_write(true);
        client.write("1");
        sleep(Millis(100)).await;
        gate.block_read(true);
        sleep(Millis(100)).await;
        assert!(io.is_filter_paused());
        assert_eq!(&data.borrow()[..], &[1]);

        sleep(Millis(2500)).await;
        assert!(io.is_active());
        assert_eq!(&data.borrow()[..], &[1]);

        gate.block_write(false);
        gate.block_read(false);
        assert_eq!(read_exact(&client, 65_536).await.len(), 65_536);
        assert!(!io.is_filter_paused());
        assert!(wait_until(|| data.borrow().len() == 2).await);
        assert_eq!(&data.borrow()[..], &[1, 2]);
    }

    /// STATEMENT: Write timeout receives a fresh deadline after a filter pause ends.
    ///
    /// HOW:
    /// - Block peer writes, retain a large response in the filter, and pause filter readiness longer than the write deadline.
    /// - Verify the connection remains active while paused, then release both filter gates without unblocking the peer.
    /// - Verify it initially remains active after resuming, then stops with backpressure enable followed by write timeout.
    #[ntex::test]
    async fn test_write_timeout_restarts_after_filter_pause() {
        let (client, gate, io, data) = gated_write_dispatcher(0);

        gate.block_write(true);
        client.write("1");
        sleep(Millis(100)).await;
        gate.block_read(true);
        sleep(Millis(1500)).await;
        assert!(io.is_filter_paused());
        assert!(io.is_active());

        // the peer does not read, the timeout starts over once writes resume
        gate.block_write(false);
        gate.block_read(false);
        sleep(Millis(500)).await;
        assert!(!io.is_filter_paused());
        assert!(io.is_active());
        assert!(wait_until(|| !io.is_active()).await);
        assert_eq!(&data.borrow()[..], &[1, 3]);
    }
}
