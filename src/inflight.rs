//! Service that limits number of in-flight async requests.
use std::task::{Context, Poll, Waker};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, fmt, future::poll_fn, rc::Rc};

use ntex_service::{BindCtx, Ctx, ReadyBinding, ReadyNotifier, Service};
use ntex_util::{future::OnDropFn, future::join, task::LocalWaker};

/// Request metadata for byte accounting, slot limiting, and payload ordering.
pub trait SizedRequest {
    /// Returns the request size in bytes for aggregate in-flight accounting.
    fn size(&self) -> u32;

    /// Returns whether more chunks of this streaming publish follow.
    ///
    /// For a limited request, aggregate byte readiness is temporarily bypassed
    /// so remaining chunks can reach its handler. Request-slot acquisition and
    /// the wrapped service's own readiness still apply.
    fn has_more_chunks(&self) -> bool;

    /// Returns whether this request ends the current streamed payload.
    ///
    /// Receiving the last chunk ends the current streaming byte-limit exemption
    /// before dispatching that chunk. The default `false` leaves the exemption
    /// active until the streaming publish call completes or is cancelled.
    fn is_last_chunk(&self) -> bool {
        false
    }

    /// Returns whether this request must acquire an in-flight slot.
    ///
    /// A request without a free slot waits for one, requests that do not take
    /// a slot are not limited by the number of in-flight requests.
    fn is_limited(&self) -> bool {
        true
    }

    /// Returns whether this request waits behind earlier slot waiters.
    ///
    /// An ordered request takes its turn after earlier waiting requests have
    /// been admitted, without waiting for their handlers to complete. It only
    /// acquires a slot when [`is_limited`](Self::is_limited) also returns `true`.
    fn is_ordered(&self) -> bool {
        false
    }
}

/// Service wrapper enforcing request-slot and aggregate byte limits.
///
/// Limited requests acquire slots in arrival order. Unordered, unlimited requests
/// can bypass slot waiters; streamed payload chunks can bypass byte readiness
/// until the last chunk arrives.
pub struct InFlightServiceImpl<S> {
    /// Shared byte accounting, slot ownership, and arrival-ordered slot waiters.
    count: Counter,
    /// Wrapped service with readiness cached until its own notifier invalidates it.
    service: ReadyBinding<S>,
    /// Current streaming publish identifier, cleared on its final chunk or guard drop.
    streaming: Cell<Option<u64>>,
    /// Wrapping identifier allocated to the next streaming publish guard.
    next_streaming: Cell<u64>,
}

impl<S> fmt::Debug for InFlightServiceImpl<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InFlightServiceImpl").finish()
    }
}

impl<S> InFlightServiceImpl<S> {
    /// Wraps a service with request-slot and aggregate byte limits.
    ///
    /// The number limit applies to limited requests only, they wait for a free slot
    /// in `call()` so that the other requests are still processed. The size limit
    /// applies to all requests and pauses readiness only when the total exceeds
    /// `max_size` bytes; a streaming publish temporarily bypasses this check.
    /// `0` disables the corresponding limit.
    /// The wrapped service retains its own readiness until it notifies a change.
    pub fn new(max_cap: u16, max_size: usize, service: S) -> Self {
        InFlightServiceImpl {
            service: ReadyBinding::new(service),
            streaming: Cell::new(None),
            next_streaming: Cell::new(0),
            count: Counter::new(max_cap, max_size),
        }
    }
}

impl<S, St, Req> Service<St, Req> for InFlightServiceImpl<S>
where
    S: Service<St, Req>,
    Req: SizedRequest + 'static,
{
    type Res = S::Res;
    type Error = S::Error;

    fn bind(&self, ctx: BindCtx<'_>) {
        ctx.subscribe(&self.count.0.changed);
        ctx.bind(&self.service);
    }

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), S::Error> {
        if self.streaming.get().is_some() || self.count.is_available() {
            ctx.ready(&self.service).await
        } else {
            let available = poll_fn(|cx| {
                if self.streaming.get().is_some() || self.count.0.available(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            });
            join(available, ctx.ready(&self.service)).await.1?;
            ctx.ready(&self.service).await
        }
    }

    #[inline]
    async fn call(&self, req: Req, ctx: Ctx<'_, Self, St>) -> Result<S::Res, S::Error> {
        // payload chunks pass the pipeline readiness check, it must not wait for
        // the limits held by the publish until its last chunk is received.
        // payloads are received one at a time, a new streaming publish ends the previous one
        let _streaming = if req.is_limited() && req.has_more_chunks() {
            let id = self.next_streaming.get();
            self.next_streaming.set(id.wrapping_add(1));
            let guard = StreamingGuard(id, &self.streaming, &self.count.0);
            if self.streaming.replace(Some(id)).is_none() {
                self.count.0.notify();
            }
            Some(guard)
        } else {
            if req.is_last_chunk() && self.streaming.take().is_some() {
                self.count.0.notify();
            }
            None
        };

        // the request is received, its size counts while it waits for a slot
        let size = if self.count.0.max_size > 0 { req.size() } else { 0 };
        let size_guard = self.count.get(size);
        let limited = req.is_limited();
        let slot_guard = if limited || req.is_ordered() {
            self.count.slot(limited).await
        } else {
            None
        };

        let result = ctx.call(&self.service, req).await;
        drop(slot_guard);
        drop(size_guard);
        result
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, St>) {
        ctx.shutdown(&self.service).await;
    }
}

/// Clears a publish's streaming readiness exemption on completion or cancellation.
///
/// Only the guard whose identifier is still current may clear the exemption.
struct StreamingGuard<'a>(u64, &'a Cell<Option<u64>>, &'a CounterInner);

impl Drop for StreamingGuard<'_> {
    fn drop(&mut self) {
        // the publish completes before its last chunk is received
        if self.1.get() == Some(self.0) {
            self.1.set(None);
            self.2.notify();
        }
    }
}

/// Shared owner of request-slot and encoded-byte accounting.
struct Counter(Rc<CounterInner>);

/// Accounting and wakeup state retained by active requests and queued waiters.
struct CounterInner {
    /// Maximum concurrent limited requests; zero disables the slot limit.
    max_cap: u16,
    /// Slots currently owned by active limited requests.
    cur_cap: Cell<u16>,
    /// Aggregate request-byte threshold; zero disables byte limiting.
    max_size: usize,
    /// Bytes held by active requests, including requests still waiting for a slot.
    cur_size: Cell<usize>,
    /// Readiness task to wake when byte availability or the streaming exemption changes.
    task: LocalWaker,
    /// Invalidates every subscribed readiness cache when availability changes.
    changed: ReadyNotifier,
    /// Requests that wait for their turn, in the order of arrival
    waiters: RefCell<VecDeque<Waiter>>,
    /// Ticket of the next waiter, tickets of the queued waiters are ascending
    next_ticket: Cell<u64>,
}

/// Queued request awaiting its arrival-order turn and, if limited, a free slot.
struct Waiter {
    /// Unique ascending identifier used to find this waiter during polling or cancellation.
    ticket: u64,
    /// Whether taking the turn also requires acquiring a request slot.
    limited: bool,
    /// Set when the head may proceed; retained until it is polled or cancelled.
    granted: bool,
    /// Latest polling task, replaced by a no-op waker when its turn is granted.
    waker: Waker,
}

impl Counter {
    /// Creates empty counters with optional slot and byte limits.
    fn new(max_cap: u16, max_size: usize) -> Self {
        Counter(Rc::new(CounterInner {
            max_cap,
            max_size,
            cur_cap: Cell::new(0),
            cur_size: Cell::new(0),
            task: LocalWaker::new(),
            changed: ReadyNotifier::new(),
            waiters: RefCell::new(VecDeque::new()),
            next_ticket: Cell::new(0),
        }))
    }

    /// Accounts for `size` bytes until the returned guard is dropped.
    fn get(&self, size: u32) -> SizeGuard {
        SizeGuard::new(size, self.0.clone())
    }

    /// Returns whether aggregate bytes are at or below the threshold, or unlimited.
    fn is_available(&self) -> bool {
        self.0.max_size == 0 || self.0.cur_size.get() <= self.0.max_size
    }

    /// Check if a request takes its turn without waiting
    fn has_turn(&self, limited: bool) -> bool {
        self.0.waiters.borrow().is_empty() && (!limited || self.0.has_slot())
    }

    /// Wait for the turn of a request
    ///
    /// Requests wait in the order of arrival, a limited request also waits
    /// for a free slot and takes it.
    async fn slot(&self, limited: bool) -> Option<SlotGuard> {
        let inner = &self.0;
        if self.has_turn(limited) {
            return limited.then(|| SlotGuard::new(inner.clone()));
        }

        let mut entry = WaiterGuard(None, inner.clone());
        poll_fn(|cx| {
            let mut waiters = inner.waiters.borrow_mut();
            if let Some(ticket) = entry.0 {
                let idx = find(&waiters, ticket);
                let waiter = &mut waiters[idx];
                if waiter.granted {
                    return Poll::Ready(());
                }
                waiter.waker.clone_from(cx.waker());
            } else {
                let ticket = inner.next_ticket.get();
                inner.next_ticket.set(ticket + 1);
                waiters.push_back(Waiter {
                    ticket,
                    limited,
                    granted: false,
                    waker: cx.waker().clone(),
                });
                entry.0 = Some(ticket);
            }
            Poll::Pending
        })
        .await;

        // the granted waiter is the first one, the next one waits until
        // this request is passed to the service
        entry.0 = None;
        inner.waiters.borrow_mut().pop_front();
        let guard = limited.then(|| SlotGuard::new(inner.clone()));
        inner.grant();
        guard
    }
}

/// Returns the position of the queued waiter with `ticket`.
///
/// # Panics
///
/// Panics if the ticket does not identify a currently queued waiter.
fn find(waiters: &VecDeque<Waiter>, ticket: u64) -> usize {
    waiters
        .binary_search_by_key(&ticket, |w| w.ticket)
        .expect("waiter is queued")
}

/// Removes a cancelled waiter from the queue
struct WaiterGuard(Option<u64>, Rc<CounterInner>);

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if let Some(ticket) = self.0.take() {
            let mut waiters = self.1.waiters.borrow_mut();
            let idx = find(&waiters, ticket);
            waiters.remove(idx);
            drop(waiters);
            if idx == 0 {
                self.1.grant();
            }
        }
    }
}

/// Accounts for request bytes for its lifetime, including time spent waiting for a slot.
struct SizeGuard(u32, Rc<CounterInner>);

impl SizeGuard {
    /// Adds request bytes to the counter and returns their lifetime guard.
    fn new(size: u32, inner: Rc<CounterInner>) -> Self {
        let guard = SizeGuard(size, inner);
        guard.1.inc(size);
        guard
    }
}

impl Drop for SizeGuard {
    fn drop(&mut self) {
        self.1.dec(self.0);
    }
}

/// Owns one limited-request slot and grants the next eligible waiter on drop.
struct SlotGuard(Rc<CounterInner>);

impl SlotGuard {
    /// Acquires one request slot after availability and arrival order have been checked.
    fn new(inner: Rc<CounterInner>) -> Self {
        inner.cur_cap.set(inner.cur_cap.get() + 1);
        SlotGuard(inner)
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.0.cur_cap.set(self.0.cur_cap.get() - 1);
        self.0.grant();
    }
}

impl CounterInner {
    /// Adds request bytes and notifies readiness when the byte threshold is exceeded.
    fn inc(&self, size: u32) {
        let prev_size = self.cur_size.get();
        let cur_size = prev_size + size as usize;
        self.cur_size.set(cur_size);

        if self.max_size != 0 && prev_size <= self.max_size && cur_size > self.max_size {
            self.notify();
        }
    }

    /// Releases request bytes and notifies readiness when the total returns to the threshold.
    fn dec(&self, size: u32) {
        let cur_size = self.cur_size.get();
        let new_size = cur_size - (size as usize);
        self.cur_size.set(new_size);

        if cur_size > self.max_size && new_size <= self.max_size {
            self.notify();
        }
    }

    /// Invalidates readiness caches and wakes the waiting task even if a callback panics.
    fn notify(&self) {
        let _wake = OnDropFn::new(|| self.task.wake());
        self.changed.notify();
    }

    /// Registers the readiness task and checks the aggregate byte threshold.
    fn available(&self, cx: &Context<'_>) -> bool {
        self.task.register(cx.waker());
        self.max_size == 0 || self.cur_size.get() <= self.max_size
    }

    /// Returns whether another limited request can acquire a slot.
    fn has_slot(&self) -> bool {
        self.max_cap == 0 || self.cur_cap.get() < self.max_cap
    }

    /// Let the first waiter proceed
    fn grant(&self) {
        let waker = if let Some(waiter) = self.waiters.borrow_mut().front_mut()
            && !waiter.granted
            && (!waiter.limited || self.has_slot())
        {
            // a granted waiter does not register again
            waiter.granted = true;
            std::mem::replace(&mut waiter.waker, Waker::noop().clone())
        } else {
            return;
        };
        waker.wake();
    }
}

/// Unit tests for byte readiness, slot ordering, cancellation, and wrapped readiness.
#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
    use std::{cell::RefCell, rc::Rc};
    use std::{future::poll_fn, task::Wake, task::Waker};

    use ntex_service::Pipeline;
    use ntex_util::channel::{condition::Condition, oneshot};
    use ntex_util::future::lazy;
    use ntex_util::task::LocalWaker;
    use ntex_util::time::{Millis, sleep, timeout};

    use super::*;

    /// Calls complete when the gate is opened
    struct GateService(Condition);

    impl Service<(), ()> for GateService {
        type Res = ();
        type Error = ();

        async fn call(&self, _r: (), _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let _ = self.0.wait().await;
            Ok::<_, ()>(())
        }
    }

    /// Forwards to a shared pipeline, so several pipelines can share one service
    struct Shared<R>(Rc<Pipeline<R, (), ()>>);

    impl<R: 'static> Service<(), R> for Shared<R> {
        type Res = ();
        type Error = ();

        fn bind(&self, ctx: BindCtx<'_>) {
            self.0.bind_to(&ctx);
        }

        async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            self.0.ready().await
        }

        async fn call(&self, req: R, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            self.0.call(req).await
        }
    }

    /// Counts started calls, calls complete when the gate is opened
    struct CountService(Condition, Rc<Cell<usize>>);

    impl Service<(), ()> for CountService {
        type Res = ();
        type Error = ();

        async fn call(&self, _r: (), _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            self.1.set(self.1.get() + 1);
            let _ = self.0.wait().await;
            Ok::<_, ()>(())
        }
    }

    impl SizedRequest for () {
        fn size(&self) -> u32 {
            12
        }

        fn has_more_chunks(&self) -> bool {
            false
        }
    }

    /// STATEMENT: The request-count limit serializes calls without blocking pipeline readiness.
    ///
    /// HOW:
    /// - Hold one slot with a gated call, spawn a second call, and verify readiness stays ready while only one handler has started.
    /// - Release the gate and verify the second handler starts and shutdown completes.
    #[ntex::test]
    async fn test_inflight() {
        let gate = Condition::new();
        let started = Rc::new(Cell::new(0));
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(1, 0, CountService(gate.clone(), started.clone())),
        );
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        for _ in 0..2 {
            let srv2 = srv.bind();
            ntex_util::spawn(async move {
                let _ = srv2.call(()).await;
            });
        }
        sleep(Millis(25)).await;

        // the number limit does not pause readiness, the second call waits for a slot
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        assert_eq!(started.get(), 1);

        // wait for in-flight call to complete
        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(started.get(), 2);
        gate.notify(());
        assert!(lazy(|cx| srv.poll_shutdown(cx)).await.is_ready());
    }

    /// STATEMENT: Aggregate request bytes exceeding the threshold pause readiness until the call completes.
    ///
    /// HOW:
    /// - Run a 12-byte gated request with a 10-byte limit and verify readiness is pending.
    /// - Release and lock the gate, then verify both awaited and polled readiness succeed.
    #[ntex::test]
    async fn test_inflight2() {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(0, 10, GateService(gate.clone())),
        );
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        sleep(Millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        gate.notify_and_lock(());
        assert_eq!(timeout(Millis(5000), srv.ready()).await, Ok(Ok(())));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
    }

    /// Readiness waiter must be woken by the in-flight counter when a call completes
    async fn check_ready_woken(max_size: usize) {
        let gate = Condition::new();
        let svc = Rc::new(Pipeline::new(
            (),
            InFlightServiceImpl::new(1, max_size, GateService(gate.clone())),
        ));
        // the call uses a separate outer pipeline so only the counter can wake `srv`
        let srv = Pipeline::new((), Shared(svc.clone()));
        let srv2 = Pipeline::new((), Shared(svc));

        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        sleep(Millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        // poll readiness from a separate task, so unrelated wakeups of
        // the test task cannot re-poll it
        let ready = Rc::new(Cell::new(false));
        let ready2 = ready.clone();
        ntex_util::spawn(async move {
            assert_eq!(srv.ready().await, Ok(()));
            ready2.set(true);
        });
        sleep(Millis(25)).await;
        assert!(!ready.get());

        gate.notify_and_lock(());
        sleep(Millis(50)).await;
        assert!(ready.get());
    }

    /// STATEMENT: Releasing request bytes wakes a readiness waiter in a separate pipeline task.
    ///
    /// HOW:
    /// - Use `check_ready_woken` with a 10-byte limit and a 12-byte gated call through another shared pipeline.
    /// - Verify the isolated readiness task stays blocked before completion and is woken after the gate opens.
    #[ntex::test]
    async fn test_inflight_size_wakes_ready() {
        check_ready_woken(10).await;
    }

    /// Waker recording how often a waiting task is notified.
    #[derive(Default)]
    struct CountWaker(AtomicUsize);

    impl CountWaker {
        /// Returns the number of recorded wakeups.
        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Register a waker with the counter
    fn register(counter: &Counter) -> (Arc<CountWaker>, bool) {
        let w = Arc::new(CountWaker::default());
        let waker = Waker::from(w.clone());
        let available = counter.0.available(&Context::from_waker(&waker));
        (w, available)
    }

    /// STATEMENT: Byte-counter wakeups occur on threshold crossings, not on every size change.
    ///
    /// HOW:
    /// - Acquire two 6-byte guards with a 10-byte limit and verify one wake when the limit is exceeded.
    /// - Drop a guard and verify one wake as readiness returns; drop the remaining guard and verify no further wake.
    #[test]
    fn test_counter_size_waker() {
        let counter = Counter::new(0, 10);
        let g1 = counter.get(6);
        let (w, available) = register(&counter);
        assert!(available);

        // size limit is exceeded
        let g2 = counter.get(6);
        assert_eq!(w.count(), 1);
        let (w, available) = register(&counter);
        assert!(!available);

        // size drops to the limit
        drop(g1);
        assert_eq!(w.count(), 1);
        assert!(counter.is_available());

        let (w, available) = register(&counter);
        assert!(available);
        drop(g2);
        assert_eq!(w.count(), 0);
    }

    /// Service invalidating readiness while a gated call is active.
    struct Srv2 {
        /// Condition that releases blocked calls.
        gate: Condition,
        /// Whether an active call currently makes the service not ready.
        cnt: Cell<bool>,
        /// Task waiting for the service to become ready.
        waker: LocalWaker,
        /// Notifier invalidating readiness when service availability changes.
        changed: ReadyNotifier,
    }

    impl Service<(), ()> for Srv2 {
        type Res = ();
        type Error = ();

        fn bind(&self, ctx: BindCtx<'_>) {
            ctx.subscribe(&self.changed);
        }

        async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            poll_fn(|cx| {
                if self.cnt.get() {
                    self.waker.register(cx.waker());
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            })
            .await
        }

        async fn call(&self, _r: (), _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let fut = self.gate.wait();
            self.cnt.set(true);
            self.changed.notify();
            self.waker.wake();

            let _ = fut.await;
            self.cnt.set(false);
            self.changed.notify();
            self.waker.wake();
            Ok::<_, ()>(())
        }
    }

    /// STATEMENT: Counter and wrapped-service readiness changes wake all registered readiness waiters.
    ///
    /// HOW:
    /// - Start a gated call that exceeds the byte limit and makes `Srv2` not ready.
    /// - Register two readiness waiters before releasing the gate and verify both finish within their timeouts.
    #[ntex::test]
    async fn test_inflight3() {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(
                1,
                10,
                Srv2 {
                    gate: gate.clone(),
                    cnt: Cell::new(false),
                    waker: LocalWaker::new(),
                    changed: ReadyNotifier::new(),
                },
            ),
        );
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        sleep(Millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        let srv2 = srv.bind();
        let (tx, rx) = oneshot::channel();
        ntex_util::spawn(async move {
            let _ = srv2.ready().await;
            let _ = tx.send(());
        });

        // both readiness waiters are registered before the call completes
        let (res, ()) = ntex_util::future::join(timeout(Millis(5000), srv.ready()), async {
            sleep(Millis(10)).await;
            gate.notify_and_lock(());
        })
        .await;
        assert_eq!(res, Ok(Ok(())));
        assert_eq!(timeout(Millis(5000), rx).await, Ok(Ok(())));
    }

    /// Publish or payload chunk, the flag indicates that payload chunks follow
    #[derive(Clone, Copy)]
    enum Req {
        /// Limited 12-byte publish; its flag indicates whether payload chunks follow.
        Publish(bool),
        /// Unlimited zero-byte payload chunk; `false` marks the final chunk.
        Chunk(bool),
    }

    impl SizedRequest for Req {
        fn size(&self) -> u32 {
            match self {
                Req::Publish(_) => 12,
                Req::Chunk(_) => 0,
            }
        }

        fn has_more_chunks(&self) -> bool {
            match self {
                Req::Publish(more) | Req::Chunk(more) => *more,
            }
        }

        fn is_limited(&self) -> bool {
            matches!(self, Req::Publish(_))
        }

        fn is_last_chunk(&self) -> bool {
            matches!(self, Req::Chunk(false))
        }
    }

    /// Request service keeping each call pending until its shared gate opens.
    struct GateReqService(Condition);

    impl Service<(), Req> for GateReqService {
        type Res = ();
        type Error = ();

        async fn call(&self, _r: Req, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let _ = self.0.wait().await;
            Ok(())
        }
    }

    /// Spawns a request and yields briefly so its accounting and readiness changes take effect.
    async fn spawn_call(srv: &Pipeline<Req, (), ()>, req: Req) {
        let srv = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv.call(req).await;
        });
        sleep(Millis(25)).await;
    }

    /// Checks that streaming bypass ends on the last chunk and completion restores byte readiness.
    async fn check_streaming(max_size: usize) {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(0, max_size, GateReqService(gate.clone())),
        );

        // streaming publish exceeds the size limit, chunks must still pass
        spawn_call(&srv, Req::Publish(true)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        spawn_call(&srv, Req::Chunk(true)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        // the limits apply after the last chunk, while the publish is processed
        spawn_call(&srv, Req::Chunk(false)).await;
        assert!(!srv.is_ready());
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        // completed publishes restore the limits
        gate.notify_and_lock(());
        let res = timeout(Millis(5000), srv.ready()).await;
        assert_eq!(res, Ok(Ok(())));
    }

    /// STATEMENT: Streaming chunks bypass the byte limit only until the final chunk arrives.
    ///
    /// HOW:
    /// - Use `check_streaming` with a 10-byte limit and a 12-byte streaming publish.
    /// - Verify intermediate chunks preserve readiness, the final chunk blocks it, and completing the calls restores readiness.
    #[ntex::test]
    async fn test_inflight_streaming_size() {
        check_streaming(10).await;
    }

    /// Each call completes when its own gate is opened
    struct QueueService(RefCell<VecDeque<oneshot::Receiver<()>>>);

    impl Service<(), Req> for QueueService {
        type Res = ();
        type Error = ();

        async fn call(&self, _r: Req, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let rx = self.0.borrow_mut().pop_front().unwrap();
            let _ = rx.await;
            Ok(())
        }
    }

    /// STATEMENT: Completing an older publish cannot clear a newer publish's streaming exemption.
    ///
    /// HOW:
    /// - Start two streaming publishes with separate completion gates, then finish only the first and verify readiness still bypasses the byte limit.
    /// - Deliver the second publish's final chunk and verify readiness blocks until the remaining calls complete.
    #[ntex::test]
    async fn test_inflight_streaming_overlap() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let (tx3, rx3) = oneshot::channel();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(0, 10, QueueService(RefCell::new([rx1, rx2, rx3].into()))),
        );

        // the next streaming publish starts before the previous one completes
        spawn_call(&srv, Req::Publish(true)).await;
        spawn_call(&srv, Req::Publish(true)).await;

        // the second publish keeps bypassing the limits
        let _ = tx1.send(());
        sleep(Millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        // the second payload is received, the limits apply
        spawn_call(&srv, Req::Chunk(false)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        let _ = tx3.send(());
        let _ = tx2.send(());
        sleep(Millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
    }

    /// STATEMENT: Cancelling a streaming publish invalidates the byte-limit exemption in every bound scope.
    ///
    /// HOW:
    /// - Start a streaming and a complete publish through two shared pipelines, exceeding the byte limit while both remain ready.
    /// - Drop the streaming call and verify both caches are invalidated and readiness becomes pending; drop the other call and verify both recover.
    #[ntex::test]
    async fn test_streaming_cancellation_invalidates_shared_readiness() {
        use std::future::Future;

        let service = Rc::new(Pipeline::new(
            (),
            InFlightServiceImpl::new(0, 10, GateReqService(Condition::new())),
        ));
        let a = Pipeline::new((), Shared(service.clone()));
        let b = Pipeline::new((), Shared(service));
        let mut streaming = Box::pin(a.call(Req::Publish(true)));
        assert!(lazy(|cx| streaming.as_mut().poll(cx)).await.is_pending());
        let mut complete = Box::pin(b.call(Req::Publish(false)));
        assert!(lazy(|cx| complete.as_mut().poll(cx)).await.is_pending());
        a.ready().await.unwrap();
        b.ready().await.unwrap();

        // Size remains above the limit, but the streaming exception ends.
        drop(streaming);
        assert!(!a.is_ready() && !b.is_ready());
        assert!(lazy(|cx| a.poll_ready(cx)).await.is_pending());
        assert!(lazy(|cx| b.poll_ready(cx)).await.is_pending());
        drop(complete);
        a.ready().await.unwrap();
        b.ready().await.unwrap();
    }

    /// STATEMENT: A panicking readiness callback does not leak byte accounting, slots, or streaming state.
    ///
    /// HOW:
    /// - For complete and streaming publishes, install a callback that panics during the first call poll and catch the unwind.
    /// - Drop the call and verify byte count, slot count, and streaming state are cleared.
    /// - Make another failing service call and verify pipeline readiness still succeeds.
    #[ntex::test]
    async fn test_notification_panic_releases_limits() {
        use std::future::Future;
        use std::panic::{AssertUnwindSafe, catch_unwind};

        for streaming in [false, true] {
            let service = Rc::new(InFlightServiceImpl::new(
                1,
                10,
                ntex_service::fn_service(async |_: Req| Err::<(), ()>(())),
            ));
            let pipeline = Pipeline::new((), service.clone());
            let armed = Cell::new(true);
            let callback = ntex_service::ReadyCallback::new(move |_| {
                assert!(!armed.replace(false), "callback panic");
            });
            pipeline.on_notify(&callback);
            let mut call = Box::pin(pipeline.call(Req::Publish(streaming)));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(&mut cx))).is_err());
            drop(call);
            assert_eq!(service.count.0.cur_size.get(), 0);
            assert_eq!(service.count.0.cur_cap.get(), 0);
            assert_eq!(service.streaming.get(), None);

            assert_eq!(pipeline.call(Req::Publish(streaming)).await, Err(()));
            pipeline.ready().await.unwrap();
        }
    }

    /// STATEMENT: A publish with its entire payload does not bypass aggregate byte readiness.
    ///
    /// HOW:
    /// - Start a complete 12-byte publish with a 10-byte limit and verify readiness is pending.
    /// - Release its completion gate and verify readiness succeeds within the timeout.
    #[ntex::test]
    async fn test_inflight_complete_payload() {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(1, 10, GateReqService(gate.clone())),
        );

        // publish with the whole payload does not bypass the limits
        spawn_call(&srv, Req::Publish(false)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        gate.notify_and_lock(());
        let res = timeout(Millis(5000), srv.ready()).await;
        assert_eq!(res, Ok(Ok(())));
    }

    /// Request kinds of the ordering tests
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Msg {
        /// Takes a slot and completes when the gate is opened
        Publish(u8),
        /// Does not take a slot
        Ack(u8),
        /// Does not take a slot, waits for the preceding publishes
        Chunk(u8),
    }

    impl SizedRequest for Msg {
        fn size(&self) -> u32 {
            0
        }

        fn has_more_chunks(&self) -> bool {
            false
        }

        fn is_limited(&self) -> bool {
            matches!(self, Msg::Publish(_))
        }

        fn is_ordered(&self) -> bool {
            matches!(self, Msg::Chunk(_))
        }
    }

    /// Logs started calls
    struct LogService(Condition, Rc<RefCell<Vec<Msg>>>);

    impl Service<(), Msg> for LogService {
        type Res = ();
        type Error = ();

        async fn call(&self, req: Msg, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            self.1.borrow_mut().push(req);
            if let Msg::Publish(_) = req {
                let _ = self.0.wait().await;
            }
            Ok(())
        }
    }

    /// Shared in-flight wrapper whose started request sequence is recorded.
    type LogSrv = Rc<InFlightServiceImpl<LogService>>;

    /// Creates a slot-limited logging pipeline, its service, completion gate, and request log.
    fn log_srv(
        max_cap: u16,
    ) -> (
        Pipeline<Msg, (), ()>,
        LogSrv,
        Condition,
        Rc<RefCell<Vec<Msg>>>,
    ) {
        let gate = Condition::new();
        let log = Rc::new(RefCell::new(Vec::new()));
        let svc = Rc::new(InFlightServiceImpl::new(
            max_cap,
            0,
            LogService(gate.clone(), log.clone()),
        ));
        (Pipeline::new((), svc.clone()), svc, gate, log)
    }

    /// Spawns a logged request and gives it time to start or join the slot queue.
    async fn spawn_msg(srv: &Pipeline<Msg, (), ()>, msg: Msg) {
        let srv = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv.call(msg).await;
        });
        sleep(Millis(10)).await;
    }

    /// STATEMENT: Slot waiters preserve arrival order while unordered acknowledgements bypass them.
    ///
    /// HOW:
    /// - Submit publishes, acknowledgements, and an ordered chunk with one available slot.
    /// - Verify acknowledgements start immediately, then release successive publishes and verify the queued publish, chunk, and next publish start in order.
    #[ntex::test]
    async fn test_inflight_slots_order() {
        let (srv, _, gate, log) = log_srv(1);
        for msg in [
            Msg::Publish(1),
            Msg::Publish(2),
            Msg::Ack(1),
            Msg::Chunk(2),
            Msg::Publish(3),
            Msg::Ack(2),
        ] {
            spawn_msg(&srv, msg).await;
            assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        }

        // packets without a slot are processed while the limit is reached,
        // the rest waits in the order of arrival
        assert_eq!(*log.borrow(), [Msg::Publish(1), Msg::Ack(1), Msg::Ack(2)]);

        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(log.borrow()[3..], [Msg::Publish(2), Msg::Chunk(2)]);

        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(log.borrow()[5..], [Msg::Publish(3)]);
        gate.notify(());
    }

    /// STATEMENT: The slot cap limits concurrent publishes and ordered chunks wait behind queued publishes.
    ///
    /// HOW:
    /// - Queue five publishes and a chunk with two slots, then verify handler counts and the chunk's position as gates are released.
    /// - Repeat with a zero slot limit and verify all five publishes and the chunk start immediately.
    #[ntex::test]
    async fn test_inflight_slots_limit() {
        let (srv, _, gate, log) = log_srv(2);
        for id in 0..5 {
            spawn_msg(&srv, Msg::Publish(id)).await;
        }
        assert_eq!(log.borrow().len(), 2);
        // nothing waits for a slot, the chunk is processed
        spawn_msg(&srv, Msg::Chunk(0)).await;
        assert_eq!(log.borrow().len(), 2);

        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(log.borrow().len(), 4);
        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(log.borrow().len(), 6);
        assert_eq!(log.borrow()[5], Msg::Chunk(0));

        // unlimited
        let (srv, _, gate, log) = log_srv(0);
        for id in 0..5 {
            spawn_msg(&srv, Msg::Publish(id)).await;
        }
        spawn_msg(&srv, Msg::Chunk(0)).await;
        assert_eq!(log.borrow().len(), 6);
        gate.notify(());
    }

    /// STATEMENT: Cancelling a queued request removes its waiter without disturbing later requests.
    ///
    /// HOW:
    /// - Fill one slot, queue a publish, time out an ordered chunk, and queue another publish.
    /// - Verify the cancelled chunk is removed, surviving publishes start in order, and all waiters and slots are released.
    #[ntex::test]
    async fn test_inflight_slots_cancel() {
        let (srv, svc, gate, log) = log_srv(1);
        spawn_msg(&srv, Msg::Publish(1)).await;
        spawn_msg(&srv, Msg::Publish(2)).await;

        // waiting call is cancelled
        let srv2 = srv.bind();
        ntex_util::spawn(async move {
            let _ = timeout(Millis(25), srv2.call(Msg::Chunk(3))).await;
        });
        sleep(Millis(5)).await;
        spawn_msg(&srv, Msg::Publish(4)).await;
        sleep(Millis(50)).await;
        assert_eq!(*log.borrow(), [Msg::Publish(1)]);
        assert_eq!(svc.count.0.waiters.borrow().len(), 2);

        gate.notify(());
        sleep(Millis(25)).await;
        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(
            *log.borrow(),
            [Msg::Publish(1), Msg::Publish(2), Msg::Publish(4)]
        );
        gate.notify(());
        sleep(Millis(25)).await;
        assert!(svc.count.0.waiters.borrow().is_empty());
        assert_eq!(svc.count.0.cur_cap.get(), 0);
    }

    /// STATEMENT: Cancelling the first slot waiter lets the next unlimited ordered request proceed.
    ///
    /// HOW:
    /// - Hold one slot, time out the first waiting publish, and queue an ordered chunk followed by another publish.
    /// - Verify the chunk starts after cancellation without a free slot, and the surviving publish starts when the active call completes.
    #[ntex::test]
    async fn test_inflight_slots_cancel_first() {
        let (srv, _, gate, log) = log_srv(1);
        spawn_msg(&srv, Msg::Publish(1)).await;

        // the first waiter is cancelled, the next one proceeds
        let srv2 = srv.bind();
        ntex_util::spawn(async move {
            let _ = timeout(Millis(25), srv2.call(Msg::Publish(2))).await;
        });
        sleep(Millis(5)).await;
        spawn_msg(&srv, Msg::Chunk(3)).await;
        spawn_msg(&srv, Msg::Publish(4)).await;
        sleep(Millis(25)).await;
        assert_eq!(*log.borrow(), [Msg::Publish(1), Msg::Chunk(3)]);

        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(log.borrow()[2..], [Msg::Publish(4)]);
        gate.notify(());
    }

    /// STATEMENT: Cancelling a granted but unpolled waiter passes its turn without leaking a slot.
    ///
    /// HOW:
    /// - Poll a waiting publish once, queue an ordered chunk, release the active publish, and verify the first waiter is granted.
    /// - Drop the unpolled publish future and verify the chunk starts, the waiter queue empties, and the slot count is zero.
    #[ntex::test]
    async fn test_inflight_slots_cancel_granted() {
        let (srv, svc, gate, log) = log_srv(1);
        spawn_msg(&srv, Msg::Publish(1)).await;

        let mut fut = Box::pin(srv.call(Msg::Publish(2)));
        assert!(lazy(|cx| fut.as_mut().poll(cx)).await.is_pending());
        spawn_msg(&srv, Msg::Chunk(3)).await;

        // the slot is granted to the waiter, but the waiter is not polled
        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(*log.borrow(), [Msg::Publish(1)]);
        assert!(svc.count.0.waiters.borrow()[0].granted);

        // the cancelled waiter passes its turn
        drop(fut);
        sleep(Millis(25)).await;
        assert_eq!(*log.borrow(), [Msg::Publish(1), Msg::Chunk(3)]);
        assert!(svc.count.0.waiters.borrow().is_empty());
        assert_eq!(svc.count.0.cur_cap.get(), 0);
    }

    /// STATEMENT: A slot waiter wakes its most recently registered task.
    ///
    /// HOW:
    /// - Poll a waiting publish with two distinct counting wakers while another publish holds the slot.
    /// - Release the active publish and verify only the second waker is notified once.
    #[ntex::test]
    async fn test_inflight_slots_waker_update() {
        use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
        use std::task::Wake;

        /// Waker counting notifications to distinguish successive polling tasks.
        struct Counter(AtomicUsize);
        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (srv, _, gate, _) = log_srv(1);
        spawn_msg(&srv, Msg::Publish(1)).await;

        // the waiter moves to another task between polls
        let (c1, c2) = (
            Arc::new(Counter(AtomicUsize::new(0))),
            Arc::new(Counter(AtomicUsize::new(0))),
        );
        let mut fut = Box::pin(srv.call(Msg::Publish(2)));
        for c in [&c1, &c2] {
            let waker = Waker::from(c.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(fut.as_mut().poll(&mut cx).is_pending());
        }

        // the latest waker is woken once the slot is released
        gate.notify(());
        sleep(Millis(25)).await;
        assert_eq!(c1.0.load(Ordering::Relaxed), 0);
        assert_eq!(c2.0.load(Ordering::Relaxed), 1);
    }

    /// STATEMENT: The wrapper's debug output identifies it without requiring debug support from the service.
    ///
    /// HOW:
    /// - Wrap `NoopSvc`, which does not implement `Debug`, in `InFlightServiceImpl`.
    /// - Format the wrapper and verify the output contains its type name.
    #[test]
    fn test_debug() {
        /// Service completing every test request successfully without extra state.
        struct NoopSvc;
        /// Zero-byte request used to verify the wrapper's debug representation.
        struct Req;
        impl Service<(), Req> for NoopSvc {
            type Res = ();
            type Error = ();
            async fn call(&self, _: Req, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
                Ok(())
            }
        }
        impl SizedRequest for Req {
            fn size(&self) -> u32 {
                0
            }
            fn has_more_chunks(&self) -> bool {
                false
            }
        }
        let svc = InFlightServiceImpl::new(16, 0, NoopSvc);
        assert!(format!("{svc:?}").contains("InFlightServiceImpl"));
    }

    /// Shared observations and readiness controls for the concurrency probe.
    #[derive(Default)]
    struct ProbeState {
        /// Number of probe calls currently running.
        active: Cell<usize>,
        /// Highest observed number of concurrent probe calls.
        max: Cell<usize>,
        /// Number of wrapped readiness checks performed.
        checks: Cell<usize>,
        /// Explicit readiness gate independent of the concurrency limit.
        closed: Cell<bool>,
        /// Task waiting for the service to become ready.
        waker: LocalWaker,
        /// Notifier invalidating readiness when service availability changes.
        changed: ReadyNotifier,
    }

    impl ProbeState {
        /// Sets the explicit readiness gate and wakes subscribed scopes and pending checks.
        fn set_closed(&self, closed: bool) {
            self.closed.set(closed);
            self.changed.notify();
            self.waker.wake();
        }
    }

    /// Inner service with its own concurrency limit, calls complete when
    /// the gate is opened if `blocking` is set
    struct Probe {
        /// Shared concurrency observations and readiness invalidation state.
        st: Rc<ProbeState>,
        /// Maximum active calls admitted by the probe's readiness check.
        cap: usize,
        /// Condition that releases blocked calls.
        gate: Condition,
        /// Whether calls wait on the gate instead of completing immediately.
        blocking: bool,
    }

    impl Service<(), ()> for Probe {
        type Res = ();
        type Error = ();

        fn bind(&self, ctx: BindCtx<'_>) {
            ctx.subscribe(&self.st.changed);
        }

        async fn ready(&self, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let st = &self.st;
            st.checks.set(st.checks.get() + 1);
            poll_fn(|cx| {
                if st.active.get() < self.cap && !st.closed.get() {
                    Poll::Ready(Ok(()))
                } else {
                    st.waker.register(cx.waker());
                    Poll::Pending
                }
            })
            .await
        }

        async fn call(&self, (): (), _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let st = &self.st;
            st.active.set(st.active.get() + 1);
            st.max.set(st.max.get().max(st.active.get()));
            if st.active.get() == self.cap {
                st.changed.notify();
            }
            if self.blocking {
                let _ = self.gate.wait().await;
            }
            st.active.set(st.active.get() - 1);
            if st.active.get() + 1 == self.cap {
                st.changed.notify();
            }
            st.waker.wake();
            Ok(())
        }
    }

    /// Creates a concurrency probe with shared observations and a call-completion gate.
    fn probe(cap: usize, blocking: bool) -> (Probe, Rc<ProbeState>, Condition) {
        let st = Rc::new(ProbeState::default());
        let gate = Condition::new();
        let srv = Probe {
            st: st.clone(),
            cap,
            gate: gate.clone(),
            blocking,
        };
        (srv, st, gate)
    }

    /// STATEMENT: Wrapped readiness is reused across calls until the wrapped service invalidates it.
    ///
    /// HOW:
    /// - Make repeated immediate calls and readiness checks through a probe, verifying only the first call checks inner readiness.
    /// - Close and reopen the probe's readiness gate and verify pending readiness and a fresh inner check.
    #[ntex::test]
    async fn test_inner_readiness_cached_until_notified() {
        let (inner, st, _) = probe(4, false);
        let srv = Pipeline::new((), InFlightServiceImpl::new(4, 0, inner));
        for _ in 0..3 {
            srv.call(()).await.unwrap();
        }
        assert_eq!(st.checks.get(), 1);

        st.checks.set(0);
        for _ in 0..3 {
            srv.ready().await.unwrap();
            srv.call(()).await.unwrap();
        }
        assert_eq!(st.checks.get(), 0);

        st.set_closed(true);
        assert!(lazy(|cx| srv.poll_ready(cx)).await.is_pending());
        st.set_closed(false);
        srv.call(()).await.unwrap();
        assert!(st.checks.get() > 0);
    }

    /// Does not check inner readiness, calls reuse this scope's cached
    /// readiness, so the inner service has to enforce its limits
    struct NoWait<S>(S);

    impl<S: Service<(), (), Res = (), Error = ()>> Service<(), ()> for NoWait<S> {
        type Res = ();
        type Error = ();

        fn bind(&self, ctx: BindCtx<'_>) {
            ctx.bind(&self.0);
        }

        async fn call(&self, req: (), ctx: Ctx<'_, Self, ()>) -> Result<(), ()> {
            ctx.call(&self.0, req).await
        }
    }

    /// Returns the max number of concurrent calls of the inner service
    async fn run_concurrent(max: u16, cap: usize) -> usize {
        let (inner, st, gate) = probe(cap, true);
        let srv = Pipeline::new((), NoWait(InFlightServiceImpl::new(max, 0, inner)));
        let mut futs = Vec::new();
        for _ in 0..4 {
            futs.push(ntex_util::spawn(srv.call_static(())));
        }
        sleep(Millis(20)).await;
        for _ in 0..4 {
            gate.notify(());
            sleep(Millis(5)).await;
        }
        for f in futs {
            let _ = f.await;
        }
        st.max.get()
    }

    /// STATEMENT: Calls enforce both wrapper and wrapped-service limits even when an outer scope skips readiness.
    ///
    /// HOW:
    /// - Use `run_concurrent` to start four gated calls through `NoWait` with several wrapper and probe limits.
    /// - Verify observed concurrency is bounded by the stricter limit, including when wrapper limiting is disabled.
    #[ntex::test]
    async fn test_limits_without_readiness_check() {
        assert_eq!(run_concurrent(1, 4).await, 1, "inflight limit");
        assert_eq!(run_concurrent(2, 1).await, 1, "inner limit");
        assert_eq!(run_concurrent(2, 4).await, 2, "inflight limit 2");
        assert_eq!(
            run_concurrent(0, 1).await,
            1,
            "inner limit, no inflight limit"
        );
    }

    /// STATEMENT: A call rechecks wrapped readiness after acquiring a previously unavailable slot.
    ///
    /// HOW:
    /// - Hold one slot and queue another call while wrapper readiness is ready.
    /// - Close the probe before releasing the slot and verify no call starts until the probe is reopened.
    #[ntex::test]
    async fn test_inner_readiness_after_slot_wait() {
        let (inner, st, gate) = probe(4, true);
        let srv = Pipeline::new((), InFlightServiceImpl::new(1, 0, inner));
        drop(ntex_util::spawn(srv.call_static(())));
        sleep(Millis(10)).await;
        drop(ntex_util::spawn(srv.call_static(())));
        sleep(Millis(10)).await;
        assert_eq!(st.active.get(), 1);

        // the readiness check passes, the call waits for a slot
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        // the inner service is not ready when the slot is released
        st.set_closed(true);
        gate.notify(());
        sleep(Millis(20)).await;
        assert_eq!(st.active.get(), 0);

        st.set_closed(false);
        sleep(Millis(20)).await;
        assert_eq!(st.active.get(), 1);
        gate.notify(());
    }

    /// Calls the inner service twice after one readiness check
    struct Twice<S>(S);

    impl<S: Service<(), (), Res = (), Error = ()>> Service<(), ()> for Twice<S> {
        type Res = ();
        type Error = ();

        fn bind(&self, ctx: BindCtx<'_>) {
            ctx.bind(&self.0);
        }

        async fn ready(&self, ctx: Ctx<'_, Self, ()>) -> Result<(), ()> {
            ctx.ready(&self.0).await
        }

        async fn call(&self, (): (), ctx: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let (a, b) = join(ctx.call(&self.0, ()), ctx.call(&self.0, ())).await;
            a.and(b)
        }
    }

    /// STATEMENT: Two calls following one outer readiness check still respect the wrapped-service concurrency limit.
    ///
    /// HOW:
    /// - Use `Twice` to issue two joined calls to a gated one-slot probe with wrapper limiting disabled.
    /// - Release the calls successively and verify the observed maximum concurrency remains one.
    #[ntex::test]
    async fn test_readiness_used_once() {
        let (inner, st, gate) = probe(1, true);
        let srv = Pipeline::new((), Twice(InFlightServiceImpl::new(0, 0, inner)));
        let fut = ntex_util::spawn(srv.call_static(()));
        sleep(Millis(20)).await;
        assert_eq!(st.active.get(), 1);

        gate.notify(());
        sleep(Millis(20)).await;
        gate.notify(());
        let _ = fut.await;
        assert_eq!(st.max.get(), 1);
    }

    /// STATEMENT: A byte-limited call resumes after release and cancellation returns its bytes and slot.
    ///
    /// HOW:
    /// - Manually poll two gated 12-byte calls against a one-slot, 10-byte service.
    /// - Complete the first call and verify the second acquires the released slot and bytes.
    /// - Cancel the second call and assert zero accounting, no slot waiters, and restored readiness.
    #[ntex::test]
    async fn test_waiting_call_resumes_after_byte_release() {
        use std::future::Future;

        let gate = Condition::new();
        let service = Rc::new(InFlightServiceImpl::new(1, 10, GateService(gate.clone())));
        let pipeline = Pipeline::new((), service.clone());
        let mut first = Box::pin(pipeline.call(()));
        let mut second = Box::pin(pipeline.call(()));
        assert!(lazy(|cx| first.as_mut().poll(cx)).await.is_pending());
        assert!(lazy(|cx| second.as_mut().poll(cx)).await.is_pending());
        assert_eq!(service.count.0.cur_cap.get(), 1);
        assert_eq!(service.count.0.cur_size.get(), 12);

        gate.notify(());
        assert_eq!(
            lazy(|cx| first.as_mut().poll(cx)).await,
            Poll::Ready(Ok(()))
        );
        assert!(lazy(|cx| second.as_mut().poll(cx)).await.is_pending());
        assert_eq!(service.count.0.cur_cap.get(), 1);
        assert_eq!(service.count.0.cur_size.get(), 12);
        drop(second);
        assert_eq!(service.count.0.cur_cap.get(), 0);
        assert_eq!(service.count.0.cur_size.get(), 0);
        assert!(service.count.0.waiters.borrow().is_empty());
        pipeline.ready().await.unwrap();
    }
}
