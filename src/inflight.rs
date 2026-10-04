//! Service that limits number of in-flight async requests.
use std::task::{Context, Poll, Waker};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, fmt, future::poll_fn, rc::Rc};

use ntex_service::{Ctx, Service};
use ntex_util::{future::join, task::LocalWaker};

/// Trait for types that could be sized
pub trait SizedRequest {
    /// Encoded size of the request in bytes
    fn size(&self) -> u32;

    /// Check if more payload chunks of a streaming publish follow this request
    ///
    /// While chunks are pending, readiness ignores the size limit so that
    /// the payload can reach the publish handler.
    fn has_more_chunks(&self) -> bool;

    /// Check if the request takes an in-flight slot
    ///
    /// A request without a free slot waits for one, requests that do not take
    /// a slot are not limited by the number of in-flight requests.
    fn is_limited(&self) -> bool {
        true
    }

    /// Check if the request is processed after the preceding requests
    /// that wait for an in-flight slot
    fn is_ordered(&self) -> bool {
        false
    }
}

pub struct InFlightServiceImpl<S> {
    count: Counter,
    service: S,
    streaming: Cell<bool>,
}

impl<S> fmt::Debug for InFlightServiceImpl<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InFlightServiceImpl").finish()
    }
}

impl<S> InFlightServiceImpl<S> {
    /// Limit the number of in-flight requests and their total size
    ///
    /// The number limit applies to limited requests only, they wait for a free slot
    /// in `call()` so that the other requests are still processed. The size limit
    /// applies to all requests and pauses readiness. `0` disables a limit.
    pub fn new(max_cap: u16, max_size: usize, service: S) -> Self {
        InFlightServiceImpl {
            service,
            streaming: Cell::new(false),
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

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, St>) -> Result<(), S::Error> {
        if self.streaming.get() || self.count.is_available() {
            ctx.ready(&self.service).await
        } else {
            join(self.count.available(), ctx.ready(&self.service))
                .await
                .1
        }
    }

    #[inline]
    async fn call(&self, req: Req, ctx: Ctx<'_, Self, St>) -> Result<S::Res, S::Error> {
        self.streaming.set(req.has_more_chunks());

        // the request is received, its size counts while it waits for a slot
        let size = if self.count.0.max_size > 0 { req.size() } else { 0 };
        let size_guard = self.count.get(size);
        let slot_guard = if req.is_limited() {
            self.count.slot(true).await
        } else {
            if req.is_ordered() {
                self.count.slot(false).await;
            }
            None
        };
        let result = ctx.call(&self.service, req).await;
        drop(slot_guard);
        drop(size_guard);
        result
    }

    ntex_service::forward_shutdown!(St, service);
}

struct Counter(Rc<CounterInner>);

struct CounterInner {
    max_cap: u16,
    cur_cap: Cell<u16>,
    max_size: usize,
    cur_size: Cell<usize>,
    task: LocalWaker,
    /// Requests that wait for their turn, in the order of arrival
    waiters: RefCell<VecDeque<Waiter>>,
    /// Ticket of the next waiter, tickets of the queued waiters are ascending
    next_ticket: Cell<u64>,
}

struct Waiter {
    ticket: u64,
    limited: bool,
    granted: bool,
    waker: Waker,
}

impl Counter {
    fn new(max_cap: u16, max_size: usize) -> Self {
        Counter(Rc::new(CounterInner {
            max_cap,
            max_size,
            cur_cap: Cell::new(0),
            cur_size: Cell::new(0),
            task: LocalWaker::new(),
            waiters: RefCell::new(VecDeque::new()),
            next_ticket: Cell::new(0),
        }))
    }

    fn get(&self, size: u32) -> SizeGuard {
        SizeGuard::new(size, self.0.clone())
    }

    fn is_available(&self) -> bool {
        self.0.max_size == 0 || self.0.cur_size.get() <= self.0.max_size
    }

    async fn available(&self) {
        poll_fn(|cx| {
            if self.0.available(cx) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    /// Wait for the turn of a request
    ///
    /// Requests wait in the order of arrival, a limited request also waits
    /// for a free slot and takes it.
    async fn slot(&self, limited: bool) -> Option<SlotGuard> {
        let inner = &self.0;
        if inner.waiters.borrow().is_empty() && (!limited || inner.has_slot()) {
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

/// Position of the queued waiter with the ticket
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

struct SizeGuard(u32, Rc<CounterInner>);

impl SizeGuard {
    fn new(size: u32, inner: Rc<CounterInner>) -> Self {
        inner.inc(size);
        SizeGuard(size, inner)
    }
}

impl Drop for SizeGuard {
    fn drop(&mut self) {
        self.1.dec(self.0);
    }
}

struct SlotGuard(Rc<CounterInner>);

impl SlotGuard {
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
    fn inc(&self, size: u32) {
        let cur_size = self.cur_size.get() + size as usize;
        self.cur_size.set(cur_size);

        if self.max_size != 0 && cur_size > self.max_size {
            self.task.wake();
        }
    }

    fn dec(&self, size: u32) {
        let cur_size = self.cur_size.get();
        let new_size = cur_size - (size as usize);
        self.cur_size.set(new_size);

        if cur_size > self.max_size && new_size <= self.max_size {
            self.task.wake();
        }
    }

    fn available(&self, cx: &Context<'_>) -> bool {
        self.task.register(cx.waker());
        self.max_size == 0 || self.cur_size.get() <= self.max_size
    }

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
        let svc = Rc::new(InFlightServiceImpl::new(
            1,
            max_size,
            GateService(gate.clone()),
        ));
        // a completed pipeline call wakes the pipeline readiness waiters,
        // the call uses a separate pipeline so only the counter can wake `srv`
        let srv = Pipeline::new((), svc.clone());
        let srv2 = Pipeline::new((), svc);

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

    #[ntex::test]
    async fn test_inflight_size_wakes_ready() {
        check_ready_woken(10).await;
    }

    #[derive(Default)]
    struct CountWaker(AtomicUsize);

    impl CountWaker {
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

    struct Srv2 {
        gate: Condition,
        cnt: Cell<bool>,
        waker: LocalWaker,
    }

    impl Service<(), ()> for Srv2 {
        type Res = ();
        type Error = ();

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
            self.waker.wake();

            let _ = fut.await;
            self.cnt.set(false);
            self.waker.wake();
            Ok::<_, ()>(())
        }
    }

    /// `InflightService::poll_ready()` must always register waker,
    /// otherwise it can lose wake up if inner service's `poll_ready()`
    /// does not wakes dispatcher.
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

    /// Request with a payload, the flag indicates that payload chunks follow
    #[derive(Clone, Copy)]
    struct Req(bool);

    impl SizedRequest for Req {
        fn size(&self) -> u32 {
            12
        }

        fn has_more_chunks(&self) -> bool {
            self.0
        }
    }

    struct GateReqService(Condition);

    impl Service<(), Req> for GateReqService {
        type Res = ();
        type Error = ();

        async fn call(&self, _r: Req, _: Ctx<'_, Self, ()>) -> Result<(), ()> {
            let _ = self.0.wait().await;
            Ok(())
        }
    }

    async fn spawn_call(srv: &Pipeline<Req, (), ()>, req: Req) {
        let srv = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv.call(req).await;
        });
        sleep(Millis(25)).await;
    }

    async fn check_streaming(max_size: usize) {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(0, max_size, GateReqService(gate.clone())),
        );

        // streaming publish exceeds the size limit, chunks must still pass
        spawn_call(&srv, Req(true)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        spawn_call(&srv, Req(true)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        // the last chunk restores the limits
        spawn_call(&srv, Req(false)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        gate.notify_and_lock(());
        let res = timeout(Millis(5000), srv.ready()).await;
        assert_eq!(res, Ok(Ok(())));
    }

    #[ntex::test]
    async fn test_inflight_streaming_size() {
        check_streaming(10).await;
    }

    #[ntex::test]
    async fn test_inflight_complete_payload() {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(1, 10, GateReqService(gate.clone())),
        );

        // publish with the whole payload does not bypass the limits
        spawn_call(&srv, Req(false)).await;
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

    type LogSrv = Rc<InFlightServiceImpl<LogService>>;

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

    async fn spawn_msg(srv: &Pipeline<Msg, (), ()>, msg: Msg) {
        let srv = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv.call(msg).await;
        });
        sleep(Millis(10)).await;
    }

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

    #[ntex::test]
    async fn test_inflight_slots_waker_update() {
        use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
        use std::task::Wake;

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

    #[test]
    fn test_debug() {
        struct NoopSvc;
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
}
