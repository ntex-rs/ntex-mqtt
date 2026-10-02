//! Service that limits number of in-flight async requests.
use std::{cell::Cell, fmt, future::poll_fn, rc::Rc, task::Context, task::Poll};

use ntex_service::{Ctx, Service};
use ntex_util::{future::join, task::LocalWaker};

/// Trait for types that could be sized
pub trait SizedRequest {
    /// Encoded size of the request in bytes
    fn size(&self) -> u32;

    /// Check if more payload chunks of a streaming publish follow this request
    ///
    /// While chunks are pending, readiness ignores the in-flight limits so that
    /// the payload can reach the publish handler that holds an in-flight slot.
    fn has_more_chunks(&self) -> bool;
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

        let size = if self.count.0.max_size > 0 { req.size() } else { 0 };
        let task_guard = self.count.get(size);
        let result = ctx.call(&self.service, req).await;
        drop(task_guard);
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
}

impl Counter {
    fn new(max_cap: u16, max_size: usize) -> Self {
        Counter(Rc::new(CounterInner {
            max_cap,
            max_size,
            cur_cap: Cell::new(0),
            cur_size: Cell::new(0),
            task: LocalWaker::new(),
        }))
    }

    fn get(&self, size: u32) -> CounterGuard {
        CounterGuard::new(size, self.0.clone())
    }

    fn is_available(&self) -> bool {
        (self.0.max_cap == 0 || self.0.cur_cap.get() < self.0.max_cap)
            && (self.0.max_size == 0 || self.0.cur_size.get() <= self.0.max_size)
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
}

struct CounterGuard(u32, Rc<CounterInner>);

impl CounterGuard {
    fn new(size: u32, inner: Rc<CounterInner>) -> Self {
        inner.inc(size);
        CounterGuard(size, inner)
    }
}

impl Unpin for CounterGuard {}

impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.1.dec(self.0);
    }
}

impl CounterInner {
    fn inc(&self, size: u32) {
        let cur_cap = self.cur_cap.get() + 1;
        self.cur_cap.set(cur_cap);
        let cur_size = self.cur_size.get() + size as usize;
        self.cur_size.set(cur_size);

        if cur_cap == self.max_cap || cur_size >= self.max_size {
            self.task.wake();
        }
    }

    fn dec(&self, size: u32) {
        let num = self.cur_cap.get();
        self.cur_cap.set(num - 1);

        let cur_size = self.cur_size.get();
        let new_size = cur_size - (size as usize);
        self.cur_size.set(new_size);

        if num == self.max_cap || (cur_size > self.max_size && new_size <= self.max_size) {
            self.task.wake();
        }
    }

    fn available(&self, cx: &Context<'_>) -> bool {
        self.task.register(cx.waker());
        (self.max_cap == 0 || self.cur_cap.get() < self.max_cap)
            && (self.max_size == 0 || self.cur_size.get() <= self.max_size)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering};
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
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(1, 0, GateService(gate.clone())),
        );
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.bind();
        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        sleep(Millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        // wait for in-flight call to complete
        gate.notify_and_lock(());
        let res = timeout(Millis(5000), srv.ready()).await;
        assert_eq!(res, Ok(Ok(())));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
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
    async fn check_ready_woken(max_cap: u16, max_size: usize) {
        let gate = Condition::new();
        let svc = Rc::new(InFlightServiceImpl::new(
            max_cap,
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
    async fn test_inflight_cap_wakes_ready() {
        check_ready_woken(1, 0).await;
    }

    #[ntex::test]
    async fn test_inflight_size_wakes_ready() {
        check_ready_woken(0, 10).await;
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
    fn test_counter_cap_waker() {
        let counter = Counter::new(2, 0);
        let g1 = counter.get(0);
        let (w, available) = register(&counter);
        assert!(available);

        // counter becomes full
        let g2 = counter.get(0);
        assert_eq!(w.count(), 1);
        let (w, available) = register(&counter);
        assert!(!available);

        // capacity is released
        drop(g1);
        assert_eq!(w.count(), 1);
        assert!(counter.is_available());

        let (w, available) = register(&counter);
        assert!(available);
        drop(g2);
        assert_eq!(w.count(), 0);
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

    async fn check_streaming(max_cap: u16, max_size: usize) {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(max_cap, max_size, GateReqService(gate.clone())),
        );

        // streaming publish holds the only slot, chunks must still pass
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
    async fn test_inflight_streaming_cap() {
        check_streaming(1, 0).await;
    }

    #[ntex::test]
    async fn test_inflight_streaming_size() {
        check_streaming(0, 10).await;
    }

    #[ntex::test]
    async fn test_inflight_complete_payload() {
        let gate = Condition::new();
        let srv = Pipeline::new(
            (),
            InFlightServiceImpl::new(1, 0, GateReqService(gate.clone())),
        );

        // publish with the whole payload does not bypass the limits
        spawn_call(&srv, Req(false)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        gate.notify_and_lock(());
        let res = timeout(Millis(5000), srv.ready()).await;
        assert_eq!(res, Ok(Ok(())));
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
