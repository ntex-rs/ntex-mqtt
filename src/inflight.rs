//! Service that limits number of in-flight async requests.
use std::{cell::Cell, future::poll_fn, rc::Rc, task::Context, task::Poll};

use ntex_service::{Service, ServiceCtx};
use ntex_util::{future::join, task::LocalWaker};

/// Trait for types that could be sized
pub trait SizedRequest {
    fn size(&self) -> u32;

    fn is_publish(&self) -> bool;

    fn is_chunk(&self) -> bool;
}

pub struct InFlightServiceImpl<S> {
    count: Counter,
    service: S,
    publish: Cell<bool>,
}

impl<S> InFlightServiceImpl<S> {
    pub fn new(max_cap: u16, max_size: usize, service: S) -> Self {
        InFlightServiceImpl {
            service,
            publish: Cell::new(false),
            count: Counter::new(max_cap, max_size),
        }
    }
}

impl<S, R> Service<R> for InFlightServiceImpl<S>
where
    S: Service<R>,
    R: SizedRequest + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), S::Error> {
        if self.publish.get() || self.count.is_available() {
            ctx.ready(&self.service).await
        } else {
            join(self.count.available(), ctx.ready(&self.service)).await.1
        }
    }

    #[inline]
    async fn call(&self, req: R, ctx: ServiceCtx<'_, Self>) -> Result<S::Response, S::Error> {
        // process payload chunks
        if self.publish.get() && !req.is_chunk() {
            self.publish.set(false);
        }
        if req.is_publish() {
            self.publish.set(true);
        }

        let size = if self.count.0.max_size > 0 { req.size() } else { 0 };
        let task_guard = self.count.get(size);
        let result = ctx.call(&self.service, req).await;
        drop(task_guard);
        result
    }

    ntex_service::forward_poll!(service);
    ntex_service::forward_shutdown!(service);
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
        .await
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
    use std::{future::poll_fn, time::Duration};

    use ntex_service::Pipeline;
    use ntex_util::{future::lazy, task::LocalWaker, time::sleep};

    use super::*;

    struct SleepService(Duration);

    impl Service<()> for SleepService {
        type Response = ();
        type Error = ();

        async fn call(&self, _: (), _: ServiceCtx<'_, Self>) -> Result<(), ()> {
            let fut = sleep(self.0);
            let _ = fut.await;
            Ok::<_, ()>(())
        }
    }

    impl SizedRequest for () {
        fn size(&self) -> u32 {
            12
        }

        fn is_publish(&self) -> bool {
            false
        }

        fn is_chunk(&self) -> bool {
            false
        }
    }

    #[ntex::test]
    async fn test_inflight() {
        let wait_time = Duration::from_millis(50);

        let srv = Pipeline::new(InFlightServiceImpl::new(1, 0, SleepService(wait_time))).bind();
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.clone();
        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        ntex_util::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        ntex_util::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        assert!(lazy(|cx| srv.poll_shutdown(cx)).await.is_ready());
    }

    #[ntex::test]
    async fn test_inflight2() {
        let wait_time = Duration::from_millis(50);

        let srv =
            Pipeline::new(InFlightServiceImpl::new(0, 10, SleepService(wait_time))).bind();
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.clone();
        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        ntex_util::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        ntex_util::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
    }

    struct Srv2 {
        dur: Duration,
        cnt: Cell<bool>,
        waker: LocalWaker,
    }

    impl Service<()> for Srv2 {
        type Response = ();
        type Error = ();

        async fn ready(&self, _: ServiceCtx<'_, Self>) -> Result<(), ()> {
            poll_fn(|cx| {
                if !self.cnt.get() {
                    Poll::Ready(Ok(()))
                } else {
                    self.waker.register(cx.waker());
                    Poll::Pending
                }
            })
            .await
        }

        async fn call(&self, _: (), _: ServiceCtx<'_, Self>) -> Result<(), ()> {
            let fut = sleep(self.dur);
            self.cnt.set(true);
            self.waker.wake();

            let _ = fut.await;
            self.cnt.set(false);
            self.waker.wake();
            Ok::<_, ()>(())
        }
    }

    /// InflightService::poll_ready() must always register waker,
    /// otherwise it can lose wake up if inner service's poll_ready
    /// does not wakes dispatcher.
    #[ntex::test]
    async fn test_inflight3() {
        let wait_time = Duration::from_millis(50);

        let srv = Pipeline::new(InFlightServiceImpl::new(
            1,
            10,
            Srv2 { dur: wait_time, cnt: Cell::new(false), waker: LocalWaker::new() },
        ))
        .bind();
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.clone();
        ntex_util::spawn(async move {
            let _ = srv2.call(()).await;
        });
        ntex_util::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        let srv2 = srv.clone();
        let (tx, rx) = ntex_util::channel::oneshot::channel();
        ntex_util::spawn(async move {
            let _ = poll_fn(|cx| srv2.poll_ready(cx)).await;
            let _ = tx.send(());
        });
        assert_eq!(poll_fn(|cx| srv.poll_ready(cx)).await, Ok(()));

        let _ = rx.await;
    }
}
