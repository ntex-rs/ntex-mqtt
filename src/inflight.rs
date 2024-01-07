//! Service that limits number of in-flight async requests.
use std::{cell::Cell, rc::Rc, task::Context, task::Poll};

use ntex::service::{Service, ServiceCtx};
use ntex::task::LocalWaker;

pub(crate) trait SizedRequest {
    fn size(&self) -> u32;
}

pub(crate) struct InFlightService<S> {
    count: Counter,
    service: S,
}

impl<S> InFlightService<S> {
    pub(crate) fn new(max_cap: u16, max_size: usize, service: S) -> Self {
        Self { service, count: Counter::new(max_cap, max_size) }
    }
}

impl<T, R> Service<R> for InFlightService<T>
where
    T: Service<R>,
    R: SizedRequest + 'static,
{
    type Response = T::Response;
    type Error = T::Error;

    ntex::forward_poll_shutdown!(service);

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let p1 = self.service.poll_ready(cx)?.is_pending();
        let p2 = !self.count.available(cx);
        if p2 {
            log::trace!("InFlight limit exceeded");
        }

        if p1 || p2 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    #[inline]
    async fn call(&self, req: R, ctx: ServiceCtx<'_, Self>) -> Result<T::Response, T::Error> {
        let size = if self.count.0.max_size > 0 { req.size() } else { 0 };
        let _task_guard = self.count.get(size);
        ctx.call(&self.service, req).await
    }
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

    fn available(&self, cx: &Context<'_>) -> bool {
        self.0.available(cx)
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
        self.cur_cap.set(self.cur_cap.get() + 1);
        self.cur_size.set(self.cur_size.get() + size as usize);
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
        if (self.max_cap == 0 || self.cur_cap.get() < self.max_cap)
            && (self.max_size == 0 || self.cur_size.get() <= self.max_size)
        {
            true
        } else {
            self.task.register(cx.waker());
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, future::poll_fn, task::Poll, time::Duration};

    use ntex::{service::Pipeline, service::Service, time::sleep, util::lazy};

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
    }

    #[ntex::test]
    async fn test_inflight() {
        let wait_time = Duration::from_millis(50);

        let srv = Pipeline::new(InFlightService::new(1, 0, SleepService(wait_time)));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.clone();
        ntex::rt::spawn(async move {
            let _ = srv2.call(()).await;
        });
        ntex::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        ntex::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        assert!(lazy(|cx| srv.poll_shutdown(cx)).await.is_ready());
    }

    #[ntex::test]
    async fn test_inflight2() {
        let wait_time = Duration::from_millis(50);

        let srv = Pipeline::new(InFlightService::new(0, 10, SleepService(wait_time)));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.clone();
        ntex::rt::spawn(async move {
            let _ = srv2.call(()).await;
        });
        ntex::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        ntex::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
    }

    struct Srv2 {
        dur: Duration,
        cnt: Cell<bool>,
    }

    impl Service<()> for Srv2 {
        type Response = ();
        type Error = ();

        fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            if !self.cnt.get() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        async fn call(&self, _: (), _: ServiceCtx<'_, Self>) -> Result<(), ()> {
            let fut = sleep(self.dur);
            self.cnt.set(true);

            let _ = fut.await;
            self.cnt.set(false);
            Ok::<_, ()>(())
        }
    }

    /// InflightService::poll_ready() must always register waker,
    /// otherwise it can lose wake up if inner service's poll_ready
    /// does not wakes dispatcher.
    #[ntex::test]
    async fn test_inflight3() {
        let wait_time = Duration::from_millis(50);

        let srv = Pipeline::new(InFlightService::new(
            1,
            10,
            Srv2 { dur: wait_time, cnt: Cell::new(false) },
        ));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let srv2 = srv.clone();
        ntex::rt::spawn(async move {
            let _ = srv2.call(()).await;
        });
        ntex::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        let srv2 = srv.clone();
        let (tx, rx) = ntex::channel::oneshot::channel();
        ntex::rt::spawn(async move {
            let _ = poll_fn(|cx| srv2.poll_ready(cx)).await;
            let _ = tx.send(());
        });

        let _ = rx.await;
    }
}
