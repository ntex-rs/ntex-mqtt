//! Service that limits number of in-flight async requests.
use std::{cell::Cell, future::Future, marker, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex::{service::Service, task::LocalWaker};

pub(crate) trait SizedRequest {
    fn size(&self) -> u32;
}

pub(crate) struct InFlightService<S> {
    count: Counter,
    service: S,
}

impl<S> InFlightService<S> {
    pub fn new(max_cap: u16, max_size: usize, service: S) -> Self {
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
    type Future<'f> = InFlightServiceResponse<'f, T, R> where Self: 'f;

    ntex::forward_poll_shutdown!(service);

    #[inline]
    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.service.poll_ready(cx)?.is_pending() {
            Poll::Pending
        } else if !self.count.available(cx) {
            log::trace!("InFlight limit exceeded");
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    #[inline]
    fn call(&self, req: R) -> Self::Future<'_> {
        let size = if self.count.0.max_size > 0 { req.size() } else { 0 };
        InFlightServiceResponse {
            _guard: self.count.get(size),
            _t: marker::PhantomData,
            fut: self.service.call(req),
        }
    }
}

pin_project_lite::pin_project! {
    #[doc(hidden)]
    pub struct InFlightServiceResponse<'f, T: Service<R>, R>
    where T: 'f, R: 'f
    {
        #[pin]
        fut: T::Future<'f>,
        _guard: CounterGuard,
        _t: marker::PhantomData<R>
    }
}

impl<'f, T: Service<R>, R> Future for InFlightServiceResponse<'f, T, R> {
    type Output = Result<T::Response, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
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

    fn available(&self, cx: &mut Context<'_>) -> bool {
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

    fn available(&self, cx: &mut Context<'_>) -> bool {
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
    use ntex::{service::Service, time::sleep, util::lazy, util::BoxFuture};
    use std::{task::Poll, time::Duration};

    use super::*;

    struct SleepService(Duration);

    impl Service<()> for SleepService {
        type Response = ();
        type Error = ();
        type Future<'f> = BoxFuture<'f, Result<(), ()>>;

        fn call(&self, _: ()) -> Self::Future<'_> {
            let fut = sleep(self.0);
            Box::pin(async move {
                let _ = fut.await;
                Ok::<_, ()>(())
            })
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

        let srv = InFlightService::new(1, 0, SleepService(wait_time));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let res = srv.call(());
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        let _ = res.await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
        assert!(lazy(|cx| srv.poll_shutdown(cx)).await.is_ready());
    }

    #[ntex::test]
    async fn test_inflight2() {
        let wait_time = Duration::from_millis(50);

        let srv = InFlightService::new(0, 10, SleepService(wait_time));
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));

        let res = srv.call(());
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Pending);

        let _ = res.await;
        assert_eq!(lazy(|cx| srv.poll_ready(cx)).await, Poll::Ready(Ok(())));
    }
}
