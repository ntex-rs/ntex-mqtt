//! Framed transport dispatcher
use std::task::{Context, Poll};
use std::{
    cell::Cell, cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc, time,
};

use ntex::codec::{Decoder, Encoder};
use ntex::io::{DispatchItem, Io, IoBoxed, IoRef, RecvError, Timer};
use ntex::service::{IntoService, Service};
use ntex::time::{now, Seconds};
use ntex::util::{ready, Either, Pool};

type Response<U> = <U as Encoder>::Item;

pin_project_lite::pin_project! {
    /// Dispatcher for mqtt protocol
    pub(crate) struct Dispatcher<S, U>
    where
        S: Service<DispatchItem<U>, Response = Option<Response<U>>>,
        S::Error: 'static,
        S::Future: 'static,
        U: Encoder,
        U: Decoder,
       <U as Encoder>::Item: 'static,
    {
        codec: U,
        service: S,
        state: Rc<RefCell<DispatcherState<S, U>>>,
        inner: DispatcherInner,
        st: IoDispatcherState,
        ready_err: bool,
        pool: Pool,
        #[pin]
        response: Option<S::Future>,
        response_idx: usize,
    }
}

struct DispatcherInner {
    io: IoBoxed,
    timer: Timer,
    updated: Cell<time::Instant>,
    keepalive_timeout: Cell<Seconds>,
}

struct DispatcherState<S: Service<DispatchItem<U>>, U: Encoder + Decoder> {
    error: Option<IoDispatcherError<S::Error, <U as Encoder>::Error>>,
    base: usize,
    queue: VecDeque<ServiceResult<Result<S::Response, S::Error>>>,
}

enum ServiceResult<T> {
    Pending,
    Ready(T),
}

impl<T> ServiceResult<T> {
    fn take(&mut self) -> Option<T> {
        let slf = std::mem::replace(self, ServiceResult::Pending);
        match slf {
            ServiceResult::Pending => None,
            ServiceResult::Ready(result) => Some(result),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum IoDispatcherState {
    Processing,
    Stop,
    Shutdown,
}

pub(crate) enum IoDispatcherError<S, U> {
    None,
    KeepAlive,
    Encoder(U),
    Service(S),
}

impl<S, U> From<S> for IoDispatcherError<S, U> {
    fn from(err: S) -> Self {
        IoDispatcherError::Service(err)
    }
}

impl<E1, E2: std::fmt::Debug> IoDispatcherError<E1, E2> {
    fn take<U>(&mut self) -> Option<DispatchItem<U>>
    where
        U: Encoder<Error = E2> + Decoder,
    {
        match self {
            IoDispatcherError::KeepAlive => {
                *self = IoDispatcherError::None;
                Some(DispatchItem::KeepAliveTimeout)
            }
            IoDispatcherError::Encoder(_) => {
                let err = std::mem::replace(self, IoDispatcherError::None);
                match err {
                    IoDispatcherError::Encoder(err) => Some(DispatchItem::EncoderError(err)),
                    _ => None,
                }
            }
            IoDispatcherError::None | IoDispatcherError::Service(_) => None,
        }
    }
}

impl<S, U> Dispatcher<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + Clone + 'static,
    <U as Encoder>::Item: 'static,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(crate) fn new<F: IntoService<S, DispatchItem<U>>>(
        io: IoBoxed,
        codec: U,
        service: F,
        timer: Timer,
    ) -> Self {
        let updated = Cell::new(now());
        let keepalive_timeout = Cell::new(Seconds(30));

        // register keepalive timer
        let expire = updated.get() + time::Duration::from(keepalive_timeout.get());
        timer.register(expire, expire, io.as_ref());

        let state = Rc::new(RefCell::new(DispatcherState {
            error: None,
            base: 0,
            queue: VecDeque::new(),
        }));
        let pool = io.memory_pool().pool();

        Dispatcher {
            state,
            codec,
            pool,
            st: IoDispatcherState::Processing,
            service: service.into_service(),
            response: None,
            response_idx: 0,
            ready_err: false,
            inner: DispatcherInner { io, timer, updated, keepalive_timeout },
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub(crate) fn keepalive_timeout(self, timeout: Seconds) -> Self {
        // register keepalive timer
        let prev =
            self.inner.updated.get() + time::Duration::from(self.inner.keepalive_timeout.get());
        if timeout.is_zero() {
            self.inner.timer.unregister(prev, self.inner.io.as_ref());
        } else {
            let expire = self.inner.updated.get() + time::Duration::from(timeout);
            self.inner.timer.register(expire, prev, self.inner.io.as_ref());
        }

        self.inner.keepalive_timeout.set(timeout);
        self
    }

    /// Set connection disconnect timeout.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 1 seconds.
    pub(crate) fn disconnect_timeout(self, val: Seconds) -> Self {
        self.inner.io.set_disconnect_timeout(val.into());
        self
    }
}

impl DispatcherInner {
    fn update_keepalive(&self) {
        // update keep-alive timer
        let ka = self.keepalive_timeout.get();
        if ka.non_zero() {
            let updated = now();
            if updated != self.updated.get() {
                let ka = time::Duration::from(ka);
                self.timer.register(updated + ka, self.updated.get() + ka, self.io.as_ref());
                self.updated.set(updated);
            }
        }
    }

    fn unregister_keepalive(&self) {
        // unregister keep-alive timer
        if self.keepalive_timeout.get().non_zero() {
            self.keepalive_timeout.set(Seconds::ZERO);
            self.timer.unregister(
                self.updated.get() + time::Duration::from(self.keepalive_timeout.get()),
                self.io.as_ref(),
            );
        }
    }
}

impl<S, U> DispatcherState<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    fn handle_result(
        &mut self,
        item: Result<S::Response, S::Error>,
        response_idx: usize,
        write: &IoRef,
        codec: &U,
        wake: bool,
    ) {
        let idx = response_idx.wrapping_sub(self.base);

        // handle first response
        if idx == 0 {
            let _ = self.queue.pop_front();
            self.base = self.base.wrapping_add(1);
            match item {
                Err(err) => {
                    self.error = Some(err.into());
                }
                Ok(Some(item)) => {
                    if let Err(err) = write.encode(item, codec) {
                        self.error = Some(IoDispatcherError::Encoder(err));
                    }
                }
                Ok(None) => (),
            }

            // check remaining response
            while let Some(item) = self.queue.front_mut().and_then(|v| v.take()) {
                let _ = self.queue.pop_front();
                self.base = self.base.wrapping_add(1);
                match item {
                    Err(err) => {
                        self.error = Some(err.into());
                    }
                    Ok(Some(item)) => {
                        if let Err(err) = write.encode(item, codec) {
                            self.error = Some(IoDispatcherError::Encoder(err));
                        }
                    }
                    Ok(None) => (),
                }
            }

            if wake && self.queue.is_empty() {
                write.wake_dispatcher()
            }
        } else {
            self.queue[idx] = ServiceResult::Ready(item);
        }
    }
}

impl<S, U> Future for Dispatcher<S, U>
where
    S: Service<DispatchItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + Clone + 'static,
    <U as Encoder>::Item: 'static,
{
    type Output = Result<(), S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        let io = &this.inner.io;

        // log::trace!("IO-DISP poll :{:?}:", this.st);

        // handle service response future
        if let Some(fut) = this.response.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Pending => (),
                Poll::Ready(item) => {
                    this.state.borrow_mut().handle_result(
                        item,
                        *this.response_idx,
                        io.as_ref(),
                        this.codec,
                        false,
                    );
                    this.response.set(None);
                }
            }
        }

        // handle memory pool pressure
        if this.pool.poll_ready(cx).is_pending() {
            io.pause();
            return Poll::Pending;
        }

        loop {
            match this.st {
                IoDispatcherState::Processing => {
                    // log::trace!("IO-DISP state :{:?}:", io.flags());

                    match this.service.poll_ready(cx) {
                        Poll::Ready(Ok(_)) => {
                            // decode incoming bytes stream
                            let item = match ready!(io.poll_recv(this.codec, cx)) {
                                Ok(el) => {
                                    // update keep-alive timer
                                    this.inner.update_keepalive();

                                    Some(DispatchItem::Item(el))
                                }
                                Err(RecvError::Stop) => {
                                    log::trace!("dispatcher is instructed to stop");
                                    let mut inner = this.state.borrow_mut();

                                    // check for errors
                                    let item = inner
                                        .error
                                        .as_mut()
                                        .and_then(|err| err.take())
                                        .or_else(|| {
                                            io.take_error()
                                                .map(|e| DispatchItem::Disconnect(Some(e)))
                                        });
                                    *this.st = IoDispatcherState::Stop;
                                    item
                                }
                                Err(RecvError::KeepAlive) => {
                                    // check keepalive timeout
                                    log::trace!("keepalive timeout");
                                    *this.st = IoDispatcherState::Stop;
                                    let mut inner = this.state.borrow_mut();
                                    if inner.error.is_none() {
                                        inner.error = Some(IoDispatcherError::KeepAlive);
                                    }
                                    Some(DispatchItem::KeepAliveTimeout)
                                }
                                Err(RecvError::WriteBackpressure) => {
                                    if let Err(err) = ready!(io.poll_flush(cx, false)) {
                                        *this.st = IoDispatcherState::Stop;
                                        Some(DispatchItem::Disconnect(Some(err)))
                                    } else {
                                        continue;
                                    }
                                }
                                Err(RecvError::Decoder(err)) => {
                                    *this.st = IoDispatcherState::Stop;
                                    Some(DispatchItem::DecoderError(err))
                                }
                                Err(RecvError::PeerGone(err)) => {
                                    *this.st = IoDispatcherState::Stop;
                                    Some(DispatchItem::Disconnect(err))
                                }
                            };

                            // call service
                            if let Some(item) = item {
                                // optimize first call
                                if this.response.is_none() {
                                    this.response.set(Some(this.service.call(item)));
                                    let res =
                                        this.response.as_mut().as_pin_mut().unwrap().poll(cx);

                                    let mut inner = this.state.borrow_mut();
                                    let response_idx =
                                        inner.base.wrapping_add(inner.queue.len() as usize);

                                    if let Poll::Ready(res) = res {
                                        // check if current result is only response atm
                                        if inner.queue.is_empty() {
                                            match res {
                                                Err(err) => {
                                                    inner.error = Some(err.into());
                                                }
                                                Ok(Some(item)) => {
                                                    if let Err(err) =
                                                        io.encode(item, this.codec)
                                                    {
                                                        inner.error = Some(
                                                            IoDispatcherError::Encoder(err),
                                                        );
                                                    }
                                                }
                                                Ok(None) => (),
                                            }
                                        } else {
                                            *this.response_idx = response_idx;
                                            inner.queue.push_back(ServiceResult::Ready(res));
                                        }
                                        this.response.set(None);
                                    } else {
                                        *this.response_idx = response_idx;
                                        inner.queue.push_back(ServiceResult::Pending);
                                    }
                                } else {
                                    let mut inner = this.state.borrow_mut();
                                    let response_idx =
                                        inner.base.wrapping_add(inner.queue.len() as usize);
                                    inner.queue.push_back(ServiceResult::Pending);

                                    let st = io.get_ref();
                                    let codec = this.codec.clone();
                                    let inner = this.state.clone();
                                    let fut = this.service.call(item);
                                    ntex::rt::spawn(async move {
                                        let item = fut.await;
                                        inner.borrow_mut().handle_result(
                                            item,
                                            response_idx,
                                            &st,
                                            &codec,
                                            true,
                                        );
                                    });
                                }
                            }
                        }
                        Poll::Pending => {
                            // pause io read task
                            log::trace!("service is not ready, pause read task");
                            io.pause();
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => {
                            log::trace!("service readiness check failed, stopping");
                            // service readiness error
                            *this.st = IoDispatcherState::Stop;
                            this.state.borrow_mut().error =
                                Some(IoDispatcherError::Service(err));
                            *this.ready_err = true;
                        }
                    }
                }
                // drain service responses and shutdown io
                IoDispatcherState::Stop => {
                    this.inner.unregister_keepalive();

                    // service may relay on poll_ready for response results
                    if !*this.ready_err {
                        let _ = this.service.poll_ready(cx);
                    }

                    if this.state.borrow().queue.is_empty() {
                        if io.poll_shutdown(cx).is_ready() {
                            *this.st = IoDispatcherState::Shutdown;
                            continue;
                        }
                    } else {
                        io.register_dispatcher(cx);
                    }
                    return Poll::Pending;
                }
                // shutdown service
                IoDispatcherState::Shutdown => {
                    let is_err = this.state.borrow().error.is_some();

                    return if this.service.poll_shutdown(cx, is_err).is_ready() {
                        log::trace!("service shutdown is completed, stop");

                        Poll::Ready(
                            if let Some(IoDispatcherError::Service(err)) =
                                this.state.borrow_mut().error.take()
                            {
                                Err(err)
                            } else {
                                Ok(())
                            },
                        )
                    } else {
                        Poll::Pending
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use ntex::channel::condition::Condition;
    use ntex::codec::BytesCodec;
    use ntex::io as nio;
    use ntex::testing::Io;
    use ntex::time::{sleep, Millis};
    use ntex::util::{Bytes, Ready};

    use super::*;

    impl<S, U> Dispatcher<S, U>
    where
        S: Service<DispatchItem<U>, Response = Option<Response<U>>>,
        S::Error: 'static,
        S::Future: 'static,
        U: Decoder + Encoder + 'static,
        <U as Encoder>::Item: 'static,
    {
        /// Construct new `Dispatcher` instance
        pub(crate) fn new_debug<F: IntoService<S, DispatchItem<U>>>(
            io: Io,
            codec: U,
            service: F,
        ) -> (Self, nio::IoRef) {
            let timer = Timer::new(Millis::ONE_SEC);
            let keepalive_timeout = Cell::new(Seconds(30));
            let updated = Cell::new(now());
            let io = nio::Io::new(io).seal();
            let rio = io.get_ref();

            let state = Rc::new(RefCell::new(DispatcherState {
                error: None,
                base: 0,
                queue: VecDeque::new(),
            }));

            (
                Dispatcher {
                    state,
                    codec,
                    service: service.into_service(),
                    st: IoDispatcherState::Processing,
                    response: None,
                    response_idx: 0,
                    pool: io.memory_pool().pool(),
                    ready_err: false,
                    inner: DispatcherInner {
                        timer,
                        updated,
                        keepalive_timeout,
                        io: IoBoxed::from(io),
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
            server,
            BytesCodec,
            ntex::service::fn_service(|msg: DispatchItem<BytesCodec>| async move {
                sleep(Millis(50)).await;
                if let DispatchItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex::rt::spawn(async move {
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
    async fn test_ordering() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("test");

        let condition = Condition::new();
        let waiter = condition.wait();

        let (disp, _) = Dispatcher::new_debug(
            server,
            BytesCodec,
            ntex::service::fn_service(move |msg: DispatchItem<BytesCodec>| {
                let waiter = waiter.clone();
                async move {
                    waiter.await;
                    if let DispatchItem::Item(msg) = msg {
                        Ok::<_, ()>(Some(msg.freeze()))
                    } else {
                        panic!()
                    }
                }
            }),
        );
        ntex::rt::spawn(async move {
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

    #[ntex::test]
    async fn test_sink() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            server,
            BytesCodec,
            ntex::service::fn_service(|msg: DispatchItem<BytesCodec>| async move {
                if let DispatchItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex::rt::spawn(async move {
            let _ = disp.disconnect_timeout(Seconds(1)).await;
        });

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        assert!(io.encode(Bytes::from_static(b"test"), &BytesCodec).is_ok());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"test"));

        io.close();
        sleep(Millis(1200)).await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, io) = Dispatcher::new_debug(
            server,
            BytesCodec,
            ntex::service::fn_service(|_: DispatchItem<BytesCodec>| async move {
                Err::<Option<Bytes>, _>(())
            }),
        );
        ntex::rt::spawn(async move {
            let _ = disp.await;
        });

        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &BytesCodec).unwrap();

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        assert!(!client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service_ready() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let counter = Rc::new(Cell::new(0));

        struct Srv(Rc<Cell<usize>>);

        impl Service<DispatchItem<BytesCodec>> for Srv {
            type Response = Option<Response<BytesCodec>>;
            type Error = ();
            type Future = Ready<Option<Response<BytesCodec>>, ()>;

            fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
                self.0.set(self.0.get() + 1);
                Poll::Ready(Err(()))
            }

            fn call(&self, _: DispatchItem<BytesCodec>) -> Self::Future {
                Ready::Ok(None)
            }
        }

        let (disp, io) = Dispatcher::new_debug(server, BytesCodec, Srv(counter.clone()));
        io.encode(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"), &mut BytesCodec).unwrap();
        ntex::rt::spawn(async move {
            let _ = disp.await;
        });

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        assert!(client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());

        // service must be checked for readiness only once
        assert_eq!(counter.get(), 1);
    }
}
