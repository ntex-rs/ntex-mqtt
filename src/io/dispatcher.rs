//! Framed transport dispatcher
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{cell::RefCell, collections::VecDeque, future::Future, pin::Pin, rc::Rc};

use either::Either;
use futures::FutureExt;
use ntex::rt::time::{delay_until, Delay, Instant as RtInstant};
use ntex::service::{IntoService, Service};
use ntex::util::time::LowResTimeService;
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::state::Flags;
use crate::io::{DispatcherItem, IoRead, IoState, IoWrite};

type Response<U> = <U as Encoder>::Item;

/// Framed dispatcher - is a future that reads frames from Framed object
/// and pass then to the service.
#[pin_project::pin_project]
pub(crate) struct IoDispatcher<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    service: S,
    state: IoState<U>,
    inner: Rc<RefCell<IoDispatcherInner<S, U>>>,
    st: IoDispatcherState,
    updated: Instant,
    time: LowResTimeService,
    keepalive: Delay,
    keepalive_timeout: u16,
    #[pin]
    response: Option<S::Future>,
    response_idx: usize,
}

struct IoDispatcherInner<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    error: Option<IoDispatcherError<S, U>>,
    base: usize,
    queue: VecDeque<Poll<Result<S::Response, S::Error>>>,
}

#[derive(Copy, Clone, Debug)]
enum IoDispatcherState {
    Processing,
    Stop,
    Shutdown,
}

enum IoDispatcherError<S: Service, U: Encoder> {
    None,
    KeepAlive,
    Encoder(U::Error),
    Service(S::Error),
}

impl<S: Service, U: Encoder + Decoder> IoDispatcherError<S, U> {
    fn take(&mut self) -> Option<DispatcherItem<U>> {
        match self {
            IoDispatcherError::KeepAlive => {
                *self = IoDispatcherError::None;
                Some(DispatcherItem::KeepAliveTimeout)
            }
            IoDispatcherError::Encoder(_) => {
                let err = std::mem::replace(self, IoDispatcherError::None);
                match err {
                    IoDispatcherError::Encoder(err) => Some(DispatcherItem::EncoderError(err)),
                    _ => None,
                }
            }
            IoDispatcherError::None | IoDispatcherError::Service(_) => None,
        }
    }
}

impl<S: Service, U: Encoder> From<Either<S::Error, U::Error>> for IoDispatcherError<S, U> {
    fn from(err: Either<S::Error, U::Error>) -> Self {
        match err {
            Either::Left(err) => IoDispatcherError::Service(err),
            Either::Right(err) => IoDispatcherError::Encoder(err),
        }
    }
}

impl<S, U> IoDispatcher<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + 'static,
    <U as Encoder>::Item: 'static,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub fn with<T, F: IntoService<S>>(
        io: T,
        state: IoState<U>,
        service: F,
        time: LowResTimeService,
    ) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let keepalive_timeout = 30;
        let expire = RtInstant::from_std(time.now() + Duration::from_secs(30));
        let io = Rc::new(RefCell::new(io));

        ntex::rt::spawn(IoRead::new(io.clone(), state.clone()));
        ntex::rt::spawn(IoWrite::new(io, state.clone()));

        let inner = Rc::new(RefCell::new(IoDispatcherInner {
            error: None,
            base: 0,
            queue: VecDeque::new(),
        }));

        IoDispatcher {
            st: IoDispatcherState::Processing,
            service: service.into_service(),
            updated: time.now(),
            keepalive: delay_until(expire),
            response: None,
            response_idx: 0,
            state,
            inner,
            time,
            keepalive_timeout,
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub fn keepalive_timeout(mut self, timeout: u16) -> Self {
        self.keepalive_timeout = timeout;

        if timeout > 0 {
            let expire =
                RtInstant::from_std(self.time.now() + Duration::from_secs(timeout as u64));
            self.keepalive.reset(expire);
        }

        self
    }

    /// Set connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 1 seconds.
    pub fn disconnect_timeout(self, val: u16) -> Self {
        self.state.inner.borrow_mut().disconnect_timeout = val;
        self
    }
}

impl<S, U> Future for IoDispatcher<S, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>> + 'static,
    U: Decoder + Encoder + 'static,
    <U as Encoder>::Item: 'static,
{
    type Output = Result<(), S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();

        // log::trace!("IO-DISP poll :{:?}:", this.st);

        // handle service response future
        if let Some(fut) = this.response.as_mut().as_pin_mut() {
            match fut.poll(cx) {
                Poll::Pending => (),
                Poll::Ready(res) => {
                    let mut inner = this.inner.borrow_mut();
                    let idx = *this.response_idx - inner.base;
                    inner.queue[idx] = Poll::Ready(res);
                    drop(inner);
                    this = self.as_mut().project();
                    this.response.set(None);
                    this.state.inner.borrow_mut().flags.insert(Flags::DSP_READY);
                }
            }
        }

        // handle responses
        {
            let mut state = this.state.inner.borrow_mut();
            if state.flags.contains(Flags::DSP_READY) {
                let mut inner = this.inner.borrow_mut();
                while let Some(res) = inner.queue.front() {
                    if res.is_ready() {
                        inner.base = inner.base.wrapping_add(1);
                        if let Poll::Ready(item) = inner.queue.pop_front().unwrap() {
                            if let Some(err) = state.write_item(item) {
                                inner.error = Some(err.into());
                                state.flags.insert(Flags::DSP_STOP);
                            }
                        }
                    } else {
                        break;
                    }
                }
                state.flags.remove(Flags::DSP_READY);
            }
        }

        match this.st {
            IoDispatcherState::Processing => {
                // keepalive timer
                {
                    let mut state = this.state.inner.borrow_mut();

                    if !state.flags.contains(Flags::DSP_STOP) && *this.keepalive_timeout != 0 {
                        match Pin::new(&mut this.keepalive).poll(cx) {
                            Poll::Ready(_) => {
                                if this.keepalive.deadline()
                                    <= RtInstant::from_std(*this.updated)
                                {
                                    state.flags.insert(Flags::DSP_STOP);
                                    state.read_task.wake();
                                    this.inner.borrow_mut().error =
                                        Some(IoDispatcherError::KeepAlive);
                                } else {
                                    let expire = RtInstant::from_std(
                                        this.time.now()
                                            + Duration::from_secs(
                                                *this.keepalive_timeout as u64,
                                            ),
                                    );
                                    this.keepalive.reset(expire);
                                    let _ = Pin::new(&mut this.keepalive).poll(cx);
                                }
                            }
                            Poll::Pending => (),
                        }
                    }
                }

                loop {
                    let mut state = this.state.inner.borrow_mut();

                    // log::trace!("IO-DISP state :{:?}:", state.flags);

                    match this.service.poll_ready(cx) {
                        Poll::Ready(Ok(_)) => {
                            let mut retry = false;

                            let item = if state.flags.contains(Flags::DSP_STOP) {
                                // check for errors
                                let item = this
                                    .inner
                                    .borrow_mut()
                                    .error
                                    .as_mut()
                                    .and_then(|err| err.take())
                                    .or_else(|| {
                                        state.error.take().map(DispatcherItem::IoError)
                                    });
                                *this.st = IoDispatcherState::Stop;
                                retry = true;
                                item
                            } else {
                                // service is ready, wake io read task
                                if state.flags.contains(Flags::RD_PAUSED) {
                                    state.flags.remove(Flags::RD_PAUSED);
                                    state.read_task.wake();
                                }

                                // decode incoming bytes stream
                                if state.flags.contains(Flags::RD_READY) {
                                    log::trace!(
                                        "attempt to decode frame, buffer size is {}",
                                        state.read_buf.len()
                                    );

                                    match state.decode_item() {
                                        Ok(Some(el)) => {
                                            log::trace!(
                                                "frame is succesfully decoded, remaining buffer {}",
                                                state.read_buf.len()
                                            );
                                            *this.updated = this.time.now();
                                            Some(DispatcherItem::Item(el))
                                        }
                                        Ok(None) => {
                                            log::trace!("not enough data to decode next frame, register dispatch task");
                                            state.dispatch_task.register(cx.waker());
                                            state.flags.remove(Flags::RD_READY);
                                            return Poll::Pending;
                                        }
                                        Err(err) => {
                                            log::warn!("frame decode error");
                                            retry = true;
                                            *this.st = IoDispatcherState::Stop;
                                            state
                                                .flags
                                                .insert(Flags::DSP_STOP | Flags::IO_SHUTDOWN);
                                            state.write_task.wake();
                                            state.read_task.wake();
                                            Some(DispatcherItem::DecoderError(err))
                                        }
                                    }
                                } else {
                                    log::trace!(
                                        "read task is not ready, register dispatch task"
                                    );
                                    state.dispatch_task.register(cx.waker());
                                    return Poll::Pending;
                                }
                            };

                            // call service
                            if let Some(item) = item {
                                // optimize first call
                                if this.response.is_none() {
                                    drop(state);
                                    this.response.set(Some(this.service.call(item)));

                                    let mut inner = this.inner.borrow_mut();
                                    let response_idx = inner.base + inner.queue.len();

                                    if let Poll::Ready(res) =
                                        this.response.as_mut().as_pin_mut().unwrap().poll(cx)
                                    {
                                        state = this.state.inner.borrow_mut();

                                        if inner.queue.is_empty() {
                                            if let Some(err) = state.write_item(res) {
                                                inner.error = Some(err.into());
                                                state.flags.insert(Flags::DSP_STOP);
                                            }
                                        } else {
                                            *this.response_idx = response_idx;
                                            state.flags.insert(Flags::DSP_READY);
                                            inner.queue.push_back(Poll::Ready(res));
                                        }
                                        this.response.set(None);
                                    } else {
                                        state = this.state.inner.borrow_mut();
                                        *this.response_idx = response_idx;
                                        inner.queue.push_back(Poll::Pending);
                                    }
                                    drop(state);
                                } else {
                                    let mut inner = this.inner.borrow_mut();
                                    let response_idx = inner.base + inner.queue.len();
                                    inner.queue.push_back(Poll::Pending);
                                    drop(inner);
                                    drop(state);

                                    let st = this.state.clone();
                                    let inner = this.inner.clone();
                                    ntex::rt::spawn(this.service.call(item).map(move |item| {
                                        let mut inner = inner.borrow_mut();
                                        let idx = response_idx - inner.base;
                                        inner.queue[idx] = Poll::Ready(item);
                                        if idx == 0 {
                                            let mut state = st.inner.borrow_mut();
                                            state.flags.insert(Flags::DSP_READY);
                                            state.dispatch_task.wake();
                                        }
                                    }));
                                }
                            } else {
                                drop(state);
                            }

                            // run again
                            if retry {
                                return self.poll(cx);
                            }
                        }
                        Poll::Pending => {
                            // pause io read task
                            log::trace!("service is not ready, register dispatch task");
                            state.flags.insert(Flags::RD_PAUSED);
                            state.dispatch_task.register(cx.waker());
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => {
                            log::trace!("service readiness check failed, stopping");
                            // service readiness error
                            *this.st = IoDispatcherState::Stop;
                            this.inner.borrow_mut().error =
                                Some(IoDispatcherError::Service(err));
                            state.flags.insert(Flags::DSP_STOP);
                            drop(state);
                            return self.poll(cx);
                        }
                    }
                }
            }
            // drain service responses
            IoDispatcherState::Stop => {
                // service may relay on poll ready for response results
                match this.service.poll_ready(cx) {
                    Poll::Ready(Ok(_)) => (),
                    Poll::Pending => (),
                    Poll::Ready(Err(_)) => (),
                }

                let inner = this.inner.borrow_mut();
                let mut state = this.state.inner.borrow_mut();
                if inner.queue.is_empty() {
                    state.read_task.wake();
                    state.write_task.wake();
                    state.flags.insert(Flags::IO_SHUTDOWN);
                    *this.st = IoDispatcherState::Shutdown;
                    drop(inner);
                    drop(state);
                    self.poll(cx)
                } else {
                    state.dispatch_task.register(cx.waker());
                    Poll::Pending
                }
            }
            // shutdown service
            IoDispatcherState::Shutdown => {
                let is_err = this.inner.borrow().error.is_some();

                return if this.service.poll_shutdown(cx, is_err).is_ready() {
                    log::trace!("service shutdown is completed, stop");

                    Poll::Ready(
                        if let Some(IoDispatcherError::Service(err)) =
                            this.inner.borrow_mut().error.take()
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::future::FutureExt;

    use ntex::rt::time::delay_for;
    use ntex::testing::Io;
    use ntex_codec::BytesCodec;

    use super::*;

    impl<S, U> IoDispatcher<S, U>
    where
        S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
        S::Error: 'static,
        S::Future: 'static,
        U: Decoder + Encoder + 'static,
        <U as Encoder>::Item: 'static,
    {
        /// Construct new `Dispatcher` instance
        pub fn new<T, F: IntoService<S>>(io: T, codec: U, service: F) -> (Self, IoState<U>)
        where
            T: AsyncRead + AsyncWrite + Unpin + 'static,
        {
            let time = LowResTimeService::with(Duration::from_secs(1));
            let keepalive_timeout = 30;
            let updated = time.now();
            let expire = RtInstant::from_std(updated + Duration::from_secs(30));
            let state = IoState::new(codec);
            let io = Rc::new(RefCell::new(io));
            let inner = Rc::new(RefCell::new(IoDispatcherInner {
                error: None,
                base: 0,
                queue: VecDeque::new(),
            }));

            ntex::rt::spawn(IoRead::new(io.clone(), state.clone()));
            ntex::rt::spawn(IoWrite::new(io.clone(), state.clone()));

            (
                IoDispatcher {
                    service: service.into_service(),
                    state: state.clone(),
                    st: IoDispatcherState::Processing,
                    updated: time.now(),
                    keepalive: delay_until(expire),
                    response: None,
                    response_idx: 0,
                    time,
                    keepalive_timeout,
                    inner,
                },
                state,
            )
        }
    }

    #[ntex::test]
    async fn test_basic() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, _) = IoDispatcher::new(
            server,
            BytesCodec,
            ntex::fn_service(|msg: DispatcherItem<BytesCodec>| async move {
                delay_for(Duration::from_millis(50)).await;
                if let DispatcherItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex::rt::spawn(disp.map(|_| ()));

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        client.close().await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_sink() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, st) = IoDispatcher::new(
            server,
            BytesCodec,
            ntex::fn_service(|msg: DispatcherItem<BytesCodec>| async move {
                if let DispatcherItem::Item(msg) = msg {
                    Ok::<_, ()>(Some(msg.freeze()))
                } else {
                    panic!()
                }
            }),
        );
        ntex::rt::spawn(disp.disconnect_timeout(25).map(|_| ()));

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        assert!(st.inner.borrow_mut().send(Bytes::from_static(b"test")).is_ok());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"test"));

        st.inner.borrow_mut().close();
        delay_for(Duration::from_millis(200)).await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let (disp, state) = IoDispatcher::new(
            server,
            BytesCodec,
            ntex::fn_service(|_: DispatcherItem<BytesCodec>| async move {
                Err::<Option<Bytes>, _>(())
            }),
        );
        ntex::rt::spawn(disp.map(|_| ()));

        state.inner.borrow_mut().send(Bytes::from_static(b"GET /test HTTP/1\r\n\r\n")).unwrap();

        let buf = client.read_any();
        assert_eq!(buf, Bytes::from_static(b""));
        delay_for(Duration::from_millis(25)).await;

        // buffer should be flushed
        client.remote_buffer_cap(1024);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        // write side must be closed, dispatcher waiting for read side to close
        assert!(client.is_closed());

        // close read side
        client.close().await;
        assert!(client.is_server_dropped());
    }
}
