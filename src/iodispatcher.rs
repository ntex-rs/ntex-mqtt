//! Framed transport dispatcher
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc};

use futures::FutureExt;
use ntex::rt::time::{delay_until, Delay, Instant as RtInstant};
use ntex::service::{IntoService, Service};
use ntex::util::time::LowResTimeService;
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use crate::ioread::IoRead;
use crate::iostate::{DispatcherItem, Flags, IoBuffer, IoState};
use crate::iowrite::IoWrite;

type Response<U> = <U as Encoder>::Item;

/// Framed dispatcher - is a future that reads frames from Framed object
/// and pass then to the service.
#[pin_project::pin_project]
pub(crate) struct IoDispatcher<S, T, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    io: Rc<RefCell<IoState<T, S::Error>>>,
    state: IoBuffer<U>,

    service: S,
    st: IoDispatcherState,

    updated: Instant,
    time: LowResTimeService,
    keepalive: Delay,
    keepalive_timeout: Duration,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum IoDispatcherState {
    Processing,
    Stop,
    Shutdown,
}

// #[cfg(test)]
// impl<S, T, U> IoDispatcher<S, T, U>
// where
//     S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
//     S::Error: 'static,
//     S::Future: 'static,
//     T: AsyncRead + AsyncWrite + Unpin,
//     U: Decoder + Encoder,
//     <U as Encoder>::Item: 'static,
// {
//     /// Construct new `Dispatcher` instance
//     pub fn new<F: IntoService<S>>(framed: Framed<T, U>, service: F) -> Self {
//         let time = LowResTimeService::with(Duration::from_secs(1));
//         let keepalive_timeout = Duration::from_secs(30);
//         let updated = time.now();
//         let expire = RtInstant::from_std(updated + keepalive_timeout);

//         let state = Rc::new(RefCell::new(IoState {
//             framed,
//             time,
//             updated,
//             keepalive_timeout,
//             sink: None,
//             rx: mpsc::channel().1,
//             service: service.into_service(),
//             flags: Flags::empty(),
//             state: FramedState::Processing,
//             disconnect_timeout: 1000,
//             keepalive: delay_until(expire),
//             dispatch_queue: VecDeque::with_capacity(16),
//         }));

//         Dispatcher { state, st: IoDispatcherState::Processing }
//     }
// }

impl<S, T, U> IoDispatcher<S, T, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    U: Decoder + Encoder + 'static,
    <U as Encoder>::Item: 'static,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub fn with<F: IntoService<S>>(
        io: T,
        state: IoBuffer<U>,
        service: F,
        time: LowResTimeService,
    ) -> Self {
        let keepalive_timeout = Duration::from_secs(30);
        let expire = RtInstant::from_std(time.now() + keepalive_timeout);

        let io = Rc::new(RefCell::new(IoState { io, error: None }));

        ntex::rt::spawn(IoRead::new(io.clone(), state.clone()));
        ntex::rt::spawn(IoWrite::new(io.clone(), state.clone()));

        IoDispatcher {
            service: service.into_service(),
            st: IoDispatcherState::Processing,
            updated: time.now(),
            keepalive: delay_until(expire),
            time,
            io,
            state,
            keepalive_timeout,
        }
    }

    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub fn keepalive_timeout(mut self, timeout: usize) -> Self {
        self.keepalive_timeout = Duration::from_secs(timeout as u64);

        if timeout > 0 {
            let expire = RtInstant::from_std(self.time.now() + self.keepalive_timeout);
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
    pub fn disconnect_timeout(self, val: u64) -> Self {
        self.state.inner.borrow_mut().disconnect_timeout = val;
        self
    }
}

impl<S, T, U> Future for IoDispatcher<S, T, U>
where
    S: Service<Request = DispatcherItem<U>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    U: Decoder + Encoder + 'static,
    <U as Encoder>::Item: 'static,
{
    type Output = Result<(), S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().get_mut();

        match this.st {
            IoDispatcherState::Processing => {
                let mut state = this.state.inner.borrow_mut();

                // keepalive timer
                if !state.flags.contains(Flags::DSP_STOP)
                    && this.keepalive_timeout != Duration::from_secs(0)
                {
                    match Pin::new(&mut this.keepalive).poll(cx) {
                        Poll::Ready(_) => {
                            if this.keepalive.deadline() <= RtInstant::from_std(this.updated) {
                                state
                                    .dispatch_queue
                                    .push_back(DispatcherItem::KeepAliveTimeout);
                                state.flags.insert(Flags::DSP_STOP);
                                state.read_task.wake()
                            } else {
                                let expire = RtInstant::from_std(
                                    this.time.now() + this.keepalive_timeout,
                                );
                                this.keepalive.reset(expire);
                                let _ = Pin::new(&mut this.keepalive).poll(cx);
                            }
                        }
                        Poll::Pending => (),
                    }
                }

                loop {
                    match this.service.poll_ready(cx) {
                        Poll::Ready(Ok(_)) => {
                            // handle write error
                            if let Some(item) = state.dispatch_queue.pop_front() {
                                // call service
                                let st = this.state.clone();
                                state.dispatch_inflight += 1;
                                ntex::rt::spawn(
                                    this.service.call(item).map(move |item| {
                                        st.inner.borrow_mut().write_item(item)
                                    }),
                                );
                                continue;
                            } else if state.flags.contains(Flags::DSP_STOP) {
                                this.st = IoDispatcherState::Stop;
                                drop(state);
                                return self.poll(cx);
                            }

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
                                let mut error = false;

                                let item = match state.decode_item() {
                                    Ok(Some(el)) => {
                                        log::trace!(
                                            "frame is succesfully decoded, remaining buffer {}",
                                            state.read_buf.len()
                                        );
                                        this.updated = this.time.now();
                                        DispatcherItem::Item(el)
                                    }
                                    Ok(None) => {
                                        log::trace!("not enough data to decode next frame, register dispatch task");
                                        state.dispatch_task.register(cx.waker());
                                        state.flags.remove(Flags::RD_READY);
                                        return Poll::Pending;
                                    }
                                    Err(err) => {
                                        log::warn!("frame decode error");
                                        error = true;
                                        this.st = IoDispatcherState::Stop;
                                        state
                                            .flags
                                            .insert(Flags::DSP_STOP | Flags::IO_SHUTDOWN);
                                        state.write_task.wake();
                                        state.read_task.wake();
                                        DispatcherItem::DecoderError(err)
                                    }
                                };

                                // call service
                                let st = this.state.clone();
                                state.dispatch_inflight += 1;
                                ntex::rt::spawn(
                                    this.service.call(item).map(move |item| {
                                        st.inner.borrow_mut().write_item(item)
                                    }),
                                );

                                // handle decoder error
                                if error {
                                    drop(state);
                                    return self.poll(cx);
                                }
                            } else {
                                log::trace!("not enough data to decode next frame, register dispatch task");
                                state.dispatch_task.register(cx.waker());
                                return Poll::Pending;
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
                            this.st = IoDispatcherState::Stop;
                            this.io.borrow_mut().error = Some(err);
                            state.flags.insert(Flags::DSP_STOP);
                            drop(state);
                            return self.poll(cx);
                        }
                    }
                }
            }
            // drain service responses
            IoDispatcherState::Stop => {
                let mut state = this.state.inner.borrow_mut();

                if state.dispatch_inflight == 0 {
                    state.read_task.wake();
                    state.write_task.wake();
                    state.flags.insert(Flags::IO_SHUTDOWN);
                    this.st = IoDispatcherState::Shutdown;
                    drop(state);
                    self.poll(cx)
                } else {
                    state.dispatch_task.register(cx.waker());
                    Poll::Pending
                }
            }
            // shutdown service
            IoDispatcherState::Shutdown => {
                let is_err = this.io.borrow().error.is_some();

                return if this.service.poll_shutdown(cx, is_err).is_ready() {
                    log::trace!("service shutdown is completed, stop");

                    Poll::Ready(if let Some(err) = this.io.borrow_mut().error.take() {
                        Err(err)
                    } else {
                        Ok(())
                    })
                } else {
                    Poll::Pending
                };
            }
        }
    }
}
