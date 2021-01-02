use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, time::Duration};

use ntex::rt::time::{delay_for, Delay};
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::{Flags, Io, IoDispatcherError, IoState};

#[derive(Debug)]
pub(crate) enum IoWriteState {
    Processing,
    Shutdown(Option<Delay>),
}

pub(crate) struct IoWrite<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    st: IoWriteState,
    io: Rc<RefCell<Io<T, E>>>,
    state: IoState<U>,
}

impl<T, U, E> IoWrite<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    pub(crate) fn new(io: Rc<RefCell<Io<T, E>>>, state: IoState<U>) -> Self {
        Self { io, state, st: IoWriteState::Processing }
    }
}

impl<T, U, E> Future for IoWrite<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let mut state = this.state.inner.borrow_mut();

        // if IO error occured
        if state.flags.intersects(Flags::IO_ERR) {
            log::trace!("io is closed");
            return Poll::Ready(());
        }

        match this.st {
            IoWriteState::Processing => {
                if state.flags.contains(Flags::IO_SHUTDOWN) {
                    log::trace!("write task is instructed to shutdown");

                    this.st = IoWriteState::Shutdown(if state.disconnect_timeout != 0 {
                        Some(delay_for(Duration::from_millis(state.disconnect_timeout)))
                    } else {
                        None
                    });
                    drop(state);
                    return self.poll(cx);
                }

                // flush framed instance, do actual IO
                if !state.write_buf.is_empty() {
                    match state.flush_io(this.io.borrow_mut().get_mut(), cx) {
                        Poll::Ready(Ok(_)) | Poll::Pending => (),
                        Poll::Ready(Err(err)) => {
                            log::trace!("error sending data: {:?}", err);
                            state.read_task.wake();
                            state.dispatch_queue.push_back(IoDispatcherError::Io(err));
                            state.dispatch_task.wake();
                            state.flags.insert(Flags::IO_ERR);
                            return Poll::Ready(());
                        }
                    }
                }
            }
            IoWriteState::Shutdown(ref mut delay) => {
                // close frame, closes io WRITE side and wait for disconnect
                // on read side. we need disconnect timeout, otherwise it
                // could hang forever.

                return if let Poll::Ready(_) =
                    state.close_io(this.io.borrow_mut().get_mut(), cx)
                {
                    state.flags.insert(Flags::IO_ERR);
                    state.read_task.wake();
                    log::trace!("write task is closed");
                    Poll::Ready(())
                } else {
                    if let Some(ref mut delay) = delay {
                        futures::ready!(Pin::new(delay).poll(cx));
                    }
                    state.flags.insert(Flags::IO_ERR);
                    state.read_task.wake();
                    log::trace!("write task is closed after delay");
                    Poll::Ready(())
                };
            }
        }

        state.write_task.register(cx.waker());

        Poll::Pending
    }
}
