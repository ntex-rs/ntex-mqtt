use std::task::{Context, Poll};
use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, time::Duration};

use ntex::rt::time::{delay_for, Delay};
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::{state::Flags, IoState};

#[derive(Debug)]
pub(crate) enum IoWriteState {
    Processing,
    Shutdown(Option<Delay>),
}

pub(crate) struct IoWrite<T, U>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    st: IoWriteState,
    io: Rc<RefCell<T>>,
    state: IoState<U>,
}

impl<T, U> IoWrite<T, U>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    pub(crate) fn new(io: Rc<RefCell<T>>, state: IoState<U>) -> Self {
        Self { io, state, st: IoWriteState::Processing }
    }
}

impl<T, U> Future for IoWrite<T, U>
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
            log::trace!("write io is closed");
            return Poll::Ready(());
        }

        match this.st {
            IoWriteState::Processing => {
                if state.flags.contains(Flags::IO_SHUTDOWN) {
                    log::trace!("write task is instructed to shutdown");

                    this.st = IoWriteState::Shutdown(if state.disconnect_timeout != 0 {
                        Some(delay_for(Duration::from_millis(state.disconnect_timeout as u64)))
                    } else {
                        None
                    });
                    drop(state);
                    return self.poll(cx);
                }

                // flush framed instance, do actual IO
                if !state.write_buf.is_empty() {
                    match state.flush_io(&mut *this.io.borrow_mut(), cx) {
                        Poll::Ready(Ok(_)) | Poll::Pending => (),
                        Poll::Ready(Err(err)) => {
                            log::trace!("error sending data: {:?}", err);
                            state.read_task.wake();
                            state.error = Some(err);
                            state.dispatch_task.wake();
                            state.flags.insert(Flags::IO_ERR | Flags::DSP_STOP);
                            return Poll::Ready(());
                        }
                    }
                }
            }
            IoWriteState::Shutdown(ref mut delay) => {
                // close io, closes WRITE side and wait for disconnect
                // on read side. we have to use disconnect timeout, otherwise it
                // could hang forever.

                if state.close_io(&mut *this.io.borrow_mut(), cx).is_ready() {
                    state.flags.insert(Flags::IO_ERR);
                    state.read_task.wake();
                    log::trace!("write task is closed");
                } else {
                    if let Some(ref mut delay) = delay {
                        futures::ready!(Pin::new(delay).poll(cx));
                    }
                    state.flags.insert(Flags::IO_ERR);
                    state.read_task.wake();
                    log::trace!("write task is closed after delay");
                }
                return Poll::Ready(());
            }
        }

        state.write_task.register(cx.waker());

        Poll::Pending
    }
}
