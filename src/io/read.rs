use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::state::{Flags, Io, IoState};

pub(crate) struct IoRead<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    io: Rc<RefCell<Io<T, E>>>,
    state: IoState<U>,
}

impl<T, U, E> IoRead<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    pub(crate) fn new(io: Rc<RefCell<Io<T, E>>>, state: IoState<U>) -> Self {
        Self { io, state }
    }
}

impl<T, U, E> Future for IoRead<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.inner.borrow_mut();

        if state.flags.contains(Flags::IO_ERR | Flags::IO_SHUTDOWN) {
            Poll::Ready(())
        } else if state.flags.contains(Flags::RD_PAUSED) {
            state.read_task.register(cx.waker());
            Poll::Pending
        } else {
            state.read_io(self.io.borrow_mut().get_mut(), cx)
        }
    }
}
