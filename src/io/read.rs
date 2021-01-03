use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::state::{Flags, IoState};

pub(crate) struct IoRead<T, U>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    io: Rc<RefCell<T>>,
    state: IoState<U>,
}

impl<T, U> IoRead<T, U>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    pub(crate) fn new(io: Rc<RefCell<T>>, state: IoState<U>) -> Self {
        Self { io, state }
    }
}

impl<T, U> Future for IoRead<T, U>
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
            let mut io = self.io.borrow_mut();
            state.read_io(&mut *io, cx)
        }
    }
}
