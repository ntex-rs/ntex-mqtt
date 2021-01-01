use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, task::Context, task::Poll};

use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::iostate::{Flags, IoBuffer, IoState};

pub(crate) struct IoRead<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    state: Rc<RefCell<IoState<T, E>>>,
    buffer: IoBuffer<U>,
}

impl<T, U, E> IoRead<T, U, E>
where
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
{
    pub(crate) fn new(state: Rc<RefCell<IoState<T, E>>>, buffer: IoBuffer<U>) -> Self {
        Self { state, buffer }
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
        let mut buffer = self.buffer.inner.borrow_mut();

        if buffer.flags.contains(Flags::IO_ERR | Flags::IO_SHUTDOWN) {
            Poll::Ready(())
        } else if buffer.flags.contains(Flags::RD_PAUSED) {
            buffer.read_task.register(cx.waker());
            Poll::Pending
        } else {
            buffer.read_io(self.state.borrow_mut().get_mut(), cx)
        }
    }
}
