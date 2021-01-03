//! Framed transport dispatcher
use std::{
    cell::RefCell, fmt, hash::Hash, hash::Hasher, io, mem, pin::Pin, rc::Rc, task::Context,
    task::Poll,
};

use bytes::{Buf, BytesMut};
use either::Either;
use futures::{future::poll_fn, ready};
use ntex::task::LocalWaker;
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder};

use super::dispatcher::IoDispatcherError;

type Request<U> = <U as Decoder>::Item;
type Response<U> = <U as Encoder>::Item;

const LW: usize = 1024;
const HW: usize = 8 * 1024;

bitflags::bitflags! {
    pub(crate) struct Flags: u8 {
        const DSP_STOP       = 0b0000_0001;
        const DSP_KEEPALIVE  = 0b0000_0100;

        const IO_ERR         = 0b0000_1000;
        const IO_SHUTDOWN    = 0b0001_0000;

        /// pause io read
        const RD_PAUSED      = 0b0010_0000;
        /// new data is available
        const RD_READY       = 0b0100_0000;

        const ST_DSP_ERR     = 0b1000_0000;
        const ST_IO_SHUTDOWN = 0b0000_0010;
    }
}

/// Framed transport item
pub(crate) enum DispatcherItem<U: Encoder + Decoder> {
    Item(Request<U>),
    /// Keep alive timeout
    KeepAliveTimeout,
    /// Decoder parse error
    DecoderError(<U as Decoder>::Error),
    /// Encoder parse error
    EncoderError(<U as Encoder>::Error),
    /// Unexpected io error
    IoError(io::Error),
}

impl<U> fmt::Debug for DispatcherItem<U>
where
    U: Encoder + Decoder,
    <U as Decoder>::Item: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DispatcherItem::Item(ref item) => write!(fmt, "DispatcherItem::Item({:?})", item),
            DispatcherItem::KeepAliveTimeout => write!(fmt, "DispatcherItem::KeepAliveTimeout"),
            DispatcherItem::EncoderError(ref e) => {
                write!(fmt, "DispatcherItem::EncoderError({:?})", e)
            }
            DispatcherItem::DecoderError(ref e) => {
                write!(fmt, "DispatcherItem::DecoderError({:?})", e)
            }
            DispatcherItem::IoError(ref e) => write!(fmt, "DispatcherItem::IoError({:?})", e),
        }
    }
}

pub struct IoState<U> {
    pub(crate) inner: Rc<RefCell<IoStateInner<U>>>,
}

pub(crate) struct IoStateInner<U> {
    codec: U,
    pub(crate) flags: Flags,
    pub(crate) error: Option<io::Error>,
    pub(crate) disconnect_timeout: u16,

    pub(crate) dispatch_task: LocalWaker,

    pub(crate) write_buf: BytesMut,
    pub(crate) write_task: LocalWaker,

    pub(crate) read_buf: BytesMut,
    pub(crate) read_task: LocalWaker,
}

impl<U> IoState<U> {
    pub(crate) fn keepalive_timeout(&self) {
        let mut state = self.inner.borrow_mut();
        state.flags.insert(Flags::DSP_STOP | Flags::DSP_KEEPALIVE);
        state.dispatch_task.wake();
    }
}

impl<U> IoState<U>
where
    U: Encoder + Decoder,
{
    pub(crate) fn new(codec: U) -> Self {
        IoState {
            inner: Rc::new(RefCell::new(IoStateInner {
                codec,
                flags: Flags::empty(),
                error: None,
                disconnect_timeout: 1000,

                dispatch_task: LocalWaker::new(),

                write_buf: BytesMut::new(),
                write_task: LocalWaker::new(),

                read_buf: BytesMut::new(),
                read_task: LocalWaker::new(),
            })),
        }
    }

    #[inline]
    /// Consume the `IoState`, returning `IoState` with different codec.
    pub fn map_codec<F, U2>(self, f: F) -> IoState<U2>
    where
        F: Fn(&U) -> U2,
        U2: Encoder + Decoder,
    {
        let mut st = self.inner.borrow_mut();
        let codec = f(&st.codec);

        IoState {
            inner: Rc::new(RefCell::new(IoStateInner {
                codec,
                flags: st.flags,
                error: st.error.take(),
                disconnect_timeout: st.disconnect_timeout,

                dispatch_task: LocalWaker::new(),

                write_buf: mem::take(&mut st.write_buf),
                write_task: LocalWaker::new(),

                read_buf: mem::take(&mut st.read_buf),
                read_task: LocalWaker::new(),
            })),
        }
    }

    pub fn with_codec<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut U) -> R,
    {
        f(&mut self.inner.borrow_mut().codec)
    }

    pub async fn next<T>(
        &self,
        io: &mut T,
    ) -> Result<Option<<U as Decoder>::Item>, Either<<U as Decoder>::Error, io::Error>>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let mut state = self.inner.borrow_mut();

        loop {
            return match state.decode_item() {
                Ok(Some(el)) => Ok(Some(el)),
                Ok(None) => {
                    let n =
                        poll_fn(|cx| Pin::new(&mut *io).poll_read_buf(cx, &mut state.read_buf))
                            .await
                            .map_err(Either::Right)?;
                    if n == 0 {
                        Ok(None)
                    } else {
                        continue;
                    }
                }
                Err(err) => Err(Either::Left(err)),
            };
        }
    }

    pub fn poll_next<T>(
        &self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<<U as Decoder>::Item>, Either<<U as Decoder>::Error, io::Error>>>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let mut state = self.inner.borrow_mut();

        loop {
            return match state.decode_item() {
                Ok(Some(el)) => Poll::Ready(Ok(Some(el))),
                Ok(None) => {
                    let n = ready!(Pin::new(&mut *io).poll_read_buf(cx, &mut state.read_buf))
                        .map_err(Either::Right)?;
                    if n == 0 {
                        Poll::Ready(Ok(None))
                    } else {
                        continue;
                    }
                }
                Err(err) => Poll::Ready(Err(Either::Left(err))),
            };
        }
    }

    pub async fn send<T>(
        &self,
        io: &mut T,
        item: <U as Encoder>::Item,
    ) -> Result<(), Either<<U as Encoder>::Error, io::Error>>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let mut state = self.inner.borrow_mut();

        state.encode_item(item).map_err(Either::Left)?;
        poll_fn(|cx| state.flush_io(io, cx)).await.map_err(Either::Right)
    }
}

impl<U> Clone for IoState<U> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<U> Eq for IoState<U> {}

impl<U> PartialEq for IoState<U> {
    fn eq(&self, other: &Self) -> bool {
        Rc::as_ptr(&self.inner) == Rc::as_ptr(&other.inner)
    }
}

impl<U> Hash for IoState<U> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Rc::as_ptr(&self.inner).hash(state);
    }
}

impl<U> IoStateInner<U>
where
    U: Encoder + Decoder,
{
    pub(crate) fn encode_item(
        &mut self,
        item: <U as Encoder>::Item,
    ) -> Result<(), <U as Encoder>::Error> {
        self.codec.encode(item, &mut self.write_buf)
    }

    pub(crate) fn decode_item(
        &mut self,
    ) -> Result<Option<<U as Decoder>::Item>, <U as Decoder>::Error> {
        self.codec.decode(&mut self.read_buf)
    }

    pub(crate) fn is_opened(&self) -> bool {
        !self.flags.intersects(Flags::IO_ERR | Flags::IO_SHUTDOWN | Flags::DSP_STOP)
    }

    pub(crate) fn close(&mut self) {
        self.flags.insert(Flags::DSP_STOP);
        self.dispatch_task.wake();
    }

    pub(crate) fn force_close(&mut self) {
        self.flags.insert(Flags::IO_SHUTDOWN | Flags::DSP_STOP);
        self.write_task.wake();
        self.dispatch_task.wake();
    }

    pub(crate) fn send(
        &mut self,
        item: <U as Encoder>::Item,
    ) -> Result<(), <U as Encoder>::Error> {
        if !self.flags.intersects(Flags::IO_ERR | Flags::IO_SHUTDOWN) {
            let is_write_sleep = self.write_buf.is_empty();

            // encode item and wake write task
            let res = self.codec.encode(item, &mut self.write_buf);
            if res.is_ok() && is_write_sleep {
                self.write_task.wake();
            }
            res
        } else {
            Ok(())
        }
    }

    pub(crate) fn write_item<E>(
        &mut self,
        item: Result<Option<Response<U>>, E>,
    ) -> Option<IoDispatcherError<E, <U as Encoder>::Error>> {
        // log::trace!("encoding service response, is err: {:?}", item.is_err(),);

        if !self.flags.intersects(Flags::IO_ERR | Flags::ST_DSP_ERR) {
            match item {
                Ok(Some(item)) => {
                    let is_write_sleep = self.write_buf.is_empty();

                    // encode item
                    if let Err(err) = self.codec.encode(item, &mut self.write_buf) {
                        log::trace!("Codec encoder error: {:?}", err);
                        self.flags.insert(Flags::DSP_STOP | Flags::ST_DSP_ERR);
                        self.dispatch_task.wake();
                        return Some(IoDispatcherError::Encoder(err));
                    } else if is_write_sleep {
                        self.write_task.wake();
                    }
                    None
                }
                Err(err) => {
                    self.flags.insert(Flags::DSP_STOP | Flags::ST_DSP_ERR);
                    self.dispatch_task.wake();
                    Some(IoDispatcherError::Service(err))
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub(crate) fn read_io<T>(&mut self, io: &mut T, cx: &mut Context<'_>) -> Poll<()>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        // read all data from socket
        let mut updated = false;
        loop {
            // make sure we've got room
            let remaining = self.read_buf.capacity() - self.read_buf.len();
            if remaining < LW {
                self.read_buf.reserve(HW - remaining)
            }

            match Pin::new(&mut *io).poll_read_buf(cx, &mut self.read_buf) {
                Poll::Pending => break,
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        log::trace!("read io is disconnected");
                        self.flags.insert(Flags::IO_ERR | Flags::DSP_STOP);
                        self.write_task.wake();
                        self.dispatch_task.wake();
                        return Poll::Ready(());
                    } else {
                        updated = true;
                    }
                }
                Poll::Ready(Err(err)) => {
                    log::trace!("read task failed on io {:?}", err);
                    self.flags.insert(Flags::IO_ERR | Flags::DSP_STOP);
                    self.error = Some(err);
                    self.write_task.wake();
                    self.dispatch_task.wake();
                    return Poll::Ready(());
                }
            }
        }

        if updated {
            // stop reading bytes stream
            if self.read_buf.len() > HW {
                log::trace!("buffer is too large {}, pause", self.read_buf.len());
                self.flags.insert(Flags::RD_READY | Flags::RD_PAUSED);
            } else {
                self.flags.insert(Flags::RD_READY);
            }
            // log::trace!(
            //     "new data is available {}, waking up dispatch task",
            //     self.read_buf.len()
            // );

            self.dispatch_task.wake();
        }

        self.read_task.register(cx.waker());
        Poll::Pending
    }

    /// Flush write buffer to underlying I/O stream.
    pub(crate) fn flush_io<T>(
        &mut self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let len = self.write_buf.len();

        // log::trace!("flushing framed transport: {}", len);

        if len == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut written = 0;
        while written < len {
            match Pin::new(&mut *io).poll_write(cx, &self.write_buf[written..]) {
                Poll::Pending => break,
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        log::trace!("Disconnected during flush, written {}", written);
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write frame to transport",
                        )));
                    } else {
                        written += n
                    }
                }
                Poll::Ready(Err(e)) => {
                    log::trace!("Error during flush: {}", e);
                    return Poll::Ready(Err(e));
                }
            }
        }
        // log::trace!("flushed {} bytes", written);

        // remove written data
        if written == len {
            // flushed same amount as in buffer, we dont need to reallocate
            unsafe { self.write_buf.set_len(0) }
        } else {
            self.write_buf.advance(written);
        }
        if self.write_buf.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// Flush write buffer and shutdown underlying I/O stream.
    ///
    /// Close method shutdown write side of a io object and
    /// then reads until disconnect or error, high level code must use
    /// timeout for close operation.
    pub(crate) fn close_io<T>(
        &mut self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>>
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        // log::trace!("closing framed transport: {} :{:?}:", self.write_buf.len(), self.flags);

        if !self.flags.contains(Flags::ST_IO_SHUTDOWN) {
            // flush write buffer
            ready!(self.flush_io(io, cx))?;

            // shutdown WRITE side
            ready!(Pin::new(&mut *io).poll_shutdown(cx))?;
            self.flags.insert(Flags::ST_IO_SHUTDOWN);
        }

        // read until 0 or err
        let mut buf = [0u8; 512];
        loop {
            match ready!(Pin::new(&mut *io).poll_read(cx, &mut buf)) {
                Err(_) | Ok(0) => {
                    break;
                }
                _ => (),
            }
        }
        // log::trace!("framed transport flushed and closed");
        Poll::Ready(Ok(()))
    }
}
