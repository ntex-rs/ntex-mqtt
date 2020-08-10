//! Framed transport dispatcher
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{fmt, io};

use either::Either;
use futures::{ready, FutureExt, Stream};
use log::debug;
use ntex::channel::mpsc;
use ntex::rt::time::{delay_for, delay_until, Delay, Instant as RtInstant};
use ntex::service::{IntoService, Service};
use ntex::util::time::LowResTimeService;
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder, Framed};

type Request<U> = <U as Decoder>::Item;
type Response<U> = <U as Encoder>::Item;

bitflags::bitflags! {
    struct Errors: u8 {
        const IO         = 0b0000_0001;
        const WRITE      = 0b0000_0010;
        const KEEP_ALIVE = 0b0000_0100;
    }
}

/// Framed transport errors
pub enum CodecError<U: Encoder + Decoder> {
    /// Max packet size exceeded
    MaxSizeExceeded,
    /// Keep alive timeout
    KeepAlive,
    /// Encoder parse error
    Encoder(<U as Encoder>::Error),
    /// Decoder parse error
    Decoder(<U as Decoder>::Error),
    /// Unexpected io error
    Io(io::Error),
}

impl<U: Encoder + Decoder> fmt::Debug for CodecError<U>
where
    <U as Encoder>::Error: fmt::Debug,
    <U as Decoder>::Error: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            CodecError::MaxSizeExceeded => write!(fmt, "CodecError::MaxSizeExceeded"),
            CodecError::KeepAlive => write!(fmt, "CodecError::KeepAlive"),
            CodecError::Encoder(ref e) => write!(fmt, "CodecError::Encoder({:?})", e),
            CodecError::Decoder(ref e) => write!(fmt, "CodecError::Decoder({:?})", e),
            CodecError::Io(ref e) => write!(fmt, "CodecError::Io({:?})", e),
        }
    }
}

impl<U: Encoder + Decoder> fmt::Display for CodecError<U>
where
    <U as Encoder>::Error: fmt::Debug,
    <U as Decoder>::Error: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            CodecError::MaxSizeExceeded => write!(fmt, "CodecError::MaxSizeExceeded"),
            CodecError::KeepAlive => write!(fmt, "CodecError::KeepAlive"),
            CodecError::Encoder(ref e) => write!(fmt, "{:?}", e),
            CodecError::Decoder(ref e) => write!(fmt, "{:?}", e),
            CodecError::Io(ref e) => write!(fmt, "CodecError::Io({:?})", e),
        }
    }
}

/// FramedTransport - is a future that reads frames from Framed object
/// and pass then to the service.
#[pin_project::pin_project]
pub struct Dispatcher<S, T, U, Out>
where
    S: Service<Request = Result<Request<U>, CodecError<U>>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
    <U as Encoder>::Error: std::fmt::Debug,
    Out: Stream<Item = <U as Encoder>::Item> + Unpin,
{
    inner: InnerDispatcher<S, T, U, Out>,
}

#[cfg(test)]
impl<S, T, U> Dispatcher<S, T, U, mpsc::Receiver<<U as Encoder>::Item>>
where
    S: Service<Request = Result<Request<U>, CodecError<U>>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Decoder + Encoder,
    <U as Encoder>::Item: 'static,
    <U as Encoder>::Error: std::fmt::Debug,
{
    /// Construct new `Dispatcher` instance
    pub fn new<F: IntoService<S>>(framed: Framed<T, U>, service: F) -> Self {
        let time = LowResTimeService::with(Duration::from_secs(1));
        let keepalive_timeout = Duration::from_secs(30);
        let updated = time.now();
        let expire = RtInstant::from_std(updated + keepalive_timeout);

        Dispatcher {
            inner: InnerDispatcher {
                framed,
                time,
                updated,
                keepalive_timeout,
                sink: None,
                rx: mpsc::channel().1,
                service: service.into_service(),
                error: None,
                errors: Errors::empty(),
                state: FramedState::Processing,
                disconnect_timeout: 1000,
                keepalive: delay_until(expire),
            },
        }
    }
}

impl<S, T, U, In> Dispatcher<S, T, U, In>
where
    S: Service<Request = Result<Request<U>, CodecError<U>>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Decoder + Encoder,
    <U as Encoder>::Item: 'static,
    <U as Encoder>::Error: std::fmt::Debug,
    In: Stream<Item = <U as Encoder>::Item> + Unpin,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub fn with<F: IntoService<S>>(
        framed: Framed<T, U>,
        sink: Option<In>,
        service: F,
        time: LowResTimeService,
    ) -> Self {
        let keepalive_timeout = Duration::from_secs(30);
        let expire = RtInstant::from_std(time.now() + keepalive_timeout);

        Dispatcher {
            inner: InnerDispatcher {
                rx: mpsc::channel().1,
                service: service.into_service(),
                error: None,
                errors: Errors::empty(),
                state: FramedState::Processing,
                disconnect_timeout: 1000,
                updated: time.now(),
                keepalive: delay_until(expire),
                framed,
                sink,
                time,
                keepalive_timeout,
            },
        }
    }

    /// Set keep-alive timeout.
    pub fn keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.inner.keepalive_timeout = timeout;

        let expire = RtInstant::from_std(self.inner.time.now() + self.inner.keepalive_timeout);
        self.inner.keepalive.reset(expire);

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
    pub fn disconnect_timeout(mut self, val: u64) -> Self {
        self.inner.disconnect_timeout = val;
        self
    }
}

impl<S, T, U, In> Future for Dispatcher<S, T, U, In>
where
    S: Service<Request = Result<Request<U>, CodecError<U>>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Decoder + Encoder,
    <U as Encoder>::Item: 'static,
    <U as Encoder>::Error: std::fmt::Debug,
    In: Stream<Item = <U as Encoder>::Item> + Unpin,
{
    type Output = Result<(), S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

enum FramedState<S: Service> {
    Processing,
    FlushAndStop(Option<S::Error>),
    Shutdown(Option<S::Error>),
    ShutdownIo(Delay, Option<Result<(), S::Error>>),
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum PollResult {
    Continue,
    Pending,
}

struct InnerDispatcher<S, T, U, Out>
where
    S: Service<Request = Result<Request<U>, CodecError<U>>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Encoder + Decoder,
    <U as Encoder>::Item: 'static,
    <U as Encoder>::Error: std::fmt::Debug,
    Out: Stream<Item = <U as Encoder>::Item> + Unpin,
{
    service: S,
    sink: Option<Out>,
    state: FramedState<S>,
    framed: Framed<T, U>,
    rx: mpsc::Receiver<Result<<U as Encoder>::Item, S::Error>>,
    keepalive: Delay,
    keepalive_timeout: Duration,
    disconnect_timeout: u64,
    error: Option<CodecError<U>>,
    errors: Errors,
    time: LowResTimeService,
    updated: Instant,
}

impl<S, T, U, Out> InnerDispatcher<S, T, U, Out>
where
    S: Service<Request = Result<Request<U>, CodecError<U>>, Response = Option<Response<U>>>,
    S::Error: 'static,
    S::Future: 'static,
    T: AsyncRead + AsyncWrite + Unpin,
    U: Decoder + Encoder,
    <U as Encoder>::Item: 'static,
    <U as Encoder>::Error: std::fmt::Debug,
    Out: Stream<Item = <U as Encoder>::Item> + Unpin,
{
    fn poll_read(&mut self, cx: &mut Context<'_>) -> PollResult {
        loop {
            match self.service.poll_ready(cx) {
                Poll::Ready(Ok(_)) => {
                    // handle write error
                    if let Some(err) = self.error.take() {
                        let tx = self.rx.sender();
                        ntex::rt::spawn(self.service.call(Err(err)).map(move |item| {
                            let item = match item {
                                Ok(Some(item)) => Ok(item),
                                Err(err) => Err(err),
                                _ => return,
                            };
                            let _ = tx.send(item);
                        }));
                        self.state = FramedState::FlushAndStop(None);
                        return PollResult::Continue;
                    }

                    // read data from io
                    let mut error = false;
                    let item = match self.framed.next_item(cx) {
                        Poll::Ready(Some(Ok(el))) => {
                            self.updated = self.time.now();
                            Ok(el)
                        }
                        Poll::Ready(Some(Err(err))) => {
                            error = true;
                            match err {
                                Either::Left(err) => {
                                    log::warn!("Framed decode error");
                                    Err(CodecError::Decoder(err))
                                }
                                Either::Right(err) => {
                                    log::trace!("Framed io error: {:?}", err);
                                    self.errors.insert(Errors::IO);
                                    Err(CodecError::Io(err))
                                }
                            }
                        }
                        Poll::Pending => return PollResult::Pending,
                        Poll::Ready(None) => {
                            log::trace!("Client disconnected");
                            self.errors.insert(Errors::IO);
                            self.state = FramedState::FlushAndStop(None);
                            return PollResult::Continue;
                        }
                    };

                    // call service
                    let tx = self.rx.sender();
                    ntex::rt::spawn(self.service.call(item).map(move |item| {
                        let item = match item {
                            Ok(Some(item)) => Ok(item),
                            Err(err) => Err(err),
                            _ => return,
                        };
                        let _ = tx.send(item);
                    }));

                    // handle read error
                    if error {
                        self.state = FramedState::FlushAndStop(None);
                        return PollResult::Continue;
                    }
                }
                Poll::Pending => return PollResult::Pending,
                Poll::Ready(Err(err)) => {
                    // service readiness error
                    self.state = FramedState::FlushAndStop(Some(err));
                    return PollResult::Continue;
                }
            }
        }
    }

    /// write to framed object
    fn poll_write(&mut self, cx: &mut Context<'_>) -> PollResult {
        // if WRITE error occured, dont do anything just drain queues
        if self.errors.contains(Errors::WRITE) {
            return PollResult::Pending;
        }

        loop {
            while !self.framed.is_write_buf_full() {
                match Pin::new(&mut self.rx).poll_next(cx) {
                    Poll::Ready(Some(Ok(msg))) => {
                        // write to framed object does not do any io
                        // so error here is from encoder
                        if let Err(err) = self.framed.write(msg) {
                            log::trace!("Framed write error: {:?}", err);
                            self.errors.insert(Errors::WRITE);
                            self.error = Some(CodecError::Encoder(err));
                            return PollResult::Continue;
                        }
                        continue;
                    }
                    // service call ended up with error
                    Poll::Ready(Some(Err(err))) => {
                        self.state = FramedState::FlushAndStop(Some(err));
                        return PollResult::Continue;
                    }
                    Poll::Ready(None) | Poll::Pending => {}
                }

                // handle sink queue
                if let Some(ref mut sink) = self.sink {
                    match Pin::new(sink).poll_next(cx) {
                        Poll::Ready(Some(msg)) => {
                            if let Err(err) = self.framed.write(msg) {
                                log::trace!("Framed write error from sink: {:?}", err);
                                self.errors.insert(Errors::WRITE);
                                self.error = Some(CodecError::Encoder(err));
                                return PollResult::Continue;
                            }
                            continue;
                        }
                        // sink closed
                        Poll::Ready(None) => {
                            let _ = self.sink.take();
                            self.state = FramedState::FlushAndStop(None);
                            return PollResult::Continue;
                        }
                        Poll::Pending => (),
                    }
                }
                break;
            }

            // flush framed instance, do actual IO
            if !self.framed.is_write_buf_empty() {
                match self.framed.flush(cx) {
                    Poll::Pending => break,
                    Poll::Ready(Ok(_)) => (),
                    Poll::Ready(Err(err)) => {
                        debug!("Error sending data: {:?}", err);
                        let _ = self.sink.take();
                        self.error = Some(CodecError::Io(err));
                        self.errors.insert(Errors::WRITE | Errors::IO);
                        return PollResult::Continue;
                    }
                }
            } else {
                break;
            }
        }
        PollResult::Pending
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        // keepalive timer
        if !self.errors.contains(Errors::KEEP_ALIVE) {
            match Pin::new(&mut self.keepalive).poll(cx) {
                Poll::Ready(_) => {
                    if self.keepalive.deadline() <= RtInstant::from_std(self.updated) {
                        self.error = Some(CodecError::KeepAlive);
                        self.errors.insert(Errors::KEEP_ALIVE);
                    } else {
                        let expire =
                            RtInstant::from_std(self.time.now() + self.keepalive_timeout);
                        self.keepalive.reset(expire);
                        let _ = Pin::new(&mut self.keepalive).poll(cx);
                    }
                }
                Poll::Pending => (),
            }
        }

        loop {
            match self.state {
                FramedState::Processing => {
                    let read = self.poll_read(cx);
                    let write = self.poll_write(cx);
                    if read == PollResult::Continue || write == PollResult::Continue {
                        continue;
                    } else {
                        return Poll::Pending;
                    }
                }
                FramedState::FlushAndStop(ref mut err) => {
                    // drain service responses
                    match Pin::new(&mut self.rx).poll_next(cx) {
                        Poll::Ready(Some(Ok(msg))) => {
                            if let Err(err) = self.framed.write(msg) {
                                log::trace!("Framed write message error: {:?}", err);
                                self.state = FramedState::Shutdown(None);
                                continue;
                            }
                        }
                        Poll::Ready(Some(Err(err))) => {
                            log::trace!("Sink poll error");
                            self.state = FramedState::Shutdown(Some(err));
                            continue;
                        }
                        Poll::Ready(None) | Poll::Pending => (),
                    }

                    // flush io
                    if !self.errors.contains(Errors::IO) {
                        if !self.framed.is_write_buf_empty() {
                            match self.framed.flush(cx) {
                                Poll::Ready(Err(err)) => {
                                    debug!("Error sending data: {:?}", err);
                                }
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(_) => (),
                            }
                        }
                        log::trace!("Framed flushed, shutdown");
                    }
                    self.state = FramedState::Shutdown(err.take());
                }
                FramedState::Shutdown(ref mut err) => {
                    return if self.service.poll_shutdown(cx, err.is_some()).is_ready() {
                        let result = if let Some(err) = err.take() { Err(err) } else { Ok(()) };

                        // no need for io shutdown because io error occured
                        if self.errors.contains(Errors::IO) {
                            return Poll::Ready(result);
                        }

                        // close frame, closes io WR side and waits for disconnect
                        // on read side. we need disconnect timeout, otherwise it
                        // could hang forever.
                        let pending = self.framed.close(cx).is_pending();
                        if self.disconnect_timeout != 0 && pending {
                            self.state = FramedState::ShutdownIo(
                                delay_for(Duration::from_millis(self.disconnect_timeout)),
                                Some(result),
                            );
                            continue;
                        } else {
                            Poll::Ready(result)
                        }
                    } else {
                        Poll::Pending
                    };
                }
                FramedState::ShutdownIo(ref mut delay, ref mut err) => {
                    if let Poll::Ready(_) = self.framed.close(cx) {
                        return match err.take() {
                            Some(Ok(_)) | None => Poll::Ready(Ok(())),
                            Some(Err(e)) => Poll::Ready(Err(e)),
                        };
                    } else {
                        ready!(Pin::new(delay).poll(cx));
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use futures::future::ok;
    use std::io;

    use ntex::channel::mpsc;
    use ntex::rt::time::delay_for;
    use ntex::testing::Io;
    use ntex_codec::{BytesCodec, Framed};

    use super::*;

    #[test]
    fn test_err() {
        type T = CodecError<BytesCodec>;
        let err = T::Encoder(io::Error::new(io::ErrorKind::Other, "err"));
        assert!(format!("{:?}", err).contains("CodecError::Encoder"));
        assert!(format!("{}", err).contains("Custom"));
        let err = T::Decoder(io::Error::new(io::ErrorKind::Other, "err"));
        assert!(format!("{:?}", err).contains("CodecError::Decoder"));
        assert!(format!("{}", err).contains("Custom"));
    }

    #[ntex::test]
    async fn test_basic() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        client.write("GET /test HTTP/1\r\n\r\n");

        let framed = Framed::new(server, BytesCodec);
        let disp = Dispatcher::new(
            framed,
            ntex::fn_service(|msg: Result<BytesMut, CodecError<BytesCodec>>| async move {
                delay_for(Duration::from_millis(50)).await;
                Ok::<_, ()>(Some(msg.unwrap().freeze()))
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

        let (tx, rx) = mpsc::channel();
        let framed = Framed::new(server, BytesCodec);
        let disp = Dispatcher::with(
            framed,
            Some(rx),
            ntex::fn_service(|msg: Result<BytesMut, CodecError<BytesCodec>>| {
                ok::<_, ()>(Some(msg.unwrap().freeze()))
            }),
            LowResTimeService::with(Duration::from_secs(1)),
        )
        .disconnect_timeout(25);
        ntex::rt::spawn(disp.map(|_| ()));

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"GET /test HTTP/1\r\n\r\n"));

        assert!(tx.send(Bytes::from_static(b"test")).is_ok());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"test"));

        drop(tx);
        delay_for(Duration::from_millis(200)).await;
        assert!(client.is_server_dropped());
    }

    #[ntex::test]
    async fn test_err_in_service() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(0);
        client.write("GET /test HTTP/1\r\n\r\n");

        let mut framed = Framed::new(server, BytesCodec);
        framed.write_buf().extend(b"GET /test HTTP/1\r\n\r\n");

        let disp = Dispatcher::new(
            framed,
            ntex::fn_service(|_: Result<BytesMut, CodecError<BytesCodec>>| async {
                Err::<Option<Bytes>, _>(())
            }),
        );
        ntex::rt::spawn(disp.map(|_| ()));

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
