use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use either::Either;
use futures::{ready, Future, Stream};
use ntex::channel::mpsc;
use ntex::rt::time::{delay_until, Delay, Instant as RtInstant};
use ntex_codec::{AsyncRead, AsyncWrite, Framed};

use crate::error::ProtocolError;
use crate::v5::codec;

use super::control::ControlMessage;
use super::sink::MqttSink;

/// Mqtt protocol connection
pub struct Client<Io> {
    io: Framed<Io, codec::Codec>,
    rx: mpsc::Receiver<codec::Packet>,
    sink: MqttSink,
    state: State,
    keepalive: Option<(Duration, Delay)>,
    disconnect_timeout: u64,
    errors: Errors,
    max_receive: usize,
    pkt: codec::ConnectAck,
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        io: Framed<T, codec::Codec>,
        pkt: codec::ConnectAck,
        max_receive: u16,
    ) -> Self {
        let (tx, rx) = mpsc::channel();

        let keepalive = pkt.server_keepalive_sec.unwrap_or(0) as u64;
        let keepalive = if keepalive > 0 {
            let keepalive = Duration::from_secs(keepalive);
            let expire = RtInstant::from_std(Instant::now() + keepalive);
            Some((keepalive, delay_until(expire)))
        } else {
            None
        };
        let server_max_receive = pkt.receive_max.map(|v| v.get()).unwrap_or(0);

        Client {
            io,
            rx,
            pkt,
            keepalive,
            sink: MqttSink::new(tx, server_max_receive as usize),
            errors: Errors::empty(),
            disconnect_timeout: 1000,
            state: State::Processing,
            max_receive: max_receive as usize,
        }
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.pkt.session_present
    }

    #[inline]
    /// Get reference to `ConnectAck` packet
    pub fn packet(&self) -> &codec::ConnectAck {
        &self.pkt
    }

    #[inline]
    /// Get mutable reference to `ConnectAck` packet
    pub fn packet_mut(&mut self) -> &mut codec::ConnectAck {
        &mut self.pkt
    }

    fn disconnect_timeout(mut self, val: u64) -> Self {
        self.disconnect_timeout = val;
        self
    }
}

impl<T> Stream for Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = ControlMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.errors.contains(Errors::IO) {
            Poll::Ready(None)
        } else {
            // send keep-alive pings
            if self.errors.is_empty() {
                self.poll_keepalive(cx);
            }

            // write and flush io
            if let Err(e) = self.poll_write(cx) {
                Poll::Ready(Some(ControlMessage::protocol_error(e, &self.sink)))
            } else {
                match ready!(self.poll_read(cx)) {
                    Some(Err(e)) => {
                        Poll::Ready(Some(ControlMessage::protocol_error(e, &self.sink)))
                    }
                    Some(Ok(pkt)) => Poll::Pending,
                    None => Poll::Pending,
                }
            }
        }
    }
}

bitflags::bitflags! {
    struct Errors: u8 {
        const IO         = 0b0000_0001;
        const READ       = 0b0000_0010;
        const WRITE      = 0b0000_0100;
    }
}

enum State {
    Processing,
    FlushAndStop,
    Shutdown,
    ShutdownIo(Delay),
}

impl<T> Client<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_keepalive(&mut self, cx: &mut Context<'_>) -> () {
        // server keepalive timer
        if let Some(ref mut item) = self.keepalive {
            match Pin::new(&mut item.1).poll(cx) {
                Poll::Ready(_) => {
                    let expire = RtInstant::from_std(Instant::now() + item.0);
                    item.1.reset(expire);
                    let _ = Pin::new(&mut item.1).poll(cx);
                }
                Poll::Pending => (),
            }
        }
    }

    /// read data from io
    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<codec::Packet, ProtocolError>>> {
        match self.io.next_item(cx) {
            Poll::Ready(Some(Ok(el))) => Poll::Ready(Some(Ok(el))),
            Poll::Ready(Some(Err(err))) => match err {
                Either::Left(err) => {
                    log::warn!("Framed decode error");
                    self.errors.insert(Errors::READ);
                    Poll::Ready(Some(Err(ProtocolError::Decode(err))))
                }
                Either::Right(err) => {
                    log::trace!("Framed io error: {:?}", err);
                    self.errors.insert(Errors::IO);
                    Poll::Ready(Some(Err(ProtocolError::Io(err))))
                }
            },
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                log::trace!("Client disconnected");
                Poll::Ready(None)
            }
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), ProtocolError> {
        // if WRITE error occured, dont do anything just drain queues
        loop {
            while !self.io.is_write_buf_full() {
                match Pin::new(&mut self.rx).poll_next(cx) {
                    Poll::Ready(Some(msg)) => {
                        // write to framed object does not do any io
                        // so error here is from encoder
                        if let Err(err) = self.io.write(msg) {
                            log::trace!("Framed write error: {:?}", err);
                            return Err(ProtocolError::Encode(err));
                        }
                        continue;
                    }
                    Poll::Ready(None) | Poll::Pending => {}
                }
                break;
            }

            // flush framed instance, actual IO
            if !self.io.is_write_buf_empty() {
                match self.io.flush(cx) {
                    Poll::Pending => break,
                    Poll::Ready(Ok(_)) => (),
                    Poll::Ready(Err(err)) => {
                        log::debug!("Error sending data: {:?}", err);
                        self.errors.insert(Errors::IO);
                        return Err(ProtocolError::Io(err));
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}
