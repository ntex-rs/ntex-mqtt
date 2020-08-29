use std::task::{Context, Poll};
use std::{io, marker::PhantomData, pin::Pin};

use either::Either;
use futures::Stream;
use ntex_codec::{AsyncRead, AsyncWrite, Decoder, Encoder, Framed};

use super::framed::Receiver;

pub(crate) struct Handshake<Io, Codec>
where
    Codec: Encoder + Decoder,
{
    io: Io,
    _t: PhantomData<Codec>,
}

impl<Io, Codec> Handshake<Io, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    Codec: Encoder + Decoder,
{
    pub(crate) fn new(io: Io) -> Self {
        Self { io, _t: PhantomData }
    }

    pub(crate) fn with_codec(framed: Framed<Io, Codec>) -> HandshakeResult<Io, (), Codec> {
        HandshakeResult { state: (), out: None, keepalive: 30, framed }
    }

    pub fn codec(self, codec: Codec) -> HandshakeResult<Io, (), Codec> {
        HandshakeResult {
            state: (),
            out: None,
            framed: Framed::new(self.io, codec),
            keepalive: 30,
        }
    }
}

pin_project_lite::pin_project! {
    pub struct HandshakeResult<Io, St, Codec: Encoder> {
        pub(crate) state: St,
        pub(crate) out: Option<Receiver<Codec>>,
        pub(crate) framed: Framed<Io, Codec>,
        pub(crate) keepalive: usize,
    }
}

impl<Io, St, Codec: Encoder + Decoder> HandshakeResult<Io, St, Codec> {
    pub(crate) fn get_codec_mut(&mut self) -> &mut Codec {
        self.framed.get_codec_mut()
    }

    #[inline]
    pub fn io(&mut self) -> &mut Framed<Io, Codec> {
        &mut self.framed
    }

    pub fn out(mut self, out: Receiver<Codec>) -> Self {
        self.out = Some(out);
        self
    }

    #[inline]
    pub fn state<S>(self, state: S) -> HandshakeResult<Io, S, Codec> {
        HandshakeResult { state, framed: self.framed, out: self.out, keepalive: self.keepalive }
    }

    #[inline]
    /// Set keep-alive timeout in seconds.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default keep-alive timeout is set to 30 seconds.
    pub fn set_keepalive_timeout(&mut self, timeout: usize) {
        self.keepalive = timeout;
    }
}

impl<Io, St, Codec> Stream for HandshakeResult<Io, St, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    Codec: Encoder + Decoder,
{
    type Item = Result<<Codec as Decoder>::Item, Either<<Codec as Decoder>::Error, io::Error>>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.framed.next_item(cx)
    }
}

impl<Io, St, Codec> futures::Sink<Codec::Item> for HandshakeResult<Io, St, Codec>
where
    Io: AsyncRead + AsyncWrite + Unpin,
    Codec: Encoder + Unpin,
{
    type Error = Either<Codec::Error, io::Error>;

    #[inline]
    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.framed).poll_ready(cx)
    }

    #[inline]
    fn start_send(
        mut self: Pin<&mut Self>,
        item: <Codec as Encoder>::Item,
    ) -> Result<(), Self::Error> {
        Pin::new(&mut self.framed).start_send(item)
    }

    #[inline]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.framed).poll_flush(cx)
    }

    #[inline]
    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.framed).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::future::lazy;
    use futures::{Sink, StreamExt};
    use ntex::testing::Io;
    use ntex_codec::BytesCodec;

    use super::*;

    #[allow(clippy::declare_interior_mutable_const)]
    const BLOB: Bytes = Bytes::from_static(b"GET /test HTTP/1.1\r\n\r\n");

    #[ntex::test]
    async fn test_result() {
        let (client, server) = Io::create();
        client.remote_buffer_cap(1024);
        let server = Framed::new(server, BytesCodec);

        let mut hnd = HandshakeResult { state: (), out: None, framed: server, keepalive: 0 };

        client.write(BLOB);
        let item = hnd.next().await.unwrap().unwrap();
        assert_eq!(item, BLOB);

        assert!(lazy(|cx| Pin::new(&mut hnd).poll_ready(cx)).await.is_ready());

        Pin::new(&mut hnd).start_send(BLOB).unwrap();
        assert_eq!(client.read_any(), b"".as_ref());
        assert_eq!(hnd.io().read_buf(), b"".as_ref());
        assert_eq!(hnd.io().write_buf(), &BLOB[..]);

        assert!(lazy(|cx| Pin::new(&mut hnd).poll_flush(cx)).await.is_ready());
        assert_eq!(client.read_any(), &BLOB[..]);

        assert!(lazy(|cx| Pin::new(&mut hnd).poll_close(cx)).await.is_pending());
        client.close().await;
        assert!(lazy(|cx| Pin::new(&mut hnd).poll_close(cx)).await.is_ready());
        assert!(client.is_closed());
    }
}
