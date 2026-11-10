use std::{cell::Cell, fmt, mem};

use ntex_bytes::{Bytes, BytesMut};
use ntex_service::{BindCtx, ReadyNotifier};
use ntex_util::{channel::bstream, future::Either};

/// Readiness outcome for the active payload channel, including receiver cancellation.
pub(crate) use ntex_util::channel::bstream::Status as PayloadStatus;

use crate::error::PayloadError;

/// Receiver of incoming payload chunks and terminal payload errors.
type PlStream = bstream::Receiver<PayloadError>;
/// Producer that feeds incoming payload chunks and observes buffer backpressure.
type PlSender = bstream::Sender<PayloadError>;

/// Stable readiness source shared by successive incoming payload streams.
#[derive(Default)]
pub(crate) struct PayloadSource {
    /// Producer for the active stream, cleared on its final chunk or an error.
    sender: Cell<Option<PlSender>>,
    /// Stable notifier shared with each new stream and all bound service scopes.
    changed: ReadyNotifier,
}

impl PayloadSource {
    /// Subscribes a service scope to payload readiness changes across stream replacements.
    pub(crate) fn bind(&self, ctx: &BindCtx<'_>) {
        ctx.subscribe(&self.changed);
    }

    /// Replaces the active stream and feeds its initial bytes.
    ///
    /// `buf_size` sets the high watermark in bytes, clamped to `u32::MAX`;
    /// the low watermark is half that value.
    pub(crate) fn start(&self, buf: Bytes, buf_size: usize) -> Payload {
        let (tx, rx) = bstream::channel_with_notifier(self.changed.clone());
        let size = u32::try_from(buf_size).unwrap_or(u32::MAX);
        rx.set_watermarks(size, size / 2);
        self.sender.set(Some(tx.clone()));
        self.changed.notify();
        if !buf.is_empty() {
            tx.feed_data(buf);
        }
        Payload {
            pl: Either::Right(rx),
        }
    }

    /// Waits for active-stream buffer capacity, or returns ready when no stream is active.
    pub(crate) async fn ready(&self) -> PayloadStatus {
        if let Some(sender) = self.sender.take() {
            self.sender.set(Some(sender.clone()));
            sender.ready().await
        } else {
            PayloadStatus::Ready
        }
    }

    /// Feeds a chunk if a stream is active, returning whether a producer was present.
    ///
    /// `eof` closes the stream after this chunk and clears the active producer.
    pub(crate) fn feed(&self, buf: Bytes, eof: bool) -> bool {
        if let Some(sender) = self.sender.take() {
            if eof {
                self.changed.notify();
            } else {
                self.sender.set(Some(sender.clone()));
            }
            sender.feed_data(buf);
            if eof {
                sender.feed_eof();
            }
            true
        } else {
            false
        }
    }

    /// Terminates the active stream with an error and notifies bound readiness scopes.
    pub(crate) fn set_error(&self, err: PayloadError) {
        if let Some(sender) = self.sender.take() {
            sender.set_error(err);
            self.changed.notify();
        }
    }
}

/// Single-use payload of an incoming publish packet.
///
/// Payloads contain either fixed bytes or a stream of chunks. Reading consumes
/// bytes even through a shared reference; subsequent reads observe only the
/// remaining data. The default payload is empty and already consumed.
pub struct Payload {
    /// Single-use fixed bytes or the receiver of a chunked payload stream.
    pl: Either<Cell<Option<Bytes>>, PlStream>,
}

impl Default for Payload {
    fn default() -> Self {
        Payload {
            pl: Either::Left(Cell::new(None)),
        }
    }
}

impl Payload {
    /// Creates a fixed payload that yields `buf` once, including when it is empty.
    pub fn from_bytes(buf: Bytes) -> Payload {
        Payload {
            pl: Either::Left(Cell::new(Some(buf))),
        }
    }

    /// Returns whether the payload uses fixed storage, even after it is consumed.
    pub fn is_fixed(&self) -> bool {
        self.pl.is_left()
    }

    /// Reads and consumes the next chunk, returning `None` at the end of the payload.
    pub async fn read(&self) -> Result<Option<Bytes>, PayloadError> {
        match &self.pl {
            Either::Left(pl) => Ok(pl.take()),
            Either::Right(pl) => pl.read().await.map_or(Ok(None), |res| res.map(Some)),
        }
    }

    /// Reads and consumes all remaining payload bytes.
    ///
    /// Streaming chunks are concatenated until the stream ends. A prior
    /// [`read`](Self::read) may have consumed a prefix; only the remainder is
    /// returned. Unlike `read`, returns [`PayloadError::Consumed`] when no
    /// unread chunk remains, including for the default empty payload.
    /// A streaming error discards any bytes accumulated by this call; already
    /// consumed chunks are not restored.
    pub async fn read_all(&self) -> Result<Bytes, PayloadError> {
        match &self.pl {
            Either::Left(pl) => pl.take().ok_or(PayloadError::Consumed),
            Either::Right(pl) => {
                let mut buf = BytesMut::from(pl.read().await.ok_or(PayloadError::Consumed)??);
                while let Some(result) = pl.read().await {
                    buf.extend_from_slice(&result?);
                }
                Ok(buf.freeze())
            }
        }
    }

    #[must_use]
    /// Take the payload, leaving an empty payload in its place
    pub fn take(&mut self) -> Payload {
        Payload {
            pl: mem::replace(&mut self.pl, Either::Left(Cell::new(None))),
        }
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.pl.is_left() {
            f.debug_struct("FixedPayload").finish()
        } else {
            f.debug_struct("StreamingPayload").finish()
        }
    }
}

/// Unit tests for fixed payload consumption and shared streaming readiness.
#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_service::{Ctx, Pipeline, Service};
    use ntex_util::future::lazy;

    use super::*;

    /// Service exposing a shared payload source as a pipeline readiness dependency.
    struct Source(Rc<PayloadSource>);

    impl Service<(), ()> for Source {
        type Res = ();
        type Error = PayloadStatus;

        fn bind(&self, ctx: BindCtx<'_>) {
            self.0.bind(&ctx);
        }

        async fn ready(&self, _: Ctx<'_, Self>) -> Result<(), PayloadStatus> {
            match self.0.ready().await {
                PayloadStatus::Ready => Ok(()),
                status => Err(status),
            }
        }

        async fn call(&self, (): (), _: Ctx<'_, Self>) -> Result<(), PayloadStatus> {
            Ok(())
        }
    }

    /// STATEMENT: Payload readiness changes reach every bound scope across stream replacements.
    ///
    /// HOW:
    /// - Bind two pipelines to one `PayloadSource`, fill its buffer, and verify readiness blocks until a chunk is consumed.
    /// - Feed the final chunk, replace the stream, then drop its receiver and verify both scopes report `PayloadStatus::Dropped`.
    /// - Set an error to clear the active producer and verify readiness is restored.
    #[ntex::test]
    async fn test_shared_payload_readiness_and_replacement() {
        let source = Rc::new(PayloadSource::default());
        let a = Pipeline::new((), Source(source.clone()));
        let b = Pipeline::new((), Source(source.clone()));
        a.ready().await.unwrap();
        b.ready().await.unwrap();

        let payload = source.start(b"ab".into(), 4);
        assert!(!a.is_ready() && !b.is_ready());
        a.ready().await.unwrap();
        b.ready().await.unwrap();
        assert!(source.feed(b"cd".into(), false));
        assert!(!a.is_ready() && !b.is_ready());
        assert!(lazy(|cx| a.poll_ready(cx)).await.is_pending());
        assert_eq!(payload.read().await.unwrap(), Some(b"ab".into()));
        a.ready().await.unwrap();
        b.ready().await.unwrap();
        assert!(source.feed(b"ef".into(), true));
        a.ready().await.unwrap();
        b.ready().await.unwrap();
        drop(payload);
        a.ready().await.unwrap();
        b.ready().await.unwrap();

        // The next stream uses the original bind-time notification source.
        let payload = source.start(b"x".into(), 4);
        a.ready().await.unwrap();
        b.ready().await.unwrap();
        drop(payload);
        assert!(!a.is_ready() && !b.is_ready());
        assert_eq!(a.ready().await, Err(PayloadStatus::Dropped));
        assert_eq!(b.ready().await, Err(PayloadStatus::Dropped));
        source.set_error(PayloadError::Disconnected);
        a.ready().await.unwrap();
    }

    /// STATEMENT: Streaming payloads concatenate chunks once and report exhaustion on a second complete read.
    ///
    /// HOW:
    /// - Start with `chunk1`, feed two delayed chunks, and end the stream with `chunk3`.
    /// - Verify `read_all` returns the concatenated bytes, streaming storage and debug output are preserved, and another read reports `PayloadError::Consumed`.
    #[ntex::test]
    async fn test_payload() {
        let tx = PayloadSource::default();
        let pl = tx.start(Bytes::from(b"chunk1"), 32 * 1024);

        ntex::rt::spawn(async move {
            ntex::time::sleep(ntex::time::Millis(50)).await;
            tx.feed(b"chunk2".into(), false);
            ntex::time::sleep(ntex::time::Millis(50)).await;
            tx.feed(b"chunk3".into(), true);
        });

        let data = pl.read_all().await.unwrap();
        assert_eq!(data, b"chunk1chunk2chunk3");
        assert!(!pl.is_fixed());
        assert_eq!(format!("{pl:?}"), "StreamingPayload");
        assert!(matches!(pl.read_all().await, Err(PayloadError::Consumed)));
    }

    /// STATEMENT: Taking a fixed payload transfers its unread bytes and leaves consumed fixed storage behind.
    ///
    /// HOW:
    /// - Create fixed `data`, verify its storage and debug output, and transfer it with `take`.
    /// - Verify the original is exhausted, the taken payload yields `data` once, and the default payload uses fixed storage.
    #[ntex::test]
    async fn test_fixed_payload() {
        let mut pl = Payload::from_bytes(Bytes::from_static(b"data"));
        assert!(pl.is_fixed());
        assert_eq!(format!("{pl:?}"), "FixedPayload");

        let taken = pl.take();
        assert_eq!(pl.read().await.unwrap(), None);
        assert!(matches!(pl.read_all().await, Err(PayloadError::Consumed)));
        assert_eq!(taken.read_all().await.unwrap(), b"data".as_ref());
        assert_eq!(taken.read().await.unwrap(), None);
        assert!(Payload::default().is_fixed());
    }

    /// STATEMENT: A streaming error discards bytes already collected and releases producer readiness.
    ///
    /// HOW:
    /// - Poll `read_all` after an initial chunk and again after a second chunk, leaving it pending.
    /// - Terminate the stream with a protocol error and assert that exact error, then exhaustion.
    /// - Verify the cleared source is ready and rejects further chunks.
    #[ntex::test]
    async fn test_stream_error_discards_collected_chunks() {
        use std::future::Future;

        let source = PayloadSource::default();
        let payload = source.start(Bytes::from_static(b"first"), 4);
        let mut all = Box::pin(payload.read_all());
        assert!(lazy(|cx| all.as_mut().poll(cx)).await.is_pending());
        assert!(source.feed(Bytes::from_static(b"second"), false));
        assert!(lazy(|cx| all.as_mut().poll(cx)).await.is_pending());

        let error = PayloadError::Protocol(crate::error::MqttProtocolError::ReadTimeout);
        source.set_error(error);
        assert_eq!(all.await, Err(error));
        assert_eq!(payload.read().await, Ok(None));
        assert_eq!(payload.read_all().await, Err(PayloadError::Consumed));
        assert_eq!(source.ready().await, PayloadStatus::Ready);
        assert!(!source.feed(Bytes::from_static(b"late"), true));
    }

    /// STATEMENT: A stream failing before its first chunk reports its terminal error to either reading API.
    ///
    /// HOW:
    /// - Start an empty stream for each reading API and fail it with `PayloadError::Disconnected`.
    /// - Verify `read` and `read_all` each return the disconnection rather than successful empty bytes.
    #[ntex::test]
    async fn test_stream_error_before_first_chunk() {
        for collect in [false, true] {
            let source = PayloadSource::default();
            let payload = source.start(Bytes::new(), 4);
            source.set_error(PayloadError::Disconnected);
            if collect {
                assert_eq!(payload.read_all().await, Err(PayloadError::Disconnected));
            } else {
                assert_eq!(payload.read().await, Err(PayloadError::Disconnected));
            }
        }
    }
}
