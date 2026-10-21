use std::{cell::Cell, fmt, mem};

use ntex_bytes::{Bytes, BytesMut};
use ntex_service::{BindCtx, ReadyNotifier};
use ntex_util::{channel::bstream, future::Either};

pub(crate) use ntex_util::channel::bstream::Status as PayloadStatus;

use crate::error::PayloadError;

type PlStream = bstream::Receiver<PayloadError>;
type PlSender = bstream::Sender<PayloadError>;

/// Stable readiness source shared by successive incoming payload streams.
#[derive(Default)]
pub(crate) struct PayloadSource {
    sender: Cell<Option<PlSender>>,
    changed: ReadyNotifier,
}

impl PayloadSource {
    pub(crate) fn bind(&self, ctx: &BindCtx<'_>) {
        ctx.subscribe(&self.changed);
    }

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

    pub(crate) async fn ready(&self) -> PayloadStatus {
        if let Some(sender) = self.sender.take() {
            self.sender.set(Some(sender.clone()));
            sender.ready().await
        } else {
            PayloadStatus::Ready
        }
    }

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

    pub(crate) fn set_error(&self, err: PayloadError) {
        if let Some(sender) = self.sender.take() {
            sender.set_error(err);
            self.changed.notify();
        }
    }
}

/// Payload for Publish packet
pub struct Payload {
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
    /// Create fixed payload from bytes
    pub fn from_bytes(buf: Bytes) -> Payload {
        Payload {
            pl: Either::Left(Cell::new(Some(buf))),
        }
    }

    /// Check if payload is fixed
    pub fn is_fixed(&self) -> bool {
        self.pl.is_left()
    }

    /// Read next payload chunk
    pub async fn read(&self) -> Result<Option<Bytes>, PayloadError> {
        match &self.pl {
            Either::Left(pl) => Ok(pl.take()),
            Either::Right(pl) => pl.read().await.map_or(Ok(None), |res| res.map(Some)),
        }
    }

    /// Read complete payload
    ///
    /// Unlike [`read`](Self::read), returns `PayloadError::Consumed` error
    /// if the payload has already been read.
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_service::{Ctx, Pipeline, Service};
    use ntex_util::future::lazy;

    use super::*;

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
}
