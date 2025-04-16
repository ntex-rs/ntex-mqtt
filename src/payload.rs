use std::{cell::Cell, fmt, io, mem};

use ntex_bytes::{Bytes, BytesMut};
use ntex_util::{channel::bstream, future::Either};

pub(crate) use ntex_util::channel::bstream::Status as PayloadStatus;

type PlStream = bstream::Receiver<()>;
pub(crate) type PlSender = bstream::Sender<()>;

/// Payload for Publish packet
pub struct Payload {
    pl: Either<Cell<Option<Bytes>>, PlStream>,
}

/// Client payload streaming
pub struct PayloadSender {
    tx: PlSender,
}

impl Default for Payload {
    fn default() -> Self {
        Payload { pl: Either::Left(Cell::new(None)) }
    }
}

impl Payload {
    pub(crate) fn from_bytes(buf: Bytes) -> Payload {
        Payload { pl: Either::Left(Cell::new(Some(buf))) }
    }

    pub(crate) fn from_stream(buf: Bytes) -> (Payload, PlSender) {
        let (tx, rx) = bstream::channel();
        if !buf.is_empty() {
            tx.feed_data(buf);
        }
        (Payload { pl: Either::Right(rx) }, tx)
    }

    /// Check if payload is fixed
    pub fn is_fixed(&self) -> bool {
        self.pl.is_left()
    }

    /// Read next chunk
    pub async fn read(&self) -> Option<Result<Bytes, ()>> {
        match &self.pl {
            Either::Left(pl) => pl.take().map(|b| Ok(b)),
            Either::Right(pl) => pl.read().await,
        }
    }

    pub async fn read_all(&self) -> Option<Result<Bytes, ()>> {
        match &self.pl {
            Either::Left(pl) => pl.take().map(|b| Ok(b)),
            Either::Right(pl) => {
                let mut buf = if let Some(buf) = pl.read().await.and_then(|res| res.ok()) {
                    BytesMut::copy_from_slice(&buf)
                } else {
                    return None;
                };

                loop {
                    match pl.read().await {
                        None => return Some(Ok(buf.freeze())),
                        Some(Ok(b)) => buf.extend_from_slice(&b),
                        Some(Err(_)) => return Some(Err(())),
                    }
                }
            }
        }
    }

    pub fn take(&mut self) -> Payload {
        Payload { pl: mem::replace(&mut self.pl, Either::Left(Cell::new(None))) }
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
