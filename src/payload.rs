use std::{cell::Cell, fmt, io, mem};

use ntex_bytes::{Bytes, BytesMut};
use ntex_util::{channel::bstream, future::Either};

type PlStream = bstream::Receiver<()>;
pub type PayloadSender = bstream::Sender<()>;

/// Payload for Publish packet
pub struct Payload {
    pl: Either<Cell<Option<Bytes>>, PlStream>,
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

    pub(crate) fn from_stream(buf: Bytes) -> (Payload, PayloadSender) {
        let (tx, rx) = bstream::channel(false);
        tx.feed_data(buf);

        (Payload { pl: Either::Right(rx) }, tx)
    }

    /// Check if payload is fixed
    pub fn is_fixed(&self) -> bool {
        self.pl.is_left()
    }

    /// Read next chunk
    pub async fn read(&self) -> Option<Bytes> {
        match &self.pl {
            Either::Left(pl) => pl.take(),
            Either::Right(pl) => pl.read().await.and_then(|res| res.ok()),
        }
    }

    pub async fn read_all(&self) -> Option<Bytes> {
        match &self.pl {
            Either::Left(pl) => pl.take(),
            Either::Right(pl) => {
                let mut buf = if let Some(buf) = pl.read().await.and_then(|res| res.ok()) {
                    BytesMut::copy_from_slice(&buf)
                } else {
                    return None;
                };

                loop {
                    match pl.read().await {
                        None => return Some(buf.freeze()),
                        Some(Ok(b)) => buf.extend_from_slice(&b),
                        Some(Err(_)) => return None,
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
