use std::{cell::Cell, fmt, mem};

use ntex_bytes::{Bytes, BytesMut};
use ntex_util::{channel::bstream, future::Either};

pub(crate) use ntex_util::channel::bstream::Status as PayloadStatus;

use crate::error::PayloadError;

type PlStream = bstream::Receiver<PayloadError>;
pub(crate) type PlSender = bstream::Sender<PayloadError>;

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
    pub fn from_bytes(buf: Bytes) -> Payload {
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

    /// Read next payload chunk
    pub async fn read(&self) -> Result<Option<Bytes>, PayloadError> {
        match &self.pl {
            Either::Left(pl) => Ok(pl.take()),
            Either::Right(pl) => {
                pl.read().await.map_or(Ok(None), |res| res.map(|val| Some(val)))
            }
        }
    }

    /// Read complete payload
    pub async fn read_all(&self) -> Result<Bytes, PayloadError> {
        match &self.pl {
            Either::Left(pl) => pl.take().ok_or(PayloadError::Consumed),
            Either::Right(pl) => {
                let mut chunk = if let Some(item) = pl.read().await {
                    Some(item?)
                } else {
                    return Err(PayloadError::Consumed);
                };

                let mut buf = BytesMut::new();
                loop {
                    return match pl.read().await {
                        Some(Ok(b)) => {
                            if let Some(chunk) = chunk.take() {
                                buf.reserve(b.len() + chunk.len());
                                buf.extend_from_slice(&chunk);
                            }
                            buf.extend_from_slice(&b);
                            continue;
                        }
                        None => Ok(chunk.unwrap_or_else(|| buf.freeze())),
                        Some(Err(err)) => Err(err),
                    };
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
