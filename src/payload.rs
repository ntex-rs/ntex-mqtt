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

impl Default for Payload {
    fn default() -> Self {
        Payload { pl: Either::Left(Cell::new(None)) }
    }
}

impl Payload {
    pub fn from_bytes(buf: Bytes) -> Payload {
        Payload { pl: Either::Left(Cell::new(Some(buf))) }
    }

    pub(crate) fn from_stream(buf: Bytes, buf_size: usize) -> (Payload, PlSender) {
        let (tx, rx) = bstream::channel();
        rx.max_buffer_size(buf_size);
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
            Either::Right(pl) => pl.read().await.map_or(Ok(None), |res| res.map(Some)),
        }
    }

    /// Read complete payload
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

#[cfg(test)]
mod tests {
    use super::*;

    #[ntex::test]
    async fn test_payload() {
        let (pl, tx) = Payload::from_stream(Bytes::from(b"chunk1"), 32 * 1024);

        ntex::rt::spawn(async move {
            ntex::time::sleep(ntex::time::Millis(50)).await;
            tx.feed_data(b"chunk2".into());
            ntex::time::sleep(ntex::time::Millis(50)).await;
            tx.feed_data(b"chunk3".into());
            tx.feed_eof();
        });

        let data = pl.read_all().await.unwrap();
        assert_eq!(data, b"chunk1chunk2chunk3");
    }
}
