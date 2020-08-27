use derive_more::Display;

pub use crate::error::*;
pub use crate::v5::codec;

#[derive(Debug, Display)]
pub enum PublishError {
    /// Negative ack
    #[display(fmt = "Negative ack: {:?}", _0)]
    Fail(codec::PublishAck),
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
}
