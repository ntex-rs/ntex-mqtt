use derive_more::Display;

pub use crate::error::*;
pub use crate::v5::codec;

#[derive(Debug, Display)]
pub enum PublishQos0Error {
    /// Encoder error
    Encode(EncodeError),
    /// Can not allocate next packet id
    #[display(fmt = "Can not allocate next packet id")]
    PacketIdNotAvailable,
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
}

#[derive(Debug, Display)]
pub enum PublishQos1Error {
    /// Negative ack from peer
    #[display(fmt = "Negative ack: {:?}", _0)]
    Fail(codec::PublishAck),
    /// Encoder error
    Encode(EncodeError),
    /// Can not allocate next packet id
    #[display(fmt = "Can not allocate next packet id")]
    PacketIdNotAvailable,
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
}
