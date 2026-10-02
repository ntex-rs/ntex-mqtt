//! MQTT v3.1.1 Protocol codec

use ntex_bytes::Bytes;

#[allow(clippy::module_inception)]
mod codec;
mod decode;
pub(crate) mod encode;
mod packet;

pub use self::codec::Codec;
pub use self::packet::{
    Connect, ConnectAck, ConnectAckReason, LastWill, Packet, Publish, SubscribeReturnCode,
};
pub use crate::types::{ConnectAckFlags, ConnectFlags, QoS};

/// Result of a decode operation
#[derive(Clone, PartialEq, Eq)]
pub enum Decoded {
    /// Decoded control packet and its total size in bytes
    Packet(Packet, u32),
    /// Decoded PUBLISH packet, the first payload chunk and the packet size in bytes
    Publish(Publish, Bytes, u32),
    /// Next chunk of a PUBLISH payload, the flag indicates the last chunk
    PayloadChunk(Bytes, bool),
}

/// Item that can be encoded and sent to the peer
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Encoded {
    /// Control packet
    Packet(Packet),
    /// PUBLISH packet with an optional first payload chunk
    Publish(Publish, Option<Bytes>),
    /// Next chunk of a PUBLISH payload
    PayloadChunk(Bytes),
}

impl From<Packet> for Encoded {
    fn from(pkt: Packet) -> Encoded {
        Encoded::Packet(pkt)
    }
}

impl std::fmt::Debug for Decoded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Decoded::Packet(pkt, size) => f
                .debug_tuple("Decoded::Packet")
                .field(pkt)
                .field(size)
                .finish(),
            Decoded::Publish(pkt, _, size) => f
                .debug_tuple("Decoded::Publish")
                .field(pkt)
                .field(&"<REDACTED>")
                .field(size)
                .finish(),
            Decoded::PayloadChunk(_, eof) => f
                .debug_tuple("Decoded::Publish")
                .field(&"<REDACTED>")
                .field(eof)
                .finish(),
        }
    }
}
