//! MQTT v3.1.1 Protocol codec

use ntex_bytes::Bytes;

/// Stateful framing and streamed publish encoding.
#[allow(clippy::module_inception)]
mod codec;
/// Packet-body parsing and wire-format validation.
mod decode;
/// Packet sizing, sender validation, and serialization.
pub(crate) mod encode;
/// MQTT 3.1.1 control packet representations.
mod packet;

pub use self::codec::Codec;
pub use self::packet::{
    Connect, ConnectAck, ConnectAckReason, LastWill, Packet, Publish, SubscribeReturnCode,
};
pub use crate::types::{ConnectAckFlags, ConnectFlags, QoS};

/// Result of a decode operation
#[derive(Clone, PartialEq, Eq)]
pub enum Decoded {
    /// Decoded control packet and its remaining length in bytes, excluding the fixed header.
    Packet(Packet, u32),
    /// Decoded PUBLISH header, optional initial payload bytes, and remaining length in bytes.
    Publish(Publish, Bytes, u32),
    /// Next payload chunk; the flag is `true` when the declared payload is complete.
    PayloadChunk(Bytes, bool),
}

/// Item that can be encoded and sent to the peer
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Encoded {
    /// Control packet
    Packet(Packet),
    /// PUBLISH metadata and optional initial bytes; later chunks must complete `payload_size`.
    Publish(Publish, Option<Bytes>),
    /// Next bytes of the current payload; no other packet can interleave until it is complete.
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
