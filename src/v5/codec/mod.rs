//! MQTT v5 Protocol codec

use ntex_bytes::{ByteString, Bytes};

#[allow(clippy::module_inception)]
mod codec;
mod decode;
mod encode;
mod packet;

pub use self::codec::Codec;
pub(crate) use self::encode::EncodeLtd;
pub use self::packet::*;

/// A single user property, a name/value pair
pub type UserProperty = (ByteString, ByteString);
/// A list of user properties
pub type UserProperties = Vec<UserProperty>;

/// Decoded MQTT v5 protocol item
#[derive(Clone, PartialEq, Eq)]
pub enum Decoded {
    /// Decoded packet and its size
    Packet(Packet, u32),
    /// Decoded `Publish` packet, the first payload chunk and the packet size
    Publish(Publish, Bytes, u32),
    /// Next chunk of the publish payload, the flag indicates the last chunk
    PayloadChunk(Bytes, bool),
}

/// Item to encode into the MQTT v5 protocol stream
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Encoded {
    /// Packet to encode
    Packet(Packet),
    /// `Publish` packet with an optional first payload chunk
    Publish(Publish, Option<Bytes>),
    /// Next chunk of the publish payload
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
