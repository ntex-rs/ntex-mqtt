//! MQTT v5 Protocol codec

use ntex_bytes::{ByteString, Bytes};

use crate::Ticket;

/// Stateful packet framing and streamed publish payloads.
#[allow(clippy::module_inception)]
mod codec;
/// Dispatch of complete non-publish packet bodies.
mod decode;
/// Packet validation, sizing, and wire encoding helpers.
mod encode;
/// MQTT v5 control packets and their wire representations.
mod packet;

pub use self::codec::Codec;
pub(crate) use self::encode::EncodeLtd;
pub use self::packet::*;

/// A single user property, a name/value pair
pub type UserProperty = (ByteString, ByteString);
/// A list of user properties
pub type UserProperties = Vec<UserProperty>;

/// Decoded MQTT v5 protocol item
#[derive(PartialEq, Eq)]
pub enum Decoded {
    /// A non-publish packet and its remaining length in bytes, excluding the fixed header.
    Packet(Packet, u32),
    /// A `Publish` header, initial payload bytes, and remaining length excluding the fixed header.
    ///
    /// The initial chunk may be empty; compare its length with `Publish::payload_size`
    /// to determine whether further payload chunks are expected.
    ///
    /// The arrival [`Ticket`] is issued for `QoS 1` and `QoS 2` publishes in decode order,
    /// dropping it skips the publish in the arrival order.
    Publish(Publish, Bytes, u32, Option<Ticket>),
    /// Subsequent publish payload bytes; `true` marks the final chunk.
    PayloadChunk(Bytes, bool),
}

/// Item to encode into the MQTT v5 protocol stream
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Encoded {
    /// Packet to encode
    Packet(Packet),
    /// A `Publish` header and optional initial payload bytes.
    ///
    /// Supply exactly `Publish::payload_size` bytes across this item and later chunks
    /// before encoding another packet.
    Publish(Publish, Option<Bytes>),
    /// Bytes continuing the current publish; rejected if no payload is pending.
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
            Decoded::Publish(pkt, _, size, ticket) => f
                .debug_tuple("Decoded::Publish")
                .field(pkt)
                .field(&"<REDACTED>")
                .field(size)
                .field(ticket)
                .finish(),
            Decoded::PayloadChunk(_, eof) => f
                .debug_tuple("Decoded::Publish")
                .field(&"<REDACTED>")
                .field(eof)
                .finish(),
        }
    }
}
