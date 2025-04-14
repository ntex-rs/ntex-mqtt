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

pub type UserProperty = (ByteString, ByteString);
pub type UserProperties = Vec<UserProperty>;

#[derive(Debug)]
pub enum DecodedPacket {
    Packet(Packet, u32),
    Publish(Publish, Bytes, u32),
    PayloadChunk(Bytes, bool),
}

#[derive(Debug)]
pub enum EncodePacket {
    Packet(Packet),
    Publish(Publish, Option<Bytes>),
    PayloadChunk(Bytes),
}

impl From<Packet> for EncodePacket {
    fn from(pkt: Packet) -> EncodePacket {
        EncodePacket::Packet(pkt)
    }
}
