use std::cell::Cell;

use ntex::codec::{Decoder, Encoder};
use ntex::util::{Buf, BytesMut};

use super::{decode, encode, Packet, Publish};
use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, QoS};
use crate::utils::decode_variable_length;

#[derive(Debug, Clone)]
/// Mqtt v3.1.1 protocol codec
pub struct Codec {
    state: Cell<DecodeState>,
    max_size: Cell<u32>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum DecodeState {
    FrameHeader,
    Frame(FixedHeader),
}

impl Codec {
    /// Create `Codec` instance
    pub fn new() -> Self {
        Codec { state: Cell::new(DecodeState::FrameHeader), max_size: Cell::new(0) }
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_size(&self, size: u32) {
        self.max_size.set(size);
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for Codec {
    type Item = Packet;
    type Error = DecodeError;

    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, DecodeError> {
        loop {
            match self.state.get() {
                DecodeState::FrameHeader => {
                    if src.len() < 2 {
                        return Ok(None);
                    }
                    let src_slice = src.as_ref();
                    let first_byte = src_slice[0];
                    match decode_variable_length(&src_slice[1..])? {
                        Some((remaining_length, consumed)) => {
                            // check max message size
                            let max_size = self.max_size.get();
                            if max_size != 0 && max_size < remaining_length {
                                return Err(DecodeError::MaxSizeExceeded);
                            }
                            src.advance(consumed + 1);
                            self.state.set(DecodeState::Frame(FixedHeader {
                                first_byte,
                                remaining_length,
                            }));
                            // todo: validate remaining_length against max frame size config
                            let remaining_length = remaining_length as usize;
                            if src.len() < remaining_length {
                                // todo: subtract?
                                src.reserve(remaining_length); // extend receiving buffer to fit the whole frame -- todo: too eager?
                                return Ok(None);
                            }
                        }
                        None => {
                            return Ok(None);
                        }
                    }
                }
                DecodeState::Frame(fixed) => {
                    if src.len() < fixed.remaining_length as usize {
                        return Ok(None);
                    }
                    let packet_buf = src.split_to(fixed.remaining_length as usize);
                    let packet = decode::decode_packet(packet_buf.freeze(), fixed.first_byte)?;
                    self.state.set(DecodeState::FrameHeader);
                    src.reserve(2);
                    return Ok(Some(packet));
                }
            }
        }
    }
}

impl Encoder for Codec {
    type Item = Packet;
    type Error = EncodeError;

    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), EncodeError> {
        if let Packet::Publish(Publish { qos, packet_id, .. }) = item {
            if (qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce) && packet_id.is_none() {
                return Err(EncodeError::PacketIdRequired);
            }
        }
        let content_size = encode::get_encoded_size(&item);
        dst.reserve(content_size + 5);
        encode::encode(&item, dst, content_size as u32)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntex::util::{ByteString, Bytes};

    #[test]
    fn test_max_size() {
        let codec = Codec::new();
        codec.set_max_size(5);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(codec.decode(&mut buf), Err(DecodeError::MaxSizeExceeded));
    }

    #[test]
    fn test_packet() {
        let codec = Codec::new();
        let mut buf = BytesMut::new();

        let pkt = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("/test"),
            packet_id: None,
            payload: Bytes::from(Vec::from("a".repeat(260 * 1024))),
        };
        codec.encode(Packet::Publish(pkt.clone()), &mut buf).unwrap();

        let pkt2 = if let Packet::Publish(v) = codec.decode(&mut buf).unwrap().unwrap() {
            v
        } else {
            panic!()
        };
        assert_eq!(pkt, pkt2);
    }
}
