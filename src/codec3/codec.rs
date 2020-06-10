use bytes::{buf::Buf, BytesMut};
use ntex_codec::{Decoder, Encoder};

use super::{decode, encode, Packet, Publish, QoS};
use crate::error::{DecodeError, EncodeError};

#[derive(Debug)]
pub struct Codec {
    state: DecodeState,
    max_size: usize,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    FrameHeader,
    Frame(FixedHeader),
}

impl Codec {
    /// Create `Codec` instance
    pub fn new() -> Self {
        Codec {
            state: DecodeState::FrameHeader,
            max_size: 0,
        }
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
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

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, DecodeError> {
        loop {
            match self.state {
                DecodeState::FrameHeader => {
                    if src.len() < 2 {
                        return Ok(None);
                    }
                    let src_slice = src.as_ref();
                    let first_byte = src_slice[0];
                    match super::decode::decode_variable_length(&src_slice[1..])? {
                        Some((remaining_length, consumed)) => {
                            // check max message size
                            if self.max_size != 0 && self.max_size < remaining_length {
                                return Err(DecodeError::MaxSizeExceeded);
                            }
                            src.advance(consumed + 1);
                            self.state = DecodeState::Frame(FixedHeader {
                                first_byte,
                                remaining_length,
                            });
                            // todo: validate remaining_length against max frame size config
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
                    if src.len() < fixed.remaining_length {
                        return Ok(None);
                    }
                    let packet_buf = src.split_to(fixed.remaining_length);
                    let packet = decode::decode_packet(packet_buf.freeze(), fixed.first_byte)?;
                    self.state = DecodeState::FrameHeader;
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

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), EncodeError> {
        if let Packet::Publish(Publish { qos, packet_id, .. }) = item {
            if (qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce) && packet_id.is_none() {
                return Err(EncodeError::PacketIdRequired);
            }
        }
        let content_size = encode::get_encoded_size(&item);
        dst.reserve(content_size + 5);
        encode::encode(&item, dst, content_size)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) struct FixedHeader {
    /// Fixed Header byte
    pub first_byte: u8,
    /// the number of bytes remaining within the current packet,
    /// including data in the variable header and the payload.
    pub remaining_length: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_size() {
        let mut codec = Codec::new().max_size(5);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(codec.decode(&mut buf), Err(DecodeError::MaxSizeExceeded));
    }
}
