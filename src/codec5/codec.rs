use super::decode::{decode_packet, decode_variable_length};
use super::encode::EncodeLtd;
use super::error::{EncodeError, ParseError};
use super::{Packet, MAX_PACKET_SIZE};
use bytes::{Buf, BytesMut};
use ntex_codec::{Decoder, Encoder};

#[derive(Debug)]
pub struct Codec {
    state: DecodeState,
    max_size: usize,
    max_packet_size: Option<u32>,
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
            max_packet_size: None,
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
    type Error = ParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, ParseError> {
        loop {
            match self.state {
                DecodeState::FrameHeader => {
                    if src.len() < 2 {
                        return Ok(None);
                    }
                    let src_slice = src.as_ref();
                    let first_byte = src_slice[0];
                    match decode_variable_length(&src_slice[1..])? {
                        Some((remaining_length, consumed)) => {
                            // check max message size
                            if self.max_size != 0 && self.max_size < remaining_length as usize {
                                return Err(ParseError::MaxSizeExceeded);
                            }
                            src.advance(consumed + 1);
                            self.state = DecodeState::Frame(FixedHeader {
                                first_byte,
                                remaining_length,
                            });
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
                    let packet_buf = src.split_to(fixed.remaining_length as usize).freeze();
                    let packet = decode_packet(packet_buf, fixed.first_byte)?;
                    self.state = DecodeState::FrameHeader;
                    src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length
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
        let max_size = self.max_packet_size.map_or(MAX_PACKET_SIZE, |v| v - 5); // fixed header = 1, var_len(remaining.max_value()) = 4
        let content_size = item.encoded_size(max_size);
        if content_size > max_size as usize {
            return Err(EncodeError::InvalidLength); // todo: separate error code
        }
        dst.reserve(content_size + 5);
        item.encode(dst, content_size as u32)?; // safe: max_size <= u32 max value

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) struct FixedHeader {
    /// Fixed Header byte
    pub first_byte: u8,
    /// the number of bytes remaining within the current packet,
    /// including data in the variable header and the payload.
    pub remaining_length: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_size() {
        let mut codec = Codec::new().max_size(5);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(codec.decode(&mut buf), Err(ParseError::MaxSizeExceeded));
    }
}
