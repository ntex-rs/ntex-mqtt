use std::cell::Cell;

use ntex::codec::{Decoder, Encoder};
use ntex::util::{Buf, BytesMut};

use super::{decode::decode_packet, encode::EncodeLtd, Packet};
use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, MAX_PACKET_SIZE};
use crate::utils::decode_variable_length;

#[derive(Debug, Clone)]
pub struct Codec {
    state: Cell<DecodeState>,
    max_in_size: Cell<u32>,
    max_out_size: Cell<u32>,
    flags: Cell<CodecFlags>,
}

bitflags::bitflags! {
    pub struct CodecFlags: u8 {
        const NO_PROBLEM_INFO = 0b0000_0001;
    }
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
            state: Cell::new(DecodeState::FrameHeader),
            max_in_size: Cell::new(0),
            max_out_size: Cell::new(0),
            flags: Cell::new(CodecFlags::empty()),
        }
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_inbound_size(self, size: u32) -> Self {
        self.max_in_size.set(size);
        self
    }

    /// Set max outbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_outbound_size(self, mut size: u32) -> Self {
        if size > 5 {
            // fixed header = 1, var_len(remaining.max_value()) = 4
            size -= 5;
        }
        self.max_out_size.set(size);
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_inbound_size(&self, size: u32) {
        self.max_in_size.set(size);
    }

    /// Set max outbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_outbound_size(&self, size: u32) {
        self.max_out_size.set(size);
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
                            let max_in_size = self.max_in_size.get();
                            if max_in_size != 0 && max_in_size < remaining_length {
                                log::debug!(
                                    "MaxSizeExceeded max-size: {}, remaining: {}",
                                    max_in_size,
                                    remaining_length
                                );
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
                    let packet_buf = src.split_to(fixed.remaining_length as usize).freeze();
                    let packet = decode_packet(packet_buf, fixed.first_byte)?;
                    self.state.set(DecodeState::FrameHeader);
                    src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length

                    if let Packet::Connect(ref pkt) = packet {
                        let mut flags = self.flags.get();
                        flags.set(CodecFlags::NO_PROBLEM_INFO, !pkt.request_problem_info);
                        self.flags.set(flags);
                    }
                    return Ok(Some(packet));
                }
            }
        }
    }
}

impl Encoder for Codec {
    type Item = Packet;
    type Error = EncodeError;

    fn encode(&self, mut item: Self::Item, dst: &mut BytesMut) -> Result<(), EncodeError> {
        // handle [MQTT 3.1.2.11.7]
        if self.flags.get().contains(CodecFlags::NO_PROBLEM_INFO) {
            match item {
                Packet::PublishAck(ref mut pkt) | Packet::PublishReceived(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::PublishRelease(ref mut pkt) | Packet::PublishComplete(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::Subscribe(ref mut pkt) => {
                    pkt.user_properties.clear();
                }
                Packet::SubscribeAck(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::Unsubscribe(ref mut pkt) => {
                    pkt.user_properties.clear();
                }
                Packet::UnsubscribeAck(ref mut pkt) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Packet::Auth(ref mut pkt) => {
                    pkt.user_properties.clear();
                    let _ = pkt.reason_string.take();
                }
                _ => (),
            }
        }

        let max_out_size = self.max_out_size.get();
        let max_size = if max_out_size != 0 { max_out_size } else { MAX_PACKET_SIZE };
        let content_size = item.encoded_size(max_size);
        if content_size > max_size as usize {
            return Err(EncodeError::InvalidLength); // todo: separate error code
        }
        dst.reserve(content_size + 5);
        item.encode(dst, content_size as u32)?; // safe: max_size <= u32 max value
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_size() {
        let codec = Codec::new().max_inbound_size(5);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(codec.decode(&mut buf), Err(DecodeError::MaxSizeExceeded));
    }
}
