use std::{cell::Cell, fmt, num::NonZeroU32};

use ntex_bytes::{Buf, BufMut, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{packet_type, FixedHeader, MAX_PACKET_SIZE};
use crate::{payload::Payload, utils, utils::decode_variable_length};

use super::{decode::decode_packet, encode::EncodeLtd, packet::Publish, Packet};
use super::{DecodedPacket, EncodePacket};

pub struct Codec {
    state: Cell<DecodeState>,
    max_in_size: Cell<u32>,
    max_out_size: Cell<u32>,
    max_fixed_payload: Cell<u32>,
    flags: Cell<CodecFlags>,
    publish: Cell<Option<Publish>>,
    encoding_payload: Cell<Option<NonZeroU32>>,
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct CodecFlags: u8 {
        const NO_PROBLEM_INFO = 0b0000_0001;
        const NO_RETAIN       = 0b0000_0010;
        const NO_SUB_IDS      = 0b0000_1000;
    }
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    FrameHeader,
    Frame(FixedHeader),
    PublishHeader(FixedHeader),
    PublishProperties(u32, FixedHeader),
    PublishFixed(usize, u32),
    PublishVariable(usize),
}

impl Codec {
    /// Create `Codec` instance
    pub fn new() -> Self {
        Codec {
            state: Cell::new(DecodeState::FrameHeader),
            max_in_size: Cell::new(0),
            max_out_size: Cell::new(0),
            max_fixed_payload: Cell::new(0),
            flags: Cell::new(CodecFlags::empty()),
            publish: Cell::new(None),
            encoding_payload: Cell::new(None),
        }
    }

    /// Set max fixed payload size.
    ///
    /// If fized size is set to `0`, size is unlimited.
    /// By default max fized size is set to `0`
    pub fn set_max_fixed_payload(&self, size: u32) {
        self.max_fixed_payload.set(size)
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_inbound_size(&self) -> u32 {
        self.max_in_size.get()
    }

    /// Set max outbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn max_outbound_size(&self) -> u32 {
        self.max_out_size.get()
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
    pub fn set_max_outbound_size(&self, mut size: u32) {
        if size > 5 {
            // fixed header = 1, var_len(remaining.max_value()) = 4
            size -= 5;
        }
        self.max_out_size.set(size);
    }

    pub(crate) fn retain_available(&self) -> bool {
        !self.flags.get().contains(CodecFlags::NO_RETAIN)
    }

    pub(crate) fn sub_ids_available(&self) -> bool {
        !self.flags.get().contains(CodecFlags::NO_SUB_IDS)
    }

    pub(crate) fn set_retain_available(&self, val: bool) {
        let mut flags = self.flags.get();
        flags.set(CodecFlags::NO_RETAIN, !val);
        self.flags.set(flags);
    }

    pub(crate) fn set_sub_ids_available(&self, val: bool) {
        let mut flags = self.flags.get();
        flags.set(CodecFlags::NO_SUB_IDS, !val);
        self.flags.set(flags);
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for Codec {
    type Item = super::DecodedPacket;
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

                            if packet_type::is_publish(first_byte) {
                                self.state.set(DecodeState::PublishHeader(FixedHeader {
                                    first_byte,
                                    remaining_length,
                                }));
                            } else {
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
                        }
                        None => {
                            return Ok(None);
                        }
                    }
                }
                DecodeState::PublishHeader(fixed) => {
                    if let Some(len) = Publish::packet_header_size(src, fixed.first_byte)? {
                        self.state.set(DecodeState::PublishProperties(len, fixed));
                    } else {
                        return Ok(None);
                    }
                }
                DecodeState::PublishProperties(props_len, fixed) => {
                    if src.len() < props_len as usize {
                        return Ok(None);
                    }
                    let payload_len = (fixed.remaining_length - props_len) as usize;
                    let mut buf = src.split_to(props_len as usize).freeze();
                    let publish =
                        Publish::decode(&mut buf, fixed.first_byte, payload_len as usize)?;

                    let fixed_size = self.max_fixed_payload.get() as usize;
                    if fixed_size == 0 || payload_len <= fixed_size {
                        if src.len() < payload_len as usize {
                            self.publish.set(Some(publish));
                            self.state.set(DecodeState::PublishFixed(
                                payload_len,
                                fixed.remaining_length,
                            ));
                        } else {
                            self.state.set(DecodeState::FrameHeader);
                            src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length

                            let payload = src.split_to(payload_len as usize).freeze();
                            return Ok(Some(DecodedPacket::Publish(
                                publish,
                                payload,
                                fixed.remaining_length,
                            )));
                        }
                    } else {
                        let payload = src.split_to(payload_len as usize).freeze();
                        let remaining = payload_len - payload.len();

                        if remaining > 0 {
                            self.state.set(DecodeState::PublishVariable(remaining));
                        } else {
                            self.state.set(DecodeState::FrameHeader);
                            src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length
                        }

                        return Ok(Some(DecodedPacket::Publish(
                            publish,
                            payload,
                            fixed.remaining_length,
                        )));
                    }
                }

                DecodeState::PublishFixed(pl_len, remaining_length) => {
                    if src.len() < pl_len {
                        return Ok(None);
                    }
                    let publish = self.publish.take().unwrap();
                    let payload = src.split_to(pl_len).freeze();

                    self.state.set(DecodeState::FrameHeader);
                    src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length

                    return Ok(Some(DecodedPacket::Publish(
                        publish,
                        payload,
                        remaining_length,
                    )));
                }
                DecodeState::PublishVariable(remaining) => {
                    let payload = src.split_to(remaining as usize).freeze();
                    let remaining = remaining - payload.len();

                    let eof = if remaining > 0 {
                        self.state.set(DecodeState::PublishVariable(remaining));
                        false
                    } else {
                        self.state.set(DecodeState::FrameHeader);
                        src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length
                        true
                    };
                    return Ok(Some(DecodedPacket::PayloadChunk(payload, eof)));
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
                    return Ok(Some(DecodedPacket::Packet(packet, fixed.remaining_length)));
                }
            }
        }
    }
}

impl Encoder for Codec {
    type Item = EncodePacket;
    type Error = EncodeError;

    fn encode(&self, mut item: Self::Item, dst: &mut BytesMut) -> Result<(), EncodeError> {
        // handle [MQTT 3.1.2.11.7]
        if self.flags.get().contains(CodecFlags::NO_PROBLEM_INFO) {
            match item {
                EncodePacket::Packet(Packet::PublishAck(ref mut pkt))
                | EncodePacket::Packet(Packet::PublishReceived(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                EncodePacket::Packet(Packet::PublishRelease(ref mut pkt))
                | EncodePacket::Packet(Packet::PublishComplete(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                EncodePacket::Packet(Packet::Subscribe(ref mut pkt)) => {
                    pkt.user_properties.clear();
                }
                EncodePacket::Packet(Packet::SubscribeAck(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                EncodePacket::Packet(Packet::Unsubscribe(ref mut pkt)) => {
                    pkt.user_properties.clear();
                }
                EncodePacket::Packet(Packet::UnsubscribeAck(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                EncodePacket::Packet(Packet::Auth(ref mut pkt)) => {
                    pkt.user_properties.clear();
                    let _ = pkt.reason_string.take();
                }
                _ => (),
            }
        }

        let max_out_size = self.max_out_size.get();
        let max_size = if max_out_size != 0 { max_out_size } else { MAX_PACKET_SIZE };
        match item {
            EncodePacket::Packet(pkt) => {
                if self.encoding_payload.get().is_some() {
                    Err(EncodeError::ExpectPayload)
                } else {
                    let content_size = pkt.encoded_size(max_size);
                    if content_size > max_size as usize {
                        Err(EncodeError::OverMaxPacketSize)
                    } else {
                        dst.reserve(content_size + 5);
                        pkt.encode(dst, content_size as u32)?; // safe: max_size <= u32 max value
                        Ok(())
                    }
                }
            }
            EncodePacket::Publish(pkt, buf) => {
                let content_size = pkt.encoded_size(max_size);
                let total_size = content_size + pkt.payload_len;
                if total_size > max_size as usize {
                    return Err(EncodeError::OverMaxPacketSize);
                }

                dst.reserve(content_size + 5);

                // publish prefix
                dst.put_u8(
                    packet_type::PUBLISH_START
                        | (u8::from(pkt.qos) << 1)
                        | ((pkt.dup as u8) << 3)
                        | (pkt.retain as u8),
                );
                utils::write_variable_length(total_size as u32, dst);

                pkt.encode(dst, content_size as u32)?; // safe: max_size <= u32 max value

                let remaining = if let Some(buf) = buf {
                    dst.extend_from_slice(&buf);
                    pkt.payload_len - buf.len()
                } else {
                    pkt.payload_len
                };
                self.encoding_payload.set(NonZeroU32::new(remaining as u32));
                Ok(())
            }
            EncodePacket::PayloadChunk(chunk) => {
                if let Some(remaining) = self.encoding_payload.get() {
                    let len = chunk.len() as u32;
                    if len > remaining.get() {
                        Err(EncodeError::OverPublishSize)
                    } else {
                        dst.extend_from_slice(&chunk);
                        self.encoding_payload.set(NonZeroU32::new(remaining.get() - len));
                        Ok(())
                    }
                } else {
                    Err(EncodeError::UnexpectedPayload)
                }
            }
        }
    }
}

impl Clone for Codec {
    fn clone(&self) -> Self {
        Codec {
            state: Cell::new(DecodeState::FrameHeader),
            max_in_size: self.max_in_size.clone(),
            max_out_size: self.max_out_size.clone(),
            max_fixed_payload: self.max_fixed_payload.clone(),
            flags: Cell::new(CodecFlags::empty()),
            publish: Cell::new(None),
            encoding_payload: Cell::new(None),
        }
    }
}

impl fmt::Debug for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Codec")
            .field("state", &self.state)
            .field("max_in_size", &self.max_in_size)
            .field("max_out_size", &self.max_out_size)
            .field("max_fixed_payload", &self.max_fixed_payload)
            .field("flags", &self.flags)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_size() {
        let codec = Codec::new();
        codec.set_max_inbound_size(5);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(codec.decode(&mut buf), Err(DecodeError::MaxSizeExceeded));
    }
}
