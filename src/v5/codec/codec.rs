use std::{cell::Cell, cmp::min, fmt, num::NonZeroU32};

use ntex_bytes::{Buf, BufMut, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, MAX_PACKET_SIZE, packet_type};
use crate::{payload::Payload, utils, utils::decode_variable_length};

use super::{Decoded, Encoded};
use super::{Packet, decode::decode_packet, encode::EncodeLtd, packet::Publish};

pub struct Codec {
    state: Cell<DecodeState>,
    max_in_size: Cell<u32>,
    max_out_size: Cell<u32>,
    min_chunk_size: Cell<u32>,
    flags: Cell<CodecFlags>,
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
    PublishPayload(u32),
}

impl Codec {
    /// Create `Codec` instance
    pub fn new() -> Self {
        Codec {
            state: Cell::new(DecodeState::FrameHeader),
            max_in_size: Cell::new(0),
            max_out_size: Cell::new(0),
            min_chunk_size: Cell::new(0),
            flags: Cell::new(CodecFlags::empty()),
            encoding_payload: Cell::new(None),
        }
    }

    /// Set min payload chunk size.
    ///
    /// If the minimum size is set to `0`, incoming payload chunks
    /// will be processed immediately. Otherwise, the codec will
    /// accumulate chunks until the total size reaches the specified minimum.
    /// By default min size is set to `0`
    pub fn set_min_chunk_size(&self, size: u32) {
        self.min_chunk_size.set(size)
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
    type Item = super::Decoded;
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
                    let payload_len = (fixed.remaining_length - props_len);
                    let mut buf = src.split_to(props_len as usize).freeze();
                    let publish = Publish::decode(&mut buf, fixed.first_byte, payload_len)?;

                    let len = src.len() as u32;
                    let min_chunk_size = self.min_chunk_size.get();
                    if len >= payload_len || min_chunk_size == 0 || len >= min_chunk_size {
                        let payload =
                            src.split_to(min(src.len(), payload_len as usize)).freeze();
                        let remaining = payload_len - payload.len() as u32;

                        if remaining > 0 {
                            self.state.set(DecodeState::PublishPayload(remaining));
                        } else {
                            self.state.set(DecodeState::FrameHeader);
                            src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length
                        }

                        return Ok(Some(Decoded::Publish(
                            publish,
                            payload,
                            fixed.remaining_length,
                        )));
                    } else {
                        self.state.set(DecodeState::PublishPayload(payload_len));
                        return Ok(Some(Decoded::Publish(
                            publish,
                            Bytes::new(),
                            fixed.remaining_length,
                        )));
                    }
                }
                DecodeState::PublishPayload(remaining) => {
                    let len = src.len() as u32;
                    let min_chunk_size = self.min_chunk_size.get();

                    return if (len >= remaining)
                        || (min_chunk_size != 0 && len >= min_chunk_size)
                    {
                        let payload = src.split_to(min(src.len(), remaining as usize)).freeze();
                        let remaining = remaining - payload.len() as u32;

                        let eof = if remaining > 0 {
                            self.state.set(DecodeState::PublishPayload(remaining));
                            false
                        } else {
                            self.state.set(DecodeState::FrameHeader);
                            src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length
                            true
                        };
                        Ok(Some(Decoded::PayloadChunk(payload, eof)))
                    } else {
                        Ok(None)
                    };
                }
                DecodeState::Frame(fixed) => {
                    return if src.len() < fixed.remaining_length as usize {
                        Ok(None)
                    } else {
                        let packet_buf = src.split_to(fixed.remaining_length as usize).freeze();
                        let packet = decode_packet(packet_buf, fixed.first_byte)?;
                        self.state.set(DecodeState::FrameHeader);
                        src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length

                        if let Packet::Connect(ref pkt) = packet {
                            let mut flags = self.flags.get();
                            flags.set(CodecFlags::NO_PROBLEM_INFO, !pkt.request_problem_info);
                            self.flags.set(flags);
                        }
                        Ok(Some(Decoded::Packet(packet, fixed.remaining_length)))
                    };
                }
            }
        }
    }
}

impl Encoder for Codec {
    type Item = Encoded;
    type Error = EncodeError;

    fn encode(&self, mut item: Self::Item, dst: &mut BytesMut) -> Result<(), EncodeError> {
        // handle [MQTT 3.1.2.11.7]
        if self.flags.get().contains(CodecFlags::NO_PROBLEM_INFO) {
            match item {
                Encoded::Packet(Packet::PublishAck(ref mut pkt))
                | Encoded::Packet(Packet::PublishReceived(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Encoded::Packet(Packet::PublishRelease(ref mut pkt))
                | Encoded::Packet(Packet::PublishComplete(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Encoded::Packet(Packet::Subscribe(ref mut pkt)) => {
                    pkt.user_properties.clear();
                }
                Encoded::Packet(Packet::SubscribeAck(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Encoded::Packet(Packet::Unsubscribe(ref mut pkt)) => {
                    pkt.user_properties.clear();
                }
                Encoded::Packet(Packet::UnsubscribeAck(ref mut pkt)) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Encoded::Packet(Packet::Auth(ref mut pkt)) => {
                    pkt.user_properties.clear();
                    let _ = pkt.reason_string.take();
                }
                _ => (),
            }
        }

        let max_out_size = self.max_out_size.get();
        let max_size = if max_out_size != 0 { max_out_size } else { MAX_PACKET_SIZE };
        match item {
            Encoded::Packet(pkt) => {
                if self.encoding_payload.get().is_some() {
                    log::trace!("Expect payload, received {:?}", pkt);
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
            Encoded::Publish(pkt, buf) => {
                let content_size = pkt.encoded_size(max_size) as u32;
                if content_size > max_size {
                    return Err(EncodeError::OverMaxPacketSize);
                }

                let total_size = content_size - pkt.payload_size
                    + buf.as_ref().map(|b| b.len() as u32).unwrap_or(0);
                dst.reserve((total_size + 5) as usize);
                pkt.encode(dst, content_size)?; // safe: max_size <= u32 max value

                let remaining = if let Some(buf) = buf {
                    dst.extend_from_slice(&buf);
                    pkt.payload_size - buf.len() as u32
                } else {
                    pkt.payload_size
                };
                self.encoding_payload.set(NonZeroU32::new(remaining as u32));
                Ok(())
            }
            Encoded::PayloadChunk(chunk) => {
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
            min_chunk_size: self.min_chunk_size.clone(),
            flags: Cell::new(CodecFlags::empty()),
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
            .field("min_chunk_size", &self.min_chunk_size)
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
        assert_eq!(codec.decode(&mut buf).err(), Some(DecodeError::MaxSizeExceeded));
    }
}
