use std::{cell::Cell, cmp::min, num::NonZeroU32};

use ntex_bytes::{Buf, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, QoS, packet_type};
use crate::utils::decode_variable_length;

use super::{Decoded, Encoded, Publish, decode, encode};

#[derive(Debug, Clone)]
/// Mqtt v3.1.1 protocol codec
pub struct Codec {
    state: Cell<DecodeState>,
    max_size: Cell<u32>,
    min_chunk_size: Cell<u32>,
    encoding_payload: Cell<Option<NonZeroU32>>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum DecodeState {
    FrameHeader,
    Frame(FixedHeader),
    PublishHeader(FixedHeader),
    PublishPayload(u32),
}

impl Codec {
    /// Create `Codec` instance
    pub fn new() -> Self {
        Codec {
            state: Cell::new(DecodeState::FrameHeader),
            max_size: Cell::new(0),
            min_chunk_size: Cell::new(0),
            encoding_payload: Cell::new(None),
        }
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_size(&self, size: u32) {
        self.max_size.set(size);
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
}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for Codec {
    type Item = Decoded;
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
                    if let Some(hdr_len) = decode::publish_size(src, fixed.first_byte)? {
                        if src.len() < hdr_len as usize {
                            return Ok(None);
                        }
                        let payload_len = fixed.remaining_length - hdr_len;
                        let mut buf = src.split_to(hdr_len as usize).freeze();
                        let publish = decode::decode_publish_packet(
                            &mut buf,
                            fixed.first_byte,
                            payload_len,
                        )?;

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
                    return Ok(None);
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
                    if src.len() < fixed.remaining_length as usize {
                        return Ok(None);
                    }
                    let packet_buf = src.split_to(fixed.remaining_length as usize);
                    let packet = decode::decode_packet(packet_buf.freeze(), fixed.first_byte)?;
                    self.state.set(DecodeState::FrameHeader);
                    src.reserve(2);
                    return Ok(Some(Decoded::Packet(packet, fixed.remaining_length)));
                }
            }
        }
    }
}

impl Encoder for Codec {
    type Item = Encoded;
    type Error = EncodeError;

    fn encode(&self, item: Self::Item, dst: &mut BytesMut) -> Result<(), EncodeError> {
        match item {
            Encoded::Packet(pkt) => {
                let content_size = encode::get_encoded_size(&pkt);
                dst.reserve(content_size + 5);
                encode::encode(&pkt, dst, content_size as u32)?;
                Ok(())
            }
            Encoded::Publish(pkt, buf) => {
                let Publish { qos, packet_id, .. } = pkt;
                if (qos == QoS::AtLeastOnce || qos == QoS::ExactlyOnce) && packet_id.is_none() {
                    return Err(EncodeError::PacketIdRequired);
                }

                let content_size = encode::get_encoded_publish_size(&pkt) as u32;
                if self.max_size.get() != 0 && content_size > self.max_size.get() {
                    return Err(EncodeError::OverMaxPacketSize);
                }

                let current_size = content_size - pkt.payload_size
                    + buf.as_ref().map(|b| b.len() as u32).unwrap_or(0);
                dst.reserve((current_size + 5) as usize);
                encode::encode_publish(&pkt, dst, content_size)?; // safe: max_size <= u32 max value

                let remaining = if let Some(buf) = buf {
                    dst.extend_from_slice(&buf);
                    pkt.payload_size - buf.len() as u32
                } else {
                    pkt.payload_size
                };
                self.encoding_payload.set(NonZeroU32::new(remaining));
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

#[cfg(test)]
mod tests {
    use super::*;
    use ntex_bytes::{ByteString, Bytes};

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
            payload_size: 260 * 1024,
        };
        let payload = Bytes::from(Vec::from("a".repeat(260 * 1024)));
        codec.encode(Encoded::Publish(pkt.clone(), Some(payload)), &mut buf).unwrap();

        let pkt2 = if let Decoded::Publish(v, _, _) = codec.decode(&mut buf).unwrap().unwrap() {
            v
        } else {
            panic!()
        };
        assert_eq!(pkt, pkt2);
    }
}
