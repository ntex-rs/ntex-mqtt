use std::{cell::Cell, cmp::min, fmt, num::NonZeroU32};

use ntex_bytes::{Buf, BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, MAX_FRAME_RESERVE, MAX_PACKET_SIZE, packet_type};
use crate::utils::decode_variable_length;

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
        self.min_chunk_size.set(size);
    }

    /// Get max inbound frame size.
    pub fn max_inbound_size(&self) -> u32 {
        self.max_in_size.get()
    }

    /// Get max outbound frame size.
    ///
    /// Returned value excludes fixed header size, see
    /// [`set_max_outbound_size`](Self::set_max_outbound_size).
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
    /// By default max size is set to `0`.
    ///
    /// Fixed header size (5 bytes) is subtracted from values greater than 5,
    /// so `max_outbound_size()` returns `size - 5`.
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

impl Codec {
    /// Returns `true` while the payload of a decoded publish is not complete.
    pub(crate) fn is_payload_pending(&self) -> bool {
        matches!(self.state.get(), DecodeState::PublishPayload(_))
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

    #[allow(clippy::too_many_lines)]
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
                                    "MaxSizeExceeded max-size: {max_in_size}, remaining: {remaining_length}"
                                );
                                return Err(DecodeError::MaxSizeExceeded {
                                    size: remaining_length,
                                    max_size: max_in_size,
                                });
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

                                let remaining_length = remaining_length as usize;
                                if src.len() < remaining_length {
                                    src.reserve(min(
                                        remaining_length - src.len(),
                                        MAX_FRAME_RESERVE,
                                    ));
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
                        if len > fixed.remaining_length {
                            return Err(DecodeError::InvalidLength);
                        }
                        self.state.set(DecodeState::PublishProperties(len, fixed));
                    } else {
                        return Ok(None);
                    }
                }
                DecodeState::PublishProperties(props_len, fixed) => {
                    if src.len() < props_len as usize {
                        return Ok(None);
                    }
                    let payload_len = fixed.remaining_length - props_len;
                    let mut buf = src.split_to(props_len as usize);
                    let publish = Publish::decode(&mut buf, fixed.first_byte, payload_len)?;

                    let len = src.len() as u32;
                    let min_chunk_size = self.min_chunk_size.get();
                    return if len >= payload_len || min_chunk_size == 0 || len >= min_chunk_size {
                        let payload = src.split_to(min(src.len(), payload_len as usize));
                        let remaining = payload_len - payload.len() as u32;

                        if remaining > 0 {
                            self.state.set(DecodeState::PublishPayload(remaining));
                        } else {
                            self.state.set(DecodeState::FrameHeader);
                            src.reserve(5); // enough to fix 1 fixed header byte + 4 bytes max variable packet length
                        }

                        Ok(Some(Decoded::Publish(
                            publish,
                            payload,
                            fixed.remaining_length,
                        )))
                    } else {
                        self.state.set(DecodeState::PublishPayload(payload_len));
                        Ok(Some(Decoded::Publish(
                            publish,
                            Bytes::new(),
                            fixed.remaining_length,
                        )))
                    };
                }
                DecodeState::PublishPayload(remaining) => {
                    let len = src.len() as u32;
                    let min_chunk_size = self.min_chunk_size.get();

                    return if (len >= remaining) || (min_chunk_size != 0 && len >= min_chunk_size) {
                        let payload = src.split_to(min(src.len(), remaining as usize));
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
                        let packet_buf = src.split_to(fixed.remaining_length as usize);
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

    fn encode(&self, mut item: Self::Item, dst: &mut BytePages) -> Result<(), EncodeError> {
        // handle [MQTT 3.1.2.11.7]
        if self.flags.get().contains(CodecFlags::NO_PROBLEM_INFO) {
            match item {
                Encoded::Packet(
                    Packet::PublishAck(ref mut pkt) | Packet::PublishReceived(ref mut pkt),
                ) => {
                    pkt.properties.clear();
                    let _ = pkt.reason_string.take();
                }
                Encoded::Packet(
                    Packet::PublishRelease(ref mut pkt) | Packet::PublishComplete(ref mut pkt),
                ) => {
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
        let max_size = if max_out_size != 0 {
            max_out_size.min(MAX_PACKET_SIZE)
        } else {
            MAX_PACKET_SIZE
        };
        match item {
            Encoded::Packet(pkt) => {
                if self.encoding_payload.get().is_some() {
                    log::trace!("Expect payload, received {pkt:?}");
                    Err(EncodeError::ExpectPayload)
                } else {
                    let content_size = pkt.encoded_size(max_size);
                    if content_size > max_size as usize {
                        Err(EncodeError::OverMaxPacketSize)
                    } else {
                        pkt.encode(dst, content_size as u32)?; // safe: max_size <= MAX_PACKET_SIZE
                        Ok(())
                    }
                }
            }
            Encoded::Publish(pkt, buf) => {
                if self.encoding_payload.get().is_some() {
                    log::trace!("Expect payload, received {pkt:?}");
                    return Err(EncodeError::ExpectPayload);
                }
                if buf
                    .as_ref()
                    .is_some_and(|buf| buf.len() > pkt.payload_size as usize)
                {
                    return Err(EncodeError::OverPublishSize);
                }
                let content_size = pkt.encoded_size(max_size);
                if content_size > max_size as usize {
                    return Err(EncodeError::OverMaxPacketSize);
                }

                pkt.encode(dst, content_size as u32)?; // safe: max_size <= MAX_PACKET_SIZE

                let remaining = if let Some(buf) = buf {
                    let remaining = pkt.payload_size - buf.len() as u32;
                    dst.append(buf);
                    remaining
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
                        dst.append(chunk);
                        self.encoding_payload
                            .set(NonZeroU32::new(remaining.get() - len));
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
        assert_eq!(
            codec.decode(&mut buf).err(),
            Some(DecodeError::MaxSizeExceeded {
                size: 9,
                max_size: 5
            })
        );
    }

    #[test]
    fn test_payload_pending() {
        use ntex_bytes::ByteString;

        let codec = Codec::new();
        codec.set_min_chunk_size(10);
        let pkt = Publish {
            topic: ByteString::from_static("/test"),
            payload_size: 100,
            ..Default::default()
        };
        let mut buf = BytePages::default();
        codec
            .encode(
                Encoded::Publish(pkt, Some(Bytes::from(vec![b'a'; 100]))),
                &mut buf,
            )
            .unwrap();
        let data = buf.freeze();
        assert!(!codec.is_payload_pending());

        let mut src = BytesMut::from(&data[..data.len() - 20]);
        let Some(Decoded::Publish(_, payload, _)) = codec.decode(&mut src).unwrap() else {
            panic!()
        };
        assert_eq!(payload.len(), 80);
        assert!(codec.is_payload_pending());

        src.extend_from_slice(&data[data.len() - 20..data.len() - 10]);
        assert_eq!(
            codec.decode(&mut src).unwrap(),
            Some(Decoded::PayloadChunk(Bytes::from(vec![b'a'; 10]), false))
        );
        assert!(codec.is_payload_pending());

        src.extend_from_slice(&data[data.len() - 10..]);
        assert_eq!(
            codec.decode(&mut src).unwrap(),
            Some(Decoded::PayloadChunk(Bytes::from(vec![b'a'; 10]), true))
        );
        assert!(!codec.is_payload_pending());
    }

    #[test]
    fn test_publish_header_over_remaining_length() {
        // topic "abc" and properties length need 6 bytes, packet declares 5
        let codec = Codec::new();
        let mut src = BytesMut::from(&b"\x30\x05\x00\x03abc\x00"[..]);
        assert_eq!(
            codec.decode(&mut src).err(),
            Some(DecodeError::InvalidLength)
        );

        // header without payload
        let codec = Codec::new();
        let mut src = BytesMut::from(&b"\x30\x06\x00\x03abc\x00"[..]);
        let Some(Decoded::Publish(pkt, payload, 6)) = codec.decode(&mut src).unwrap() else {
            panic!()
        };
        assert_eq!(pkt.topic, "abc");
        assert_eq!(pkt.payload_size, 0);
        assert!(payload.is_empty());
        assert!(!codec.is_payload_pending());
    }

    #[test]
    fn test_encode_payload_over_publish_size() {
        use ntex_bytes::ByteString;

        let codec = Codec::new();
        let pkt = Publish {
            topic: ByteString::from_static("/test"),
            payload_size: 2,
            ..Default::default()
        };
        let mut buf = BytePages::default();
        assert_eq!(
            codec.encode(
                Encoded::Publish(pkt.clone(), Some(Bytes::from_static(b"abc"))),
                &mut buf
            ),
            Err(EncodeError::OverPublishSize)
        );
        assert!(buf.freeze().is_empty());

        // no payload is expected after the failed publish
        assert_eq!(
            codec.encode(Encoded::PayloadChunk(Bytes::from_static(b"a")), &mut buf),
            Err(EncodeError::UnexpectedPayload)
        );

        codec
            .encode(
                Encoded::Publish(pkt, Some(Bytes::from_static(b"ab"))),
                &mut buf,
            )
            .unwrap();
        assert!(buf.freeze().ends_with(b"ab"));
    }

    #[test]
    fn test_encode_over_protocol_max_size() {
        use ntex_bytes::ByteString;
        use std::num::NonZeroU16;

        use crate::v5::codec::{Subscribe, SubscriptionOptions};

        let codec = Codec::new();
        let pkt = Publish {
            topic: ByteString::from_static("/test"),
            payload_size: MAX_PACKET_SIZE - 8,
            ..Default::default()
        };
        let mut buf = BytePages::default();

        // remaining length is limited by the protocol, regardless of max size
        for max_size in [0, u32::MAX] {
            codec.set_max_outbound_size(max_size);
            for payload_size in [MAX_PACKET_SIZE - 7, u32::MAX] {
                let pkt = Publish {
                    payload_size,
                    ..pkt.clone()
                };
                assert_eq!(
                    codec.encode(Encoded::Publish(pkt, None), &mut buf),
                    Err(EncodeError::OverMaxPacketSize)
                );
                assert!(buf.freeze().is_empty());
                assert_eq!(
                    codec.encode(Encoded::PayloadChunk(Bytes::from_static(b"a")), &mut buf),
                    Err(EncodeError::UnexpectedPayload)
                );
            }
        }

        codec.encode(Encoded::Publish(pkt, None), &mut buf).unwrap();
        assert_eq!(&buf.freeze()[..], b"\x30\xff\xff\xff\x7f\x00\x05/test\x00");

        // 4097 * (2 + 65535 + 1) bytes, the filter is shared
        let codec = Codec::new();
        codec.set_max_outbound_size(u32::MAX);
        let filter = ByteString::from("a".repeat(65_535));
        let pkt = Packet::Subscribe(Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![(filter, SubscriptionOptions::default()); 4097],
        });
        assert_eq!(
            codec.encode(Encoded::Packet(pkt), &mut buf),
            Err(EncodeError::OverMaxPacketSize)
        );
        assert!(buf.freeze().is_empty());
    }

    #[test]
    fn test_encode_expect_payload() {
        use ntex_bytes::ByteString;

        let codec = Codec::new();
        let pkt = Publish {
            topic: ByteString::from_static("/test"),
            payload_size: 4,
            ..Default::default()
        };
        let mut buf = BytePages::default();
        codec
            .encode(
                Encoded::Publish(pkt.clone(), Some(Bytes::from_static(b"ab"))),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf.freeze()[..], b"\x30\x0c\x00\x05/test\x00ab");

        // nothing is written until the payload is complete
        assert_eq!(
            codec.encode(Encoded::Packet(Packet::PingRequest), &mut buf),
            Err(EncodeError::ExpectPayload)
        );
        assert_eq!(
            codec.encode(Encoded::Publish(pkt, None), &mut buf),
            Err(EncodeError::ExpectPayload)
        );
        assert!(buf.freeze().is_empty());

        codec
            .encode(Encoded::PayloadChunk(Bytes::from_static(b"cd")), &mut buf)
            .unwrap();
        codec
            .encode(Encoded::Packet(Packet::PingRequest), &mut buf)
            .unwrap();
        assert_eq!(&buf.freeze()[..], b"cd\xc0\x00");
    }

    #[test]
    fn test_frame_reserve() {
        let codec = Codec::new();

        // header of max size subscribe packet
        let mut src = BytesMut::from(&b"\x82\xff\xff\xff\x7f"[..]);
        assert_eq!(codec.decode(&mut src), Ok(None));
        assert!(src.is_empty());
        assert!(src.capacity() >= MAX_FRAME_RESERVE);
        assert!(src.capacity() < MAX_FRAME_RESERVE * 4);

        // small frames reserve the rest of the frame
        let codec = Codec::new();
        let mut src = BytesMut::from(&b"\x82\xe8\x07\x00\x01"[..]);
        assert_eq!(codec.decode(&mut src), Ok(None));
        assert_eq!(src.len(), 2);
        assert!(src.capacity() >= 1000);
        assert!(src.capacity() < MAX_FRAME_RESERVE);
    }
}
