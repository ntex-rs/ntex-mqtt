use std::{cell::Cell, cmp::min, fmt, num::NonZeroU32};

use ntex_bytes::{Buf, BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, MAX_FRAME_RESERVE, MAX_PACKET_SIZE, packet_type};
use crate::utils::decode_variable_length;

use super::{Decoded, Encoded};
use super::{Packet, decode::decode_packet, encode, encode::EncodeLtd, packet::Publish};

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

    /// Returns `true` while the payload of an encoded publish is not complete.
    pub(crate) fn is_encoding_payload(&self) -> bool {
        self.encoding_payload.get().is_some()
    }

    fn max_out_size(&self) -> u32 {
        match self.max_out_size.get() {
            0 => MAX_PACKET_SIZE,
            size => size.min(MAX_PACKET_SIZE),
        }
    }

    /// Encodes a packet, an incomplete publish payload is not checked.
    pub(crate) fn encode_packet(
        &self,
        mut pkt: Packet,
        dst: &mut BytePages,
    ) -> Result<(), EncodeError> {
        // handle [MQTT 3.1.2.11.7]
        if self.flags.get().contains(CodecFlags::NO_PROBLEM_INFO) {
            match pkt {
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

        encode::validate(&pkt)?;
        let max_size = self.max_out_size();
        let content_size = pkt.encoded_size(max_size);
        if content_size > max_size as usize {
            Err(EncodeError::OverMaxPacketSize)
        } else {
            pkt.encode(dst, content_size as u32) // safe: max_size <= MAX_PACKET_SIZE
        }
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

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), EncodeError> {
        match item {
            Encoded::Packet(pkt) => {
                if self.encoding_payload.get().is_some() {
                    log::trace!("Expect payload, received {pkt:?}");
                    Err(EncodeError::ExpectPayload)
                } else {
                    self.encode_packet(pkt, dst)
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
                encode::validate_publish(&pkt)?;
                let max_size = self.max_out_size();
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
    use ntex_bytes::ByteString;
    use std::num::NonZeroU16;

    use crate::v5::codec::{
        Auth, AuthReasonCode, Connect, ConnectAck, Disconnect, LastWill, PublishAck, PublishAck2,
        QoS, Subscribe, SubscribeAck, SubscribeAckReason, SubscriptionOptions, Unsubscribe,
        UnsubscribeAck, UnsubscribeAckReason,
    };

    fn assert_rejected(codec: &Codec, item: Encoded) {
        let mut buf = BytePages::default();
        assert_eq!(
            codec.encode(item, &mut buf),
            Err(EncodeError::MalformedPacket)
        );
        assert!(buf.freeze().is_empty(), "nothing is written");
    }

    fn assert_encoded(codec: &Codec, item: Encoded) {
        let mut buf = BytePages::default();
        codec.encode(item, &mut buf).unwrap();
        assert!(!buf.freeze().is_empty());
    }

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
    fn test_encode_sub_id_range() {
        use std::num::NonZeroU32;

        let codec = Codec::new();
        let max = NonZeroU32::new(268_435_455).unwrap();
        let over = NonZeroU32::new(268_435_456).unwrap();
        let encode = |item| {
            let mut buf = BytePages::default();
            codec.encode(item, &mut buf).unwrap();
            buf.freeze()
        };

        // [MQTT 5.0, 3.8.2.1.2] 1 to 268,435,455
        let subscribe = |id| {
            Encoded::Packet(Packet::Subscribe(Subscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                id: Some(id),
                user_properties: Vec::new(),
                topic_filters: vec![(ByteString::from_static("a"), SubscriptionOptions::default())],
            }))
        };
        assert_rejected(&codec, subscribe(over));
        assert_rejected(&codec, subscribe(NonZeroU32::MAX));
        assert_eq!(
            &encode(subscribe(max))[..],
            b"\x82\x0c\x00\x01\x05\x0b\xff\xff\xff\x7f\x00\x01a\x00"
        );

        // [MQTT 5.0, 3.3.2.3.8] 1 to 268,435,455
        let publish = |id| {
            let mut pkt = Publish {
                topic: ByteString::from_static("a"),
                ..Default::default()
            };
            pkt.properties.subscription_ids = vec![NonZeroU32::MIN, id];
            Encoded::Publish(pkt, None)
        };
        assert_rejected(&codec, publish(over));
        assert_eq!(
            &encode(publish(max))[..],
            b"\x30\x0b\x00\x01a\x07\x0b\x01\x0b\xff\xff\xff\x7f"
        );
    }

    #[test]
    fn test_no_problem_info() {
        let ack = || {
            Encoded::Packet(Packet::PublishAck(PublishAck {
                packet_id: NonZeroU16::new(1).unwrap(),
                reason_code: crate::v5::codec::PublishAckReason::Success,
                properties: vec![("k".into(), "v".into())],
                reason_string: Some("reason".into()),
            }))
        };
        let encode = |codec: &Codec, item| {
            let mut buf = BytePages::default();
            codec.encode(item, &mut buf).unwrap();
            buf.freeze()
        };

        let codec = Codec::new();
        let connect = Connect {
            client_id: "user".into(),
            request_problem_info: false,
            ..Default::default()
        };
        let mut buf =
            BytesMut::from(&encode(&Codec::new(), Packet::Connect(Box::new(connect)).into())[..]);
        codec.decode(&mut buf).unwrap().unwrap();

        // [MQTT-3.1.2-29] no reason string or user properties
        assert_eq!(&encode(&codec, ack())[..], b"\x40\x04\x00\x01\x00\x00");
        assert!(encode(&Codec::new(), ack()).len() > 6);
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

    #[test]
    fn test_encode_publish_sender_rules() {
        let codec = Codec::new();
        let id = NonZeroU16::new(1);
        let publish = Publish {
            topic: ByteString::from_static("a/b"),
            ..Default::default()
        };
        let invalid = [
            Publish {
                topic: ByteString::new(),
                ..publish.clone()
            },
            Publish {
                topic: ByteString::from_static("a/+"),
                ..publish.clone()
            },
            Publish {
                topic: ByteString::from_static("a/#"),
                ..publish.clone()
            },
            Publish {
                dup: true,
                ..publish.clone()
            },
            Publish {
                packet_id: id,
                ..publish.clone()
            },
        ];
        for pkt in invalid {
            assert_rejected(&codec, Encoded::Publish(pkt, None));
        }

        assert_encoded(&codec, Encoded::Publish(publish.clone(), None));
        let pkt = Publish {
            dup: true,
            qos: QoS::AtLeastOnce,
            packet_id: id,
            ..publish.clone()
        };
        assert_encoded(&codec, Encoded::Publish(pkt, None));

        // empty topic is allowed with a topic alias
        let mut pkt = Publish {
            topic: ByteString::new(),
            ..publish
        };
        pkt.properties.topic_alias = id;
        assert_encoded(&codec, Encoded::Publish(pkt.clone(), None));
        pkt.topic = ByteString::from_static("a/+");
        assert_rejected(&codec, Encoded::Publish(pkt, None));
    }

    #[test]
    fn test_encode_packet_sender_rules() {
        let codec = Codec::new();
        let packet_id = NonZeroU16::new(1).unwrap();
        let connect = Connect {
            client_id: ByteString::from_static("id"),
            ..Connect::default()
        };
        let will = |topic| LastWill {
            qos: QoS::AtMostOnce,
            retain: false,
            topic: ByteString::from_static(topic),
            message: Bytes::new(),
            will_delay_interval_sec: None,
            correlation_data: None,
            message_expiry_interval: None,
            content_type: None,
            user_properties: Vec::new(),
            is_utf8_payload: None,
            response_topic: None,
        };
        let opts = SubscriptionOptions::default();
        let subscribe = |topic_filters| Subscribe {
            packet_id,
            id: None,
            user_properties: Vec::new(),
            topic_filters,
        };
        let unsubscribe = |topic_filters| Unsubscribe {
            packet_id,
            user_properties: Vec::new(),
            topic_filters,
        };
        let invalid = [
            Packet::Connect(Box::new(Connect {
                last_will: Some(will("")),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                last_will: Some(will("w/#")),
                ..connect.clone()
            })),
            Packet::Subscribe(subscribe(vec![])),
            Packet::Subscribe(subscribe(vec![
                (ByteString::from_static("a"), opts),
                (ByteString::new(), opts),
            ])),
            Packet::Unsubscribe(unsubscribe(vec![])),
            Packet::Unsubscribe(unsubscribe(vec![
                ByteString::from_static("a"),
                ByteString::new(),
            ])),
        ];
        for pkt in invalid {
            assert_rejected(&codec, Encoded::Packet(pkt));
        }

        let valid = [
            // password without username and empty client id are allowed in v5
            Packet::Connect(Box::new(Connect {
                client_id: ByteString::new(),
                password: Some(Bytes::from_static(b"pwd")),
                last_will: Some(will("w/t")),
                ..connect
            })),
            Packet::Subscribe(subscribe(vec![
                (ByteString::from_static("a/+"), opts),
                (ByteString::from_static("#"), opts),
            ])),
            Packet::Unsubscribe(unsubscribe(vec![ByteString::from_static("a/#")])),
        ];
        for pkt in valid {
            assert_encoded(&codec, Encoded::Packet(pkt));
        }
    }

    /// Strings that must be rejected: one with the null character U+0000,
    /// [MQTT-1.5.4-2], and one over 65,535 bytes (MQTT 5.0, 1.5.4)
    fn invalid_strs() -> [ByteString; 2] {
        [
            ByteString::from_static("a\0b"),
            ByteString::from("a".repeat(65_536)),
        ]
    }

    fn props(key: ByteString) -> Vec<(ByteString, ByteString)> {
        vec![(key, ByteString::from_static("v"))]
    }

    /// `pkt` is encoded, each setter puts an invalid string into one string field
    fn assert_str_rejected<T: Clone>(
        pkt: &T,
        encoded: fn(T) -> Encoded,
        setters: &[fn(&mut T, ByteString)],
    ) {
        let codec = Codec::new();
        assert_encoded(&codec, encoded(pkt.clone()));
        for set in setters {
            for s in invalid_strs() {
                let mut pkt = pkt.clone();
                set(&mut pkt, s);
                assert_rejected(&codec, encoded(pkt));
            }
            let mut pkt = pkt.clone();
            set(&mut pkt, ByteString::from("a".repeat(65_535)));
            assert_encoded(&codec, encoded(pkt));
        }
    }

    fn auth_with_method() -> Auth {
        Auth {
            auth_method: Some(ByteString::from_static("m")),
            ..Auth::default()
        }
    }

    fn will_connect() -> Connect {
        Connect {
            client_id: ByteString::from_static("id"),
            last_will: Some(LastWill {
                qos: QoS::AtMostOnce,
                retain: false,
                topic: ByteString::from_static("w"),
                message: Bytes::new(),
                will_delay_interval_sec: None,
                correlation_data: None,
                message_expiry_interval: None,
                content_type: None,
                user_properties: Vec::new(),
                is_utf8_payload: None,
                response_topic: None,
            }),
            ..Connect::default()
        }
    }

    #[test]
    fn test_encode_invalid_str_connect() {
        let connect = will_connect();
        assert_str_rejected(
            &connect,
            |p| Encoded::Packet(Packet::Connect(Box::new(p))),
            &[
                |p, s| p.client_id = s,
                |p, s| p.username = Some(s),
                |p, s| p.auth_method = Some(s),
                |p, s| p.user_properties = props(s),
                |p, s| p.user_properties = vec![(ByteString::from_static("k"), s)],
                |p, s| p.last_will.as_mut().unwrap().topic = s,
                |p, s| p.last_will.as_mut().unwrap().content_type = Some(s),
                |p, s| p.last_will.as_mut().unwrap().response_topic = Some(s),
                |p, s| p.last_will.as_mut().unwrap().user_properties = props(s),
            ],
        );
        assert_str_rejected(
            &ConnectAck::default(),
            |p| Encoded::Packet(Packet::ConnectAck(Box::new(p))),
            &[
                |p, s| p.assigned_client_id = Some(s),
                |p, s| p.response_info = Some(s),
                |p, s| p.server_reference = Some(s),
                |p, s| p.auth_method = Some(s),
                |p, s| p.reason_string = Some(s),
                |p, s| p.user_properties = props(s),
            ],
        );
        assert_str_rejected(
            &Publish {
                topic: ByteString::from_static("a/b"),
                ..Publish::default()
            },
            |p| Encoded::Publish(p, None),
            &[
                |p, s| p.topic = s,
                |p, s| p.properties.content_type = Some(s),
                |p, s| p.properties.response_topic = Some(s),
                |p, s| p.properties.user_properties = props(s),
            ],
        );
    }

    #[test]
    fn test_encode_invalid_str_packets() {
        let packet_id = NonZeroU16::new(1).unwrap();
        assert_str_rejected(
            &PublishAck::default(),
            |p| Encoded::Packet(Packet::PublishReceived(p)),
            &[
                |p, s| p.reason_string = Some(s),
                |p, s| p.properties = props(s),
            ],
        );
        assert_str_rejected(
            &PublishAck2::default(),
            |p| Encoded::Packet(Packet::PublishRelease(p)),
            &[
                |p, s| p.reason_string = Some(s),
                |p, s| p.properties = props(s),
            ],
        );
        assert_str_rejected(
            &Subscribe {
                packet_id,
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![(ByteString::from_static("a"), SubscriptionOptions::default())],
            },
            |p| Encoded::Packet(Packet::Subscribe(p)),
            &[
                |p, s| p.topic_filters[0].0 = s,
                |p, s| p.user_properties = props(s),
            ],
        );
        assert_str_rejected(
            &SubscribeAck {
                packet_id,
                properties: Vec::new(),
                reason_string: None,
                status: vec![SubscribeAckReason::GrantedQos0],
            },
            |p| Encoded::Packet(Packet::SubscribeAck(p)),
            &[
                |p, s| p.reason_string = Some(s),
                |p, s| p.properties = props(s),
            ],
        );
        assert_str_rejected(
            &Unsubscribe {
                packet_id,
                user_properties: Vec::new(),
                topic_filters: vec![ByteString::from_static("a")],
            },
            |p| Encoded::Packet(Packet::Unsubscribe(p)),
            &[
                |p, s| p.topic_filters[0] = s,
                |p, s| p.user_properties = props(s),
            ],
        );
        assert_str_rejected(
            &UnsubscribeAck {
                packet_id,
                properties: Vec::new(),
                reason_string: None,
                status: vec![UnsubscribeAckReason::Success],
            },
            |p| Encoded::Packet(Packet::UnsubscribeAck(p)),
            &[
                |p, s| p.reason_string = Some(s),
                |p, s| p.properties = props(s),
            ],
        );
        assert_str_rejected(
            &Disconnect::default(),
            |p| Encoded::Packet(Packet::Disconnect(p)),
            &[
                |p, s| p.server_reference = Some(s),
                |p, s| p.reason_string = Some(s),
                |p, s| p.user_properties = props(s),
            ],
        );
        assert_str_rejected(
            &auth_with_method(),
            |p| Encoded::Packet(Packet::Auth(p)),
            &[
                |p, s| p.auth_method = Some(s),
                |p, s| p.reason_string = Some(s),
                |p, s| p.user_properties = props(s),
            ],
        );
    }

    /// `pkt` is encoded, each setter puts binary data into one binary field,
    /// it is limited to 65,535 bytes (MQTT 5.0, 1.5.6)
    fn assert_bin_rejected<T: Clone>(
        pkt: &T,
        encoded: fn(T) -> Encoded,
        setters: &[fn(&mut T, Bytes)],
    ) {
        let codec = Codec::new();
        for set in setters {
            let mut pkt = pkt.clone();
            set(&mut pkt, Bytes::from(vec![b'a'; 65_535]));
            assert_encoded(&codec, encoded(pkt.clone()));
            set(&mut pkt, Bytes::from(vec![b'a'; 65_536]));
            assert_rejected(&codec, encoded(pkt));
        }
    }

    #[test]
    fn test_encode_bin_too_long() {
        assert_bin_rejected(
            &will_connect(),
            |p| Encoded::Packet(Packet::Connect(Box::new(p))),
            &[
                |p, b| p.password = Some(b),
                |p, b| {
                    p.auth_method = Some(ByteString::from_static("m"));
                    p.auth_data = Some(b);
                },
                |p, b| p.last_will.as_mut().unwrap().message = b,
                |p, b| p.last_will.as_mut().unwrap().correlation_data = Some(b),
            ],
        );
        assert_bin_rejected(
            &ConnectAck::default(),
            |p| Encoded::Packet(Packet::ConnectAck(Box::new(p))),
            &[|p, b| p.auth_data = Some(b)],
        );
        assert_bin_rejected(
            &auth_with_method(),
            |p| Encoded::Packet(Packet::Auth(p)),
            &[|p, b| p.auth_data = Some(b)],
        );
        assert_bin_rejected(
            &Publish {
                topic: ByteString::from_static("a/b"),
                ..Publish::default()
            },
            |p| Encoded::Publish(p, None),
            &[|p, b| p.properties.correlation_data = Some(b)],
        );
    }

    #[test]
    fn test_encode_response_topic_wildcards() {
        let codec = Codec::new();
        let publish = |topic: &'static str| {
            let mut p = Publish {
                topic: ByteString::from_static("a/b"),
                ..Publish::default()
            };
            p.properties.response_topic = Some(ByteString::from_static(topic));
            Encoded::Publish(p, None)
        };
        let connect = |topic: &'static str| {
            let mut p = will_connect();
            p.last_will.as_mut().unwrap().response_topic = Some(ByteString::from_static(topic));
            Encoded::Packet(Packet::Connect(Box::new(p)))
        };
        assert_encoded(&codec, publish("a/b"));
        assert_encoded(&codec, connect("a/b"));
        for topic in ["a/+", "+", "a/#", "#", ""] {
            assert_rejected(&codec, publish(topic));
            assert_rejected(&codec, connect(topic));
        }
    }

    #[test]
    fn test_encode_shared_subscription_no_local() {
        let codec = Codec::new();
        let subscribe = |filter: &'static str, no_local| {
            Encoded::Packet(Packet::Subscribe(Subscribe {
                packet_id: NonZeroU16::new(1).unwrap(),
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![
                    (ByteString::from_static("a"), SubscriptionOptions::default()),
                    (
                        ByteString::from_static(filter),
                        SubscriptionOptions {
                            no_local,
                            ..SubscriptionOptions::default()
                        },
                    ),
                ],
            }))
        };
        for filter in ["$share/g/a", "$share/g/#"] {
            assert_encoded(&codec, subscribe(filter, false));
            assert_rejected(&codec, subscribe(filter, true));
        }
        for filter in ["a", "$sharex/g/a", "$share", "a/$share/g"] {
            assert_encoded(&codec, subscribe(filter, true));
        }
    }

    #[test]
    fn test_connack_max_packet_size_zero() {
        let codec = Codec::new();
        let ack = |max_packet_size| {
            Encoded::Packet(Packet::ConnectAck(Box::new(ConnectAck {
                max_packet_size,
                ..ConnectAck::default()
            })))
        };
        assert_encoded(&codec, ack(None));
        assert_encoded(&codec, ack(Some(1)));
        assert_rejected(&codec, ack(Some(0)));

        let mut src = BytesMut::from(&b"\x20\x08\x00\x00\x05\x27\x00\x00\x00\x01"[..]);
        let Some(Decoded::Packet(Packet::ConnectAck(ack), _)) = codec.decode(&mut src).unwrap()
        else {
            panic!()
        };
        assert_eq!(ack.max_packet_size, Some(1));

        let mut src = BytesMut::from(&b"\x20\x08\x00\x00\x05\x27\x00\x00\x00\x00"[..]);
        assert_eq!(
            codec.decode(&mut src).err(),
            Some(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn test_connack_max_qos() {
        let codec = Codec::new();
        for (qos, expected) in [(0, QoS::AtMostOnce), (1, QoS::AtLeastOnce)] {
            let mut src = BytesMut::from(&[0x20, 0x05, 0x00, 0x00, 0x02, 0x24, qos][..]);
            let Some(Decoded::Packet(Packet::ConnectAck(ack), _)) = codec.decode(&mut src).unwrap()
            else {
                panic!()
            };
            assert_eq!(ack.max_qos, expected);

            // encoded value round-trips
            let mut buf = BytePages::default();
            codec
                .encode(Encoded::Packet(Packet::ConnectAck(ack)), &mut buf)
                .unwrap();
            assert_eq!(
                &buf.freeze()[..],
                &[0x20, 0x05, 0x00, 0x00, 0x02, 0x24, qos]
            );
        }

        // absent property means QoS 2
        let mut src = BytesMut::from(&b"\x20\x03\x00\x00\x00"[..]);
        let Some(Decoded::Packet(Packet::ConnectAck(ack), _)) = codec.decode(&mut src).unwrap()
        else {
            panic!()
        };
        assert_eq!(ack.max_qos, QoS::ExactlyOnce);

        for qos in [2, 3] {
            let mut src = BytesMut::from(&[0x20, 0x05, 0x00, 0x00, 0x02, 0x24, qos][..]);
            assert_eq!(
                Codec::new().decode(&mut src).err(),
                Some(DecodeError::MalformedPacket)
            );
        }
    }

    #[test]
    fn test_connect_auth_data_requires_method() {
        let codec = Codec::new();
        let connect = |auth_method: Option<&'static str>| {
            Encoded::Packet(Packet::Connect(Box::new(Connect {
                client_id: ByteString::from_static("id"),
                auth_method: auth_method.map(ByteString::from_static),
                auth_data: Some(Bytes::from_static(b"x")),
                ..Connect::default()
            })))
        };
        assert_encoded(&codec, connect(Some("m")));
        assert_rejected(&codec, connect(None));

        let pkt = Connect::decode(&mut Bytes::from_static(
            b"\x00\x04MQTT\x05\x02\x00\x3C\x08\x15\x00\x01m\x16\x00\x01x\x00\x02id",
        ))
        .unwrap();
        assert_eq!(pkt.auth_method, Some(ByteString::from_static("m")));
        assert_eq!(pkt.auth_data, Some(Bytes::from_static(b"x")));
        assert_eq!(
            Connect::decode(&mut Bytes::from_static(
                b"\x00\x04MQTT\x05\x02\x00\x3C\x04\x16\x00\x01x\x00\x02id"
            )),
            Err(DecodeError::MalformedPacket)
        );
    }

    #[test]
    fn test_auth_method_required() {
        let encode = |pkt: Auth| {
            let mut buf = BytePages::default();
            Codec::new()
                .encode(Encoded::Packet(Packet::Auth(pkt)), &mut buf)
                .map(|()| buf.freeze())
        };
        let decode = |data: &[u8]| match Codec::new().decode(&mut BytesMut::from(data)) {
            Ok(Some(Decoded::Packet(Packet::Auth(pkt), _))) => Ok(pkt),
            Ok(_) => panic!(),
            Err(e) => Err(e),
        };

        // Success without properties uses Remaining Length of 0
        assert_eq!(&encode(Auth::default()).unwrap()[..], b"\xF0\x00");
        assert_eq!(decode(b"\xF0\x00"), Ok(Auth::default()));

        let pkt = Auth {
            reason_code: AuthReasonCode::ContinueAuth,
            auth_data: Some(Bytes::from_static(b"x")),
            ..auth_with_method()
        };
        let data = encode(pkt.clone()).unwrap();
        assert_eq!(&data[..], b"\xF0\x0A\x18\x08\x15\x00\x01m\x16\x00\x01x");
        assert_eq!(decode(&data), Ok(pkt));
        let data = encode(auth_with_method()).unwrap();
        assert_eq!(&data[..], b"\xF0\x06\x00\x04\x15\x00\x01m");
        assert_eq!(decode(&data), Ok(auth_with_method()));

        for pkt in [
            Auth {
                reason_code: AuthReasonCode::ContinueAuth,
                ..Auth::default()
            },
            Auth {
                reason_code: AuthReasonCode::ReAuth,
                ..Auth::default()
            },
            Auth {
                auth_data: Some(Bytes::from_static(b"x")),
                ..Auth::default()
            },
            Auth {
                reason_string: Some(ByteString::from_static("r")),
                ..Auth::default()
            },
            Auth {
                user_properties: vec![("a".into(), "b".into())],
                ..Auth::default()
            },
        ] {
            assert_eq!(encode(pkt), Err(EncodeError::MalformedPacket));
        }

        for data in [
            &b"\xF0\x01\x00"[..],
            b"\xF0\x01\x18",
            b"\xF0\x02\x00\x00",
            b"\xF0\x02\x18\x00",
            b"\xF0\x06\x18\x04\x16\x00\x01x",
        ] {
            assert_eq!(decode(data), Err(DecodeError::MalformedPacket));
        }
    }
}
