use std::{cell::Cell, cmp::min, num::NonZeroU32};

use ntex_bytes::{Buf, BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{FixedHeader, MAX_FRAME_RESERVE, MAX_PACKET_SIZE, packet_type};
use crate::utils::decode_variable_length;

use super::{Decoded, Encoded, decode, encode};

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
    /// By default max size is set to `0`.
    ///
    /// Inbound packets over the limit fail with `DecodeError::MaxSizeExceeded`,
    /// outgoing publish packets over the limit fail with `EncodeError::OverMaxPacketSize`.
    /// Outgoing packets over the protocol limit (268,435,455 bytes) always fail
    /// with `EncodeError::OverMaxPacketSize`.
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
        self.min_chunk_size.set(size);
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
    type Item = Decoded;
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
                            let max_size = self.max_size.get();
                            if max_size != 0 && max_size < remaining_length {
                                return Err(DecodeError::MaxSizeExceeded {
                                    size: remaining_length,
                                    max_size,
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
                    if let Some(hdr_len) = decode::publish_size(src, fixed.first_byte)? {
                        let Some(payload_len) = fixed.remaining_length.checked_sub(hdr_len) else {
                            return Err(DecodeError::InvalidLength);
                        };
                        if src.len() < hdr_len as usize {
                            return Ok(None);
                        }
                        let mut buf = src.split_to(hdr_len as usize);
                        let publish =
                            decode::decode_publish_packet(&mut buf, fixed.first_byte, payload_len)?;

                        let len = src.len() as u32;
                        let min_chunk_size = self.min_chunk_size.get();
                        if len >= payload_len || min_chunk_size == 0 || len >= min_chunk_size {
                            let payload = src.split_to(min(src.len(), payload_len as usize));
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
                        }
                        self.state.set(DecodeState::PublishPayload(payload_len));
                        return Ok(Some(Decoded::Publish(
                            publish,
                            Bytes::new(),
                            fixed.remaining_length,
                        )));
                    }
                    return Ok(None);
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
                    if src.len() < fixed.remaining_length as usize {
                        return Ok(None);
                    }
                    let packet_buf = src.split_to(fixed.remaining_length as usize);
                    let packet = decode::decode_packet(packet_buf, fixed.first_byte)?;
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

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), EncodeError> {
        if self.encoding_payload.get().is_some() && !matches!(item, Encoded::PayloadChunk(_)) {
            return Err(EncodeError::ExpectPayload);
        }
        match item {
            Encoded::Packet(pkt) => {
                encode::validate(&pkt)?;
                let content_size = encode::get_encoded_size(&pkt);
                if content_size > MAX_PACKET_SIZE as usize {
                    return Err(EncodeError::OverMaxPacketSize);
                }
                encode::encode(&pkt, dst, content_size as u32)?; // safe: content_size <= MAX_PACKET_SIZE
                Ok(())
            }
            Encoded::Publish(pkt, buf) => {
                encode::validate_publish(&pkt)?;
                if buf
                    .as_ref()
                    .is_some_and(|buf| buf.len() > pkt.payload_size as usize)
                {
                    return Err(EncodeError::OverPublishSize);
                }

                let max_size = match self.max_size.get() {
                    0 => MAX_PACKET_SIZE,
                    size => size.min(MAX_PACKET_SIZE),
                };
                let content_size = encode::get_encoded_publish_size(&pkt);
                if content_size > max_size as usize {
                    return Err(EncodeError::OverMaxPacketSize);
                }

                encode::encode_publish(&pkt, dst, content_size as u32)?; // safe: content_size <= MAX_PACKET_SIZE

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

#[cfg(test)]
mod tests {
    use super::*;
    use ntex_bytes::{ByteString, Bytes};
    use std::num::NonZeroU16;

    use crate::v3::codec::{Connect, LastWill, Packet, Publish, QoS};

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
        codec.set_max_size(5);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(
            codec.decode(&mut buf),
            Err(DecodeError::MaxSizeExceeded {
                size: 9,
                max_size: 5
            })
        );
    }

    #[test]
    fn test_packet() {
        let codec = Codec::new();
        let mut buf = BytePages::default();

        let pkt = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("/test"),
            packet_id: None,
            payload_size: 260 * 1024,
        };
        let payload = Bytes::from(Vec::from("a".repeat(260 * 1024)));
        codec
            .encode(Encoded::Publish(pkt.clone(), Some(payload)), &mut buf)
            .unwrap();

        let Decoded::Publish(pkt2, _, _) = codec
            .decode(&mut BytesMut::from(buf.freeze()))
            .unwrap()
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(pkt, pkt2);
    }

    #[test]
    fn test_payload_pending() {
        let codec = Codec::new();
        codec.set_min_chunk_size(10);
        let pkt = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("/test"),
            packet_id: None,
            payload_size: 100,
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
        // topic "abc" needs 5 bytes, packet declares 4
        let codec = Codec::new();
        let mut src = BytesMut::from(&b"\x30\x04\x00\x03abc"[..]);
        assert_eq!(codec.decode(&mut src), Err(DecodeError::InvalidLength));

        // qos 1 adds the packet id
        let codec = Codec::new();
        let mut src = BytesMut::from(&b"\x32\x06\x00\x03abc\x00\x01"[..]);
        assert_eq!(codec.decode(&mut src), Err(DecodeError::InvalidLength));

        // header without payload
        let codec = Codec::new();
        let mut src = BytesMut::from(&b"\x30\x05\x00\x03abc"[..]);
        let Some(Decoded::Publish(pkt, payload, 5)) = codec.decode(&mut src).unwrap() else {
            panic!()
        };
        assert_eq!(pkt.topic, "abc");
        assert_eq!(pkt.payload_size, 0);
        assert!(payload.is_empty());
        assert!(!codec.is_payload_pending());
    }

    #[test]
    fn test_encode_payload_over_publish_size() {
        let codec = Codec::new();
        let pkt = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("/test"),
            packet_id: None,
            payload_size: 2,
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
        let codec = Codec::new();
        let pkt = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("/test"),
            packet_id: None,
            payload_size: MAX_PACKET_SIZE - 7,
        };
        let mut buf = BytePages::default();

        // remaining length is limited by the protocol, regardless of max size
        for max_size in [0, u32::MAX] {
            codec.set_max_size(max_size);
            for payload_size in [MAX_PACKET_SIZE - 6, u32::MAX] {
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
        assert_eq!(&buf.freeze()[..], b"\x30\xff\xff\xff\x7f\x00\x05/test");

        // 4097 * (2 + 65535 + 1) bytes, the filter is shared
        let codec = Codec::new();
        codec.set_max_size(u32::MAX);
        let filter = ByteString::from("a".repeat(65_535));
        let pkt = Packet::Subscribe {
            packet_id: NonZeroU16::new(1).unwrap(),
            topic_filters: vec![(filter, QoS::AtMostOnce); 4097],
        };
        assert_eq!(
            codec.encode(Encoded::Packet(pkt), &mut buf),
            Err(EncodeError::OverMaxPacketSize)
        );
        assert!(buf.freeze().is_empty());
    }

    #[test]
    fn test_encode_expect_payload() {
        let codec = Codec::new();
        let pkt = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("/test"),
            packet_id: None,
            payload_size: 4,
        };
        let mut buf = BytePages::default();
        codec
            .encode(
                Encoded::Publish(pkt.clone(), Some(Bytes::from_static(b"ab"))),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf.freeze()[..], b"\x30\x0b\x00\x05/testab");

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

    #[test]
    fn test_encode_publish_sender_rules() {
        let codec = Codec::new();
        let id = NonZeroU16::new(1);
        let publish = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: ByteString::from_static("a/b"),
            packet_id: None,
            payload_size: 0,
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
            ..publish
        };
        assert_encoded(&codec, Encoded::Publish(pkt, None));
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
        };
        let qos = QoS::AtMostOnce;
        let invalid = [
            Packet::Connect(Box::new(Connect {
                password: Some(Bytes::from_static(b"pwd")),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                client_id: ByteString::new(),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                last_will: Some(will("")),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                last_will: Some(will("w/+")),
                ..connect.clone()
            })),
            Packet::Subscribe {
                packet_id,
                topic_filters: vec![],
            },
            Packet::Subscribe {
                packet_id,
                topic_filters: vec![
                    (ByteString::from_static("a"), qos),
                    (ByteString::new(), qos),
                ],
            },
            Packet::Unsubscribe {
                packet_id,
                topic_filters: vec![],
            },
            Packet::Unsubscribe {
                packet_id,
                topic_filters: vec![ByteString::from_static("a"), ByteString::new()],
            },
        ];
        for pkt in invalid {
            assert_rejected(&codec, Encoded::Packet(pkt));
        }

        let valid = [
            Packet::Connect(Box::new(Connect {
                username: Some(ByteString::from_static("user")),
                password: Some(Bytes::from_static(b"pwd")),
                last_will: Some(will("w/t")),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                client_id: ByteString::new(),
                clean_session: true,
                ..connect
            })),
            Packet::Subscribe {
                packet_id,
                topic_filters: vec![
                    (ByteString::from_static("a/+"), qos),
                    (ByteString::from_static("#"), qos),
                ],
            },
            Packet::Unsubscribe {
                packet_id,
                topic_filters: vec![ByteString::from_static("a/#")],
            },
        ];
        for pkt in valid {
            assert_encoded(&codec, Encoded::Packet(pkt));
        }
    }

    #[test]
    fn test_encode_null_char() {
        let codec = Codec::new();
        let nul = || ByteString::from_static("a\0b");
        let packet_id = NonZeroU16::new(1).unwrap();
        let connect = Connect {
            client_id: ByteString::from_static("id"),
            last_will: Some(LastWill {
                qos: QoS::AtMostOnce,
                retain: false,
                topic: ByteString::from_static("w"),
                message: Bytes::new(),
            }),
            ..Connect::default()
        };
        assert_encoded(
            &codec,
            Encoded::Packet(Packet::Connect(Box::new(connect.clone()))),
        );

        let invalid = [
            Packet::Connect(Box::new(Connect {
                client_id: nul(),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                username: Some(nul()),
                ..connect.clone()
            })),
            Packet::Connect(Box::new(Connect {
                last_will: Some(LastWill {
                    topic: nul(),
                    ..connect.last_will.clone().unwrap()
                }),
                ..connect
            })),
            Packet::Subscribe {
                packet_id,
                topic_filters: vec![(nul(), QoS::AtMostOnce)],
            },
            Packet::Unsubscribe {
                packet_id,
                topic_filters: vec![nul()],
            },
        ];
        for pkt in invalid {
            assert_rejected(&codec, Encoded::Packet(pkt));
        }

        let publish = Publish {
            dup: false,
            retain: false,
            qos: QoS::AtMostOnce,
            topic: nul(),
            packet_id: None,
            payload_size: 0,
        };
        assert_rejected(&codec, Encoded::Publish(publish, None));
    }
}
