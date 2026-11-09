use std::{cell::Cell, cmp::min, fmt, num::NonZeroU32};

use ntex_bytes::{Buf, BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::MqttServiceConfig;
use crate::error::{DecodeError, EncodeError};
use crate::ticket::Tickets;
use crate::types::{FixedHeader, MAX_FRAME_RESERVE, MAX_PACKET_SIZE, packet_type};
use crate::utils::decode_variable_length_v5;

use super::{Decoded, Encoded};
use super::{Packet, decode::decode_packet, encode, encode::EncodeLtd, packet::Publish};

/// Stateful MQTT v5 packet codec with streamed publish payloads.
///
/// Cloning copies limits and in-progress framing state into an independent codec,
/// publish tickets of the clone start a new sequence.
/// Complete an outgoing publish payload before encoding another packet.
#[derive(Clone)]
pub struct Codec {
    /// Current inbound framing stage, retained between decode calls.
    state: Cell<DecodeState>,
    /// Inbound total packet byte limit; zero means the protocol limit only.
    max_in_size: Cell<u32>,
    /// Outbound total packet byte limit; zero means the protocol limit only.
    max_out_size: Cell<u32>,
    /// Byte threshold for partial payload chunks; zero has special buffering behavior.
    min_chunk_size: Cell<u32>,
    /// Connection role, diagnostic preference, and negotiated server capabilities.
    flags: Cell<CodecFlags>,
    /// Unwritten outbound publish payload bytes; cleared when all bytes are encoded.
    encoding_payload: Cell<Option<NonZeroU32>>,
    /// Arrival tickets of incoming `QoS 1` and `QoS 2` publishes.
    tickets: Tickets,
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    /// Connection role and preferences retained alongside the framing state.
    pub struct CodecFlags: u8 {
        /// The decoded `CONNECT` disabled optional diagnostic properties.
        const NO_PROBLEM_INFO = 0b0000_0001;
        /// The server does not support retained publications.
        const NO_RETAIN       = 0b0000_0010;
        /// A decoded `CONNECT` identified this codec as the server side.
        const SERVER          = 0b0000_0100;
        /// The server does not support subscription identifiers.
        const NO_SUB_IDS      = 0b0000_1000;
        /// The server does not support shared subscriptions.
        const NO_SHARED_SUBS  = 0b0001_0000;
        /// The server does not support wildcard subscriptions.
        const NO_WILDCARD_SUBS = 0b0010_0000;
    }
}

/// Inbound packet framing progress, including the remaining publish payload byte count.
#[derive(Debug, Clone, Copy)]
enum DecodeState {
    /// Waiting for the first byte and variable-length remaining length.
    FrameHeader,
    /// Waiting for the complete body of a non-publish packet.
    Frame(FixedHeader),
    /// Waiting to determine the publish header and properties byte length.
    PublishHeader(FixedHeader),
    /// Waiting for the given header-plus-properties byte count before decoding it.
    PublishProperties(u32, FixedHeader),
    /// Streaming the given number of remaining payload bytes before the next packet.
    PublishPayload(u32),
}

impl Codec {
    /// Creates an idle codec with no configured packet limits or chunk threshold.
    pub fn new() -> Self {
        Codec {
            state: Cell::new(DecodeState::FrameHeader),
            max_in_size: Cell::new(0),
            max_out_size: Cell::new(0),
            min_chunk_size: Cell::new(0),
            flags: Cell::new(CodecFlags::empty()),
            encoding_payload: Cell::new(None),
            tickets: Tickets::default(),
        }
    }

    /// Create `Codec` instance with inbound limits from the service configuration
    pub(crate) fn from_config(cfg: &MqttServiceConfig) -> Self {
        let codec = Self::new();
        codec.set_max_inbound_size(cfg.max_size);
        codec.set_min_chunk_size(cfg.min_chunk_size);
        codec
    }

    /// Sets the byte threshold for partial inbound publish payload chunks.
    ///
    /// The publish header is emitted as soon as it is complete. Its initial payload
    /// chunk contains buffered bytes if they reach this threshold or finish the payload.
    /// Later chunks wait for the threshold or the end of the payload; the final chunk
    /// may be smaller than the threshold.
    ///
    /// With the default of `0`, the initial chunk is emitted immediately, while later
    /// chunks wait for all remaining payload bytes.
    pub fn set_min_chunk_size(&self, size: u32) {
        self.min_chunk_size.set(size);
    }

    /// Returns the total inbound packet byte limit, or `0` for no configured limit.
    pub fn max_inbound_size(&self) -> u32 {
        self.max_in_size.get()
    }

    /// Returns the total outbound packet byte limit, or `0` for no configured limit.
    pub fn max_outbound_size(&self) -> u32 {
        self.max_out_size.get()
    }

    /// Sets the total inbound packet byte limit.
    ///
    /// The size is the total packet size, including the fixed header
    /// (MQTT 5.0, 3.1.2.11.4, 3.2.2.3.6).
    /// A size of `0` leaves only the MQTT protocol size limit in effect.
    /// By default max size is set to `0`
    pub fn set_max_inbound_size(&self, size: u32) {
        self.max_in_size.set(size);
    }

    /// Sets the total outbound packet byte limit.
    ///
    /// The size is the total packet size, including the fixed header
    /// (MQTT 5.0, 3.1.2.11.4, 3.2.2.3.6).
    /// A size of `0` leaves only the MQTT protocol size limit in effect.
    /// By default max size is set to `0`.
    pub fn set_max_outbound_size(&self, size: u32) {
        self.max_out_size.set(size);
    }

    /// Returns whether retained publications are supported by the server.
    pub(crate) fn retain_available(&self) -> bool {
        !self.flags.get().contains(CodecFlags::NO_RETAIN)
    }

    /// Returns whether subscription identifiers are supported by the server.
    pub(crate) fn sub_ids_available(&self) -> bool {
        !self.flags.get().contains(CodecFlags::NO_SUB_IDS)
    }

    /// Records whether the server supports retained publications.
    pub(crate) fn set_retain_available(&self, val: bool) {
        let mut flags = self.flags.get();
        flags.set(CodecFlags::NO_RETAIN, !val);
        self.flags.set(flags);
    }

    /// Records whether the server supports subscription identifiers.
    pub(crate) fn set_sub_ids_available(&self, val: bool) {
        let mut flags = self.flags.get();
        flags.set(CodecFlags::NO_SUB_IDS, !val);
        self.flags.set(flags);
    }

    /// Returns whether shared subscriptions are supported by the server.
    pub(crate) fn shared_subs_available(&self) -> bool {
        !self.flags.get().contains(CodecFlags::NO_SHARED_SUBS)
    }

    /// Records whether the server supports shared subscriptions.
    pub(crate) fn set_shared_subs_available(&self, val: bool) {
        let mut flags = self.flags.get();
        flags.set(CodecFlags::NO_SHARED_SUBS, !val);
        self.flags.set(flags);
    }

    /// Returns whether wildcard subscriptions are supported by the server.
    pub(crate) fn wildcard_subs_available(&self) -> bool {
        !self.flags.get().contains(CodecFlags::NO_WILDCARD_SUBS)
    }

    /// Records whether the server supports wildcard subscriptions.
    pub(crate) fn set_wildcard_subs_available(&self, val: bool) {
        let mut flags = self.flags.get();
        flags.set(CodecFlags::NO_WILDCARD_SUBS, !val);
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

    /// Max remaining length of an outbound packet, fails if no packet fits.
    fn max_out_size(&self) -> Result<u32, EncodeError> {
        match self.max_out_size.get() {
            0 => Ok(MAX_PACKET_SIZE),
            size => max_remaining_length(size).ok_or(EncodeError::OverMaxPacketSize),
        }
    }

    /// Encodes a packet, an incomplete publish payload is not checked.
    pub(crate) fn encode_packet(
        &self,
        mut pkt: Packet,
        dst: &mut BytePages,
    ) -> Result<(), EncodeError> {
        // Session Expiry Interval must not be sent on a DISCONNECT by the Server,
        // [MQTT-3.14.2-2] (MQTT 5.0, 3.14.2.2.2)
        if let Packet::Disconnect(ref mut pkt) = pkt
            && self.flags.get().contains(CodecFlags::SERVER)
        {
            pkt.session_expiry_interval_secs = None;
        }

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
        let max_size = self.max_out_size()?;
        let content_size = pkt.encoded_size(max_size);
        if content_size > max_size as usize {
            Err(EncodeError::OverMaxPacketSize)
        } else {
            pkt.encode(dst, content_size as u32) // safe: max_size <= MAX_PACKET_SIZE
        }
    }
}

/// Max remaining length of a packet with a total size of `size` or less, the fixed
/// header is 1 byte and 1 to 4 bytes of the remaining length (MQTT 5.0, 2.1.1, 1.5.5)
fn max_remaining_length(size: u32) -> Option<u32> {
    (1..=4).find_map(|len| {
        let remaining = size.checked_sub(1 + len)?.min(MAX_PACKET_SIZE);
        (encode::var_int_len(remaining as usize) <= len).then_some(remaining)
    })
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
                    match decode_variable_length_v5(&src_slice[1..])? {
                        Some((remaining_length, consumed)) => {
                            // check max packet size, it is the total packet size,
                            // (MQTT 5.0, 3.1.2.11.4, 3.2.2.3.6)
                            let max_in_size = self.max_in_size.get();
                            let size = remaining_length + consumed as u32 + 1; // safe: remaining_length <= MAX_PACKET_SIZE
                            if max_in_size != 0 && max_in_size < size {
                                log::debug!(
                                    "MaxSizeExceeded max-size: {max_in_size}, size: {size}"
                                );
                                return Err(DecodeError::MaxSizeExceeded {
                                    size,
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
                    let ticket = self.tickets.issue(publish.qos);

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
                            ticket,
                        )))
                    } else {
                        self.state.set(DecodeState::PublishPayload(payload_len));
                        Ok(Some(Decoded::Publish(
                            publish,
                            Bytes::new(),
                            fixed.remaining_length,
                            ticket,
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
                            flags.insert(CodecFlags::SERVER);
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
                let max_size = self.max_out_size()?;
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

impl fmt::Debug for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Codec")
            .field("state", &self.state)
            .field("max_in_size", &self.max_in_size)
            .field("max_out_size", &self.max_out_size)
            .field("min_chunk_size", &self.min_chunk_size)
            .field("flags", &self.flags)
            .field("tickets", &self.tickets)
            .finish()
    }
}

/// Unit tests for packet framing, streaming, and sender validation.
#[cfg(test)]
mod tests {
    use super::*;
    use ntex_bytes::ByteString;
    use std::num::NonZeroU16;

    use crate::v5::codec::{
        Auth, AuthReasonCode, Connect, ConnectAck, ConnectAckReason, Disconnect, LastWill,
        PublishAck, PublishAck2, QoS, Subscribe, SubscribeAck, SubscribeAckReason,
        SubscriptionOptions, Unsubscribe, UnsubscribeAck, UnsubscribeAckReason,
    };

    /// Checks malformed-packet rejection without appending any output bytes.
    fn assert_rejected(codec: &Codec, item: Encoded) {
        let mut buf = BytePages::default();
        assert_eq!(
            codec.encode(item, &mut buf),
            Err(EncodeError::MalformedPacket)
        );
        assert!(buf.freeze().is_empty(), "nothing is written");
    }

    /// Checks successful encoding produces a nonempty output buffer.
    fn assert_encoded(codec: &Codec, item: Encoded) {
        let mut buf = BytePages::default();
        codec.encode(item, &mut buf).unwrap();
        assert!(!buf.freeze().is_empty());
    }

    /// STATEMENT: Inbound size limits count the entire packet, including the fixed header.
    ///
    /// HOW:
    /// - Decode oversized and boundary-length headers under small configured byte limits.
    /// - Check the reported total and maximum sizes, or `None` when only the body is missing.
    #[test]
    fn test_max_size() {
        let codec = Codec::new();
        codec.set_max_inbound_size(5);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"\0\x09");
        assert_eq!(
            codec.decode(&mut buf).err(),
            Some(DecodeError::MaxSizeExceeded {
                size: 11,
                max_size: 5
            })
        );

        // max size includes the fixed header
        for (max_size, header, size) in [
            (130_u32, &b"\x30\x7f"[..], 0),
            (130, b"\x30\x80\x01", 131),
            (131, b"\x30\x80\x01", 0),
            (131, b"\x30\x81\x01", 132),
        ] {
            let codec = Codec::new();
            codec.set_max_inbound_size(max_size);
            let mut buf = BytesMut::from(header);
            if size == 0 {
                assert_eq!(codec.decode(&mut buf), Ok(None));
            } else {
                assert_eq!(
                    codec.decode(&mut buf),
                    Err(DecodeError::MaxSizeExceeded { size, max_size })
                );
            }
        }
    }

    /// STATEMENT: The outbound remaining-length budget accounts for variable-length header boundaries.
    ///
    /// HOW:
    /// - Compare `max_remaining_length` with expected budgets around each prefix-width transition.
    /// - Include a total size too small for a packet and a limit above the protocol maximum.
    #[test]
    fn test_max_remaining_length() {
        for (size, remaining) in [
            (1, None),
            (2, Some(0)),
            (6, Some(4)),
            (129, Some(127)),
            (130, Some(127)),
            (131, Some(128)),
            (16_386, Some(16_383)),
            (16_387, Some(16_383)),
            (16_388, Some(16_384)),
            (2_097_156, Some(2_097_151)),
            (2_097_157, Some(2_097_152)),
            (MAX_PACKET_SIZE + 5, Some(MAX_PACKET_SIZE)),
            (u32::MAX, Some(MAX_PACKET_SIZE)),
        ] {
            assert_eq!(max_remaining_length(size), remaining, "size: {size}");
        }
    }

    /// STATEMENT: Inbound payload state clears only after the final publish chunk.
    ///
    /// HOW:
    /// - Decode 80 of 100 payload bytes with a ten-byte threshold, then append two ten-byte chunks.
    /// - Check chunk bytes, final-chunk flags, and `is_payload_pending` before and after completion.
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
        let Some(Decoded::Publish(_, payload, _, _)) = codec.decode(&mut src).unwrap() else {
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

    /// STATEMENT: A publish header must fit within its declared remaining length.
    ///
    /// HOW:
    /// - Decode a header needing six bytes with remaining lengths of five and six.
    /// - Expect `InvalidLength` for the first and an empty, completed payload for the second.
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
        let Some(Decoded::Publish(pkt, payload, 6, _)) = codec.decode(&mut src).unwrap() else {
            panic!()
        };
        assert_eq!(pkt.topic, "abc");
        assert_eq!(pkt.payload_size, 0);
        assert!(payload.is_empty());
        assert!(!codec.is_payload_pending());
    }

    /// STATEMENT: Decoding issues arrival tickets to `QoS 1` and `QoS 2` publishes only.
    ///
    /// HOW:
    /// - Decode `QoS 1`, `QoS 0`, `QoS 2`, and a streamed `QoS 1` publish; check ticket sequence
    ///   numbers 0, none, 1, and 2, with the streamed ticket issued on its header.
    /// - Decode with a clone and check it starts an independent sequence that does not advance
    ///   the original.
    #[test]
    fn test_publish_ticket() {
        let seq = |codec: &Codec, data: &[u8]| {
            let Some(Decoded::Publish(.., ticket)) =
                codec.decode(&mut BytesMut::from(data)).unwrap()
            else {
                panic!()
            };
            ticket.map(|t| t.seq())
        };
        let codec = Codec::new();
        assert_eq!(seq(&codec, b"\x32\x08\x00\x03abc\x00\x01\x00"), Some(0));
        assert_eq!(seq(&codec, b"\x30\x06\x00\x03abc\x00"), None);
        assert_eq!(seq(&codec, b"\x34\x08\x00\x03abc\x00\x02\x00"), Some(1));

        // a clone starts its own sequence
        let clone = codec.clone();
        assert_eq!(seq(&clone, b"\x32\x08\x00\x03abc\x00\x01\x00"), Some(0));
        assert_eq!(seq(&clone, b"\x32\x08\x00\x03abc\x00\x01\x00"), Some(1));

        let mut src = BytesMut::from(&b"\x32\x6c\x00\x03abc\x00\x03\x00"[..]);
        src.extend_from_slice(&[b'a'; 50]);
        let Some(Decoded::Publish(.., Some(ticket))) = codec.decode(&mut src).unwrap() else {
            panic!()
        };
        assert_eq!(ticket.seq(), 2);
        assert!(codec.is_payload_pending());
    }

    /// STATEMENT: An oversized initial payload is rejected without changing outbound payload state.
    ///
    /// HOW:
    /// - Supply three bytes for a two-byte publish and check the error and empty output.
    /// - Verify a following chunk is unexpected, then encode a correctly sized publish successfully.
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

    /// STATEMENT: Outbound bodies cannot exceed the MQTT remaining-length maximum.
    ///
    /// HOW:
    /// - Try oversized declared publish payloads with unset and very large configured limits.
    /// - Check empty output and no pending payload after rejection, and the exact boundary header on success.
    /// - Reject a subscription whose repeated filters exceed the protocol maximum.
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

    /// STATEMENT: An unfinished outbound publish prevents another packet from being encoded.
    ///
    /// HOW:
    /// - Encode half a four-byte payload and attempt a ping and another publish.
    /// - Check `ExpectPayload` without output, then finish the payload and verify the ping bytes follow.
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

    /// STATEMENT: Subscription identifiers must fit in an MQTT variable-byte integer.
    ///
    /// HOW:
    /// - Encode subscriptions and publishes using identifiers at and above 268,435,455.
    /// - Check oversized identifiers leave no output and boundary identifiers produce the expected bytes.
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

    /// STATEMENT: A decoded diagnostic opt-out suppresses properties on outgoing publish acknowledgments.
    ///
    /// HOW:
    /// - Decode a `CONNECT` with `request_problem_info` disabled.
    /// - Encode an acknowledgment with diagnostics and compare its stripped bytes with a fresh codec output.
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

    /// STATEMENT: Incomplete non-publish frames reserve bounded receive-buffer capacity.
    ///
    /// HOW:
    /// - Decode a maximum-size subscription header and a short partially received frame.
    /// - Check consumed bytes and capacity ranges against `MAX_FRAME_RESERVE` and the smaller frame length.
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

    /// STATEMENT: Publish encoding validates topic names and the relationship between flags and quality of service.
    ///
    /// HOW:
    /// - Reject empty or wildcard topics, duplicate delivery at quality-of-service zero, and an unexpected identifier.
    /// - Accept valid normal and aliased topics and an identified redelivery, checking invalid packets write nothing.
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

    /// STATEMENT: Connection and subscription encoding rejects invalid Will topics and empty filter lists.
    ///
    /// HOW:
    /// - Try missing or wildcard Will topics and subscription requests with absent or empty filters.
    /// - Check rejection without output and successful encoding of valid Wills, wildcard filters, and password-only credentials.
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

    /// Builds one user property with the supplied key and a fixed valid value.
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

    /// Builds a successful authentication packet with a valid method.
    fn auth_with_method() -> Auth {
        Auth {
            auth_method: Some(ByteString::from_static("m")),
            ..Auth::default()
        }
    }

    /// Builds a valid connection request with a mutable Will for validation tests.
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

    /// STATEMENT: Subscription requests enforce wildcard placement and shared-filter syntax.
    ///
    /// HOW:
    /// - Encode each valid and invalid filter in both subscribe and unsubscribe requests.
    /// - Check malformed filters leave no output and valid wildcard and shared filters encode successfully.
    #[test]
    fn test_encode_topic_filter_syntax() {
        let codec = Codec::new();
        let packet_id = NonZeroU16::new(1).unwrap();
        let subscribe = |filter: &'static str| {
            Encoded::Packet(Packet::Subscribe(Subscribe {
                packet_id,
                id: None,
                user_properties: Vec::new(),
                topic_filters: vec![
                    (ByteString::from_static("a"), SubscriptionOptions::default()),
                    (
                        ByteString::from_static(filter),
                        SubscriptionOptions::default(),
                    ),
                ],
            }))
        };
        let unsubscribe = |filter: &'static str| {
            Encoded::Packet(Packet::Unsubscribe(Unsubscribe {
                packet_id,
                user_properties: Vec::new(),
                topic_filters: vec![
                    ByteString::from_static("a"),
                    ByteString::from_static(filter),
                ],
            }))
        };

        // [MQTT-4.7.1-1] `#` must be the last character and follow a level separator,
        // [MQTT-4.7.1-2] `+` must occupy an entire level (MQTT 5.0, 4.7.1),
        // [MQTT-4.8.2-1], [MQTT-4.8.2-2] ShareName must be at least one character long,
        // must not contain wildcards and must be followed by a Topic Filter (MQTT 5.0, 4.8.2)
        for filter in [
            "a/#/b",
            "#/",
            "a#",
            "a/b#",
            "a+",
            "+a",
            "a/+b/c",
            "++",
            "$share//a",
            "$share/g",
            "$share/g/",
            "$share/g+/a",
            "$share/#/a",
            "$share/g/a#",
        ] {
            assert_rejected(&codec, subscribe(filter));
            assert_rejected(&codec, unsubscribe(filter));
        }
        for filter in [
            "#",
            "+",
            "a/#",
            "+/+",
            "/+/",
            "a//b",
            "$SYS/#",
            "$share/g/a",
            "$share/g/#",
            "$share/g//",
            "$sharex/+",
        ] {
            assert_encoded(&codec, subscribe(filter));
            assert_encoded(&codec, unsubscribe(filter));
        }
    }

    /// STATEMENT: Connection and publish string fields reject null characters and excessive encoded lengths.
    ///
    /// HOW:
    /// - Replace each tested connection, acknowledgment, Will, and publish string field with invalid strings.
    /// - Check rejection without output and successful encoding at the 65,535-byte boundary.
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

    /// STATEMENT: Other control-packet string fields enforce MQTT string validity before encoding.
    ///
    /// HOW:
    /// - Inject null-containing and oversized strings into tested acknowledgment, subscription, disconnect, and authentication fields.
    /// - Check invalid strings leave no output and strings of 65,535 bytes remain encodable.
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
            |p| Encoded::Packet(Packet::Disconnect(Box::new(p))),
            &[
                |p, s| p.server_reference = Some(s),
                |p, s| p.reason_string = Some(s),
                |p, s| p.user_properties = props(s),
            ],
        );
        assert_str_rejected(
            &auth_with_method(),
            |p| Encoded::Packet(Packet::Auth(Box::new(p))),
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

    /// STATEMENT: Length-prefixed binary fields accept 65,535 bytes but reject 65,536 bytes.
    ///
    /// HOW:
    /// - Set each tested credential, Will, authentication, and correlation-data field to both lengths.
    /// - Check successful boundary encoding and rejection without output for oversized data.
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
            |p| Encoded::Packet(Packet::Auth(Box::new(p))),
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

    /// STATEMENT: Response topics in publishes and Wills must be nonempty topic names without wildcards.
    ///
    /// HOW:
    /// - Encode both packet forms with `a/b`, then with empty and wildcard response topics.
    /// - Check valid encoding and rejection without output for each invalid topic.
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

    /// STATEMENT: Shared subscriptions cannot request the no-local option.
    ///
    /// HOW:
    /// - Encode shared filters with `no_local` disabled and enabled, checking success and rejection without output.
    /// - Confirm ordinary filters and names merely resembling shared subscriptions permit `no_local`.
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

    /// STATEMENT: A connection acknowledgment cannot advertise a zero maximum packet size.
    ///
    /// HOW:
    /// - Encode absent, one-byte, and zero limits, checking acceptance or rejection without output.
    /// - Decode wire limits of one and zero and compare the property or malformed-packet error.
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

    /// STATEMENT: The maximum-quality-of-service property permits only zero or one, with absence implying two.
    ///
    /// HOW:
    /// - Decode and re-encode both permitted values and compare their exact wire bytes.
    /// - Check the absent default and reject explicitly encoded values of two and three.
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

    /// STATEMENT: Connection authentication data requires an authentication method.
    ///
    /// HOW:
    /// - Encode otherwise identical connections with and without a method, checking rejection without output.
    /// - Decode matching wire examples and compare the recovered credentials or malformed-packet error.
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

    /// STATEMENT: Authentication packets require a method except for property-free success in short form.
    ///
    /// HOW:
    /// - Round-trip short success and method-bearing packets, checking their exact wire bytes.
    /// - Reject non-short packets lacking a method during both encoding and decoding.
    #[test]
    fn test_auth_method_required() {
        let encode = |pkt: Auth| {
            let mut buf = BytePages::default();
            Codec::new()
                .encode(Encoded::Packet(Packet::Auth(Box::new(pkt))), &mut buf)
                .map(|()| buf.freeze())
        };
        let decode = |data: &[u8]| match Codec::new().decode(&mut BytesMut::from(data)) {
            Ok(Some(Decoded::Packet(Packet::Auth(pkt), _))) => Ok(*pkt),
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

    /// STATEMENT: Only the client-side codec emits a disconnect session-expiry property.
    ///
    /// HOW:
    /// - Encode a disconnect with an expiry interval using a fresh codec.
    /// - Decode a `CONNECT` to mark the codec as server-side and verify the next encoded disconnect omits expiry.
    #[test]
    fn test_server_disconnect_session_expiry() {
        let disconnect = || {
            Encoded::Packet(Packet::Disconnect(Box::new(Disconnect {
                session_expiry_interval_secs: Some(10),
                ..Disconnect::default()
            })))
        };
        let encode = |codec: &Codec| {
            let mut buf = BytePages::default();
            codec.encode(disconnect(), &mut buf).unwrap();
            buf.freeze()
        };

        // client keeps Session Expiry Interval
        let codec = Codec::new();
        assert_eq!(&encode(&codec)[..], b"\xE0\x07\x00\x05\x11\x00\x00\x00\x0A");

        // server drops it
        let mut src = BytesMut::from(&b"\x10\x0F\x00\x04MQTT\x05\x02\x00\x3C\x00\x00\x02id"[..]);
        let Some(Decoded::Packet(Packet::Connect(_), _)) = codec.decode(&mut src).unwrap() else {
            panic!()
        };
        assert_eq!(&encode(&codec)[..], b"\xE0\x02\x00\x00");
    }

    /// STATEMENT: An unsuccessful connection acknowledgment never encodes the session-present flag.
    ///
    /// HOW:
    /// - Encode successful acknowledgments with both flag values and an unsuccessful one requesting the flag.
    /// - Compare exact bytes to confirm success preserves the flag and failure clears it.
    #[test]
    fn test_connack_session_present_with_error() {
        let encode = |session_present, reason_code| {
            let mut buf = BytePages::default();
            Codec::new()
                .encode(
                    Encoded::Packet(Packet::ConnectAck(Box::new(ConnectAck {
                        session_present,
                        reason_code,
                        ..ConnectAck::default()
                    }))),
                    &mut buf,
                )
                .unwrap();
            buf.freeze()
        };
        assert_eq!(
            &encode(true, ConnectAckReason::Success)[..],
            b"\x20\x03\x01\x00\x00"
        );
        assert_eq!(
            &encode(false, ConnectAckReason::Success)[..],
            b"\x20\x03\x00\x00\x00"
        );
        assert_eq!(
            &encode(true, ConnectAckReason::NotAuthorized)[..],
            b"\x20\x03\x00\x87\x00"
        );
    }

    /// STATEMENT: A zero message-expiry interval survives publish and Will round trips.
    ///
    /// HOW:
    /// - Decode wire packets containing zero expiry intervals and inspect the recovered properties.
    /// - Re-encode both packets and compare the complete output bytes with the original input.
    #[test]
    fn test_message_expiry_interval_zero() {
        // a Message Expiry Interval of 0 is valid (MQTT 5.0, 3.3.2.3.3, 3.1.3.2.4)
        let publish = b"\x30\x09\x00\x01t\x05\x02\x00\x00\x00\x00";
        let connect = b"\x10\x1A\x00\x04MQTT\x05\x06\x00\x3C\x00\x00\x02id\
\x05\x02\x00\x00\x00\x00\x00\x01t\x00\x00";

        let codec = Codec::new();
        let Some(Decoded::Publish(pkt, _, _, _)) =
            codec.decode(&mut BytesMut::from(&publish[..])).unwrap()
        else {
            panic!()
        };
        assert_eq!(pkt.properties.message_expiry_interval, Some(0));
        let mut buf = BytePages::default();
        codec
            .encode(Encoded::Publish(pkt, Some(Bytes::new())), &mut buf)
            .unwrap();
        assert_eq!(&buf.freeze()[..], &publish[..]);

        let codec = Codec::new();
        let Some(Decoded::Packet(Packet::Connect(pkt), _)) =
            codec.decode(&mut BytesMut::from(&connect[..])).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            pkt.last_will.as_ref().unwrap().message_expiry_interval,
            Some(0)
        );
        let mut buf = BytePages::default();
        codec
            .encode(Encoded::Packet(Packet::Connect(pkt)), &mut buf)
            .unwrap();
        assert_eq!(&buf.freeze()[..], &connect[..]);
    }

    /// STATEMENT: Cloning preserves codec limits, capabilities, and in-progress framing.
    ///
    /// HOW:
    /// - Clone after configuring limits and capabilities and reading a partial acknowledgment.
    /// - Check copied settings and finish decoding through the clone; also clone an unfinished outgoing publish and check its pending state.
    #[test]
    fn test_clone() {
        let codec = Codec::new();
        codec.set_max_inbound_size(100);
        codec.set_max_outbound_size(200);
        codec.set_retain_available(false);
        codec.set_sub_ids_available(false);
        codec.set_shared_subs_available(false);
        codec.set_wildcard_subs_available(false);

        // partially decoded PUBACK
        let mut src = BytesMut::from(&b"\x40\x02"[..]);
        assert!(codec.decode(&mut src).unwrap().is_none());

        let cloned = codec.clone();
        assert_eq!(cloned.max_inbound_size(), 100);
        assert_eq!(cloned.max_outbound_size(), 200);
        assert!(!cloned.retain_available());
        assert!(!cloned.sub_ids_available());
        assert!(!cloned.shared_subs_available());
        assert!(!cloned.wildcard_subs_available());
        assert_eq!(cloned.flags.get(), codec.flags.get());

        src.extend_from_slice(b"\x00\x01");
        assert_eq!(
            cloned.decode(&mut src).unwrap(),
            Some(Decoded::Packet(
                Packet::PublishAck(PublishAck {
                    packet_id: NonZeroU16::new(1).unwrap(),
                    ..PublishAck::default()
                }),
                2
            ))
        );

        // payload of an encoded publish is expected
        let codec = Codec::new();
        let mut buf = BytePages::default();
        codec
            .encode(
                Encoded::Publish(
                    Publish {
                        topic: ByteString::from_static("t"),
                        payload_size: 1,
                        ..Default::default()
                    },
                    None,
                ),
                &mut buf,
            )
            .unwrap();
        assert!(codec.clone().is_encoding_payload());
    }

    /// STATEMENT: Small outbound limits reject required bytes while allowing diagnostic properties to be omitted.
    ///
    /// HOW:
    /// - Encode pings, publish acknowledgments, and subscription acknowledgments under several small total-size limits.
    /// - Compare errors and exact bytes with or without a reason string, then check publish payload-size boundaries.
    #[test]
    fn test_small_max_outbound_size() {
        let encode = |max_size, pkt: Packet| {
            let codec = Codec::new();
            codec.set_max_outbound_size(max_size);
            assert_eq!(codec.max_outbound_size(), max_size);
            let mut buf = BytePages::default();
            codec
                .encode(Encoded::Packet(pkt), &mut buf)
                .map(|()| buf.freeze())
        };
        let puback = || {
            Packet::PublishAck(PublishAck {
                packet_id: NonZeroU16::new(1).unwrap(),
                reason_string: Some(ByteString::from_static("reason")),
                ..PublishAck::default()
            })
        };
        let pubrel = || {
            Packet::PublishRelease(PublishAck2 {
                packet_id: NonZeroU16::new(1).unwrap(),
                reason_string: Some(ByteString::from_static("reason")),
                ..PublishAck2::default()
            })
        };
        let suback = || {
            Packet::SubscribeAck(SubscribeAck {
                packet_id: NonZeroU16::new(1).unwrap(),
                status: vec![SubscribeAckReason::GrantedQos0],
                reason_string: Some(ByteString::from_static("reason")),
                properties: Vec::new(),
            })
        };

        assert_eq!(
            encode(1, Packet::PingRequest),
            Err(EncodeError::OverMaxPacketSize)
        );
        assert_eq!(&encode(2, Packet::PingRequest).unwrap()[..], b"\xC0\x00");
        for size in [1, 2, 3, 4, 5] {
            assert_eq!(encode(size, puback()), Err(EncodeError::OverMaxPacketSize));
            assert_eq!(encode(size, pubrel()), Err(EncodeError::OverMaxPacketSize));
            assert_eq!(encode(size, suback()), Err(EncodeError::OverMaxPacketSize));
        }
        // reason string doesn't fit
        assert_eq!(
            &encode(6, puback()).unwrap()[..],
            b"\x40\x04\x00\x01\x00\x00"
        );
        assert_eq!(
            &encode(6, pubrel()).unwrap()[..],
            b"\x62\x04\x00\x01\x00\x00"
        );
        assert_eq!(
            &encode(6, suback()).unwrap()[..],
            b"\x90\x04\x00\x01\x00\x00"
        );
        // the whole packet fits
        assert_eq!(
            &encode(18, puback()).unwrap()[..],
            b"\x40\x0D\x00\x01\x00\x09\x1F\x00\x06reason"
        );
        assert_eq!(
            &encode(17, puback()).unwrap()[..],
            b"\x40\x04\x00\x01\x00\x00"
        );
        assert_eq!(
            &encode(18, suback()).unwrap()[..],
            b"\x90\x0D\x00\x01\x09\x1F\x00\x06reason\x00"
        );
        assert_eq!(
            &encode(17, suback()).unwrap()[..],
            b"\x90\x04\x00\x01\x00\x00"
        );

        // publish
        let codec = Codec::new();
        codec.set_max_outbound_size(9);
        let pkt = |payload_size| Publish {
            topic: ByteString::from_static("t"),
            payload_size,
            ..Default::default()
        };
        let mut buf = BytePages::default();
        for size in [1, 2] {
            let codec = Codec::new();
            codec.set_max_outbound_size(size);
            assert_eq!(
                codec.encode(Encoded::Publish(pkt(0), None), &mut buf),
                Err(EncodeError::OverMaxPacketSize)
            );
        }
        assert!(buf.is_empty());
        assert_eq!(
            codec.encode(Encoded::Publish(pkt(4), None), &mut buf),
            Err(EncodeError::OverMaxPacketSize)
        );
        codec
            .encode(
                Encoded::Publish(pkt(3), Some(Bytes::from_static(b"abc"))),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf.freeze()[..], b"\x30\x07\x00\x01t\x00abc");
    }

    /// STATEMENT: Service configuration initializes inbound codec limits without setting an outbound limit.
    ///
    /// HOW:
    /// - Create a codec from configured maximum packet and minimum chunk sizes.
    /// - Check both copied values and the unchanged zero outbound limit.
    #[test]
    fn test_from_config() {
        let cfg = MqttServiceConfig::new()
            .set_max_size(1)
            .set_min_chunk_size(2);
        let codec = Codec::from_config(&cfg);
        assert_eq!(codec.max_inbound_size(), 1);
        assert_eq!(codec.min_chunk_size.get(), 2);
        assert_eq!(codec.max_outbound_size(), 0);
    }
}
