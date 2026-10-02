use std::num::{NonZeroU16, NonZeroU32};

use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};

use super::ack_props;
use crate::error::{DecodeError, EncodeError};
use crate::types::QoS;
use crate::utils::{self, Decode, Encode, write_variable_length};
use crate::v5::codec::{UserProperties, UserProperty, encode, property_type as pt};

/// Represents SUBSCRIBE packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Subscribe {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// Subscription Identifier
    pub id: Option<NonZeroU32>,
    /// User Property pairs, additional diagnostic or other information.
    pub user_properties: UserProperties,
    /// the list of Topic Filters and `QoS` to which the Client wants to subscribe.
    pub topic_filters: Vec<(ByteString, SubscriptionOptions)>,
}

/// Subscription Options of a Topic Filter
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct SubscriptionOptions {
    /// Maximum `QoS` level at which the Server may send messages to the Client.
    pub qos: QoS,
    /// No Local option, messages must not be forwarded to a connection with a Client Identifier
    /// equal to the Client Identifier of the publishing connection.
    pub no_local: bool,
    /// Retain As Published option, keep the RETAIN flag of forwarded messages.
    pub retain_as_published: bool,
    /// Retain Handling option, whether retained messages are sent when the subscription
    /// is established.
    pub retain_handling: RetainHandling,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::AtSubscribe,
        }
    }
}

prim_enum! {
    /// Retain Handling subscription option
    pub enum RetainHandling {
        /// Send retained messages at the time of the subscribe.
        AtSubscribe = 0,
        /// Send retained messages at subscribe only if the subscription does not
        /// currently exist.
        AtSubscribeNew = 1,
        /// Do not send retained messages at the time of the subscribe.
        NoAtSubscribe = 2
    }
}

/// Represents SUBACK packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SubscribeAck {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// User Property pairs, additional diagnostic or other information.
    pub properties: UserProperties,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
    /// corresponds to a Topic Filter in the SUBSCRIBE Packet being acknowledged.
    pub status: Vec<SubscribeAckReason>,
}

/// Represents UNSUBSCRIBE packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Unsubscribe {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// User Property pairs, additional diagnostic or other information.
    pub user_properties: UserProperties,
    /// the list of Topic Filters that the Client wishes to unsubscribe from.
    pub topic_filters: Vec<ByteString>,
}

/// Represents UNSUBACK packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnsubscribeAck {
    /// Packet Identifier
    pub packet_id: NonZeroU16,
    /// User Property pairs, additional diagnostic or other information.
    pub properties: UserProperties,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
    /// Reason codes, one for each Topic Filter in the UNSUBSCRIBE packet being acknowledged.
    pub status: Vec<UnsubscribeAckReason>,
}

prim_enum! {
    /// SUBACK reason codes
    pub enum SubscribeAckReason {
        /// The subscription is accepted and the maximum `QoS` sent will be `QoS` 0.
        GrantedQos0 = 0,
        /// The subscription is accepted and the maximum `QoS` sent will be `QoS` 1.
        GrantedQos1 = 1,
        /// The subscription is accepted and any received `QoS` will be sent.
        GrantedQos2 = 2,
        /// The subscription is not accepted and the Server either does not wish to reveal
        /// the reason or none of the other reason codes apply.
        UnspecifiedError = 128,
        /// The SUBSCRIBE is valid but the Server does not accept it.
        ImplementationSpecificError = 131,
        /// The Client is not authorized to make this subscription.
        NotAuthorized = 135,
        /// The Topic Filter is correctly formed but is not allowed for this Client.
        TopicFilterInvalid = 143,
        /// The specified Packet Identifier is already in use.
        PacketIdentifierInUse = 145,
        /// An implementation or administrative imposed limit has been exceeded.
        QuotaExceeded = 151,
        /// The Server does not support Shared Subscriptions for this Client.
        SharedSubscriptionNotSupported = 158,
        /// The Server does not support Subscription Identifiers.
        SubscriptionIdentifiersNotSupported = 161,
        /// The Server does not support Wildcard Subscriptions.
        WildcardSubscriptionsNotSupported = 162
    }
}

prim_enum! {
    /// UNSUBACK reason codes
    pub enum UnsubscribeAckReason {
        /// The subscription is deleted.
        Success = 0,
        /// No matching Topic Filter is being used by the Client.
        NoSubscriptionExisted = 17,
        /// The unsubscribe could not be completed and the Server either does not wish to
        /// reveal the reason or none of the other reason codes apply.
        UnspecifiedError = 128,
        /// The UNSUBSCRIBE is valid but the Server does not accept it.
        ImplementationSpecificError = 131,
        /// The Client is not authorized to unsubscribe.
        NotAuthorized = 135,
        /// The Topic Filter is correctly formed but is not allowed for this Client.
        TopicFilterInvalid = 143,
        /// The specified Packet Identifier is already in use.
        PacketIdentifierInUse = 145
    }
}

impl Subscribe {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let prop_src = &mut utils::take_properties(src)?;
        let mut sub_id = None;
        let mut user_properties = Vec::new();
        while prop_src.has_remaining() {
            let prop_id = prop_src.get_u8();
            match prop_id {
                pt::SUB_ID => {
                    ensure!(sub_id.is_none(), DecodeError::MalformedPacket); // can't appear twice
                    let val = utils::decode_variable_length_cursor(prop_src)?;
                    sub_id = Some(NonZeroU32::new(val).ok_or(DecodeError::MalformedPacket)?);
                }
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket),
            }
        }

        let mut topic_filters = Vec::new();
        while src.has_remaining() {
            let topic = ByteString::decode(src)?;
            let opts = SubscriptionOptions::decode(src)?;
            topic_filters.push((topic, opts));
        }
        // [MQTT-3.8.3-2] at least one topic filter is required (5.0, 3.8.3)
        ensure!(!topic_filters.is_empty(), DecodeError::MalformedPacket);

        Ok(Self {
            packet_id,
            id: sub_id,
            user_properties,
            topic_filters,
        })
    }
}

impl SubscribeAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let (properties, reason_string) = ack_props::decode(src)?;
        let mut status = Vec::with_capacity(src.remaining());
        for code in src.as_ref().iter().copied() {
            status.push(code.try_into()?);
        }
        Ok(Self {
            packet_id,
            properties,
            reason_string,
            status,
        })
    }
}

impl Unsubscribe {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;

        let prop_src = &mut utils::take_properties(src)?;
        let mut user_properties = Vec::new();
        while prop_src.has_remaining() {
            let prop_id = prop_src.get_u8();
            match prop_id {
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket),
            }
        }

        let mut topic_filters = Vec::new();
        while src.remaining() > 0 {
            topic_filters.push(ByteString::decode(src)?);
        }
        // [MQTT-3.10.3-2] at least one topic filter is required (5.0, 3.10.3)
        ensure!(!topic_filters.is_empty(), DecodeError::MalformedPacket);

        Ok(Self {
            packet_id,
            user_properties,
            topic_filters,
        })
    }
}

impl UnsubscribeAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let packet_id = NonZeroU16::decode(src)?;
        let (properties, reason_string) = ack_props::decode(src)?;
        let mut status = Vec::with_capacity(src.remaining());
        for code in src.as_ref().iter().copied() {
            status.push(code.try_into()?);
        }
        Ok(Self {
            packet_id,
            properties,
            reason_string,
            status,
        })
    }
}

impl encode::EncodeLtd for Subscribe {
    fn encoded_size(&self, _limit: u32) -> usize {
        let prop_len = self.id.map_or(0, |v| 1 + encode::var_int_len(v.get() as usize) as usize) // +1 to account for property type byte
            + self.user_properties.encoded_size();
        let payload_len = self
            .topic_filters
            .iter()
            .fold(0, |acc, (filter, _opts)| acc + filter.encoded_size() + 1);
        self.packet_id.encoded_size()
            + encode::var_int_len(prop_len) as usize
            + prop_len
            + payload_len
    }

    fn encode(&self, buf: &mut BytePages, _: u32) -> Result<(), EncodeError> {
        self.packet_id.encode(buf)?;

        // encode properties
        let prop_len = self
            .id
            .map_or(0, |v| 1 + encode::var_int_len(v.get() as usize))
            + self.user_properties.encoded_size() as u32; // safe: size was already checked against maximum
        utils::write_variable_length(prop_len, buf);

        if let Some(id) = self.id {
            buf.put_u8(pt::SUB_ID);
            write_variable_length(id.get(), buf);
        }

        self.user_properties.encode(buf)?;

        // payload
        for (filter, opts) in &self.topic_filters {
            filter.encode(buf)?;
            opts.encode(buf)?;
        }

        Ok(())
    }
}

impl Decode for SubscriptionOptions {
    fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        ensure!(src.has_remaining(), DecodeError::InvalidLength);
        let val = src.get_u8();
        // [MQTT-3.8.3-5] reserved bits of subscription options must be zero (5.0, 3.8.3.1)
        ensure!(val & 0b1100_0000 == 0, DecodeError::MalformedPacket);
        let qos = (val & 0b0000_0011).try_into()?;
        let retain_handling = ((val & 0b0011_0000) >> 4).try_into()?;
        Ok(SubscriptionOptions {
            qos,
            no_local: val & 0b0000_0100 != 0,
            retain_as_published: val & 0b0000_1000 != 0,
            retain_handling,
        })
    }
}

impl Encode for SubscriptionOptions {
    fn encoded_size(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut BytePages) -> Result<(), EncodeError> {
        buf.put_u8(
            u8::from(self.qos)
                | (u8::from(self.no_local) << 2)
                | (u8::from(self.retain_as_published) << 3)
                | (u8::from(self.retain_handling) << 4),
        );
        Ok(())
    }
}

impl encode::EncodeLtd for SubscribeAck {
    fn encoded_size(&self, limit: u32) -> usize {
        let len = self.status.len();
        2 + len
            + ack_props::encoded_size(
                &self.properties,
                &self.reason_string,
                encode::reduce_limit(limit, 2 + len),
            )
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        self.packet_id.encode(buf)?;
        let len = self.status.len() as u32; // safe: max size checked already
        ack_props::encode(&self.properties, &self.reason_string, buf, size - 2 - len)?;
        for &reason in &self.status {
            buf.put_u8(reason.into());
        }
        Ok(())
    }
}

impl encode::EncodeLtd for Unsubscribe {
    fn encoded_size(&self, _limit: u32) -> usize {
        let prop_len = self.user_properties.encoded_size();
        2 + encode::var_int_len(prop_len) as usize
            + prop_len
            + self
                .topic_filters
                .iter()
                .fold(0, |acc, filter| acc + 2 + filter.len())
    }

    fn encode(&self, buf: &mut BytePages, _size: u32) -> Result<(), EncodeError> {
        self.packet_id.encode(buf)?;

        // properties
        let prop_len = self.user_properties.encoded_size();
        utils::write_variable_length(prop_len as u32, buf); // safe: max size check is done already
        self.user_properties.encode(buf)?;

        // payload
        for filter in &self.topic_filters {
            filter.encode(buf)?;
        }
        Ok(())
    }
}

impl encode::EncodeLtd for UnsubscribeAck {
    // todo: almost identical to SUBACK
    fn encoded_size(&self, limit: u32) -> usize {
        let len = self.status.len();
        2 + len
            + ack_props::encoded_size(
                &self.properties,
                &self.reason_string,
                encode::reduce_limit(limit, 2 + len),
            )
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        self.packet_id.encode(buf)?;
        let len = self.status.len() as u32;

        ack_props::encode(&self.properties, &self.reason_string, buf, size - 2 - len)?;
        for &reason in &self.status {
            buf.put_u8(reason.into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ntex_codec::{Decoder, Encoder};

    use super::super::super::{Codec, Decoded, EncodeLtd, Packet};
    // use crate::v5::codec::encode::EncodeLtd;
    use super::*;

    fn packet(res: Decoded) -> Packet {
        match res {
            Decoded::Packet(pkt, _) => pkt,
            _ => panic!(),
        }
    }

    #[test]
    fn test_sub() {
        let pkt = Subscribe {
            packet_id: 12.try_into().unwrap(),
            id: Some(10.try_into().unwrap()),
            user_properties: vec![("a".into(), "1".into())],
            topic_filters: vec![("test".into(), SubscriptionOptions::default())],
        };

        let size = pkt.encoded_size(99999);
        let mut buf = BytePages::default();
        pkt.encode(&mut buf, size as u32).unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(
            pkt,
            Subscribe::decode(&mut buf.take().unwrap().freeze()).unwrap()
        );

        let pkt = Unsubscribe {
            packet_id: 12.try_into().unwrap(),
            user_properties: vec![("a".into(), "1".into())],
            topic_filters: vec!["test".into()],
        };

        let size = pkt.encoded_size(99999);
        let mut buf = BytePages::default();
        pkt.encode(&mut buf, size as u32).unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(
            pkt,
            Unsubscribe::decode(&mut buf.take().unwrap().freeze()).unwrap()
        );
    }

    #[test]
    fn test_sub_pkt() {
        let pkt = Packet::Subscribe(Subscribe {
            packet_id: 12.try_into().unwrap(),
            id: None,
            user_properties: vec![("a".into(), "1".into())],
            topic_filters: vec![("test".into(), SubscriptionOptions::default())],
        });
        let codec = Codec::new();

        let mut buf = BytePages::default();
        codec.encode(pkt.clone().into(), &mut buf).unwrap();

        assert_eq!(
            pkt,
            packet(
                codec
                    .decode(&mut buf.take().unwrap().into())
                    .unwrap()
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_sub_ack() {
        let ack = SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: Vec::new(),
            reason_string: Some("some reason".into()),
            status: Vec::new(),
        };

        let size = ack.encoded_size(99999);
        let mut buf = BytePages::default();
        ack.encode(&mut buf, size as u32).unwrap();
        assert_eq!(
            ack,
            SubscribeAck::decode(&mut buf.take().unwrap().freeze()).unwrap()
        );

        let ack = SubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: vec![
                ("prop1".into(), "val1".into()),
                ("prop2".into(), "val2".into()),
            ],
            reason_string: None,
            status: vec![SubscribeAckReason::GrantedQos0],
        };
        let size = ack.encoded_size(99999);
        let mut buf = BytePages::default();
        ack.encode(&mut buf, size as u32).unwrap();
        assert_eq!(
            ack,
            SubscribeAck::decode(&mut buf.take().unwrap().freeze()).unwrap()
        );

        let ack = UnsubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: Vec::new(),
            reason_string: Some("some reason".into()),
            status: Vec::new(),
        };
        let mut buf = BytePages::default();
        let size = ack.encoded_size(99999);
        ack.encode(&mut buf, size as u32).unwrap();
        assert_eq!(
            ack,
            UnsubscribeAck::decode(&mut buf.take().unwrap().freeze()).unwrap()
        );

        let ack = UnsubscribeAck {
            packet_id: NonZeroU16::new(1).unwrap(),
            properties: vec![
                ("prop1".into(), "val1".into()),
                ("prop2".into(), "val2".into()),
            ],
            reason_string: None,
            status: vec![UnsubscribeAckReason::Success],
        };
        let size = ack.encoded_size(99999);
        let mut buf = BytePages::default();
        ack.encode(&mut buf, size as u32).unwrap();
        assert_eq!(
            ack,
            UnsubscribeAck::decode(&mut buf.take().unwrap().freeze()).unwrap()
        );
    }
}
