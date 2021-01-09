use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_more::From;

pub use crate::types::{ConnectAckFlags, ConnectFlags, QoS};

use super::{encode::*, property_type as pt, UserProperties};
use crate::error::{DecodeError, EncodeError};
use crate::types::packet_type;
use crate::utils::{take_properties, write_variable_length, Decode, Property};

mod auth;
mod connack;
mod connect;
mod disconnect;
mod pubacks;
mod publish;
mod subscribe;

pub use auth::*;
pub use connack::*;
pub use connect::*;
pub use disconnect::*;
pub use pubacks::*;
pub use publish::*;
pub use subscribe::*;

#[derive(Debug, PartialEq, Clone, From)]
/// MQTT Control Packets
pub enum Packet {
    /// Client request to connect to Server
    Connect(Connect),
    /// Connect acknowledgment
    ConnectAck(ConnectAck),
    /// Publish message
    Publish(Publish),
    /// Publish acknowledgment
    PublishAck(PublishAck),
    #[from(ignore)]
    /// Publish received (assured delivery part 1)
    PublishReceived(PublishAck),
    /// Publish release (assured delivery part 2)
    PublishRelease(PublishAck2),
    #[from(ignore)]
    /// Publish complete (assured delivery part 3)
    PublishComplete(PublishAck2),
    /// Client subscribe request
    Subscribe(Subscribe),
    /// Subscribe acknowledgment
    SubscribeAck(SubscribeAck),
    /// Unsubscribe request
    Unsubscribe(Unsubscribe),
    /// Unsubscribe acknowledgment
    UnsubscribeAck(UnsubscribeAck),
    /// PING request
    PingRequest,
    /// PING response
    PingResponse,
    /// Disconnection is advertised
    Disconnect(Disconnect),
    /// Auth exchange
    Auth(Auth),
}

impl Packet {
    pub fn packet_type(&self) -> u8 {
        match self {
            Packet::Connect(_) => packet_type::CONNECT,
            Packet::ConnectAck(_) => packet_type::CONNACK,
            Packet::Publish(_) => packet_type::PUBLISH_START,
            Packet::PublishAck(_) => packet_type::PUBACK,
            Packet::PublishReceived(_) => packet_type::PUBREC,
            Packet::PublishRelease(_) => packet_type::PUBREL,
            Packet::PublishComplete(_) => packet_type::PUBCOMP,
            Packet::Subscribe(_) => packet_type::SUBSCRIBE,
            Packet::SubscribeAck(_) => packet_type::SUBACK,
            Packet::Unsubscribe(_) => packet_type::UNSUBSCRIBE,
            Packet::UnsubscribeAck(_) => packet_type::UNSUBACK,
            Packet::PingRequest => packet_type::PINGREQ,
            Packet::PingResponse => packet_type::PINGRESP,
            Packet::Disconnect(_) => packet_type::DISCONNECT,
            Packet::Auth(_) => packet_type::AUTH,
        }
    }
}

pub(super) mod property_type {
    pub(crate) const UTF8_PAYLOAD: u8 = 0x01;
    pub(crate) const MSG_EXPIRY_INT: u8 = 0x02;
    pub(crate) const CONTENT_TYPE: u8 = 0x03;
    pub(crate) const RESP_TOPIC: u8 = 0x08;
    pub(crate) const CORR_DATA: u8 = 0x09;
    pub(crate) const SUB_ID: u8 = 0x0B;
    pub(crate) const SESS_EXPIRY_INT: u8 = 0x11;
    pub(crate) const ASSND_CLIENT_ID: u8 = 0x12;
    pub(crate) const SERVER_KA: u8 = 0x13;
    pub(crate) const AUTH_METHOD: u8 = 0x15;
    pub(crate) const AUTH_DATA: u8 = 0x16;
    pub(crate) const REQ_PROB_INFO: u8 = 0x17;
    pub(crate) const WILL_DELAY_INT: u8 = 0x18;
    pub(crate) const REQ_RESP_INFO: u8 = 0x19;
    pub(crate) const RESP_INFO: u8 = 0x1A;
    pub(crate) const SERVER_REF: u8 = 0x1C;
    pub(crate) const REASON_STRING: u8 = 0x1F;
    pub(crate) const RECEIVE_MAX: u8 = 0x21;
    pub(crate) const TOPIC_ALIAS_MAX: u8 = 0x22;
    pub(crate) const TOPIC_ALIAS: u8 = 0x23;
    pub(crate) const MAX_QOS: u8 = 0x24;
    pub(crate) const RETAIN_AVAIL: u8 = 0x25;
    pub(crate) const USER: u8 = 0x26;
    pub(crate) const MAX_PACKET_SIZE: u8 = 0x27;
    pub(crate) const WILDCARD_SUB_AVAIL: u8 = 0x28;
    pub(crate) const SUB_IDS_AVAIL: u8 = 0x29;
    pub(crate) const SHARED_SUB_AVAIL: u8 = 0x2A;
}

mod ack_props {
    use bytestring::ByteString;

    use super::*;
    use crate::v5::codec::UserProperty;

    pub(crate) fn encoded_size(
        properties: &[UserProperty],
        reason_string: &Option<ByteString>,
        limit: u32,
    ) -> usize {
        if limit < 4 {
            // todo: not really needed in practice
            return 1; // 1 byte to encode property length = 0
        }

        let len = encoded_size_opt_props(properties, reason_string, limit - 4);
        var_int_len(len) as usize + len
    }

    pub(crate) fn encode(
        properties: &[UserProperty],
        reason_string: &Option<ByteString>,
        buf: &mut BytesMut,
        size: u32,
    ) -> Result<(), EncodeError> {
        debug_assert!(size > 0); // formalize in signature?

        if size == 1 {
            // empty properties
            buf.put_u8(0);
            return Ok(());
        }

        let size = var_int_len_from_size(size);
        write_variable_length(size, buf);
        encode_opt_props(properties, reason_string, buf, size)
    }

    /// Parses ACK properties (User and Reason String properties) from `src`
    pub(crate) fn decode(
        src: &mut Bytes,
    ) -> Result<(UserProperties, Option<ByteString>), DecodeError> {
        let prop_src = &mut take_properties(src)?;
        let mut reason_string = None;
        let mut user_props = Vec::new();
        while prop_src.has_remaining() {
            let prop_id = prop_src.get_u8();
            match prop_id {
                pt::REASON_STRING => reason_string.read_value(prop_src)?,
                pt::USER => user_props.push(<(ByteString, ByteString)>::decode(prop_src)?),
                _ => return Err(DecodeError::MalformedPacket),
            }
        }

        Ok((user_props, reason_string))
    }
}
