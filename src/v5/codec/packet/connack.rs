use ntex::util::{Buf, BufMut, ByteString, Bytes, BytesMut};
use std::{convert::TryInto, num::NonZeroU16};

use crate::error::{DecodeError, EncodeError};
use crate::types::{ConnectAckFlags, QoS};
use crate::utils::{self, Decode, Encode, Property};
use crate::v5::codec::{encode::*, property_type as pt, UserProperties, UserProperty};

/// Connect acknowledgment packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConnectAck {
    /// enables a Client to establish whether the Client and Server have a consistent view
    /// about whether there is already stored Session state.
    pub session_present: bool,
    pub reason_code: ConnectAckReason,

    pub session_expiry_interval_secs: Option<u32>,
    pub receive_max: Option<NonZeroU16>,
    pub max_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_id: Option<ByteString>,
    pub topic_alias_max: u16,
    pub reason_string: Option<ByteString>,
    pub user_properties: UserProperties,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keepalive_sec: Option<u16>,
    pub response_info: Option<ByteString>,
    pub server_reference: Option<ByteString>,
    pub auth_method: Option<ByteString>,
    pub auth_data: Option<Bytes>,
}

impl Default for ConnectAck {
    fn default() -> ConnectAck {
        ConnectAck {
            session_present: false,
            reason_code: ConnectAckReason::Success,
            session_expiry_interval_secs: None,
            receive_max: None,
            max_qos: None,
            retain_available: None,
            max_packet_size: None,
            assigned_client_id: None,
            topic_alias_max: 0,
            reason_string: None,
            user_properties: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keepalive_sec: None,
            response_info: None,
            server_reference: None,
            auth_method: None,
            auth_data: None,
        }
    }
}

prim_enum! {
    /// CONNACK reason codes
    pub enum ConnectAckReason {
        Success = 0,
        UnspecifiedError = 128,
        MalformedPacket = 129,
        ProtocolError = 130,
        ImplementationSpecificError = 131,
        UnsupportedProtocolVersion = 132,
        ClientIdentifierNotValid = 133,
        BadUserNameOrPassword = 134,
        NotAuthorized = 135,
        ServerUnavailable = 136,
        ServerBusy = 137,
        Banned = 138,
        BadAuthenticationMethod = 140,
        TopicNameInvalid = 144,
        PacketTooLarge = 149,
        QuotaExceeded = 151,
        PayloadFormatInvalid = 153,
        RetainNotSupported = 154,
        QosNotSupported = 155,
        UseAnotherServer = 156,
        ServerMoved = 157,
        ConnectionRateExceeded = 159
    }
}

impl ConnectAckReason {
    pub fn reason(self) -> &'static str {
        match self {
            ConnectAckReason::Success => "Connection Accepted",
            ConnectAckReason::UnsupportedProtocolVersion => "protocol version is not supported",
            ConnectAckReason::ClientIdentifierNotValid => "client identifier is invalid",
            ConnectAckReason::ServerUnavailable => "Server unavailable",
            ConnectAckReason::BadUserNameOrPassword => "bad user name or password",
            ConnectAckReason::NotAuthorized => "not authorized",
            _ => "Connection Refused",
        }
    }
}

impl ConnectAck {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        ensure!(src.remaining() >= 2, DecodeError::InvalidLength);
        let flags = ConnectAckFlags::from_bits(src.get_u8())
            .ok_or(DecodeError::ConnAckReservedFlagSet)?;

        let reason_code = src.get_u8().try_into()?;

        let prop_src = &mut utils::take_properties(src)?;

        let mut session_expiry_interval_secs = None;
        let mut receive_max = None;
        let mut max_qos = None;
        let mut retain_available = None;
        let mut max_packet_size = None;
        let mut assigned_client_id = None;
        let mut topic_alias_max = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut wildcard_sub_avail = None;
        let mut sub_ids_avail = None;
        let mut shared_sub_avail = None;
        let mut server_ka_sec = None;
        let mut response_info = None;
        let mut server_reference = None;
        let mut auth_method = None;
        let mut auth_data = None;
        while prop_src.has_remaining() {
            match prop_src.get_u8() {
                pt::SESS_EXPIRY_INT => session_expiry_interval_secs.read_value(prop_src)?,
                pt::RECEIVE_MAX => receive_max.read_value(prop_src)?,
                pt::MAX_QOS => {
                    ensure!(max_qos.is_none(), DecodeError::MalformedPacket); // property is set twice while not allowed
                    ensure!(prop_src.has_remaining(), DecodeError::InvalidLength);
                    max_qos = Some(prop_src.get_u8().try_into()?);
                }
                pt::RETAIN_AVAIL => retain_available.read_value(prop_src)?,
                pt::MAX_PACKET_SIZE => max_packet_size.read_value(prop_src)?,
                pt::ASSND_CLIENT_ID => assigned_client_id.read_value(prop_src)?,
                pt::TOPIC_ALIAS_MAX => topic_alias_max.read_value(prop_src)?,
                pt::REASON_STRING => reason_string.read_value(prop_src)?,
                pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                pt::WILDCARD_SUB_AVAIL => wildcard_sub_avail.read_value(prop_src)?,
                pt::SUB_IDS_AVAIL => sub_ids_avail.read_value(prop_src)?,
                pt::SHARED_SUB_AVAIL => shared_sub_avail.read_value(prop_src)?,
                pt::SERVER_KA => server_ka_sec.read_value(prop_src)?,
                pt::RESP_INFO => response_info.read_value(prop_src)?,
                pt::SERVER_REF => server_reference.read_value(prop_src)?,
                pt::AUTH_METHOD => auth_method.read_value(prop_src)?,
                pt::AUTH_DATA => auth_data.read_value(prop_src)?,
                _ => return Err(DecodeError::MalformedPacket),
            }
        }
        ensure!(!src.has_remaining(), DecodeError::InvalidLength);

        Ok(ConnectAck {
            session_present: flags.contains(ConnectAckFlags::SESSION_PRESENT),
            reason_code,
            session_expiry_interval_secs,
            receive_max,
            max_qos,
            retain_available,
            max_packet_size,
            assigned_client_id,
            topic_alias_max: topic_alias_max.unwrap_or(0u16),
            reason_string,
            user_properties,
            wildcard_subscription_available: wildcard_sub_avail,
            subscription_identifiers_available: sub_ids_avail,
            shared_subscription_available: shared_sub_avail,
            server_keepalive_sec: server_ka_sec,
            response_info,
            server_reference,
            auth_method,
            auth_data,
        })
    }
}

impl EncodeLtd for ConnectAck {
    fn encoded_size(&self, limit: u32) -> usize {
        const HEADER_LEN: usize = 2; // state flags byte + reason code

        let mut prop_len = encoded_property_size(&self.session_expiry_interval_secs)
            + encoded_property_size(&self.receive_max)
            + self.max_qos.map_or(0, |_| 1 + 1) // [property type, value]
            + encoded_property_size(&self.retain_available)
            + encoded_property_size(&self.max_packet_size)
            + encoded_property_size(&self.assigned_client_id)
            + encoded_property_size(&self.wildcard_subscription_available)
            + encoded_property_size(&self.subscription_identifiers_available)
            + encoded_property_size(&self.shared_subscription_available)
            + encoded_property_size(&self.server_keepalive_sec)
            + encoded_property_size(&self.response_info)
            + encoded_property_size(&self.server_reference)
            + encoded_property_size(&self.auth_method)
            + encoded_property_size(&self.auth_data);
        if self.topic_alias_max > 0 {
            prop_len += 1 + self.topic_alias_max.encoded_size(); // [property type, value..]
        }

        let diag_len = encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            reduce_limit(limit, HEADER_LEN + 4 + prop_len),
        ); // exclude other props and max of 4 bytes for property length value
        prop_len += diag_len;
        HEADER_LEN + var_int_len(prop_len) as usize + prop_len
    }

    fn encode(&self, buf: &mut BytesMut, size: u32) -> Result<(), EncodeError> {
        // todo: move upstream: write_variable_length(size, buf);
        let start_len = buf.len();

        buf.put_slice(&[
            if self.session_present { 0x01 } else { 0x00 },
            self.reason_code.into(),
        ]);

        let prop_len = var_int_len_from_size(size - 2);
        utils::write_variable_length(prop_len, buf);

        encode_property(&self.session_expiry_interval_secs, pt::SESS_EXPIRY_INT, buf)?;
        encode_property(&self.receive_max, pt::RECEIVE_MAX, buf)?;
        if let Some(max_qos) = self.max_qos {
            buf.put_slice(&[pt::MAX_QOS, max_qos.into()]);
        }
        encode_property(&self.retain_available, pt::RETAIN_AVAIL, buf)?;
        encode_property(&self.max_packet_size, pt::MAX_PACKET_SIZE, buf)?;
        encode_property(&self.assigned_client_id, pt::ASSND_CLIENT_ID, buf)?;
        if self.topic_alias_max > 0 {
            buf.put_u8(pt::TOPIC_ALIAS_MAX);
            self.topic_alias_max.encode(buf)?;
        }
        encode_property(&self.wildcard_subscription_available, pt::WILDCARD_SUB_AVAIL, buf)?;
        encode_property(&self.subscription_identifiers_available, pt::SUB_IDS_AVAIL, buf)?;
        encode_property(&self.shared_subscription_available, pt::SHARED_SUB_AVAIL, buf)?;
        encode_property(&self.server_keepalive_sec, pt::SERVER_KA, buf)?;
        encode_property(&self.response_info, pt::RESP_INFO, buf)?;
        encode_property(&self.server_reference, pt::SERVER_REF, buf)?;
        encode_property(&self.auth_method, pt::AUTH_METHOD, buf)?;
        encode_property(&self.auth_data, pt::AUTH_DATA, buf)?;

        encode_opt_props(
            &self.user_properties,
            &self.reason_string,
            buf,
            size - (buf.len() - start_len) as u32,
        )
    }
}
