use std::num::{NonZeroU16, NonZeroU32};

use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};

use crate::error::{DecodeError, EncodeError};
use crate::types::{ConnectAckFlags, QoS};
use crate::utils::{self, Decode, Encode, Property};
use crate::v5::RECEIVE_MAX_DEFAULT;
use crate::v5::codec::{UserProperties, UserProperty, encode, property_type as pt};

/// Connect acknowledgment packet
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ConnectAck {
    /// enables a Client to establish whether the Client and Server have a consistent view
    /// about whether there is already stored Session state.
    pub session_present: bool,
    /// Connect Reason Code.
    pub reason_code: ConnectAckReason,

    /// Session Expiry Interval property, in seconds.
    ///
    /// `None` means the interval requested in the CONNECT packet is used.
    pub session_expiry_interval_secs: Option<u32>,
    /// Receive Maximum property, the number of `QoS` 1 and `QoS` 2 publications
    /// the Server is willing to process concurrently.
    pub receive_max: NonZeroU16,
    /// Maximum `QoS` property, the highest `QoS` level the Server supports.
    ///
    /// Defaults to `QoS::ExactlyOnce` if the property is absent.
    pub max_qos: QoS,
    /// Maximum Packet Size property, in bytes.
    ///
    /// `None` means the Server imposes no limit beyond the protocol one.
    pub max_packet_size: Option<u32>,
    /// Assigned Client Identifier property, set when the Server generated the client id.
    pub assigned_client_id: Option<ByteString>,
    /// Topic Alias Maximum property, the highest value the Server accepts as a Topic Alias.
    ///
    /// Zero means the Server does not accept any Topic Alias.
    pub topic_alias_max: u16,
    /// Retain Available property, whether the Server supports retained messages.
    pub retain_available: bool,
    /// Wildcard Subscription Available property, whether the Server supports
    /// Wildcard Subscriptions.
    pub wildcard_subscription_available: bool,
    /// Subscription Identifiers Available property, whether the Server supports
    /// Subscription Identifiers.
    pub subscription_identifiers_available: bool,
    /// Shared Subscription Available property, whether the Server supports
    /// Shared Subscriptions.
    pub shared_subscription_available: bool,
    /// Server Keep Alive property, in seconds.
    ///
    /// `None` means the Keep Alive requested in the CONNECT packet is used.
    pub server_keepalive_sec: Option<u16>,
    /// Response Information property, used as the basis for creating a Response Topic.
    pub response_info: Option<ByteString>,
    /// Server Reference property, identifies another Server the Client should use.
    pub server_reference: Option<ByteString>,
    /// Authentication Method property, the name of the authentication method in use.
    pub auth_method: Option<ByteString>,
    /// Authentication Data property, its content is defined by the authentication method.
    pub auth_data: Option<Bytes>,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
    /// User Property pairs, additional diagnostic or other information.
    pub user_properties: UserProperties,
}

impl Default for ConnectAck {
    fn default() -> ConnectAck {
        ConnectAck {
            session_present: false,
            reason_code: ConnectAckReason::Success,
            session_expiry_interval_secs: None,
            receive_max: RECEIVE_MAX_DEFAULT,
            max_qos: QoS::ExactlyOnce,
            max_packet_size: None,
            assigned_client_id: None,
            topic_alias_max: 0,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
            server_keepalive_sec: None,
            response_info: None,
            server_reference: None,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

prim_enum! {
    /// CONNACK reason codes
    pub enum ConnectAckReason {
        /// The connection is accepted.
        Success = 0,
        /// The Server does not wish to reveal the reason for the failure, or none of the other reason codes apply.
        UnspecifiedError = 128,
        /// Data within the CONNECT packet could not be correctly parsed.
        MalformedPacket = 129,
        /// Data in the CONNECT packet does not conform to the specification.
        ProtocolError = 130,
        /// The CONNECT is valid but is not accepted by this Server.
        ImplementationSpecificError = 131,
        /// The Server does not support the version of the MQTT protocol requested by the Client.
        UnsupportedProtocolVersion = 132,
        /// The Client Identifier is a valid string but is not allowed by the Server.
        ClientIdentifierNotValid = 133,
        /// The Server does not accept the user name or password specified by the Client.
        BadUserNameOrPassword = 134,
        /// The Client is not authorized to connect.
        NotAuthorized = 135,
        /// The MQTT Server is not available.
        ServerUnavailable = 136,
        /// The Server is busy, try again later.
        ServerBusy = 137,
        /// This Client has been banned by administrative action.
        Banned = 138,
        /// The authentication method is not supported or does not match the method in use.
        BadAuthenticationMethod = 140,
        /// The Will Topic Name is not malformed, but is not accepted by this Server.
        TopicNameInvalid = 144,
        /// The CONNECT packet exceeded the maximum permissible size.
        PacketTooLarge = 149,
        /// An implementation or administrative imposed limit has been exceeded.
        QuotaExceeded = 151,
        /// The Will Payload does not match the specified Payload Format Indicator.
        PayloadFormatInvalid = 153,
        /// The Server does not support retained messages, and Will Retain was set to 1.
        RetainNotSupported = 154,
        /// The Server does not support the `QoS` set in Will `QoS`.
        QosNotSupported = 155,
        /// The Client should temporarily use another server.
        UseAnotherServer = 156,
        /// The Client should permanently use another server.
        ServerMoved = 157,
        /// The connection rate limit has been exceeded.
        ConnectionRateExceeded = 159
    }
}

impl ConnectAckReason {
    /// Human readable description of the reason code
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
        let flags =
            ConnectAckFlags::from_bits(src.get_u8()).ok_or(DecodeError::ConnAckReservedFlagSet)?;

        let reason_code = src.get_u8().try_into()?;

        let prop_src = &mut utils::take_properties(src)?;

        let mut session_expiry_interval_secs = None;
        let mut receive_max = None;
        let mut max_qos = None;
        let mut retain_available = None;
        // Maximum Packet Size of zero is a Protocol Error (MQTT 5.0, 3.2.2.3.6)
        let mut max_packet_size: Option<NonZeroU32> = None;
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
                    // Maximum QoS other than 0 or 1 is a Protocol Error (MQTT 5.0, 3.2.2.3.4),
                    // absent property means QoS 2
                    let qos = prop_src.get_u8();
                    ensure!(qos <= 1, DecodeError::MalformedPacket);
                    max_qos = Some(qos.try_into()?);
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
            receive_max: receive_max.unwrap_or(RECEIVE_MAX_DEFAULT),
            max_qos: max_qos.unwrap_or(QoS::ExactlyOnce),
            max_packet_size: max_packet_size.map(NonZeroU32::get),
            assigned_client_id,
            topic_alias_max: topic_alias_max.unwrap_or(0u16),
            retain_available: retain_available.unwrap_or(true),
            wildcard_subscription_available: wildcard_sub_avail.unwrap_or(true),
            subscription_identifiers_available: sub_ids_avail.unwrap_or(true),
            shared_subscription_available: shared_sub_avail.unwrap_or(true),
            server_keepalive_sec: server_ka_sec,
            response_info,
            server_reference,
            auth_method,
            auth_data,
            reason_string,
            user_properties,
        })
    }
}

impl encode::EncodeLtd for ConnectAck {
    fn encoded_size(&self, limit: u32) -> usize {
        const HEADER_LEN: usize = 2; // state flags byte + reason code

        let mut prop_len = encode::encoded_property_size(&self.session_expiry_interval_secs)
            + encode::encoded_property_size_default(&self.receive_max, RECEIVE_MAX_DEFAULT)
            + if self.max_qos < QoS::ExactlyOnce { 1 + 1 } else { 0 }
            + encode::encoded_property_size(&self.max_packet_size)
            + encode::encoded_property_size(&self.assigned_client_id)
            + encode::encoded_property_size_default(&self.retain_available, true)
            + encode::encoded_property_size_default(&self.wildcard_subscription_available, true)
            + encode::encoded_property_size_default(&self.subscription_identifiers_available, true)
            + encode::encoded_property_size_default(&self.shared_subscription_available, true)
            + encode::encoded_property_size(&self.server_keepalive_sec)
            + encode::encoded_property_size(&self.response_info)
            + encode::encoded_property_size(&self.server_reference)
            + encode::encoded_property_size(&self.auth_method)
            + encode::encoded_property_size(&self.auth_data);
        if self.topic_alias_max > 0 {
            prop_len += 1 + self.topic_alias_max.encoded_size(); // [property type, value..]
        }

        let diag_len = encode::encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            encode::reduce_limit(limit, HEADER_LEN + 4 + prop_len),
        ); // exclude other props and max of 4 bytes for property length value
        prop_len += diag_len;
        HEADER_LEN + encode::var_int_len(prop_len) as usize + prop_len
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        let start_len = buf.len();

        // Session Present must be 0 with a non-zero Reason Code,
        // [MQTT-3.2.2-6] (MQTT 5.0, 3.2.2.1.1)
        let session_present = self.session_present && self.reason_code == ConnectAckReason::Success;
        buf.put_slice(&[u8::from(session_present), self.reason_code.into()]);

        let prop_len = encode::var_int_len_from_size(size - 2);
        utils::write_variable_length(prop_len, buf);

        encode::encode_property(&self.session_expiry_interval_secs, pt::SESS_EXPIRY_INT, buf)?;
        encode::encode_property_default(
            &self.receive_max,
            RECEIVE_MAX_DEFAULT,
            pt::RECEIVE_MAX,
            buf,
        )?;
        if self.max_qos < QoS::ExactlyOnce {
            buf.put_slice(&[pt::MAX_QOS, self.max_qos.into()]);
        }
        encode::encode_property_default(&self.retain_available, true, pt::RETAIN_AVAIL, buf)?;
        encode::encode_property(&self.max_packet_size, pt::MAX_PACKET_SIZE, buf)?;
        encode::encode_property(&self.assigned_client_id, pt::ASSND_CLIENT_ID, buf)?;
        encode::encode_property_default(&self.topic_alias_max, 0, pt::TOPIC_ALIAS_MAX, buf)?;
        encode::encode_property_default(
            &self.wildcard_subscription_available,
            true,
            pt::WILDCARD_SUB_AVAIL,
            buf,
        )?;
        encode::encode_property_default(
            &self.subscription_identifiers_available,
            true,
            pt::SUB_IDS_AVAIL,
            buf,
        )?;
        encode::encode_property_default(
            &self.shared_subscription_available,
            true,
            pt::SHARED_SUB_AVAIL,
            buf,
        )?;
        encode::encode_property(&self.server_keepalive_sec, pt::SERVER_KA, buf)?;
        encode::encode_property(&self.response_info, pt::RESP_INFO, buf)?;
        encode::encode_property(&self.server_reference, pt::SERVER_REF, buf)?;
        encode::encode_property(&self.auth_method, pt::AUTH_METHOD, buf)?;
        encode::encode_property(&self.auth_data, pt::AUTH_DATA, buf)?;

        encode::encode_opt_props(
            &self.user_properties,
            &self.reason_string,
            buf,
            size - (buf.len() - start_len) as u32,
        )
    }
}
