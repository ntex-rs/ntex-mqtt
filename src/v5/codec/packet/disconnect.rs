use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};

use crate::error::{DecodeError, EncodeError, MqttProtocolError};
use crate::utils::{self, Decode, Property};
use crate::v5::codec::{UserProperties, UserProperty, encode, property_type as pt};

/// DISCONNECT message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Disconnect {
    /// Disconnect Reason Code.
    pub reason_code: DisconnectReasonCode,
    /// Session Expiry Interval property, in seconds.
    ///
    /// `None` means the interval from the CONNECT packet is used.
    pub session_expiry_interval_secs: Option<u32>,
    /// Server Reference property, identifies another Server the Client should use.
    pub server_reference: Option<ByteString>,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
    /// User Property pairs, additional diagnostic or other information.
    pub user_properties: UserProperties,
}

prim_enum! {
    /// DISCONNECT reason codes
    pub enum DisconnectReasonCode {
        /// Close the connection normally, do not send the Will Message.
        NormalDisconnection = 0,
        /// The Client wishes to disconnect but requires that the Server also publishes its Will Message.
        DisconnectWithWillMessage = 4,
        /// The sender either does not wish to reveal the reason, or none of the other reason codes apply.
        UnspecifiedError = 128,
        /// The received packet does not conform to the specification.
        MalformedPacket = 129,
        /// An unexpected or out of order packet was received.
        ProtocolError = 130,
        /// The packet received is valid but cannot be processed by this implementation.
        ImplementationSpecificError = 131,
        /// The request is not authorized.
        NotAuthorized = 135,
        /// The Server is busy and cannot continue processing requests from this Client.
        ServerBusy = 137,
        /// The Server is shutting down.
        ServerShuttingDown = 139,
        /// The authentication method is not supported or does not match the method in use.
        BadAuthenticationMethod = 140,
        /// No packet has been received for 1.5 times the Keep Alive time.
        KeepAliveTimeout = 141,
        /// Another connection using the same Client Identifier has connected, closing this one.
        SessionTakenOver = 142,
        /// The Topic Filter is correctly formed, but is not accepted by this Server.
        TopicFilterInvalid = 143,
        /// The Topic Name is correctly formed, but is not accepted by this Client or Server.
        TopicNameInvalid = 144,
        /// More than Receive Maximum publications were received without being acknowledged.
        ReceiveMaximumExceeded = 147,
        /// A PUBLISH packet contained a Topic Alias greater than the advertised Topic Alias Maximum.
        TopicAliasInvalid = 148,
        /// The packet size is greater than the Maximum Packet Size for this Client or Server.
        PacketTooLarge = 149,
        /// The received data rate is too high.
        MessageRateTooHigh = 150,
        /// An implementation or administrative imposed limit has been exceeded.
        QuotaExceeded = 151,
        /// The connection is closed due to an administrative action.
        AdministrativeAction = 152,
        /// The payload format does not match the one specified by the Payload Format Indicator.
        PayloadFormatInvalid = 153,
        /// The Server does not support retained messages.
        RetainNotSupported = 154,
        /// The Client specified a `QoS` greater than the Maximum `QoS` from the CONNACK.
        QosNotSupported = 155,
        /// The Client should temporarily change its Server.
        UseAnotherServer = 156,
        /// The Server is moved and the Client should permanently change its server location.
        ServerMoved = 157,
        /// The Server does not support Shared Subscriptions.
        SharedSubscriptionNotSupported = 158,
        /// The connection is closed because the connection rate is too high.
        ConnectionRateExceeded = 159,
        /// The maximum connection time authorized for this connection has been exceeded.
        MaximumConnectTime = 160,
        /// The Server does not support Subscription Identifiers.
        SubscriptionIdentifiersNotSupported = 0xa1,
        /// The Server does not support Wildcard Subscriptions.
        WildcardSubscriptionsNotSupported = 162
    }
}

impl Disconnect {
    /// Create new instance of `Disconnect` with specified code
    pub fn new(reason_code: DisconnectReasonCode) -> Self {
        Self {
            reason_code,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }

    /// Create new instance of `Disconnect`, set reason from protocol error
    pub fn from_proto_error(err: &MqttProtocolError) -> Self {
        Self {
            reason_code: match err {
                MqttProtocolError::Decode(DecodeError::InvalidLength) => {
                    DisconnectReasonCode::MalformedPacket
                }
                MqttProtocolError::Decode(DecodeError::MaxSizeExceeded { .. }) => {
                    DisconnectReasonCode::PacketTooLarge
                }
                MqttProtocolError::KeepAliveTimeout => DisconnectReasonCode::KeepAliveTimeout,
                MqttProtocolError::ProtocolViolation(e) => e.reason(),
                _ => DisconnectReasonCode::ImplementationSpecificError,
            },
            ..Default::default()
        }
    }

    #[inline]
    #[must_use]
    /// Set reason string for disconnect packet
    pub fn reason_string(mut self, reason: Option<ByteString>) -> Self {
        self.reason_string = reason;
        self
    }

    #[inline]
    #[must_use]
    /// Set server reference for disconnect packet
    pub fn server_reference(mut self, reference: ByteString) -> Self {
        self.server_reference = Some(reference);
        self
    }

    #[inline]
    #[must_use]
    /// Update disconnect packet properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut UserProperties),
    {
        f(&mut self.user_properties);
        self
    }

    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        let disconnect = if src.has_remaining() {
            let reason_code = src.get_u8().try_into()?;

            if src.has_remaining() {
                let mut session_exp_secs = None;
                let mut server_reference = None;
                let mut reason_string = None;
                let mut user_properties = Vec::new();

                let prop_src = &mut utils::take_properties(src)?;
                while prop_src.has_remaining() {
                    match prop_src.get_u8() {
                        pt::SESS_EXPIRY_INT => session_exp_secs.read_value(prop_src)?,
                        pt::REASON_STRING => reason_string.read_value(prop_src)?,
                        pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                        pt::SERVER_REF => server_reference.read_value(prop_src)?,
                        _ => return Err(DecodeError::MalformedPacket),
                    }
                }
                ensure!(!src.has_remaining(), DecodeError::InvalidLength);

                Self {
                    reason_code,
                    server_reference,
                    reason_string,
                    user_properties,
                    session_expiry_interval_secs: session_exp_secs,
                }
            } else {
                Self {
                    reason_code,
                    ..Default::default()
                }
            }
        } else {
            Self::default()
        };
        Ok(disconnect)
    }
}

impl Default for Disconnect {
    fn default() -> Self {
        Self {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            session_expiry_interval_secs: None,
            server_reference: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

impl encode::EncodeLtd for Disconnect {
    fn encoded_size(&self, limit: u32) -> usize {
        const HEADER_LEN: usize = 1; // reason code

        let mut prop_len = encode::encoded_property_size(&self.session_expiry_interval_secs)
            + encode::encoded_property_size(&self.server_reference);
        let diag_len = encode::encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            encode::reduce_limit(limit, prop_len + HEADER_LEN + 4),
        ); // exclude other props and max of 4 bytes for property length value
        prop_len += diag_len;
        HEADER_LEN + encode::var_int_len(prop_len) as usize + prop_len
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        let start_len = buf.len();
        buf.put_u8(self.reason_code.into());

        let prop_len = encode::var_int_len_from_size(size - 1);
        utils::write_variable_length(prop_len, buf);
        encode::encode_property(&self.session_expiry_interval_secs, pt::SESS_EXPIRY_INT, buf)?;
        encode::encode_property(&self.server_reference, pt::SERVER_REF, buf)?;
        encode::encode_opt_props(
            &self.user_properties,
            &self.reason_string,
            buf,
            size - (buf.len() - start_len) as u32,
        )
    }
}
