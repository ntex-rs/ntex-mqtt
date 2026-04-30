use ntex_bytes::{Buf, BufMut, ByteString, Bytes, BytesMut};

use crate::error::{DecodeError, EncodeError, ProtocolError};
use crate::utils::{self, Decode, Property};
use crate::v5::codec::{UserProperties, UserProperty, encode, property_type as pt};

/// DISCONNECT message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Disconnect {
    pub reason_code: DisconnectReasonCode,
    pub session_expiry_interval_secs: Option<u32>,
    pub server_reference: Option<ByteString>,
    pub reason_string: Option<ByteString>,
    pub user_properties: UserProperties,
}

prim_enum! {
    /// DISCONNECT reason codes
    pub enum DisconnectReasonCode {
        NormalDisconnection = 0,
        DisconnectWithWillMessage = 4,
        UnspecifiedError = 128,
        MalformedPacket = 129,
        ProtocolError = 130,
        ImplementationSpecificError = 131,
        NotAuthorized = 135,
        ServerBusy = 137,
        ServerShuttingDown = 139,
        BadAuthenticationMethod = 140,
        KeepAliveTimeout = 141,
        SessionTakenOver = 142,
        TopicFilterInvalid = 143,
        TopicNameInvalid = 144,
        ReceiveMaximumExceeded = 147,
        TopicAliasInvalid = 148,
        PacketTooLarge = 149,
        MessageRateTooHigh = 150,
        QuotaExceeded = 151,
        AdministrativeAction = 152,
        PayloadFormatInvalid = 153,
        RetainNotSupported = 154,
        QosNotSupported = 155,
        UseAnotherServer = 156,
        ServerMoved = 157,
        SharedSubscriptionNotSupported = 158,
        ConnectionRateExceeded = 159,
        MaximumConnectTime = 160,
        SubscriptionIdentifiersNotSupported = 0xa1,
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
    pub fn from_proto_error(err: &ProtocolError) -> Self {
        Self {
            reason_code: match err {
                ProtocolError::Decode(DecodeError::InvalidLength) => {
                    DisconnectReasonCode::MalformedPacket
                }
                ProtocolError::Decode(DecodeError::MaxSizeExceeded { .. }) => {
                    DisconnectReasonCode::PacketTooLarge
                }
                ProtocolError::KeepAliveTimeout => DisconnectReasonCode::KeepAliveTimeout,
                ProtocolError::ProtocolViolation(e) => e.reason(),
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
                Self { reason_code, ..Default::default() }
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

    fn encode(&self, buf: &mut BytesMut, size: u32) -> Result<(), EncodeError> {
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
