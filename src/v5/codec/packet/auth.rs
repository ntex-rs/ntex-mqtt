use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};

use crate::error::{DecodeError, EncodeError};
use crate::utils::{self, Decode, Property};
use crate::v5::codec::{UserProperties, UserProperty, encode, property_type as pt};

/// AUTH message
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Auth {
    /// Authenticate Reason Code.
    pub reason_code: AuthReasonCode,
    /// Authentication Method property, the name of the authentication method in use.
    ///
    /// It is a Protocol Error to omit it, so it is always set on a decoded packet.
    pub auth_method: Option<ByteString>,
    /// Authentication Data property, its content is defined by the authentication method.
    pub auth_data: Option<Bytes>,
    /// Reason String property, a human readable string designed for diagnostics.
    pub reason_string: Option<ByteString>,
    /// User Property pairs, additional diagnostic or other information.
    pub user_properties: UserProperties,
}

prim_enum! {
    /// AUTH reason codes
    pub enum AuthReasonCode {
        /// Authentication is successful, sent by the Server.
        Success = 0,
        /// Continue the authentication with another step.
        ContinueAuth = 24,
        /// Initiate a re-authentication, sent by the Client.
        ReAuth = 25
    }
}

impl Auth {
    pub(crate) fn decode(src: &mut Bytes) -> Result<Self, DecodeError> {
        // Reason Code and Property Length are omitted for Success without properties,
        // the AUTH has a Remaining Length of 0 (MQTT 5.0, 3.15.2.1)
        if !src.has_remaining() {
            return Ok(Self::default());
        }

        let reason_code = src.get_u8().try_into()?;
        let mut auth_method = None;
        let mut auth_data = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();

        if src.has_remaining() {
            let prop_src = &mut utils::take_properties(src)?;
            while prop_src.has_remaining() {
                match prop_src.get_u8() {
                    pt::AUTH_METHOD => auth_method.read_value(prop_src)?,
                    pt::AUTH_DATA => auth_data.read_value(prop_src)?,
                    pt::REASON_STRING => reason_string.read_value(prop_src)?,
                    pt::USER => user_properties.push(UserProperty::decode(prop_src)?),
                    _ => return Err(DecodeError::MalformedPacket),
                }
            }
            ensure!(!src.has_remaining(), DecodeError::InvalidLength);
        }
        // omitting Authentication Method is a Protocol Error (MQTT 5.0, 3.15.2.2.2)
        ensure!(auth_method.is_some(), DecodeError::MalformedPacket);

        Ok(Self {
            reason_code,
            auth_method,
            auth_data,
            reason_string,
            user_properties,
        })
    }

    /// Success without properties is encoded with a Remaining Length of 0,
    /// (MQTT 5.0, 3.15.2.1)
    pub(crate) fn is_short_form(&self) -> bool {
        self.reason_code == AuthReasonCode::Success
            && self.auth_method.is_none()
            && self.auth_data.is_none()
            && self.reason_string.is_none()
            && self.user_properties.is_empty()
    }
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            reason_code: AuthReasonCode::Success,
            auth_method: None,
            auth_data: None,
            reason_string: None,
            user_properties: Vec::new(),
        }
    }
}

impl encode::EncodeLtd for Auth {
    fn encoded_size(&self, limit: u32) -> usize {
        const HEADER_LEN: usize = 1; // reason code

        if self.is_short_form() {
            return 0;
        }

        let mut prop_len = encode::encoded_property_size(&self.auth_method)
            + encode::encoded_property_size(&self.auth_data);
        let diag_len = encode::encoded_size_opt_props(
            &self.user_properties,
            &self.reason_string,
            encode::reduce_limit(limit, prop_len + HEADER_LEN + 4),
        ); // exclude other props and max of 4 bytes for property length value
        prop_len += diag_len;
        HEADER_LEN + encode::var_int_len(prop_len) as usize + prop_len
    }

    fn encode(&self, buf: &mut BytePages, size: u32) -> Result<(), EncodeError> {
        if self.is_short_form() {
            return Ok(());
        }
        let start_len = buf.len();
        buf.put_u8(self.reason_code.into());

        let prop_len = encode::var_int_len_from_size(size - 1);
        utils::write_variable_length(prop_len, buf);
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
