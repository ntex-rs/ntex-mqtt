#![allow(clippy::struct_excessive_bools)]
use ntex_bytes::{Buf, BufMut, BytePages, ByteString, Bytes};

pub use crate::types::{ConnectAckFlags, ConnectFlags, QoS};

use super::{UserProperties, encode, property_type as pt};
use crate::error::{DecodeError, EncodeError};
use crate::types::packet_type;
use crate::utils::{Decode, Property, take_properties, write_variable_length};

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

#[derive(Debug, PartialEq, Eq, Clone)]
/// MQTT Control Packets
pub enum Packet {
    /// Client request to connect to Server
    Connect(Box<Connect>),
    /// Connect acknowledgment
    ConnectAck(Box<ConnectAck>),
    /// Publish acknowledgment
    PublishAck(PublishAck),
    /// Publish received (assured delivery part 1)
    PublishReceived(PublishAck),
    /// Publish release (assured delivery part 2)
    PublishRelease(PublishAck2),
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
    Disconnect(Box<Disconnect>),
    /// Auth exchange
    Auth(Box<Auth>),
}

impl Packet {
    /// Returns MQTT control packet type of this packet
    pub fn packet_type(&self) -> u8 {
        match self {
            Packet::Connect(_) => packet_type::CONNECT,
            Packet::ConnectAck(_) => packet_type::CONNACK,
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

impl From<Connect> for Packet {
    fn from(pkt: Connect) -> Self {
        Self::Connect(Box::new(pkt))
    }
}

impl From<Box<Connect>> for Packet {
    fn from(pkt: Box<Connect>) -> Self {
        Self::Connect(pkt)
    }
}

impl From<ConnectAck> for Packet {
    fn from(pkt: ConnectAck) -> Self {
        Self::ConnectAck(Box::new(pkt))
    }
}

impl From<Box<ConnectAck>> for Packet {
    fn from(pkt: Box<ConnectAck>) -> Self {
        Self::ConnectAck(pkt)
    }
}

impl From<PublishAck> for Packet {
    fn from(pkt: PublishAck) -> Self {
        Self::PublishAck(pkt)
    }
}

impl From<Subscribe> for Packet {
    fn from(pkt: Subscribe) -> Self {
        Self::Subscribe(pkt)
    }
}

impl From<SubscribeAck> for Packet {
    fn from(pkt: SubscribeAck) -> Self {
        Self::SubscribeAck(pkt)
    }
}

impl From<Unsubscribe> for Packet {
    fn from(pkt: Unsubscribe) -> Self {
        Self::Unsubscribe(pkt)
    }
}

impl From<UnsubscribeAck> for Packet {
    fn from(pkt: UnsubscribeAck) -> Self {
        Self::UnsubscribeAck(pkt)
    }
}

impl From<Disconnect> for Packet {
    fn from(pkt: Disconnect) -> Self {
        Self::Disconnect(Box::new(pkt))
    }
}

impl From<Box<Disconnect>> for Packet {
    fn from(pkt: Box<Disconnect>) -> Self {
        Self::Disconnect(pkt)
    }
}

impl From<Auth> for Packet {
    fn from(pkt: Auth) -> Self {
        Self::Auth(Box::new(pkt))
    }
}

impl From<Box<Auth>> for Packet {
    fn from(pkt: Box<Auth>) -> Self {
        Self::Auth(pkt)
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

#[allow(clippy::ref_option, clippy::wildcard_imports)]
mod ack_props {
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

        let len = encode::encoded_size_opt_props(properties, reason_string, limit - 4);
        encode::var_int_len(len) as usize + len
    }

    pub(crate) fn encode(
        properties: &[UserProperty],
        reason_string: &Option<ByteString>,
        buf: &mut BytePages,
        size: u32,
    ) -> Result<(), EncodeError> {
        debug_assert!(size > 0); // formalize in signature?

        if size == 1 {
            // empty properties
            buf.put_u8(0);
            return Ok(());
        }

        let size = encode::var_int_len_from_size(size);
        write_variable_length(size, buf);
        encode::encode_opt_props(properties, reason_string, buf, size)
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

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};

    use ntex_bytes::BytesMut;
    use ntex_codec::{Decoder, Encoder};

    use super::*;
    use crate::v5::codec::{Codec, Decoded, Encoded};

    fn pid(v: u16) -> NonZeroU16 {
        NonZeroU16::new(v).unwrap()
    }

    fn props() -> UserProperties {
        vec![("k".into(), "v".into()), ("k".into(), "v2".into())]
    }

    fn roundtrip(pkt: Packet) {
        let codec = Codec::new();
        let mut buf = BytePages::default();
        let expected = pkt.clone();
        codec.encode(Encoded::Packet(pkt), &mut buf).unwrap();
        let encoded = buf.freeze();
        let mut src = BytesMut::copy_from_slice(&encoded);
        match codec.decode(&mut src).unwrap().unwrap() {
            Decoded::Packet(decoded, _) => assert_eq!(decoded, expected),
            other => panic!("unexpected {other:?}"),
        }
        assert!(src.is_empty());
    }

    fn decode(first: u8, body: &[u8]) -> Result<Packet, DecodeError> {
        let mut src = BytesMut::new();
        src.extend_from_slice(&[first, u8::try_from(body.len()).unwrap()]);
        src.extend_from_slice(body);
        Codec::new().decode(&mut src).map(|p| match p.unwrap() {
            Decoded::Packet(p, _) => p,
            other => panic!("unexpected {other:?}"),
        })
    }

    fn connect() -> Connect {
        Connect {
            clean_start: true,
            keep_alive: 30,
            session_expiry_interval_secs: 120,
            auth_method: Some("m".into()),
            auth_data: Some(Bytes::from_static(b"d")),
            request_problem_info: false,
            request_response_info: true,
            receive_max: Some(pid(10)),
            topic_alias_max: 5,
            user_properties: props(),
            max_packet_size: NonZeroU32::new(1024),
            last_will: Some(LastWill {
                qos: QoS::AtLeastOnce,
                retain: true,
                topic: "will".into(),
                message: Bytes::from_static(b"bye"),
                will_delay_interval_sec: Some(3),
                correlation_data: Some(Bytes::from_static(b"c")),
                message_expiry_interval: Some(4),
                content_type: Some("text".into()),
                user_properties: props(),
                is_utf8_payload: Some(true),
                response_topic: Some("resp".into()),
            }),
            client_id: "cid".into(),
            username: Some("user".into()),
            password: Some(Bytes::from_static(b"pwd")),
        }
    }

    fn connack() -> ConnectAck {
        ConnectAck {
            session_present: true,
            reason_code: ConnectAckReason::Success,
            session_expiry_interval_secs: Some(1),
            receive_max: pid(7),
            max_qos: QoS::AtLeastOnce,
            max_packet_size: Some(2048),
            assigned_client_id: Some("assigned".into()),
            topic_alias_max: 3,
            retain_available: false,
            wildcard_subscription_available: false,
            subscription_identifiers_available: false,
            shared_subscription_available: false,
            server_keepalive_sec: Some(15),
            response_info: Some("info".into()),
            server_reference: Some("srv".into()),
            auth_method: Some("m".into()),
            auth_data: Some(Bytes::from_static(b"d")),
            reason_string: Some("ok".into()),
            user_properties: props(),
        }
    }

    #[test]
    fn packet_type_and_from() {
        let ack = PublishAck {
            packet_id: pid(1),
            reason_code: PublishAckReason::Success,
            properties: Vec::new(),
            reason_string: None,
        };
        let ack2 = PublishAck2 {
            packet_id: pid(1),
            reason_code: PublishAck2Reason::Success,
            properties: Vec::new(),
            reason_string: None,
        };
        let sub = Subscribe {
            packet_id: pid(1),
            id: None,
            user_properties: Vec::new(),
            topic_filters: vec![("t".into(), SubscriptionOptions::default())],
        };
        let suback = SubscribeAck {
            packet_id: pid(1),
            properties: Vec::new(),
            reason_string: None,
            status: vec![SubscribeAckReason::GrantedQos0],
        };
        let unsub = Unsubscribe {
            packet_id: pid(1),
            user_properties: Vec::new(),
            topic_filters: vec!["t".into()],
        };
        let unsuback = UnsubscribeAck {
            packet_id: pid(1),
            properties: Vec::new(),
            reason_string: None,
            status: vec![UnsubscribeAckReason::Success],
        };
        let cases: Vec<(Packet, u8)> = vec![
            (Connect::default().into(), packet_type::CONNECT),
            (Box::new(Connect::default()).into(), packet_type::CONNECT),
            (ConnectAck::default().into(), packet_type::CONNACK),
            (Box::new(ConnectAck::default()).into(), packet_type::CONNACK),
            (ack.clone().into(), packet_type::PUBACK),
            (Packet::PublishReceived(ack), packet_type::PUBREC),
            (Packet::PublishRelease(ack2.clone()), packet_type::PUBREL),
            (Packet::PublishComplete(ack2), packet_type::PUBCOMP),
            (sub.into(), packet_type::SUBSCRIBE),
            (suback.into(), packet_type::SUBACK),
            (unsub.into(), packet_type::UNSUBSCRIBE),
            (unsuback.into(), packet_type::UNSUBACK),
            (Packet::PingRequest, packet_type::PINGREQ),
            (Packet::PingResponse, packet_type::PINGRESP),
            (Disconnect::default().into(), packet_type::DISCONNECT),
            (
                Box::new(Disconnect::default()).into(),
                packet_type::DISCONNECT,
            ),
            (Auth::default().into(), packet_type::AUTH),
            (Box::new(Auth::default()).into(), packet_type::AUTH),
        ];
        for (pkt, ty) in cases {
            assert_eq!(pkt.packet_type(), ty, "{pkt:?}");
            roundtrip(pkt);
        }
    }

    #[test]
    fn roundtrip_all_properties() {
        roundtrip(connect().into());
        let mut c = connect();
        c.last_will = None;
        c.username = None;
        c.password = None;
        c.auth_data = None;
        roundtrip(c.into());

        roundtrip(connack().into());
        roundtrip(
            ConnectAck {
                reason_code: ConnectAckReason::NotAuthorized,
                session_present: false,
                max_qos: QoS::AtMostOnce,
                ..ConnectAck::default()
            }
            .into(),
        );

        roundtrip(
            Disconnect {
                reason_code: DisconnectReasonCode::ServerMoved,
                session_expiry_interval_secs: Some(9),
                server_reference: Some("srv".into()),
                reason_string: Some("moved".into()),
                user_properties: props(),
            }
            .into(),
        );
        roundtrip(Disconnect::new(DisconnectReasonCode::ServerBusy).into());

        roundtrip(
            Auth {
                reason_code: AuthReasonCode::ContinueAuth,
                auth_method: Some("m".into()),
                auth_data: Some(Bytes::from_static(b"d")),
                reason_string: Some("r".into()),
                user_properties: props(),
            }
            .into(),
        );
        roundtrip(
            Auth {
                reason_code: AuthReasonCode::ReAuth,
                auth_method: Some("m".into()),
                ..Auth::default()
            }
            .into(),
        );

        roundtrip(Packet::PublishReceived(PublishAck {
            packet_id: pid(2),
            reason_code: PublishAckReason::QuotaExceeded,
            properties: props(),
            reason_string: Some("q".into()),
        }));
        roundtrip(Packet::PublishComplete(PublishAck2 {
            packet_id: pid(3),
            reason_code: PublishAck2Reason::PacketIdNotFound,
            properties: props(),
            reason_string: Some("nf".into()),
        }));
    }

    #[test]
    fn connect_builders() {
        let c = Connect::default().client_id("id").receive_max(5);
        assert_eq!(c.client_id, "id");
        assert_eq!(c.receive_max, Some(pid(5)));
        assert_eq!(c.receive_max(0).receive_max, None);
    }

    #[test]
    fn connack_reason() {
        use ConnectAckReason::*;

        for (code, txt) in [
            (Success, "Connection Accepted"),
            (
                UnsupportedProtocolVersion,
                "protocol version is not supported",
            ),
            (ClientIdentifierNotValid, "client identifier is invalid"),
            (ServerUnavailable, "Server unavailable"),
            (BadUserNameOrPassword, "bad user name or password"),
            (NotAuthorized, "not authorized"),
            (Banned, "Connection Refused"),
        ] {
            assert_eq!(code.reason(), txt);
        }
    }

    #[test]
    fn disconnect_builders() {
        let d = Disconnect::new(DisconnectReasonCode::AdministrativeAction)
            .reason_string(Some("r".into()))
            .server_reference("s".into())
            .properties(|p| p.push(("a".into(), "b".into())));
        assert_eq!(d.reason_string.as_deref(), Some("r"));
        assert_eq!(d.server_reference.as_deref(), Some("s"));
        assert_eq!(d.user_properties, vec![("a".into(), "b".into())]);
    }

    #[test]
    fn disconnect_from_proto_error() {
        use crate::error::{MqttProtocolError as E, SpecViolation};
        use DisconnectReasonCode as R;

        for (err, code) in [
            (E::Decode(DecodeError::InvalidLength), R::MalformedPacket),
            (
                E::Decode(DecodeError::MaxSizeExceeded {
                    size: 2,
                    max_size: 1,
                }),
                R::PacketTooLarge,
            ),
            (E::KeepAliveTimeout, R::KeepAliveTimeout),
            (E::spec(SpecViolation::Connack_3_2_2_11), R::QosNotSupported),
            (E::ReadTimeout, R::ImplementationSpecificError),
            (
                E::Decode(DecodeError::MalformedPacket),
                R::ImplementationSpecificError,
            ),
        ] {
            assert_eq!(
                Disconnect::from_proto_error(&err).reason_code,
                code,
                "{err:?}"
            );
        }
    }

    #[test]
    fn decode_errors() {
        use crate::types::packet_type as t;

        // unknown property id
        for (first, body) in [
            (t::DISCONNECT, &[0x80, 2, 0x01, 0][..]),
            (t::CONNACK, &[0, 0, 2, 0x01, 0][..]),
            (t::PUBACK, &[0, 1, 0, 2, 0x01, 0][..]),
            (t::AUTH, &[0x18, 2, 0x01, 0][..]),
        ] {
            assert_eq!(
                decode(first, body).err(),
                Some(DecodeError::MalformedPacket)
            );
        }

        // trailing data after properties
        assert_eq!(
            decode(t::DISCONNECT, &[0x80, 0, 1]).err(),
            Some(DecodeError::InvalidLength)
        );
        // invalid reason code
        assert!(decode(t::DISCONNECT, &[3]).is_err());

        // short forms
        assert_eq!(
            decode(t::DISCONNECT, &[]).unwrap(),
            Packet::Disconnect(Box::default())
        );
        assert_eq!(
            decode(t::DISCONNECT, &[0x8B]).unwrap(),
            Disconnect::new(DisconnectReasonCode::ServerShuttingDown).into()
        );
    }

    #[test]
    fn auth_without_method_rejected() {
        let mut buf = BytePages::default();
        let auth = Auth {
            reason_code: AuthReasonCode::ReAuth,
            ..Auth::default()
        };
        assert_eq!(
            Codec::new().encode(Encoded::Packet(auth.into()), &mut buf),
            Err(EncodeError::MalformedPacket)
        );
    }

    #[test]
    fn connack_decode_errors() {
        use crate::types::packet_type as t;

        // max qos set twice, qos 2, missing value
        for body in [
            &[0, 0, 4, pt::MAX_QOS, 0, pt::MAX_QOS, 0][..],
            &[0, 0, 2, pt::MAX_QOS, 2][..],
            &[0, 0, 1, pt::MAX_QOS][..],
        ] {
            assert!(decode(t::CONNACK, body).is_err(), "{body:?}");
        }
    }
}
