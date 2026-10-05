use ntex_bytes::{BytePages, BytesMut};
use ntex_codec::{Decoder, Encoder};

use crate::error::{DecodeError, EncodeError};
use crate::types::{MQTT, MQTT_LEVEL_3, MQTT_LEVEL_5, packet_type};
use crate::utils;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ProtocolVersion {
    MQTT3,
    MQTT5,
}

#[derive(Debug)]
pub(super) struct VersionCodec;

/// Reads the protocol level of a CONNECT packet without consuming it
pub(crate) fn peek_connect_level(src: &[u8]) -> Result<Option<u8>, DecodeError> {
    let len = src.len();
    if len < 2 {
        return Ok(None);
    }

    match utils::decode_variable_length(&src[1..])? {
        Some((_, mut consumed)) => {
            consumed += 1;

            if src[0] == packet_type::CONNECT {
                if len <= consumed + 6 {
                    return Ok(None);
                }

                let len = u16::from_be_bytes(src[consumed..consumed + 2].try_into().unwrap());
                ensure!(
                    len == 4 && &src[consumed + 2..consumed + 6] == MQTT,
                    DecodeError::InvalidProtocol
                );
                Ok(Some(src[consumed + 6]))
            } else {
                Err(DecodeError::UnsupportedPacketType)
            }
        }
        None => Ok(None),
    }
}

impl Decoder for VersionCodec {
    type Item = ProtocolVersion;
    type Error = DecodeError;

    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, DecodeError> {
        match peek_connect_level(src)? {
            Some(MQTT_LEVEL_3) => Ok(Some(ProtocolVersion::MQTT3)),
            Some(MQTT_LEVEL_5) => Ok(Some(ProtocolVersion::MQTT5)),
            Some(_) => Err(DecodeError::UnsupportedProtocolLevel),
            None => Ok(None),
        }
    }
}

impl Encoder for VersionCodec {
    type Item = ProtocolVersion;
    type Error = EncodeError;

    fn encode(&self, _: Self::Item, _: &mut BytePages) -> Result<(), EncodeError> {
        Err(EncodeError::UnsupportedVersion)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_connect_packets() {
        let mut buf = BytesMut::from(
            b"\x10\x7f\x7f\x00\x04MQTT\x06\xC0\x00\x3C\x00\x0512345\x00\x04user\x00\x04pass"
                .as_ref(),
        );
        assert_eq!(
            Err(DecodeError::InvalidProtocol),
            VersionCodec.decode(&mut buf)
        );

        let mut buf = BytesMut::from(b"\x10\x0c\x00\x04MQTT\x06\x02\x00\x3C\x00\x00".as_ref());
        assert_eq!(
            Err(DecodeError::UnsupportedProtocolLevel),
            VersionCodec.decode(&mut buf)
        );

        let mut buf =
            BytesMut::from(b"\x10\x98\x02\0\x04MQTT\x04\xc0\0\x0f\0\x02d1\0|testhub.".as_ref());
        assert_eq!(
            ProtocolVersion::MQTT3,
            VersionCodec.decode(&mut buf).unwrap().unwrap()
        );

        let mut buf =
            BytesMut::from(b"\x10\x98\x02\0\x04MQTT\x05\xc0\0\x0f\0\x02d1\0|testhub.".as_ref());
        assert_eq!(
            ProtocolVersion::MQTT5,
            VersionCodec.decode(&mut buf).unwrap().unwrap()
        );

        let mut buf = BytesMut::from(b"\x10\x98\x02\0\x04MQTT\x05".as_ref());
        assert_eq!(
            ProtocolVersion::MQTT5,
            VersionCodec.decode(&mut buf).unwrap().unwrap()
        );

        let mut buf = BytesMut::from(b"\x10\x98\x02\0\x04".as_ref());
        assert_eq!(None, VersionCodec.decode(&mut buf).unwrap());

        let mut buf = BytesMut::from(b"\x10\x98\x02\0\x04MQTT".as_ref());
        assert_eq!(None, VersionCodec.decode(&mut buf).unwrap());

        // not a CONNECT packet
        let mut buf = BytesMut::from(b"\x20\x02\0\0".as_ref());
        assert_eq!(
            Err(DecodeError::UnsupportedPacketType),
            VersionCodec.decode(&mut buf)
        );

        // incomplete remaining length
        let mut buf = BytesMut::from(b"\x10\x98".as_ref());
        assert_eq!(None, VersionCodec.decode(&mut buf).unwrap());
        let mut buf = BytesMut::from(b"\x10".as_ref());
        assert_eq!(None, VersionCodec.decode(&mut buf).unwrap());
    }

    #[test]
    fn test_encode() {
        let mut buf = BytePages::default();
        assert_eq!(
            VersionCodec.encode(ProtocolVersion::MQTT5, &mut buf),
            Err(EncodeError::UnsupportedVersion)
        );
    }
}
