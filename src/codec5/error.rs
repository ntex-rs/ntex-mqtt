use std::{io, str};

#[derive(Debug)]
pub enum ParseError {
    InvalidProtocol,
    InvalidLength,
    MalformedPacket,
    UnsupportedProtocolLevel,
    ConnectReservedFlagSet,
    ConnAckReservedFlagSet,
    InvalidClientId,
    UnsupportedPacketType,
    MaxSizeExceeded,
    IoError(io::Error),
    Utf8Error(str::Utf8Error),
}

#[derive(Debug)]
pub enum EncodeError {
    InvalidLength,
    MalformedPacket,
    PacketIdRequired,
    IoError(io::Error),
}

impl PartialEq for ParseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ParseError::InvalidProtocol, ParseError::InvalidProtocol) => true,
            (ParseError::InvalidLength, ParseError::InvalidLength) => true,
            (ParseError::UnsupportedProtocolLevel, ParseError::UnsupportedProtocolLevel) => {
                true
            }
            (ParseError::ConnectReservedFlagSet, ParseError::ConnectReservedFlagSet) => true,
            (ParseError::ConnAckReservedFlagSet, ParseError::ConnAckReservedFlagSet) => true,
            (ParseError::InvalidClientId, ParseError::InvalidClientId) => true,
            (ParseError::UnsupportedPacketType, ParseError::UnsupportedPacketType) => true,
            (ParseError::MaxSizeExceeded, ParseError::MaxSizeExceeded) => true,
            (ParseError::MalformedPacket, ParseError::MalformedPacket) => true,
            (ParseError::IoError(_), _) => false,
            (ParseError::Utf8Error(_), _) => false,
            _ => false,
        }
    }
}

impl From<io::Error> for ParseError {
    fn from(err: io::Error) -> Self {
        ParseError::IoError(err)
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(err: str::Utf8Error) -> Self {
        ParseError::Utf8Error(err)
    }
}

impl From<io::Error> for EncodeError {
    fn from(err: io::Error) -> Self {
        EncodeError::IoError(err)
    }
}
