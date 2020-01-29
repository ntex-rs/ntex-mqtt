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
    PacketIdRequired,
    MaxSizeExceeded,
    IoError(io::Error),
    Utf8Error(str::Utf8Error),
}

impl PartialEq for ParseError {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ParseError::InvalidProtocol => match other {
                ParseError::InvalidProtocol => true,
                _ => false,
            },
            ParseError::InvalidLength => match other {
                ParseError::InvalidLength => true,
                _ => false,
            },
            ParseError::MalformedPacket => match other {
                ParseError::MalformedPacket => true,
                _ => false,
            },
            ParseError::UnsupportedProtocolLevel => match other {
                ParseError::UnsupportedProtocolLevel => true,
                _ => false,
            },
            ParseError::ConnectReservedFlagSet => match other {
                ParseError::ConnectReservedFlagSet => true,
                _ => false,
            },
            ParseError::ConnAckReservedFlagSet => match other {
                ParseError::ConnAckReservedFlagSet => true,
                _ => false,
            },
            ParseError::InvalidClientId => match other {
                ParseError::InvalidClientId => true,
                _ => false,
            },
            ParseError::UnsupportedPacketType => match other {
                ParseError::UnsupportedPacketType => true,
                _ => false,
            },
            ParseError::PacketIdRequired => match other {
                ParseError::PacketIdRequired => true,
                _ => false,
            },
            ParseError::MaxSizeExceeded => match other {
                ParseError::MaxSizeExceeded => true,
                _ => false,
            },
            ParseError::IoError(_) => false,
            ParseError::Utf8Error(_) => false,
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

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TopicError {
    InvalidTopic,
    InvalidLevel,
}
