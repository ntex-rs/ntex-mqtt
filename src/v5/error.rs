use derive_more::{Display, From};
use ntex::util::Either;

pub use crate::error::*;
pub use crate::v5::codec;

/// Errors which can occur when attempting to handle mqtt client connection.
#[derive(Debug, Display, From)]
pub enum ClientError {
    /// Connect negotiation failed
    #[display(fmt = "Connect ack failed: {:?}", _0)]
    Ack(Box<codec::ConnectAck>),
    /// Protocol error
    #[display(fmt = "Protocol error: {:?}", _0)]
    Protocol(ProtocolError),
    /// Handshake timeout
    #[display(fmt = "Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected(Option<std::io::Error>),
    /// Connect error
    #[display(fmt = "Connect error: {}", _0)]
    Connect(ntex::connect::ConnectError),
}

impl std::error::Error for ClientError {}

impl From<Either<EncodeError, std::io::Error>> for ClientError {
    fn from(err: Either<EncodeError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(ProtocolError::Encode(err)),
            Either::Right(err) => ClientError::Disconnected(Some(err)),
        }
    }
}

impl From<Either<DecodeError, std::io::Error>> for ClientError {
    fn from(err: Either<DecodeError, std::io::Error>) -> Self {
        match err {
            Either::Left(err) => ClientError::Protocol(ProtocolError::Decode(err)),
            Either::Right(err) => ClientError::Disconnected(Some(err)),
        }
    }
}

#[derive(Debug, Display, PartialEq)]
pub enum PublishQos1Error {
    /// Negative ack from peer
    #[display(fmt = "Negative ack: {:?}", _0)]
    Fail(codec::PublishAck),
    /// Encoder error
    Encode(EncodeError),
    /// Provided packet id is in use
    #[display(fmt = "Provided packet id is in use")]
    PacketIdInUse(u16),
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
}

#[derive(Debug, Display, PartialEq)]
pub enum PublishQos2Error {
    /// Negative ack from peer
    #[display(fmt = "Negative ack: {:?}", _0)]
    Fail(codec::PublishAck2),
    /// Encoder error
    Encode(EncodeError),
    /// Provided packet id is in use
    #[display(fmt = "Provided packet id is in use")]
    PacketIdInUse(u16),
    /// Peer disconnected
    #[display(fmt = "Peer disconnected")]
    Disconnected,
}
