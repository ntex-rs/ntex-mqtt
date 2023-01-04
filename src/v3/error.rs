use ntex::util::Either;

pub use crate::{error::*, v3::codec};

/// Errors which can occur when attempting to handle mqtt client connection.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Connect negotiation failed
    #[error("Connect ack failed")]
    Ack { session_present: bool, return_code: codec::ConnectAckReason },
    /// Protocol error
    #[error("Protocol error: {:?}", _0)]
    Protocol(#[from] ProtocolError),
    /// Handshake timeout
    #[error("Handshake timeout")]
    HandshakeTimeout,
    /// Peer disconnected
    #[error("Peer disconnected")]
    Disconnected(Option<std::io::Error>),
    /// Connect error
    #[error("Connect error: {}", _0)]
    Connect(#[from] ntex::connect::ConnectError),
}

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
