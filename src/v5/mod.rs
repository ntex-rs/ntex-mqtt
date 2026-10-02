//! MQTT5 Client/Server framework

pub mod client;
pub mod codec;
/// Server side MQTT protocol messages
pub mod control;

mod connect;
mod default;
mod dispatcher;
mod publish;
mod router;
mod server;
mod shared;
mod sink;

/// MQTT v5 session, provides access to the application state and the server sink
pub type Session<St> = crate::Session<MqttSink, St>;

use ntex_error::Error;
use std::num::NonZeroU16;

pub use self::connect::{Connect, ConnectAck};
pub use self::control::{ProtocolMessage, ProtocolMessageAck};
pub use self::publish::{Publish, PublishAck};
pub use self::router::Router;
pub use self::server::MqttServer;
pub use self::sink::{MqttSink, SubscribeBuilder, UnsubscribeBuilder};
pub use self::sink::{PublishBuilder, StreamingPayload};

pub use crate::{error, topic::TopicFilter, topic::TopicFilterError, types::QoS};

const RECEIVE_MAX_DEFAULT: NonZeroU16 = NonZeroU16::new(65_535).unwrap();

fn disconnect(msg: &'static str) -> ProtocolMessageAck {
    log::error!("{msg}");

    ProtocolMessageAck {
        packet: control::Pkt::Disconnect(codec::Disconnect::new(
            codec::DisconnectReasonCode::ImplementationSpecificError,
        )),
        disconnect: true,
    }
}

/// Converts a publish service error into a `PublishAck` packet.
///
/// It allows the publish service to report an error to the client with a
/// `PublishAck` packet instead of terminating the connection.
pub trait ToPublishAck {
    /// Error type that is returned if the error cannot be converted to an ack
    type Error;

    /// Try to convert the error into a `PublishAck`
    fn try_ack(self) -> Result<PublishAck, Self::Error>;

    /// Convert the error into the resulting error type
    fn into_error(self) -> Self::Error;
}

impl<E: ToPublishAck + Clone> ToPublishAck for Error<E> {
    type Error = Error<E::Error>;

    fn try_ack(self) -> Result<PublishAck, Self::Error> {
        self.try_map(ToPublishAck::try_ack)
    }

    fn into_error(self) -> Self::Error {
        self.map(ToPublishAck::into_error)
    }
}

impl<E> ToPublishAck for E
where
    E: TryInto<PublishAck> + Into<E::Error>,
{
    type Error = E::Error;

    fn try_ack(self) -> Result<PublishAck, Self::Error> {
        self.try_into()
    }

    fn into_error(self) -> Self::Error {
        self.into()
    }
}
