//! MQTT 5 client and server framework.

pub mod client;
pub mod codec;
/// Server-side MQTT protocol messages and response builders.
pub mod control;

/// Server handshake requests, acknowledgements, and keep-alive negotiation.
mod connect;
/// Default protocol handling and connection-control adapters.
mod default;
/// Inbound packet validation, dispatch, and acknowledgement state.
mod dispatcher;
/// Inbound publish messages and application acknowledgement builders.
mod publish;
/// Topic-pattern routing for inbound publish messages.
mod router;
/// MQTT 5 server construction and handshake processing.
mod server;
/// Per-connection send credits, acknowledgement tracking, and payload state.
mod shared;
/// Connection sink and outgoing packet builders.
mod sink;

/// MQTT 5 session providing application state, configuration, and the connection sink.
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

/// Receive Maximum used when no smaller limit is advertised: 65,535 publishes.
const RECEIVE_MAX_DEFAULT: NonZeroU16 = NonZeroU16::new(65_535).unwrap();

/// Logs an unsupported operation and requests an implementation-specific disconnect.
fn disconnect(msg: &'static str) -> ProtocolMessageAck {
    log::error!("{msg}");

    ProtocolMessageAck {
        packet: control::Pkt::Disconnect(codec::Disconnect::new(
            codec::DisconnectReasonCode::ImplementationSpecificError,
        )),
        disconnect: true,
    }
}

/// Converts a publish service error into an application publish acknowledgement.
///
/// For `QoS 1` and `QoS 2`, convertible errors are sent as `PUBACK` or `PUBREC`
/// instead of terminating the connection. Errors for `QoS 0` and service readiness
/// failures use [`Self::into_error`] because no publish acknowledgement can be sent.
pub trait ToPublishAck {
    /// Error returned when conversion to an acknowledgement is impossible.
    type Error;

    /// Attempts to convert this error into an application publish acknowledgement.
    fn try_ack(self) -> Result<PublishAck, Self::Error>;

    /// Converts this error for propagation when no publish acknowledgement can be sent.
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
