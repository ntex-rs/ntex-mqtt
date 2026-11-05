//! MQTT 3.1.1 Client/Server framework
pub mod client;
pub mod codec;
/// Connection requests and handshake acknowledgments.
mod connect;
pub mod control;
/// Default services and connection-control adapters.
mod default;
/// Inbound packet dispatch and acknowledgment tracking.
mod dispatcher;
/// Publish messages with streamed payload access.
mod publish;
/// Topic-pattern routing for publish services.
mod router;
/// MQTT 3.1.1 server construction and handshakes.
mod server;
/// Connection state shared by the dispatcher and sink.
mod shared;
/// Outgoing packet builders and acknowledgment handling.
mod sink;

/// MQTT 3.1.1 session, provides access to the sink and the application state.
pub type Session<St> = crate::Session<MqttSink, St>;

pub use self::connect::{Connect, ConnectAck};
pub use self::control::{ProtocolMessage, ProtocolMessageAck};
pub use self::publish::Publish;
pub use self::router::Router;
pub use self::server::MqttServer;
pub use self::sink::{MqttSink, PublishBuilder, SubscribeBuilder, UnsubscribeBuilder};

pub use crate::error::{self, MqttError};
pub use crate::topic::{TopicFilter, TopicFilterError};
pub use crate::types::QoS;

pub(crate) use self::server::reject_connect;
