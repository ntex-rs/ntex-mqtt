//! MQTT5 Client/Server framework

pub mod client;
pub mod codec;
pub mod control;
mod default;
mod dispatcher;
pub mod error;
mod handshake;
mod publish;
mod router;
mod server;
mod sink;

pub type Session<St> = crate::Session<MqttSink, St>;

pub use self::control::{ControlMessage, ControlResult};
pub use self::handshake::{Handshake, HandshakeAck};
pub use self::publish::{Publish, PublishAck};
pub use self::router::Router;
pub use self::server::MqttServer;
pub use self::sink::{MqttSink, PublishBuilder};

pub use crate::topic::Topic;
pub use crate::types::QoS;

#[doc(hidden)]
pub type ControlPacket<E> = ControlMessage<E>;
