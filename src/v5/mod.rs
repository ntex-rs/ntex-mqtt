//! MQTT5 Client/Server framework

pub mod client;
pub mod codec;
pub mod control;
mod default;
mod dispatcher;
mod handshake;
mod publish;
mod router;
mod selector;
mod server;
mod shared;
mod sink;

pub type Session<St> = crate::Session<MqttSink, St>;

use std::num::NonZeroU16;

pub use self::control::{ControlMessage, ControlResult};
pub use self::handshake::{Handshake, HandshakeAck};
pub use self::publish::{Publish, PublishAck};
pub use self::router::Router;
pub use self::selector::Selector;
pub use self::server::MqttServer;
pub use self::sink::{MqttSink, PublishBuilder, SubscribeBuilder, UnsubscribeBuilder};

pub use crate::error;
pub use crate::topic::{TopicFilter, TopicFilterError};
pub use crate::types::QoS;

pub(self) const RECEIVE_MAX_DEFAULT: NonZeroU16 = unsafe { NonZeroU16::new_unchecked(65_535) };
