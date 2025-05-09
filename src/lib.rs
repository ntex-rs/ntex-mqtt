//#![deny(rust_2018_idioms, warnings, unreachable_pub)]
#![allow(rust_2018_idioms, warnings, unreachable_pub)]
#![allow(clippy::type_complexity)]

//! MQTT Client/Server framework

mod topic;
#[macro_use]
mod utils;

pub mod error;
pub mod v3;
pub mod v5;

mod inflight;
mod io;
mod payload;
mod server;
mod service;
mod session;
mod types;
mod version;

pub use self::error::{HandshakeError, MqttError, ProtocolError};
pub use self::inflight::{InFlightService, SizedRequest};
pub use self::payload::Payload;
pub use self::server::MqttServer;
pub use self::session::Session;
pub use self::topic::{TopicFilter, TopicFilterError, TopicFilterLevel};
pub use self::types::QoS;

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const TLS_PORT: u16 = 8883;
