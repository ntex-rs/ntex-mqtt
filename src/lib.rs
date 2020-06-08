#![allow(clippy::type_complexity, clippy::new_ret_no_self)]
//! MQTT v3.1.1 Server framework

pub mod client;
pub mod codec3;
pub mod codec5;

#[macro_use]
mod topic;
mod connect;
mod default;
mod dispatcher;
mod error;
mod publish;
mod router;
mod server;
mod session;
mod sink;
mod subs;

pub use self::client::Client;
pub use self::connect::{Connect, ConnectAck};
pub use self::error::MqttError;
pub use self::publish::Publish;
pub use self::router::Router;
pub use self::server::MqttServer;
pub use self::session::Session;
pub use self::sink::MqttSink;
pub use self::subs::{Subscribe, SubscribeIter, SubscribeResult, Subscription, Unsubscribe};

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const SSL_PORT: u16 = 8883;
