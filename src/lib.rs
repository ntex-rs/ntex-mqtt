#![deny(rust_2018_idioms)]
#![allow(clippy::type_complexity, clippy::await_holding_refcell_ref)]
#![type_length_limit = "1638773"]
//! MQTT Client/Server framework

#[macro_use]
mod topic;
#[macro_use]
mod utils;

mod iodispatcher;
mod ioread;
mod iostate;
mod iowrite;

pub mod error;
pub mod v3;
pub mod v5;

mod server;
mod service;
mod session;
pub mod types;
mod version;

pub use self::error::MqttError;
pub use self::server::MqttServer;
pub use self::session::Session;
pub use self::topic::{Level as TopicLevel, Topic};

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const SSL_PORT: u16 = 8883;
