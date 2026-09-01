//! MQTT Client/Server framework
#![deny(clippy::pedantic)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::missing_fields_in_debug,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::struct_field_names,
    clippy::type_complexity,
    clippy::unused_async,
    clippy::unused_async_trait_impl
)]
use ntex_io::IoBoxed;
use ntex_service::pipeline::PipelineState;
use ntex_util::time::Seconds;

mod topic;
#[macro_use]
mod utils;

pub mod control;
pub mod error;
pub mod v3;
pub mod v5;

mod config;
mod inflight;
mod io;
mod payload;
mod server;
mod service;
mod session;
mod types;
mod version;

pub use self::config::MqttServiceConfig;
pub use self::control::{Control, Reason};
pub use self::error::{HandshakeError, MqttError, ProtocolError};
pub use self::inflight::SizedRequest;
pub use self::payload::Payload;
pub use self::server::MqttServer;
pub use self::session::Session;
pub use self::topic::{TopicFilter, TopicFilterError, TopicFilterLevel};
pub use self::types::QoS;

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const TLS_PORT: u16 = 8883;

pub(crate) type HandshakePipeline<St, ImSt, AppSt, Codec, Cfg, Err> =
    PipelineState<St, (IoBoxed, ImSt), (IoBoxed, Codec, Session<Cfg, AppSt>, Seconds), Err>;
