//! MQTT v3.1.1 and v5 clients and servers built on `ntex`.
//!
//! The [`v3`] and [`v5`] modules provide protocol-specific servers, client
//! connectors, packet types, and connection sinks. [`MqttServer`] combines
//! both server versions on one listener, selecting a service from the initial
//! CONNECT packet. [`MqttServiceConfig`] configures connection timeouts,
//! packet limits, and flow control.
//!
//! See the [MQTT guide](https://docs.ntex.rs/15-mqtt.html) for usage examples.
#![warn(missing_docs)]
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

/// Topic filter parsing, validation, and matching.
mod topic;
/// Shared wire-format helpers and codec macros.
#[macro_use]
mod utils;

pub mod control;
/// Errors from MQTT connection handling, packet codecs, and outgoing sends.
pub mod error;
pub mod v3;
pub mod v5;

/// Shared client and server configuration.
mod config;
/// Readiness and concurrency limits for incoming publishes.
mod inflight;
/// Connection dispatch and streaming frame state.
mod io;
/// Streaming publish payload delivery and flow control.
mod payload;
/// Protocol detection and combined v3.1.1/v5 server dispatch.
mod server;
/// Handshake-to-dispatcher adapter for protocol-specific servers.
mod service;
/// Reference-counted connection state shared with application services.
mod session;
/// Protocol constants, flags, and quality-of-service levels.
mod types;
/// Non-consuming detection of the initial CONNECT protocol level.
mod version;

pub use self::config::MqttServiceConfig;
pub use self::control::{Control, Reason};
pub use self::error::{MqttConnectError, MqttError, MqttProtocolError};
pub use self::inflight::SizedRequest;
pub use self::payload::Payload;
pub use self::server::MqttServer;
pub use self::session::Session;
pub use self::topic::{TopicFilter, TopicFilterError, TopicFilterLevel};
pub use self::types::QoS;

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
/// Default TCP port for unencrypted MQTT connections.
pub const TCP_PORT: u16 = 1883;
/// Default TCP port for TLS-encrypted MQTT connections.
pub const TLS_PORT: u16 = 8883;

/// Handshake pipeline yielding the transport, codec, session, and keep-alive interval.
pub(crate) type ConnectPipeline<St, ImSt, AppSt, Codec, Cfg, Err> =
    PipelineState<St, (IoBoxed, ImSt), (IoBoxed, Codec, Session<Cfg, AppSt>, Seconds), Err>;
