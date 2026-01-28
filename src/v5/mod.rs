//! MQTT5 Client/Server framework

pub mod client;
pub mod codec;
pub mod control;
mod default;
mod dispatcher;
mod handshake;
mod publish;
mod router;
mod server;
mod shared;
mod sink;

pub type Session<St> = crate::Session<MqttSink, St>;

use std::num::NonZeroU16;

pub use self::control::{Control, ControlAck, CtlFlow, CtlFrame, CtlReason};
pub use self::handshake::{Handshake, HandshakeAck};
pub use self::publish::{Publish, PublishAck};
pub use self::router::Router;
pub use self::server::MqttServer;
pub use self::sink::{MqttSink, SubscribeBuilder, UnsubscribeBuilder};
pub use self::sink::{PublishBuilder, StreamingPayload};

pub use crate::error;
pub use crate::topic::{TopicFilter, TopicFilterError};
pub use crate::types::QoS;

const RECEIVE_MAX_DEFAULT: NonZeroU16 = NonZeroU16::new(65_535).unwrap();

fn disconnect(msg: &'static str) -> ControlAck {
    log::error!("{}", msg);

    ControlAck {
        packet: Some(
            codec::Disconnect::new(codec::DisconnectReasonCode::ImplementationSpecificError)
                .into(),
        ),
        disconnect: true,
    }
}
