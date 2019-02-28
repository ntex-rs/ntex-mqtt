use std::fmt;

use derive_more::From;
use mqtt_codec;

#[derive(From, Debug)]
pub enum MqttError<E1: fmt::Debug, E2: fmt::Debug> {
    Connect(MqttConnectError<E1>),
    Publish(MqttPublishError<E2>),
    Io(std::io::Error),
}

#[derive(Debug)]
pub enum MqttConnectError<E: fmt::Debug> {
    Service(E),
    Protocol(mqtt_codec::ParseError),
    UnexpectedPacket(mqtt_codec::Packet),
    Disconnected,
}

#[derive(Debug)]
pub enum MqttPublishError<E: fmt::Debug> {
    Service(E),
    /// "SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1]."
    PacketIdRequired,
    Protocol(mqtt_codec::ParseError),
    KeepAliveTimeout,
    Disconnected,
    ConstructError,
    InternalError,
}
