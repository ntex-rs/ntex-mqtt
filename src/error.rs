use derive_more::From;
use mqtt_codec;

#[derive(From)]
pub enum MqttError<E1, E2> {
    Connect(MqttConnectError<E1>),
    Publish(MqttPublishError<E2>),
}

pub enum MqttConnectError<E> {
    Service(E),
    Protocol(mqtt_codec::ParseError),
    UnexpectedPacket(mqtt_codec::Packet),
    Disconnected,
}

pub enum MqttPublishError<E> {
    Service(E),
    /// "SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets MUST contain a non-zero 16-bit Packet Identifier [MQTT-2.3.1-1]."
    PacketIdRequired,
    Protocol(mqtt_codec::ParseError),
    KeepAliveTimeout,
    Disconnected,
    ConstructError,
    InternalError,
}
