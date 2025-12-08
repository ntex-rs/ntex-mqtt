use ntex_service::cfg::{CfgContext, Configuration};
use ntex_util::time::{Millis, Seconds};

use crate::types::QoS;

#[derive(Copy, Clone, Debug)]
pub struct MqttServiceConfig {
    pub(crate) max_qos: QoS,
    pub(crate) max_size: u32,
    pub(crate) max_receive: u16,
    pub(crate) max_receive_size: usize,
    pub(crate) max_topic_alias: u16,
    pub(crate) max_send: u16,
    pub(crate) max_send_size: (u32, u32),
    pub(crate) min_chunk_size: u32,
    pub(crate) max_payload_buffer_size: usize,
    pub(crate) handle_qos_after_disconnect: Option<QoS>,
    pub(crate) connect_timeout: Seconds,
    pub(crate) handshake_timeout: Seconds,
    pub(crate) protocol_version_timeout: Millis,
    config: CfgContext,
}

impl Default for MqttServiceConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl Configuration for MqttServiceConfig {
    const NAME: &str = "MQTT Service configuration";

    fn ctx(&self) -> &CfgContext {
        &self.config
    }

    fn set_ctx(&mut self, ctx: CfgContext) {
        self.config = ctx;
    }
}

impl MqttServiceConfig {
    pub fn new() -> Self {
        Self {
            max_qos: QoS::AtLeastOnce,
            max_size: 0,
            max_send: 16,
            max_send_size: (65535, 512),
            max_receive: 16,
            max_receive_size: 65535,
            max_topic_alias: 32,
            min_chunk_size: 32 * 1024,
            max_payload_buffer_size: 32 * 1024,
            handle_qos_after_disconnect: None,
            connect_timeout: Seconds::ZERO,
            handshake_timeout: Seconds::ZERO,
            protocol_version_timeout: Millis(5_000),
            config: CfgContext::default(),
        }
    }

    /// Set client timeout reading protocol version.
    ///
    /// Defines a timeout for reading protocol version. If a client does not transmit
    /// version of the protocol within this time, the connection is terminated with
    /// Mqtt::Handshake(HandshakeError::Timeout) error.
    ///
    /// By default, timeuot is 5 seconds.
    pub fn protocol_version_timeout(mut self, timeout: Seconds) -> Self {
        self.protocol_version_timeout = timeout.into();
        self
    }

    /// Set client timeout for first `Connect` frame.
    ///
    /// Defines a timeout for reading `Connect` frame. If a client does not transmit
    /// the entire frame within this time, the connection is terminated with
    /// Mqtt::Handshake(HandshakeError::Timeout) error.
    ///
    /// By default, connect timeout is disabled.
    pub fn set_connect_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set max allowed QoS.
    ///
    /// If peer sends publish with higher qos then ProtocolError::MaxQoSViolated(..)
    /// By default max qos is set to `ExactlyOnce`.
    pub fn set_max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = qos;
        self
    }

    /// Set max inbound frame size.
    ///
    /// If max size is set to `0`, size is unlimited.
    /// By default max size is set to `0`
    pub fn set_max_size(mut self, size: u32) -> Self {
        self.max_size = size;
        self
    }

    /// Set `receive max`
    ///
    /// Number of in-flight publish packets. By default receive max is set to 15 packets.
    /// To disable timeout set value to 0.
    pub fn set_max_receive(mut self, val: u16) -> Self {
        self.max_receive = val;
        self
    }

    /// Total size of received in-flight messages.
    ///
    /// By default total in-flight size is set to 64Kb
    pub fn set_max_receive_size(mut self, val: usize) -> Self {
        self.max_receive_size = val;
        self
    }

    /// Number of topic aliases.
    ///
    /// By default value is set to 32
    pub fn set_max_topic_alias(mut self, val: u16) -> Self {
        self.max_topic_alias = val;
        self
    }

    /// Number of outgoing concurrent messages.
    ///
    /// By default outgoing is set to 16 messages
    pub fn set_max_send(mut self, val: u16) -> Self {
        self.max_send = val;
        self
    }

    /// Total size of outgoing messages.
    ///
    /// By default total outgoing size is set to 64Kb
    pub fn set_max_send_size(mut self, val: u32) -> Self {
        self.max_send_size = (val, val / 10);
        self
    }

    /// Set min payload chunk size.
    ///
    /// If the minimum size is set to `0`, incoming payload chunks
    /// will be processed immediately. Otherwise, the codec will
    /// accumulate chunks until the total size reaches the specified minimum.
    /// By default min size is set to `0`
    pub fn set_min_chunk_size(mut self, size: u32) -> Self {
        self.min_chunk_size = size;
        self
    }

    /// Max payload buffer size for payload streaming.
    ///
    /// By default buffer size is set to 32Kb
    pub fn set_max_payload_buffer_size(mut self, val: usize) -> Self {
        self.max_payload_buffer_size = val;
        self
    }

    /// Handle max received QoS messages after client disconnect.
    ///
    /// By default, messages received before dispatched to the publish service will be dropped if
    /// the client disconnect is detected on the server.
    ///
    /// If this option is set to `Some(QoS::AtMostOnce)`, only the received QoS 0 messages will
    /// always be handled by the server's publish service no matter if the client is disconnected
    /// or not.
    ///
    /// If this option is set to `Some(QoS::AtLeastOnce)`, the received QoS 0 and QoS 1 messages
    /// will always be handled by the server's publish service no matter if the client
    /// is disconnected or not. The QoS 2 messages will be dropped if the client disconnecting is
    /// detected before the server dispatches them to the publish service.
    ///
    /// If this option is set to `Some(QoS::ExactlyOnce)`, all the messages received will always
    /// be handled by the server's publish service no matter if the client is disconnected or not.
    ///
    /// The received messages which QoS larger than the `max_handle_qos` will not be guaranteed to
    /// be handled or not after the client disconnect. It depends on the network condition.
    ///
    /// By default handle-qos-after-disconnect is set to `None`
    pub fn set_handle_qos_after_disconnect(mut self, max_handle_qos: Option<QoS>) -> Self {
        self.handle_qos_after_disconnect = max_handle_qos;
        self
    }

    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn set_handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
        self
    }
}
