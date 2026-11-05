use ntex_service::cfg::{CfgContext, Configuration};
use ntex_util::time::{Millis, Seconds};

use crate::types::QoS;

/// Shared MQTT server and client connection configuration.
///
/// # Read timeouts
///
/// The dispatcher uses one read-side timer, selected by the read state:
///
/// * Between packets the keep-alive timeout applies. It is restarted after
///   every received packet, a streamed publish is one packet and its payload
///   chunks do not restart it. The server sets keep-alive during the handshake,
///   see [`v3::ConnectAck::idle_timeout()`] and
///   [`v5::ConnectAck::keep_alive()`].
/// * While a packet is partially received and a frame read rate is configured
///   with `IoConfig::set_frame_read_rate(timeout, max_timeout, rate)`, the
///   frame read rate replaces keep-alive. The peer must send more than `rate`
///   bytes every `timeout` period, and the whole packet must be received within
///   `max_timeout`. If `max_timeout` is zero, a packet is not limited in time
///   as long as the peer keeps the rate.
/// * Without a frame read rate, keep-alive also bounds a partially received
///   packet.
///
/// No read timer runs while reading is paused, because the service is not
/// ready or the response queue is full (see [`set_max_queue`]). The frame read
/// budget restarts when reading resumes. During write backpressure only the
/// write timeout (`IoConfig::set_write_timeout()`) applies.
/// Read and write timeouts are suspended while the io filter chain is paused
/// (`IoRef::is_filter_paused()`), since that pause is not caused by the peer.
///
/// # Read pause
///
/// The dispatcher stops reading new packets while:
///
/// * the response queue is full, see [`set_max_queue`],
/// * the total size of in-flight publishes reaches [`set_max_receive_size`],
/// * the unread part of a streamed payload reaches [`set_max_payload_buffer_size`],
/// * the publish or control service is not ready.
///
/// Successful service readiness is cached across packets and calls. Custom
/// publish, protocol-message, connection-control, and handshake services whose
/// readiness changes must subscribe to a [`ntex_service::ReadyNotifier`] in
/// [`ntex_service::Service::bind`] and notify synchronously whenever the gate
/// changes, before waking waiters. Middleware and other composite services must
/// bind every child service or pipeline, including call-only children; forwarding
/// `ready()` alone does not forward notifications.
///
/// Built-in in-flight limits and payload streams notify automatically, including
/// cancellation, payload-buffer watermarks, and a dropped payload receiver.
/// The in-flight count limit still queues publishes in `call()` rather than
/// pausing acks and pings. A streaming publish bypasses the size limit until
/// its handler completes, so its remaining payload can reach that handler.
///
/// Acks of outgoing publishes and the peer's PINGREQ are packets on the same
/// connection, they are not read while reading is paused. A handler that awaits
/// an ack from the same peer, for example a publish handler that awaits
/// `send_at_least_once()` to the client it serves, can hold the pause forever:
/// the ack cannot be read until the handler completes. No read timer runs
/// during the pause, the connection stalls until the peer closes it, typically
/// when its PINGREQ is not answered within keep-alive. Do not await acks from
/// the same peer in handlers that can be paused, spawn the send or use
/// `send_at_least_once_no_block()` instead.
///
/// [`set_max_queue`]: MqttServiceConfig::set_max_queue
/// [`set_max_receive_size`]: MqttServiceConfig::set_max_receive_size
/// [`set_max_payload_buffer_size`]: MqttServiceConfig::set_max_payload_buffer_size
/// [`v3::ConnectAck::idle_timeout()`]: crate::v3::ConnectAck::idle_timeout
/// [`v5::ConnectAck::keep_alive()`]: crate::v5::ConnectAck::keep_alive
#[derive(Debug)]
pub struct MqttServiceConfig {
    /// Highest accepted incoming publish quality of service.
    pub(crate) max_qos: QoS,
    /// Maximum inbound packet size in bytes; zero removes the configured limit.
    pub(crate) max_size: u32,
    /// Maximum incoming in-flight publish count, with protocol-specific zero semantics.
    pub(crate) max_receive: u16,
    /// Incoming in-flight byte budget; reaching it pauses new packet reads.
    pub(crate) max_receive_size: usize,
    /// Maximum outstanding dispatcher responses; zero disables this limit.
    pub(crate) max_queue: usize,
    /// Maximum incoming topic alias advertised by a v5 server.
    pub(crate) max_topic_alias: u16,
    /// Configured outgoing in-flight publish count before handshake overrides.
    pub(crate) max_send: u16,
    /// Reserved outgoing byte limit and low-water mark; currently unused.
    pub(crate) max_send_size: (u32, u32),
    /// Minimum bytes accumulated before emitting an incoming payload chunk.
    pub(crate) min_chunk_size: u32,
    /// Unread streaming-payload byte threshold that pauses connection reads.
    pub(crate) max_payload_buffer_size: usize,
    /// Highest publish quality of service still dispatched after disconnection, if configured.
    pub(crate) handle_qos_after_disconnect: Option<QoS>,
    /// Whether v5 subscription requests must respect advertised capabilities.
    pub(crate) check_subs_availability: bool,
    /// Server timeout in seconds for receiving the complete CONNECT packet.
    pub(crate) connect_timeout: Seconds,
    /// Client CONNECT/CONNACK exchange timeout in seconds; zero disables it.
    pub(crate) handshake_timeout: Seconds,
    /// Combined-server timeout in milliseconds for detecting the CONNECT protocol level.
    pub(crate) protocol_version_timeout: Millis,
    /// Shared configuration context attached by the configuration registry.
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
    /// Create the default packet limits, streaming thresholds, and timeouts.
    pub fn new() -> Self {
        Self {
            max_qos: QoS::AtLeastOnce,
            max_size: 256 * 1024,
            max_send: 16,
            max_send_size: (65535, 512),
            max_receive: 16,
            max_receive_size: 65535,
            max_queue: 64,
            max_topic_alias: 32,
            min_chunk_size: 32 * 1024,
            max_payload_buffer_size: 32 * 1024,
            handle_qos_after_disconnect: None,
            check_subs_availability: true,
            connect_timeout: Seconds(5),
            handshake_timeout: Seconds::ZERO,
            protocol_version_timeout: Millis(5_000),
            config: CfgContext::default(),
        }
    }

    #[must_use]
    /// Set the combined server timeout for detecting the incoming protocol version.
    ///
    /// Applies when using `MqttServer` to select between v3.1.1 and v5. If a client
    /// does not transmit the protocol level in time, the connection is terminated with
    /// `MqttError::Connect(MqttConnectError::Timeout)` error.
    ///
    /// By default, timeout is 5 seconds.
    pub fn protocol_version_timeout(mut self, timeout: Seconds) -> Self {
        self.protocol_version_timeout = timeout.into();
        self
    }

    #[must_use]
    /// Set the server timeout for receiving the first CONNECT packet.
    ///
    /// Defines a timeout for reading `Connect` frame. If a client does not transmit
    /// the entire frame within this time, the connection is terminated with
    /// `MqttError::Connect(MqttConnectError::Timeout)` error.
    ///
    /// Use `Seconds::ZERO` to disable the timeout. Frame read rate
    /// (`IoConfig::set_frame_read_rate`) does not apply to the `Connect` frame.
    ///
    /// By default, connect timeout is set to 5 seconds.
    pub fn set_connect_timeout(mut self, timeout: Seconds) -> Self {
        self.connect_timeout = timeout;
        self
    }

    #[must_use]
    /// Set the client timeout for the CONNECT/CONNACK exchange.
    ///
    /// Used by the client connectors only. Handshake includes `connect` packet and
    /// response `connect-ack`. If it does not complete in time, connect fails with
    /// `MqttClientError::ConnectTimeout`. Server side uses
    /// [`set_connect_timeout`](Self::set_connect_timeout) instead.
    ///
    /// By default handshake timeout is disabled.
    pub fn set_handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    #[must_use]
    /// Set the highest accepted incoming publish `QoS`.
    ///
    /// If peer sends publish with higher qos, the connection is terminated with
    /// `MqttProtocolError::ProtocolViolation` error (`SpecViolation::Connack_3_2_2_11`).
    ///
    /// By default max qos is set to `AtLeastOnce`.
    pub fn set_max_qos(mut self, qos: QoS) -> Self {
        self.max_qos = qos;
        self
    }

    #[must_use]
    /// Set the maximum inbound packet size in bytes.
    ///
    /// Larger packets close the connection with a `MaxSizeExceeded` decode error.
    /// Applies to MQTT v3 servers and clients and MQTT v5 servers, a v5 server advertises
    /// it as Maximum Packet Size in CONNACK. Non-PUBLISH packets are buffered whole, and
    /// decoding expands them in memory (up to ~10x for SUBSCRIBE topic filters or user
    /// properties), keep the limit bounded for untrusted peers.
    ///
    /// If max size is set to `0`, size is unlimited (up to 256 MB, the protocol maximum).
    /// By default max size is set to 256 KB.
    pub fn set_max_size(mut self, size: u32) -> Self {
        self.max_size = size;
        self
    }

    #[must_use]
    /// Set the incoming in-flight publish count limit.
    ///
    /// Number of in-flight incoming publish packets. By default receive max is set
    /// to 16 packets.
    ///
    /// For MQTT v3, publish packets over the limit wait for a slot in the order
    /// of arrival, other packets such as acks and pings are still processed.
    /// `0` disables the limit. For MQTT v5, `0` means the protocol
    /// default of 65535 packets.
    pub fn set_max_receive(mut self, val: u16) -> Self {
        self.max_receive = val;
        self
    }

    #[must_use]
    /// Set the incoming in-flight publish size limit in bytes.
    ///
    /// The dispatcher stops reading new packets while the limit is reached,
    /// handlers that await acks from the same peer can stall the connection,
    /// see [read pause](MqttServiceConfig#read-pause).
    ///
    /// By default total in-flight size is set to 65535 bytes
    pub fn set_max_receive_size(mut self, val: usize) -> Self {
        self.max_receive_size = val;
        self
    }

    #[must_use]
    /// Set the maximum number of outstanding dispatcher responses.
    ///
    /// Publish acks are sent in the order of incoming packets. An ack that is
    /// ready waits in the queue until all earlier acks are sent. Responses to
    /// other packets, such as pings and subscriptions, are sent once ready and
    /// at most once publishes have no response, these are not queued but
    /// pending ones count towards the limit.
    ///
    /// When the queue reaches this limit, the dispatcher stops reading new
    /// packets until queued responses are sent, only the rest of a partially
    /// read packet, such as the payload of a streaming publish, is read.
    /// `0` disables the limit.
    ///
    /// Acks of outgoing packets are not read while reading is paused. If the
    /// pending calls await these acks, the queue never gets room and the
    /// connection stalls, no read timer runs while reading is paused. Set the
    /// limit above the number of handlers that can await acks at the same time,
    /// see [read pause](MqttServiceConfig#read-pause).
    ///
    /// By default the limit is set to 64 responses.
    pub fn set_max_queue(mut self, val: usize) -> Self {
        self.max_queue = val;
        self
    }

    #[must_use]
    /// Set the maximum incoming topic alias advertised by a v5 server.
    ///
    /// Zero disables incoming aliases. The default maximum is 32.
    pub fn set_max_topic_alias(mut self, val: u16) -> Self {
        self.max_topic_alias = val;
        self
    }

    #[must_use]
    /// Set the maximum number of in-flight outgoing acknowledged publishes.
    ///
    /// For MQTT v5, this is an upper bound for in-flight outgoing messages, the
    /// effective value is the minimum of this value (or `ConnectAck::max_send()`)
    /// and the client's receive maximum.
    ///
    /// By default outgoing is set to 16 messages
    pub fn set_max_send(mut self, val: u16) -> Self {
        self.max_send = val;
        self
    }

    #[must_use]
    /// Store the reserved outgoing message size limit in bytes.
    ///
    /// Note: this value is not used at the moment.
    pub fn set_max_send_size(mut self, val: u32) -> Self {
        self.max_send_size = (val, val / 10);
        self
    }

    #[must_use]
    /// Set the minimum incoming payload chunk size in bytes.
    ///
    /// If the minimum size is set to `0`, incoming payload chunks
    /// will be processed immediately. Otherwise, the codec will
    /// accumulate chunks until the total size reaches the specified minimum.
    /// The default minimum is 32 KiB; the final chunk can be smaller.
    ///
    /// Payload chunks are read as one publish packet, keep-alive and
    /// frame read rate timeouts apply to the whole publish, not to each chunk.
    pub fn set_min_chunk_size(mut self, size: u32) -> Self {
        self.min_chunk_size = size;
        self
    }

    #[must_use]
    /// Set the unread streaming-payload buffer threshold in bytes.
    ///
    /// Reading from the connection pauses while the unread part of a streamed
    /// payload is at or above this size, and resumes once the publish handler
    /// reads it down to half of the size. Acks and pings are not read while
    /// reading is paused, a handler that awaits the ack of its own publish before
    /// it reads the payload deadlocks the connection. The connection is closed
    /// if the handler drops the payload before the stream ends.
    ///
    /// The default buffer threshold is 32 KiB.
    pub fn set_max_payload_buffer_size(mut self, val: usize) -> Self {
        self.max_payload_buffer_size = val;
        self
    }

    #[must_use]
    /// Set the highest received publish `QoS` still handled after disconnection.
    ///
    /// By default, messages received before dispatched to the publish service will be dropped
    /// once the connection is closing, either the client disconnected or the server
    /// started closing the connection.
    ///
    /// If this option is set to `Some(QoS::AtMostOnce)`, only the received `QoS 0` messages will
    /// always be handled by the server's publish service no matter if the client is disconnected
    /// or not.
    ///
    /// If this option is set to `Some(QoS::AtLeastOnce)`, the received `QoS 0` and `QoS 1` messages
    /// will always be handled by the server's publish service no matter if the client
    /// is disconnected or not. The `QoS 2` messages will be dropped if the client disconnecting is
    /// detected before the server dispatches them to the publish service.
    ///
    /// If this option is set to `Some(QoS::ExactlyOnce)`, all the messages received will always
    /// be handled by the server's publish service no matter if the client is disconnected or not.
    ///
    /// The received messages which `QoS` larger than the `max_handle_qos` will not be guaranteed to
    /// be handled or not after the client disconnect. It depends on the network condition.
    ///
    /// By default handle-qos-after-disconnect is set to `None`
    pub fn set_handle_qos_after_disconnect(mut self, max_handle_qos: Option<QoS>) -> Self {
        self.handle_qos_after_disconnect = max_handle_qos;
        self
    }

    #[must_use]
    /// Enable v5 validation of advertised shared and wildcard subscription support.
    ///
    /// If enabled, `Subscribe` packet with a shared or wildcard topic filter closes
    /// the connection with a protocol error when the `ConnectAck` reported such
    /// subscriptions as not available (MQTT 5.0, 3.2.2.3.11 and 3.2.2.3.13).
    /// If disabled, the packet is passed to the control service, which can reject
    /// individual topic filters with `SubscribeAck` reason codes.
    ///
    /// By default the check is enabled.
    pub fn set_check_subs_availability(mut self, val: bool) -> Self {
        self.check_subs_availability = val;
        self
    }
}

/// Unit tests for configuration builder values and timeout conversions.
#[cfg(test)]
mod tests {
    use super::*;

    /// STATEMENT: Configuration builders retain supplied limits and convert timeout units correctly.
    ///
    /// HOW:
    /// - Build a configuration with nondefault values for every setter.
    /// - Assert all stored limits, the outgoing low-water mark, timeout values, and configuration name.
    #[test]
    fn setters() {
        let cfg = MqttServiceConfig::default()
            .set_max_qos(QoS::ExactlyOnce)
            .set_max_size(1)
            .set_max_receive(2)
            .set_max_receive_size(3)
            .set_max_queue(4)
            .set_max_topic_alias(5)
            .set_max_send(6)
            .set_max_send_size(70)
            .set_min_chunk_size(8)
            .set_max_payload_buffer_size(9)
            .set_handle_qos_after_disconnect(Some(QoS::AtMostOnce))
            .set_check_subs_availability(false)
            .set_connect_timeout(Seconds(10))
            .set_handshake_timeout(Seconds(11))
            .protocol_version_timeout(Seconds(12));

        assert_eq!(cfg.max_qos, QoS::ExactlyOnce);
        assert_eq!(cfg.max_size, 1);
        assert_eq!(cfg.max_receive, 2);
        assert_eq!(cfg.max_receive_size, 3);
        assert_eq!(cfg.max_queue, 4);
        assert_eq!(cfg.max_topic_alias, 5);
        assert_eq!(cfg.max_send, 6);
        assert_eq!(cfg.max_send_size, (70, 7));
        assert_eq!(cfg.min_chunk_size, 8);
        assert_eq!(cfg.max_payload_buffer_size, 9);
        assert_eq!(cfg.handle_qos_after_disconnect, Some(QoS::AtMostOnce));
        assert!(!cfg.check_subs_availability);
        assert_eq!(cfg.connect_timeout, Seconds(10));
        assert_eq!(cfg.handshake_timeout, Seconds(11));
        assert_eq!(cfg.protocol_version_timeout, Millis(12_000));
        assert_eq!(MqttServiceConfig::NAME, "MQTT Service configuration");
    }
}
