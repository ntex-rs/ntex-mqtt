//! MQTT 5 client connections, routing, and protocol handling.
use std::num::{NonZeroU16, NonZeroU32};

use ntex_bytes::{ByteString, Bytes};
use ntex_net::connect::Address;
use ntex_util::time::Seconds;

/// Established client connections and topic routing.
mod connection;
/// Transport connection and MQTT handshake service.
mod connector;
/// Client-side protocol messages and acknowledgement builders.
pub mod control;
/// Incoming packet dispatch and acknowledgement tracking.
mod dispatcher;

pub use self::connection::{Client, ClientRouter};
pub use self::connector::MqttConnector;
pub use self::control::{ProtocolMessage, ProtocolMessageAck};

pub use crate::topic::{TopicFilter, TopicFilterError};
pub use crate::types::QoS;
pub use crate::v5::{codec, error, sink::MqttSink};

/// Server address and MQTT `CONNECT` packet used by the connector.
#[derive(Clone, Debug)]
pub struct Connect<A: Address> {
    /// Destination passed to the transport connector.
    addr: A,
    /// Handshake packet populated by the builder methods.
    pkt: codec::Connect,
}

impl<A: Address> Connect<A> {
    #[inline]
    /// Build a connection request with a default `CONNECT` packet.
    pub fn new(addr: A) -> Self {
        Self {
            addr,
            pkt: codec::Connect::default(),
        }
    }

    #[inline]
    /// Build a connection request using the supplied address and `CONNECT` packet.
    pub fn with(addr: A, pkt: codec::Connect) -> Self {
        Self { addr, pkt }
    }

    #[inline]
    #[must_use]
    /// Set the client identifier sent in the `CONNECT` packet.
    pub fn client_id<U>(mut self, client_id: U) -> Self
    where
        ByteString: From<U>,
    {
        self.pkt.client_id = client_id.into();
        self
    }

    #[inline]
    #[must_use]
    /// Request a fresh broker session instead of resuming stored session state.
    ///
    /// Session expiry remains controlled by the packet's session-expiry property.
    pub fn clean_start(mut self) -> Self {
        self.pkt.clean_start = true;
        self
    }

    #[inline]
    #[must_use]
    /// Set the keep-alive interval in seconds.
    ///
    /// Zero disables keep-alive and is the default. Values above `u16::MAX`
    /// are truncated to their low 16 bits. The broker may override the interval
    /// in its `CONNACK` packet.
    pub fn keep_alive(mut self, val: Seconds) -> Self {
        self.pkt.keep_alive = val.seconds() as u16;
        self
    }

    #[inline]
    #[must_use]
    /// Set the will message the broker publishes after an ungraceful disconnect.
    ///
    /// No will message is configured by default.
    pub fn last_will(mut self, val: codec::LastWill) -> Self {
        self.pkt.last_will = Some(val);
        self
    }

    #[inline]
    #[must_use]
    /// Set the authentication method and initial authentication data in `CONNECT`.
    ///
    /// This does not enable an `AUTH` exchange; the connector expects `CONNACK`
    /// as the first broker response.
    pub fn auth(mut self, method: ByteString, data: Bytes) -> Self {
        self.pkt.auth_method = Some(method);
        self.pkt.auth_data = Some(data);
        self
    }

    #[inline]
    #[must_use]
    /// Set the username sent to the broker for authentication and authorization.
    pub fn username(mut self, val: ByteString) -> Self {
        self.pkt.username = Some(val);
        self
    }

    #[inline]
    #[must_use]
    /// Set the password bytes sent to the broker.
    pub fn password(mut self, val: Bytes) -> Self {
        self.pkt.password = Some(val);
        self
    }

    #[inline]
    #[must_use]
    /// Set the maximum inbound MQTT packet size in bytes advertised in `CONNECT`.
    ///
    /// Zero omits the property and disables the local inbound size limit.
    pub fn max_packet_size(mut self, val: u32) -> Self {
        if let Some(val) = NonZeroU32::new(val) {
            self.pkt.max_packet_size = Some(val);
        } else {
            self.pkt.max_packet_size = None;
        }
        self
    }

    #[inline]
    #[must_use]
    /// Set the receive maximum advertised to the broker.
    ///
    /// This limits unacknowledged incoming `QoS` 1 and `QoS` 2 publications; `QoS` 2
    /// identifiers remain in use until `PUBCOMP`. Zero omits the property,
    /// selecting the default limit of 65,535 publications.
    pub fn max_receive(mut self, val: u16) -> Self {
        if let Some(val) = NonZeroU16::new(val) {
            self.pkt.receive_max = Some(val);
        } else {
            self.pkt.receive_max = None;
        }
        self
    }

    #[inline]
    #[must_use]
    /// Modify the user properties sent in the `CONNECT` packet.
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.pkt.user_properties);
        self
    }

    #[inline]
    #[must_use]
    /// Modify the `CONNECT` packet before the handshake.
    pub fn packet<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::Connect),
    {
        f(&mut self.pkt);
        self
    }

    /// Consume the request, transferring its address and handshake packet.
    fn into_parts(self) -> (A, codec::Connect) {
        (self.addr, self.pkt)
    }
}
