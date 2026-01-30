//! MQTT 3.1.1 client
use ntex_bytes::{ByteString, Bytes};
use ntex_net::connect::Address;
use ntex_util::time::Seconds;

mod connection;
mod connector;
pub mod control;
mod dispatcher;

pub use self::connection::{Client, ClientRouter};
pub use self::connector::{MqttConnector, MqttConnectorService};
pub use self::control::{Control, ControlAck, CtlFrame, CtlReason};

pub use crate::topic::{TopicFilter, TopicFilterError};
pub use crate::types::QoS;
pub use crate::v3::{codec, error, error::ClientError, sink::MqttSink};

#[derive(Clone, Debug)]
pub struct Connect<A: Address> {
    addr: A,
    pkt: codec::Connect,
}

impl<A: Address> Connect<A> {
    #[inline]
    /// Construct new connect message
    pub fn new(addr: A) -> Self {
        Self { addr, pkt: codec::Connect::default() }
    }

    #[inline]
    /// Construct new connect message with connect packet
    pub fn with(addr: A, pkt: codec::Connect) -> Self {
        Self { addr, pkt }
    }

    #[inline]
    /// Create new client and provide client id
    pub fn client_id<U>(mut self, client_id: U) -> Self
    where
        ByteString: From<U>,
    {
        self.pkt.client_id = client_id.into();
        self
    }

    #[inline]
    /// The handling of the Session state.
    pub fn clean_session(mut self) -> Self {
        self.pkt.clean_session = true;
        self
    }

    #[inline]
    /// A time interval measured in seconds.
    ///
    /// keep-alive is set to 30 seconds by default.
    pub fn keep_alive(mut self, val: Seconds) -> Self {
        self.pkt.keep_alive = val.seconds() as u16;
        self
    }

    #[inline]
    /// Will Message be stored on the Server and associated with the Network Connection.
    ///
    /// by default last will value is not set
    pub fn last_will(mut self, val: codec::LastWill) -> Self {
        self.pkt.last_will = Some(val);
        self
    }

    #[inline]
    /// Username can be used by the Server for authentication and authorization.
    pub fn username<U>(mut self, val: U) -> Self
    where
        ByteString: From<U>,
    {
        self.pkt.username = Some(val.into());
        self
    }

    #[inline]
    /// Password can be used by the Server for authentication and authorization.
    pub fn password(mut self, val: Bytes) -> Self {
        self.pkt.password = Some(val);
        self
    }

    #[inline]
    /// Update connect packet
    pub fn packet<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::Connect),
    {
        f(&mut self.pkt);
        self
    }

    fn into_parts(self) -> (A, codec::Connect) {
        (self.addr, self.pkt)
    }
}
