use std::{num::NonZeroU16, num::NonZeroU32, rc::Rc};

use ntex_bytes::{ByteString, Bytes, PoolId};
use ntex_io::{DispatcherConfig, IoBoxed};
use ntex_net::connect::{self, Address, Connect, Connector};
use ntex_service::{IntoService, Pipeline, Service};
use ntex_util::time::{timeout_checked, Seconds};

use super::{codec, connection::Client, error::ClientError, error::ProtocolError};
use crate::v5::shared::{MqttShared, MqttSinkPool};

/// Mqtt client connector
pub struct MqttConnector<A, T> {
    address: A,
    connector: Pipeline<T>,
    pkt: codec::Connect,
    handshake_timeout: Seconds,
    config: DispatcherConfig,
    pool: Rc<MqttSinkPool>,
}

impl<A> MqttConnector<A, ()>
where
    A: Address + Clone,
{
    #[allow(clippy::new_ret_no_self)]
    /// Create new mqtt connector
    pub fn new(address: A) -> MqttConnector<A, Connector<A>> {
        let config = DispatcherConfig::default();
        config.set_disconnect_timeout(Seconds(3)).set_keepalive_timeout(Seconds(0));
        MqttConnector {
            address,
            config,
            pkt: codec::Connect::default(),
            connector: Pipeline::new(Connector::default()),
            handshake_timeout: Seconds::ZERO,
            pool: Rc::new(MqttSinkPool::default()),
        }
    }
}

impl<A, T> MqttConnector<A, T>
where
    A: Address + Clone,
{
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
    pub fn clean_start(mut self) -> Self {
        self.pkt.clean_start = true;
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
    /// Set auth-method and auth-data for connect packet.
    pub fn auth(mut self, method: ByteString, data: Bytes) -> Self {
        self.pkt.auth_method = Some(method);
        self.pkt.auth_data = Some(data);
        self
    }

    #[inline]
    /// Username can be used by the Server for authentication and authorization.
    pub fn username(mut self, val: ByteString) -> Self {
        self.pkt.username = Some(val);
        self
    }

    #[inline]
    /// Password can be used by the Server for authentication and authorization.
    pub fn password(mut self, val: Bytes) -> Self {
        self.pkt.password = Some(val);
        self
    }

    #[inline]
    /// Max incoming packet size.
    ///
    /// To disable max size limit set value to 0.
    pub fn max_packet_size(mut self, val: u32) -> Self {
        if let Some(val) = NonZeroU32::new(val) {
            self.pkt.max_packet_size = Some(val);
        } else {
            self.pkt.max_packet_size = None;
        }
        self
    }

    #[inline]
    /// Set `receive max`
    ///
    /// Number of in-flight incoming publish packets. By default receive max is set to 16 packets.
    /// To disable in-flight limit set value to 0.
    pub fn max_receive(mut self, val: u16) -> Self {
        if let Some(val) = NonZeroU16::new(val) {
            self.pkt.receive_max = Some(val);
        } else {
            self.pkt.receive_max = None;
        }
        self
    }

    #[inline]
    /// Update connect user properties
    pub fn properties<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut codec::UserProperties),
    {
        f(&mut self.pkt.user_properties);
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

    /// Set handshake timeout.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: Seconds) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set client connection disconnect timeout.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(self, timeout: Seconds) -> Self {
        self.config.set_disconnect_timeout(timeout);
        self
    }

    /// Set memory pool.
    ///
    /// Use specified memory pool for memory allocations. By default P5
    /// memory pool is used.
    pub fn memory_pool(self, id: PoolId) -> Self {
        self.pool.pool.set(id.pool_ref());
        self
    }

    /// Use custom connector
    pub fn connector<U, F>(self, connector: F) -> MqttConnector<A, U>
    where
        F: IntoService<U, Connect<A>>,
        U: Service<Connect<A>, Error = connect::ConnectError>,
        IoBoxed: From<U::Response>,
    {
        MqttConnector {
            connector: Pipeline::new(connector.into_service()),
            pkt: self.pkt,
            address: self.address,
            config: self.config,
            handshake_timeout: self.handshake_timeout,
            pool: self.pool,
        }
    }
}

impl<A, T> MqttConnector<A, T>
where
    A: Address + Clone,
    T: Service<Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    /// Connect to mqtt server
    pub async fn connect(&self) -> Result<Client, ClientError<Box<codec::ConnectAck>>> {
        timeout_checked(self.handshake_timeout, self._connect())
            .await
            .map_err(|_| ClientError::HandshakeTimeout)
            .and_then(|res| res)
    }

    async fn _connect(&self) -> Result<Client, ClientError<Box<codec::ConnectAck>>> {
        let io: IoBoxed = self.connector.call(Connect::new(self.address.clone())).await?.into();
        let pkt = self.pkt.clone();
        let keep_alive = pkt.keep_alive;
        let max_packet_size = pkt.max_packet_size.map(|v| v.get()).unwrap_or(0);
        let max_receive = pkt.receive_max.map(|v| v.get()).unwrap_or(65535);
        let codec = codec::Codec::new();
        codec.set_max_inbound_size(max_packet_size);
        let pool = self.pool.clone();
        let config = self.config.clone();

        io.encode(codec::Packet::Connect(Box::new(pkt)), &codec)?;

        let packet = io.recv(&codec).await.map_err(ClientError::from)?.ok_or_else(|| {
            log::trace!("Mqtt server is disconnected during handshake");
            ClientError::Disconnected(None)
        })?;

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, pool));
        match packet {
            (codec::Packet::ConnectAck(pkt), _) => {
                log::trace!("Connect ack response from server: {:#?}", pkt);
                if pkt.reason_code == codec::ConnectAckReason::Success {
                    // set max outbound (encoder) packet size
                    if let Some(size) = pkt.max_packet_size {
                        shared.codec.set_max_outbound_size(size);
                    }
                    // server keep-alive
                    let keep_alive = pkt.server_keepalive_sec.unwrap_or(keep_alive);

                    shared.set_cap(pkt.receive_max.get() as usize);

                    Ok(Client::new(io, shared, pkt, max_receive, Seconds(keep_alive), config))
                } else {
                    Err(ClientError::Ack(pkt))
                }
            }
            (p, _) => Err(ProtocolError::unexpected_packet(
                p.packet_type(),
                "CONNACK packet expected from server first [MQTT-3.2.0-1]",
            )
            .into()),
        }
    }
}
