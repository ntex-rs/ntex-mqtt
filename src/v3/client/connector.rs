use std::rc::Rc;

use ntex_bytes::{ByteString, Bytes, PoolId};
use ntex_io::{DispatcherConfig, IoBoxed};
use ntex_net::connect::{self, Address, Connect, Connector};
use ntex_service::{IntoService, Pipeline, Service};
use ntex_util::time::{timeout_checked, Seconds};

use super::{codec, connection::Client, error::ClientError, error::ProtocolError};
use crate::v3::shared::{MqttShared, MqttSinkPool};

/// Mqtt client connector
pub struct MqttConnector<A, T> {
    address: A,
    connector: Pipeline<T>,
    pkt: codec::Connect,
    max_size: u32,
    max_send: usize,
    max_receive: usize,
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
            max_size: 64 * 1024,
            max_send: 16,
            max_receive: 16,
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
    /// Max incoming packet size.
    ///
    /// To disable max size limit set value to 0.
    pub fn max_size(mut self, val: u32) -> Self {
        self.max_size = val;
        self
    }

    #[inline]
    /// Set max send packets number
    ///
    /// Number of in-flight outgoing publish packets. By default send max is set to 16 packets.
    /// To disable in-flight limit set value to 0.
    pub fn max_send(mut self, val: u16) -> Self {
        self.max_send = val as usize;
        self
    }

    #[inline]
    /// Number of inbound in-flight concurrent messages.
    ///
    /// By default inbound is set to 16 messages To disable in-flight limit set value to 0.
    pub fn max_receive(mut self, val: u16) -> Self {
        self.max_receive = val as usize;
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
            max_size: self.max_size,
            max_send: self.max_send,
            max_receive: self.max_receive,
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
    pub async fn connect(&self) -> Result<Client, ClientError<codec::ConnectAck>> {
        timeout_checked(self.handshake_timeout, self._connect())
            .await
            .map_err(|_| ClientError::HandshakeTimeout)
            .and_then(|res| res)
    }

    async fn _connect(&self) -> Result<Client, ClientError<codec::ConnectAck>> {
        let io: IoBoxed = self.connector.call(Connect::new(self.address.clone())).await?.into();
        let pkt = self.pkt.clone();
        let max_send = self.max_send;
        let max_receive = self.max_receive;
        let keepalive_timeout = pkt.keep_alive;
        let config = self.config.clone();
        let pool = self.pool.clone();
        let codec = codec::Codec::new();
        codec.set_max_size(self.max_size);

        io.encode(pkt.into(), &codec)?;

        let packet = io.recv(&codec).await.map_err(ClientError::from)?.ok_or_else(|| {
            log::trace!("Mqtt server is disconnected during handshake");
            ClientError::Disconnected(None)
        })?;

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, true, pool));

        match packet {
            (codec::Packet::ConnectAck(pkt), _) => {
                log::trace!("Connect ack response from server: session: present: {:?}, return code: {:?}", pkt.session_present, pkt.return_code);
                if pkt.return_code == codec::ConnectAckReason::ConnectionAccepted {
                    shared.set_cap(max_send);
                    Ok(Client::new(
                        io,
                        shared,
                        pkt.session_present,
                        Seconds(keepalive_timeout),
                        max_receive,
                        config,
                    ))
                } else {
                    Err(ClientError::Ack(pkt))
                }
            }
            (p, _) => Err(ProtocolError::unexpected_packet(
                p.packet_type(),
                "Expected CONNACK packet",
            )
            .into()),
        }
    }
}
