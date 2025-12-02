use std::{marker::PhantomData, num::NonZeroU16, num::NonZeroU32, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_io::IoBoxed;
use ntex_net::connect::{self, Address, Connect, Connector};
use ntex_service::cfg::SharedCfg;
use ntex_service::{IntoServiceFactory, Pipeline, Service, ServiceCtx, ServiceFactory};
use ntex_util::time::{Seconds, timeout_checked};

use super::codec::{self, Decoded, Encoded, Packet};
use super::{connection::Client, error::ClientError, error::ProtocolError};
use crate::v5::shared::{MqttShared, MqttSinkPool};

/// Mqtt client connector factory
pub struct MqttConnector<A, T> {
    connector: T,
    pkt: codec::Connect,
    handshake_timeout: Seconds,
    min_chunk_size: u32,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<A>,
}

/// Mqtt client connector
pub struct MqttConnectorService<A, T> {
    connector: T,
    pkt: codec::Connect,
    handshake_timeout: Seconds,
    min_chunk_size: u32,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<A>,
}

impl<A> MqttConnector<A, ()>
where
    A: Address,
{
    #[allow(clippy::new_ret_no_self)]
    /// Create new mqtt connector
    pub fn new() -> MqttConnector<A, Connector<A>> {
        MqttConnector {
            pkt: codec::Connect::default(),
            connector: Connector::default(),
            handshake_timeout: Seconds::ZERO,
            min_chunk_size: 32 * 1024,
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<A, T> MqttConnector<A, T>
where
    A: Address,
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

    /// Set min payload chunk size.
    ///
    /// If the minimum size is set to `0`, incoming payload chunks
    /// will be processed immediately. Otherwise, the codec will
    /// accumulate chunks until the total size reaches the specified minimum.
    /// By default min size is set to `0`
    pub fn min_chunk_size(mut self, size: u32) -> Self {
        self.min_chunk_size = size;
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

    /// Use custom connector
    pub fn connector<U, F>(self, connector: F) -> MqttConnector<A, U>
    where
        F: IntoServiceFactory<U, Connect<A>, SharedCfg>,
        U: ServiceFactory<Connect<A>, SharedCfg, Error = connect::ConnectError>,
        IoBoxed: From<U::Response>,
    {
        MqttConnector {
            connector: connector.into_factory(),
            pkt: self.pkt,
            handshake_timeout: self.handshake_timeout,
            min_chunk_size: self.min_chunk_size,
            pool: self.pool,
            _t: PhantomData,
        }
    }
}

impl<A, T> ServiceFactory<A, SharedCfg> for MqttConnector<A, T>
where
    A: Address,
    T: ServiceFactory<Connect<A>, SharedCfg, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    type Response = Client;
    type Error = ClientError<Box<codec::ConnectAck>>;
    type InitError = T::InitError;
    type Service = MqttConnectorService<A, T::Service>;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(MqttConnectorService {
            connector: self.connector.create(cfg).await?,
            pkt: self.pkt.clone(),
            handshake_timeout: self.handshake_timeout,
            min_chunk_size: self.min_chunk_size,
            pool: self.pool.clone(),
            _t: PhantomData,
        })
    }
}

impl<A, T> Service<A> for MqttConnectorService<A, T>
where
    A: Address,
    T: Service<Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    type Response = Client;
    type Error = ClientError<Box<codec::ConnectAck>>;

    ntex_service::forward_ready!(connector);
    ntex_service::forward_poll!(connector);
    ntex_service::forward_shutdown!(connector);

    /// Connect to mqtt server
    async fn call(&self, req: A, ctx: ServiceCtx<'_, Self>) -> Result<Client, Self::Error> {
        timeout_checked(self.handshake_timeout, self._connect(req, ctx))
            .await
            .map_err(|_| ClientError::HandshakeTimeout)
            .and_then(|res| res)
    }
}

impl<A, T> MqttConnectorService<A, T>
where
    A: Address,
    T: Service<Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    async fn _connect(
        &self,
        req: A,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Client, ClientError<Box<codec::ConnectAck>>> {
        let io: IoBoxed = ctx.call(&self.connector, Connect::new(req)).await?.into();
        let pkt = self.pkt.clone();
        let keep_alive = pkt.keep_alive;
        let max_packet_size = pkt.max_packet_size.map(|v| v.get()).unwrap_or(0);
        let max_receive = pkt.receive_max.map(|v| v.get()).unwrap_or(65535);
        let pool = self.pool.clone();

        let codec = codec::Codec::new();
        codec.set_max_inbound_size(max_packet_size);
        codec.set_min_chunk_size(self.min_chunk_size);

        io.encode(Encoded::Packet(Packet::Connect(Box::new(pkt))), &codec)?;

        let packet = io.recv(&codec).await.map_err(ClientError::from)?.ok_or_else(|| {
            log::trace!("Mqtt server is disconnected during handshake");
            ClientError::Disconnected(None)
        })?;

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, pool));
        match packet {
            Decoded::Packet(Packet::ConnectAck(pkt), ..) => {
                log::trace!("Connect ack response from server: {:#?}", pkt);
                if pkt.reason_code == codec::ConnectAckReason::Success {
                    // set max outbound (encoder) packet size
                    if let Some(size) = pkt.max_packet_size {
                        shared.codec.set_max_outbound_size(size);
                    }
                    // server keep-alive
                    let keep_alive = pkt.server_keepalive_sec.unwrap_or(keep_alive);

                    shared.set_cap(pkt.receive_max.get() as usize);

                    Ok(Client::new(io, shared, pkt, max_receive, Seconds(keep_alive)))
                } else {
                    Err(ClientError::Ack(pkt))
                }
            }
            Decoded::Packet(packet, ..) => Err(ProtocolError::unexpected_packet(
                packet.packet_type(),
                "CONNACK packet expected from server first [MQTT-3.2.0-1]",
            )
            .into()),
            Decoded::Publish(..) => Err(ProtocolError::unexpected_packet(
                crate::types::packet_type::PUBLISH_START,
                "CONNACK packet expected from server first [MQTT-3.2.0-1]",
            )
            .into()),
            Decoded::PayloadChunk(..) => unreachable!(),
        }
    }
}
