use std::{marker::PhantomData, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_io::IoBoxed;
use ntex_net::connect::{self, Address, Connect, Connector};
use ntex_service::cfg::SharedCfg;
use ntex_service::{IntoServiceFactory, Service, ServiceCtx, ServiceFactory};
use ntex_util::time::{timeout_checked, Seconds};

use super::{connection::Client, error::ClientError, error::ProtocolError};
use crate::v3::codec::{self, Decoded, Encoded, Packet};
use crate::v3::shared::{MqttShared, MqttSinkPool};

/// Mqtt client connector
pub struct MqttConnector<A, T> {
    connector: T,
    pkt: codec::Connect,
    max_size: u32,
    max_send: usize,
    max_receive: usize,
    min_chunk_size: u32,
    handshake_timeout: Seconds,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<A>,
}

pub struct MqttConnectorService<A, T> {
    connector: T,
    pkt: codec::Connect,
    max_size: u32,
    max_send: usize,
    max_receive: usize,
    min_chunk_size: u32,
    handshake_timeout: Seconds,
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
            max_size: 64 * 1024,
            max_send: 16,
            max_receive: 16,
            min_chunk_size: 32 * 1024,
            handshake_timeout: Seconds::ZERO,
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
            max_size: self.max_size,
            max_send: self.max_send,
            max_receive: self.max_receive,
            min_chunk_size: self.min_chunk_size,
            handshake_timeout: self.handshake_timeout,
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
    type Error = ClientError<codec::ConnectAck>;
    type Service = MqttConnectorService<A, T::Service>;
    type InitError = T::InitError;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(MqttConnectorService {
            connector: self.connector.create(cfg).await?,
            pkt: self.pkt.clone(),
            max_size: self.max_size,
            max_send: self.max_send,
            max_receive: self.max_receive,
            min_chunk_size: self.min_chunk_size,
            handshake_timeout: self.handshake_timeout,
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
    type Error = ClientError<codec::ConnectAck>;

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
    ) -> Result<Client, ClientError<codec::ConnectAck>> {
        let io: IoBoxed = ctx.call(&self.connector, Connect::new(req)).await?.into();
        let pkt = self.pkt.clone();
        let max_send = self.max_send;
        let max_receive = self.max_receive;
        let keepalive_timeout = pkt.keep_alive;
        let pool = self.pool.clone();
        let codec = codec::Codec::new();
        codec.set_max_size(self.max_size);
        codec.set_min_chunk_size(self.min_chunk_size);

        io.encode(Encoded::Packet(pkt.into()), &codec)?;

        let packet = io.recv(&codec).await.map_err(ClientError::from)?.ok_or_else(|| {
            log::trace!("Mqtt server is disconnected during handshake");
            ClientError::Disconnected(None)
        })?;

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, true, pool));

        match packet {
            Decoded::Packet(codec::Packet::ConnectAck(pkt), _) => {
                log::trace!("Connect ack response from server: session: present: {:?}, return code: {:?}", pkt.session_present, pkt.return_code);
                if pkt.return_code == codec::ConnectAckReason::ConnectionAccepted {
                    shared.set_cap(max_send);
                    Ok(Client::new(
                        io,
                        shared,
                        pkt.session_present,
                        Seconds(keepalive_timeout),
                        max_receive,
                    ))
                } else {
                    Err(ClientError::Ack(pkt))
                }
            }
            Decoded::Packet(p, _) => Err(ProtocolError::unexpected_packet(
                p.packet_type(),
                "Expected CONNACK packet",
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
