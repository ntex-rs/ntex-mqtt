use std::{marker::PhantomData, rc::Rc};

use ntex_io::IoBoxed;
use ntex_net::connect::{self, Address, Connector};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{IntoServiceFactory, Service, ServiceCtx, ServiceFactory};
use ntex_util::time::{Seconds, timeout_checked};

use super::{Connect, connection::Client, error::ClientError, error::ProtocolError};
use crate::MqttServiceConfig;
use crate::v3::codec::{self, Decoded, Encoded};
use crate::v3::shared::{MqttShared, MqttSinkPool};

/// Mqtt client connector
pub struct MqttConnector<A, T> {
    connector: T,
    pool: Rc<MqttSinkPool>,
    _t: PhantomData<A>,
}

pub struct MqttConnectorService<A, T> {
    connector: T,
    cfg: Cfg<MqttServiceConfig>,
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
            connector: Connector::default(),
            pool: Rc::new(MqttSinkPool::default()),
            _t: PhantomData,
        }
    }
}

impl<A, T> MqttConnector<A, T>
where
    A: Address,
{
    /// Use custom connector
    pub fn connector<U, F>(self, connector: F) -> MqttConnector<A, U>
    where
        F: IntoServiceFactory<U, connect::Connect<A>, SharedCfg>,
        U: ServiceFactory<connect::Connect<A>, SharedCfg, Error = connect::ConnectError>,
        IoBoxed: From<U::Response>,
    {
        MqttConnector { connector: connector.into_factory(), pool: self.pool, _t: PhantomData }
    }
}

impl<A, T> ServiceFactory<Connect<A>, SharedCfg> for MqttConnector<A, T>
where
    A: Address,
    T: ServiceFactory<connect::Connect<A>, SharedCfg, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    type Response = Client;
    type Error = ClientError<codec::ConnectAck>;
    type Service = MqttConnectorService<A, T::Service>;
    type InitError = T::InitError;

    async fn create(&self, cfg: SharedCfg) -> Result<Self::Service, Self::InitError> {
        Ok(MqttConnectorService {
            cfg: cfg.get(),
            connector: self.connector.create(cfg).await?,
            pool: self.pool.clone(),
            _t: PhantomData,
        })
    }
}

impl<A, T> Service<Connect<A>> for MqttConnectorService<A, T>
where
    A: Address,
    T: Service<connect::Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    type Response = Client;
    type Error = ClientError<codec::ConnectAck>;

    ntex_service::forward_ready!(connector);
    ntex_service::forward_poll!(connector);
    ntex_service::forward_shutdown!(connector);

    /// Connect to mqtt server
    async fn call(
        &self,
        req: Connect<A>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Client, Self::Error> {
        let (addr, pkt) = req.into_parts();

        timeout_checked(self.cfg.handshake_timeout, self._connect(addr, pkt, ctx))
            .await
            .map_err(|_| ClientError::HandshakeTimeout)
            .and_then(|res| res)
    }
}

impl<A, T> MqttConnectorService<A, T>
where
    A: Address,
    T: Service<connect::Connect<A>, Error = connect::ConnectError>,
    IoBoxed: From<T::Response>,
{
    async fn _connect(
        &self,
        addr: A,
        pkt: codec::Connect,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Client, ClientError<codec::ConnectAck>> {
        let io: IoBoxed = ctx.call(&self.connector, connect::Connect::new(addr)).await?.into();
        let pool = self.pool.clone();
        let keepalive_timeout = pkt.keep_alive;
        let codec = codec::Codec::new();
        codec.set_max_size(self.cfg.max_size);
        codec.set_min_chunk_size(self.cfg.min_chunk_size);

        io.encode(Encoded::Packet(pkt.into()), &codec)?;

        let packet = io.recv(&codec).await.map_err(ClientError::from)?.ok_or_else(|| {
            log::trace!("Mqtt server is disconnected during handshake");
            ClientError::Disconnected(None)
        })?;

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, true, pool));

        match packet {
            Decoded::Packet(codec::Packet::ConnectAck(pkt), _) => {
                log::trace!(
                    "Connect ack response from server: session: present: {:?}, return code: {:?}",
                    pkt.session_present,
                    pkt.return_code
                );
                if pkt.return_code == codec::ConnectAckReason::ConnectionAccepted {
                    shared.set_cap(self.cfg.max_send as usize);
                    Ok(Client::new(
                        io,
                        shared,
                        pkt.session_present,
                        Seconds(keepalive_timeout),
                        self.cfg.max_receive as usize,
                        self.cfg.max_payload_buffer_size,
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
