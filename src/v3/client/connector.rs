use std::{fmt, marker::PhantomData, rc::Rc};

use ntex_error::Error;
use ntex_io::IoBoxed;
use ntex_net::connect::{self, Address, Connector};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{BindCtx, Ctx, IntoService, Service};
use ntex_util::time::{Seconds, timeout_checked};

use super::{Connect, connection::Client, error::MqttClientError, error::MqttProtocolError};
use crate::MqttServiceConfig;
use crate::v3::codec::{self, Decoded, Encoded};
use crate::v3::shared::{MqttShared, MqttSinkPool};

/// Service that opens a transport and performs an MQTT 3.1.1 client handshake.
pub struct MqttConnector<A, T> {
    /// Underlying transport connector, also used for readiness and shutdown.
    connector: T,
    /// Reusable acknowledgement channels shared by connections from this service.
    pool: Rc<MqttSinkPool>,
    /// Address type accepted by this connector.
    _t: PhantomData<A>,
}

impl<A, T> fmt::Debug for MqttConnector<A, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::MqttConnector").finish()
    }
}

impl<A> MqttConnector<A, ()>
where
    A: Address,
{
    #[allow(clippy::new_ret_no_self)]
    /// Create a client connector using the default network transport connector.
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
    /// Replace the transport connector while retaining the shared sink pool.
    pub fn connector<U, F>(self, connector: F) -> MqttConnector<A, U>
    where
        F: IntoService<U, SharedCfg, connect::Connect<A>>,
        U: Service<SharedCfg, connect::Connect<A>, Error = Error<connect::ConnectError>>,
        IoBoxed: From<U::Res>,
    {
        MqttConnector {
            connector: connector.into_service(),
            pool: self.pool,
            _t: PhantomData,
        }
    }
}

impl<A, S> Service<SharedCfg, Connect<A>> for MqttConnector<A, S>
where
    A: Address,
    S: Service<SharedCfg, connect::Connect<A>, Error = Error<connect::ConnectError>>,
    IoBoxed: From<S::Res>,
{
    type Res = Client;
    type Error = Error<MqttClientError<codec::ConnectAck>>;

    fn bind(&self, ctx: BindCtx<'_>) {
        ctx.bind(&self.connector);
    }

    async fn ready(&self, ctx: Ctx<'_, Self, SharedCfg>) -> Result<(), Self::Error> {
        ctx.ready(&self.connector).await.map_err(Error::map_err)
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, SharedCfg>) {
        ctx.shutdown(&self.connector).await;
    }

    /// Open the transport and await `CONNACK` within the configured handshake timeout.
    async fn call(
        &self,
        req: Connect<A>,
        ctx: Ctx<'_, Self, SharedCfg>,
    ) -> Result<Client, Self::Error> {
        let (addr, pkt) = req.into_parts();
        let cfg = ctx.st().get::<MqttServiceConfig>();

        timeout_checked(
            cfg.handshake_timeout,
            self.connect_inner(addr, pkt, ctx, cfg),
        )
        .await
        .map_err(|()| Error::from(MqttClientError::ConnectTimeout))
        .and_then(|res| res)
    }
}

impl<A, S> MqttConnector<A, S>
where
    A: Address,
    S: Service<SharedCfg, connect::Connect<A>, Error = Error<connect::ConnectError>>,
    IoBoxed: From<S::Res>,
{
    /// Send `CONNECT`, validate `CONNACK`, and retain negotiated limits for the client.
    async fn connect_inner(
        &self,
        addr: A,
        pkt: codec::Connect,
        ctx: Ctx<'_, Self, SharedCfg>,
        cfg: Cfg<MqttServiceConfig>,
    ) -> Result<Client, Error<MqttClientError<codec::ConnectAck>>> {
        let io: IoBoxed = ctx
            .call(&self.connector, connect::Connect::new(addr))
            .await
            .map_err(Error::map_err)?
            .into();
        let pool = self.pool.clone();
        let keepalive_timeout = pkt.keep_alive;
        let codec = codec::Codec::from_config(&cfg);

        io.encode(Encoded::Packet(pkt.into()), &codec)
            .map_err(Error::from_err)?;

        let packet = io
            .recv(&codec)
            .await
            .map_err(MqttClientError::from)?
            .ok_or_else(|| {
                log::trace!("Mqtt server is disconnected during handshake");
                MqttClientError::Disconnected(None)
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
                    shared.set_cap(cfg.max_send as usize);
                    Ok(Client::new(
                        io,
                        shared,
                        pkt.session_present,
                        Seconds(keepalive_timeout),
                        cfg.max_receive,
                        cfg.max_payload_buffer_size,
                    ))
                } else {
                    Err(MqttClientError::Ack(pkt).into())
                }
            }
            Decoded::Packet(p, _) => Err(MqttClientError::from(
                MqttProtocolError::unexpected_packet(p.packet_type(), "Expected CONNACK packet"),
            )
            .into()),
            Decoded::Publish(..) => {
                Err(MqttClientError::from(MqttProtocolError::unexpected_packet(
                    crate::types::packet_type::PUBLISH_START,
                    "CONNACK packet expected from server first [MQTT-3.2.0-1]",
                ))
                .into())
            }
            Decoded::PayloadChunk(..) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntex_bytes::BytesMut;
    use ntex_codec::Decoder;
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::Pipeline;
    use ntex_util::time::{Millis, timeout};
    use std::cell::{Cell, RefCell};

    struct Transport {
        io: RefCell<Option<IoBoxed>>,
        shutdowns: Rc<Cell<usize>>,
    }

    impl Service<SharedCfg, connect::Connect<&'static str>> for Transport {
        type Res = IoBoxed;
        type Error = Error<connect::ConnectError>;

        async fn call(
            &self,
            _: connect::Connect<&'static str>,
            _: Ctx<'_, Self, SharedCfg>,
        ) -> Result<Self::Res, Self::Error> {
            Ok(self.io.borrow_mut().take().unwrap())
        }

        async fn shutdown(&self, _: Ctx<'_, Self, SharedCfg>) {
            self.shutdowns.set(self.shutdowns.get() + 1);
        }
    }

    type TestConnector =
        Pipeline<Connect<&'static str>, Client, Error<MqttClientError<codec::ConnectAck>>>;

    fn setup() -> (IoTest, Rc<Cell<usize>>, TestConnector) {
        let (peer, transport) = IoTest::create();
        peer.remote_buffer_cap(1024);
        let cfg = SharedCfg::new("test")
            .add(MqttServiceConfig::new().set_max_send(3))
            .build();
        let io = Io::new(transport, cfg.clone());
        let shutdowns = Rc::new(Cell::new(0));
        let connector = MqttConnector::<&'static str, ()>::new().connector(Transport {
            io: RefCell::new(Some(io.into())),
            shutdowns: shutdowns.clone(),
        });
        (peer, shutdowns, Pipeline::new(cfg, connector))
    }

    /// STATEMENT: An accepted client handshake retains session state and forwards connector shutdown.
    ///
    /// HOW:
    /// - Preload an accepted `CONNACK`, connect with an explicit client identifier, and decode the emitted `CONNECT`.
    /// - Assert the client identifier, resumed-session flag, configured send credit, and one transport shutdown call.
    #[ntex::test]
    async fn test_handshake_and_transport_shutdown() {
        let (peer, shutdowns, connector) = setup();
        peer.write(b"\x20\x02\x01\x00");
        connector.ready().await.unwrap();
        let client = timeout(
            Millis(2000),
            connector.call(Connect::new("broker").client_id("unit")),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(client.session_present());
        assert_eq!(client.sink().credit(), 3);
        let wire = timeout(Millis(2000), peer.read()).await.unwrap().unwrap();
        let Some(Decoded::Packet(codec::Packet::Connect(packet), _)) = codec::Codec::new()
            .decode(&mut BytesMut::from(wire))
            .unwrap()
        else {
            panic!("expected CONNECT")
        };
        assert_eq!(packet.client_id, "unit");
        connector.shutdown().await;
        assert_eq!(shutdowns.get(), 1);
    }

    /// STATEMENT: The client connector reports refusal and wrong-first-packet handshake errors distinctly.
    ///
    /// HOW:
    /// - Preload a refused `CONNACK`, then a `PINGRESP` on a separate transport.
    /// - Assert the refusal reason is retained and the unexpected-packet error identifies `PINGRESP`.
    #[ntex::test]
    async fn test_refused_and_unexpected_handshake_packets() {
        let (peer, _shutdowns, connector) = setup();
        peer.write(b"\x20\x02\x00\x05");
        let err = timeout(
            Millis(2000),
            connector.call(Connect::new("broker").client_id("unit")),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(&*err, MqttClientError::Ack(ack) if ack.return_code == codec::ConnectAckReason::NotAuthorized)
        );

        let (peer, _shutdowns, connector) = setup();
        peer.write(b"\xd0\x00");
        let err = timeout(
            Millis(2000),
            connector.call(Connect::new("broker").client_id("unit")),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(&*err, MqttClientError::Protocol(MqttProtocolError::ProtocolViolation(err))
            if matches!(err.inner, crate::error::ViolationInner::UnexpectedPacket { packet_type: crate::types::packet_type::PINGRESP, .. }))
        );
    }
}
