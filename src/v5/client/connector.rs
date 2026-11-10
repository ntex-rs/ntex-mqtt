use std::{fmt, marker::PhantomData, num::NonZero, rc::Rc};

use ntex_error::Error;
use ntex_io::IoBoxed;
use ntex_net::connect::{self, Address, Connector};
use ntex_service::cfg::{Cfg, SharedCfg};
use ntex_service::{BindCtx, Ctx, IntoService, Service};
use ntex_util::time::{Seconds, timeout_checked};

use super::codec::{self, Decoded, Encoded, Packet};
use super::{Connect, connection::Client, error::MqttClientError, error::MqttProtocolError};
use crate::MqttServiceConfig;
use crate::v5::shared::{MqttShared, MqttSinkPool};

/// Service that opens a transport and performs an MQTT 5 client handshake.
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
        f.debug_struct("v5::MqttConnector").finish()
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
    type Error = Error<MqttClientError<Box<codec::ConnectAck>>>;

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
    ) -> Result<Client, Error<MqttClientError<Box<codec::ConnectAck>>>> {
        let io: IoBoxed = ctx
            .call(&self.connector, connect::Connect::new(addr))
            .await
            .map_err(Error::map_err)?
            .into();
        let keep_alive = pkt.keep_alive;
        let max_packet_size = pkt.max_packet_size.map_or(0, NonZero::get);
        let max_receive = pkt.receive_max.map_or(65535, NonZero::get);
        let pool = self.pool.clone();

        // the client advertises its own inbound limit in CONNECT
        let codec = codec::Codec::from_config(&cfg);
        codec.set_max_inbound_size(max_packet_size);

        io.encode(Encoded::Packet(Packet::Connect(Box::new(pkt))), &codec)
            .map_err(Error::from_err)?;

        let packet = io
            .recv(&codec)
            .await
            .map_err(MqttClientError::from)?
            .ok_or_else(|| {
                log::trace!("Mqtt server is disconnected during handshake");
                MqttClientError::Disconnected(None)
            })?;

        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, pool));
        shared.set_client();
        match packet {
            Decoded::Packet(Packet::ConnectAck(pkt), ..) => {
                log::trace!("Connect ack response from server: {pkt:#?}");
                if pkt.reason_code == codec::ConnectAckReason::Success {
                    // set max outbound (encoder) packet size
                    if let Some(size) = pkt.max_packet_size {
                        shared.codec.set_max_outbound_size(size);
                    }
                    // server keep-alive
                    let keep_alive = pkt.server_keepalive_sec.unwrap_or(keep_alive);

                    shared.set_cap(pkt.receive_max.get() as usize);

                    Ok(Client::new(
                        io,
                        shared,
                        pkt,
                        max_receive,
                        Seconds(keep_alive),
                        cfg,
                    ))
                } else {
                    Err(MqttClientError::Ack(pkt).into())
                }
            }
            Decoded::Packet(packet, ..) => {
                Err(MqttClientError::from(MqttProtocolError::unexpected_packet(
                    packet.packet_type(),
                    "CONNACK packet expected from server first [MQTT-3.2.0-1]",
                ))
                .into())
            }
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
    use std::{cell::Cell, pin::pin};

    use ntex_io::{Io, testing::IoTest};
    use ntex_service::Pipeline;
    use ntex_util::{channel::condition::Condition, future::lazy};

    use super::*;

    /// STATEMENT: MQTT connector shutdown awaits completion of the underlying transport connector's shutdown.
    ///
    /// HOW:
    /// - Wrap a transport connector whose shutdown waits for a notification before setting a completion flag.
    /// - Poll MQTT connector shutdown and assert it is pending with the flag unset.
    /// - Notify the transport connector, await completion, and verify the flag is set.
    #[ntex::test]
    async fn test_shutdown_waits_for_connector() {
        struct Transport {
            gate: Condition,
            stopped: Rc<Cell<bool>>,
        }

        impl Service<SharedCfg, connect::Connect<&'static str>> for Transport {
            type Res = Io;
            type Error = Error<connect::ConnectError>;

            async fn call(
                &self,
                _: connect::Connect<&'static str>,
                ctx: Ctx<'_, Self, SharedCfg>,
            ) -> Result<Io, Self::Error> {
                Ok(Io::new(IoTest::create().0, ctx.st().clone()))
            }

            async fn shutdown(&self, _: Ctx<'_, Self, SharedCfg>) {
                self.gate.wait().await;
                self.stopped.set(true);
            }
        }

        let gate = Condition::new();
        let stopped = Rc::new(Cell::new(false));
        let connector = MqttConnector::new().connector(Transport {
            gate: gate.clone(),
            stopped: stopped.clone(),
        });
        let svc = Pipeline::new(SharedCfg::default(), connector);
        let mut shutdown = pin!(svc.shutdown());
        assert!(lazy(|cx| shutdown.as_mut().poll(cx).is_pending()).await);
        assert!(!stopped.get());
        gate.notify(());
        shutdown.await;
        assert!(stopped.get());
    }
}
