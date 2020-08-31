use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::Either;
use futures::{Future, FutureExt, SinkExt, StreamExt};
use ntex::connect::{self, Address, Connect, Connector};
use ntex::rt::time::delay_for;
use ntex::service::Service;
use ntex_codec::{AsyncRead, AsyncWrite, Framed};

#[cfg(feature = "openssl")]
use ntex::connect::openssl::{OpensslConnector, SslConnector};

#[cfg(feature = "rustls")]
use ntex::connect::rustls::{ClientConfig, RustlsConnector};

use super::{codec, connection::Client, error::ClientError, error::ProtocolError};
use crate::utils::Select;

/// Mqtt client connector
pub struct MqttConnector<A, T> {
    address: A,
    connector: T,
    pkt: codec::Connect,
    max_send: usize,
    max_receive: usize,
    max_packet_size: u32,
    handshake_timeout: u64,
    disconnect_timeout: u64,
}

impl<A> MqttConnector<A, ()>
where
    A: Address + Clone,
{
    #[allow(clippy::new_ret_no_self)]
    /// Create new mqtt connector
    pub fn new(address: A) -> MqttConnector<A, Connector<A>> {
        MqttConnector {
            address,
            pkt: codec::Connect::default(),
            connector: Connector::default(),
            max_send: 16,
            max_receive: 16,
            max_packet_size: 64 * 1024,
            handshake_timeout: 0,
            disconnect_timeout: 3000,
        }
    }
}

impl<A, T> MqttConnector<A, T>
where
    A: Address + Clone,
    T: Service<Request = Connect<A>, Error = connect::ConnectError>,
    T::Response: AsyncRead + AsyncWrite + Unpin + 'static,
{
    #[inline]
    /// Create new client and provide client id
    pub fn client_id(mut self, client_id: ByteString) -> Self {
        self.pkt.client_id = client_id;
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
    pub fn keep_alive(mut self, val: u16) -> Self {
        self.pkt.keep_alive = val;
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
    /// Set max send packets number
    ///
    /// Number of in-flight outgoing publish packets. By default receive max is set to 16 packets.
    /// To disable in-flight limit set value to 0.
    pub fn max_send(mut self, val: u16) -> Self {
        self.max_send = val as usize;
        self
    }

    #[inline]
    /// Set max receive packets number
    ///
    /// Number of in-flight incoming publish packets. By default receive max is set to 16 packets.
    /// To disable in-flight limit set value to 0.
    pub fn max_receive(mut self, val: u16) -> Self {
        self.max_receive = val as usize;
        self
    }

    #[inline]
    /// Max incoming packet size.
    ///
    /// To disable max size limit set value to 0.
    pub fn max_packet_size(mut self, val: u32) -> Self {
        self.max_packet_size = val;
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

    /// Set handshake timeout in milliseconds.
    ///
    /// Handshake includes `connect` packet and response `connect-ack`.
    /// By default handshake timeuot is disabled.
    pub fn handshake_timeout(mut self, timeout: usize) -> Self {
        self.handshake_timeout = timeout as u64;
        self
    }

    /// Set client connection disconnect timeout in milliseconds.
    ///
    /// Defines a timeout for disconnect connection. If a disconnect procedure does not complete
    /// within this time, the connection get dropped.
    ///
    /// To disable timeout set value to 0.
    ///
    /// By default disconnect timeout is set to 3 seconds.
    pub fn disconnect_timeout(mut self, timeout: usize) -> Self {
        self.disconnect_timeout = timeout as u64;
        self
    }

    /// Use custom connector
    pub fn connector<U>(self, connector: U) -> MqttConnector<A, U>
    where
        U: Service<Request = Connect<A>, Error = connect::ConnectError>,
        U::Response: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        MqttConnector {
            connector,
            pkt: self.pkt,
            address: self.address,
            max_send: self.max_send,
            max_receive: self.max_receive,
            max_packet_size: self.max_packet_size,
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
        }
    }

    #[cfg(feature = "openssl")]
    /// Use openssl connector
    pub fn openssl(self, connector: SslConnector) -> MqttConnector<A, OpensslConnector<A>> {
        MqttConnector {
            pkt: self.pkt,
            address: self.address,
            max_send: self.max_send,
            max_receive: self.max_receive,
            max_packet_size: self.max_packet_size,
            connector: OpensslConnector::new(connector),
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
        }
    }

    #[cfg(feature = "rustls")]
    /// Use rustls connector
    pub fn rustls(self, config: ClientConfig) -> MqttConnector<A, RustlsConnector<A>> {
        use std::sync::Arc;

        MqttConnector {
            pkt: self.pkt,
            address: self.address,
            max_send: self.max_send,
            max_receive: self.max_receive,
            max_packet_size: self.max_packet_size,
            connector: RustlsConnector::new(Arc::new(config)),
            handshake_timeout: self.handshake_timeout,
            disconnect_timeout: self.disconnect_timeout,
        }
    }

    /// Connect to mqtt server
    pub fn connect(&self) -> impl Future<Output = Result<Client<T::Response>, ClientError>> {
        if self.handshake_timeout > 0 {
            Either::Left(
                Select::new(
                    delay_for(Duration::from_millis(self.handshake_timeout)),
                    self._connect(),
                )
                .map(|result| match result {
                    either::Either::Left(_) => Err(ClientError::HandshakeTimeout),
                    either::Either::Right(res) => res.map_err(From::from),
                }),
            )
        } else {
            Either::Right(self._connect())
        }
    }

    fn _connect(&self) -> impl Future<Output = Result<Client<T::Response>, ClientError>> {
        let fut = self.connector.call(Connect::new(self.address.clone()));
        let pkt = self.pkt.clone();
        let max_send = self.max_send;
        let max_receive = self.max_receive;
        let max_packet_size = self.max_packet_size;
        let keepalive_timeout = pkt.keep_alive as u64;
        let disconnect_timeout = self.disconnect_timeout;

        async move {
            let io = fut.await?;
            let mut framed = Framed::new(io, codec::Codec::new().max_size(max_packet_size));

            framed.send(codec::Packet::Connect(pkt)).await?;

            let packet = framed
                .next()
                .await
                .ok_or_else(|| {
                    log::trace!("Mqtt server is disconnected during handshake");
                    ClientError::Disconnected
                })
                .and_then(|res| res.map_err(|e| ClientError::from(ProtocolError::from(e))))?;

            match packet {
                codec::Packet::ConnectAck { session_present, return_code } => {
                    log::trace!("Connect ack response from server: session: present: {:?}, return code: {:?}", session_present, return_code);
                    if return_code == codec::ConnectAckReason::ConnectionAccepted {
                        Ok(Client::new(
                            framed,
                            session_present,
                            keepalive_timeout,
                            disconnect_timeout,
                            max_send,
                            max_receive,
                        ))
                    } else {
                        Err(ClientError::Ack { session_present, return_code })
                    }
                }
                p => Err(ProtocolError::Unexpected(
                    p.packet_type(),
                    "Expected CONNECT-ACK packet",
                )
                .into()),
            }
        }
    }
}
