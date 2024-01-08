use std::{fs::File, io::BufReader, sync::Arc};

use ntex::service::chain_factory;
use ntex_mqtt::{v3, v5, MqttError, MqttServer};
use ntex_tls::rustls::TlsAcceptor;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, rsa_private_keys};

#[derive(Clone)]
struct Session;

#[derive(Debug)]
struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

impl std::convert::TryFrom<ServerError> for v5::PublishAck {
    type Error = ServerError;

    fn try_from(err: ServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn handshake_v3(
    handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session, false))
}

async fn publish_v3(publish: v3::Publish) -> Result<(), ServerError> {
    log::info!("incoming publish: {:?} -> {:?}", publish.id(), publish.topic());
    Ok(())
}

async fn handshake_v5(
    handshake: v5::Handshake,
) -> Result<v5::HandshakeAck<Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session))
}

async fn publish_v5(publish: v5::Publish) -> Result<v5::PublishAck, ServerError> {
    log::info!("incoming publish: {:?} -> {:?}", publish.id(), publish.topic());
    Ok(publish.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");
    env_logger::init();

    // create self-signed certificates using:
    //   openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:4096 -keyout examples/key8.pem -out examples/cert.pem -days 365 -keyform PEM
    //   openssl rsa -in examples/key8.pem -out examples/key.pem
    let cert_file = &mut BufReader::new(File::open("./examples/cert.pem").unwrap());
    let key_file = &mut BufReader::new(File::open("./examples/key.pem").unwrap());

    let keys = PrivateKey(rsa_private_keys(key_file).unwrap().remove(0));
    let cert_chain =
        certs(cert_file).unwrap().iter().map(|c| Certificate(c.to_vec())).collect();
    let tls_config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert_chain, keys)
        .unwrap();

    let tls_acceptor = Arc::new(tls_config);

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:8883", move |_| {
            chain_factory(TlsAcceptor::new(tls_acceptor.clone()))
                .map_err(|_err| MqttError::Service(ServerError {}))
                .and_then(
                    MqttServer::new()
                        .v3(v3::MqttServer::new(handshake_v3).publish(publish_v3))
                        .v5(v5::MqttServer::new(handshake_v5).publish(publish_v5)),
                )
        })?
        .workers(1)
        .run()
        .await
}
