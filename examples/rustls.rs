use ntex::pipeline_factory;
use ntex::server::rustls::Acceptor;
use ntex_mqtt::{v3, v5, MqttError, MqttServer};
use rustls::internal::pemfile::{certs, rsa_private_keys};
use rustls::{NoClientAuth, ServerConfig};
use std::fs::File;
use std::io::BufReader;

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

async fn handshake_v3<Io>(
    handshake: v3::Handshake<Io>,
) -> Result<v3::HandshakeAck<Io, Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session, false))
}

async fn publish_v3(publish: v3::Publish) -> Result<(), ServerError> {
    log::info!("incoming publish: {:?} -> {:?}", publish.id(), publish.topic());
    Ok(())
}

async fn handshake_v5<Io>(
    handshake: v5::Handshake<Io>,
) -> Result<v5::HandshakeAck<Io, Session>, ServerError> {
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

    let mut tls_config = ServerConfig::new(NoClientAuth::new());

    // create self-signed certificates using:
    //   openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:4096 -keyout examples/key8.pem -out examples/cert.pem -days 365 -keyform PEM
    //   openssl rsa -in examples/key8.pem -out examples/key.pem
    let cert_file = &mut BufReader::new(File::open("./examples/cert.pem").unwrap());
    let key_file = &mut BufReader::new(File::open("./examples/key.pem").unwrap());

    let cert_chain = certs(cert_file).unwrap();
    let mut keys = rsa_private_keys(key_file).unwrap();
    tls_config.set_single_cert(cert_chain, keys.remove(0)).unwrap();

    let tls_acceptor = Acceptor::new(tls_config);

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:8883", move || {
            pipeline_factory(tls_acceptor.clone())
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
