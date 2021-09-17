use ntex::rt::net::TcpStream;
use ntex::server::openssl::Acceptor;
use ntex::service::pipeline_factory;
use ntex_mqtt::{v3, v5, MqttError, MqttServer};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use tokio_openssl::SslStream;

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
    handshake: v3::Handshake<SslStream<TcpStream>>,
) -> Result<v3::HandshakeAck<SslStream<TcpStream>, Session>, ServerError> {
    log::info!("new connection: {:?}", handshake);
    Ok(handshake.ack(Session, false))
}

async fn publish_v3(publish: v3::Publish) -> Result<(), ServerError> {
    log::info!("incoming publish: {:?} -> {:?}", publish.id(), publish.topic());
    Ok(())
}

async fn handshake_v5(
    handshake: v5::Handshake<SslStream<TcpStream>>,
) -> Result<v5::HandshakeAck<SslStream<TcpStream>, Session>, ServerError> {
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
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder.set_private_key_file("./examples/key.pem", SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file("./examples/cert.pem").unwrap();
    let acceptor = builder.build();

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:8883", move || {
            pipeline_factory(Acceptor::new(acceptor.clone()))
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
