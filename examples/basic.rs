use ntex_mqtt::{MqttServer, v3, v5};

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
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", async |_| {
            MqttServer::new()
                .v3(v3::MqttServer::new(handshake_v3).publish(publish_v3))
                .v5(v5::MqttServer::new(handshake_v5).publish(publish_v5))
        })?
        .workers(1)
        .run()
        .await
}
