use ntex::SharedCfg;
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

async fn connect_v3(msg: v3::Connect) -> Result<v3::ConnectAck<Session>, ServerError> {
    log::info!("new connection: {:?}", msg);
    Ok(msg.ack(Session, false))
}

async fn publish_v3(publish: v3::Publish) -> Result<(), ServerError> {
    log::info!(
        "incoming publish: {:?} -> {:?}",
        publish.id(),
        publish.topic()
    );
    Ok(())
}

async fn connect_v5(msg: v5::Connect) -> Result<v5::ConnectAck<Session>, ServerError> {
    log::info!("new connection: {:?}", msg);
    Ok(msg.ack(Session))
}

async fn publish_v5(publish: v5::Publish) -> Result<v5::PublishAck, ServerError> {
    log::info!(
        "incoming publish: {:?} -> {:?}",
        publish.id(),
        publish.topic()
    );
    Ok(publish.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", SharedCfg::default(), async |_| {
            MqttServer::new()
                .v3(v3::MqttServer::new(publish_v3).build(connect_v3))
                .v5(v5::MqttServer::new(publish_v5).build(connect_v5))
        })?
        .workers(1)
        .run()
        .await
}
