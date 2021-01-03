#![type_length_limit = "1638773"]

use futures::future::ok;
use ntex::{fn_factory_with_config, fn_service};
use ntex_mqtt::v5::codec::PublishAckReason;
use ntex_mqtt::{v3, v5, MqttServer};

#[derive(Clone, Debug)]
struct MySession {
    // our custom session information
    client_id: String,
}

#[derive(Debug)]
struct MyServerError;

impl From<()> for MyServerError {
    fn from(_: ()) -> Self {
        MyServerError
    }
}

impl std::convert::TryFrom<MyServerError> for v5::PublishAck {
    type Error = MyServerError;

    fn try_from(err: MyServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn handshake_v3<Io>(
    handshake: v3::Handshake<Io>,
) -> Result<v3::HandshakeAck<Io, MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession { client_id: handshake.packet().client_id.to_string() };

    Ok(handshake.ack(session, false))
}

async fn publish_v3(
    session: v3::Session<MySession>,
    publish: v3::Publish,
) -> Result<(), MyServerError> {
    log::info!(
        "incoming publish ({:?}): {:?} -> {:?}",
        session.state(),
        publish.id(),
        publish.topic()
    );

    // example: only "my-client-id" may publish
    if session.state().client_id == "my-client-id" {
        Ok(())
    } else {
        // with MQTTv3 we can only close the connection
        Err(MyServerError)
    }
}

async fn handshake_v5<Io>(
    handshake: v5::Handshake<Io>,
) -> Result<v5::HandshakeAck<Io, MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession { client_id: handshake.packet().client_id.to_string() };

    Ok(handshake.ack(session))
}

async fn publish_v5(
    session: v5::Session<MySession>,
    publish: v5::Publish,
) -> Result<v5::PublishAck, MyServerError> {
    log::info!(
        "incoming publish ({:?}) : {:?} -> {:?}",
        session.state(),
        publish.id(),
        publish.topic()
    );

    // example: only "my-client-id" may publish
    if session.state().client_id == "my-client-id" {
        Ok(publish.ack())
    } else {
        Ok(publish.ack().reason_code(PublishAckReason::NotAuthorized))
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "session=trace,ntex=trace,ntex_mqtt=trace,basic=trace");
    env_logger::init();

    log::info!("Hello");

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:1883", || {
            MqttServer::new()
                .v3(v3::MqttServer::new(handshake_v3).publish(fn_factory_with_config(
                    |session: v3::Session<MySession>| {
                        ok::<_, MyServerError>(fn_service(move |req| {
                            publish_v3(session.clone(), req)
                        }))
                    },
                )))
                .v5(v5::MqttServer::new(handshake_v5).publish(fn_factory_with_config(
                    |session: v5::Session<MySession>| {
                        ok::<_, MyServerError>(fn_service(move |req| {
                            publish_v5(session.clone(), req)
                        }))
                    },
                )))
        })?
        .workers(1)
        .run()
        .await
}
