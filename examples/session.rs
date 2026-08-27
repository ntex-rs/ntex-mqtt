use ntex::service::{cfg::SharedCfg, fn_factory_with_config, fn_service_st};
use ntex_mqtt::{MqttServer, v3, v5, v5::codec::PublishAckReason};

#[derive(Clone, Debug)]
struct MySession {
    // our custom session information
    client_id: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Server error")]
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

async fn handshake_v3(
    handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession {
        client_id: handshake.packet().client_id.to_string(),
    };

    Ok(handshake.ack(session, false))
}

async fn publish_v3(
    session: &v3::Session<MySession>,
    publish: v3::Publish,
) -> Result<(), MyServerError> {
    log::info!(
        "incoming publish ({:?}): {:?} -> {:?}",
        &*session,
        publish.id(),
        publish.topic()
    );

    // example: only "my-client-id" may publish
    if session.client_id == "my-client-id" {
        Ok(())
    } else {
        // with MQTTv3 we can only close the connection
        Err(MyServerError)
    }
}

async fn handshake_v5(
    handshake: v5::Handshake,
) -> Result<v5::HandshakeAck<MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession {
        client_id: handshake.packet().client_id.to_string(),
    };

    Ok(handshake.ack(session))
}

async fn publish_v5(
    session: &v5::Session<MySession>,
    publish: v5::Publish,
) -> Result<v5::PublishAck, MyServerError> {
    log::info!(
        "incoming publish ({:?}) : {:?} -> {:?}",
        &*session,
        publish.id(),
        publish.topic()
    );

    // example: only "my-client-id" may publish
    if session.client_id == "my-client-id" {
        Ok(publish.ack())
    } else {
        Ok(publish.ack().reason_code(PublishAckReason::NotAuthorized))
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "session=trace,ntex=trace,ntex_mqtt=trace,basic=trace");
    env_logger::init();

    log::info!("Hello");

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", SharedCfg::default(), async |_| {
            MqttServer::new()
                .v3(
                    v3::MqttServer::new(fn_factory_with_config(async |_: &v3::Connection<()>| {
                        Ok::<_, MyServerError>(fn_service_st(
                            async move |ses: &v3::Session<MySession>, req| {
                                publish_v3(ses, req).await
                            },
                        ))
                    }))
                    .build(handshake_v3),
                )
                .v5(v5::MqttServer::new(fn_factory_with_config(
                    async |_con: &v5::Connection<()>| {
                        Ok::<_, MyServerError>(fn_service_st(
                            async move |ses: &v5::Session<MySession>, req| {
                                publish_v5(ses, req).await
                            },
                        ))
                    },
                ))
                .build(handshake_v5))
        })?
        .workers(1)
        .run()
        .await
}
