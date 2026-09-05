use std::convert::Infallible;

use ntex::service::cfg::SharedCfg;
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

async fn connect_v3(msg: v3::Connect) -> Result<v3::ConnectAck<MySession>, MyServerError> {
    log::info!("new connection: {:?}", msg);

    let session = MySession {
        client_id: msg.packet().client_id.to_string(),
    };

    Ok(msg.ack(session, false))
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

async fn connect_v5(msg: v5::Connect) -> Result<v5::ConnectAck<MySession>, MyServerError> {
    log::info!("new connection: {:?}", msg);

    let session = MySession {
        client_id: msg.packet().client_id.to_string(),
    };

    Ok(msg.ack(session))
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
                .v3(v3::MqttServer::new(async |_: &v3::Session<MySession>| {
                    Ok::<_, Infallible>(ntex::service(
                        async move |ses: &v3::Session<MySession>, req| publish_v3(ses, req).await,
                    ))
                })
                .build(connect_v3))
                .v5(v5::MqttServer::new(async |_con: &v5::Session<MySession>| {
                    Ok::<_, Infallible>(ntex::service(
                        async move |ses: &v5::Session<MySession>, req| publish_v5(ses, req).await,
                    ))
                })
                .build(connect_v5))
        })?
        .workers(1)
        .run()
        .await
}
