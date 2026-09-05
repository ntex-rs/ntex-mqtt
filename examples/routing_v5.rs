//! Examples show how to handle different mqtt topics
use std::convert::Infallible;

use ntex::service::{cfg::SharedCfg, fn_service};
use ntex_mqtt::v5;

#[derive(Clone)]
struct MySession;

#[derive(Debug, thiserror::Error)]
#[error("Server error")]
struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

/// mqtt5 supports negative acks, so service error could be converted to PublishAck
impl std::convert::TryFrom<ServerError> for v5::PublishAck {
    type Error = ServerError;

    fn try_from(err: ServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,routing_v5=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", SharedCfg::default(), async |_| {
            v5::MqttServer::new(
                // create router with default publish handler, default service handles
                // all topics that are not recognized by router
                v5::Router::new(async move |p: v5::Publish| {
                    log::info!("incoming publish: {:?} -> {:?}", p.id(), p.topic());
                    Ok::<_, ServerError>(p.ack())
                })
                // this handler can handle topic1, topic2 and topic3 topics
                .resource(
                    ["topic1", "topic2", "topic3"],
                    async |_: &v5::Session<MySession>| {
                        Ok::<_, Infallible>(fn_service(async move |p: v5::Publish| {
                            log::info!("incoming publish for {:?} -> {:?}", p.topic(), p.id());
                            Ok(p.ack())
                        }))
                    },
                )
                // this handler can handle topic with dynamic section
                // ie `topic4/id1/files`, `topic4/id100/files`, etc
                .resource(["topic4/{id}/files"], async move |p: v5::Publish| {
                    // get dynamic section from topic
                    let id = p.topic().get("id").unwrap();
                    log::info!(
                        "incoming publish for {:?} -> {:?} ({:?})",
                        p.topic(),
                        p.id(),
                        id
                    );
                    Ok(p.ack())
                }),
            )
            .build(async move |msg: v5::Connect| {
                log::info!("new mqtt v3 connection: {:?}", msg);
                Ok::<_, ServerError>(msg.ack(MySession))
            })
        })?
        .workers(1)
        .run()
        .await
}
