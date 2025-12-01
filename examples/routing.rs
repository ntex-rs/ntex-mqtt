//! Examples show how to handle different mqtt topics
use ntex_mqtt::v3;

#[derive(Clone)]
struct Session;

#[derive(Debug)]
struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,routing=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", async |_| {
            v3::MqttServer::new(|handshake: v3::Handshake| async move {
                log::info!("new mqtt v3 connection: {:?}", handshake);
                Ok::<_, ServerError>(handshake.ack(Session, false))
            })
            .publish(
                // create router with default publish handler, default service handles
                // all topics that are not recognized by router
                v3::Router::new(|p: v3::Publish| async move {
                    log::info!("incoming publish: {:?} -> {:?}", p.id(), p.topic());
                    Ok(())
                })
                // this handler can handle topic1, topic2 and topic3 topics
                .resource(["topic1", "topic2", "topic3"], |p: v3::Publish| async move {
                    log::info!("incoming publish for {:?} -> {:?}", p.topic(), p.id());
                    Ok(())
                })
                // this handler can handle topic with dynamic section
                // ie `topic4/id1/files`, `topic4/id100/files`, etc
                .resource(
                    ["topic4/{id}/files"],
                    |p: v3::Publish| async move {
                        // get dynamic section from topic
                        let id = p.topic().get("id").unwrap();
                        log::info!(
                            "incoming publish for {:?} -> {:?} ({:?})",
                            p.topic(),
                            p.id(),
                            id
                        );
                        Ok(())
                    },
                ),
            )
            .finish()
        })?
        .workers(1)
        .run()
        .await
}
