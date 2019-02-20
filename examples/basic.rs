use futures::future::ok;
use futures::Future;

use actix_mqtt::{MqttServer, Publish};
use mqtt_codec as mqtt;

struct Session;

fn connect(
    packet: mqtt::Connect,
) -> impl Future<Item = Result<Session, mqtt::ConnectCode>, Error = ()> {
    log::info!("new connection: {:?}", packet);
    ok(Ok(Session))
}

fn publish(publish: Publish<Session>) -> impl Future<Item = (), Error = ()> {
    log::info!(
        "incoming publish: {:?} -> {:?}",
        publish.packet_id(),
        publish.topic()
    );
    ok(())
}

fn main() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_server=trace,actix_mqtt=trace,basic=trace",
    );
    env_logger::init();

    actix_server::Server::build()
        .bind("mqtt", "127.0.0.1:1883", || {
            MqttServer::new(connect).publish(publish)
        })
        .unwrap()
        .workers(1)
        .run();

    Ok(())
}
