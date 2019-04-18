use futures::future::ok;
use futures::Future;

use actix_mqtt::{Connect, ConnectAck, MqttServer, Publish};
use actix_service::fn_service;

struct Session;

fn connect(packet: Connect) -> impl Future<Item = ConnectAck<Session>, Error = ()> {
    log::info!("new connection: {:?}", packet);
    ok(ConnectAck::new(Session, false))
}

fn publish(publish: Publish<Session>) -> impl Future<Item = (), Error = ()> {
    log::info!(
        "incoming publish: {:?} -> {:?}",
        publish.id(),
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
            MqttServer::new(connect).publish(fn_service(publish))
        })?
        .workers(1)
        .run()
}
