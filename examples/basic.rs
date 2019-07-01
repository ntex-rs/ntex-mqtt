use actix_mqtt::{Connect, ConnectAck, MqttServer, Publish};
use futures::future::ok;
use futures::Future;

struct Session;

fn connect<Io>(
    connect: Connect<Io>,
) -> impl Future<Item = ConnectAck<Io, Session>, Error = ()> {
    log::info!("new connection: {:?}", connect);
    ok(connect.ack(Session, false))
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
            MqttServer::new(connect).finish(publish)
        })?
        .workers(1)
        .run()
}
