use actix_test_server::TestServer;
use futures::future::ok;
use futures::Future;

use actix_mqtt::{ConnectAck, MqttServer};
use mqtt_codec as mqtt;

struct Session;

fn connect(packet: mqtt::Connect) -> impl Future<Item = ConnectAck<Session>, Error = ()> {
    println!("CONNECT: {:?}", packet);
    ok(ConnectAck::new(Session, false))
}

#[test]
fn test_simple() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_codec=info,actix_server=trace,actix_connector=trace,amqp_transport=trace",
    );
    env_logger::init();

    let _srv = TestServer::with(|| MqttServer::new(connect));

    Ok(())
}
