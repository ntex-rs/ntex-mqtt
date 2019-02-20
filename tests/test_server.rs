// use actix_connector::{Connect, Connector};
use actix_service::{IntoNewService, NewService, Service};
use actix_test_server::TestServer;
use futures::future::{err, lazy, ok};
use futures::Future;

use actix_mqtt::MqttServer;
use mqtt_codec as mqtt;
// use amqp_transport::{self, client, sasl, Configuration};

struct Session;

fn connect(
    packet: mqtt::Connect,
) -> impl Future<Item = Result<Session, mqtt::ConnectCode>, Error = ()> {
    ok(Ok(Session))
}

#[test]
fn test_simple() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_codec=info,actix_server=trace,actix_connector=trace,amqp_transport=trace",
    );
    env_logger::init();

    let mut srv = TestServer::with(|| MqttServer::new().connect(connect));

    Ok(())
}
