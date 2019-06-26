use actix_ioframe as ioframe;
use actix_service::Service;
use actix_test_server::TestServer;
use bytes::Bytes;
use futures::future::ok;
use futures::{Future, Sink};
use mqtt_codec as mqtt;
use string::TryFrom;

use actix_mqtt::{Connect, ConnectAck, MqttServer};

struct Session;

fn connect(packet: Connect) -> impl Future<Item = ConnectAck<Session>, Error = ()> {
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

    let mut srv = TestServer::with(|| MqttServer::new(connect));

    struct ClientSession;

    let mut client = ioframe::Builder::new()
        .service(|conn: ioframe::Connect<_>| {
            // construct connect result
            let result = conn
                .codec(mqtt::Codec::new())
                .state(ClientSession)
                .into_result();

            // send Connect packet
            result
                .send(mqtt::Packet::Connect(mqtt::Connect {
                    protocol: mqtt::Protocol::default(),
                    clean_session: true,
                    keep_alive: 10,
                    last_will: None,
                    client_id: string::String::try_from(Bytes::from_static(b"user")).unwrap(),
                    username: None,
                    password: None,
                }))
                .map_err(|_| panic!())
        })
        .finish(|t: ioframe::Item<_, mqtt::Codec>| {
            t.sink().close();
            Ok(None)
        });

    let conn = srv
        .block_on(
            actix_connect::default_connector()
                .call(actix_connect::Connect::with(String::new(), srv.addr())),
        )
        .unwrap();

    srv.block_on(client.call(conn.into_parts().0)).unwrap();

    Ok(())
}
