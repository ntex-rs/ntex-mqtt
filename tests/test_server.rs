#![type_length_limit = "1134953"]
use bytes::Bytes;
use bytestring::ByteString;
use futures::future::ok;
use ntex::server;

use ntex_mqtt::v3::{client, Connect, ConnectAck, MqttServer};

struct St;

async fn connect<Io>(packet: Connect<Io>) -> Result<ConnectAck<Io, St>, ()> {
    println!("CONNECT: {:?}", packet);
    Ok(packet.ack(St, false))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
    env_logger::init();

    let srv = server::test_server(|| MqttServer::new(connect).publish(|_t| ok(())).finish());

    // connect to server
    let client = client::MqttConnector::new(srv.addr())
        .client_id(ByteString::from_static("user"))
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());

    sink.close();
    Ok(())
}
