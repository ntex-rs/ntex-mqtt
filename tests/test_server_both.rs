#![type_length_limit = "1638773"]
use std::convert::TryFrom;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::ok;
use ntex::server;

use ntex_mqtt::{v3, v5, MqttServer};

struct St;

#[derive(Debug)]
struct TestError;

impl From<()> for TestError {
    fn from(_: ()) -> Self {
        TestError
    }
}

impl TryFrom<TestError> for v5::PublishAck {
    type Error = TestError;

    fn try_from(err: TestError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
    env_logger::init();

    let srv = server::test_server(|| {
        MqttServer::new()
            .v3(v3::MqttServer::new(|con: v3::Handshake<_>| {
                ok::<_, TestError>(con.ack(St, false))
            })
            .publish(|_| ok::<_, TestError>(())))
            .v5(v5::MqttServer::new(|con: v5::Handshake<_>| ok::<_, TestError>(con.ack(St)))
                .publish(|p: v5::Publish| ok::<_, TestError>(p.ack())))
    });

    // connect to v5 server
    let client =
        v5::client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    sink.close();

    // connect to v3 server
    let client =
        v3::client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("#"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    sink.close();

    Ok(())
}
