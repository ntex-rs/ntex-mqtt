use ntex::server;
use ntex::util::{ByteString, Bytes, Ready};

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
    let srv = server::test_server(|| {
        MqttServer::new()
            .v3(v3::MqttServer::new(|con: v3::Handshake| {
                Ready::Ok::<_, TestError>(con.ack(St, false))
            })
            .publish(|_| Ready::Ok::<_, TestError>(()))
            .finish())
            .v5(v5::MqttServer::new(|con: v5::Handshake| {
                Ready::Ok::<_, TestError>(con.ack(St))
            })
            .publish(|p: v5::Publish| Ready::Ok::<_, TestError>(p.ack()))
            .finish())
    });

    // connect to v5 server
    let client =
        v5::client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    sink.close();

    // connect to v3 server
    let client =
        v3::client::MqttConnector::new(srv.addr()).client_id("user").connect().await.unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic"), Bytes::new()).send_at_least_once().await;
    assert!(res.is_ok());
    sink.close();

    Ok(())
}
