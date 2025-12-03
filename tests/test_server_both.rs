use ntex::util::{ByteString, Bytes, Ready};
use ntex::{ServiceFactory, SharedCfg, server};

use ntex_mqtt::{MqttServer, v3, v5};

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
    let srv = server::test_server(async || {
        MqttServer::new()
            .v3(v3::MqttServer::new(|con: v3::Handshake| {
                Ready::Ok::<_, TestError>(con.ack(St, false))
            })
            .publish(|_| Ready::Ok::<_, TestError>(())))
            .v5(v5::MqttServer::new(|con: v5::Handshake| {
                Ready::Ok::<_, TestError>(con.ack(St))
            })
            .publish(|p: v5::Publish| Ready::Ok::<_, TestError>(p.ack())))
    });

    // connect to v5 server
    let client = v5::client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(v5::client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
    sink.close();

    // connect to v3 server
    let client = v3::client::MqttConnector::new()
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call(v3::client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res =
        sink.publish(ByteString::from_static("topic")).send_at_least_once(Bytes::new()).await;
    assert!(res.is_ok());
    sink.close();

    Ok(())
}
