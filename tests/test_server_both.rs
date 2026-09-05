use std::convert::Infallible;

use ntex::util::{ByteString, Bytes};
use ntex::{Pipeline, SharedCfg, server};
use ntex_mqtt::{MqttServer, v3, v5};

struct St;

#[derive(Debug)]
struct TestError;

impl From<Infallible> for TestError {
    fn from(_: Infallible) -> Self {
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
            .v3(v3::MqttServer::new(async |_| Ok::<_, TestError>(()))
                .build(async move |con: v3::Connect| Ok::<_, TestError>(con.ack(St, false))))
            .v5(
                v5::MqttServer::new(async move |p: v5::Publish| Ok::<_, TestError>(p.ack()))
                    .build(async move |con: v5::Connect| Ok::<_, TestError>(con.ack(St))),
            )
    });

    // connect to v5 server
    let client = Pipeline::new(SharedCfg::default(), v5::client::MqttConnector::new())
        .call(v5::client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    sink.close();

    // connect to v3 server
    let client = Pipeline::new(SharedCfg::default(), v3::client::MqttConnector::new())
        .call(v3::client::Connect::new(srv.addr()).client_id("user"))
        .await
        .unwrap();
    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("topic"))
        .send_at_least_once(Bytes::new())
        .await;
    assert!(res.is_ok());
    sink.close();

    Ok(())
}
