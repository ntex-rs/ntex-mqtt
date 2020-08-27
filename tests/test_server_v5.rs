use std::convert::TryFrom;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::ok;
use ntex::server;

use ntex_mqtt::v5::{client, codec, Connect, ConnectAck, MqttServer, Publish, PublishAck};

struct St;

#[derive(Debug)]
struct TestError;

impl From<()> for TestError {
    fn from(_: ()) -> Self {
        TestError
    }
}

impl TryFrom<TestError> for PublishAck {
    type Error = TestError;

    fn try_from(err: TestError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn connect<Io>(packet: Connect<Io>) -> Result<ConnectAck<Io, St>, TestError> {
    Ok(packet.ack(St))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
    env_logger::init();

    let srv = server::test_server(|| {
        MqttServer::new(connect).publish(|p: Publish| ok::<_, TestError>(p.ack())).finish()
    });

    // connect to server
    let client = client::MqttConnector::new(srv.addr())
        .client_id(ByteString::from_static("user"))
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    ntex::rt::spawn(client.start_default());

    let res = sink
        .publish(ByteString::from_static("#"), Bytes::new())
        .send_at_least_once()
        .await
        .unwrap();
    assert_eq!(res.reason_code, codec::PublishAckReason::Success);

    sink.close();
    Ok(())
}
