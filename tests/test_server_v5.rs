use bytes::Bytes;
use bytestring::ByteString;
use futures::future::ok;
use ntex::server;
use ntex::service::Service;
use std::convert::TryFrom;

use ntex_mqtt::v5::{Connect, ConnectAck, MqttServer, Publish, PublishAck, Session};

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
    println!("CONNECT: {:?}", packet);
    Ok(packet.ack(St))
}

// #[ntex::test]
// async fn test_simple() -> std::io::Result<()> {
//     std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
//     env_logger::init();

//     let srv = server::test_server(|| {
//         MqttServer::new(connect).publish(|p: Publish| ok::<_, TestError>(p.ack())).finish()
//     });

//     struct Client;

//     let client = client::Client::new(ByteString::from_static("user"))
//         .state(|ack: client::ConnectAck<_>| async move {
//             ack.sink().publish(ByteString::from_static("#"), Bytes::new()).send_at_most_once();
//             ack.sink().close();
//             Ok(ack.state(Client))
//         })
//         .finish(ntex::fn_factory_with_config(|session: Session<Client>| {
//             let session = session.clone();

//             ok::<_, ()>(ntex::into_service(move |p: Publish| {
//                 session.sink().close();
//                 ok(p.ack())
//             }))
//         }));

//     let conn = ntex::connect::Connector::default()
//         .call(ntex::connect::Connect::with(String::new(), srv.addr()))
//         .await
//         .unwrap();

//     client.call(conn).await.unwrap();

//     Ok(())
// }
