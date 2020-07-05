use bytes::Bytes;
use bytestring::ByteString;
use futures::future::ok;
use ntex::server;
use ntex::service::Service;

use ntex_mqtt::v3::{client, Connect, ConnectAck, MqttServer, Publish, Session};

struct St;

async fn connect<Io>(packet: Connect<Io>) -> Result<ConnectAck<Io, St>, ()> {
    println!("CONNECT: {:?}", packet);
    Ok(packet.ack(St, false))
}

#[ntex::test]
async fn test_simple() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex_mqtt=trace,ntex_codec=info,ntex=trace");
    env_logger::init();

    let srv = server::test_server(|| MqttServer::new(connect).publish(|_t| ok(())));

    struct Client;

    let client = client::Client::new(ByteString::from_static("user"))
        .state(|ack: client::ConnectAck<_>| async move {
            ack.sink()
                .publish_qos0(ByteString::from_static("#"), Bytes::new(), false);
            ack.sink().close();
            Ok(ack.state(Client))
        })
        .finish(ntex::fn_factory_with_config(|session: Session<Client>| {
            let session = session.clone();

            ok::<_, ()>(ntex::into_service(move |_t: Publish| {
                session.sink().close();
                async { Ok(()) }
            }))
        }));

    let conn = ntex::connect::Connector::default()
        .call(ntex::connect::Connect::with(String::new(), srv.addr()))
        .await
        .unwrap();

    client.call(conn).await.unwrap();

    Ok(())
}
