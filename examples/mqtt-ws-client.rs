//! Mqtt-over-WS client
use std::{io, rc::Rc};

use ntex::connect::{openssl::Connector, Connect, ConnectError};
use ntex::time::{sleep, Millis, Seconds};
use ntex::{util::Bytes, ws};
use ntex_mqtt::v3;
use openssl::ssl;

#[derive(Debug)]
struct Error;

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "mqtt_ws_client=trace,ntex=trace,ntex_mqtt=trace");
    env_logger::init();

    // ssl connector
    let mut builder = ssl::SslConnector::builder(ssl::SslMethod::tls()).unwrap();
    builder.set_verify(ssl::SslVerifyMode::NONE);

    // we need custom connector that would open ws connection and enable ws transport
    let ws_client = Rc::new(
        ws::WsClient::with_connector("https://127.0.0.1:8883", Connector::new(builder.build()))
            .finish()
            .unwrap(),
    );

    // connect to server
    let client = v3::client::MqttConnector::new("127.0.0.1:8883")
        .client_id("user")
        .keep_alive(Seconds::ONE)
        .connector(move |_: Connect<&str>| {
            let client = ws_client.clone();
            async move {
                Ok(client
                    .connect()
                    .await
                    .map_err(|e| ConnectError::Io(io::Error::new(io::ErrorKind::Other, e)))?
                    .into_transport())
            }
        })
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    // publish handler
    let router = client.resource("response", |pkt: v3::Publish| async move {
        log::info!(
            "incoming publish: {:?} -> {:?} payload {:?}",
            pkt.id(),
            pkt.topic(),
            pkt.payload()
        );
        Ok::<_, Error>(())
    });
    ntex::rt::spawn(router.start_default());

    sink.publish("test-topic", Bytes::from_static(b"data")).send_at_least_once().await.unwrap();
    println!("ack is received");

    sleep(Millis(10_000)).await;

    log::info!("closing connection");
    sink.close();
    sleep(Millis(1_000)).await;

    Ok(())
}
