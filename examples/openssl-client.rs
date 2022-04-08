use ntex::connect::openssl::Connector;
use ntex::time::{sleep, Millis, Seconds};
use ntex_mqtt::v5;
use openssl::ssl;

#[derive(Debug)]
struct Error;

impl std::convert::TryFrom<Error> for v5::PublishAck {
    type Error = Error;

    fn try_from(err: Error) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn publish(pkt: v5::Publish) -> Result<v5::PublishAck, Error> {
    log::info!(
        "incoming publish: {:?} -> {:?} payload {:?}",
        pkt.id(),
        pkt.topic(),
        pkt.payload()
    );
    Ok(pkt.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "openssl_client=trace,ntex=info,ntex_mqtt=trace");
    env_logger::init();

    // ssl connector
    let mut builder = ssl::SslConnector::builder(ssl::SslMethod::tls()).unwrap();
    builder.set_verify(ssl::SslVerifyMode::NONE);

    // connect to server
    let client = v5::client::MqttConnector::new("127.0.0.1:8883")
        .connector(Connector::new(builder.build()))
        .client_id("user")
        .keep_alive(Seconds::ONE)
        .max_packet_size(30)
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    let router = client.resource("response", publish);
    ntex::rt::spawn(router.start_default());

    sink.publish("test-topic", "Publish data".into()).send_at_least_once().await.unwrap();

    sleep(Millis(10_000)).await;

    log::info!("closing connection");
    sink.close();
    sleep(Millis(1_000)).await;

    Ok(())
}
