use std::time::Duration;

use ntex::rt::time::delay_for;
use ntex_mqtt::v5;

#[derive(Debug)]
struct Error;

impl std::convert::TryFrom<Error> for v5::PublishAck {
    type Error = Error;

    fn try_from(err: Error) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn publish(pkt: v5::Publish) -> Result<v5::PublishAck, Error> {
    log::info!("incoming publish: {:?} -> {:?}", pkt.id(), pkt.topic());
    Ok(pkt.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace");
    env_logger::init();

    // connect to server
    let client = v5::client::MqttConnector::new("127.0.0.1:1883")
        .client_id("user")
        .keep_alive(1)
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    let router = client.resource("response", publish);
    ntex::rt::spawn(router.start_default());

    delay_for(Duration::from_secs(10)).await;

    log::info!("closing connection");
    sink.close();
    delay_for(Duration::from_secs(1)).await;

    Ok(())
}
