use ntex::time::{Millis, Seconds, sleep};
use ntex::{ServiceFactory, SharedCfg};
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
    let payload = pkt.read_all().await;

    log::info!("incoming publish: {:?} -> {:?} payload {:?}", pkt.id(), pkt.topic(), payload);
    Ok(pkt.ack())
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "client=trace,ntex=info,ntex_mqtt=trace");
    env_logger::init();

    // connect to server
    let client = v5::client::MqttConnector::new()
        .client_id("user")
        .max_packet_size(30)
        .keep_alive(Seconds::ONE)
        .pipeline(SharedCfg::default())
        .await
        .unwrap()
        .call("127.0.0.1:1883")
        .await
        .unwrap();

    let sink = client.sink();

    let router = client.resource("response", publish);
    ntex::rt::spawn(router.start_default());

    sink.subscribe(None)
        .topic_filter(
            "response".into(),
            v5::codec::SubscriptionOptions {
                qos: v5::codec::QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: v5::codec::RetainHandling::AtSubscribe,
            },
        )
        .send()
        .await
        .unwrap();

    sleep(Millis(10_000)).await;

    log::info!("closing connection");
    sink.close();
    sleep(Millis(1_000)).await;

    Ok(())
}
