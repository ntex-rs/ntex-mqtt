use ntex::service::fn_service;
use ntex::time::{sleep, Millis, Seconds};
use ntex_mqtt::v5;

#[derive(Debug)]
struct Error;

impl std::convert::TryFrom<Error> for v5::PublishAck {
    type Error = Error;

    fn try_from(err: Error) -> Result<Self, Self::Error> {
        Err(err)
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=info,ntex_mqtt=trace,subs_client=trace");
    env_logger::init();

    // connect to server
    let client = v5::client::MqttConnector::new("127.0.0.1:1883")
        .client_id("my-client-id")
        .keep_alive(Seconds(30))
        .connect()
        .await
        .unwrap();

    let sink = client.sink();

    // handle incoming publishes
    ntex::rt::spawn(client.start(fn_service(
        |control: v5::client::Control<Error>| async move {
            match control {
                v5::client::Control::Publish(publish) => {
                    let pl = publish.read_all().await;
                    log::info!(
                        "incoming publish: {:?} -> {:?}, payload: {:?}",
                        publish.packet().packet_id,
                        publish.packet().topic,
                        pl,
                    );
                    Ok(publish.ack(v5::codec::PublishAckReason::Success))
                }
                v5::client::Control::PublishRelease(msg) => Ok(msg.ack()),
                v5::client::Control::Disconnect(msg) => {
                    log::warn!("Server disconnecting: {:?}", msg);
                    Ok(msg.ack())
                }
                v5::client::Control::Error(msg) => {
                    log::error!("Codec error: {:?}", msg);
                    Ok(msg.ack(v5::codec::DisconnectReasonCode::UnspecifiedError))
                }
                v5::client::Control::ProtocolError(msg) => {
                    log::error!("Protocol error: {:?}", msg);
                    Ok(msg.ack())
                }
                v5::client::Control::PeerGone(msg) => {
                    log::warn!("Peer closed connection: {:?}", msg.error());
                    Ok(msg.ack())
                }
                v5::client::Control::Closed(msg) => {
                    log::warn!("Server closed connection: {:?}", msg);
                    Ok(msg.ack())
                }
            }
        },
    )));

    // subscribe to topic
    sink.subscribe(None)
        .topic_filter(
            "topic1".into(),
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

    log::info!("sending client publish");
    let ack = sink.publish("topic1").send_at_least_once("Hello world!".into()).await.unwrap();
    log::info!("ack received: {:?}", ack);

    sleep(Millis(1_000)).await;
    log::info!("closing connection");
    sink.close();
    sleep(Millis(1_000)).await;

    Ok(())
}
