use std::cell::RefCell;

use ntex::service::{ServiceFactory, fn_factory_with_config, fn_service};
use ntex::util::ByteString;
use ntex_mqtt::v5::{self, MqttServer, Publish, PublishAck, Session};
use ntex_mqtt::{Control, Reason};

#[derive(Clone, Debug)]
struct MySession {
    _client_id: String,
    subscriptions: RefCell<Vec<ByteString>>,
    sink: v5::MqttSink,
}

#[derive(Debug)]
struct MyServerError;

impl From<()> for MyServerError {
    fn from(_: ()) -> Self {
        MyServerError
    }
}

impl std::convert::TryFrom<MyServerError> for PublishAck {
    type Error = MyServerError;

    fn try_from(err: MyServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

async fn handshake(
    handshake: v5::Handshake,
) -> Result<v5::HandshakeAck<MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession {
        _client_id: handshake.packet().client_id.to_string(),
        subscriptions: RefCell::new(Vec::new()),
        sink: handshake.sink(),
    };

    Ok(handshake.ack(session))
}

async fn publish(
    session: Session<MySession>,
    publish: Publish,
) -> Result<PublishAck, MyServerError> {
    log::info!(
        "incoming client publish ({:?}) : {:?} -> {:?}",
        &*session,
        publish.id(),
        publish.topic()
    );

    // client is subscribed to this topic, send echo
    if session.subscriptions.borrow().contains(&publish.packet().topic) {
        log::info!("client is subscribed to topic, sending echo");

        let payload = publish.read_all().await.unwrap();
        session
            .sink
            .publish(publish.packet().topic.clone())
            .send_at_least_once(payload)
            .await
            .unwrap();
    }

    Ok(publish.ack())
}

fn protocol_service_factory() -> impl ServiceFactory<
    v5::ProtocolMessage,
    Session<MySession>,
    Response = v5::ProtocolMessageAck,
    Error = MyServerError,
    InitError = MyServerError,
> {
    fn_factory_with_config(async move |session: Session<MySession>| {
        Ok(fn_service(async move |msg| match msg {
            v5::ProtocolMessage::Auth(a) => Ok(a.ack(v5::codec::Auth::default())),
            v5::ProtocolMessage::Disconnect(d) => Ok(d.ack()),
            v5::ProtocolMessage::Subscribe(mut s) => {
                // store subscribed topics in session, publish service uses this list for echos
                s.iter_mut().for_each(|mut s| {
                    session.subscriptions.borrow_mut().push(s.topic().clone());
                    s.confirm(v5::QoS::AtLeastOnce);
                });

                Ok(s.ack())
            }
            v5::ProtocolMessage::Unsubscribe(s) => Ok(s.ack()),
            v5::ProtocolMessage::Ping(p) => Ok(p.ack()),
            _ => Ok(msg.ack()),
        }))
    })
}

fn control_service_factory() -> impl ServiceFactory<
    Control<MyServerError>,
    Session<MySession>,
    Response = Option<v5::codec::Encoded>,
    Error = MyServerError,
    InitError = MyServerError,
> {
    fn_factory_with_config(async move |_: Session<MySession>| {
        Ok(fn_service(async move |control| match control {
            Control::Stop(Reason::Error(_)) => Ok(Some(
                v5::codec::Packet::from(v5::codec::Disconnect {
                    reason_code: v5::codec::DisconnectReasonCode::UnspecifiedError,
                    ..Default::default()
                })
                .into(),
            )),
            Control::Stop(Reason::Protocol(_)) => Ok(None),
            Control::Stop(Reason::PeerGone(_)) => Ok(None),
            _ => Ok(None),
        }))
    })
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,subs=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", async |_| {
            MqttServer::new(handshake)
                .control(control_service_factory())
                .protocol(protocol_service_factory())
                .publish(fn_factory_with_config(async |session: Session<MySession>| {
                    Ok::<_, MyServerError>(fn_service(move |req| publish(session.clone(), req)))
                }))
        })?
        .workers(1)
        .run()
        .await
}
