use std::cell::RefCell;

use ntex::util::{ByteString, Ready};
use ntex::{fn_factory_with_config, fn_service, ServiceFactory};
use ntex_mqtt::v5::{
    self, ControlMessage, ControlResult, MqttServer, Publish, PublishAck, Session,
};

#[derive(Clone, Debug)]
struct MySession {
    client_id: String,
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

async fn handshake<Io>(
    handshake: v5::Handshake<Io>,
) -> Result<v5::HandshakeAck<Io, MySession>, MyServerError> {
    log::info!("new connection: {:?}", handshake);

    let session = MySession {
        client_id: handshake.packet().client_id.to_string(),
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
        session.state(),
        publish.id(),
        publish.topic()
    );

    // client is subscribed to this topic, send echo
    if session.subscriptions.borrow().contains(&publish.packet().topic) {
        log::info!("client is subscribed to topic, sending echo");
        session
            .sink
            .publish(publish.packet().topic.clone(), publish.packet().payload.clone())
            .send_at_least_once()
            .await
            .unwrap();
    }

    Ok(publish.ack())
}

fn control_service_factory() -> impl ServiceFactory<
    Config = Session<MySession>,
    Request = ControlMessage<MyServerError>,
    Response = ControlResult,
    Error = MyServerError,
    InitError = MyServerError,
> {
    fn_factory_with_config(|session: Session<MySession>| {
        Ready::Ok(fn_service(move |control| match control {
            v5::ControlMessage::Auth(a) => Ready::Ok(a.ack(v5::codec::Auth::default())),
            v5::ControlMessage::Error(e) => {
                Ready::Ok(e.ack(v5::codec::DisconnectReasonCode::UnspecifiedError))
            }
            v5::ControlMessage::ProtocolError(e) => Ready::Ok(e.ack()),
            v5::ControlMessage::Ping(p) => Ready::Ok(p.ack()),
            v5::ControlMessage::Disconnect(d) => Ready::Ok(d.ack()),
            v5::ControlMessage::Subscribe(mut s) => {
                // store subscribed topics in session, publish service uses this list for echos
                s.iter_mut().for_each(|mut s| {
                    session.subscriptions.borrow_mut().push(s.topic().clone());
                    s.subscribe(v5::QoS::AtLeastOnce);
                });

                Ready::Ok(s.ack())
            }
            v5::ControlMessage::Unsubscribe(s) => Ready::Ok(s.ack()),
            v5::ControlMessage::Closed(c) => Ready::Ok(c.ack()),
        }))
    })
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,subs=trace");
    env_logger::init();

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:1883", || {
            MqttServer::new(handshake)
                .control(control_service_factory())
                .publish(fn_factory_with_config(|session: Session<MySession>| {
                    Ready::Ok::<_, MyServerError>(fn_service(move |req| {
                        publish(session.clone(), req)
                    }))
                }))
                .finish()
        })?
        .workers(1)
        .run()
        .await
}
