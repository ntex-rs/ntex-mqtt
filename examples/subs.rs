use std::cell::RefCell;

use ntex::service::{fn_factory_with_config, fn_service, ServiceFactory};
use ntex::util::{ByteString, Ready};
use ntex_mqtt::v5::{self, Control, ControlAck, MqttServer, Publish, PublishAck, Session};

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
        session.state(),
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

fn control_service_factory() -> impl ServiceFactory<
    Control<MyServerError>,
    Session<MySession>,
    Response = ControlAck,
    Error = MyServerError,
    InitError = MyServerError,
> {
    fn_factory_with_config(|session: Session<MySession>| {
        Ready::Ok(fn_service(move |control| match control {
            v5::Control::Auth(a) => Ready::Ok(a.ack(v5::codec::Auth::default())),
            v5::Control::Error(e) => {
                Ready::Ok(e.ack(v5::codec::DisconnectReasonCode::UnspecifiedError))
            }
            v5::Control::ProtocolError(e) => Ready::Ok(e.ack()),
            v5::Control::Ping(p) => Ready::Ok(p.ack()),
            v5::Control::Disconnect(d) => Ready::Ok(d.ack()),
            v5::Control::Subscribe(mut s) => {
                // store subscribed topics in session, publish service uses this list for echos
                s.iter_mut().for_each(|mut s| {
                    session.subscriptions.borrow_mut().push(s.topic().clone());
                    s.confirm(v5::QoS::AtLeastOnce);
                });

                Ready::Ok(s.ack())
            }
            v5::Control::Unsubscribe(s) => Ready::Ok(s.ack()),
            v5::Control::Closed(c) => Ready::Ok(c.ack()),
            v5::Control::PeerGone(c) => Ready::Ok(c.ack()),
            _ => Ready::Ok(control.ack()),
        }))
    })
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,subs=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", |_| {
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
