use std::cell::RefCell;

use ntex::{SharedCfg, util::ByteString};
use ntex_mqtt::v5::{self, MqttServer, Publish, PublishAck, Session};
use ntex_mqtt::{Control, Reason};

#[derive(Clone, Debug)]
struct MySession {
    _client_id: String,
    subscriptions: RefCell<Vec<ByteString>>,
    sink: v5::MqttSink,
}

#[derive(Debug, thiserror::Error)]
#[error("Server error")]
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

async fn connect(msg: v5::Connect) -> Result<v5::ConnectAck<MySession>, MyServerError> {
    log::info!("new connection: {:?}", msg);

    let session = MySession {
        _client_id: msg.packet().client_id.to_string(),
        subscriptions: RefCell::new(Vec::new()),
        sink: msg.sink(),
    };

    Ok(msg.ack(session))
}

async fn publish(
    session: &Session<MySession>,
    publish: Publish,
) -> Result<PublishAck, MyServerError> {
    log::info!(
        "incoming client publish ({:?}) : {:?} -> {:?}",
        &*session,
        publish.id(),
        publish.topic()
    );

    // client is subscribed to this topic, send echo
    if session
        .subscriptions
        .borrow()
        .contains(&publish.packet().topic)
    {
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

async fn protocol_service(
    st: &Session<MySession>,
    msg: v5::ProtocolMessage,
) -> Result<v5::ProtocolMessageAck, MyServerError> {
    match msg {
        v5::ProtocolMessage::Auth(a) => Ok(a.ack(v5::codec::Auth::default())),
        v5::ProtocolMessage::Disconnect(d) => Ok(d.ack()),
        v5::ProtocolMessage::Subscribe(mut s) => {
            // store subscribed topics in session, publish service uses this list for echos
            s.iter_mut().for_each(|mut s| {
                st.subscriptions.borrow_mut().push(s.topic().clone());
                s.confirm(v5::QoS::AtLeastOnce);
            });

            Ok(s.ack())
        }
        v5::ProtocolMessage::Unsubscribe(s) => Ok(s.ack()),
        v5::ProtocolMessage::Ping(p) => Ok(p.ack()),
        _ => Ok(msg.ack()),
    }
}

async fn control_service(
    control: Control<MyServerError>,
) -> Result<Option<v5::codec::Encoded>, MyServerError> {
    match control {
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
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,subs=trace");
    env_logger::init();

    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", SharedCfg::default(), async |_| {
            MqttServer::new(async |ses: &Session<MySession>, req| publish(ses, req).await)
                .control(control_service)
                .protocol(protocol_service)
                .build(connect)
        })?
        .workers(1)
        .run()
        .await
}
