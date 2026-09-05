//! Mqtt-over-WS server
use std::io;

use ntex::http::{self, HttpService, Request, Response, error::DispatchError, h1};
use ntex::io::{Filter, Io, IoBoxed, Layer};
use ntex::{Pipeline, PipelineBinding, Service, SharedCfg, ws};
use ntex_mqtt::{MqttError, MqttServer, v3, v5};
use ntex_tls::openssl::SslAcceptor;
use openssl::ssl::{self, SslFiletype, SslMethod};

#[derive(Clone)]
struct Session;

#[derive(Debug)]
struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

impl std::convert::TryFrom<ServerError> for v5::PublishAck {
    type Error = ServerError;

    fn try_from(err: ServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

/// Mqtt server factory
fn mqtt_server() -> impl Service<(), IoBoxed, Res = (), Error = io::Error> {
    MqttServer::new()
        .v3(v3::MqttServer::new(async move |publish: v3::Publish| {
            log::info!(
                "incoming publish: {:?} -> {:?}",
                publish.id(),
                publish.topic()
            );
            Ok::<_, ServerError>(())
        })
        .build(async move |msg: v3::Connect| {
            log::info!("new mqtt v3 connection: {:?}", msg);
            Ok(msg.ack(Session, false))
        }))
        .v5(v5::MqttServer::new(async move |publish: v5::Publish| {
            log::info!(
                "incoming publish: {:?} -> {:?}",
                publish.id(),
                publish.topic()
            );
            Ok::<_, ServerError>(publish.ack())
        })
        .build(async move |msg: v5::Connect| {
            log::info!("new mqtt v5 connection: {:?}", msg);
            Ok(msg.ack(Session))
        }))
        .map_err(|e: MqttError<ServerError>| {
            log::info!("Mqtt server error: {:?}", e);
            io::Error::other(format!("Mqtt error {e:?}"))
        })
}

/// Mqtt server factory
fn http_server<F: Filter>(
    mqtt: PipelineBinding<IoBoxed, (), io::Error>,
) -> impl Service<(), Io<F>, Res = (), Error = DispatchError> {
    HttpService::new(async |_| {
        // ntex::web could be used for normal http
        //
        // this impl doe not allow http
        Ok::<_, io::Error>(Response::NotFound().body("Use WebSocket proto"))
    })
    // websocket handler, we need to verify websocket handshake
    // and then switch to websokets streaming
    .h1_control(async move |msg: h1::Control<_, _>| {
        let ack = match msg {
            h1::Control::Upgrade(ctl) => {
                let (ack, io, req, codec) = ctl.handle();
                // negotiate ws protocol and install Ws transport
                let io = ws(io, req, codec).await?;
                // handle mqtt protocol
                mqtt.call(io.boxed()).await?;

                ack
            }
            _ => msg.ack(),
        };
        Ok::<_, io::Error>(ack)
    })
}

/// WebSocket service
///
/// ws server negotiates ws protocol and switch to websocket transport
async fn ws<F: Filter>(
    io: Io<F>,
    req: Request,
    codec: h1::Codec,
) -> Result<Io<Layer<ws::WsTransport, F>>, io::Error> {
    log::trace!("Got http request: {:?}", req);

    match ws::handshake(req.head()) {
        Err(e) => {
            // invalid WebSocket handshake request
            log::info!("WebSocket negotiation failed: {:?}", e);
            Err(io::Error::other(e))
        }
        Ok(mut res) => {
            // send success http response and switch to ws codec
            io.send(
                h1::Message::Item((res.finish().drop_body(), http::body::BodySize::Empty)),
                &codec,
            )
            .await?;

            log::trace!("WebSocket handshake is completed");
            Ok(ws::WsTransport::create(io, ws::Codec::new()))
        }
    }
}

enum Protocol {
    Http,
    Mqtt,
    Unknown,
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=info,ntex_io=info,ntex_mqtt=trace,mqtt_ws_server=trace");
    env_logger::init();

    // create self-signed certificates using:
    //   openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:4096 -keyout examples/key8.pem -out examples/cert.pem -days 365 -keyform PEM
    //   openssl rsa -in examples/key8.pem -out examples/key.pem
    let mut builder = ssl::SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file("./tests/key.pem", SslFiletype::PEM)
        .unwrap();
    builder
        .set_certificate_chain_file("./tests/cert.pem")
        .unwrap();
    let acceptor = builder.build();

    ntex::server::Server::builder()
        .bind(
            "mqtt",
            "127.0.0.1:8883",
            SharedCfg::default(),
            async move |_| {
                let mqtt = Pipeline::new((), mqtt_server());
                let http = Pipeline::new((), http_server(mqtt.bind()));

                // first switch to ssl stream
                SslAcceptor::new(acceptor.clone())
                    .map_err(|e| io::Error::other(e))
                    // we need to read first 4 bytes and detect protocol GET or MQTT
                    .and_then(async move |io: Io<_>| {
                        println!("Connection is established, select protocol");

                        // we can read incoming bytes stream without consuming it
                        let mut buf = [0; 8];
                        io.read(&mut buf).await?;

                        let result = if &buf[4..8] == b"MQTT" {
                            println!("MQTT protocol is selected");
                            Protocol::Mqtt
                        } else if &buf[..4] == b"GET " {
                            println!("HTTP protocol is selected");
                            Protocol::Http
                        } else {
                            println!("Protocol is unknown {:?}", buf);
                            Protocol::Unknown
                        };

                        return match result {
                            Protocol::Mqtt => mqtt.call(io.boxed()).await,
                            Protocol::Http => http
                                .call(io)
                                .await
                                .map_err(|e| io::Error::other(format!("Http error {e:?}"))),
                            Protocol::Unknown => Err(io::Error::other("Unsupported protocol")),
                        };
                    })
            },
        )?
        .workers(1)
        .run()
        .await
}
