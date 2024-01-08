//! Mqtt over WebSockets
use std::io;

use ntex::http::{body, h1, HttpService, Request, Response, ResponseError};
use ntex::io::{Filter, Io};
use ntex::service::{chain_factory, ServiceFactory};
use ntex::util::{variant, Ready};
use ntex::ws;
use ntex_mqtt::{v3, v5, HandshakeError, MqttError, MqttServer, ProtocolError};
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

/// Create mqtt server factory
fn mqtt_server<F: Filter>(
) -> impl ServiceFactory<Io<F>, Response = (), Error = MqttError<ServerError>, InitError = ()> {
    MqttServer::new()
        .v3(v3::MqttServer::new(|handshake: v3::Handshake| async move {
            log::info!("new mqtt v3 connection: {:?}", handshake);
            Ok(handshake.ack(Session, false))
        })
        .publish(|publish: v3::Publish| async move {
            log::info!("incoming publish: {:?} -> {:?}", publish.id(), publish.topic());
            Ok::<_, ServerError>(())
        }))
        .v5(v5::MqttServer::new(|handshake: v5::Handshake| async move {
            log::info!("new mqtt v5 connection: {:?}", handshake);
            Ok(handshake.ack(Session))
        })
        .publish(|publish: v5::Publish| async move {
            log::info!("incoming publish: {:?} -> {:?}", publish.id(), publish.topic());
            Ok::<_, ServerError>(publish.ack())
        }))
}

enum Protocol {
    Http,
    Mqtt,
    Unknown,
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "ntex=info,ntex_io=info,ntex_mqtt=trace,mqtt_ws_server=trace",
    );
    env_logger::init();

    // create self-signed certificates using:
    //   openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:4096 -keyout examples/key8.pem -out examples/cert.pem -days 365 -keyform PEM
    //   openssl rsa -in examples/key8.pem -out examples/key.pem
    let mut builder = ssl::SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder.set_private_key_file("./tests/key.pem", SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file("./tests/cert.pem").unwrap();
    let acceptor = builder.build();

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:8883", move |_| {
            // first switch to ssl stream
            chain_factory(SslAcceptor::new(acceptor.clone()))
                .map_err(|_err| MqttError::Service(ServerError {}))
                // we need to read first 4 bytes and detect protocol GET or MQTT
                .and_then(|io: Io<_>| async move {
                    println!("Connection is established, chossing protocol");
                    loop {
                        // we can read incoming bytes stream without consuming it
                        let result = io.with_read_buf(|buf| {
                            if buf.len() < 4 {
                                None
                            } else if &buf[..4] == b"MQTT" {
                                println!("MQTT protocol is selected");
                                Some(Protocol::Mqtt)
                            } else if &buf[..4] == b"GET " {
                                println!("HTTP protocol is selected");
                                Some(Protocol::Http)
                            } else {
                                println!("Protocol is unknown {:?}", buf);
                                Some(Protocol::Unknown)
                            }
                        });
                        return match result {
                            Some(Protocol::Mqtt) => Ok(variant::Variant2::V1(io)),
                            Some(Protocol::Http) => Ok(variant::Variant2::V2(io)),
                            Some(Protocol::Unknown) => {
                                Err(MqttError::Handshake(HandshakeError::Protocol(
                                    ProtocolError::generic_violation("Unsupported protocol"),
                                )))
                            }
                            None => {
                                // need to read more data
                                io.read_ready().await?;
                                continue;
                            }
                        };
                    }
                })
                // start actual mqtt server depends on protocol.
                // we need two different servers one for mqtt protocol and another for websockets
                // for this purpose we are going to use ntex::util::variant helper service
                .and_then(
                    // normal mqtt server
                    variant::variant(mqtt_server())
                        // http server for websockets
                        .v2(HttpService::build()
                            // websocket handler, we need to verify websocket handshake
                            // and then switch to websokets streaming
                            .upgrade(
                                // validate ws request and init ws transport
                                chain_factory(
                                    |(req, io, codec): (Request, Io<_>, h1::Codec)| {
                                        async move {
                                            match ws::handshake(req.head()) {
                                                // invalid websockets handshake request
                                                Err(e) => {
                                                    // send http handshake respone
                                                    io.send(
                                                        h1::Message::Item((
                                                            e.error_response().drop_body(),
                                                            body::BodySize::None,
                                                        )),
                                                        &codec,
                                                    )
                                                    .await?;
                                                    return Err(MqttError::Handshake(
                                                        HandshakeError::Protocol(
                                                            ProtocolError::generic_violation(
                                                                "WebSockets handshake error",
                                                            ),
                                                        ),
                                                    ));
                                                }
                                                Ok(mut res) => {
                                                    // send http handshake respone
                                                    io.encode(
                                                        h1::Message::Item((
                                                            res.finish().drop_body(),
                                                            body::BodySize::None,
                                                        )),
                                                        &codec,
                                                    )?;
                                                }
                                            }

                                            // enable websocket streamimng, ws transport
                                            // converts incoming stream of ws frames into bytes stream
                                            Ok(ws::WsTransport::create(
                                                io,
                                                ws::Codec::default(),
                                            ))
                                        }
                                    },
                                )
                                // and then start mqtt server
                                .and_then(mqtt_server()),
                            )
                            .finish(|_req| {
                                // normal http requests are not allowed
                                Ready::Ok::<_, io::Error>(
                                    Response::NotFound().body("Use WebSocket proto"),
                                )
                            })
                            // adapt service error to mqtt error
                            .map_err(|e| {
                                log::info!("Http server error: {:?}", e);
                                MqttError::Handshake(HandshakeError::Protocol(
                                    ProtocolError::generic_violation("Http server error"),
                                ))
                            })),
                )
        })?
        .workers(1)
        .run()
        .await
}
