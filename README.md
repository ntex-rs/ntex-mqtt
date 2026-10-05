# MQTT Client/Server framework

[![build status](https://github.com/ntex-rs/ntex-mqtt/actions/workflows/linux.yml/badge.svg?branch=main&event=push)](https://github.com/ntex-rs/ntex-mqtt/actions/workflows/linux.yml/badge.svg) [![codecov](https://codecov.io/gh/ntex-rs/ntex-mqtt/branch/main/graph/badge.svg)](https://codecov.io/gh/ntex-rs/ntex-mqtt) [![crates.io](https://img.shields.io/crates/v/ntex-mqtt.svg)](https://crates.io/crates/ntex-mqtt) [![Documentation](https://img.shields.io/docsrs/ntex-mqtt/latest)](https://docs.rs/ntex-mqtt)

MQTT Client/Server framework for [ntex](https://github.com/ntex-rs/ntex) with support of v5 and v3.1.1 protocols.

## Features

- MQTT v3.1.1 and v5 protocols, a single server can serve both
- Client and server
- `QoS` 0, 1 and 2
- Streaming publish payloads, large messages are processed in chunks
- Flow control: receive maximum, in-flight size limits and read pause
- Topic routing and v5 topic aliases
- TLS (openssl, rustls) via `ntex-tls`, and MQTT over WebSocket

## Installation

```toml
[dependencies]
ntex = "4"
ntex-mqtt = "9"
```

## Server

```rust
use ntex::SharedCfg;
use ntex_mqtt::{MqttServer, v3, v5};

#[derive(Debug)]
struct Error;

impl From<()> for Error {
    fn from(_: ()) -> Self {
        Error
    }
}

impl TryFrom<Error> for v5::PublishAck {
    type Error = Error;

    fn try_from(err: Error) -> Result<Self, Self::Error> {
        Err(err)
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    ntex::server::build()
        .bind("mqtt", "127.0.0.1:1883", SharedCfg::default(), async |_| {
            MqttServer::new()
                .v3(
                    v3::MqttServer::new(async |pkt: v3::Publish| {
                        println!("v3 publish: {}", pkt.topic().path());
                        Ok::<_, Error>(())
                    })
                    .build(async |con: v3::Connect| Ok::<_, Error>(con.ack((), false))),
                )
                .v5(
                    v5::MqttServer::new(async |pkt: v5::Publish| {
                        println!("v5 publish: {}", pkt.topic().path());
                        Ok::<_, Error>(pkt.ack())
                    })
                    .build(async |con: v5::Connect| Ok::<_, Error>(con.ack(()))),
                )
        })?
        .run()
        .await
}
```

## Client

```rust
use ntex::{Pipeline, SharedCfg, util::Bytes};
use ntex_mqtt::v3;

#[ntex::main]
async fn main() {
    let client = Pipeline::new(SharedCfg::default(), v3::client::MqttConnector::new())
        .call(v3::client::Connect::new("127.0.0.1:1883").client_id("my-client"))
        .await
        .unwrap();

    let sink = client.sink();
    ntex::rt::spawn(client.start_default());

    sink.publish("topic")
        .send_at_least_once(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    sink.close();
}
```

## Links

- [API documentation](https://docs.rs/ntex-mqtt)
- [Examples](https://github.com/ntex-rs/ntex-mqtt/tree/main/examples): routing, sessions, subscriptions, TLS and WebSocket transports

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
