#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate bytes;
extern crate futures;
extern crate rand;
extern crate slab;
extern crate string;
extern crate tokio_codec;

mod error;
#[macro_use]
mod topic;
#[macro_use]
mod proto;
mod codec;
mod packet;

pub use codec::Codec;
pub use error::{DecodeError, MqttError};
pub use packet::{Connect, ConnectReturnCode, LastWill, Packet, SubscribeReturnCode};
pub use proto::{Protocol, QoS};
pub use topic::{Level, MatchTopic, Topic, TopicTree};

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const SSL_PORT: u16 = 8883;