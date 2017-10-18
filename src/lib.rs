#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate error_chain;
extern crate rand;
extern crate byteorder;
extern crate bytes;
extern crate slab;
extern crate tokio_io;
extern crate string;

mod error;
#[macro_use]
mod topic;
#[macro_use]
mod proto;
mod packet;
mod codec;

pub use proto::{QoS, Protocol};
pub use topic::{Level, Topic, TopicTree, MatchTopic};
pub use packet::{Packet, LastWill, Connect, ConnectReturnCode, SubscribeReturnCode};
pub use codec::Codec;

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const SSL_PORT: u16 = 8883;
