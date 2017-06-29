#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate itertools;
extern crate rand;
#[macro_use]
extern crate nom;
extern crate byteorder;
extern crate bytes;
extern crate slab;
extern crate tokio_io;

mod error;
#[macro_use]
mod topic;
#[macro_use]
mod proto;
mod packet;
mod encode;
mod decode;
mod codec;

pub use proto::{QoS, Protocol};
pub use topic::{Level, Topic, TopicTree, MatchTopic};
pub use packet::{Packet, LastWill, Connect, ConnectReturnCode, SubscribeReturnCode};
pub use encode::{WritePacketExt, calc_remaining_length};
pub use decode::{ReadPacketExt, read_packet};
pub use codec::Codec;

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const SSL_PORT: u16 = 8883;
