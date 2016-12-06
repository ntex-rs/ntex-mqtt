#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate nom;
extern crate byteorder;
extern crate bytes;

mod error;
mod packet;
mod encode;
mod decode;

pub mod transport;
pub mod server;
pub mod client;

#[cfg(test)]
mod tests;

pub use packet::{Packet, ConnectionWill, ConnectReturnCode, QoS, SubscribeReturnCode};
pub use encode::WritePacketExt;
pub use decode::{ReadPacketExt, read_packet};

// http://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
pub const TCP_PORT: u16 = 1883;
pub const SSL_PORT: u16 = 8883;
