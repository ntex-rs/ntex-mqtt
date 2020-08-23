use ntex_codec::Framed;

use super::connection::Client;
use crate::v5::codec;

pub struct ConnectAck<Io> {
    io: Framed<Io, codec::Codec>,
    pkt: codec::ConnectAck,
}

impl<Io> ConnectAck<Io> {
    pub(super) fn new(pkt: codec::ConnectAck, io: Framed<Io, codec::Codec>) -> ConnectAck<Io> {
        Self { io, pkt }
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.pkt.session_present
    }

    #[inline]
    /// Connect return code
    pub fn reason_code(&self) -> codec::ConnectAckReason {
        self.pkt.reason_code
    }

    #[inline]
    /// Get reference to `ConnectAck` packet
    pub fn packet(&self) -> &codec::ConnectAck {
        &self.pkt
    }

    #[inline]
    /// Get mutable reference to `ConnectAck` packet
    pub fn packet_mut(&mut self) -> &mut codec::ConnectAck {
        &mut self.pkt
    }

    pub async fn ack(self) -> Client<Io> {
        todo!()
    }
}

// let srv = self.connect.clone();
// let packet = self.packet.clone();
// let keep_alive = Duration::from_secs(self.keep_alive as u64);
// let max_receive = self.max_receive;
// if max_receive > 0 {
//     packet.receive_max = Some(NonZeroU16::new(max_receive).unwrap())
// }

// // send Connect packet
// async move {
//     let mut framed = req.codec(codec::Codec::new());
//     framed.set_keepalive_timeout(keep_alive);
//     framed.send(codec::Packet::Connect(packet)).await.map_err(MqttError::from)?;

//     let packet = framed
//         .next()
//         .await
//         .ok_or_else(|| {
//             log::trace!("Client mqtt is disconnected during handshake");
//             MqttError::Disconnected
//         })
//         .and_then(|res| res.map_err(From::from))?;

//     match packet {
//         codec::Packet::ConnectAck(packet) => {
//             let (tx, rx) = mpsc::channel();
//             let sink = MqttSink::new(
//                 tx,
//                 packet.receive_max.map(|v| v.get()).unwrap_or(16) as usize,
//             );
//             let ack = ConnectAck { sink, packet, io: framed };
//             Ok(srv
//                 .as_ref()
//                 .call(ack)
//                 .await
//                 .map_err(MqttError::Service)
//                 .map(move |ack| ack.io.out(rx).state(ack.state))?)
//         }
//         p => Err(MqttError::Protocol(ProtocolError::Unexpected(
//             p.packet_type(),
//             "Expected CONNECT-ACK packet",
//         ))),
//     }
// }
