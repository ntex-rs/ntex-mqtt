use std::rc::Rc;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use slab::Slab;

use error::*;
use proto::*;
use topic::*;
use packet::*;
use transport::{self, Transport};

#[derive(Debug, PartialEq, Clone)]
enum Waiting<'a> {
    Ack {
        packet_id: PacketId,
        msg: Rc<Message<'a>>,
    },
    Complete { packet_id: PacketId },
}

pub trait Handler {
    fn on_received_message(&mut self, msg: &Message);
}

#[derive(Debug)]
pub struct Session<'a, H: 'a + Handler> {
    handler: &'a mut H,

    // The Client Identifier (ClientId) identifies the Client to the Server.
    client_id: ClientId,
    // QoS 1 and QoS 2 messages which have been sent to the Server,
    // but have not been completely acknowledged.
    waiting_reply: Slab<Waiting<'a>>,
}

impl<'a, H: Handler> Session<'a, H> {
    pub fn new(handler: &'a mut H) -> Self {
        Session {
            handler: handler,
            client_id: ClientId::new(),
            waiting_reply: Slab::with_capacity(256),
        }
    }

    pub fn reset(&mut self) {
        self.client_id = ClientId::new();
        self.waiting_reply.clear();
    }

    pub fn connect(&'a mut self,
                   clean_session: bool,
                   keep_alive: u16,
                   auth: Option<(&'a str, &'a [u8])>,
                   last_will: Option<Rc<Message<'a>>>)
                   -> Packet<'a> {
        Packet::Connect {
            protocol: Default::default(),
            clean_session: clean_session,
            keep_alive: keep_alive,
            last_will: last_will.map(|msg| {
                LastWill {
                    topic: msg.topic,
                    message: msg.payload,
                    qos: msg.qos,
                    retain: false,
                }
            }),
            client_id: &self.client_id,
            username: auth.map(|(username, _)| username),
            password: auth.map(|(_, password)| password),
        }
    }

    fn delivery_retry(&mut self) -> Vec<Packet<'a>> {
        self.waiting_reply
            .iter()
            .map(|ref waiting| {
                match *waiting {
                    &Waiting::Ack { packet_id, ref msg } => {
                        Packet::Publish {
                            dup: false,
                            retain: false,
                            qos: msg.qos,
                            topic: msg.topic,
                            packet_id: Some(packet_id),
                            payload: msg.payload,
                        }
                    }
                    &Waiting::Complete { packet_id } => {
                        Packet::PublishRelease { packet_id: packet_id }
                    }
                }
            })
            .collect()
    }

    pub fn disconnect(&mut self) -> Packet<'a> {
        Packet::Disconnect
    }

    pub fn publish(&mut self, msg: Rc<Message<'a>>) -> Packet<'a> {
        Packet::Publish {
            dup: false,
            retain: false,
            qos: msg.qos,
            topic: msg.topic,
            packet_id: match msg.qos {
                QoS::AtLeastOnce | QoS::ExactlyOnce => self.wait_reply(msg.clone()),
                _ => None,
            },
            payload: msg.payload,
        }
    }

    fn wait_reply(&mut self, msg: Rc<Message<'a>>) -> Option<PacketId> {
        if let Some(entry) = self.waiting_reply.vacant_entry() {
            let packet_id = entry.index() as PacketId;

            Some(entry.insert(Waiting::Ack {
                    packet_id: packet_id,
                    msg: msg.clone(),
                })
                .index() as PacketId)
        } else {
            warn!("too many message waiting ack, downgrade to QoS level 0");

            None
        }
    }

    fn on_publish_ack(&mut self, packet_id: PacketId) -> Option<Packet<'a>> {
        if let Some(_) = self.waiting_reply.remove(packet_id as usize) {
            debug!("message {} acknowledged", packet_id);
        } else {
            warn!("unexpected packet id {}", packet_id)
        }

        None
    }

    fn on_publish_received(&mut self, packet_id: PacketId) -> Option<Packet<'a>> {
        if let Some(mut entry) = self.waiting_reply.entry(packet_id as usize) {
            debug!("message {} received at server side", packet_id);

            entry.replace(Waiting::Complete { packet_id: packet_id });

            Some(Packet::PublishRelease { packet_id: packet_id })
        } else {
            warn!("unexpected packet id {}", packet_id);

            None
        }
    }

    fn on_publish_complete(&mut self, packet_id: PacketId) -> Option<Packet<'a>> {
        if let Some(_) = self.waiting_reply.remove(packet_id as usize) {
            debug!("message {} completed", packet_id);
        } else {
            warn!("unexpected packet id {}", packet_id)
        }

        None
    }

    fn on_publish(&mut self,
                  dup: bool,
                  retain: bool,
                  packet_id: Option<PacketId>,
                  msg: Rc<Message<'a>>)
                  -> Option<Packet<'a>> {
        self.handler.on_received_message(&msg);

        packet_id.and_then(|packet_id| self.send_reply(packet_id, msg))
    }

    fn send_reply(&mut self, packet_id: PacketId, msg: Rc<Message<'a>>) -> Option<Packet<'a>> {
        match msg.qos {
            QoS::AtLeastOnce => Some(Packet::PublishAck { packet_id: packet_id }),
            QoS::ExactlyOnce => Some(Packet::PublishReceived { packet_id: packet_id }),
            _ => None,
        }
    }

    fn on_publish_release(&mut self, packet_id: PacketId) -> Option<Packet<'a>> {
        Some(Packet::PublishComplete { packet_id: packet_id })
    }
}

pub struct Client<'a, T: Transport, H: 'a + Handler> {
    transport: T,
    session: Session<'a, H>,
    keep_alive: Duration,
    last_packet: Instant,
}

impl<'a, T: Transport, H: 'a + Handler> Client<'a, T, H> {
    pub fn close(&mut self) -> Result<()> {
        info!("client session closed");

        self.transport.close()
    }
}

impl<'a, T: Transport, H: 'a + Handler> transport::Handler<'a> for Client<'a, T, H> {
    fn on_received_packet(&mut self, packet: &Packet<'a>) {
        match *packet {
            Packet::ConnectAck { session_present, return_code } => {
                match return_code {
                    ConnectReturnCode::ConnectionAccepted => {
                        info!("client session `{}` {}",
                              self.session.client_id,
                              if session_present {
                                  "resumed"
                              } else {
                                  "created"
                              });

                        self.session
                            .delivery_retry()
                            .iter()
                            .map(|packet| self.transport.send_packet(&packet));
                    }
                    _ => {
                        info!("client session `{}` refused, {}",
                              self.session.client_id,
                              return_code.reason());

                        self.close();
                    }
                }
            }
            Packet::Publish { dup, retain, qos, topic, packet_id, payload } => {
                self.session
                    .on_publish(dup,
                                retain,
                                packet_id,
                                Rc::new(Message {
                                    topic: topic,
                                    payload: payload,
                                    qos: qos,
                                }))
                    .and_then(|packet| self.transport.send_packet(&packet).ok());
            }
            Packet::PublishAck { packet_id } => {
                self.session
                    .on_publish_ack(packet_id)
                    .and_then(|packet| self.transport.send_packet(&packet).ok());
            }
            Packet::PublishReceived { packet_id } => {
                self.session
                    .on_publish_received(packet_id)
                    .and_then(|packet| self.transport.send_packet(&packet).ok());
            }
            Packet::PublishRelease { packet_id } => {
                self.session
                    .on_publish_release(packet_id)
                    .and_then(|packet| self.transport.send_packet(&packet).ok());
            }
            Packet::PublishComplete { packet_id } => {
                self.session
                    .on_publish_complete(packet_id)
                    .and_then(|packet| self.transport.send_packet(&packet).ok());
            }
            Packet::PingResponse => {
                debug!("received ping response in {} seconds",
                       self.last_packet.elapsed().as_secs());

                self.last_packet = Instant::now();
            }
            _ => {
                warn!("unexpected packet {}", packet.packet_type());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
}
