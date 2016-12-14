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
    PublishAck {
        packet_id: PacketId,
        msg: Rc<Message<'a>>,
    },
    PublishComplete { packet_id: PacketId },
    SubscribeAck {
        packet_id: PacketId,
        topic_filters: &'a [(&'a str, QoS)],
    },
    UnsubscribeAck {
        packet_id: PacketId,
        topic_filters: &'a [&'a str],
    },
}

pub trait Handler {
    fn on_received_message(&mut self, msg: &Message);

    fn on_subscribed_topic(&mut self, topics: &[(&str, SubscribeReturnCode)]);

    fn on_unsubscribed_topic(&mut self, topics: &[&str]);
}

#[derive(Debug)]
pub struct Session<'a, H: 'a + Handler> {
    handler: &'a mut H,

    // QoS 1 and QoS 2 messages which have been sent to the Server,
    // but have not been completely acknowledged.
    waiting_reply: Slab<Waiting<'a>>,
}

impl<'a, H: Handler> Session<'a, H> {
    pub fn new(handler: &'a mut H) -> Self {
        Session {
            handler: handler,
            waiting_reply: Slab::with_capacity(256),
        }
    }

    pub fn reset(&mut self) {
        self.waiting_reply.clear();
    }

    pub fn connect(&'a mut self,
                   client_id: &'a ClientId,
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
            client_id: client_id,
            username: auth.map(|(username, _)| username),
            password: auth.map(|(_, password)| password),
        }
    }

    fn delivery_retry(&mut self) -> Vec<Packet<'a>> {
        self.waiting_reply
            .iter()
            .map(|ref waiting| {
                match *waiting {
                    &Waiting::PublishAck { packet_id, ref msg } => {
                        Packet::Publish {
                            dup: false,
                            retain: false,
                            qos: msg.qos,
                            topic: msg.topic,
                            packet_id: Some(packet_id),
                            payload: msg.payload,
                        }
                    }
                    &Waiting::PublishComplete { packet_id } => {
                        Packet::PublishRelease { packet_id: packet_id }
                    }
                    &Waiting::SubscribeAck { packet_id, ref topic_filters } => {
                        Packet::Subscribe {
                            packet_id: packet_id,
                            topic_filters: From::from(*topic_filters),
                        }
                    }
                    &Waiting::UnsubscribeAck { packet_id, ref topic_filters } => {
                        Packet::Unsubscribe {
                            packet_id: packet_id,
                            topic_filters: From::from(*topic_filters),
                        }
                    }
                }
            })
            .collect()
    }

    pub fn ping(&mut self) -> Packet<'a> {
        Packet::PingRequest
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

            Some(entry.insert(Waiting::PublishAck {
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

            entry.replace(Waiting::PublishComplete { packet_id: packet_id });

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

    pub fn subscribe(&mut self, topic_filters: &'a [(&'a str, QoS)]) -> Option<Packet<'a>> {
        if let Some(entry) = self.waiting_reply.vacant_entry() {
            let packet_id = entry.index() as PacketId;

            entry.insert(Waiting::SubscribeAck {
                packet_id: packet_id,
                topic_filters: topic_filters,
            });

            Some(Packet::Subscribe {
                packet_id: packet_id,
                topic_filters: From::from(topic_filters),
            })
        } else {
            warn!("too many message waiting ack, downgrade to QoS level 0");

            None
        }
    }

    fn on_subscribe_ack(&mut self, packet_id: PacketId, status: &[SubscribeReturnCode]) {
        if let Some(Waiting::SubscribeAck { topic_filters, .. }) = self.waiting_reply
            .remove(packet_id as usize) {
            debug!("subscribe {} acked", packet_id);

            let status = topic_filters.iter()
                .map(|&(topic, _)| topic)
                .zip(status.iter().map(|code| *code))
                .collect::<Vec<(&str, SubscribeReturnCode)>>();

            self.handler.on_subscribed_topic(status.as_slice());
        } else {
            warn!("unexpected packet id {}", packet_id);
        }
    }

    pub fn unsubscribe(&mut self, topic_filters: &'a [&'a str]) -> Option<Packet<'a>> {
        if let Some(entry) = self.waiting_reply.vacant_entry() {
            let packet_id = entry.index() as PacketId;

            entry.insert(Waiting::UnsubscribeAck {
                packet_id: packet_id,
                topic_filters: topic_filters,
            });

            Some(Packet::Unsubscribe {
                packet_id: packet_id,
                topic_filters: From::from(topic_filters),
            })
        } else {
            warn!("too many message waiting ack, downgrade to QoS level 0");

            None
        }
    }

    fn on_unsubscribe_ack(&mut self, packet_id: PacketId) {
        if let Some(Waiting::UnsubscribeAck { topic_filters, .. }) = self.waiting_reply
            .remove(packet_id as usize) {
            debug!("unsubscribe {} acked", packet_id);

            self.handler.on_unsubscribed_topic(topic_filters)
        } else {
            warn!("unexpected packet id {}", packet_id)
        }
    }
}

pub struct Client<'a, T: Transport, H: 'a + Handler> {
    transport: T,
    session: Session<'a, H>,
    client_id: ClientId,
    keep_alive: Duration,
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
                              self.client_id,
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
                              self.client_id,
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
            Packet::SubscribeAck { packet_id, ref status } => {
                self.session
                    .on_subscribe_ack(packet_id, status);
            }
            Packet::UnsubscribeAck { packet_id } => {
                self.session
                    .on_unsubscribe_ack(packet_id);
            }
            Packet::PingResponse => {
                debug!("received ping response");
            }
            _ => {
                warn!("unexpected packet {}", packet.packet_type());
            }
        }
    }
}

pub struct Builder {
    client_id: ClientId,
    keep_alive: Duration,
}

impl Builder {
    pub fn client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = client_id;
        self
    }

    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn build<'a, T: Transport, H: 'a + Handler>(self,
                                                    transport: T,
                                                    handler: &'a mut H)
                                                    -> Client<'a, T, H> {
        Client {
            transport: transport,
            session: Session::new(handler),
            client_id: self.client_id,
            keep_alive: self.keep_alive,
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            client_id: ClientId::new(),
            keep_alive: Duration::new(0, 0),
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
}
