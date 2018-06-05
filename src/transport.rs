use std::{rc::Rc, cell::RefCell, collections::VecDeque};
use futures::prelude::*;
use futures::{future, Future, unsync::{oneshot, mpsc}};
use tokio_io::{AsyncRead, AsyncWrite, codec::Framed};
use tokio_core::reactor;
use bytes::Bytes;
use string::String;
use proto::{QoS, Protocol};
use packet::{Packet, Connect, ConnectReturnCode};
use codec::Codec;

use error::{Error, Result};

type InnerRef = Rc<RefCell<ConnectionInner>>;
type DeliveryResponse = ();
type DeliveryPromise = oneshot::Sender<Result<DeliveryResponse>>;

pub struct Connection {
    inner: InnerRef
}

pub struct ConnectionInner {
    write_sender: mpsc::UnboundedSender<Packet>,
    pending_ack: VecDeque<(u16, DeliveryPromise)>,
    pending_sub_acks: VecDeque<(u16, DeliveryPromise)>,
    next_packet_id: u16
}

pub enum Delivery {
    Resolved(Result<DeliveryResponse>),
    Pending(oneshot::Receiver<Result<DeliveryResponse>>),
    Gone
}

impl Connection {
    pub fn open<T: AsyncRead + AsyncWrite + 'static>(client_id: String<Bytes>, handle: reactor::Handle, io: T) -> impl Future<Item = Connection, Error = Error> {
        let io = io.framed(Codec::new());
        let connect = Connect {
            protocol: Protocol::default(),
            clean_session: false,
            keep_alive: 0,
            last_will: None,
            client_id: client_id,
            username: None,
            password: None
        };
        io.send(Packet::Connect { connect: Box::new(connect) } )
            .from_err()
            .and_then(|io| io.into_future().map_err(|e| e.0.into()))
            .and_then(|(packet_opt, io)| {
                if let Some(packet) = packet_opt {
                    if let Packet::ConnectAck { session_present, return_code } = packet {
                        if let ConnectReturnCode::ConnectionAccepted = return_code {
                            // todo: surface session_present
                            Ok(Connection::new(handle, io))
                        }
                        else {
                            Err(return_code.reason().into())
                        }
                    }
                    else {
                        Err("Protocol violation: expected CONNACK".into())
                    }
                }
                else {
                    Err("Connection is closed.".into())
                }
            })
    }

    fn new<T: AsyncRead + AsyncWrite + 'static>(handle: reactor::Handle, io: Framed<T, Codec>) -> Connection {
        let (writer, reader) = io.split();
        let (tx, rx) = mpsc::unbounded();
        let connection = Rc::new(RefCell::new(ConnectionInner::new(tx)));
        let send_fut = writer.sink_map_err(|e| println!("sink err: {}", e)).send_all(rx);
        let reader_conn = connection.clone();
        let read_handling = reader.for_each(move |packet| {
            reader_conn
                .borrow_mut()
                .handle_packet(packet);
            Ok(())
        });
        handle.spawn(read_handling.map_err(|e| {
            // todo: handle error while reading
            println!("Error reading: {:?}", e);
        }));
        handle.spawn(send_fut.map(|_| ()));
        Connection { inner: connection }
    }

    pub fn send(&self, qos: QoS, topic: String<Bytes>, payload: Bytes) -> Delivery {
        if qos == QoS::ExactlyOnce {
            return Delivery::Resolved(Err("QoS 2 is not supported at this time.".into()));
        }
        self.inner.borrow_mut().send(qos, topic, payload)
    }

    pub fn subscribe(&mut self, topic_filters: Vec<(String<Bytes>, QoS)>) -> Delivery {
        self.inner.borrow_mut().subscribe(topic_filters)
    }

    pub fn close(self) -> impl Future<Item = (), Error = Error> {
        future::ok(())
    }
}

impl ConnectionInner {
    pub fn new(sender: mpsc::UnboundedSender<Packet>) -> ConnectionInner {
        ConnectionInner {
            write_sender: sender,
            pending_ack: VecDeque::new(),
            pending_sub_acks: VecDeque::new(),
            next_packet_id: 1
        }
    }

    pub fn post_packet(&self, packet: Packet) -> ::std::result::Result<(), mpsc::SendError<Packet>> {
        self.write_sender.unbounded_send(packet)
    }

    pub fn handle_packet(&mut self, packet: Packet) {
        match packet {
            Packet::PublishAck { packet_id } => {
                if let Some(pending) = self.pending_ack.pop_front() {
                    if pending.0 != packet_id {
                        println!("protocol violation");
                        // todo: handle protocol violation
                    }
                    pending.1.send(Ok(()));
                }
                else {
                    // todo: handle protocol violation
                }
            },
            Packet::SubscribeAck { packet_id, status } => {
                if let Some(pending) = self.pending_sub_acks.pop_front() {
                    if pending.0 != packet_id {
                        // todo: handle protocol violation                        
                    }
                    pending.1.send(Ok(())); // todo: surface subscribe outcome
                }
                else {
                    // todo: handle protocol violation                        
                }
            }
            Packet::UnsubscribeAck { packet_id } => {} // todo
            Packet::PingResponse => {} // todo
            _ => {
                // todo: handle protocol violation                        
            }
        }
    }

    pub fn send(&mut self, qos: QoS, topic: String<Bytes>, payload: Bytes) -> Delivery {
        if qos == QoS::AtMostOnce {
            let publish = Packet::Publish {
                dup: false,
                retain: false,
                qos,
                topic,
                packet_id: None,
                payload
            };
            if let Err(e) = self.post_packet(publish) {
                return Delivery::Resolved(Err(e.into()));
            }
            Delivery::Resolved(Ok(())) // todo: delay until handed out to network successfully
        }
        else {
            let packet_id = self.next_packet_id();
            let publish = Packet::Publish {
                dup: false,
                retain: false,
                qos,
                topic,
                packet_id: Some(packet_id),
                payload
            };
            let (delivery_tx, delivery_rx) = oneshot::channel();
            if let Err(e) = self.post_packet(publish) {
                return Delivery::Resolved(Err(e.into()));
            }
            self.pending_ack.push_back((packet_id, delivery_tx));
            Delivery::Pending(delivery_rx)
        }
    }

    pub fn subscribe(&mut self, topic_filters: Vec<(String<Bytes>, QoS)>) -> Delivery {
        let packet_id = self.next_packet_id();
        let subscribe = Packet::Subscribe {
            packet_id,
            topic_filters
        };
        self.post_packet(subscribe);
        let (delivery_tx, delivery_rx) = oneshot::channel();
        self.pending_sub_acks.push_back((packet_id, delivery_tx));
        Delivery::Pending(delivery_rx)
    }

    fn next_packet_id(&mut self) -> u16 { // todo: simple wrapping may not work for everything, think about honoring known in-flights
        let packet_id = self.next_packet_id;
        if packet_id == ::std::u16::MAX {
            self.next_packet_id = 1;
        }
        else {
            self.next_packet_id = packet_id + 1;
        }
        packet_id
    }
}

impl Future for Delivery {
    type Item = DeliveryResponse;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Delivery::Pending(ref mut receiver) = *self {
            return match receiver.poll() {
                Ok(Async::Ready(r)) => r.map(|state| Async::Ready(state)),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e.into())
            };
        }

        let old_v = ::std::mem::replace(self, Delivery::Gone);
        if let Delivery::Resolved(r) = old_v {
            return match r {
                Ok(state) => Ok(Async::Ready(state)),
                Err(e) => Err(e)
            };
        }
        panic!("Delivery was polled already.");
    }
}
