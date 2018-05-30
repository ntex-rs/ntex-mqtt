use std::{rc::Rc, cell::RefCell, collections::VecDeque};
use futures::prelude::*;
use futures::{future, Future, task::{self, Task}, unsync::oneshot};
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
    write_queue: VecDeque<Packet>,
    write_task: Option<Task>,
    pending_ack: VecDeque<(u16, DeliveryPromise)>,
    pending_sub_acks: VecDeque<(u16, DeliveryPromise)>,
    next_packet_id: u16
}

struct ConnectionTransport<T: Sink<SinkItem = Packet, SinkError = Error> + 'static> {
    sink: T,
    connection: InnerRef,
    flushed: bool,
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
        let connection = Rc::new(RefCell::new(ConnectionInner::new()));
        let conn_transport = ConnectionTransport {
            sink: writer,
            connection: connection.clone(),
            flushed: true,
        };
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
        handle.spawn(conn_transport.map_err(|e| {
            // todo: handle error while writing
            println!("Error writing: {:?}", e);
        }));
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
    pub fn new() -> ConnectionInner {
        ConnectionInner {
            write_queue: VecDeque::new(),
            write_task: None,
            pending_ack: VecDeque::new(),
            pending_sub_acks: VecDeque::new(),
            next_packet_id: 1
        }
    }

    fn pop_next_packet(&mut self) -> Option<Packet> {
        self.write_queue.pop_front()
    }

    fn prepend_packet(&mut self, packet: Packet) {
        self.write_queue.push_front(packet);
    }

    pub fn post_packet(&mut self, packet: Packet) {
        self.write_queue.push_back(packet);
        if let Some(task) = self.write_task.take() {
            task.notify();
        }
        else {
            println!("there was no task around");
        }
    }

    fn set_write_task(&mut self) {
        if self.write_task.is_none() {
            self.write_task = Some(task::current());
        }
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
            self.post_packet(publish);
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
            self.post_packet(publish);
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

impl<T: Sink<SinkItem = Packet, SinkError = Error> + 'static> Future for ConnectionTransport<T> {
    type Item = ();
    type Error = Error;

    // Tick the state machine
    fn poll(&mut self) -> Poll<(), Error> {
        // TODO: Always tick the transport first -- heartbeat, etc.
        // self.dispatch.get_mut().inner.transport().tick();

        let mut conn = self.connection.borrow_mut();

        loop {
            loop {
                if let Some(packet) = conn.pop_next_packet() {
                    match self.sink.start_send(packet) {
                        Ok(AsyncSink::NotReady(packet)) => {
                            conn.prepend_packet(packet);
                            break;
                        }
                        Ok(AsyncSink::Ready) => {
                            //let _ = tx.send(Ok(())); todo: feedback for write out?
                            self.flushed = false;
                            continue;
                        }
                        Err(e) => {
                            bail!(e);
                            // let _ = tx.send(Err(err));
                        }
                    }
                } else {
                    conn.set_write_task();
                    break;
                }
            }

            let mut not_ready = true;

            // flush sink
            if !self.flushed {
                match self.sink.poll_complete() {
                    Ok(Async::Ready(_)) => {
                        not_ready = false;
                        self.flushed = true;
                        conn.set_write_task();
                    }
                    Ok(Async::NotReady) => (),
                    Err(e) => bail!(e),
                };
            }

            if not_ready {
                return Ok(Async::NotReady);
            }
        }
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
