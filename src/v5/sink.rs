use std::{cell::RefCell, collections::VecDeque, fmt, num::NonZeroU16, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::Future;
use ntex::channel::{mpsc, oneshot};

use super::codec as mqtt;
use crate::types::QoS;

pub struct MqttSink(Rc<RefCell<MqttSinkInner>>);

pub(crate) struct MqttSinkInner {
    idx: u16,
    cap: usize,
    sink: Option<mpsc::Sender<mqtt::Packet>>,
    queue: VecDeque<(u16, oneshot::Sender<()>)>,
    waiters: VecDeque<oneshot::Sender<()>>,
}

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    pub(crate) fn new(sink: mpsc::Sender<mqtt::Packet>, max_receive: usize) -> Self {
        MqttSink(Rc::new(RefCell::new(MqttSinkInner {
            idx: 0,
            cap: max_receive,
            sink: Some(sink),
            queue: VecDeque::new(),
            waiters: VecDeque::new(),
        })))
    }

    /// Get client receive credit
    pub fn credit(&self) -> usize {
        let inner = self.0.borrow();
        inner.cap - inner.queue.len()
    }

    /// Close mqtt connection with default Disconnect message
    pub fn close(&self) {
        if let Some(sink) = self.0.borrow_mut().sink.take() {
            let _ = sink.send(mqtt::Packet::Disconnect(mqtt::Disconnect::default()));
        }
    }

    /// Close mqtt connection
    pub fn close_with_reason(&self, pkt: mqtt::Disconnect) {
        if let Some(sink) = self.0.borrow_mut().sink.take() {
            let _ = sink.send(mqtt::Packet::Disconnect(pkt));
        }
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self) {
        let _ = self.0.borrow_mut().sink.take();
    }

    /// Send publish packet
    pub fn publish(&self, topic: ByteString, payload: Bytes) -> PublishBuilder<'_> {
        PublishBuilder {
            packet: Some(mqtt::Publish {
                topic,
                payload,
                dup: false,
                retain: false,
                qos: QoS::AtMostOnce,
                packet_id: None,
                properties: mqtt::PublishProperties::default(),
            }),
            sink: self,
        }
    }

    pub(crate) fn complete_publish_qos1(&self, packet_id: NonZeroU16) -> bool {
        let mut inner = self.0.borrow_mut();

        if let Some((idx, tx)) = inner.queue.pop_front() {
            if idx != packet_id.get() {
                log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    packet_id
                );
            } else {
                log::trace!("Ack publish packet with id: {}", packet_id);
                let _ = tx.send(());

                while let Some(tx) = inner.waiters.pop_front() {
                    if tx.send(()).is_ok() {
                        break;
                    }
                }
                return true;
            }
        } else {
            log::trace!("Unexpected PublishAck packet");
        }
        false
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

pub struct PublishBuilder<'a> {
    sink: &'a MqttSink,
    packet: Option<mqtt::Publish>,
}

impl<'a> PublishBuilder<'a> {
    /// this might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(&mut self, val: bool) -> &mut Self {
        if let Some(ref mut packet) = self.packet {
            packet.dup = val;
        }
        self
    }

    pub fn retain(&mut self) -> &mut Self {
        if let Some(ref mut packet) = self.packet {
            packet.retain = true;
        }
        self
    }

    pub fn properties<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut mqtt::PublishProperties),
    {
        if let Some(ref mut packet) = self.packet {
            f(&mut packet.properties);
        }
        self
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(&mut self) {
        if let Some(packet) = self.packet.take() {
            if let Some(ref sink) = self.sink.0.borrow().sink {
                log::trace!("Publish (QoS-0) to {:?}", packet.topic);
                let _ = sink.send(mqtt::Packet::Publish(packet));
            } else {
                log::error!("Mqtt sink is disconnected");
            }
        } else {
            panic!("PublishBuilder can be used only once.");
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(&mut self) -> impl Future<Output = Result<(), ()>> {
        if let Some(mut packet) = self.packet.take() {
            let sink = self.sink.0.clone();

            async move {
                let mut inner = sink.borrow_mut();
                if inner.sink.is_some() {
                    // handle client receive maximum
                    if inner.cap - inner.queue.len() == 0 {
                        let (tx, rx) = oneshot::channel();
                        inner.waiters.push_back(tx);

                        drop(inner);
                        if rx.await.is_err() {
                            return Err(());
                        }

                        inner = sink.borrow_mut();
                    }

                    // send publish to client
                    let (tx, rx) = oneshot::channel();

                    inner.idx += 1;
                    if inner.idx == 0 {
                        inner.idx = 1
                    }
                    let idx = inner.idx;
                    inner.queue.push_back((idx, tx));

                    packet.qos = QoS::AtLeastOnce;
                    packet.packet_id = NonZeroU16::new(inner.idx);

                    log::trace!("Publish (QoS1) to {:#?}", packet);

                    let send_result =
                        inner.sink.as_ref().unwrap().send(mqtt::Packet::Publish(packet));
                    drop(inner);

                    if send_result.is_err() {
                        Err(())
                    } else {
                        rx.await.map_err(|_| ())
                    }
                } else {
                    Err(())
                }
            }
        } else {
            panic!("PublishBuilder can be used only once.");
        }
    }
}
