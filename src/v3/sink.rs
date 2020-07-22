use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroU16;
use std::rc::Rc;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{err, Either, Future, TryFutureExt};
use ntex::channel::{mpsc, oneshot};

use super::codec as mqtt;

pub struct MqttSink(Rc<RefCell<MqttSinkInner>>);

pub(crate) struct MqttSinkInner {
    idx: u16,
    queue: VecDeque<(u16, oneshot::Sender<()>)>,
    sink: Option<mpsc::Sender<mqtt::Packet>>,
}

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    pub(crate) fn new(sink: mpsc::Sender<mqtt::Packet>) -> Self {
        MqttSink(Rc::new(RefCell::new(MqttSinkInner {
            idx: 0,
            sink: Some(sink),
            queue: VecDeque::new(),
        })))
    }

    /// Close mqtt connection
    pub fn close(&self) {
        let _ = self.0.borrow_mut().sink.take();
    }

    /// Create publish message builder
    pub fn publish(&self, topic: ByteString, payload: Bytes) -> PublishBuilder<'_> {
        PublishBuilder {
            packet: Some(mqtt::Publish {
                topic,
                payload,
                dup: false,
                retain: false,
                qos: mqtt::QoS::AtMostOnce,
                packet_id: None,
            }),
            sink: self,
        }
    }

    pub(crate) fn complete_publish_qos1(&self, packet_id: NonZeroU16) {
        if let Some((idx, tx)) = self.0.borrow_mut().queue.pop_front() {
            if idx != packet_id.get() {
                log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    packet_id
                );
                self.close();
            } else {
                log::trace!("Ack publish packet with id: {}", packet_id);
                let _ = tx.send(());
            }
        } else {
            log::trace!("Unexpected PublishAck packet");
            self.close();
        }
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

    /// Send publish packet with QoS 0
    pub fn at_most_once(&mut self) {
        if let Some(packet) = self.packet.take() {
            if let Some(ref sink) = self.sink.0.borrow().sink {
                log::trace!("Publish (QoS-0) to {:?}", packet.topic);
                let _ = sink.send(mqtt::Packet::Publish(packet)).map_err(|_| {
                    log::error!("Mqtt sink is disconnected");
                });
            } else {
                log::error!("Mqtt sink is disconnected");
            }
        } else {
            panic!("PublishBuilder can be used only once.");
        }
    }

    /// Send publish packet with QoS 1
    pub fn at_least_once(&mut self) -> impl Future<Output = Result<(), ()>> {
        if let Some(mut packet) = self.packet.take() {
            let mut inner = self.sink.0.borrow_mut();

            if inner.sink.is_some() {
                let (tx, rx) = oneshot::channel();

                inner.idx += 1;
                if inner.idx == 0 {
                    inner.idx = 1
                }
                let idx = inner.idx;
                inner.queue.push_back((idx, tx));

                packet.qos = mqtt::QoS::AtLeastOnce;
                packet.packet_id = NonZeroU16::new(inner.idx);

                log::trace!("Publish (QoS1) to {:#?}", packet);
                if inner
                    .sink
                    .as_ref()
                    .unwrap()
                    .send(mqtt::Packet::Publish(packet))
                    .is_err()
                {
                    Either::Right(err(()))
                } else {
                    Either::Left(rx.map_err(|_| ()))
                }
            } else {
                Either::Right(err(()))
            }
        } else {
            panic!("PublishBuilder can be used only once.");
        }
    }
}
