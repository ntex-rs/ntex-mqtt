use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroU16;
use std::rc::Rc;

use bytes::Bytes;
use bytestring::ByteString;
use futures::future::{err, Either, Future, TryFutureExt};
use ntex::channel::{mpsc, oneshot};

use crate::codec3 as mqtt;

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

    /// Send publish packet with qos set to 0
    pub fn publish_qos0(&self, topic: ByteString, payload: Bytes, dup: bool) {
        if let Some(ref sink) = self.0.borrow().sink {
            log::trace!("Publish (QoS-0) to {:?}", topic);
            let publish = mqtt::Publish {
                topic,
                payload,
                dup,
                retain: false,
                qos: mqtt::QoS::AtMostOnce,
                packet_id: None,
            };
            let _ = sink.send(mqtt::Packet::Publish(publish));
        } else {
            log::error!("Mqtt sink is disconnected");
        }
    }

    /// Send publish packet
    pub fn publish_qos1(
        &mut self,
        topic: ByteString,
        payload: Bytes,
        dup: bool,
    ) -> impl Future<Output = Result<(), ()>> {
        let mut inner = self.0.borrow_mut();

        if inner.sink.is_some() {
            let (tx, rx) = oneshot::channel();

            inner.idx += 1;
            if inner.idx == 0 {
                inner.idx = 1
            }
            let idx = inner.idx;
            inner.queue.push_back((idx, tx));

            let publish = mqtt::Packet::Publish(mqtt::Publish {
                topic,
                payload,
                dup,
                retain: false,
                qos: mqtt::QoS::AtLeastOnce,
                packet_id: NonZeroU16::new(inner.idx),
            });
            log::trace!("Publish (QoS1) to {:#?}", publish);
            if inner.sink.as_ref().unwrap().send(publish).is_err() {
                Either::Right(err(()))
            } else {
                Either::Left(rx.map_err(|_| ()))
            }
        } else {
            Either::Right(err(()))
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
