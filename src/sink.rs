use std::collections::VecDeque;
use std::fmt;

use actix_utils::framed::FramedMessage;
use bytes::Bytes;
use futures::unsync::{mpsc, oneshot};
use futures::Future;
use mqtt_codec as mqtt;

use crate::cell::Cell;

#[derive(Clone)]
pub struct MqttSink(Cell<MqttSinkInner>);

struct MqttSinkInner {
    idx: u16,
    tx: mpsc::UnboundedSender<FramedMessage<mqtt::Packet>>,
    queue: VecDeque<(u16, oneshot::Sender<mqtt::Packet>)>,
}

impl MqttSink {
    pub(crate) fn new(tx: mpsc::UnboundedSender<FramedMessage<mqtt::Packet>>) -> Self {
        MqttSink(Cell::new(MqttSinkInner {
            tx,
            idx: 0,
            queue: VecDeque::new(),
        }))
    }

    pub fn close(&mut self) {
        let _ = self.0.get_mut().tx.unbounded_send(FramedMessage::Close);
    }

    pub fn publish(
        &mut self,
        topic: string::String<Bytes>,
        payload: Bytes,
    ) -> impl Future<Item = mqtt::Packet, Error = ()> {
        let (tx, rx) = oneshot::channel();

        let inner = self.0.get_mut();
        inner.queue.push_back((inner.idx, tx));

        let publish = mqtt::Publish {
            topic,
            payload,
            dup: false,
            retain: false,
            qos: mqtt::QoS::AtLeastOnce,
            packet_id: Some(inner.idx),
        };
        let _ = inner
            .tx
            .unbounded_send(FramedMessage::Message(mqtt::Packet::Publish(publish)));
        inner.idx += 1;
        rx.map_err(|_| ())
    }

    pub fn publish_qos0(&mut self, topic: string::String<Bytes>, payload: Bytes) {
        let inner = self.0.get_mut();
        let publish = mqtt::Publish {
            topic,
            payload,
            dup: false,
            retain: false,
            qos: mqtt::QoS::AtMostOnce,
            packet_id: Some(inner.idx),
        };
        let _ = inner
            .tx
            .unbounded_send(FramedMessage::Message(mqtt::Packet::Publish(publish)));
        inner.idx += 1;
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}
