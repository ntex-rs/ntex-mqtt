use std::collections::VecDeque;
use std::fmt;

use actix_ioframe::Sink;
use bytes::Bytes;
use futures::unsync::oneshot;
use futures::Future;
use mqtt_codec as mqtt;

use crate::cell::Cell;

#[derive(Clone)]
pub struct MqttSink {
    sink: Sink<mqtt::Packet>,
    pub(crate) inner: Cell<MqttSinkInner>,
}

#[derive(Default)]
pub(crate) struct MqttSinkInner {
    pub(crate) idx: u16,
    pub(crate) queue: VecDeque<(u16, oneshot::Sender<mqtt::Packet>)>,
}

impl MqttSink {
    pub(crate) fn new(sink: Sink<mqtt::Packet>) -> Self {
        MqttSink {
            sink,
            inner: Cell::new(MqttSinkInner::default()),
        }
    }

    /// Close mqtt connection
    pub fn close(&self) {
        self.sink.close();
    }

    /// Send publish packet
    pub fn publish(
        &mut self,
        topic: string::String<Bytes>,
        payload: Bytes,
    ) -> impl Future<Item = mqtt::Packet, Error = ()> {
        let (tx, rx) = oneshot::channel();

        let inner = self.inner.get_mut();
        inner.queue.push_back((inner.idx, tx));

        let publish = mqtt::Publish {
            topic,
            payload,
            dup: false,
            retain: false,
            qos: mqtt::QoS::AtLeastOnce,
            packet_id: Some(inner.idx),
        };
        self.sink.send(mqtt::Packet::Publish(publish));
        inner.idx += 1;
        rx.map_err(|_| ())
    }

    /// Send publish packet with qos set to 0
    pub fn publish_qos0(&self, topic: string::String<Bytes>, payload: Bytes) {
        let publish = mqtt::Publish {
            topic,
            payload,
            dup: false,
            retain: false,
            qos: mqtt::QoS::AtMostOnce,
            packet_id: None,
        };
        self.sink.send(mqtt::Packet::Publish(publish));
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}
