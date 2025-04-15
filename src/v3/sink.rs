use std::{cell::Cell, fmt, future::ready, future::Future, num::NonZeroU16, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either, future::Ready};

use super::{codec, error::SendPacketError, shared::AckType, shared::MqttShared};
use crate::{error::EncodeError, types::QoS};

pub struct MqttSink(Rc<MqttShared>);

impl Clone for MqttSink {
    fn clone(&self) -> Self {
        MqttSink(self.0.clone())
    }
}

impl MqttSink {
    pub(crate) fn new(state: Rc<MqttShared>) -> Self {
        MqttSink(state)
    }

    pub(super) fn shared(&self) -> Rc<MqttShared> {
        self.0.clone()
    }

    #[inline]
    /// Check if io stream is open
    pub fn is_open(&self) -> bool {
        !self.0.is_closed()
    }

    #[inline]
    /// Check if sink is ready
    pub fn is_ready(&self) -> bool {
        if self.0.is_closed() {
            false
        } else {
            self.0.is_ready()
        }
    }

    #[inline]
    /// Get client receive credit
    pub fn credit(&self) -> usize {
        self.0.credit()
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.is_closed() {
            self.0
                .wait_readiness()
                .map(|rx| Either::Right(async move { rx.await.is_ok() }))
                .unwrap_or_else(|| Either::Left(ready(true)))
        } else {
            Either::Left(ready(false))
        }
    }

    #[inline]
    /// Close mqtt connection
    pub fn close(&self) {
        self.0.close();
    }

    #[inline]
    /// Force close mqtt connection. mqtt dispatcher does not wait for uncompleted
    /// responses, but it flushes buffers.
    pub fn force_close(&self) {
        self.0.force_close();
    }

    #[inline]
    /// Send ping
    pub(super) fn ping(&self) -> bool {
        self.0.encode_packet(codec::Packet::PingRequest).is_ok()
    }

    #[inline]
    /// Create publish message builder
    pub fn publish<U>(&self, topic: U, payload: Bytes) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        self.publish_pkt(
            codec::Publish {
                dup: false,
                retain: false,
                topic: topic.into(),
                qos: codec::QoS::AtMostOnce,
                packet_id: None,
                payload_size: payload.len() as u32,
            },
            payload,
        )
    }

    #[inline]
    /// Create publish builder with publish packet
    pub fn publish_pkt(&self, packet: codec::Publish, payload: Bytes) -> PublishBuilder {
        PublishBuilder { packet, payload, shared: self.0.clone() }
    }

    /// Set publish ack callback
    ///
    /// Use non-blocking send, PublishBuilder::send_at_least_once_no_block()
    /// First argument is packet id, second argument is "disconnected" state
    pub fn publish_ack_cb<F>(&self, f: F)
    where
        F: Fn(NonZeroU16, bool) + 'static,
    {
        self.0.set_publish_ack(Box::new(f));
    }

    #[inline]
    /// Create subscribe packet builder
    ///
    /// panics if id is 0
    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder { id: None, topic_filters: Vec::new(), shared: self.0.clone() }
    }

    #[inline]
    /// Create unsubscribe packet builder
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder { id: None, topic_filters: Vec::new(), shared: self.0.clone() }
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

pub struct PublishBuilder {
    packet: codec::Publish,
    shared: Rc<MqttShared>,
    payload: Bytes,
}

impl PublishBuilder {
    #[inline]
    /// Set packet id.
    ///
    /// Note: if packet id is not set, it gets generated automatically.
    /// Packet id management should not be mixed, it should be auto-generated
    /// or set by user. Otherwise collisions could occure.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        let id = NonZeroU16::new(id).expect("id 0 is not allowed");
        self.packet.packet_id = Some(id);
        self
    }

    #[inline]
    /// This might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    #[inline]
    /// Set retain flag
    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    #[inline]
    /// Get size of the publish packet
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_publish_size(&self.packet) as u32
    }

    /// Create streamimng publish builder
    pub fn streaming(mut self, size: u32) -> (StreamingPublishBuilder, StreamingPayload) {
        self.packet.payload_size = size;
        let payload = if self.payload.is_empty() { None } else { Some(self.payload) };

        let (tx, rx) = self.shared.pool.waiters.channel();
        (
            StreamingPublishBuilder {
                size,
                payload,
                tx: Some(tx),
                shared: self.shared.clone(),
                packet: self.packet,
            },
            StreamingPayload {
                rx: Cell::new(Some(rx)),
                shared: self.shared.clone(),
                inprocess: Cell::new(false),
            },
        )
    }

    #[inline]
    /// Send publish packet with QoS 0
    pub fn send_at_most_once(mut self) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = codec::QoS::AtMostOnce;
            self.shared
                .encode_publish(self.packet, Some(self.payload))
                .map_err(SendPacketError::Encode)
                .map(|_| ())
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(mut self) -> impl Future<Output = Result<(), SendPacketError>> {
        if !self.shared.is_closed() {
            self.packet.qos = codec::QoS::AtLeastOnce;

            // handle client receive maximum
            if let Some(rx) = self.shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    self.send_at_least_once_inner().await
                }))
            } else {
                Either::Left(Either::Right(self.send_at_least_once_inner()))
            }
        } else {
            Either::Right(Ready::Err(SendPacketError::Disconnected))
        }
    }

    /// Non-blocking send publish packet with QoS 1
    ///
    /// Panics if sink is not ready or publish ack callback is not set
    pub fn send_at_least_once_no_block(mut self) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            // check readiness
            if !self.shared.is_ready() {
                panic!("Mqtt sink is not ready");
            }
            self.packet.qos = codec::QoS::AtLeastOnce;
            let idx = self.shared.set_publish_id(&mut self.packet);

            log::trace!("Publish (QoS1) to {:#?}", self.packet);

            self.shared.wait_publish_response_no_block(
                idx,
                AckType::Publish,
                self.packet,
                Some(self.payload),
            )
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    fn send_at_least_once_inner(mut self) -> impl Future<Output = Result<(), SendPacketError>> {
        let idx = self.shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        let rx = self.shared.wait_publish_response(
            idx,
            AckType::Publish,
            self.packet,
            Some(self.payload),
        );
        async move { rx?.await.map(|_| ()).map_err(|_| SendPacketError::Disconnected) }
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: Option<NonZeroU16>,
    shared: Rc<MqttShared>,
    topic_filters: Vec<(ByteString, codec::QoS)>,
}

impl SubscribeBuilder {
    #[inline]
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, qos: codec::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

    #[inline]
    /// Get size of the subscribe packet
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_subscribe_size(&self.topic_filters) as u32
    }

    /// Send subscribe packet
    pub async fn send(self) -> Result<Vec<codec::SubscribeReturnCode>, SendPacketError> {
        if !self.shared.is_closed() {
            // handle client receive maximum
            if let Some(rx) = self.shared.wait_readiness() {
                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            let idx = self.id.unwrap_or_else(|| self.shared.next_id());
            let rx = self.shared.wait_response(idx, AckType::Subscribe)?;

            // send subscribe to client
            log::trace!(
                "Sending subscribe packet id: {} filters:{:?}",
                idx,
                self.topic_filters
            );

            match self.shared.encode_packet(codec::Packet::Subscribe {
                packet_id: idx,
                topic_filters: self.topic_filters,
            }) {
                Ok(_) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|pkt| pkt.subscribe())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

/// Unsubscribe packet builder
pub struct UnsubscribeBuilder {
    id: Option<NonZeroU16>,
    shared: Rc<MqttShared>,
    topic_filters: Vec<ByteString>,
}

impl UnsubscribeBuilder {
    #[inline]
    /// Set packet id.
    ///
    /// panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.topic_filters.push(filter);
        self
    }

    #[inline]
    /// Get size of the unsubscribe packet
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_unsubscribe_size(&self.topic_filters) as u32
    }

    /// Send unsubscribe packet
    pub async fn send(self) -> Result<(), SendPacketError> {
        let shared = self.shared;
        let filters = self.topic_filters;

        if !shared.is_closed() {
            // handle client receive maximum
            if let Some(rx) = shared.wait_readiness() {
                if rx.await.is_err() {
                    return Err(SendPacketError::Disconnected);
                }
            }
            // allocate packet id
            let idx = self.id.unwrap_or_else(|| shared.next_id());
            let rx = shared.wait_response(idx, AckType::Unsubscribe)?;

            // send subscribe to client
            log::trace!("Sending unsubscribe packet id: {} filters:{:?}", idx, filters);

            match shared.encode_packet(codec::Packet::Unsubscribe {
                packet_id: idx,
                topic_filters: filters,
            }) {
                Ok(_) => {
                    // wait ack from peer
                    rx.await.map_err(|_| SendPacketError::Disconnected).map(|_| ())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

pub struct StreamingPublishBuilder {
    shared: Rc<MqttShared>,
    packet: codec::Publish,
    payload: Option<Bytes>,
    size: u32,
    tx: Option<pool::Sender<()>>,
}

impl StreamingPublishBuilder {
    fn notify_payload_streamer(&mut self) -> Result<(), SendPacketError> {
        if let Some(tx) = self.tx.take() {
            tx.send(()).map_err(|_| SendPacketError::StreamingCancelled)
        } else {
            Ok(())
        }
    }

    /// Send publish packet with QoS 0
    pub fn send_at_most_once(mut self) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.notify_payload_streamer()?;

            self.packet.qos = QoS::AtMostOnce;
            self.shared
                .encode_publish(self.packet, self.payload)
                .map_err(SendPacketError::Encode)
                .map(|_| ())
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with QoS 1
    pub fn send_at_least_once(mut self) -> impl Future<Output = Result<(), SendPacketError>> {
        if !self.shared.is_closed() {
            self.packet.qos = QoS::AtLeastOnce;

            // handle client receive maximum
            if let Some(rx) = self.shared.wait_readiness() {
                Either::Left(Either::Left(async move {
                    if rx.await.is_err() {
                        return Err(SendPacketError::Disconnected);
                    }
                    self.send_at_least_once_inner().await
                }))
            } else {
                Either::Left(Either::Right(self.send_at_least_once_inner()))
            }
        } else {
            Either::Right(Ready::Err(SendPacketError::Disconnected))
        }
    }

    /// Non-blocking send publish packet with QoS 1
    ///
    /// Panics if sink is not ready or publish ack callback is not set
    pub fn send_at_least_once_no_block(mut self) -> Result<(), SendPacketError> {
        if !self.shared.is_closed() {
            // check readiness
            if !self.shared.is_ready() {
                panic!("Mqtt sink is not ready");
            }
            self.packet.qos = codec::QoS::AtLeastOnce;
            let tx = self.tx.take().unwrap();
            let idx = self.shared.set_publish_id(&mut self.packet);

            if tx.is_canceled() {
                Err(SendPacketError::StreamingCancelled)
            } else {
                log::trace!("Publish (QoS1) to {:#?}", self.packet);
                let _ = tx.send(());
                self.shared.wait_publish_response_no_block(
                    idx,
                    AckType::Publish,
                    self.packet,
                    self.payload,
                )
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    async fn send_at_least_once_inner(mut self) -> Result<(), SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        let tx = self.tx.take().unwrap();
        if tx.is_canceled() {
            Err(SendPacketError::StreamingCancelled)
        } else {
            let rx = self.shared.wait_publish_response(
                idx,
                AckType::Publish,
                self.packet,
                self.payload,
            );
            let _ = tx.send(());

            rx?.await.map(|_| ()).map_err(|_| SendPacketError::Disconnected)
        }
    }
}

pub struct StreamingPayload {
    shared: Rc<MqttShared>,
    rx: Cell<Option<pool::Receiver<()>>>,
    inprocess: Cell<bool>,
}

impl StreamingPayload {
    fn drop(&mut self) {
        if self.inprocess.get() {
            if self.shared.is_streaming() {
                self.shared.streaming_dropped();
            }
        }
    }
}

impl StreamingPayload {
    /// Send payload chunk
    pub async fn send(&self, chunk: Bytes) -> Result<(), SendPacketError> {
        if let Some(rx) = self.rx.take() {
            if rx.await.is_err() {
                return Err(SendPacketError::StreamingCancelled);
            }
            log::trace!("Publish is encoded, ready to process payload");
            self.inprocess.set(true);
        }

        if !self.inprocess.get() {
            Err(EncodeError::UnexpectedPayload.into())
        } else {
            log::trace!("Sending payload chunk: {:?}", chunk.len());
            self.shared.want_payload_stream().await?;

            if !self.shared.encode_publish_payload(chunk)? {
                self.inprocess.set(false);
            }
            Ok(())
        }
    }
}
