use std::{cell::Cell, fmt, future::Future, future::ready, num::NonZeroU16, rc::Rc};

use ntex_bytes::{ByteString, Bytes};
use ntex_util::{channel::pool, future::Either};

use crate::v3::shared::{Ack, AckType, MqttShared};
use crate::v3::{codec, error::SendPacketError};
use crate::{error::EncodeError, types::QoS};

/// Mqtt client/server sink, it is used to send packets to the peer
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
    /// Check if connection is active
    ///
    /// Returns `false` as soon as the connection starts closing, either
    /// locally or because the peer has gone. Buffered data may still be flushing.
    pub fn is_open(&self) -> bool {
        self.0.is_active()
    }

    #[inline]
    /// Check if sink is ready
    pub fn is_ready(&self) -> bool {
        if self.0.is_active() {
            self.0.is_ready()
        } else {
            false
        }
    }

    #[inline]
    /// Get remaining send credit
    ///
    /// Number of `QoS 1` and `QoS 2` publish packets that can be sent before
    /// `max_send` in-flight limit is reached.
    pub fn credit(&self) -> usize {
        self.0.credit()
    }

    /// Get notification when packet could be send to the peer.
    ///
    /// Result indicates if connection is alive
    pub fn ready(&self) -> impl Future<Output = bool> {
        if !self.0.is_active() {
            Either::Left(ready(false))
        } else if self.0.is_ready() {
            Either::Left(ready(true))
        } else {
            let shared = self.0.clone();
            Either::Right(async move { shared.wait_readiness().await })
        }
    }

    #[inline]
    /// Close mqtt connection.
    ///
    /// Client sink sends `Disconnect` packet first. Pending acks are cancelled
    /// with `SendPacketError::Disconnected` error.
    pub fn close(&self) {
        self.0.close();
    }

    #[inline]
    /// Force close mqtt connection.
    ///
    /// The connection is aborted immediately, mqtt dispatcher does not wait for
    /// uncompleted responses and buffered data is discarded. Use
    /// [`close`](Self::close) to close connection gracefully.
    pub fn force_close(&self) {
        self.0.force_close();
    }

    #[inline]
    /// Send ping.
    pub(super) fn ping(&self) -> bool {
        if self.0.encode_packet(codec::Packet::PingRequest).is_ok() {
            self.0.set_ping_pending(true);
            true
        } else {
            false
        }
    }

    /// Check if PINGREQ is sent and PINGRESP is not received yet
    pub(super) fn is_ping_pending(&self) -> bool {
        self.0.is_ping_pending()
    }

    /// Wait until already encoded packets can be written, returns `false` if the
    /// connection is closed
    pub(super) async fn wait_write_unblocked(&self) -> bool {
        self.0.wait_write_unblocked().await
    }

    #[inline]
    /// Create publish message builder.
    pub fn publish<U>(&self, topic: U) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        self.publish_pkt(codec::Publish {
            dup: false,
            retain: false,
            topic: topic.into(),
            qos: codec::QoS::AtMostOnce,
            packet_id: None,
            payload_size: 0,
        })
    }

    #[inline]
    /// Create publish builder with publish packet.
    pub fn publish_pkt(&self, packet: codec::Publish) -> PublishBuilder {
        PublishBuilder {
            packet,
            shared: self.0.clone(),
        }
    }

    /// Set publish ack callback.
    ///
    /// Use non-blocking send, `PublishBuilder::send_at_least_once_no_block()`
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
    /// Only a client sink sends the packet, on a server sink
    /// [`SubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
    pub fn subscribe(&self) -> SubscribeBuilder {
        SubscribeBuilder {
            id: None,
            topic_filters: Vec::new(),
            shared: self.0.clone(),
        }
    }

    #[inline]
    /// Create unsubscribe packet builder
    ///
    /// Only a client sink sends the packet, on a server sink
    /// [`UnsubscribeBuilder::send`] returns [`SendPacketError::NotAllowed`].
    pub fn unsubscribe(&self) -> UnsubscribeBuilder {
        UnsubscribeBuilder {
            id: None,
            topic_filters: Vec::new(),
            shared: self.0.clone(),
        }
    }
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("MqttSink").finish()
    }
}

/// Publish packet builder
pub struct PublishBuilder {
    packet: codec::Publish,
    shared: Rc<MqttShared>,
}

impl fmt::Debug for PublishBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishBuilder")
            .field("packet", &self.packet)
            .finish()
    }
}

impl PublishBuilder {
    #[inline]
    #[must_use]
    /// Set packet id.
    ///
    /// Note: if packet id is not set, it gets generated automatically.
    /// Packet id management should not be mixed, it should be auto-generated
    /// or set by user. Otherwise collisions could occure.
    ///
    /// # Panics
    ///
    /// Panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        let id = NonZeroU16::new(id).expect("id 0 is not allowed");
        self.packet.packet_id = Some(id);
        self
    }

    #[inline]
    #[must_use]
    /// This might be re-delivery of an earlier attempt to send the Packet.
    pub fn dup(mut self, val: bool) -> Self {
        self.packet.dup = val;
        self
    }

    #[inline]
    #[must_use]
    /// Set retain flag
    pub fn retain(mut self) -> Self {
        self.packet.retain = true;
        self
    }

    #[inline]
    /// Get size of the publish packet
    ///
    /// Size excludes fixed header. It is calculated with the current `QoS` of
    /// the packet, which is `QoS 0` until the packet is sent. Add 2 bytes for
    /// `QoS 1` and `QoS 2` packets.
    pub fn size(&self, payload_size: usize) -> u32 {
        (codec::encode::get_encoded_publish_size(&self.packet) + payload_size) as u32
    }

    /// Send publish packet with `QoS 0`
    ///
    /// Waits while an outgoing streaming publish is in progress, packets cannot
    /// interleave with its payload, and while write backpressure is enabled,
    /// until the write buffer drains to half of the high watermark.
    /// Fails with `SendPacketError::Disconnected` if the connection is closed
    /// or the write timeout of the io expires.
    pub async fn send_at_most_once(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.wait_publish_ready().await {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);
            self.packet.qos = codec::QoS::AtMostOnce;
            self.packet.payload_size = payload.len() as u32;
            self.shared
                .encode_publish(self.packet, Some(payload))
                .map_err(SendPacketError::Encode)
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Start streaming publish packet with `QoS 0`
    ///
    /// `size` is the total payload size, the payload must be sent via returned
    /// `StreamingPayload`. Dropping `StreamingPayload` before the entire payload
    /// is sent terminates the connection.
    ///
    /// Waits while an outgoing streaming publish is in progress, packets cannot
    /// interleave with its payload, and while write backpressure is enabled,
    /// until the write buffer drains to half of the high watermark.
    /// Fails with `SendPacketError::Disconnected` if the connection is closed
    /// or the write timeout of the io expires.
    pub async fn stream_at_most_once(
        mut self,
        size: u32,
    ) -> Result<StreamingPayload, SendPacketError> {
        if self.shared.wait_publish_ready().await {
            log::trace!("Publish (QoS-0) to {:?}", self.packet.topic);

            let stream = StreamingPayload {
                rx: Cell::new(None),
                shared: self.shared.clone(),
                inprocess: Cell::new(true),
            };

            self.packet.qos = QoS::AtMostOnce;
            self.packet.payload_size = size;
            self.shared
                .encode_publish(self.packet, None)
                .map_err(SendPacketError::Encode)
                .map(|()| stream)
        } else {
            log::error!("Mqtt sink is disconnected");
            Err(SendPacketError::Disconnected)
        }
    }

    /// Send publish packet with `QoS 1`
    ///
    /// The ack is read by the connection dispatcher, awaiting it in a handler of
    /// the same connection can stall it while reading is paused, see
    /// [read pause](crate::MqttServiceConfig#read-pause).
    pub async fn send_at_least_once(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = codec::QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            if !self.shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            self.send_at_least_once_inner(payload).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Non-blocking send publish packet with `QoS 1`
    ///
    /// # Panics
    ///
    /// Panics if sink is not ready or publish ack callback is not set
    pub fn send_at_least_once_no_block(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        if self.shared.is_active() {
            // check readiness
            assert!(self.shared.is_ready(), "Mqtt sink is not ready");

            self.packet.qos = codec::QoS::AtLeastOnce;
            self.packet.payload_size = payload.len() as u32;
            let idx = self.shared.set_publish_id(&mut self.packet);

            log::trace!("Publish (QoS1) to {:#?}", self.packet);

            self.shared.wait_publish_response_no_block(
                idx,
                AckType::Publish,
                self.packet,
                Some(payload),
            )
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    async fn send_at_least_once_inner(mut self, payload: Bytes) -> Result<(), SendPacketError> {
        let idx = self.shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        self.shared
            .wait_publish_response(idx, AckType::Publish, self.packet, Some(payload))?
            .await
            .map(|_| ())
            .map_err(|_| SendPacketError::Disconnected)
    }

    /// Send publish packet with `QoS 2`
    ///
    /// If the returned future is dropped after the publish is sent, the publish
    /// is released, `PublishRelease` is sent once `PublishReceived` is received.
    ///
    /// The ack is read by the connection dispatcher, awaiting it in a handler of
    /// the same connection can stall it while reading is paused, see
    /// [read pause](crate::MqttServiceConfig#read-pause).
    pub async fn send_exactly_once(
        mut self,
        payload: Bytes,
    ) -> Result<PublishReceived, SendPacketError> {
        if self.shared.is_active() {
            self.packet.qos = codec::QoS::ExactlyOnce;
            self.packet.payload_size = payload.len() as u32;

            // handle client receive maximum
            if !self.shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            self.send_exactly_once_inner(payload).await
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    async fn send_exactly_once_inner(
        mut self,
        payload: Bytes,
    ) -> Result<PublishReceived, SendPacketError> {
        let idx = self.shared.set_publish_id(&mut self.packet);
        log::trace!("Publish (QoS2) to {:#?}", self.packet);

        let rx =
            self.shared
                .wait_publish_response(idx, AckType::Receive, self.packet, Some(payload))?;
        let guard = ReleaseGuard(idx, &self.shared);
        let result = rx.await;
        std::mem::forget(guard);

        result
            .map(move |_| PublishReceived {
                packet_id: Some(idx),
                shared: self.shared,
            })
            .map_err(|_| SendPacketError::Disconnected)
    }

    /// Send publish packet with `QoS 1`
    pub fn stream_at_least_once(
        mut self,
        size: u32,
    ) -> (
        impl Future<Output = Result<(), SendPacketError>>,
        StreamingPayload,
    ) {
        let (tx, rx) = self.shared.pool.waiters.channel();
        let stream = StreamingPayload {
            rx: Cell::new(Some(rx)),
            shared: self.shared.clone(),
            inprocess: Cell::new(false),
        };

        if self.shared.is_active() {
            self.packet.qos = QoS::AtLeastOnce;
            self.packet.payload_size = size;

            // handle client receive maximum
            let fut = Either::Left(async move {
                if !self.shared.wait_readiness().await {
                    return Err(SendPacketError::Disconnected);
                }
                self.stream_at_least_once_inner(tx).await
            });
            (fut, stream)
        } else {
            (
                Either::Right(async { Err(SendPacketError::Disconnected) }),
                stream,
            )
        }
    }

    async fn stream_at_least_once_inner(
        mut self,
        tx: pool::Sender<()>,
    ) -> Result<(), SendPacketError> {
        // packet id
        let idx = self.shared.set_publish_id(&mut self.packet);

        // send publish to client
        log::trace!("Publish (QoS1) to {:#?}", self.packet);

        if tx.is_canceled() {
            Err(SendPacketError::StreamingCancelled)
        } else {
            let rx = self
                .shared
                .wait_publish_response(idx, AckType::Publish, self.packet, None);
            let _ = tx.send(());

            rx?.await
                .map(|_| ())
                .map_err(|_| SendPacketError::Disconnected)
        }
    }
}

/// `PublishReceived` packet is received for `QoS 2` publish
///
/// Call [`release`](Self::release) to send `PublishRelease` packet and wait for
/// `PublishComplete`. If the value is dropped, `PublishRelease` is sent without
/// waiting for `PublishComplete`.
pub struct PublishReceived {
    packet_id: Option<NonZeroU16>,
    shared: Rc<MqttShared>,
}

impl fmt::Debug for PublishReceived {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishReceived")
            .field("packet_id", &self.packet_id)
            .finish()
    }
}

impl PublishReceived {
    /// Release publish
    pub async fn release(mut self) -> Result<(), SendPacketError> {
        let rx = self
            .shared
            .release_publish(self.packet_id.take().unwrap())?;

        rx.await
            .map(|_| ())
            .map_err(|_| SendPacketError::Disconnected)
    }
}

impl Drop for PublishReceived {
    fn drop(&mut self) {
        if let Some(id) = self.packet_id.take() {
            let _ = self.shared.release_publish(id);
        }
    }
}

/// Releases `QoS 2` publish if the publish future is dropped after `PublishReceived`
/// packet is delivered to it but before the future is polled, PUBREC must be
/// answered with PUBREL (MQTT 3.1.1, 4.3.3). If the PUBREC is not received yet,
/// the dispatcher releases the publish.
struct ReleaseGuard<'a>(NonZeroU16, &'a MqttShared);

impl Drop for ReleaseGuard<'_> {
    fn drop(&mut self) {
        let _ = self.1.release_publish(self.0);
    }
}

/// Subscribe packet builder
pub struct SubscribeBuilder {
    id: Option<NonZeroU16>,
    shared: Rc<MqttShared>,
    topic_filters: Vec<(ByteString, codec::QoS)>,
}

impl fmt::Debug for SubscribeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeBuilder")
            .field("id", &self.id)
            .field("topic_filters", &self.topic_filters)
            .finish()
    }
}

impl SubscribeBuilder {
    #[inline]
    #[must_use]
    /// Set packet id.
    ///
    /// # Panics
    ///
    /// Panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    #[must_use]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString, qos: codec::QoS) -> Self {
        self.topic_filters.push((filter, qos));
        self
    }

    #[inline]
    /// Get size of the subscribe packet, excluding fixed header
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_subscribe_size(&self.topic_filters) as u32
    }

    /// Send subscribe packet
    ///
    /// Only the client sends SUBSCRIBE packets (MQTT 3.1.1, 3.8), a server sink
    /// returns [`SendPacketError::NotAllowed`].
    pub async fn send(self) -> Result<Vec<codec::SubscribeReturnCode>, SendPacketError> {
        if !self.shared.is_client() {
            return Err(SendPacketError::NotAllowed);
        }
        if self.shared.is_active() {
            // handle client receive maximum
            if !self.shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
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
                Ok(()) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(Ack::subscribe)
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

impl fmt::Debug for UnsubscribeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnsubscribeBuilder")
            .field("id", &self.id)
            .field("topic_filters", &self.topic_filters)
            .finish()
    }
}

impl UnsubscribeBuilder {
    #[inline]
    #[must_use]
    /// Set packet id.
    ///
    /// # Panics
    ///
    /// Panics if id is 0
    pub fn packet_id(mut self, id: u16) -> Self {
        if let Some(id) = NonZeroU16::new(id) {
            self.id = Some(id);
            self
        } else {
            panic!("id 0 is not allowed");
        }
    }

    #[inline]
    #[must_use]
    /// Add topic filter
    pub fn topic_filter(mut self, filter: ByteString) -> Self {
        self.topic_filters.push(filter);
        self
    }

    #[inline]
    /// Get size of the unsubscribe packet, excluding fixed header
    pub fn size(&self) -> u32 {
        codec::encode::get_encoded_unsubscribe_size(&self.topic_filters) as u32
    }

    /// Send unsubscribe packet
    ///
    /// Only the client sends UNSUBSCRIBE packets (MQTT 3.1.1, 3.10), a server
    /// sink returns [`SendPacketError::NotAllowed`].
    pub async fn send(self) -> Result<(), SendPacketError> {
        let shared = self.shared;
        let filters = self.topic_filters;

        if !shared.is_client() {
            return Err(SendPacketError::NotAllowed);
        }

        if shared.is_active() {
            // handle client receive maximum
            if !shared.wait_readiness().await {
                return Err(SendPacketError::Disconnected);
            }
            // allocate packet id
            let idx = self.id.unwrap_or_else(|| shared.next_id());
            let rx = shared.wait_response(idx, AckType::Unsubscribe)?;

            // send subscribe to client
            log::trace!("Sending unsubscribe packet id: {idx} filters:{filters:?}");

            match shared.encode_packet(codec::Packet::Unsubscribe {
                packet_id: idx,
                topic_filters: filters,
            }) {
                Ok(()) => {
                    // wait ack from peer
                    rx.await
                        .map_err(|_| SendPacketError::Disconnected)
                        .map(|_| ())
                }
                Err(err) => Err(SendPacketError::Encode(err)),
            }
        } else {
            Err(SendPacketError::Disconnected)
        }
    }
}

pub struct StreamingPayload {
    shared: Rc<MqttShared>,
    rx: Cell<Option<pool::Receiver<()>>>,
    inprocess: Cell<bool>,
}

impl fmt::Debug for StreamingPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingPayload").finish()
    }
}

impl Drop for StreamingPayload {
    fn drop(&mut self) {
        if self.inprocess.get() && self.shared.is_streaming() {
            self.shared.streaming_dropped();
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

        if self.inprocess.get() {
            log::trace!("Sending payload chunk: {:?}", chunk.len());
            self.shared.want_payload_stream().await?;

            if !self.shared.encode_publish_payload(chunk)? {
                self.inprocess.set(false);
            }
            Ok(())
        } else {
            Err(EncodeError::UnexpectedPayload.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use ntex_io::{Io, testing::IoTest};
    use ntex_service::cfg::SharedCfg;

    use super::*;
    use crate::v3::shared::MqttShared;

    #[ntex::test]
    async fn test_debug() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("test"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, true, Rc::default()));
        let sink = MqttSink::new(shared);

        // MqttSink
        assert!(format!("{sink:?}").contains("MqttSink"));

        // PublishBuilder
        let pb = sink.publish("test/topic");
        assert!(format!("{pb:?}").contains("PublishBuilder"));

        // SubscribeBuilder
        let sb = sink.subscribe();
        assert!(format!("{sb:?}").contains("SubscribeBuilder"));

        // UnsubscribeBuilder
        let ub = sink.unsubscribe();
        assert!(format!("{ub:?}").contains("UnsubscribeBuilder"));
    }

    #[ntex::test]
    async fn test_server_subscribe_not_allowed() {
        use std::{future::Future, pin::pin, task::Poll};

        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let sink = MqttSink::new(shared);

        // MQTT 3.1.1, 3.8 and 3.10: SUBSCRIBE and UNSUBSCRIBE are client packets
        let mut f = pin!(
            sink.subscribe()
                .topic_filter("a".into(), codec::QoS::AtLeastOnce)
                .send()
        );
        let res = lazy(|cx| f.as_mut().poll(cx)).await;
        assert_eq!(
            res.map(|r| r.map(|_| ())),
            Poll::Ready(Err(SendPacketError::NotAllowed))
        );
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        let res = lazy(|cx| f.as_mut().poll(cx)).await;
        assert_eq!(res, Poll::Ready(Err(SendPacketError::NotAllowed)));
        assert!(sink.is_open());
        assert!(client.read_any().is_empty());
    }

    #[ntex::test]
    async fn test_ack_type_mismatch() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        use crate::{error::MqttProtocolError, types::packet_type};

        fn setup() -> ((IoTest, Io), Rc<MqttShared>, MqttSink) {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                true,
                Rc::default(),
            ));
            shared.set_cap(16);
            ((client, io), shared.clone(), MqttSink::new(shared))
        }
        async fn is_pending<F: Future>(f: &mut std::pin::Pin<&mut F>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }
        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn err(pkt: u8, expected: &'static str) -> Result<(), MqttProtocolError> {
            Err(MqttProtocolError::unexpected_packet(pkt, expected))
        }

        // QoS 1 PUBLISH acknowledged with PUBREC or PUBCOMP
        for (ack, expected) in [
            (
                Ack::Receive(id(1)),
                err(packet_type::PUBREC, "Expected PUBACK packet"),
            ),
            (
                Ack::Complete(id(1)),
                err(packet_type::PUBCOMP, "Expected PUBACK packet"),
            ),
        ] {
            let (_io, shared, sink) = setup();
            let mut f = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
            assert!(is_pending(&mut f).await);
            assert_eq!(shared.pkt_ack(ack), expected);
            assert!(!sink.is_open());
            assert_eq!(f.await, Err(SendPacketError::Disconnected));
        }

        // SUBSCRIBE acknowledged with PUBREC
        let (_io, shared, sink) = setup();
        let mut f = pin!(
            sink.subscribe()
                .topic_filter("a".into(), QoS::AtMostOnce)
                .send()
        );
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Receive(id(1))),
            err(packet_type::PUBREC, "Expected SUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // UNSUBSCRIBE acknowledged with PUBCOMP
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.unsubscribe().topic_filter("a".into()).send());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Complete(id(1))),
            err(packet_type::PUBCOMP, "Expected UNSUBACK packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // QoS 2 PUBLISH acknowledged with PUBCOMP before PUBREC
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Complete(id(1))),
            err(packet_type::PUBCOMP, "Expected PUBREC packet")
        );
        assert!(matches!(f.await, Err(SendPacketError::Disconnected)));

        // QoS 2 PUBREL acknowledged with a second PUBREC
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(Ack::Receive(id(1))), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(
            shared.pkt_ack(Ack::Receive(id(1))),
            err(packet_type::PUBREC, "Expected PUBCOMP packet")
        );
        assert_eq!(f.await, Err(SendPacketError::Disconnected));

        // valid QoS 2 flow
        let (_io, shared, sink) = setup();
        let mut f = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(Ack::Receive(id(1))), Ok(()));
        let mut f = pin!(f.await.unwrap().release());
        assert!(is_pending(&mut f).await);
        assert_eq!(shared.pkt_ack(Ack::Complete(id(1))), Ok(()));
        assert_eq!(f.await, Ok(()));
        assert!(sink.is_open());
    }

    #[ntex::test]
    async fn test_release_multiple() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        async fn is_pending<F: Future>(f: &mut std::pin::Pin<&mut F>) -> bool {
            lazy(|cx| f.as_mut().poll(cx).is_pending()).await
        }
        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn rec(packet_id: u16) -> Ack {
            Ack::Receive(id(packet_id))
        }
        fn comp(packet_id: u16) -> Ack {
            Ack::Complete(id(packet_id))
        }

        for drop_second in [false, true] {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                true,
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            // two QoS 2 PUBLISH packets are received before release
            let mut f1 = pin!(sink.publish("a").send_exactly_once(Bytes::new()));
            let mut f2 = pin!(sink.publish("b").send_exactly_once(Bytes::new()));
            assert!(is_pending(&mut f1).await);
            assert!(is_pending(&mut f2).await);
            let _ = client.read().await.unwrap();
            assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
            assert_eq!(shared.pkt_ack(rec(2)), Ok(()));
            let rec1 = f1.await.unwrap();
            let rec2 = f2.await.unwrap();

            // each release sends PUBREL with its own packet id
            let mut r1 = pin!(rec1.release());
            assert!(is_pending(&mut r1).await);
            let mut r2 = if drop_second {
                drop(rec2);
                None
            } else {
                let mut r2 = Box::pin(rec2.release());
                assert!(is_pending(&mut r2.as_mut()).await);
                Some(r2)
            };
            let buf = client.read().await.unwrap();
            assert_eq!(buf, Bytes::from_static(b"\x62\x02\x00\x01\x62\x02\x00\x02"));

            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(shared.pkt_ack(comp(2)), Ok(()));
            assert_eq!(r1.await, Ok(()));
            if let Some(r2) = r2.take() {
                assert_eq!(r2.await, Ok(()));
            }
            assert!(sink.is_open());
            assert_eq!(shared.credit(), 16);
        }
    }

    #[ntex::test]
    async fn test_rejected_publish_does_not_start_streaming() {
        let (client, server) = IoTest::create();
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());
        let err = Err(SendPacketError::Encode(
            crate::error::EncodeError::MalformedPacket,
        ));

        let res = sink
            .publish("a/+")
            .stream_at_most_once(10)
            .await
            .map(|_| ());
        assert_eq!(res, err);
        assert!(!shared.is_streaming());

        let (fut, _stream) = sink.publish("a/+").stream_at_least_once(10);
        assert_eq!(fut.await, err);
        assert!(!shared.is_streaming());

        // sink is still usable
        assert!(sink.is_open());
        sink.publish("a/b")
            .send_at_most_once(Bytes::from_static(b"data"))
            .await
            .unwrap();
        drop(client);
    }

    /// `QoS 0` publish waiting for the streaming payload fails once the
    /// connection is closed
    #[ntex::test]
    async fn test_at_most_once_streaming_close() {
        use std::task::Poll;

        use ntex_util::future::lazy;

        for close in 0..3 {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                true,
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            let payload = sink.publish("a").stream_at_most_once(4).await.unwrap();
            let mut send = Box::pin(sink.publish("b").send_at_most_once(Bytes::new()));
            assert!(lazy(|cx| send.as_mut().poll(cx).is_pending()).await);

            match close {
                0 => shared.close(),
                1 => sink.force_close(),
                _ => drop(payload),
            }
            // waiting publish is woken by the close, not by the io shutdown
            assert_eq!(
                lazy(|cx| send.as_mut().poll(cx)).await,
                Poll::Ready(Err(SendPacketError::Disconnected))
            );
        }
    }

    /// `QoS 0` publish waits for write backpressure to be released, and fails
    /// once the connection is closed
    #[ntex::test]
    async fn test_at_most_once_write_backpressure() {
        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        for close in 0..3 {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(0);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                true,
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            sink.publish("a")
                .send_at_most_once(Bytes::from(vec![0u8; 128 * 1024]))
                .await
                .unwrap();
            assert!(lazy(|cx| io.poll_flush(cx, false).is_pending()).await);
            assert!(io.is_wr_backpressure());

            let mut send = Box::pin(sink.publish("b").send_at_most_once(Bytes::new()));
            let mut stream = Box::pin(sink.publish("c").stream_at_most_once(2));
            assert!(lazy(|cx| send.as_mut().poll(cx).is_pending()).await);
            assert!(lazy(|cx| stream.as_mut().poll(cx).is_pending()).await);

            if close == 0 {
                client.remote_buffer_cap(1024 * 1024);
                let _ = client.read().await;
                timeout(Millis(1000), send).await.unwrap().unwrap();
                let payload = timeout(Millis(1000), stream).await.unwrap().unwrap();
                assert!(shared.is_streaming());
                payload.send(Bytes::from_static(b"ab")).await.unwrap();
                assert!(!shared.is_streaming());
            } else {
                if close == 1 {
                    sink.force_close();
                } else {
                    // the write buffer drains during graceful shutdown
                    shared.close();
                    client.remote_buffer_cap(1024 * 1024);
                    let _ = client.read().await;
                }
                assert_eq!(
                    timeout(Millis(1000), send).await.unwrap(),
                    Err(SendPacketError::Disconnected)
                );
                assert!(matches!(
                    timeout(Millis(1000), stream).await.unwrap(),
                    Err(SendPacketError::Disconnected)
                ));
            }
        }
    }

    #[ntex::test]
    async fn test_packets_deferred_while_streaming() {
        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());

        let stream = sink.publish("a/b").stream_at_most_once(6).await.unwrap();
        stream.send(Bytes::from_static(b"ab")).await.unwrap();

        // client keep-alive and dispatcher responses
        assert!(sink.ping());
        let ack = codec::Packet::PublishAck {
            packet_id: NonZeroU16::new(1).unwrap(),
        };
        io.encode(codec::Encoded::Packet(ack), &shared).unwrap();

        // publish cannot interleave with payload, it waits for the payload
        let mut qos0 = Box::pin(sink.publish("c").send_at_most_once(Bytes::new()));
        assert!(lazy(|cx| qos0.as_mut().poll(cx).is_pending()).await);

        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x0b\x00\x03a/bab"));

        // incomplete payload, packets are still deferred
        stream.send(Bytes::from_static(b"cd")).await.unwrap();
        assert!(shared.is_streaming());
        assert!(lazy(|cx| qos0.as_mut().poll(cx).is_pending()).await);
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"cd"));

        stream.send(Bytes::from_static(b"ef")).await.unwrap();
        assert!(!shared.is_streaming());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"ef\xc0\x00\x40\x02\x00\x01"));

        // completed payload releases the waiting publish
        qos0.await.unwrap();
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\x30\x03\x00\x01c"));

        // nothing left behind
        assert!(sink.ping());
        let buf = client.read().await.unwrap();
        assert_eq!(buf, Bytes::from_static(b"\xc0\x00"));
    }

    /// Payload chunk waiting for write backpressure fails when the connection
    /// is closed, backpressure is never disabled after close
    #[ntex::test]
    async fn test_streaming_waiter_fails_on_close() {
        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        for close in 0..2 {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                true,
                Rc::default(),
            ));
            shared.set_cap(16);
            let sink = MqttSink::new(shared.clone());

            let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
            stream.send(Bytes::from_static(b"ab")).await.unwrap();
            shared.enable_wr_backpressure();
            let mut chunk = Box::pin(stream.send(Bytes::from_static(b"cd")));
            assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);

            match close {
                0 => shared.close(),
                _ => sink.force_close(),
            }
            assert_eq!(
                timeout(Millis(1000), chunk).await.unwrap(),
                Err(SendPacketError::Disconnected)
            );
        }
    }

    /// Publish waiting while the streaming payload is released first by
    /// `disable_wr_backpressure` is sent once the payload is complete
    #[ntex::test]
    async fn test_streaming_completion_releases_waiters() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(16);
        let sink = MqttSink::new(shared.clone());

        let stream = sink.publish("a/b").stream_at_most_once(4).await.unwrap();
        stream.send(Bytes::from_static(b"ab")).await.unwrap();
        shared.enable_wr_backpressure();
        let mut chunk = pin!(stream.send(Bytes::from_static(b"cd")));
        assert!(lazy(|cx| chunk.as_mut().poll(cx).is_pending()).await);
        let mut publish = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);

        // the payload goes first, the publish cannot interleave with it
        shared.disable_wr_backpressure();
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 16);
        chunk.await.unwrap();
        assert!(!shared.is_streaming());

        // the completed payload releases the waiting publish
        assert!(lazy(|cx| publish.as_mut().poll(cx).is_pending()).await);
        assert_eq!(sink.credit(), 15);
    }

    /// Dropped `QoS 2` publish future is released, PUBREL is sent and
    /// packet id and in-flight slot are freed (MQTT 3.1.1, 4.3.3)
    #[ntex::test]
    async fn test_dropped_exactly_once() {
        use std::future::Future;

        use ntex_util::future::lazy;
        use ntex_util::time::{Millis, timeout};

        fn id(id: u16) -> NonZeroU16 {
            NonZeroU16::new(id).unwrap()
        }
        fn rec(packet_id: u16) -> Ack {
            Ack::Receive(id(packet_id))
        }
        fn comp(packet_id: u16) -> Ack {
            Ack::Complete(id(packet_id))
        }
        const PUBLISH: &[u8] = b"\x34\x05\x00\x01a\x00\x01";
        const PUBREL: &[u8] = b"\x62\x02\x00\x01";

        // PUBREC received before or after the future is dropped
        for delivered in [false, true] {
            let (client, server) = IoTest::create();
            client.remote_buffer_cap(1024);
            let io = Io::new(server, SharedCfg::new("test"));
            let shared = Rc::new(MqttShared::new(
                io.get_ref(),
                codec::Codec::default(),
                true,
                Rc::default(),
            ));
            shared.set_cap(1);
            let sink = MqttSink::new(shared.clone());

            let mut f = Box::pin(sink.publish("a").send_exactly_once(Bytes::new()));
            assert!(lazy(|cx| f.as_mut().poll(cx).is_pending()).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBLISH)
            );
            if delivered {
                assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
                drop(f);
            } else {
                drop(f);
                assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
            }
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBREL)
            );
            assert_eq!(shared.credit(), 0);
            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(shared.credit(), 1);

            // packet id and in-flight slot are available
            let f = sink
                .publish("a")
                .packet_id(1)
                .send_exactly_once(Bytes::new());
            let mut f = Box::pin(f);
            assert!(lazy(|cx| f.as_mut().poll(cx).is_pending()).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBLISH)
            );
            assert_eq!(shared.pkt_ack(rec(1)), Ok(()));
            let received = timeout(Millis(1000), f).await.unwrap().unwrap();
            let mut r = Box::pin(received.release());
            assert!(lazy(|cx| r.as_mut().poll(cx).is_pending()).await);
            assert_eq!(
                timeout(Millis(1000), client.read()).await.unwrap().unwrap(),
                Bytes::from_static(PUBREL)
            );
            assert_eq!(shared.pkt_ack(comp(1)), Ok(()));
            assert_eq!(timeout(Millis(1000), r).await, Ok(Ok(())));
            assert_eq!(shared.credit(), 1);
        }
    }

    /// Woken waiter checks send credit again, a publish sent without waiting
    /// may take the freed credit first (MQTT 3.1.1, 4.4)
    #[ntex::test]
    async fn test_woken_waiter_rechecks_credit() {
        use std::{future::Future, pin::pin};

        use ntex_util::{future::lazy, time::Millis, time::sleep};

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(1);
        let sink = MqttSink::new(shared.clone());

        let mut a = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| a.as_mut().poll(cx).is_pending()).await);
        let mut b = pin!(sink.publish("b").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        let mut ready = pin!(sink.ready());
        assert!(lazy(|cx| ready.as_mut().poll(cx).is_pending()).await);

        // ack wakes "b", "c" takes the credit before "b" is polled
        assert_eq!(
            shared.pkt_ack(Ack::Publish(NonZeroU16::new(1).unwrap())),
            Ok(())
        );
        assert!(a.await.is_ok());
        let mut c = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| c.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| ready.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        sleep(Millis(50)).await;
        assert_eq!(
            client.read_any(),
            Bytes::from_static(b"\x32\x05\x00\x01a\x00\x01\x32\x05\x00\x01c\x00\x02")
        );

        // "b" is still first in line
        assert_eq!(
            shared.pkt_ack(Ack::Publish(NonZeroU16::new(2).unwrap())),
            Ok(())
        );
        assert!(c.await.is_ok());
        assert!(lazy(|cx| ready.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        assert_eq!(
            shared.pkt_ack(Ack::Publish(NonZeroU16::new(3).unwrap())),
            Ok(())
        );
        assert!(b.await.is_ok());
        assert!(ready.await);
    }

    /// Ack does not release waiters while write backpressure is enabled
    #[ntex::test]
    async fn test_ack_respects_write_backpressure() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(1);
        let sink = MqttSink::new(shared.clone());

        let mut a = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| a.as_mut().poll(cx).is_pending()).await);
        let mut b = pin!(sink.publish("b").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);

        shared.enable_wr_backpressure();
        assert_eq!(
            shared.pkt_ack(Ack::Publish(NonZeroU16::new(1).unwrap())),
            Ok(())
        );
        assert!(a.await.is_ok());
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 1);

        shared.disable_wr_backpressure();
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
    }

    /// `set_cap` wakes waiters within the send credit only
    #[ntex::test]
    async fn test_set_cap_wakes_within_credit() {
        use std::{future::Future, pin::pin};

        use ntex_util::future::lazy;

        let (client, server) = IoTest::create();
        client.remote_buffer_cap(1024);
        let io = Io::new(server, SharedCfg::new("test"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            true,
            Rc::default(),
        ));
        shared.set_cap(1);
        let sink = MqttSink::new(shared.clone());

        let mut a = pin!(sink.publish("a").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| a.as_mut().poll(cx).is_pending()).await);
        let mut b = pin!(sink.publish("b").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        let mut c = pin!(sink.publish("c").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| c.as_mut().poll(cx).is_pending()).await);
        let mut d = pin!(sink.publish("d").send_at_least_once(Bytes::new()));
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);

        // one in flight, cap 3 leaves credit for "b" and "c"
        shared.set_cap(3);
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 2);
        assert!(lazy(|cx| b.as_mut().poll(cx).is_pending()).await);
        assert!(lazy(|cx| c.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);

        // no credit while write backpressure is enabled
        shared.enable_wr_backpressure();
        shared.set_cap(4);
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 1);
        shared.disable_wr_backpressure();
        assert!(lazy(|cx| d.as_mut().poll(cx).is_pending()).await);
        assert_eq!(shared.credit(), 0);
    }
}
