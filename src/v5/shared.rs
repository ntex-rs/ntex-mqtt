#![allow(clippy::type_complexity)]
use std::task::{Context, Poll, Waker};
use std::{cell::Cell, cell::RefCell, collections::VecDeque, fmt, num, pin::Pin, rc::Rc};

use ntex_bytes::{BytePages, Bytes, BytesMut};
use ntex_codec::{Decoder, Encoder};
use ntex_io::IoRef;
use ntex_util::{HashMap, HashSet, channel::pool};

use crate::io::{FrameState, QueueLimit};
use crate::v5::codec::{self, Decoded, Encoded, Packet, Publish};
use crate::{QoS, error, error::SendPacketError, payload::PlSender, types::packet_type};

bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct Flags: u16 {
        const WRB_ENABLED     = 0b0000_0001; // write-backpressure
        const ON_PUBLISH_ACK  = 0b0000_0010; // on-publish-ack callback

        const QOS_ATLEAST     = 0b0000_0100; // AtLeastOnce
        const QOS_EXACTLY     = 0b0000_1000; // ExactlyOnce

        const ZERO_SES_EXPIRY = 0b0001_0000; // Session expiry is zero in Connect

        const DISCONNECT      = 0b0010_0000; // Disconnect frame is sent
        const DISCONNECT_RECV = 0b0100_0000; // Disconnect frame is received
        const STOPPED         = 0b1000_0000; // DispatchItem::Stop() is sent
        const CLIENT          = 0b1_0000_0000; // Client side of the connection
    }
}

pub struct MqttShared {
    io: IoRef,
    cap: Cell<usize>,
    receive_max: Cell<u16>,
    topic_alias_max: Cell<u16>,
    inflight_idx: Cell<u16>,
    queues: RefCell<MqttSharedQueues>,
    encode_error: Cell<Option<error::EncodeError>>,
    streaming_waiter: Cell<Option<pool::Sender<()>>>,
    streaming_remaining: Cell<Option<num::NonZeroU32>>,
    /// Packets encoded while a publish payload is incomplete
    deferred: Cell<Option<BytePages>>,
    on_publish_ack: Cell<Option<Box<dyn Fn(codec::PublishAck, bool)>>>,
    pub(super) payload: Cell<Option<PlSender>>,
    pub(super) flags: Cell<Flags>,
    pub(super) pool: Rc<MqttSinkPool>,
    pub(super) codec: codec::Codec,
}

impl fmt::Debug for MqttShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttShared").finish()
    }
}

#[derive(Debug)]
pub(super) struct MqttSharedQueues {
    inflight: VecDeque<(num::NonZeroU16, Option<pool::Sender<Ack>>, AckType)>,
    inflight_ids: HashSet<num::NonZeroU16>,
    // SUBSCRIBE and UNSUBSCRIBE packets in `inflight`
    subscribes: usize,
    // publishes that wait for send credit, in the order of arrival
    waiters: VecDeque<Waiter>,
    // send credit granted to waiters and held by permits, not in `inflight` yet
    reserved: usize,
    next_ticket: u64,
    // PUBCOMP receivers, one per QoS 2 PUBLISH awaiting release
    rx: HashMap<num::NonZeroU16, pool::Receiver<Ack>>,
}

impl MqttSharedQueues {
    /// In-flight `QoS 1` and `QoS 2` PUBLISH packets, Receive Maximum counts
    /// PUBLISH packets only [MQTT-4.9.0-2]
    fn publishes(&self) -> usize {
        self.inflight.len() - self.subscribes
    }

    /// Check if a publish waits for send credit
    ///
    /// Waiting entries follow the granted and closed ones.
    fn has_waiting(&self) -> bool {
        self.waiters
            .back()
            .is_some_and(|w| w.state == WaiterState::Waiting)
    }

    /// Position of the queued waiter, tickets of the queued waiters are ascending
    fn find(&self, ticket: u64) -> usize {
        self.waiters
            .binary_search_by_key(&ticket, |w| w.ticket)
            .expect("waiter is queued")
    }
}

#[derive(Debug)]
struct Waiter {
    ticket: u64,
    state: WaiterState,
    waker: Option<Waker>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum WaiterState {
    Waiting,
    /// Send credit is reserved for the waiter
    Granted,
    /// Queues are cleared, the connection is closed
    Closed,
}

pub(super) struct MqttSinkPool {
    queue: pool::Pool<Ack>,
    pub(super) waiters: pool::Pool<()>,
}

impl Default for MqttSinkPool {
    fn default() -> Self {
        Self {
            queue: pool::new(),
            waiters: pool::new(),
        }
    }
}

impl MqttShared {
    pub(super) fn new(io: IoRef, codec: codec::Codec, pool: Rc<MqttSinkPool>) -> Self {
        Self {
            io,
            pool,
            codec,
            cap: Cell::new(0),
            queues: RefCell::new(MqttSharedQueues {
                inflight: VecDeque::with_capacity(8),
                inflight_ids: HashSet::default(),
                subscribes: 0,
                waiters: VecDeque::new(),
                reserved: 0,
                next_ticket: 0,
                rx: HashMap::default(),
            }),
            receive_max: Cell::new(0),
            topic_alias_max: Cell::new(0),
            inflight_idx: Cell::new(0),
            flags: Cell::new(Flags::QOS_ATLEAST),
            payload: Cell::new(None),
            on_publish_ack: Cell::new(None),
            encode_error: Cell::new(None),
            streaming_waiter: Cell::new(None),
            streaming_remaining: Cell::new(None),
            deferred: Cell::new(None),
        }
    }

    pub(super) fn tag(&self) -> &'static str {
        self.io.tag()
    }

    /// Send credit that is not in use or reserved, Receive Maximum [MQTT-4.9.0-1]
    pub(super) fn credit(&self) -> usize {
        let queues = self.queues.borrow();
        self.cap
            .get()
            .saturating_sub(queues.publishes() + queues.reserved)
    }

    pub(super) fn receive_max(&self) -> u16 {
        self.receive_max.get()
    }

    pub(super) fn topic_alias_max(&self) -> u16 {
        self.topic_alias_max.get()
    }

    pub(super) fn max_qos(&self) -> QoS {
        let flags = self.flags.get();
        if flags.contains(Flags::QOS_ATLEAST) {
            QoS::AtLeastOnce
        } else if flags.contains(Flags::QOS_EXACTLY) {
            QoS::ExactlyOnce
        } else {
            QoS::AtMostOnce
        }
    }

    pub(super) fn set_receive_max(&self, val: u16) {
        self.receive_max.set(val);
    }

    pub(super) fn set_topic_alias_max(&self, val: u16) {
        self.topic_alias_max.set(val);
    }

    pub(super) fn set_max_qos(&self, val: QoS) {
        let mut flags = self.flags.get();
        match val {
            QoS::AtLeastOnce => {
                flags.insert(Flags::QOS_ATLEAST);
                flags.remove(Flags::QOS_EXACTLY);
            }
            QoS::ExactlyOnce => {
                flags.insert(Flags::QOS_EXACTLY);
                flags.remove(Flags::QOS_ATLEAST);
            }
            QoS::AtMostOnce => {
                flags.remove(Flags::QOS_ATLEAST);
                flags.remove(Flags::QOS_EXACTLY);
            }
        }
        self.flags.set(flags);
    }

    pub(super) fn is_zero_session_expiry(&self) -> bool {
        self.flags.get().contains(Flags::ZERO_SES_EXPIRY)
    }

    pub(super) fn is_client(&self) -> bool {
        self.flags.get().contains(Flags::CLIENT)
    }

    pub(super) fn set_client(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::CLIENT);
        self.flags.set(flags);
    }

    pub(super) fn set_zero_session_expiry(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ZERO_SES_EXPIRY);
        self.flags.set(flags);
    }

    pub(super) fn close(&self, pkt: Option<codec::Disconnect>) {
        if self.is_active() {
            if let Some(pkt) = pkt
                && !self.is_disconnect_sent()
            {
                let _ = self
                    .io
                    .encode(Encoded::Packet(Packet::Disconnect(pkt)), self);
            }
            self.io.close();
        }
        self.clear_queues();
    }

    pub(super) fn force_close(&self) {
        self.io.terminate();
        self.clear_queues();
    }

    pub(super) fn streaming_dropped(&self) {
        self.force_close();
        self.encode_error
            .set(Some(error::EncodeError::PublishIncomplete));
    }

    pub(super) fn is_active(&self) -> bool {
        self.io.is_active()
    }

    pub(super) fn is_streaming(&self) -> bool {
        self.streaming_remaining.get().is_some()
    }

    /// Check if a `QoS 1` or `QoS 2` publish can be sent without waiting
    ///
    /// Waiting publishes get send credit first.
    pub(super) fn is_ready(&self) -> bool {
        self.credit() > 0
            && !self.flags.get().contains(Flags::WRB_ENABLED)
            && !self.queues.borrow().has_waiting()
    }

    pub(super) fn is_disconnect_sent(&self) -> bool {
        let mut flags = self.flags.get();
        let disconnect = flags.contains(Flags::DISCONNECT);
        if !disconnect {
            flags.insert(Flags::DISCONNECT);
            self.flags.set(flags);
        }
        disconnect
    }

    pub(super) fn set_disconnect_recv(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::DISCONNECT_RECV);
        self.flags.set(flags);
    }

    pub(super) fn is_disconnect_recv(&self) -> bool {
        self.flags.get().contains(Flags::DISCONNECT_RECV)
    }

    /// publish packet id
    pub(super) fn set_publish_id(&self, pkt: &mut Publish) -> num::NonZeroU16 {
        if let Some(idx) = pkt.packet_id {
            idx
        } else {
            let idx = self.next_id();
            pkt.packet_id = Some(idx);
            idx
        }
    }

    pub(super) fn next_id(&self) -> num::NonZeroU16 {
        let idx = self.inflight_idx.get() + 1;
        self.inflight_idx.set(idx);
        let idx = if idx == u16::MAX {
            self.inflight_idx.set(0);
            u16::MAX
        } else {
            self.inflight_idx.set(idx);
            idx
        };
        num::NonZeroU16::new(idx).unwrap()
    }

    pub(super) fn set_cap(&self, cap: usize) {
        self.cap.set(cap);
        self.grant(&mut self.queues.borrow_mut());
    }

    pub(super) fn set_publish_ack(&self, f: Box<dyn Fn(codec::PublishAck, bool)>) {
        let mut flags = self.flags.get();
        flags.insert(Flags::ON_PUBLISH_ACK);
        self.flags.set(flags);
        self.on_publish_ack.set(Some(f));
    }

    /// Close mqtt connection, dont send disconnect message
    pub(super) fn drop_sink(&self, io: bool) {
        self.clear_queues();
        if io {
            self.io.close();
        }
    }

    pub(super) fn drop_payload<E>(&self, err: &E)
    where
        E: Clone,
        error::PayloadError: From<E>,
    {
        if let Some(pl) = self.payload.take() {
            pl.set_error(err.clone().into());
        }
    }

    fn clear_queues(&self) {
        // the payload waiting for write backpressure fails, the connection
        // is closed and backpressure would never be disabled
        self.streaming_waiter.take();

        let mut queues = self.queues.borrow_mut();
        queues.subscribes = 0;
        let MqttSharedQueues {
            waiters, reserved, ..
        } = &mut *queues;
        for waiter in waiters {
            if waiter.state == WaiterState::Granted {
                *reserved -= 1;
            }
            waiter.state = WaiterState::Closed;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }

        if let Some(cb) = self.on_publish_ack.take() {
            for (idx, tx, _) in queues.inflight.drain(..) {
                if tx.is_none() {
                    (*cb)(
                        codec::PublishAck {
                            packet_id: idx,
                            ..Default::default()
                        },
                        true,
                    );
                }
            }
        } else {
            queues.inflight.clear();
        }
    }

    pub(super) fn enable_wr_backpressure(&self) {
        let mut flags = self.flags.get();
        flags.insert(Flags::WRB_ENABLED);
        self.flags.set(flags);
    }

    pub(super) fn disable_wr_backpressure(&self) {
        let mut flags = self.flags.get();
        flags.remove(Flags::WRB_ENABLED);
        self.flags.set(flags);

        // streaming payload goes first, waiting publishes cannot be written
        // until the payload is complete, `encode_publish_payload` grants them
        if let Some(tx) = self.streaming_waiter.take()
            && tx.send(()).is_ok()
        {
            return;
        }

        self.grant(&mut self.queues.borrow_mut());
    }

    /// Reserve send credit for the waiting publishes in the order of arrival
    ///
    /// A granted waiter keeps the credit until its publish is in flight, so that
    /// the number of in-flight publishes stays within the peer's Receive Maximum
    /// [MQTT-4.9.0-1].
    fn grant(&self, queues: &mut MqttSharedQueues) {
        if self.flags.get().contains(Flags::WRB_ENABLED) {
            return;
        }
        let publishes = queues.publishes();
        let MqttSharedQueues {
            waiters, reserved, ..
        } = queues;
        for waiter in waiters.iter_mut() {
            if waiter.state != WaiterState::Waiting {
                continue;
            }
            if publishes + *reserved >= self.cap.get() {
                break;
            }
            waiter.state = WaiterState::Granted;
            *reserved += 1;
            if let Some(waker) = waiter.waker.take() {
                waker.wake();
            }
        }
    }

    /// Wait for send credit of a `QoS 1` or `QoS 2` publish
    ///
    /// Publishes get the credit in the order of the calls, the waiter is queued
    /// immediately, not on the first poll.
    pub(super) fn send_permit(self: &Rc<Self>) -> WaitSendPermit {
        if self.is_ready() {
            self.queues.borrow_mut().reserved += 1;
            return WaitSendPermit {
                shared: self.clone(),
                ticket: None,
                permit: Some(SendPermit(self.clone())),
            };
        }

        let mut queues = self.queues.borrow_mut();
        let ticket = queues.next_ticket;
        queues.next_ticket += 1;
        queues.waiters.push_back(Waiter {
            ticket,
            state: WaiterState::Waiting,
            waker: None,
        });
        WaitSendPermit {
            shared: self.clone(),
            ticket: Some(ticket),
            permit: None,
        }
    }

    pub(super) async fn want_payload_stream(&self) -> Result<(), SendPacketError> {
        if !self.is_active() {
            Err(SendPacketError::Disconnected)
        } else if self.flags.get().contains(Flags::WRB_ENABLED) {
            let (tx, rx) = self.pool.waiters.channel();
            self.streaming_waiter.set(Some(tx));
            if rx.await.is_ok() {
                Ok(())
            } else {
                Err(SendPacketError::Disconnected)
            }
        } else {
            Ok(())
        }
    }

    fn check_streaming(&self) -> Result<(), error::EncodeError> {
        if self.streaming_remaining.get().is_some() {
            Err(error::EncodeError::ExpectPayload)
        } else {
            Ok(())
        }
    }

    /// Remaining streaming payload size, it is applied only after the
    /// publish packet is encoded successfully
    fn streaming_size(pkt: &Publish, payload: Option<&Bytes>) -> Option<num::NonZeroU32> {
        let len = payload.map_or(0, Bytes::len);
        num::NonZeroU32::new(pkt.payload_size - len as u32)
    }

    /// Encodes a packet, it is written after the payload of a streaming publish
    pub(super) fn encode_packet(&self, pkt: codec::Packet) -> Result<(), error::EncodeError> {
        self.io.encode(Encoded::Packet(pkt), self)
    }

    pub(super) fn encode_publish(
        &self,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), error::EncodeError> {
        self.check_streaming()?;
        let remaining = Self::streaming_size(&pkt, payload.as_ref());
        self.io
            .encode(Encoded::Publish(pkt, payload), &self.codec)?;
        self.streaming_remaining.set(remaining);
        Ok(())
    }

    pub(super) fn encode_publish_payload(
        &self,
        payload: Bytes,
    ) -> Result<bool, error::EncodeError> {
        if let Some(remaining) = self.streaming_remaining.get() {
            let len = payload.len() as u32;
            if len > remaining.get() {
                self.force_close();
                Err(error::EncodeError::OverPublishSize)
            } else {
                self.io.encode(Encoded::PayloadChunk(payload), self)?;
                let remaining = num::NonZeroU32::new(remaining.get() - len);
                self.streaming_remaining.set(remaining);
                if remaining.is_none() {
                    // publishes waiting while the streaming payload was released
                    // first by `disable_wr_backpressure` get their send credit
                    self.grant(&mut self.queues.borrow_mut());
                }
                Ok(remaining.is_some())
            }
        } else {
            Err(error::EncodeError::UnexpectedPayload)
        }
    }

    pub(super) fn pkt_ack(&self, ack: Ack) -> Result<(), error::MqttProtocolError> {
        // invalid ack is a protocol error, DISCONNECT uses its reason code,
        // 0x82 (Protocol Error) for unexpected or out of order acks (MQTT 5.0, 4.13.1)
        self.pkt_ack_inner(ack).inspect_err(|e| {
            self.close(Some(codec::Disconnect::from_proto_error(e)));
        })
    }

    fn pkt_ack_inner(&self, pkt: Ack) -> Result<(), error::MqttProtocolError> {
        let mut queues = self.queues.borrow_mut();

        // check ack order
        if let Some((idx, tx, tp)) = queues.inflight.pop_front() {
            if matches!(tp, AckType::Subscribe | AckType::Unsubscribe) {
                queues.subscribes -= 1;
            }
            if idx != pkt.packet_id() {
                log::trace!(
                    "MQTT protocol error, packet_id order does not match, expected {}, got: {}",
                    idx,
                    pkt.packet_id()
                );
                Err(error::MqttProtocolError::packet_id_mismatch())
            } else if !pkt.is_match(tp) {
                // ack type must match the in-flight packet, PUBREC acknowledges only
                // a QoS 2 PUBLISH and PUBCOMP only a PUBREL (MQTT 5.0, 4.3.2, 4.3.3)
                log::trace!(
                    "MQTT protocol error, unexpected packet {}, {}",
                    pkt.packet_type(),
                    tp.expected_str()
                );
                Err(error::MqttProtocolError::unexpected_packet(
                    pkt.packet_type(),
                    tp.expected_str(),
                ))
            } else if let Ack::Receive(ref ack) = pkt {
                // get publish ack channel
                log::trace!("Ack packet receive with id: {}", pkt.packet_id());

                if u8::from(ack.reason_code) < 0x80 {
                    if tx.is_none_or(|tx| tx.send(pkt).is_err()) {
                        // the publish future is dropped, nothing can release the publish,
                        // PUBREL must be sent for a PUBREC below 0x80 [MQTT-4.3.3-4]
                        log::trace!("Release dropped publish with id: {idx}");
                        let _ = self.io.encode(
                            Encoded::Packet(Packet::PublishRelease(codec::PublishAck2 {
                                packet_id: idx,
                                ..Default::default()
                            })),
                            self,
                        );
                        queues.inflight.push_back((idx, None, AckType::Complete));
                    } else {
                        let (tx, rx) = self.pool.queue.channel();
                        queues.rx.insert(idx, rx);
                        queues
                            .inflight
                            .push_back((idx, Some(tx), AckType::Complete));
                    }
                } else {
                    // PUBREL is sent only for a PUBREC with a reason code below 0x80
                    // [MQTT-4.3.3-4], a failed PUBREC ends the QoS 2 flow and the packet id
                    // is available for reuse (MQTT 5.0, 4.3.3)
                    queues.inflight_ids.remove(&idx);

                    if let Some(tx) = tx {
                        let _ = tx.send(pkt);
                    }

                    // wake up queued request (receive max limit)
                    self.grant(&mut queues);
                }
                Ok(())
            } else if matches!(pkt, Ack::Complete(_)) {
                // get publish ack channel
                log::trace!("Ack packet complete with id: {}", pkt.packet_id());
                queues.inflight_ids.remove(&pkt.packet_id());
                queues.rx.remove(&idx);

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                }

                // wake up queued request (receive max limit)
                self.grant(&mut queues);
                Ok(())
            } else {
                // get publish ack channel
                log::trace!("Ack packet with id: {}", pkt.packet_id());

                // cleanup ack queue
                queues.inflight_ids.remove(&pkt.packet_id());

                if let Some(tx) = tx {
                    let _ = tx.send(pkt);
                } else {
                    let cb = self.on_publish_ack.take().unwrap();
                    (*cb)(pkt.publish(), false);
                    self.on_publish_ack.set(Some(cb));
                }

                // wake up queued request (receive max limit)
                self.grant(&mut queues);
                Ok(())
            }
        } else {
            log::trace!("Unexpected PublishAck packet");
            Err(error::MqttProtocolError::generic_violation(
                "Received PUBACK packet while there are no unacknowledged PUBLISH packets",
            ))
        }
    }

    /// Register ack in response channel
    pub(super) fn wait_response(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            let (tx, rx) = self.pool.queue.channel();
            queues.inflight.push_back((id, Some(tx), ack));
            queues.inflight_ids.insert(id);
            if matches!(ack, AckType::Subscribe | AckType::Unsubscribe) {
                queues.subscribes += 1;
            }
            Ok(rx)
        }
    }

    /// Register ack in response channel
    pub(super) fn wait_publish_response(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        self.check_streaming()?;
        let remaining = Self::streaming_size(&pkt, payload.as_ref());

        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(()) => {
                    self.streaming_remaining.set(remaining);
                    let (tx, rx) = self.pool.queue.channel();
                    queues.inflight.push_back((id, Some(tx), ack));
                    queues.inflight_ids.insert(id);
                    Ok(rx)
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    pub(super) fn wait_publish_response_no_block(
        &self,
        id: num::NonZeroU16,
        ack: AckType,
        pkt: Publish,
        payload: Option<Bytes>,
    ) -> Result<(), SendPacketError> {
        self.check_streaming()?;
        let remaining = Self::streaming_size(&pkt, payload.as_ref());

        let mut queues = self.queues.borrow_mut();
        if queues.inflight_ids.contains(&id) {
            Err(SendPacketError::PacketIdInUse(id))
        } else {
            match self.io.encode(Encoded::Publish(pkt, payload), &self.codec) {
                Ok(()) => {
                    self.streaming_remaining.set(remaining);
                    queues.inflight.push_back((id, None, ack));
                    queues.inflight_ids.insert(id);
                    Ok(())
                }
                Err(e) => Err(SendPacketError::Encode(e)),
            }
        }
    }

    /// Wait for write backpressure only
    ///
    /// Receive Maximum limits `QoS 1` and `QoS 2` PUBLISH packets only
    /// [MQTT-4.9.0-2], SUBSCRIBE and UNSUBSCRIBE do not wait for send credit.
    /// The io write task wakes waiters directly, a write timeout is reported
    /// as disconnect. Backpressure can be released while the io is closing,
    /// a packet registered after `clear_queues` would never be acked.
    pub(super) async fn wait_wr_readiness(&self) -> Result<(), SendPacketError> {
        if self.io.write_ready().await.is_ok() && self.is_active() {
            Ok(())
        } else {
            Err(SendPacketError::Disconnected)
        }
    }

    /// Register ack in response channel
    pub(super) fn release_publish(
        &self,
        pkt: codec::PublishAck2,
    ) -> Result<pool::Receiver<Ack>, SendPacketError> {
        let Some(rx) = self.queues.borrow_mut().rx.remove(&pkt.packet_id) else {
            return Err(SendPacketError::UnexpectedRelease);
        };

        match self
            .io
            .encode(Encoded::Packet(codec::Packet::PublishRelease(pkt)), self)
        {
            Ok(()) => Ok(rx),
            Err(e) => Err(SendPacketError::Encode(e)),
        }
    }
}

/// Send credit of a `QoS 1` or `QoS 2` publish, Receive Maximum [MQTT-4.9.0-1]
///
/// The credit is released on drop, the publish holds the credit once it is
/// registered as in flight.
pub(super) struct SendPermit(Rc<MqttShared>);

impl Drop for SendPermit {
    fn drop(&mut self) {
        let mut queues = self.0.queues.borrow_mut();
        queues.reserved -= 1;
        self.0.grant(&mut queues);
    }
}

/// Future of [`MqttShared::send_permit`]
///
/// Fails if the connection is closed while the publish waits. A dropped waiter
/// passes granted credit to the next one.
pub(super) struct WaitSendPermit {
    shared: Rc<MqttShared>,
    ticket: Option<u64>,
    permit: Option<SendPermit>,
}

impl Future for WaitSendPermit {
    type Output = Result<SendPermit, SendPacketError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(permit) = this.permit.take() {
            return Poll::Ready(Ok(permit));
        }
        let Some(ticket) = this.ticket else {
            return Poll::Ready(Err(SendPacketError::Disconnected));
        };

        let mut queues = this.shared.queues.borrow_mut();
        let idx = queues.find(ticket);
        let waiter = &mut queues.waiters[idx];
        let state = waiter.state;
        if state == WaiterState::Waiting {
            match waiter.waker {
                Some(ref mut waker) => waker.clone_from(cx.waker()),
                None => waiter.waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }
        queues.waiters.remove(idx);
        this.ticket = None;

        if state == WaiterState::Granted {
            Poll::Ready(Ok(SendPermit(this.shared.clone())))
        } else {
            Poll::Ready(Err(SendPacketError::Disconnected))
        }
    }
}

impl Drop for WaitSendPermit {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            let mut queues = self.shared.queues.borrow_mut();
            let idx = queues.find(ticket);
            if queues.waiters.remove(idx).map(|w| w.state) == Some(WaiterState::Granted) {
                queues.reserved -= 1;
                self.shared.grant(&mut queues);
            }
        }
    }
}

impl Encoder for MqttShared {
    type Item = Encoded;
    type Error = error::EncodeError;

    fn encode(&self, item: Self::Item, dst: &mut BytePages) -> Result<(), Self::Error> {
        match item {
            // packets cannot be written in the middle of a publish payload,
            // they are written after the payload is complete
            Encoded::Packet(pkt) if self.codec.is_encoding_payload() => {
                let mut buf = self
                    .deferred
                    .take()
                    .unwrap_or_else(|| BytePages::new(self.io.cfg().write_page_size()));
                let res = self.codec.encode_packet(pkt, &mut buf);
                self.deferred.set(Some(buf));
                res
            }
            Encoded::PayloadChunk(_) => {
                self.codec.encode(item, dst)?;
                if !self.codec.is_encoding_payload()
                    && let Some(mut buf) = self.deferred.take()
                {
                    buf.move_to(dst);
                }
                Ok(())
            }
            _ => self.codec.encode(item, dst),
        }
    }
}

impl FrameState for MqttShared {
    #[inline]
    fn is_partial(&self) -> bool {
        self.codec.is_payload_pending()
    }

    #[inline]
    fn is_ordered(&self, item: &Decoded) -> bool {
        // mqtt orders acks of publish packets only
        // payload chunks have no response, they do not keep a queue slot
        // while the publish handler is pending
        match item {
            Decoded::Publish(publish, ..) => publish.qos != QoS::AtMostOnce,
            Decoded::Packet(pkt, _) => !matches!(
                pkt,
                Packet::PingRequest
                    | Packet::Subscribe(_)
                    | Packet::Unsubscribe(_)
                    | Packet::PublishRelease(_)
            ),
            Decoded::PayloadChunk(..) => false,
        }
    }

    #[inline]
    fn queue_limit(&self, item: &Decoded) -> QueueLimit {
        match item {
            // publish handlers can wait for acks of outgoing packets, acks and
            // pings are dispatched while publishes wait for the response queue.
            // PUBREL follows the PUBREC of an already handled publish [MQTT-4.3.3-4]
            Decoded::Packet(
                Packet::PublishAck(_)
                | Packet::PublishReceived(_)
                | Packet::PublishRelease(_)
                | Packet::PublishComplete(_)
                | Packet::SubscribeAck(_)
                | Packet::UnsubscribeAck(_)
                | Packet::PingRequest
                | Packet::PingResponse,
                _,
            ) => QueueLimit::Bypass,
            // Receive Maximum bounds in-flight QoS 1 and QoS 2 publishes, more
            // is a protocol error [MQTT-3.3.4-9]
            Decoded::Publish(publish, ..)
                if publish.qos != QoS::AtMostOnce && self.receive_max() != 0 =>
            {
                QueueLimit::Bounded
            }
            _ => QueueLimit::Hold,
        }
    }

    #[inline]
    fn bounded_slots(&self) -> usize {
        self.receive_max() as usize
    }
}

impl Decoder for MqttShared {
    type Item = Decoded;
    type Error = error::DecodeError;

    #[inline]
    fn decode(&self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.codec.decode(src)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum AckType {
    Publish,
    Receive,
    Complete,
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Ack {
    Publish(codec::PublishAck),
    Receive(codec::PublishAck),
    Complete(codec::PublishAck2),
    Subscribe(codec::SubscribeAck),
    Unsubscribe(codec::UnsubscribeAck),
}

impl Ack {
    pub(super) fn packet_type(&self) -> u8 {
        match self {
            Ack::Publish(_) => packet_type::PUBACK,
            Ack::Receive(_) => packet_type::PUBREC,
            Ack::Complete(_) => packet_type::PUBCOMP,
            Ack::Subscribe(_) => packet_type::SUBACK,
            Ack::Unsubscribe(_) => packet_type::UNSUBACK,
        }
    }

    pub(super) fn packet_id(&self) -> num::NonZeroU16 {
        match self {
            Ack::Publish(pkt) | Ack::Receive(pkt) => pkt.packet_id,
            Ack::Complete(pkt) => pkt.packet_id,
            Ack::Subscribe(pkt) => pkt.packet_id,
            Ack::Unsubscribe(pkt) => pkt.packet_id,
        }
    }

    pub(super) fn publish(self) -> codec::PublishAck {
        if let Ack::Publish(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn receive(self) -> codec::PublishAck {
        if let Ack::Receive(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn subscribe(self) -> codec::SubscribeAck {
        if let Ack::Subscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn unsubscribe(self) -> codec::UnsubscribeAck {
        if let Ack::Unsubscribe(pkt) = self {
            pkt
        } else {
            panic!()
        }
    }

    pub(super) fn is_match(&self, tp: AckType) -> bool {
        match (self, tp) {
            (Ack::Publish(_), AckType::Publish)
            | (Ack::Receive(_), AckType::Receive)
            | (Ack::Complete(_), AckType::Complete)
            | (Ack::Subscribe(_), AckType::Subscribe)
            | (Ack::Unsubscribe(_), AckType::Unsubscribe) => true,
            (_, _) => false,
        }
    }
}

impl AckType {
    pub(super) fn expected_str(self) -> &'static str {
        match self {
            AckType::Publish => "Expected PUBACK packet",
            AckType::Receive => "Expected PUBREC packet",
            AckType::Complete => "Expected PUBCOMP packet",
            AckType::Subscribe => "Expected SUBACK packet",
            AckType::Unsubscribe => "Expected UNSUBACK packet",
        }
    }
}
