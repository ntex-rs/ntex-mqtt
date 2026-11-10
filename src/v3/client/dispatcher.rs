use std::{cell::Cell, cell::RefCell, marker::PhantomData, num::NonZeroU16, rc::Rc};

use ntex_service::{BindCtx, Ctx, Service};
use ntex_util::HashMap;
use ntex_util::future::{Either, join};

use crate::error::{DecodeError, DispatcherError, MqttProtocolError, PayloadError, SpecViolation};
use crate::inflight::InFlightServiceImpl;
use crate::payload::{Payload, PayloadSource, PayloadStatus};
use crate::types::packet_type;
use crate::v3::codec::{self, Decoded, Packet};
use crate::v3::shared::{Ack, MqttShared};
use crate::v3::{QoS, Session, control::ProtocolMessageKind, publish::Publish};

use super::control::{ProtocolMessage, ProtocolMessageAck};

/// Build a client packet dispatcher with an in-flight publish-call limit.
pub(super) fn create_dispatcher<St, T, C, E>(
    sink: Rc<MqttShared>,
    inflight: u16,
    max_buffer_size: usize,
    publish: T,
    control: C,
) -> impl Service<Session<St>, Decoded, Res = Option<Packet>, Error = DispatcherError<E>>
where
    St: 'static,
    E: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E> + 'static,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = E> + 'static,
{
    // limit number of in-flight publish messages
    InFlightServiceImpl::new(
        inflight,
        0,
        Dispatcher::new(
            sink,
            publish,
            control.map_err(DispatcherError::Service),
            max_buffer_size,
        ),
    )
}

/// Dispatch decoded client packets and maintain incoming publish exchange state.
pub(crate) struct Dispatcher<St, T, C, E> {
    /// Service invoked before falling back to the protocol-message service.
    publish: T,
    /// State shared by publish and protocol-message processing.
    inner: Inner<C>,
    /// Streamed payload buffer high watermark in bytes.
    max_buffer_size: usize,
    /// Session state and error types carried by the service implementation.
    st: PhantomData<(St, E)>,
}

/// Protocol service and connection state shared across concurrent publish calls.
struct Inner<C> {
    /// Service handling unrouted publications and other protocol messages.
    control: C,
    /// Connection state used for acknowledgements, payloads, and shutdown.
    sink: Rc<MqttShared>,
    /// Source feeding chunks to the current incoming streamed payload.
    payload: PayloadSource,
    /// Set for an ignored streamed publication and cleared at its final chunk.
    discard_payload: Cell<bool>,
    /// Incoming packet identifiers retained until their acknowledgement exchange completes.
    inflight: RefCell<HashMap<NonZeroU16, InFlight>>,
}

/// State of an incoming publish packet id
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InFlight {
    /// Publish is being handled
    Publish(QoS),
    /// `QoS 2` publish is handled and `PublishReceived` is sent, waiting for `PublishRelease`
    Received,
}

impl<St, T, C, E> Dispatcher<St, T, C, E>
where
    St: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E>,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    E: 'static,
{
    /// Initialize services and empty incoming identifier and payload state.
    pub(crate) fn new(
        sink: Rc<MqttShared>,
        publish: T,
        control: C,
        max_buffer_size: usize,
    ) -> Self {
        Self {
            publish,
            max_buffer_size,
            inner: Inner {
                sink,
                control,
                payload: PayloadSource::default(),
                discard_payload: Cell::new(false),
                inflight: RefCell::new(HashMap::default()),
            },
            st: PhantomData,
        }
    }
}

impl<C> Inner<C> {
    /// Fail the current payload receiver and release its sending side.
    fn drop_payload<PErr>(&self, err: &PErr)
    where
        PErr: Clone,
        PayloadError: From<PErr>,
    {
        self.payload.set_error(err.clone().into());
    }

    /// Acknowledge handled publish packet
    ///
    /// `QoS 1` publish is acknowledged with `PublishAck` [MQTT-4.3.2-2], `QoS 2` publish
    /// with `PublishReceived`, the packet id stays in use until `PublishRelease` [MQTT-4.3.3-2]
    fn publish_ack(&self, packet_id: NonZeroU16) -> Packet {
        let mut inflight = self.inflight.borrow_mut();
        if inflight.get(&packet_id) == Some(&InFlight::Publish(QoS::ExactlyOnce)) {
            inflight.insert(packet_id, InFlight::Received);
            Packet::PublishReceived { packet_id }
        } else {
            inflight.remove(&packet_id);
            Packet::PublishAck { packet_id }
        }
    }
}

impl<St, T, C, E> Service<Session<St>, Decoded> for Dispatcher<St, T, C, E>
where
    St: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E> + 'static,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
    E: 'static,
{
    type Res = Option<Packet>;
    type Error = DispatcherError<E>;

    fn bind(&self, ctx: BindCtx<'_>) {
        self.inner.payload.bind(&ctx);
        ctx.bind(&self.publish);
        ctx.bind(&self.inner.control);
    }

    #[inline]
    async fn ready(&self, ctx: Ctx<'_, Self, Session<St>>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), ctx.ready(&self.inner.control)).await;
        // payload backpressure, reading pauses while the streamed payload buffer
        // is above its high watermark, the connection is closed if the payload
        // receiver is dropped before the stream ends
        if res1.is_ok() && res2.is_ok() && self.inner.payload.ready().await != PayloadStatus::Ready
        {
            self.inner.sink.force_close();
        }

        res1.map_err(DispatcherError::Service)?;
        res2?;
        Ok(())
    }

    async fn shutdown(&self, ctx: Ctx<'_, Self, Session<St>>) {
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.close();

        ctx.shutdown(&self.inner.control).await;
        ctx.shutdown(&self.publish).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn call(
        &self,
        packet: Decoded,
        ctx: Ctx<'_, Self, Session<St>>,
    ) -> Result<Self::Res, Self::Error> {
        log::trace!("Dispatch packet: {packet:#?}");

        match packet {
            Decoded::Publish(publish, payload, size, ticket) => {
                // the Topic Name must not contain wildcards, [MQTT-3.3.2-2] (MQTT 3.1.1, 3.3.2)
                if publish.topic.contains(['#', '+']) {
                    return Err(SpecViolation::Pub_3_3_2_2.into());
                }

                let inner = &self.inner;
                let packet_id = publish.packet_id;

                // check for duplicated packet id
                if let Some(pid) = packet_id {
                    let state = inner.inflight.borrow().get(&pid).copied();
                    match state {
                        None => {
                            inner
                                .inflight
                                .borrow_mut()
                                .insert(pid, InFlight::Publish(publish.qos));
                        }
                        // a re-delivery keeps the packet id [MQTT-2.3.1-2], [MQTT-3.3.1-1]
                        // and is not a new publication until the ack is sent [MQTT-4.3.2-2],
                        // the ack of the first delivery acks both
                        Some(InFlight::Publish(qos)) if publish.dup && qos == publish.qos => {
                            log::trace!("Re-delivered publish packet is ignored: {pid:?}");
                            if publish.payload_size != payload.len() as u32 {
                                inner.discard_payload.set(true);
                            }
                            return Ok(None);
                        }
                        // until PUBREL, PUBLISH with the same packet id is acked by PUBREC
                        // and is not delivered [MQTT-4.3.3-2]
                        Some(InFlight::Received)
                            if publish.dup && publish.qos == QoS::ExactlyOnce =>
                        {
                            log::trace!("Re-delivered publish packet is received: {pid:?}");
                            if publish.payload_size != payload.len() as u32 {
                                inner.discard_payload.set(true);
                            }
                            return Ok(Some(Packet::PublishReceived { packet_id: pid }));
                        }
                        Some(_) => {
                            log::trace!("Duplicated packet id for publish packet: {pid:?}");
                            return Err(SpecViolation::PacketId_2_2_1_3_Pub.into());
                        }
                    }
                }

                let payload = if publish.payload_size == payload.len() as u32 {
                    Payload::from_bytes(payload)
                } else {
                    self.inner.payload.start(payload, self.max_buffer_size)
                };

                publish_fn(
                    &self.publish,
                    Publish::new(publish, payload, size).with_ticket(ticket),
                    packet_id,
                    inner,
                    ctx,
                )
                .await
            }
            Decoded::PayloadChunk(buf, eof) => {
                if self.inner.discard_payload.get() {
                    if eof {
                        self.inner.discard_payload.set(false);
                    }
                    Ok(None)
                } else if self.inner.payload.feed(buf, eof) {
                    Ok(None)
                } else {
                    // the publish of the chunk failed, the connection is closing
                    Err(MqttProtocolError::Decode(DecodeError::UnexpectedPayload).into())
                }
            }
            Decoded::Packet(Packet::PublishAck { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Publish(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishReceived { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishComplete { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PublishRelease { packet_id }, _) => {
                let state = self.inner.inflight.borrow().get(&packet_id).copied();
                match state {
                    Some(InFlight::Received) => {
                        self.inner
                            .control(ProtocolMessage::pubrel(packet_id), ctx)
                            .await
                    }
                    // PUBREL is a response to PUBREC [MQTT-4.3.3-1]
                    Some(InFlight::Publish(_)) => Err(MqttProtocolError::unexpected_packet(
                        packet_type::PUBREL,
                        "PublishRelease packet before PublishReceived",
                    )
                    .into()),
                    None => {
                        // PUBREL is re-sent after a session resumes [MQTT-4.4.0-1], the
                        // release could be already completed, PUBCOMP is required [MQTT-4.3.3-2]
                        log::trace!("Unknown packet-id in PublishRelease packet: {packet_id:?}");
                        Ok(Some(Packet::PublishComplete { packet_id }))
                    }
                }
            }
            Decoded::Packet(Packet::SubscribeAck { packet_id, status }, _) => {
                if let Err(e) = self
                    .inner
                    .sink
                    .pkt_ack(Ack::Subscribe { packet_id, status })
                {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::UnsubscribeAck { packet_id }, _) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet_id)) {
                    Err(e.into())
                } else {
                    Ok(None)
                }
            }
            Decoded::Packet(Packet::PingResponse, _) => {
                self.inner.sink.set_ping_pending(false);
                Ok(None)
            }
            Decoded::Packet(
                pkt @ (Packet::Connect(_)
                | Packet::ConnectAck(_)
                | Packet::PingRequest
                | Packet::Disconnect
                | Packet::Subscribe { .. }
                | Packet::Unsubscribe { .. }),
                _,
            ) => Err(MqttProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            )
            .into()),
        }
    }
}

/// Invoke the publish handler, acknowledging handled packets or forwarding unrouted ones.
async fn publish_fn<'f, St, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: Option<NonZeroU16>,
    inner: &'f Inner<C>,
    ctx: Ctx<'f, Dispatcher<St, T, C, E>, Session<St>>,
) -> Result<Option<Packet>, DispatcherError<E>>
where
    E: 'static,
    T: Service<Session<St>, Publish, Res = Either<(), Publish>, Error = E>,
    C: Service<Session<St>, ProtocolMessage, Res = ProtocolMessageAck, Error = DispatcherError<E>>,
{
    let res = ctx.call(svc, pkt).await.map_err(DispatcherError::Service)?;
    match res {
        Either::Left(()) => {
            log::trace!("Publish result for packet {packet_id:?} is ready");

            Ok(packet_id.map(|packet_id| inner.publish_ack(packet_id)))
        }
        Either::Right(pkt) => {
            let (pkt, payload, size) = pkt.into_inner();
            inner
                .control(ProtocolMessage::publish(pkt, payload, size), ctx)
                .await
        }
    }
}

impl<C> Inner<C> {
    /// Apply a protocol-service response, closing the connection on service failure.
    ///
    /// # Panics
    ///
    /// Panics if the client service returns a subscribe or unsubscribe response.
    async fn control<St, T, E>(
        &self,
        pkt: ProtocolMessage,
        ctx: Ctx<'_, Dispatcher<St, T, C, E>, Session<St>>,
    ) -> Result<Option<Packet>, DispatcherError<E>>
    where
        C: Service<
                Session<St>,
                ProtocolMessage,
                Res = ProtocolMessageAck,
                Error = DispatcherError<E>,
            >,
    {
        let packet = match ctx
            .call(&self.control, pkt)
            .await
            .inspect_err(|_| {
                self.drop_payload(&PayloadError::Service);
                self.sink.close();
            })?
            .result
        {
            ProtocolMessageKind::Ping => Some(codec::Packet::PingResponse),
            ProtocolMessageKind::PublishAck(id) => Some(self.publish_ack(id)),
            ProtocolMessageKind::PublishRelease(id) => {
                self.inflight.borrow_mut().remove(&id);
                Some(Packet::PublishComplete { packet_id: id })
            }
            ProtocolMessageKind::Subscribe(_) | ProtocolMessageKind::Unsubscribe(_) => {
                unreachable!()
            }
            ProtocolMessageKind::Disconnect => {
                self.drop_payload(&PayloadError::Service);
                self.sink.close();
                None
            }
            ProtocolMessageKind::Nothing => None,
        };

        Ok(packet)
    }
}

/// Unit tests for incoming publish validation, acknowledgements, and payload flow control.
#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin};

    use ntex_bytes::{ByteString, Bytes};
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{Pipeline, cfg::SharedCfg, fn_service};
    use ntex_util::future::lazy;
    use ntex_util::time::{Millis, Seconds, sleep, timeout};

    use super::*;
    use crate::v3::{MqttSink, QoS, codec::Decoded};

    /// Type-erased test dispatcher using unit session state and service errors.
    type Disp = Pipeline<Decoded, Option<Packet>, DispatcherError<()>>;

    /// Build a test packet identifier.
    ///
    /// # Panics
    ///
    /// Panics if `id` is zero.
    fn pid(id: u16) -> NonZeroU16 {
        NonZeroU16::new(id).unwrap()
    }

    /// Construct a dispatcher with a test transport and caller-supplied services.
    fn dispatcher<T, C>(
        publish: T,
        control: C,
        max_buffer_size: usize,
    ) -> (Io, Rc<MqttShared>, Disp)
    where
        T: Service<Session<()>, Publish, Res = Either<(), Publish>, Error = ()> + 'static,
        C: Service<
                Session<()>,
                ProtocolMessage,
                Res = ProtocolMessageAck,
                Error = DispatcherError<()>,
            > + 'static,
    {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let shared = Rc::new(MqttShared::new(
            io.get_ref(),
            codec::Codec::default(),
            false,
            Rc::default(),
        ));
        let disp = Pipeline::new(
            Session::new((), MqttSink::new(shared.clone()), SharedCfg::default()),
            Dispatcher::new(shared.clone(), publish, control, max_buffer_size),
        );
        (io, shared, disp)
    }

    /// Return a protocol response that sends no packet and keeps the connection open.
    async fn nothing(_: ProtocolMessage) -> Result<ProtocolMessageAck, DispatcherError<()>> {
        Ok(ProtocolMessageAck {
            result: ProtocolMessageKind::Nothing,
        })
    }

    /// STATEMENT: A second non-duplicate publication cannot reuse an identifier whose handler is still running.
    ///
    /// HOW:
    /// - Start a `QoS` 1 publication using a deliberately slow publish service.
    /// - Send another publication with the same identifier and assert the packet-identifier-in-use error.
    #[ntex::test]
    async fn test_dup_packet_id() {
        let (_io, _, disp) = dispatcher(
            fn_service(|_| async {
                sleep(Seconds(10)).await;
                Ok(Either::Left(()))
            }),
            fn_service(nothing),
            32 * 1024,
        );

        let mut f = Box::pin(disp.call(publish(1, QoS::AtLeastOnce, "")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        let err = disp
            .call(publish(1, QoS::AtLeastOnce, ""))
            .await
            .err()
            .unwrap();
        match err {
            DispatcherError::Protocol(msg) => {
                assert!(
                    format!("{msg}")
                        .contains("PUBLISH received with packet id that is already in use")
                );
            }
            DispatcherError::Service(()) => panic!(),
        }
    }

    /// STATEMENT: Incoming publish topic names reject MQTT wildcard characters.
    ///
    /// HOW:
    /// - Deliver publications whose topics contain `+` or `#` in different positions.
    /// - Assert the specific topic-name violation for each and accept a plain `a/b` topic.
    #[ntex::test]
    async fn test_publish_topic_wildcards() {
        let (_io, _, disp) = dispatcher(
            fn_service(async |_| Ok(Either::Left(()))),
            fn_service(nothing),
            32 * 1024,
        );
        let publish = |topic| publish(0, QoS::AtMostOnce, topic);

        // [MQTT-3.3.2-2] the Topic Name must not contain wildcards
        for topic in ["a/+", "a/#", "+", "a+b"] {
            let Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err))) =
                disp.call(publish(topic)).await
            else {
                panic!("expected protocol violation for {topic}")
            };
            assert_eq!(
                err.inner,
                crate::error::ViolationInner::Spec(SpecViolation::Pub_3_3_2_2)
            );
        }
        assert!(disp.call(publish("a/b")).await.is_ok());
    }

    /// STATEMENT: An unknown `QoS` 2 release receives completion without closing the connection.
    ///
    /// HOW:
    /// - Deliver `PUBREL` for identifier 100 without a preceding publication.
    /// - Assert matching `PUBCOMP` is returned and shared connection state stays active.
    #[ntex::test]
    async fn test_unknown_pubrel() {
        let (_io, shared, disp) = dispatcher(
            fn_service(async |_| Ok(Either::Left(()))),
            fn_service(nothing),
            32 * 1024,
        );

        // PUBREL is re-sent after a session resumes [MQTT-4.4.0-1]
        let packet_id = pid(100);
        let pkt = disp
            .call(Decoded::Packet(Packet::PublishRelease { packet_id }, 999))
            .await
            .unwrap();
        assert_eq!(pkt, Some(Packet::PublishComplete { packet_id }));
        assert!(shared.is_active());
    }

    /// Build counted publish and release services, delaying `publish/slow` by 100 ms.
    ///
    /// Topics starting with `publish` are handled after reading the payload; other
    /// publications are forwarded to the protocol-message service.
    fn qos2_dispatcher(
        pubrel: &Rc<Cell<usize>>,
        published: &Rc<Cell<usize>>,
    ) -> (Io, Rc<MqttShared>, Disp) {
        let pubrel = pubrel.clone();
        let published = published.clone();
        dispatcher(
            fn_service(async move |pkt: Publish| {
                published.set(published.get() + 1);
                if pkt.topic().path() == "publish/slow" {
                    sleep(Millis(100)).await;
                }
                let _ = pkt.read_all().await;
                if pkt.topic().path().starts_with("publish") {
                    Ok(Either::Left(()))
                } else {
                    Ok(Either::Right(pkt))
                }
            }),
            fn_service(move |msg: ProtocolMessage| {
                if let ProtocolMessage::PublishRelease(_) = msg {
                    pubrel.set(pubrel.get() + 1);
                }
                async move { Ok(msg.ack()) }
            }),
            32 * 1024,
        )
    }

    /// Build a complete empty publication, with no identifier when `id` is zero.
    fn publish(id: u16, qos: QoS, topic: &'static str) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup: false,
                retain: false,
                qos,
                topic: ByteString::from_static(topic),
                packet_id: NonZeroU16::new(id),
                payload_size: 0,
            },
            Bytes::new(),
            999,
            None,
        )
    }

    /// Build a duplicate publication with either complete or partially buffered payload.
    fn redelivery(id: u16, qos: QoS, chunked: bool) -> Decoded {
        Decoded::Publish(
            codec::Publish {
                dup: true,
                retain: false,
                qos,
                topic: ByteString::from_static("publish"),
                packet_id: NonZeroU16::new(id),
                payload_size: if chunked { 6 } else { 3 },
            },
            Bytes::from_static(b"abc"),
            999,
            None,
        )
    }

    /// Build the incoming release packet for a nonzero test identifier.
    fn pubrel(id: u16) -> Decoded {
        Decoded::Packet(Packet::PublishRelease { packet_id: pid(id) }, 999)
    }

    /// STATEMENT: `QoS` 2 identifiers stay reserved until release, while `QoS` 1 identifiers are freed on acknowledgement.
    ///
    /// HOW:
    /// - Exercise both routed and protocol-service publications and assert `PUBREC` responses.
    /// - Reject identifier reuse before `PUBREL`, then assert `PUBCOMP`, release-call counts, and successful reuse.
    /// - Acknowledge `QoS` 1 with `PUBACK` and complete an unknown release without another service call.
    #[ntex::test]
    async fn test_publish_qos2() {
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, shared, disp) = qos2_dispatcher(&pubrel_calls, &Rc::default());

        for topic in ["publish", "control"] {
            pubrel_calls.set(0);

            // QoS 2 publish is acknowledged with PUBREC [MQTT-4.3.3-2]
            let res = disp.call(publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(
                res.unwrap(),
                Some(Packet::PublishReceived { packet_id: pid(1) })
            );

            // packet id stays in use until PUBREL
            let res = disp.call(publish(1, QoS::ExactlyOnce, topic)).await;
            assert!(matches!(res, Err(DispatcherError::Protocol(_))));

            // PUBREL is passed to the control service, PUBCOMP is sent
            let res = disp.call(pubrel(1)).await;
            assert_eq!(
                res.unwrap(),
                Some(Packet::PublishComplete { packet_id: pid(1) })
            );
            assert_eq!(pubrel_calls.get(), 1);

            // packet id is released
            let res = disp.call(publish(1, QoS::ExactlyOnce, topic)).await;
            assert_eq!(
                res.unwrap(),
                Some(Packet::PublishReceived { packet_id: pid(1) })
            );
            let res = disp.call(pubrel(1)).await;
            assert_eq!(
                res.unwrap(),
                Some(Packet::PublishComplete { packet_id: pid(1) })
            );
            assert_eq!(pubrel_calls.get(), 2);

            // QoS 1 publish is acknowledged with PUBACK, packet id is released
            let res = disp.call(publish(2, QoS::AtLeastOnce, topic)).await;
            assert_eq!(res.unwrap(), Some(Packet::PublishAck { packet_id: pid(2) }));
            let res = disp.call(pubrel(2)).await;
            assert_eq!(
                res.unwrap(),
                Some(Packet::PublishComplete { packet_id: pid(2) })
            );
            assert_eq!(pubrel_calls.get(), 2);
        }
        assert!(shared.is_active());
    }

    /// STATEMENT: An incoming `QoS` 2 release is rejected until the original publish handler completes.
    ///
    /// HOW:
    /// - Start a slow `QoS` 2 publication without waiting for its receive acknowledgement.
    /// - Deliver `PUBREL` and assert an unexpected-packet violation with no protocol-service release call.
    #[ntex::test]
    async fn test_pubrel_before_pubrec() {
        let pubrel_calls = Rc::new(Cell::new(0));
        let (_io, _, disp) = qos2_dispatcher(&pubrel_calls, &Rc::default());

        let mut f = Box::pin(disp.call(publish(1, QoS::ExactlyOnce, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;

        // PUBREL is a response to PUBREC [MQTT-4.3.3-1]
        let res = disp.call(pubrel(1)).await;
        assert!(matches!(
            res,
            Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(ref err)))
                if matches!(err.inner, crate::error::ViolationInner::UnexpectedPacket { .. })
        ));
        assert_eq!(pubrel_calls.get(), 0);
    }

    /// STATEMENT: Payload chunks following a rejected publication return a decode error rather than panic.
    ///
    /// HOW:
    /// - Reject a partially buffered publication whose topic contains a wildcard.
    /// - Feed both final and non-final chunks and assert `DecodeError::UnexpectedPayload` for each.
    #[ntex::test]
    async fn test_unexpected_payload_chunk() {
        let unexpected = |res: Result<Option<Packet>, DispatcherError<()>>| {
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::Decode(
                    DecodeError::UnexpectedPayload
                )))
            )
        };
        let chunk = |eof| Decoded::PayloadChunk(Bytes::from_static(b"def"), eof);
        let (_io, _, disp) = qos2_dispatcher(&Rc::default(), &Rc::default());

        // the streaming publish is rejected before its payload is set up
        let Decoded::Publish(mut pkt, payload, size, ticket) =
            redelivery(1, QoS::AtLeastOnce, true)
        else {
            unreachable!()
        };
        pkt.dup = false;
        pkt.topic = ByteString::from_static("a/+");
        let res = disp
            .call(Decoded::Publish(pkt, payload, size, ticket))
            .await;
        assert!(matches!(res, Err(DispatcherError::Protocol(_))), "{res:?}");
        assert!(unexpected(disp.call(chunk(true)).await));
        assert!(unexpected(disp.call(chunk(false)).await));
    }

    /// STATEMENT: Valid retransmissions are not redelivered until their previous acknowledgement exchange completes.
    ///
    /// HOW:
    /// - Hold a `QoS` 1 handler pending, ignore a duplicate and its chunks, and reject `QoS` or duplicate-flag conflicts.
    /// - Repeat a `QoS` 2 publication before `PUBREL` and assert repeated `PUBREC` without another handler call.
    /// - Complete release, deliver the duplicate as a new streamed publication, and verify handler counts and acknowledgements.
    #[ntex::test]
    async fn test_publish_redelivery() {
        let is_err = |res: Result<Option<Packet>, DispatcherError<()>>| {
            matches!(res, Err(DispatcherError::Protocol(_)))
        };
        let pubrel_calls = Rc::new(Cell::new(0));
        let published = Rc::new(Cell::new(0));
        let (_io, _, disp) = qos2_dispatcher(&pubrel_calls, &published);

        // re-delivery is ignored until PUBACK [MQTT-4.3.2-2]
        let mut f = Box::pin(disp.call(publish(1, QoS::AtLeastOnce, "publish/slow")));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        let res = disp.call(redelivery(1, QoS::AtLeastOnce, true)).await;
        assert_eq!(res.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"def"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert!(is_err(
            disp.call(redelivery(1, QoS::ExactlyOnce, false)).await
        ));
        assert!(is_err(
            disp.call(publish(1, QoS::AtLeastOnce, "publish")).await
        ));
        assert_eq!(
            f.await.unwrap(),
            Some(Packet::PublishAck { packet_id: pid(1) })
        );
        assert_eq!(published.get(), 1);

        // re-delivery is acked by PUBREC until PUBREL [MQTT-4.3.3-2]
        let (_io, _, disp) = qos2_dispatcher(&pubrel_calls, &published);
        let pubrec = Some(Packet::PublishReceived { packet_id: pid(2) });
        let res = disp.call(publish(2, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), pubrec);
        let res = disp.call(redelivery(2, QoS::ExactlyOnce, true)).await;
        assert_eq!(res.unwrap(), pubrec);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"de"), false);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"f"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert_eq!(published.get(), 2);

        // after PUBCOMP re-delivery is a new message
        let res = disp.call(pubrel(2)).await;
        assert_eq!(
            res.unwrap(),
            Some(Packet::PublishComplete { packet_id: pid(2) })
        );
        assert_eq!(pubrel_calls.get(), 1);
        let mut f = Box::pin(disp.call(redelivery(2, QoS::ExactlyOnce, true)));
        let _ = lazy(|cx| Pin::new(&mut f).poll(cx)).await;
        assert_eq!(published.get(), 3);
        let chunk = Decoded::PayloadChunk(Bytes::from_static(b"def"), true);
        assert_eq!(disp.call(chunk).await.unwrap(), None);
        assert_eq!(timeout(Millis(100), f).await.unwrap().unwrap(), pubrec);

        // a different QoS or a missing DUP flag is a packet id conflict
        assert!(is_err(
            disp.call(redelivery(2, QoS::AtLeastOnce, false)).await
        ));
        let (_io, _, disp) = qos2_dispatcher(&pubrel_calls, &published);
        let res = disp.call(publish(2, QoS::ExactlyOnce, "publish")).await;
        assert_eq!(res.unwrap(), pubrec);
        assert!(is_err(
            disp.call(publish(2, QoS::ExactlyOnce, "publish")).await
        ));
        assert_eq!(published.get(), 4);
    }

    /// Assert that a protocol error identifies the expected unexpected packet type.
    fn assert_unexpected<E: std::fmt::Debug>(
        res: &Result<Option<Packet>, DispatcherError<E>>,
        expected: u8,
    ) {
        assert!(
            matches!(
                res,
                Err(DispatcherError::Protocol(MqttProtocolError::ProtocolViolation(err)))
                    if matches!(
                        err.inner,
                        crate::error::ViolationInner::UnexpectedPacket { packet_type, .. }
                            if packet_type == expected
                    )
            ),
            "{res:?}"
        );
    }

    /// STATEMENT: Ping responses clear pending state while client-only and repeated handshake packets are rejected.
    ///
    /// HOW:
    /// - Deliver `PINGRESP` and assert no response, no pending ping, and an active connection.
    /// - Deliver `CONNECT`, `CONNACK`, `PINGREQ`, and `DISCONNECT` and check each unexpected packet type.
    #[ntex::test]
    async fn test_unexpected_packets() {
        let (_io, shared, disp) = qos2_dispatcher(&Rc::default(), &Rc::default());

        // PINGRESP is handled, pending ping is cleared
        shared.set_ping_pending(true);
        let res = disp.call(Decoded::Packet(Packet::PingResponse, 999)).await;
        assert_eq!(res.unwrap(), None);
        assert!(!shared.is_ping_pending());
        assert!(shared.is_active());

        // packets sent by the client only and a second CONNACK are not expected from server
        for (pkt, tp) in [
            (Packet::Connect(Box::default()), packet_type::CONNECT),
            (
                Packet::ConnectAck(codec::ConnectAck {
                    return_code: codec::ConnectAckReason::ConnectionAccepted,
                    session_present: false,
                }),
                packet_type::CONNACK,
            ),
            (Packet::PingRequest, packet_type::PINGREQ),
            (Packet::Disconnect, packet_type::DISCONNECT),
        ] {
            assert_unexpected(&disp.call(Decoded::Packet(pkt, 999)).await, tp);
        }
    }

    /// STATEMENT: Payload buffering pauses reads without hiding service failures, and dropped payloads close I/O.
    ///
    /// HOW:
    /// - Retain a streamed publication and fill its four-byte buffer until readiness becomes pending.
    /// - Read the first chunk and verify readiness recovers with I/O still active.
    /// - Refill the buffer and independently trigger protocol and publish readiness errors.
    /// - Clear the errors, drop the unread payload receiver, and verify readiness completes with I/O closed.
    #[ntex::test]
    async fn test_payload_backpressure() {
        use std::{cell::RefCell, task::Poll};

        /// Protocol service whose shared failure flag controls its readiness result.
        struct FailReady<E>(Rc<Cell<bool>>, fn() -> E, ntex_service::ReadyNotifier);

        impl<St, E> Service<St, ProtocolMessage> for FailReady<E> {
            type Res = ProtocolMessageAck;
            type Error = E;

            fn bind(&self, ctx: BindCtx<'_>) {
                ctx.subscribe(&self.2);
            }

            async fn ready(&self, _: Ctx<'_, Self, St>) -> Result<(), E> {
                if self.0.get() { Err((self.1)()) } else { Ok(()) }
            }

            async fn call(
                &self,
                msg: ProtocolMessage,
                _: Ctx<'_, Self, St>,
            ) -> Result<Self::Res, E> {
                Ok(msg.ack())
            }
        }

        /// Hold the publication for external payload reads and expose switchable readiness.
        struct Hold<R, E>(
            Rc<RefCell<Option<Publish>>>,
            Rc<Cell<bool>>,
            fn() -> E,
            fn() -> R,
            ntex_service::ReadyNotifier,
        );

        impl<St, R, E> Service<St, Publish> for Hold<R, E> {
            type Res = R;
            type Error = E;

            fn bind(&self, ctx: BindCtx<'_>) {
                ctx.subscribe(&self.4);
            }

            async fn ready(&self, _: Ctx<'_, Self, St>) -> Result<(), E> {
                if self.1.get() { Err((self.2)()) } else { Ok(()) }
            }

            async fn call(&self, pkt: Publish, _: Ctx<'_, Self, St>) -> Result<R, E> {
                *self.0.borrow_mut() = Some(pkt);
                Ok((self.3)())
            }
        }

        let fail = Rc::new(Cell::new(false));
        let pfail = Rc::new(Cell::new(false));
        let held = Rc::new(RefCell::new(None));
        let changed = ntex_service::ReadyNotifier::new();
        let (io, _, disp) = dispatcher(
            Hold(
                held.clone(),
                pfail.clone(),
                || (),
                || Either::Left(()),
                changed.clone(),
            ),
            FailReady(
                fail.clone(),
                || DispatcherError::Protocol(MqttProtocolError::ReadTimeout),
                changed.clone(),
            ),
            4,
        );
        let chunk = |data: &'static [u8]| {
            disp.call_static(Decoded::PayloadChunk(Bytes::from_static(data), false))
        };

        // streamed payload below the high watermark
        let res = disp
            .call(Decoded::Publish(
                codec::Publish {
                    dup: false,
                    retain: false,
                    qos: QoS::AtMostOnce,
                    topic: ByteString::from_static("t"),
                    packet_id: None,
                    payload_size: 16,
                },
                Bytes::from_static(b"ab"),
                999,
                None,
            ))
            .await;
        assert_eq!(res.unwrap(), None);
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Ok(()))
        ));

        // the payload buffer reached the high watermark, reading pauses
        assert_eq!(chunk(b"cd").await.unwrap(), None);
        assert!(lazy(|cx| disp.poll_ready(cx)).await.is_pending());

        // the payload buffer is drained to the low watermark
        let pkt = held.borrow_mut().take().unwrap();
        assert_eq!(pkt.read().await.unwrap(), Some(Bytes::from_static(b"ab")));
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Ok(()))
        ));
        assert!(io.is_active());

        // a readiness error is not delayed by the paused payload
        assert_eq!(chunk(b"efgh").await.unwrap(), None);
        fail.set(true);
        changed.notify();
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Err(_))
        ));
        fail.set(false);
        pfail.set(true);
        changed.notify();
        assert!(matches!(
            lazy(|cx| disp.poll_ready(cx)).await,
            Poll::Ready(Err(_))
        ));
        pfail.set(false);
        changed.notify();

        // the payload receiver is dropped before the stream ends
        assert!(lazy(|cx| disp.poll_ready(cx)).await.is_pending());
        drop(pkt);
        assert!(disp.ready().await.is_ok());
        assert!(!io.is_active());
    }

    /// STATEMENT: A custom client protocol handler's ping acknowledgement is encoded as `PINGRESP`.
    ///
    /// HOW:
    /// - Forward an unrouted publication to a protocol handler that returns `ProtocolMessage::Ping(...).ack()`.
    /// - Assert the dispatcher returns `PingResponse` and leaves the connection active.
    #[ntex::test]
    async fn test_control_ping_response() {
        let (_io, shared, disp) = dispatcher(
            fn_service(async |pkt| Ok(Either::Right(pkt))),
            fn_service(async |_: ProtocolMessage| {
                Ok(ProtocolMessage::Ping(crate::v3::control::Ping).ack())
            }),
            32 * 1024,
        );
        assert_eq!(
            disp.call(publish(0, QoS::AtMostOnce, "a")).await.unwrap(),
            Some(Packet::PingResponse)
        );
        assert!(shared.is_active());
    }

    /// STATEMENT: A failed protocol handler closes the client and aborts its retained streaming payload.
    ///
    /// HOW:
    /// - Forward a partial publication to a protocol handler that retains it and returns a service error.
    /// - Assert dispatch returns that error, the connection closes, and reading the retained payload returns `PayloadError::Service`.
    #[ntex::test]
    async fn test_protocol_failure_aborts_payload() {
        let held = Rc::new(std::cell::RefCell::new(None));
        let held2 = held.clone();
        let (_io, shared, disp) = dispatcher(
            fn_service(async |pkt| Ok(Either::Right(pkt))),
            fn_service(move |msg: ProtocolMessage| {
                *held2.borrow_mut() = Some(msg);
                std::future::ready(Err::<ProtocolMessageAck, _>(DispatcherError::Service(())))
            }),
            4,
        );
        assert!(matches!(
            disp.call(redelivery(1, QoS::AtLeastOnce, true)).await,
            Err(DispatcherError::Service(()))
        ));
        assert!(!shared.is_active());
        let Some(ProtocolMessage::Publish(msg)) = held.borrow_mut().take() else {
            panic!("expected retained publication")
        };
        assert_eq!(msg.read_all().await, Err(PayloadError::Service));
    }
}
