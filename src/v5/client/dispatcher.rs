use std::cell::{Cell, RefCell};
use std::{marker::PhantomData, num::NonZeroU16, rc::Rc, task::Context};

use ntex_bytes::ByteString;
use ntex_dispatcher::{Control as DispControl, DispatchItem, Reason};
use ntex_service::{Pipeline, Service, ServiceCtx, cfg::Cfg};
use ntex_util::{HashMap, HashSet, future::Either, future::join};

use crate::error::{HandshakeError, MqttError, PayloadError, ProtocolError};
use crate::payload::{Payload, PayloadStatus, PlSender};
use crate::v5::codec::{Decoded, DisconnectReasonCode, Encoded, Packet};
use crate::v5::shared::{Ack, MqttShared};
use crate::v5::{codec, control::Pkt, publish::Publish, publish::PublishAck, sink::MqttSink};
use crate::{MqttServiceConfig, types::packet_type};

use super::control::{Control, ControlAck};

/// mqtt5 protocol dispatcher
pub(super) fn create_dispatcher<T, C, E>(
    sink: MqttSink,
    publish: T,
    control: C,
    max_receive: usize,
    max_topic_alias: u16,
    cfg: Cfg<MqttServiceConfig>,
) -> impl Service<DispatchItem<Rc<MqttShared>>, Response = Option<Encoded>, Error = MqttError<E>>
where
    E: From<T::Error> + 'static,
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<Control<E>, Response = ControlAck, Error = E> + 'static,
{
    Dispatcher {
        cfg,
        publish,
        max_receive,
        max_topic_alias,
        inner: Rc::new(Inner {
            sink: sink.shared(),
            payload: Cell::new(None),
            control: Pipeline::new(control.map_err(MqttError::Service)),
            info: RefCell::new(PublishInfo {
                aliases: HashMap::default(),
                inflight: HashSet::default(),
            }),
        }),
        _t: PhantomData,
    }
}

/// Mqtt protocol dispatcher
pub(crate) struct Dispatcher<T, C: Service<Control<E>>, E> {
    publish: T,
    inner: Rc<Inner<C>>,
    max_receive: usize,
    max_topic_alias: u16,
    cfg: Cfg<MqttServiceConfig>,
    _t: PhantomData<E>,
}

struct Inner<C> {
    control: Pipeline<C>,
    sink: Rc<MqttShared>,
    info: RefCell<PublishInfo>,
    payload: Cell<Option<PlSender>>,
}

struct PublishInfo {
    inflight: HashSet<NonZeroU16>,
    aliases: HashMap<NonZeroU16, ByteString>,
}

impl<C> Inner<C> {
    fn drop_payload<PErr>(&self, err: &PErr)
    where
        PErr: Clone,
        PayloadError: From<PErr>,
    {
        if let Some(pl) = self.payload.take() {
            pl.set_error(err.clone().into());
        }
    }
}

impl<T, C, E> Service<DispatchItem<Rc<MqttShared>>> for Dispatcher<T, C, E>
where
    E: 'static,
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E> + 'static,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>> + 'static,
{
    type Response = Option<Encoded>;
    type Error = MqttError<E>;

    #[inline]
    async fn ready(&self, ctx: ServiceCtx<'_, Self>) -> Result<(), Self::Error> {
        let (res1, res2) = join(ctx.ready(&self.publish), self.inner.control.ready()).await;
        let result = if let Err(e) = res1 {
            if res2.is_err() {
                Err(MqttError::Service(e))
            } else {
                match self.inner.control.call(Control::error(e)).await {
                    Ok(res) => {
                        if res.disconnect {
                            self.inner.sink.drop_sink();
                        }
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
        } else {
            res2
        };

        if result.is_ok()
            && let Some(pl) = self.inner.payload.take()
        {
            self.inner.payload.set(Some(pl.clone()));
            if pl.ready().await != PayloadStatus::Ready {
                self.inner.sink.force_close();
            }
        }
        result
    }

    #[inline]
    fn poll(&self, cx: &mut Context<'_>) -> Result<(), Self::Error> {
        if let Err(e) = self.publish.poll(cx) {
            let inner = self.inner.clone();
            ntex_rt::spawn(async move {
                if let Ok(res) = inner.control.call(Control::error(e)).await
                    && res.disconnect
                {
                    inner.sink.drop_sink();
                }
            });
        }
        self.inner.control.poll(cx)
    }

    async fn shutdown(&self) {
        self.inner.drop_payload(&PayloadError::Disconnected);
        self.inner.sink.drop_sink();
        let _ = self.inner.control.call(Control::shutdown()).await;

        self.publish.shutdown().await;
        self.inner.control.shutdown().await;
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn call(
        &self,
        request: DispatchItem<Rc<MqttShared>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        log::trace!("Dispatch packet: {:#?}", request);

        match request {
            DispatchItem::Item(Decoded::Publish(mut publish, payload, size)) => {
                let info = self.inner.as_ref();
                let packet_id = publish.packet_id;

                {
                    let mut inner = info.info.borrow_mut();

                    if let Some(pid) = packet_id {
                        // check for receive maximum
                        if self.max_receive != 0 && inner.inflight.len() >= self.max_receive {
                            log::trace!(
                                "Receive maximum exceeded: max: {} inflight: {}",
                                self.max_receive,
                                inner.inflight.len()
                            );
                            drop(inner);
                            return control(
                                Control::proto_error(
                                    ProtocolError::violation(
                                        codec::DisconnectReasonCode::ReceiveMaximumExceeded,
                                        "Number of in-flight messages exceeds set maximum, [MQTT-3.3.4-9]"
                                    )
                                ),
                                &self.inner,
                                0,
                            ).await;
                        }

                        // check for duplicated packet id
                        if !inner.inflight.insert(pid) {
                            let _ = self.inner.sink.encode_packet(Packet::PublishAck(
                                codec::PublishAck {
                                    packet_id: pid,
                                    reason_code: codec::PublishAckReason::PacketIdentifierInUse,
                                    ..Default::default()
                                },
                            ));
                            return Ok(None);
                        }
                    }

                    // handle topic aliases
                    if let Some(alias) = publish.properties.topic_alias {
                        if publish.topic.is_empty() {
                            // lookup topic by provided alias
                            match inner.aliases.get(&alias) {
                                Some(aliased_topic) => publish.topic = aliased_topic.clone(),
                                None => {
                                    drop(inner);
                                    return control(
                                        Control::proto_error(ProtocolError::violation(
                                            DisconnectReasonCode::TopicAliasInvalid,
                                            "Unknown topic alias",
                                        )),
                                        &self.inner,
                                        0,
                                    )
                                    .await;
                                }
                            }
                        } else {
                            // record new alias
                            match inner.aliases.entry(alias) {
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    if entry.get().as_str() != publish.topic.as_str() {
                                        let mut topic = publish.topic.clone();
                                        topic.trimdown();
                                        entry.insert(topic);
                                    }
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    if alias.get() > self.max_topic_alias {
                                        drop(inner);
                                        return control(
                                                Control::proto_error(
                                                    ProtocolError::generic_violation(
                                                        "Topic alias is greater than max allowed [MQTT-3.1.2-26]",
                                                    )
                                                ),
                                            &self.inner,
                                            0
                                            ).await
                                        ;
                                    }
                                    let mut topic = publish.topic.clone();
                                    topic.trimdown();
                                    entry.insert(topic);
                                }
                            }
                        }
                    }
                }

                let payload = if publish.payload_size == payload.len() as u32 {
                    Payload::from_bytes(payload)
                } else {
                    let (pl, sender) =
                        Payload::from_stream(payload, self.cfg.max_payload_buffer_size);
                    self.inner.payload.set(Some(sender));
                    pl
                };

                publish_fn(
                    &self.publish,
                    Publish::new(publish, payload, size),
                    packet_id.map(|v| v.get()).unwrap_or(0),
                    size,
                    info,
                    ctx,
                )
                .await
            }
            DispatchItem::Item(Decoded::PayloadChunk(buf, eof)) => {
                let pl = self.inner.payload.take().unwrap();
                pl.feed_data(buf);
                if eof {
                    pl.feed_eof();
                } else {
                    self.inner.payload.set(Some(pl));
                }
                Ok(None)
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishAck(pkt), ..)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Publish(pkt)) {
                    control(Control::proto_error(err), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishReceived(pkt), _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Receive(pkt)) {
                    control(Control::proto_error(e), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishRelease(pkt), size)) => {
                if self.inner.info.borrow().inflight.contains(&pkt.packet_id) {
                    control(Control::pubrel(pkt, size), &self.inner, 0).await
                } else {
                    Ok(Some(Encoded::Packet(codec::Packet::PublishComplete(
                        codec::PublishAck2 {
                            packet_id: pkt.packet_id,
                            reason_code: codec::PublishAck2Reason::PacketIdNotFound,
                            properties: codec::UserProperties::default(),
                            reason_string: None,
                        },
                    ))))
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::PublishComplete(pkt), _)) => {
                if let Err(e) = self.inner.sink.pkt_ack(Ack::Complete(pkt)) {
                    control(Control::proto_error(e), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::SubscribeAck(packet), ..)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Subscribe(packet)) {
                    control(Control::proto_error(err), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::UnsubscribeAck(packet), ..)) => {
                if let Err(err) = self.inner.sink.pkt_ack(Ack::Unsubscribe(packet)) {
                    control(Control::proto_error(err), &self.inner, 0).await
                } else {
                    Ok(None)
                }
            }
            DispatchItem::Item(Decoded::Packet(Packet::Disconnect(pkt), size)) => {
                control(Control::dis(pkt, size), &self.inner, 0).await
            }
            DispatchItem::Item(Decoded::Packet(Packet::Auth(_), ..)) => {
                control(
                    Control::proto_error(ProtocolError::unexpected_packet(
                        packet_type::AUTH,
                        "AUTH packet is not supported at this time",
                    )),
                    &self.inner,
                    0,
                )
                .await
            }
            DispatchItem::Item(Decoded::Packet(Packet::PingResponse, ..)) => Ok(None),
            DispatchItem::Item(
                Decoded::Packet(pkt @ Packet::PingRequest, _)
                | Decoded::Packet(pkt @ Packet::Subscribe(_), _)
                | Decoded::Packet(pkt @ Packet::Unsubscribe(_), _),
            ) => Err(HandshakeError::Protocol(ProtocolError::unexpected_packet(
                pkt.packet_type(),
                "Packet of the type is not expected from server",
            ))
            .into()),
            DispatchItem::Item(Decoded::Packet(pkt, _)) => {
                log::debug!("Unsupported packet: {:?}", pkt);
                Ok(None)
            }
            DispatchItem::Stop(Reason::Encoder(err)) => {
                let err = ProtocolError::Encode(err);
                self.inner.drop_payload(&err);
                control(Control::proto_error(err), &self.inner, 0).await
            }
            DispatchItem::Stop(Reason::Decoder(err)) => {
                let err = ProtocolError::Decode(err);
                self.inner.drop_payload(&err);
                control(Control::proto_error(err), &self.inner, 0).await
            }
            DispatchItem::Stop(Reason::Io(err)) => {
                self.inner.drop_payload(&PayloadError::Disconnected);
                control(Control::peer_gone(err), &self.inner, 0).await
            }
            DispatchItem::Stop(Reason::KeepAliveTimeout) => {
                self.inner.drop_payload(&ProtocolError::KeepAliveTimeout);
                control(Control::proto_error(ProtocolError::KeepAliveTimeout), &self.inner, 0)
                    .await
            }
            DispatchItem::Stop(Reason::ReadTimeout) => {
                self.inner.drop_payload(&ProtocolError::ReadTimeout);
                control(Control::proto_error(ProtocolError::ReadTimeout), &self.inner, 0).await
            }
            DispatchItem::Control(DispControl::WBackPressureEnabled) => {
                self.inner.sink.enable_wr_backpressure();
                Ok(None)
            }
            DispatchItem::Control(DispControl::WBackPressureDisabled) => {
                self.inner.sink.disable_wr_backpressure();
                Ok(None)
            }
        }
    }
}

/// Publish service response future
async fn publish_fn<'f, T, C, E>(
    svc: &'f T,
    pkt: Publish,
    packet_id: u16,
    packet_size: u32,
    inner: &'f Inner<C>,
    ctx: ServiceCtx<'f, Dispatcher<T, C, E>>,
) -> Result<Option<Encoded>, MqttError<E>>
where
    T: Service<Publish, Response = Either<Publish, PublishAck>, Error = E>,
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let ack = match ctx.call(svc, pkt).await {
        Ok(res) => match res {
            Either::Right(ack) => ack,
            Either::Left(pkt) => {
                let (pkt, payload) = pkt.into_inner();
                return control(Control::publish(pkt, payload, packet_size), inner, packet_id)
                    .await;
            }
        },
        Err(e) => return control(Control::error(e), inner, 0).await,
    };

    if let Some(id) = NonZeroU16::new(packet_id) {
        log::trace!("Sending publish ack for {} id", packet_id);
        inner.info.borrow_mut().inflight.remove(&id);
        let ack = codec::PublishAck {
            packet_id: id,
            reason_code: ack.reason_code,
            reason_string: ack.reason_string,
            properties: ack.properties,
        };
        Ok(Some(Encoded::Packet(Packet::PublishAck(ack))))
    } else {
        Ok(None)
    }
}

async fn control<C, E>(
    mut pkt: Control<E>,
    inner: &Inner<C>,
    packet_id: u16,
) -> Result<Option<Encoded>, MqttError<E>>
where
    C: Service<Control<E>, Response = ControlAck, Error = MqttError<E>>,
{
    let mut error = matches!(pkt, Control::Stop(_));

    loop {
        let result = match inner.control.call(pkt).await {
            Ok(result) => {
                if let Some(id) = NonZeroU16::new(packet_id) {
                    inner.info.borrow_mut().inflight.remove(&id);
                }
                result
            }
            Err(err) => {
                // do not handle nested error
                return if error {
                    inner.drop_payload(&PayloadError::Service);
                    inner.sink.drop_sink();
                    Err(err)
                } else {
                    // handle error from control service
                    match err {
                        MqttError::Service(err) => {
                            error = true;
                            pkt = Control::error(err);
                            continue;
                        }
                        _ => Err(err),
                    }
                };
            }
        };

        let response = if error {
            match result.packet {
                Pkt::Packet(pkt) => {
                    let _ = inner.sink.encode_packet(pkt);
                }
                Pkt::Disconnect(pkt) => {
                    if !inner.sink.is_disconnect_sent() {
                        let _ = inner.sink.encode_packet(pkt.into());
                    }
                }
                Pkt::None => {}
            }
            Ok(None)
        } else {
            match result.packet {
                Pkt::Packet(pkt) => Ok(Some(Encoded::Packet(pkt))),
                Pkt::Disconnect(pkt) => {
                    if !inner.sink.is_disconnect_sent() {
                        Ok(Some(Encoded::Packet(codec::Packet::from(pkt))))
                    } else {
                        Ok(None)
                    }
                }
                Pkt::None => Ok(None),
            }
        };

        if result.disconnect {
            inner.drop_payload(&PayloadError::Service);
            inner.sink.drop_sink();
        }
        return response;
    }
}

#[cfg(test)]
mod tests {
    use ntex_io::{Io, testing::IoTest};
    use ntex_service::{cfg::SharedCfg, fn_service};
    use ntex_util::future::{Ready, lazy};

    use super::*;

    #[derive(Debug)]
    struct TestError;

    #[ntex::test]
    async fn test_wr_backpressure() {
        let io = Io::new(IoTest::create().0, SharedCfg::new("DBG"));
        let codec = codec::Codec::default();
        let shared = Rc::new(MqttShared::new(io.get_ref(), codec, Default::default()));
        let sink = MqttSink::new(shared.clone());

        let disp = Pipeline::new(create_dispatcher(
            sink.clone(),
            fn_service(|p: Publish| Ready::Ok::<_, TestError>(Either::Right(p.ack()))),
            fn_service(|_| {
                Ready::Ok::<_, TestError>(ControlAck { packet: Pkt::None, disconnect: false })
            }),
            16,
            16,
            Default::default(),
        ));

        assert!(!sink.is_ready());
        shared.set_cap(1);
        assert!(sink.is_ready());
        assert!(shared.wait_readiness().is_none());

        disp.call(DispatchItem::Control(DispControl::WBackPressureEnabled)).await.unwrap();
        assert!(!sink.is_ready());
        let rx = shared.wait_readiness();
        let rx2 = shared.wait_readiness().unwrap();
        assert!(rx.is_some());

        let rx = rx.unwrap();
        disp.call(DispatchItem::Control(DispControl::WBackPressureDisabled)).await.unwrap();
        assert!(lazy(|cx| rx.poll_recv(cx).is_ready()).await);
        assert!(!lazy(|cx| rx2.poll_recv(cx).is_ready()).await);
    }
}
