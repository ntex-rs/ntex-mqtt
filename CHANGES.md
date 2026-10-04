# Changes

## [9.1.0] - Unreleased

* v5: Client applies its Receive Maximum to the response queue, `QoS 1` and `QoS 2`
  publishes within the limit are handled while the queue is full as on the server,
  previously these were held back and acks read after them waited

* v3, v5: Payload chunks do not keep response queue slots, previously each chunk received
  while the publish handler was pending kept a slot until the publish completed and filled
  the queue, holding back the packets after the publish

* v3, v5: Payload of a held back streaming publish is read once the publish is dispatched,
  previously the payload chunks were held back while the publish filled the response queue
  and the publish handler waiting for them stalled the connection

* v3, v5: Restore payload backpressure, reading pauses while the unread part of a streamed
  payload reaches `max_payload_buffer_size`, the connection is closed if the publish handler
  drops the payload before the stream ends. Previously the whole payload was buffered and
  the readiness error of a service waited for the payload handler

* v3, v5: Acks and pings are read and handled while the response queue is full, reading
  pauses at the first other packet, it is held back until the queue has room. Previously
  reading stopped at the limit and publish handlers that awaited acks of their own
  publishes deadlocked the connection, they still do if a held back packet precedes the
  acks. Payload chunks of a streaming publish follow the publish

* v5: `QoS 1` and `QoS 2` publishes are handled while the response queue is full and
  nothing is held back, Receive Maximum bounds them, the queue grows up to `max_queue`
  plus Receive Maximum responses

* v3, v5: Payload chunk of a streaming publish that waits for write backpressure fails
  with `Disconnected` when the connection is closed, previously it waited forever

* v5: Send credit released by an ack is reserved for the first waiting publish, waiting
  publishes, `ready()` and `stream_at_least_once` get the credit in the order of the calls,
  previously a new publish could take the credit of a woken one and the peer received more
  publishes than its Receive Maximum [MQTT-4.9.0-1]. `is_ready()` is false while publishes
  wait, a cancelled waiter passes the credit on and waiters fail on disconnect

* v3: `max_receive` limits incoming PUBLISH packets only, publishes over the limit wait
  for a slot while acks, pings and other packets are still read, previously a publish
  handler that awaited the ack of its own publish deadlocked at the limit. The v3 client
  no longer blocks the payload chunks of a streaming publish, and `0` disables its limit
  instead of blocking every packet

* v5: Receive Maximum counts `QoS 1` and `QoS 2` PUBLISH packets only, pending SUBSCRIBE and
  UNSUBSCRIBE packets no longer use send credit or cause a false 0x93 disconnect, and
  SUBSCRIBE/UNSUBSCRIBE wait for io write backpressure only (`IoRef::write_ready()`)

* v3: Round up 1.5 times of the client keep-alive, previously odd keep-alive values
  closed the connection early

* v3, v5: Release `QoS 2` publish if `send_exactly_once` future is dropped, previously
  PUBREL was never sent and the packet id and in-flight slot leaked

* v3: Default protocol-message service no longer logs a "Subscribe is not supported" warning
  for every message, only unsupported messages are logged

* v3, v5: Drop the payload chunks of a PUBLISH that is dropped after disconnect, previously
  the next chunk failed with an unexpected payload error

* v5: Use DISCONNECT 0x94 (Topic Alias invalid) for a Topic Alias greater than the maximum
  and the error's reason code, 0x82 (Protocol Error), for invalid acks instead of 0x83
  (MQTT 5.0, 3.3.2.3.4, 4.13.1)

* v5: PUBREC with a reason code of 0x80 or greater ends the QoS 2 flow, PUBREL is not sent,
  the packet id is released and `PublishReceived::release()` returns `UnexpectedRelease`
  (MQTT 5.0, 4.3.3)

* v3, v5: Several received QoS 2 publishes can be released at the same time, previously
  only the most recent PUBREC could be released and earlier ones never sent PUBREL

* v3, v5: Close the connection with a protocol error when PUBREC or PUBCOMP does not match
  the type of the in-flight packet, previously publish and subscribe futures could panic

* v5: Server closes the connection with DISCONNECT 0x9E or 0xA2 when a SUBSCRIBE contains
  a Shared or Wildcard Subscription that is not available in CONNACK
  (MQTT 5.0, 3.2.2.3.11, 3.2.2.3.13)

* v5: Server rejects a QoS 0 PUBLISH with RETAIN set when retain is not available
  [MQTT-3.2.2-14], previously only QoS 1 and QoS 2 were checked

* v3, v5: Server closes the connection on a second CONNECT [MQTT-3.1.0-2] and on CONNACK,
  SUBACK, UNSUBACK or PINGRESP from client, client closes the connection on CONNECT or a second
  CONNACK from server, control service is called with an unexpected packet protocol error,
  previously these packets were ignored

* v3: Client handles a re-delivered PUBLISH (DUP) with a packet id in use instead of closing
  the connection, it is ignored until PUBACK [MQTT-4.3.2-2] or acked by PUBREC without delivery
  until PUBREL [MQTT-4.3.3-2], payload chunks of a not delivered PUBLISH are discarded

* v5: Server and client handle a re-delivered PUBLISH (DUP) with a packet id in use, it is
  ignored until PUBACK [MQTT-4.3.2-5] or acked by PUBREC without delivery until PUBREL
  [MQTT-4.3.3-10]. Other packet id conflicts are answered by PUBREC for QoS 2 (server sent
  PUBACK) and payload chunks of a not delivered PUBLISH are discarded

* v3, v5: Server default protocol service acknowledges PUBREL instead of closing the connection

* v5: Server releases the packet id of a received QoS 2 PUBLISH after PUBCOMP [MQTT-4.3.3-12]
  or after PUBREC with an error reason code [MQTT-4.3.3-9], previously the id stayed in use and
  consumed receive maximum quota. PUBREL before PUBREC is a protocol violation

* v5: Client acknowledges a received QoS 2 PUBLISH with PUBREC instead of PUBACK, the packet
  id stays in use until PUBCOMP [MQTT-4.3.3-10] or is released after PUBREC with an error
  reason code [MQTT-4.3.3-9], PUBREL before PUBREC is a protocol violation.
  `ClientRouter::start_default()` acknowledges PUBREL instead of closing the connection

* v3: Client acknowledges a received QoS 2 PUBLISH with PUBREC instead of PUBACK, the packet
  id stays in use until PUBREL [MQTT-4.3.3-2], PUBREL before PUBREC is a protocol violation.
  `ClientRouter::start_default()` acknowledges PUBREL instead of closing the connection

* v3: Server accepts a re-delivered PUBLISH (DUP) with a packet id in use instead of closing
  the connection, it is ignored until PUBACK [MQTT-4.3.2-2] or acked by PUBREC until PUBREL
  without delivery [MQTT-4.3.3-2], PUBREL before PUBREC is a protocol violation

* v3: Reply with PUBCOMP to a PUBREL with an unknown packet id instead of closing the
  connection, PUBREL is re-sent after a session resumes [MQTT-4.4.0-1]

* `TopicFilter` supports Shared Subscriptions `$share/{ShareName}/{filter}`, malformed ones
  are rejected [MQTT-4.8.2-1], [MQTT-4.8.2-2], matching uses the filter after the prefix,
  a `$` first level of the filter is a `System` level, add `TopicFilter::share_name()`

* `TopicFilter` created from levels rejects a single `Blank` level, an empty topic filter

* Fix `TopicFilter::matches_filter()` matching a filter that starts with a `$` level by
  a wildcard at the first level [MQTT-4.7.2-1]

* Encoders reject SUBSCRIBE and UNSUBSCRIBE with malformed topic filters, misplaced
  wildcards [MQTT-4.7.1-*] and, for v5, malformed Shared Subscriptions [MQTT-4.8.2-1],
  [MQTT-4.8.2-2]

* Client dispatchers reject PUBLISH with wildcards in the Topic Name [MQTT-3.3.2-2] and,
  for v5, in the Response Topic [MQTT-3.3.2-14]

* Fix `TopicFilter` panicking in `Display` when created without levels, `TryFrom` rejects
  an empty list of levels and `Deserialize` validates the levels like `TryFrom`

* `TopicFilter` created from levels rejects levels that parsing never produces, an empty or
  `/`-containing `Normal` level, a `Normal` first level starting with `$` and a `System` level
  without `$`

* Fix in-flight limits being skipped for the request after a PUBLISH with a complete payload
  or after the last payload chunk, `SizedRequest::is_publish()` and `is_chunk()` are replaced
  by `has_more_chunks()`

* Default `MqttServiceConfig::max_size` is 256 KB instead of unlimited, the v5 server
  advertises it as Maximum Packet Size in CONNACK, use `set_max_size(0)` for no limit

* v5: Reject a non-minimal Variable Byte Integer encoding [MQTT-1.5.5-1]

* codec: Reject a QoS 0 PUBLISH with the DUP flag set [MQTT-3.3.1-2]

* codec: Reject a CONNACK with Session Present set and a non-zero return code
  [MQTT-3.2.2-4] (v3), [MQTT-3.2.2-6] (v5)

* v5: Send `server_keepalive_sec` if the client's keep-alive is 0 [MQTT-3.2.2-22], and close
  the connection after 1.2 times of the advertised `server_keepalive_sec` instead of exactly it,
  a not advertised `ConnectAck::keep_alive()` timeout is enforced as is

* v5: Default keep-alive timeout is 1.2 times of the client's keep-alive rounded up

* codec: v5 `Codec::clone()` copies the decode state and flags, the codec returned by client
  `into_inner()` keeps the limits from the server's CONNACK

* codec: Accept a v5 Message Expiry Interval of 0, `PublishProperties::message_expiry_interval`
  and `LastWill::message_expiry_interval` are `Option<u32>`

* Reject a CONNECT with an empty Will Topic or a Will Topic with wildcards,
  v5 server also rejects a Will Response Topic with wildcards and sends CONNACK 0x82 Protocol Error,
  v3 server closes the connection without CONNACK

* codec: Check the v5 max inbound and outbound packet size against the total packet size,
  `Codec::max_outbound_size()` returns the configured size, `DecodeError::MaxSizeExceeded`
  reports the total packet size

* codec: Fix limit underflow for v5 PUBACK, PUBREC, PUBREL, PUBCOMP and SUBACK with small max packet size

* Reject a v5 PUBLISH from a client with a Subscription Identifier or a Response Topic
  with wildcards, reject a malformed ShareName and No Local on a Shared Subscription

* codec: Clear Session Present in a CONNACK with a non-zero reason code

* codec: Drop Session Expiry Interval from a v5 DISCONNECT sent by the server

* codec: Require a v5 Authentication Method in AUTH and with CONNECT Authentication Data,
  encode AUTH Success without properties with a Remaining Length of 0

* codec: Reject a v5 CONNACK Maximum QoS other than 0 or 1

* codec: Reject a v5 CONNACK Maximum Packet Size of zero on decode and encode

* codec: Reject encoding a v5 Shared Subscription with No Local set

* codec: Reject encoding a v5 Response Topic with wildcard characters

* codec: Check string and binary data lengths before encoding, packets with fields over
  65,535 bytes fail without writing partial data

* codec: Fail encoding subscription identifiers over 268,435,455 instead of panicking

* v5: Send CONNACK 0x84 for an unsupported protocol level, v3 CONNACK 0x01 for MQTT 3.1.1 clients

* codec: Reject publish packets with a header longer than the packet, reject
  encoding a publish with a payload bigger than its payload size

* codec: Fail encoding packets over the protocol size limit instead of panicking

* codec: Fail encoding packets while a publish payload is incomplete

* Write acks, pings and other packets sent while a publish payload is streamed after the payload
  instead of failing with ExpectPayload

* codec: Reserve at most 8kb of read buffer ahead for non-publish packets

* codec: Reject SUBSCRIBE packets with reserved subscription options bits set

* codec: Reject SUBSCRIBE and UNSUBSCRIBE packets without topic filters

* codec: Reject CONNECT packets with invalid will or password flags

* codec: Reject trailing bytes in CONNECT, CONNACK, PINGREQ, PINGRESP and DISCONNECT packets

* codec: Reject PUBLISH packets with an empty topic name (v5: unless a topic alias is set)

* codec: Enforce sender-side rules in the encoder: valid topic names in PUBLISH and Will, no DUP flag
  for QoS 0, non-empty topic filter lists, v3 password requires username and empty client id requires
  clean session

* Fix sink being stuck in streaming state after a failed publish encode

* codec: Reject strings containing the null character U+0000, on decode and encode

* v3: Send CONNACK 0x01 for an unsupported protocol level and CONNACK 0x02 for an empty
  client id without clean session before closing the connection

* Update to ntex-io 4.1, ntex-codec 2.0

* Stop the dispatcher on clean peer eof while the service is not ready

* Api docs fixes

* Support IoConfig::write_timeout(), add MqttProtocolError::WriteTimeout

* Limit dispatcher response queue, add MqttServiceConfig::set_max_queue()

* Refactor dispatcher timers, frame read rate counts bytes consumed by the codec

* Keep-alive and frame read rate timeouts apply to the whole streamed publish, not each payload chunk

* v5: Send PacketIdentifierInUse publish acks in the order packets are received

* Control service readiness pauses reading only, readiness errors shut down the service and io

* Report the first service call error to the control service, later errors no longer overwrite it

* Release write backpressure and stop the write timer once output is flushed, even if the service is not ready

* Deliver write backpressure changes to the control service in order and before the stop message, write their responses, control errors shut down the dispatcher

* Stop dispatcher on response encode errors in spawned service calls

* Only publish acks keep the order of incoming packets, other responses are sent once ready, pending ones count towards max queue

* Set default MqttServiceConfig connect timeout to 5 seconds

## [9.0.0] - 2026-09-14

* Refactor state management

* Update to ntex-service 5.0

## [8.2.1] - 2026-06-18

* Fix keep-alive flag setting #253

## [8.2.0] - 2026-06-13

* Fix tight loop when service is not ready and write back-pressure is enabled

## [8.1.0] - 2026-06-05

* Allow to specify ConnectAckReason for failed v3 handshake

## [8.0.0] - 2026-05-12

* Update to ntex-io 3.11

## [8.0.0-beta.5] - 2026-05-05

* Use new codec api with BytePages support

## [8.0.0-beta.4] - 2026-04-30

* Include MaxSize error parameters

## [8.0.0-beta.3] - 2026-04-24

* Add control service support for client

## [8.0.0-beta.2] - 2026-04-24

* Restore Handshake::packet_mut()

## [8.0.0-beta.1] - 2026-04-18

* Introduce new control service only for network level messages

* Split control messages to protocol control messages and
  connection control messages.

* v5: MqttSink::close_with_no_reason() propery close io

## [7.6.1] - 2026-03-28

* Fix deadlock in io::dispatcher if disconnect happen during long in-flight
  publish and Disconnect packet.

## [7.6.0] - 2026-03-14

* Add support for ntex-error

## [7.5.0] - 2026-02-23

* Close connection after Receiving Disconnect packet

* Handle MQTT-3.14.2-2 error cases

* Better handling for protocol spec violation errors

## [7.4.0] - 2026-02-22

* Do not send `DISCONNECT` if `DISCONNECT` packet is already received

## [7.3.2] - 2026-02-20

* `HandshakeAck::max_send()` uses `Option`

## [7.3.1] - 2026-02-20

* Enforces an upper bound for max concurrent outbound messages

## [7.3.0] - 2026-02-18

* Allow to set max_send for v5 handshake

## [7.2.0] - 2026-02-16

* MqttServiceConfig is not Clone

## [7.1.0] - 2026-02-16

* SharedCfg is not Copy

## [7.0.0] - 2026-01-30

* Better disconnect handling on control service failure

## [7.0.0-pre.2] - 2026-01-29

* Send `Disconnect` packet once

## [7.0.0-pre.1] - 2026-01-29

* Use ntex_dispatcher::DispatchItem instead of ntex_io

## [7.0.0-pre.0] - 2026-01-28

* Refactor `control` message

* Send control stop message out of order

## [6.6.2] - 2026-01-20

* Fix in-flight handling after drop

## [6.6.1] - 2026-01-17

* Update bytes dependency

## [6.6.0] - 2026-01-12

* Wait control service handling completion before dropping in-flight
  publish handlers on error or disconnect.

## [6.5.0] - 2026-01-04

* Use .split_to_bytes()

## [6.4.1] - 2025-12-18

* Add v5::MqttServer::replace_middleware() helper method.

## [6.4.0] - 2025-12-17

* Upgrade to ntex-service v4

## [6.3.3] - 2025-12-16

* Use proper context for control service call

## [6.3.2] - 2025-12-16

* Use call_nowait() for service only if it is ready

## [6.3.1] - 2025-12-15

* Expose ProtocolViolationError info

## [6.3.0] - 2025-12-14

* Update ntex-io primitives

## [6.2.1] - 2025-12-09

* Drop first handler future on stop

## [6.2.0] - 2025-12-08

* Update bstream

* Allow to configure payload buffer size

## [6.1.1] - 2025-12-04

* Add helper method for Connect message

## [6.1.0] - 2025-12-04

* Refactor service configuration

## [6.0.0] - 2025-12-03

* Update edition

## [6.0.0-pre.0] - 2025-11-28

* Use shared configuration

* Update MSRV to 1.85

## [5.5.0] - 2025-10-07

* Drop payload stream on service error

## [5.4.0] - 2025-10-02

* Cancel packet handling on stop

## [5.3.0] - 2025-08-15

* Added v3::HandshakeAck.max_packet_size(..) to allow overriding maximum supported packet size on per connection basis

## [5.2.1] - 2025-05-21

* Try to fix docs.rs build

## [5.2.0] - 2025-05-08

* Refactor stream support in publish builder

## [5.1.0] - 2025-05-05

* Fix .read_all() method return type

## [5.0.0] - 2025-04-28

* Raise ProtocolError for unexpected PublishRelease

## [5.0.0-beta.2] - 2025-04-21

* Add QoS::ExactlyOnce support

## [5.0.0-beta.1] - 2025-04-16

* Cleanup payload errors

## [5.0.0-beta.0] - 2025-04-15

* Add publish packet streaming

## [4.6.0] - 2025-04-02

* Remove "client-id" check for v5

## [4.5.1] - 2024-12-04

* Check service readiness for every turn

## [4.5.0] - 2024-12-04

* Use updated Service trait

## [4.4.0] - 2024-11-10

* Check service readiness once per decoded item

* Run un-readiness check in separate task

## [4.3.1] - 2024-11-05

* Do not rely on not_ready(), always check service readiness

## [4.3.0] - 2024-11-04

* Use updated Service trait

## [4.2.1] - 2024-11-01

* Better rediness error handling

## [4.2.0] - 2024-10-31

* Call control service on readiness error

## [4.1.1] - 2024-10-15

* Disconnect on error from service readiness check

## [4.1.0] - 2024-10-10

* Do not check readiness for call

* Handle service readiness errors during shutdown

## [4.0.0] - 2024-10-05

* Middlewares support for mqtt server

## [3.1.0] - 2024-08-23

* Derive Hash for the QoS enum #175

## [3.0.0] - 2024-05-28

* Switch to individual ntex_* crates

* Use ntex-service 3.0

## [2.0.2] - 2024-05-15

* Remove non_exhaustive marker

## [2.0.1] - 2024-05-14

* Better naming

## [2.0.0] - 2024-05-1x

* Mark `Control` type as `non exhaustive`

* Rename `ControlMessage` to `Control`

* Remove protocol variant services

* Disable keep-alive timer if not configured

* Add write back-pressure to io dispatcher

## [1.1.0] - 2024-03-07

* Use MqttService::connect_timeout() only for reading protocol version

## [1.0.0] - 2024-01-09

* Release

## [1.0.0-b.0] - 2024-01-07

* Use "async fn" in trait for Service definition

## [0.12.16] - 2023-12-25

* Handle QoS 0 messages when the client disconnect #164

* Use io tags

## [0.12.15] - 2023-12-10

* Fix KEEP-ALIVE timer handling

## [0.12.14] - 2023-12-03

* Optimize KEEP-ALIVE timer

## [0.12.13] - 2023-11-29

* Refactor io timers

## [0.12.12] - 2023-11-25

* Fix keep-alive timeout handling

## [0.12.11] - 2023-11-23

* Refactor slow frame timeout handling

## [0.12.10] - 2023-11-21

* Remove slow frame timer if service is not ready

## [0.12.9] - 2023-11-17

* Do not process data in read buffer after disconnect

## [0.12.8] - 2023-11-12

* Use new ntex-io apis

## [0.12.7] - 2023-11-04

* Fix v5::Subscribe/Unsubscribe packet properties encoding

## [0.12.6] - 2023-10-31

* Send server ConnectAck without io flushing

## [0.12.5] - 2023-10-23

* Fix typo

## [0.12.4] - 2023-10-03

* Fix nested error handling for control service

## [0.12.3] - 2023-10-01

* Fix Publish and Control error type

## [0.12.2] - 2023-09-25

* Drop unneeded HandshakeError::Server

## [0.12.1] - 2023-09-25

* Change handshake timeout behavior (renamed to connect timeout).
  Timeout handles slow client's Control frame.

## [0.12.0] - 2023-09-18

* Refactor MqttError type

## [0.11.4] - 2023-08-10

* Update ntex deps

## [0.11.3] - 2023-06-26

* Update BufferService usage

## [0.11.2] - 2023-06-23

* Fix client connector usage, fixes lifetime constraint

## [0.11.1] - 2023-06-23

* `PipelineCall` is static

## [0.11.0] - 2023-06-22

* Release v0.11.0

## [0.11.0-beta.3] - 2023-06-21

* Use ContainerCall, remove unsafe

## [0.11.0-beta.2] - 2023-06-19

* Fix Dispatcher impl, poll Container<S> instead of S

## [0.11.0-beta.1] - 2023-06-19

* Use ServiceCtx instead of Ctx

## [0.11.0-beta.0] - 2023-06-17

* Migrate to ntex-0.7

## [0.10.4] - 2023-05-12

* Expose size of prepared packet

* Return packet and packet size from decoder

## [0.10.3] - 2023-04-06

* Adds non-blocking qos1 publish sender

* Adds validation of topic filters in SUBSCRIBE and UNSUBSCRIBE (#136)

## [0.10.2] - 2023-03-15

* Sink readiness depends on write back-pressure

## [0.10.1] - 2023-01-31

* Fix missing ready wakes up from InFlightService

* Register Dispatcher waker when service is not ready

## [0.10.0] - 2023-01-24

* Change ConnectAck session_expiry_interval_secs type to Option<u32>

* Introduce EncodeError::OverMaxPacketSize to differentiate failure to encode due to going over peer's Maximum packet size

## [0.10.0-beta.3] - 2023-01-20

* Revert builders refactoring

## [0.10.0-beta.2] - 2023-01-20

* Fix dispatcher leak during stop process

* Refactor client error

* Drop derive_more dep

* Exposed QoS at crate's level, disbanded types module

* Added v5::Sink::force_close()

* Added v5::Client::into_inner()

* packet properties with clear defaults per spec are represented without Option, use default when absent; for example, Session Expiry Interval, Maximum QoS, Retain Available, etc. in Connect and ConnectAck

* server-level settings for Maximum QoS, Topic Alias Maximum and Receive Maximum are now applied at ConnectAck construction. Any changes to ConnectAck in Handshake service are honored on connection level.

* Setting RETAIN on PUBLISH when CONNACK stated `Retain Available: 0` triggers Protocol Error

* Setting Subscription Identifier on SUBSCRIBE when CONNACK stated `Subscription Identifier Available: 0` triggers Protocol Error

* Topic name with `+` or `#` in it will trigger Protocol Error

* Protocol violation errors are now grouped under opaque ProtocolViolationError

* Removed Client re-export under v3 module. Use v3::client::Client instead.

## [0.10.0-beta.1] - 2023-01-04

* Migrate to ntex-0.6

* Use thiserror::Error for error definitions

## [0.10.0-beta.0] - 2022-12-28

* Migrate to ntex-service 1.0

## [0.9.2] - 2022-12-16

* v5: Fix topic alias handling #122

* v3: Allow to change outgoing in-flight limit

* v3/v5: Fix sink inflight messages handling after local codec error #123

## [0.9.1] - 2022-11-17

* v5: allow omitting properties length if it is 0 in packets without payload regardless of reason code or its presence.

## [0.9.0] - 2022-11-01

* Rename `Level` to `TopicFilterLevel` for better spec compliance

* v5: Use correct reason code for MaxQosViolated error #117

## [0.9.0-b.2] - 2022-10-28

* v3/v5: MqttSink::ready() is not ready until CONNACK get sent to peer

## [0.9.0-b.1] - 2022-10-17

* Remove deprecated methods

## [0.9.0-b.0] - 2022-10-10

* Renamed Topic into TopicFilter, TopicError into TopicFilterError
* Changes to topic filter validation: levels starting with `$` are allowed at any level and are recognized as system
  only at first position
* Changes to topic matching logic: when topic filter is matched against another topic filter via TopicFilter.match_filter(),
  left hand side topic filter must be strict superset of all topics allowed with topic filter on right hand side
* Changes to topic matching logic: having `+/#` in the end of topic filter does not wrongly recover failed match on `+` level
* Validation is now part of TopicFilter instantiation, e.g. it is impossible to create non-validated topic filter from
  set of Levels.
* Level API is removed completely as level itself is not a valuable concept.

## [0.8.11] - 2022-10-07

* v3/v5: Allow to create `PublishBuilder` with predefined Publish packet

* v3: Allow to specify max allowed qos for server publishes

* v5: Check max qos violations in server dispatcher

## [0.8.10] - 2022-09-25

* Add .into_inner() client's helper for publish control message

## [0.8.9] - 2022-09-16

* v3: Send disconnect packet on sink close

* v3: Treat disconnect packet as error on client side

## [0.8.8] - 2022-08-22

* Allow to get inner io stream and codec for negotiated clients

* Remove inflight limit for client's control service

* v3: Add Debug trait for client's ControlMessage

## [0.8.7] - 2022-06-09

* v5: Encoding missing will properties: will_delay_interval_sec, is_utf8_payload, message_expiry_interval, content_type, response_topic, correlation_data, user_properties

## [0.8.6] - 2022-05-05

* v5: Account for property type byte in property length when encoding Subscribe packet

* v5: Add Router::finish() helper method, it converts router to service factory

* v3/v3: Clearify session type for Router

## [0.8.5] - 2022-04-20

* v3: Make topic generic type for MqttSink::publish() method

* v5: Correct receive max value for v5 connector when broker omits value #100

## [0.8.4] - 2022-03-14

* Add support in-flight messages size back-pressure

* Refactor handshake timeout handling

* Add serializer and deserializer derive (#89)

* Correct spelling of SubscribeAckReason::SharedSubsriptionNotSupported and DisconnectReasonCode::SharedSubsriptionNotSupported (#93)

* Removed PubAckReason::ReceiveMaximumExceeded as this error code is only valid for DISCONNECT packets (#95)

* Update subs.rs example to use confirm instead of subscribe (#97)

## [0.8.3] - 2022-01-10

* Cleanup v3/v5 client connectors

## [0.8.2] - 2022-01-04

* Optimize compilation times

## [0.8.1] - 2022-01-03

* Cleanup MqttError types

## [0.8.0] - 2021-12-30

* Upgrade to ntex 0.5.0

## [0.8.0-b.6] - 2021-12-30

* Update to ntex-io 0.1.0-b.10

## [0.8.0-b.5] - 2021-12-28

* Shutdown io stream after failed handshake

## [0.8.0-b.4] - 2021-12-27

* Use IoBoxed for all server interfaces

## [0.8.0-b.3] - 2021-12-27

* Upgrade to ntex 0.5 b4

## [0.8.0-b.2] - 2021-12-24

* Upgrade to ntex-service 0.3.0

## [0.8.0-b.1] - 2021-12-22

* Better handling for io::Error

* Upgrade to ntex 0.5.0-b.2

## [0.8.0-b.0] - 2021-12-21

* Upgrade to ntex 0.5

## [0.7.7] - 2021-12-17

* Wait for close control message and inner services on dispatcher shutdown #78

* Use default keepalive from Connect packet. #75

## [0.7.6] - 2021-12-02

* Add memory pools support

## [0.7.5] - 2021-11-04

* v5: Use variable length byte to encode the subscription ID #73

## [0.7.4] - 2021-10-29

* Expose some control plane type constructors

## [0.7.3] - 2021-10-20

* Do not poll service for readiness if it failed before

## [0.7.2] - 2021-10-01

* Serialize control message handling

## [0.7.1] - 2021-09-18

* Allow to extract error from control message

## [0.7.0] - 2021-09-17

* Update ntex to 0.4

## [0.7.0-b.10] - 2021-09-07

* v3: add ControlMessage::Error and ControlMessage::ProtocolError

## [0.7.0-b.9] - 2021-09-07

* v5: add helper methods to client control publish message

## [0.7.0-b.8] - 2021-08-28

* use new ntex's timer api

## [0.7.0-b.7] - 2021-08-16

* v3: Boxed Packet::Connect to trim down Packet size
* v5: Boxed Packet::Connect and Packet::ConnAck variants to trim down Packet size

## [0.7.0-b.6] - 2021-07-28

* v3/v5: Fixed nested with_queues calls in sink impl

## [0.7.0-b.5] - 2021-07-15

* v3/v5: PublishBuilder::send_at_least_once initiates publish synchronously

* v3/v5: Publish::take_payload() replaces payload with empty bytes, returns existing

## [0.7.0-b.4] - 2021-07-12

* v3: avoid nested borrow_mut() calls in sink on puback mismatch

## [0.7.0-b.3] - 2021-07-04

* Re-export ClientRouter, SubscribeBuilder, UnsubscribeBuilder

## [0.7.0-b.2] - 2021-06-30

* v3: Remove special treatment for "?" in publish's topic

## [0.7.0-b.1] - 2021-06-27

* Upgrade to ntex-0.4

## [0.6.9] - 2021-06-17

* Use `Handshake<Io>` instead of `codec::Connect` for selector

## [0.6.8] - 2021-06-17

* Add coonditional mqtt server selector

## [0.6.7] - 2021-05-17

* Process unhandled data on disconnect #51

* Fix for panic while parsing MQTT version #52

## [0.6.6] - 2021-04-29

* v5: Fix reason string encoding

* v5: Allow to set reason and properties to SUBACK

## [0.6.5] - 2021-04-03

* v5: Add a `packet()` function to `Subscribe` and `Unsubscribe`

* upgrade ntex, drop direct futures dependency

## [0.6.4] - 2021-03-15

* `HandshakeAck::buffer_params()` replaces individual methods for buffer sizes

## [0.6.2] - 2021-03-04

* Allow to override io buffer params

## [0.6.1] - 2021-02-25

* Cleanup dependencies

## [0.6.0] - 2021-02-24

* Upgrade to ntex v0.3

## [0.5.0] - 2021-02-21

* Upgrade to ntex v0.2

## [0.5.0-b.5] - 2021-01-25

* Upgrade to ntex v0.2-b.7

## [0.5.0-b.4] - 2021-01-23

* Use ntex v0.2-b.5 framed types

## [0.5.0-b.3] - 2021-01-21

* v5: Flush io stream before disconnect

## [0.5.0-b.2] - 2021-01-20

* v5: Restore `set_properties` sink method

## [0.5.0-b.1] - 2021-01-19

* Use ntex 0.2

## [0.4.7] - 2021-01-13

* v5: Add ping and disconnect support to default control service

## [0.4.6] - 2021-01-12

* Use pin-project-lite instead of pin-project

## [0.4.5] - 2021-01-12

* v5: Check publish service readiness error

* io: Fix potential BorrowMut error in io dispatcher

## [0.4.4] - 2021-01-09

* Fix public re-exports

## [0.4.3] - 2021-01-09

* Fix out of bounds panic

## [0.4.2] - 2021-01-05

* Better read back-pressure support

## [0.4.1] - 2021-01-04

* Use ashash instead on fxhash

* Drop unneeded InOrder service usage

## [0.4.0] - 2021-01-03

* Refactor io dispatcher

* Rename Connect/ConnectAck to Handshake/HandshakeAck

## [0.3.17] - 2020-11-04

* v5: Allow to configure ConnectAck::max_qos value

## [0.3.16] - 2020-10-28

* Do not print publish payload in debug fmt

* v5: Create topic handlers on firse use

## [0.3.15] - 2020-10-20

* v5: Handle "Request Problem Information" flag

## [0.3.14] - 2020-10-07

* v3: Fix borrow error in sink impl

## [0.3.13] - 2020-10-07

* Allow to set packet id for sink operations

## [0.3.12] - 2020-10-05

* v5: Add helper method Connect::fail_with()

* v5: Better name SubscribeIter::confirm()

## [0.3.11] - 2020-09-29

* v5: Fix borrow error in MqttSink::close_with_reason()

## [0.3.10] - 2020-09-22

* Add async fn `MqttSink::ready()` returns when there is available client credit.

## [0.3.9] - 2020-09-18

* `ControlMessage` (v3/v5) and referenced types have `#[derive(Debug)]` added

* Add `Deref` impl for `Session<_>`

* v5: Do not override `max_packet_size`, `receive_max` and `topic_alias_max`

## [0.3.8] - 2020-09-03

* Fix packet ordering

* Check default router service readiness

* v5: Fix in/out bound frame size checks in codec

## [0.3.7] - 2020-09-02

* v5: Add PublishBuilder::set_properties() helper method

* v3: Fix PublishBuilder methods

## [0.3.6] - 2020-09-02

* v5: Add Error::ack_with() helper method

## [0.3.5] - 2020-08-31

* v3: New client api

* v5: New client api

* v5: Send publish packet returns ack or publish error

## [0.3.4] - 2020-08-14

* v5: set `max_qos` to `AtLeastOnce` for server `ConnectAck` response

* v5: do not set `session_expiry_interval_secs` prop

## [0.3.3] - 2020-08-13

* v5: do not convert publish error to ack for QoS0 packets

## [0.3.2] - 2020-08-13

* v5: Handle packet id in use for publish, subscribe and unsubscribe packets

* v5: Handle 16 concurrent control service requests

* v3: Handle packet id in use for subscribe and unsubscribe packets

* v3: Handle 16 concurrent control service requests

* Removed ProtocolError::DuplicatedPacketId error

## [0.3.1] - 2020-08-12

* v5: Fix max inflight check

## [0.3.0] - 2020-08-12

* v5: Add topic aliases support

* v5: Forward publish errors to control service

* Move keep-alive timeout to Framed dispatcher

* Rename PublishBuilder::at_most_once/at_least_once into send_at_most_once/send_at_least_once

* Replace ConnectAck::properties with ConnectAck::with

## [0.2.1] - 2020-08-03

* Fix v5 decoding for properties going beyond properties boundary

## [0.2.0] - 2020-07-28

* Fix v5 server constraints

* Add v3::Connect::service_unavailable()

* Refactor Topics matching

## [0.2.0-beta.2] - 2020-07-22

* Add Publish::packet_mut() method

## [0.2.0-beta.1] - 2020-07-06

* Add mqtt v5 protocol support

* Refactor control packets handling

## [0.1.3] - 2020-05-26

* Check for duplicated in-flight packet ids

## [0.1.2] - 2020-04-20

* Update ntex

## [0.1.1] - 2020-04-07

* Add disconnect timeout

## [0.1.0] - 2020-04-01

* For to ntex namespace

## [0.2.3] - 2020-03-10

* Add server handshake timeout

## [0.2.2] - 2020-02-04

* Fix server keep-alive impl

## [0.2.1] - 2019-12-25

* Allow to specify multi-pattern for topics

## [0.2.0] - 2019-12-11

* Migrate to `std::future`

* Support publish with QoS 1

## [0.1.0] - 2019-09-25

* Initial release
