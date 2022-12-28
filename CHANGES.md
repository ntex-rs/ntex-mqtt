# Changes

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
