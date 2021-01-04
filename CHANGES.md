# Changes

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
