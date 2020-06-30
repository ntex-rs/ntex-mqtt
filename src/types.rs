pub const MQTT_LEVEL_3: u8 = 4;
pub const MQTT_LEVEL_5: u8 = 5;

/// Max possible packet size
pub const MAX_PACKET_SIZE: u32 = 0xF_FF_FF_FF;

prim_enum! {
    /// Quality of Service
    pub enum QoS {
        /// At most once delivery
        ///
        /// The message is delivered according to the capabilities of the underlying network.
        /// No response is sent by the receiver and no retry is performed by the sender.
        /// The message arrives at the receiver either once or not at all.
        AtMostOnce = 0,
        /// At least once delivery
        ///
        /// This quality of service ensures that the message arrives at the receiver at least once.
        /// A QoS 1 PUBLISH Packet has a Packet Identifier in its variable header
        /// and is acknowledged by a PUBACK Packet.
        AtLeastOnce = 1,
        /// Exactly once delivery
        ///
        /// This is the highest quality of service,
        /// for use when neither loss nor duplication of messages are acceptable.
        /// There is an increased overhead associated with this quality of service.
        ExactlyOnce = 2
    }
}
