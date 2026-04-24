// src/combined.rs
use lapin::types::FieldTable;
use quickcheck::{Arbitrary, Gen};

use crate::arguments::{
    CacheSize, CacheTtl, ConsumerTimeout, DeadLetterExchange, DeadLetterRoutingKey,
    DeduplicationHeader, DeliveryLimit, Expires, MaxLength, MaxLengthBytes, MessageDeduplication,
    MessageTtl, Overflow, SingleActiveConsumer,
};

#[derive(Clone, Debug)]
pub struct ClassicQueueArgs {
    pub max_length: Option<MaxLength>,
    pub max_length_bytes: Option<MaxLengthBytes>,
    pub overflow: Option<Overflow>,
    pub message_ttl: Option<MessageTtl>,
    pub expires: Option<Expires>,
    pub dead_letter_exchange: Option<DeadLetterExchange>,
    pub dead_letter_routing_key: Option<DeadLetterRoutingKey>,
    pub delivery_limit: Option<DeliveryLimit>,
    pub consumer_timeout: Option<ConsumerTimeout>,
    pub single_active_consumer: Option<SingleActiveConsumer>,
    pub message_deduplication: Option<MessageDeduplication>,
    pub deduplication_header: Option<DeduplicationHeader>,
    pub cache_size: Option<CacheSize>,
    pub cache_ttl: Option<CacheTtl>,
}

impl ClassicQueueArgs {
    pub fn apply(&self, table: &mut FieldTable) {
        if let Some(a) = &self.max_length {
            a.insert_into(table);
        }
        if let Some(a) = &self.max_length_bytes {
            a.insert_into(table);
        }
        if let Some(a) = &self.overflow {
            a.insert_into(table);
        }
        if let Some(a) = &self.message_ttl {
            a.insert_into(table);
        }
        if let Some(a) = &self.expires {
            a.insert_into(table);
        }
        if let Some(a) = &self.dead_letter_exchange {
            a.insert_into(table);
        }
        // LavinMQ rejects `x-dead-letter-routing-key` without
        // `x-dead-letter-exchange`. Only insert the routing key when an
        // exchange is also set — otherwise the routing key is an orphan.
        if let (Some(a), Some(_)) = (&self.dead_letter_routing_key, &self.dead_letter_exchange) {
            a.insert_into(table);
        }
        if let Some(a) = &self.delivery_limit {
            a.insert_into(table);
        }
        if let Some(a) = &self.consumer_timeout {
            a.insert_into(table);
        }
        if let Some(a) = &self.single_active_consumer {
            a.insert_into(table);
        }
        if let Some(a) = &self.message_deduplication {
            a.insert_into(table);
        }
        if let Some(a) = &self.deduplication_header {
            a.insert_into(table);
        }
        if let Some(a) = &self.cache_size {
            a.insert_into(table);
        }
        if let Some(a) = &self.cache_ttl {
            a.insert_into(table);
        }
    }
}

impl Arbitrary for ClassicQueueArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        ClassicQueueArgs {
            max_length: Option::arbitrary(g),
            max_length_bytes: Option::arbitrary(g),
            overflow: Option::arbitrary(g),
            message_ttl: Option::arbitrary(g),
            expires: Option::arbitrary(g),
            dead_letter_exchange: Option::arbitrary(g),
            dead_letter_routing_key: Option::arbitrary(g),
            delivery_limit: Option::arbitrary(g),
            consumer_timeout: Option::arbitrary(g),
            single_active_consumer: Option::arbitrary(g),
            message_deduplication: Option::arbitrary(g),
            deduplication_header: Option::arbitrary(g),
            cache_size: Option::arbitrary(g),
            cache_ttl: Option::arbitrary(g),
        }
    }
}

use crate::arguments::MaxPriority;

#[derive(Clone, Debug)]
pub struct PriorityQueueArgs {
    pub max_priority: MaxPriority,
    pub classic: ClassicQueueArgs,
}

impl PriorityQueueArgs {
    pub fn apply(&self, table: &mut FieldTable) {
        self.max_priority.insert_into(table);
        self.classic.apply(table);
    }
}

impl Arbitrary for PriorityQueueArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        PriorityQueueArgs {
            max_priority: MaxPriority::arbitrary(g),
            classic: ClassicQueueArgs::arbitrary(g),
        }
    }
}

use crate::arguments::MaxAge;

#[derive(Clone, Debug)]
pub struct StreamQueueArgs {
    pub max_length: Option<MaxLength>,
    pub max_length_bytes: Option<MaxLengthBytes>,
    pub max_age: Option<MaxAge>,
}

impl StreamQueueArgs {
    /// Inserts all set arguments PLUS x-queue-type: "stream".
    pub fn apply(&self, table: &mut FieldTable) {
        table.insert(
            lapin::types::ShortString::from("x-queue-type"),
            lapin::types::AMQPValue::LongString("stream".into()),
        );
        if let Some(a) = &self.max_length {
            a.insert_into(table);
        }
        if let Some(a) = &self.max_length_bytes {
            a.insert_into(table);
        }
        if let Some(a) = &self.max_age {
            a.insert_into(table);
        }
    }
}

impl Arbitrary for StreamQueueArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        StreamQueueArgs {
            max_length: Option::arbitrary(g),
            max_length_bytes: Option::arbitrary(g),
            max_age: Option::arbitrary(g),
        }
    }
}
