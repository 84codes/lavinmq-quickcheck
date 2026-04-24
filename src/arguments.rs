// src/arguments.rs
use lapin::types::{AMQPValue, FieldTable, ShortString};
use quickcheck::{Arbitrary, Gen};
use crate::names::{QUEUE_NAME_CHARS, RESERVED_QUEUE_PREFIX, RoutingKey};

/// `x-max-length` — max number of messages.
#[derive(Clone, Debug)]
pub struct MaxLength(pub u32);

impl MaxLength {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-length"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for MaxLength {
    fn arbitrary(g: &mut Gen) -> Self {
        MaxLength(u32::arbitrary(g))
    }
}

/// `x-max-length-bytes` — max total byte size of messages.
#[derive(Clone, Debug)]
pub struct MaxLengthBytes(pub u64);

impl MaxLengthBytes {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-length-bytes"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for MaxLengthBytes {
    fn arbitrary(g: &mut Gen) -> Self {
        // AMQP long-long-int is signed. LavinMQ rejects negative byte limits,
        // so mask to i64::MAX to stay non-negative after the `as i64` cast in
        // insert_into.
        MaxLengthBytes(u64::arbitrary(g) & i64::MAX as u64)
    }
}

/// `x-message-ttl` — per-message TTL in milliseconds.
#[derive(Clone, Debug)]
pub struct MessageTtl(pub u32);

impl MessageTtl {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-message-ttl"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for MessageTtl {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageTtl(u32::arbitrary(g))
    }
}

/// `x-expires` — queue-level idle TTL in milliseconds (must be ≥ 1).
#[derive(Clone, Debug)]
pub struct Expires(pub u32);

impl Expires {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-expires"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for Expires {
    fn arbitrary(g: &mut Gen) -> Self {
        // Spec requires >= 1.
        let v = u32::arbitrary(g).saturating_add(1);
        Expires(v)
    }
}

/// `x-delivery-limit` — max redelivery attempts before dead-lettering.
#[derive(Clone, Debug)]
pub struct DeliveryLimit(pub u32);

impl DeliveryLimit {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-delivery-limit"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for DeliveryLimit {
    fn arbitrary(g: &mut Gen) -> Self {
        DeliveryLimit(u32::arbitrary(g))
    }
}

/// `x-consumer-timeout` — max consumer idle time in milliseconds.
#[derive(Clone, Debug)]
pub struct ConsumerTimeout(pub u32);

impl ConsumerTimeout {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-consumer-timeout"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for ConsumerTimeout {
    fn arbitrary(g: &mut Gen) -> Self {
        ConsumerTimeout(u32::arbitrary(g))
    }
}

/// `x-cache-size` — dedup cache capacity.
#[derive(Clone, Debug)]
pub struct CacheSize(pub u32);

impl CacheSize {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-cache-size"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for CacheSize {
    fn arbitrary(g: &mut Gen) -> Self {
        CacheSize(u32::arbitrary(g))
    }
}

/// `x-cache-ttl` — dedup cache entry TTL in milliseconds.
#[derive(Clone, Debug)]
pub struct CacheTtl(pub u32);

impl CacheTtl {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-cache-ttl"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for CacheTtl {
    fn arbitrary(g: &mut Gen) -> Self {
        CacheTtl(u32::arbitrary(g))
    }
}

#[derive(Clone, Debug)]
pub enum OverflowKind {
    DropHead,
    RejectPublish,
}

impl OverflowKind {
    fn as_str(&self) -> &'static str {
        match self {
            OverflowKind::DropHead => "drop-head",
            OverflowKind::RejectPublish => "reject-publish",
        }
    }
}

/// `x-overflow` — behaviour when a length limit is hit.
#[derive(Clone, Debug)]
pub struct Overflow(pub OverflowKind);

impl Overflow {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-overflow"),
            AMQPValue::LongString(self.0.as_str().into()),
        );
    }
}

impl Arbitrary for Overflow {
    fn arbitrary(g: &mut Gen) -> Self {
        let kind = if bool::arbitrary(g) {
            OverflowKind::DropHead
        } else {
            OverflowKind::RejectPublish
        };
        Overflow(kind)
    }
}

/// `x-single-active-consumer` — only one consumer active at a time.
#[derive(Clone, Debug)]
pub struct SingleActiveConsumer(pub bool);

impl SingleActiveConsumer {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-single-active-consumer"),
            AMQPValue::Boolean(self.0),
        );
    }
}

impl Arbitrary for SingleActiveConsumer {
    fn arbitrary(g: &mut Gen) -> Self {
        SingleActiveConsumer(bool::arbitrary(g))
    }
}

/// `x-message-deduplication` — enable dedup on this queue.
#[derive(Clone, Debug)]
pub struct MessageDeduplication(pub bool);

impl MessageDeduplication {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-message-deduplication"),
            AMQPValue::Boolean(self.0),
        );
    }
}

impl Arbitrary for MessageDeduplication {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageDeduplication(bool::arbitrary(g))
    }
}

/// `x-dead-letter-exchange` — exchange dead-letters are republished to.
#[derive(Clone, Debug)]
pub struct DeadLetterExchange(pub String);

impl DeadLetterExchange {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-dead-letter-exchange"),
            AMQPValue::LongString(self.0.clone().into()),
        );
    }
}

impl Arbitrary for DeadLetterExchange {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        loop {
            let name: String = (0..len)
                .map(|_| {
                    let &byte = g.choose(QUEUE_NAME_CHARS).unwrap();
                    byte as char
                })
                .collect();
            if !name.starts_with(RESERVED_QUEUE_PREFIX) {
                return DeadLetterExchange(name);
            }
        }
    }
}

/// `x-dead-letter-routing-key` — routing key used when dead-lettering.
#[derive(Clone, Debug)]
pub struct DeadLetterRoutingKey(pub RoutingKey);

impl DeadLetterRoutingKey {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-dead-letter-routing-key"),
            AMQPValue::LongString(self.0.0.clone().into()),
        );
    }
}

impl Arbitrary for DeadLetterRoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        DeadLetterRoutingKey(RoutingKey::arbitrary(g))
    }
}

/// `x-deduplication-header` — message-header name carrying the dedup key.
#[derive(Clone, Debug)]
pub struct DeduplicationHeader(pub String);

const HEADER_NAME_CHARS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

impl DeduplicationHeader {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-deduplication-header"),
            AMQPValue::LongString(self.0.clone().into()),
        );
    }
}

impl Arbitrary for DeduplicationHeader {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 64);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        let name: String = (0..len)
            .map(|_| {
                let &byte = g.choose(HEADER_NAME_CHARS).unwrap();
                byte as char
            })
            .collect();
        DeduplicationHeader(name)
    }
}
