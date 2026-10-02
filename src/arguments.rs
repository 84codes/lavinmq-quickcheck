// src/arguments.rs
use crate::names::{QUEUE_NAME_CHARS, RESERVED_QUEUE_PREFIX, RoutingKey};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use quickcheck::{Arbitrary, Gen};

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

/// Shortest generated `x-expires`. The spec allows 1 ms, but a queue
/// that expires mid-test races the test's own checks.
pub const MIN_EXPIRES: u32 = 60_000;

/// `x-expires` — queue-level idle TTL in milliseconds (≥ `MIN_EXPIRES`).
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
        Expires(u32::arbitrary(g).max(MIN_EXPIRES))
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
        // Cap at 100_000 to avoid triggering OOM crashes in LavinMQ when the
        // dedup cache is actually allocated (i.e. when x-message-deduplication
        // is also set). The broker allocates the cache up front; very large
        // values (> ~500 M on a typical test host) cause it to crash.
        CacheSize(u32::arbitrary(g) % 100_000)
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

/// `x-max-priority` — turns the queue into a priority queue (0..=255).
#[derive(Clone, Debug)]
pub struct MaxPriority(pub u8);

impl MaxPriority {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-priority"),
            AMQPValue::ShortShortUInt(self.0),
        );
    }
}

impl Arbitrary for MaxPriority {
    fn arbitrary(g: &mut Gen) -> Self {
        MaxPriority(u8::arbitrary(g))
    }
}

/// `x-max-age` — stream retention by age. Format: N + unit, where
/// unit ∈ {Y, M, D, h, m, s}. Example: "7D", "12h".
#[derive(Clone, Debug)]
pub struct MaxAge(pub String);

const MAX_AGE_UNITS: &[char] = &['Y', 'M', 'D', 'h', 'm', 's'];

impl MaxAge {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-age"),
            AMQPValue::LongString(self.0.clone().into()),
        );
    }
}

impl Arbitrary for MaxAge {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = *g.choose(&(1u32..=999).collect::<Vec<_>>()).unwrap();
        let unit = *g.choose(MAX_AGE_UNITS).unwrap();
        MaxAge(format!("{}{}", n, unit))
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn expires_outlives_a_test(e: Expires) -> bool {
        e.0 >= MIN_EXPIRES
    }
}
