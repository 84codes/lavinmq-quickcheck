// src/arguments.rs
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
        // Cast to i64 via LongLongInt; clamp to non-negative range.
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
