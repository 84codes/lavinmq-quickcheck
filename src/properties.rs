//! Arbitrary generators for AMQP `BasicProperties` fields. Each newtype
//! wraps a single value, exposes `apply_to(BasicProperties) -> BasicProperties`
//! that calls lapin's chainable setter, and implements `Arbitrary` with
//! broker-valid values.

use lapin::BasicProperties;
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use quickcheck::{Arbitrary, Gen};

use crate::names::QUEUE_NAME_CHARS;

/// Max length for short-string-valued properties (spec: up to 255 bytes).
/// We cap at 64 for ease of reading in failure reports.
const SHORT_STRING_MAX: usize = 64;

/// Generates 1..=max characters uniformly from `QUEUE_NAME_CHARS`.
fn short_string(g: &mut Gen, max: usize) -> String {
    let len = *g.choose(&(1..=max).collect::<Vec<_>>()).unwrap();
    (0..len)
        .map(|_| {
            let &byte = g.choose(QUEUE_NAME_CHARS).unwrap();
            byte as char
        })
        .collect()
}

/// `content_type` message property.
#[derive(Clone, Debug)]
pub struct ContentType(pub String);

impl ContentType {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_content_type(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ContentType {
    fn arbitrary(g: &mut Gen) -> Self {
        ContentType(short_string(g, SHORT_STRING_MAX))
    }
}

/// `content_encoding` message property.
#[derive(Clone, Debug)]
pub struct ContentEncoding(pub String);

impl ContentEncoding {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_content_encoding(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ContentEncoding {
    fn arbitrary(g: &mut Gen) -> Self {
        ContentEncoding(short_string(g, SHORT_STRING_MAX))
    }
}

/// `correlation_id` message property.
#[derive(Clone, Debug)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_correlation_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for CorrelationId {
    fn arbitrary(g: &mut Gen) -> Self {
        CorrelationId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `reply_to` message property. Opaque — broker does not validate as a queue name.
#[derive(Clone, Debug)]
pub struct ReplyTo(pub String);

impl ReplyTo {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_reply_to(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ReplyTo {
    fn arbitrary(g: &mut Gen) -> Self {
        ReplyTo(short_string(g, SHORT_STRING_MAX))
    }
}

/// `message_id` message property.
#[derive(Clone, Debug)]
pub struct MessageId(pub String);

impl MessageId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_message_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for MessageId {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `type` message property. Named `MessageKind` because `type` is a
/// reserved Rust keyword.
#[derive(Clone, Debug)]
pub struct MessageKind(pub String);

impl MessageKind {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_type(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for MessageKind {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageKind(short_string(g, SHORT_STRING_MAX))
    }
}

/// `app_id` message property.
#[derive(Clone, Debug)]
pub struct AppId(pub String);

impl AppId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_app_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for AppId {
    fn arbitrary(g: &mut Gen) -> Self {
        AppId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `cluster_id` message property. Deprecated by AMQP but still wire-legal.
#[derive(Clone, Debug)]
pub struct ClusterId(pub String);

impl ClusterId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_cluster_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ClusterId {
    fn arbitrary(g: &mut Gen) -> Self {
        ClusterId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `delivery_mode` — `1` (transient) or `2` (persistent).
#[derive(Clone, Debug)]
pub struct DeliveryMode(pub u8);

impl DeliveryMode {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_delivery_mode(self.0)
    }
}

impl Arbitrary for DeliveryMode {
    fn arbitrary(g: &mut Gen) -> Self {
        DeliveryMode(if bool::arbitrary(g) { 1 } else { 2 })
    }
}

/// `priority` — 0..=9 (conservative bound; brokers accept higher but
/// priority queues typically use 0-9).
#[derive(Clone, Debug)]
pub struct Priority(pub u8);

impl Priority {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_priority(self.0)
    }
}

impl Arbitrary for Priority {
    fn arbitrary(g: &mut Gen) -> Self {
        Priority(*g.choose(&(0u8..=9).collect::<Vec<_>>()).unwrap())
    }
}

/// `timestamp` — arbitrary u64 unix timestamp.
#[derive(Clone, Debug)]
pub struct MessageTimestamp(pub u64);

impl MessageTimestamp {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_timestamp(self.0)
    }
}

impl Arbitrary for MessageTimestamp {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageTimestamp(u64::arbitrary(g))
    }
}

/// `expiration` — short string parsed as a stringified integer millisecond value.
#[derive(Clone, Debug)]
pub struct Expiration(pub String);

impl Expiration {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_expiration(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for Expiration {
    fn arbitrary(g: &mut Gen) -> Self {
        // LavinMQ parses this as an integer number of milliseconds.
        // Generate any u32 formatted as decimal.
        Expiration(format!("{}", u32::arbitrary(g)))
    }
}

const HEADER_NAME_CHARS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

fn header_name(g: &mut Gen) -> String {
    let len = *g.choose(&(1..=32usize).collect::<Vec<_>>()).unwrap();
    (0..len)
        .map(|_| {
            let &byte = g.choose(HEADER_NAME_CHARS).unwrap();
            byte as char
        })
        .collect()
}

fn arbitrary_scalar_value(g: &mut Gen) -> AMQPValue {
    // Twelve uniform variants; one is picked per entry.
    let variant = *g.choose(&(0u8..12).collect::<Vec<_>>()).unwrap();
    match variant {
        0 => AMQPValue::Boolean(bool::arbitrary(g)),
        1 => AMQPValue::ShortShortInt(i8::arbitrary(g)),
        2 => AMQPValue::ShortShortUInt(u8::arbitrary(g)),
        3 => AMQPValue::ShortInt(i16::arbitrary(g)),
        4 => AMQPValue::ShortUInt(u16::arbitrary(g)),
        5 => AMQPValue::LongInt(i32::arbitrary(g)),
        6 => AMQPValue::LongUInt(u32::arbitrary(g)),
        7 => AMQPValue::LongLongInt(i64::arbitrary(g)),
        8 => AMQPValue::Float(f32::arbitrary(g)),
        9 => AMQPValue::Double(f64::arbitrary(g)),
        10 => {
            let s = short_string(g, SHORT_STRING_MAX);
            AMQPValue::LongString(LongString::from(s))
        }
        _ => AMQPValue::Timestamp(u64::arbitrary(g)),
    }
}

/// `headers` message property. Flat scalars only — no nested tables, arrays,
/// or exotic types (Void / DecimalValue / ByteArray / FieldArray / FieldTable).
#[derive(Clone, Debug)]
pub struct Headers(pub FieldTable);

impl Headers {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_headers(self.0.clone())
    }
}

impl Arbitrary for Headers {
    fn arbitrary(g: &mut Gen) -> Self {
        let n_entries = *g.choose(&(0usize..=10).collect::<Vec<_>>()).unwrap();
        let mut table = FieldTable::default();
        for _ in 0..n_entries {
            let key = header_name(g);
            let value = arbitrary_scalar_value(g);
            table.insert(key.into(), value);
        }
        Headers(table)
    }
}
