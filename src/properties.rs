//! Arbitrary generators for AMQP `BasicProperties` fields. Each newtype
//! wraps a single value, exposes `apply_to(BasicProperties) -> BasicProperties`
//! that calls lapin's chainable setter, and implements `Arbitrary` with
//! broker-valid values.

use lapin::BasicProperties;
use lapin::types::ShortString;
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
