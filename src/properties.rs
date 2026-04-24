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
