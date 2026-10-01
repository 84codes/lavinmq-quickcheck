//! Property-based testing helpers for AMQP (LavinMQ) interactions.
//!
//! Provides `Arbitrary` implementations for AMQP names and queue arguments,
//! together with integration tests that exercise them against a real LavinMQ
//! broker.

pub mod alternate;
pub mod arguments;
pub mod combined;
pub mod consistent_hash;
pub mod consumer_priority;
pub mod delayed;
pub mod headers;
pub mod names;
pub mod properties;
pub mod routing;
pub mod stream_offset;

pub use names::{QueueName, RoutingKey, TopicRoutingKey};

#[cfg(test)]
mod tests;
