//! Property-based testing helpers for AMQP (LavinMQ) interactions.
//!
//! Provides `Arbitrary` implementations for AMQP names and queue arguments,
//! together with integration tests that exercise them against a real LavinMQ
//! broker.

pub mod names;

pub use names::{QueueName, RoutingKey, TopicRoutingKey};

#[cfg(test)]
mod tests;
