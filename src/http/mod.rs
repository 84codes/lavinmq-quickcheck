//! Differential testing: the same operation over AMQP and over the HTTP API.

pub mod bindings;
pub mod client;
pub mod diff;
pub mod exchanges;
pub mod harness;
pub mod json;
pub mod message;
pub mod messages;
pub mod queues;
pub mod validation;
