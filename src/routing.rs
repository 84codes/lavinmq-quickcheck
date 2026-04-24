//! Routing-graph topology model: fanout exchanges, queues with optional
//! dead-letter references, and forward-only bindings between them.
//!
//! Every topology produced by the Arbitrary impl is a DAG: nodes are
//! ordered (exchanges first, then queues) and every edge points strictly
//! later in that order. This guarantees simulation terminates and keeps
//! real LavinMQ runs from looping indefinitely.

#[derive(Clone, Debug)]
pub struct Topology {
    /// All fanout exchanges. Index 0 is the publish point.
    pub exchanges: Vec<ExchangeNode>,
    pub queues: Vec<QueueNode>,
    pub bindings: Vec<Binding>,
}

#[derive(Clone, Debug)]
pub struct ExchangeNode {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct QueueNode {
    pub name: String,
    /// Optional dead-letter exchange (as an exchange index).
    pub dlx: Option<usize>,
    pub action: QueueAction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueAction {
    /// Consumer acks the delivery; the message terminates at this queue.
    Ack,
    /// Consumer nacks with requeue=false; LavinMQ routes to the DLX, or
    /// drops the message if no DLX is set.
    Reject,
}

#[derive(Clone, Copy, Debug)]
pub enum Binding {
    ExchangeToExchange { src: usize, dst: usize },
    ExchangeToQueue { src: usize, dst: usize },
}
