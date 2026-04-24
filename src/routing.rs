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

use std::collections::HashMap;

/// Predicts the per-queue ack count for one message published at
/// `exchanges[0]`. Keyed by queue index. Queues with zero acks are absent.
pub fn simulate(topo: &Topology) -> HashMap<usize, u32> {
    let mut delivered: HashMap<usize, u32> = HashMap::new();
    let mut stack: Vec<Node> = vec![Node::Exchange(0)];
    while let Some(node) = stack.pop() {
        match node {
            Node::Exchange(ex) => {
                for b in &topo.bindings {
                    match *b {
                        Binding::ExchangeToExchange { src, dst } if src == ex => {
                            stack.push(Node::Exchange(dst));
                        }
                        Binding::ExchangeToQueue { src, dst } if src == ex => {
                            stack.push(Node::Queue(dst));
                        }
                        _ => {}
                    }
                }
            }
            Node::Queue(q) => match topo.queues[q].action {
                QueueAction::Ack => {
                    *delivered.entry(q).or_insert(0) += 1;
                }
                QueueAction::Reject => {
                    if let Some(dlx) = topo.queues[q].dlx {
                        stack.push(Node::Exchange(dlx));
                    }
                    // else: LavinMQ drops the message; nothing to record.
                }
            },
        }
    }
    delivered
}

enum Node {
    Exchange(usize),
    Queue(usize),
}

#[cfg(test)]
mod simulator_tests {
    use super::*;
    use std::collections::HashMap;

    fn ex(name: &str) -> ExchangeNode {
        ExchangeNode { name: name.into() }
    }

    fn qn(name: &str, dlx: Option<usize>, action: QueueAction) -> QueueNode {
        QueueNode { name: name.into(), dlx, action }
    }

    #[test]
    fn empty_graph_delivers_nothing() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![],
            bindings: vec![],
        };
        assert_eq!(simulate(&topo), HashMap::new());
    }

    #[test]
    fn single_exchange_to_ack_queue() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![Binding::ExchangeToQueue { src: 0, dst: 0 }],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn fanout_to_two_queues() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![
                qn("q0", None, QueueAction::Ack),
                qn("q1", None, QueueAction::Ack),
            ],
            bindings: vec![
                Binding::ExchangeToQueue { src: 0, dst: 0 },
                Binding::ExchangeToQueue { src: 0, dst: 1 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1), (1, 1)]));
    }

    #[test]
    fn exchange_to_exchange_chain() {
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![
                Binding::ExchangeToExchange { src: 0, dst: 1 },
                Binding::ExchangeToQueue { src: 1, dst: 0 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn reject_with_dlx_routes_through_to_ack_queue() {
        // e0 → q0 (Reject, DLX=e1); e1 → q1 (Ack). Result: q1 gets 1.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![
                qn("q0", Some(1), QueueAction::Reject),
                qn("q1", None, QueueAction::Ack),
            ],
            bindings: vec![
                Binding::ExchangeToQueue { src: 0, dst: 0 },
                Binding::ExchangeToQueue { src: 1, dst: 1 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(1, 1)]));
    }

    #[test]
    fn reject_without_dlx_drops_message() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![qn("q0", None, QueueAction::Reject)],
            bindings: vec![Binding::ExchangeToQueue { src: 0, dst: 0 }],
        };
        assert_eq!(simulate(&topo), HashMap::new());
    }

    #[test]
    fn multi_path_convergence_duplicates_at_target() {
        // e0 → e1 → q0   (path A)
        // e0 → q0        (path B)
        // q0 gets 2 copies.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![
                Binding::ExchangeToExchange { src: 0, dst: 1 },
                Binding::ExchangeToQueue { src: 1, dst: 0 },
                Binding::ExchangeToQueue { src: 0, dst: 0 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 2)]));
    }
}
