//! Routing-graph topology model: fanout exchanges, queues with optional
//! dead-letter references, and forward-only bindings between them.
//!
//! Every topology produced by the Arbitrary impl is a DAG over the
//! *unfolded* routing graph (bindings AND dead-letter transitions). The
//! generator maintains this by giving each queue a "cutpoint" k ∈
//! [0, n_ex]: E→Q bindings can come only from exchanges with index < k,
//! and the queue's DLX (when set) must be an exchange with index ≥ k.
//! Rejection therefore jumps strictly forward in exchange-index space,
//! so no cycle can close — simulation terminates and real LavinMQ runs
//! can't loop indefinitely.

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

use quickcheck::{Arbitrary, Gen};
use std::collections::{HashMap, HashSet};

/// Predicts the per-queue ack count for one message published at
/// `exchanges[0]`. Keyed by queue index. Queues with zero acks are absent.
///
/// LavinMQ (like RabbitMQ) implements loop detection / deduplication for
/// message routing: within a single routing traversal (a single message
/// delivery pass), each exchange and each queue is visited at most once.
/// This prevents loops and avoids duplicate deliveries through diamond paths.
///
/// Dead-letter transitions are treated as fresh message publications:
/// the dead-lettered message starts routing at the DLX with its own fresh
/// visited sets, so it can reach exchanges and queues that were already
/// visited during the original routing pass.
pub fn simulate(topo: &Topology) -> HashMap<usize, u32> {
    let mut delivered: HashMap<usize, u32> = HashMap::new();
    // Work queue of (exchange_index, already_visited_exchanges, already_visited_queues)
    // tuples representing independent message delivery events.
    let mut work: Vec<(usize, HashSet<usize>, HashSet<usize>)> =
        vec![(0, HashSet::new(), HashSet::new())];
    while let Some((start_ex, mut visited_exchanges, mut visited_queues)) = work.pop() {
        // BFS/DFS within one routing event using per-event visited sets.
        let mut stack: Vec<Node> = vec![Node::Exchange(start_ex)];
        while let Some(node) = stack.pop() {
            match node {
                Node::Exchange(ex) => {
                    if !visited_exchanges.insert(ex) {
                        continue;
                    }
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
                Node::Queue(q) => {
                    if !visited_queues.insert(q) {
                        continue;
                    }
                    match topo.queues[q].action {
                        QueueAction::Ack => {
                            *delivered.entry(q).or_insert(0) += 1;
                        }
                        QueueAction::Reject => {
                            if let Some(dlx) = topo.queues[q].dlx {
                                // Dead-letter: start a fresh routing event at the DLX.
                                work.push((dlx, HashSet::new(), HashSet::new()));
                            }
                            // else: LavinMQ drops the message; nothing to record.
                        }
                    }
                }
            }
        }
    }
    delivered
}

enum Node {
    Exchange(usize),
    Queue(usize),
}

impl Arbitrary for Topology {
    fn arbitrary(g: &mut Gen) -> Self {
        // Topologies generated here must form a DAG over the *unfolded*
        // routing graph — including dead-letter edges. Exchanges are laid
        // out in positions 0..n_ex. Each queue is assigned a "cutpoint"
        // k ∈ [0, n_ex]: it can receive E→Q bindings only from exchanges
        // with index < k, and its DLX must be an exchange with index ≥ k.
        // After a reject, the message jumps strictly forward in exchange-
        // index space, so no cycle can close.

        let n_ex = *g
            .choose(&(1..=g.size().clamp(1, 8)).collect::<Vec<_>>())
            .unwrap();
        let n_q = *g
            .choose(&(1..=g.size().clamp(1, 8)).collect::<Vec<_>>())
            .unwrap();

        let suffix: u64 = u64::arbitrary(g);

        let exchanges: Vec<ExchangeNode> = (0..n_ex)
            .map(|i| ExchangeNode {
                name: format!("qc_ex_{:x}_{}", suffix, i),
            })
            .collect();

        let mut queues: Vec<QueueNode> = Vec::with_capacity(n_q);
        let mut bindings: Vec<Binding> = Vec::new();

        for qi in 0..n_q {
            let cutpoint = *g.choose(&(0..=n_ex).collect::<Vec<_>>()).unwrap();

            // DLX: Some exchange at index ≥ cutpoint, if any such exchange
            // exists. Otherwise None.
            let dlx = if cutpoint < n_ex && bool::arbitrary(g) {
                Some(*g.choose(&(cutpoint..n_ex).collect::<Vec<_>>()).unwrap())
            } else {
                None
            };

            let action = if bool::arbitrary(g) {
                QueueAction::Ack
            } else {
                QueueAction::Reject
            };

            queues.push(QueueNode {
                name: format!("qc_q_{:x}_{}", suffix, qi),
                dlx,
                action,
            });

            // E→Q bindings: only from exchanges with index < cutpoint.
            for src in 0..cutpoint {
                if bool::arbitrary(g) {
                    bindings.push(Binding::ExchangeToQueue { src, dst: qi });
                }
            }
        }

        // E→E bindings: forward-only in exchange index.
        for src in 0..n_ex {
            for dst in (src + 1)..n_ex {
                if bool::arbitrary(g) {
                    bindings.push(Binding::ExchangeToExchange { src, dst });
                }
            }
        }

        Topology {
            exchanges,
            queues,
            bindings,
        }
    }
}

#[cfg(test)]
mod simulator_tests {
    use super::*;
    use std::collections::HashMap;

    fn ex(name: &str) -> ExchangeNode {
        ExchangeNode { name: name.into() }
    }

    fn qn(name: &str, dlx: Option<usize>, action: QueueAction) -> QueueNode {
        QueueNode {
            name: name.into(),
            dlx,
            action,
        }
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
    fn multi_path_queue_dedup_delivers_once() {
        // e0 → e1 → q0   (path A)
        // e0 → q0        (path B)
        // q0 gets 1 delivery: LavinMQ deduplicates queue deliveries so even
        // though two different exchanges have an E→Q binding to q0, it is
        // only delivered once.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![
                Binding::ExchangeToExchange { src: 0, dst: 1 },
                Binding::ExchangeToQueue { src: 1, dst: 0 },
                Binding::ExchangeToQueue { src: 0, dst: 0 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn diamond_exchange_dedup_delivers_once() {
        // e0 → e1 → e2 → q0
        // e0 → e2         (q0 gets 1, not 2: e2 visited only once)
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1"), ex("e2")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![
                Binding::ExchangeToExchange { src: 0, dst: 1 },
                Binding::ExchangeToExchange { src: 0, dst: 2 },
                Binding::ExchangeToExchange { src: 1, dst: 2 },
                Binding::ExchangeToQueue { src: 2, dst: 0 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn two_independent_queues_each_get_one() {
        // e0 → q0 and e0 → q1: two distinct queues each get one copy.
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
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn generated_topology_is_a_dag(topo: Topology) -> bool {
        // Every binding points forward in topo order, and every index is
        // in-bounds for the declared node counts.
        for b in &topo.bindings {
            match *b {
                Binding::ExchangeToExchange { src, dst } => {
                    if src >= dst {
                        return false;
                    }
                    if dst >= topo.exchanges.len() {
                        return false;
                    }
                }
                Binding::ExchangeToQueue { src, dst } => {
                    if src >= topo.exchanges.len() || dst >= topo.queues.len() {
                        return false;
                    }
                }
            }
        }
        // DLX must be in-bounds AND, crucially, must be an exchange index
        // strictly greater than every exchange that binds TO this queue.
        // Otherwise rejected messages would loop back through the queue.
        for (qi, q) in topo.queues.iter().enumerate() {
            let Some(dlx) = q.dlx else { continue };
            if dlx >= topo.exchanges.len() {
                return false;
            }
            for b in &topo.bindings {
                if let Binding::ExchangeToQueue { src, dst } = *b
                    && dst == qi
                    && src >= dlx
                {
                    return false;
                }
            }
        }
        true
    }

    #[quickcheck]
    fn simulate_terminates_on_any_generated_topology(topo: Topology) -> bool {
        // simulate() is guaranteed to return on any DAG. If it hangs,
        // either the generator has a bug or simulate() is wrong.
        let _ = simulate(&topo);
        true
    }
}
