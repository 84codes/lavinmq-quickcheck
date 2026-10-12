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
    /// Optional alternate exchange. Always points forward in exchange index.
    pub ae: Option<AlternateExchange>,
}

#[derive(Clone, Copy, Debug)]
pub struct AlternateExchange {
    /// Exchange index.
    pub target: usize,
    pub spelling: AeSpelling,
}

/// LavinMQ accepts both the `x-` argument and the legacy unprefixed one.
#[derive(Clone, Copy, Debug)]
pub enum AeSpelling {
    XAlternate,
    Legacy,
}

impl AeSpelling {
    pub fn key(&self) -> &'static str {
        match self {
            AeSpelling::XAlternate => "x-alternate-exchange",
            AeSpelling::Legacy => "alternate-exchange",
        }
    }
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
/// Within one routing pass each exchange and each queue is visited at
/// most once, so loops and diamond paths deliver one copy. Bindings are
/// walked in insertion order. An exchange hands the message to its
/// alternate exchange iff none of its own bindings match, as in RabbitMQ.
/// All exchanges here are fanouts, so that means it has no bindings; an
/// exchange-to-exchange binding counts even if that exchange routes
/// nowhere. LavinMQ before cloudamqp/lavinmq#2376 decided per routing
/// pass instead; see `lavinmq-quirks.md` #6.
///
/// Dead-letter transitions start a fresh pass at the DLX, so they can
/// reach exchanges and queues the original pass already visited.
pub fn simulate(topo: &Topology) -> HashMap<usize, u32> {
    let mut delivered: HashMap<usize, u32> = HashMap::new();
    let mut passes: Vec<usize> = vec![0];
    while let Some(start) = passes.pop() {
        let mut visited = HashSet::new();
        let mut found = Vec::new();
        visit_exchange(topo, start, &mut visited, &mut found);
        for q in found {
            match topo.queues[q].action {
                QueueAction::Ack => *delivered.entry(q).or_insert(0) += 1,
                QueueAction::Reject => {
                    // Dead-letter as a fresh pass; without a DLX LavinMQ drops it.
                    if let Some(dlx) = topo.queues[q].dlx {
                        passes.push(dlx);
                    }
                }
            }
        }
    }
    delivered
}

fn visit_exchange(
    topo: &Topology,
    ex: usize,
    visited: &mut HashSet<usize>,
    found: &mut Vec<usize>,
) {
    if !visited.insert(ex) {
        return;
    }
    let mut matched = false;
    for b in &topo.bindings {
        match *b {
            Binding::ExchangeToQueue { src, dst } if src == ex => {
                matched = true;
                if !found.contains(&dst) {
                    found.push(dst);
                }
            }
            Binding::ExchangeToExchange { src, dst } if src == ex => {
                matched = true;
                visit_exchange(topo, dst, visited, found);
            }
            _ => {}
        }
    }
    if !matched && let Some(ae) = topo.exchanges[ex].ae {
        visit_exchange(topo, ae.target, visited, found);
    }
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
                ae: (i + 1 < n_ex && bool::arbitrary(g)).then(|| AlternateExchange {
                    target: *g.choose(&((i + 1)..n_ex).collect::<Vec<_>>()).unwrap(),
                    spelling: *g
                        .choose(&[AeSpelling::XAlternate, AeSpelling::Legacy])
                        .unwrap(),
                }),
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

        // Binding order matters to LavinMQ's routing, so mix E→Q and E→E.
        for i in (1..bindings.len()).rev() {
            let j = *g.choose(&(0..=i).collect::<Vec<_>>()).unwrap();
            bindings.swap(i, j);
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
        ExchangeNode {
            name: name.into(),
            ae: None,
        }
    }

    fn ex_ae(name: &str, target: usize) -> ExchangeNode {
        ExchangeNode {
            name: name.into(),
            ae: Some(AlternateExchange {
                target,
                spelling: AeSpelling::XAlternate,
            }),
        }
    }

    fn q_ack(name: &str) -> QueueNode {
        qn(name, None, QueueAction::Ack)
    }

    fn e2q(src: usize, dst: usize) -> Binding {
        Binding::ExchangeToQueue { src, dst }
    }

    fn e2e(src: usize, dst: usize) -> Binding {
        Binding::ExchangeToExchange { src, dst }
    }

    #[test]
    fn ae_fires_when_exchange_has_no_bindings() {
        let topo = Topology {
            exchanges: vec![ex_ae("e0", 1), ex("e1")],
            queues: vec![q_ack("q0")],
            bindings: vec![e2q(1, 0)],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn ae_unused_when_exchange_routes() {
        let topo = Topology {
            exchanges: vec![ex_ae("e0", 1), ex("e1")],
            queues: vec![q_ack("q0"), q_ack("q1")],
            bindings: vec![e2q(0, 0), e2q(1, 1)],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn ae_chain_follows_each_unroutable_exchange() {
        let topo = Topology {
            exchanges: vec![ex_ae("e0", 1), ex_ae("e1", 2), ex("e2")],
            queues: vec![q_ack("q0")],
            bindings: vec![e2q(2, 0)],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    // An exchange's AE depends only on its own bindings, so sibling
    // binding order doesn't matter (LavinMQ before #2376: it did).
    #[test]
    fn sub_exchange_ae_fires_after_sibling_found_a_queue() {
        // e0 → q0, then e0 → e1; e1 has no bindings, AE → e2 → q1.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex_ae("e1", 2), ex("e2")],
            queues: vec![q_ack("q0"), q_ack("q1")],
            bindings: vec![e2q(0, 0), e2e(0, 1), e2q(2, 1)],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1), (1, 1)]));
    }

    #[test]
    fn sub_exchange_ae_fires_when_visited_before_sibling_queue() {
        // Same graph, e0 → e1 bound before e0 → q0.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex_ae("e1", 2), ex("e2")],
            queues: vec![q_ack("q0"), q_ack("q1")],
            bindings: vec![e2e(0, 1), e2q(0, 0), e2q(2, 1)],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1), (1, 1)]));
    }

    // An e2e binding counts as a match even if its subtree reaches no
    // queue, so the AE doesn't fire (LavinMQ before #2376: it did).
    #[test]
    fn ae_unused_when_e2e_subtree_reaches_no_queue() {
        // e0 (AE → e2) → e1, e1 has no bindings.
        let topo = Topology {
            exchanges: vec![ex_ae("e0", 2), ex("e1"), ex("e2")],
            queues: vec![q_ack("q0")],
            bindings: vec![e2e(0, 1), e2q(2, 0)],
        };
        assert_eq!(simulate(&topo), HashMap::new());
    }

    // A binding to an exchange already visited in this pass still matches.
    #[test]
    fn ae_unused_when_e2e_target_was_already_visited() {
        // e0 → e1 → e3 → q0 and e0 → e2 → e3; e2 has AE → e4 → q1.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1"), ex_ae("e2", 4), ex("e3"), ex("e4")],
            queues: vec![q_ack("q0"), q_ack("q1")],
            bindings: vec![
                e2e(0, 1),
                e2e(0, 2),
                e2e(1, 3),
                e2e(2, 3),
                e2q(3, 0),
                e2q(4, 1),
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn ae_to_already_visited_exchange_is_skipped() {
        // e0 → e1 (no bindings, AE → e2); e0 → e2 visited after; e2 → q0.
        // e1's AE runs first and claims e2, so q0 still gets exactly one.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex_ae("e1", 2), ex("e2")],
            queues: vec![q_ack("q0")],
            bindings: vec![e2e(0, 1), e2e(0, 2), e2q(2, 0)],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
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
    fn alternate_exchanges_point_forward(topo: Topology) -> bool {
        topo.exchanges.iter().enumerate().all(|(i, e)| {
            e.ae.is_none_or(|ae| ae.target > i && ae.target < topo.exchanges.len())
        })
    }

    #[quickcheck]
    fn simulate_terminates_on_any_generated_topology(topo: Topology) -> bool {
        // simulate() is guaranteed to return on any DAG. If it hangs,
        // either the generator has a bug or simulate() is wrong.
        let _ = simulate(&topo);
        true
    }
}
