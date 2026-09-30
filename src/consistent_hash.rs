//! Consistent-hash exchange (`x-consistent-hash`) scenarios: an exchange
//! with a hashing algorithm, a set of queues, a sequence of weighted
//! bind/unbind ops, and a list of keys to publish.

use crate::names::RoutingKey;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use quickcheck::{Arbitrary, Gen};

/// Binding weights above this make LavinMQ allocate `weight` ring vnodes /
/// jump buckets per binding, so huge weights can exhaust broker memory.
pub const MAX_WEIGHT: u32 = 100;
pub const MAX_QUEUES: usize = 5;
const MAX_OPS: usize = 20;
const MAX_KEYS: usize = 10;

/// `x-algorithm` on an `x-consistent-hash` exchange.
#[derive(Clone, Copy, Debug)]
pub enum HashAlgorithm {
    Ring,
    Jump,
}

impl HashAlgorithm {
    pub fn insert_into(&self, table: &mut FieldTable) {
        let v = match self {
            HashAlgorithm::Ring => "ring",
            HashAlgorithm::Jump => "jump",
        };
        table.insert(
            ShortString::from("x-algorithm"),
            AMQPValue::LongString(v.into()),
        );
    }
}

impl Arbitrary for HashAlgorithm {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[HashAlgorithm::Ring, HashAlgorithm::Jump])
            .unwrap()
    }
}

/// Binding weight; sent as the binding's routing key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Weight(pub u32);

impl Weight {
    pub fn routing_key(&self) -> String {
        self.0.to_string()
    }
}

impl Arbitrary for Weight {
    fn arbitrary(g: &mut Gen) -> Self {
        Weight(*g.choose(&(0..=MAX_WEIGHT).collect::<Vec<_>>()).unwrap())
    }
}

/// One bind or unbind of `queues[queue]` to the exchange.
#[derive(Clone, Copy, Debug)]
pub enum BindOp {
    Bind { queue: usize, weight: Weight },
    Unbind { queue: usize, weight: Weight },
}

impl BindOp {
    pub fn queue(&self) -> usize {
        match *self {
            BindOp::Bind { queue, .. } | BindOp::Unbind { queue, .. } => queue,
        }
    }

    pub fn weight(&self) -> Weight {
        match *self {
            BindOp::Bind { weight, .. } | BindOp::Unbind { weight, .. } => weight,
        }
    }
}

/// A key to publish. `routing_key` is hashed by default.
#[derive(Clone, Debug)]
pub struct Key {
    pub routing_key: String,
}

impl Arbitrary for Key {
    fn arbitrary(g: &mut Gen) -> Self {
        Key {
            routing_key: RoutingKey::arbitrary(g).0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConsistentHashScenario {
    pub exchange: String,
    pub algorithm: HashAlgorithm,
    pub queues: Vec<String>,
    pub ops: Vec<BindOp>,
    pub keys: Vec<Key>,
}

fn pick(g: &mut Gen, range: std::ops::RangeInclusive<usize>) -> usize {
    *g.choose(&range.collect::<Vec<_>>()).unwrap()
}

impl Arbitrary for ConsistentHashScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let suffix = u64::arbitrary(g);
        let n_queues = pick(g, 1..=MAX_QUEUES);
        let queues = (0..n_queues)
            .map(|i| format!("qc_chq_{suffix:x}_{i}"))
            .collect();

        // Unbinds only ever remove a binding that is live at that point.
        let mut live: Vec<(usize, Weight)> = Vec::new();
        let mut ops = Vec::new();
        for _ in 0..pick(g, 0..=MAX_OPS) {
            if !live.is_empty() && pick(g, 0..=2) == 0 {
                let (queue, weight) = live.swap_remove(pick(g, 0..=live.len() - 1));
                ops.push(BindOp::Unbind { queue, weight });
            } else {
                let queue = pick(g, 0..=n_queues - 1);
                let weight = Weight::arbitrary(g);
                if !live.contains(&(queue, weight)) {
                    live.push((queue, weight));
                }
                ops.push(BindOp::Bind { queue, weight });
            }
        }

        let keys = (0..pick(g, 1..=MAX_KEYS))
            .map(|_| Key::arbitrary(g))
            .collect();

        ConsistentHashScenario {
            exchange: format!("qc_chx_{suffix:x}"),
            algorithm: HashAlgorithm::arbitrary(g),
            queues,
            ops,
            keys,
        }
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;
    use std::collections::HashSet;

    #[quickcheck]
    fn weights_are_capped(s: ConsistentHashScenario) -> bool {
        s.ops.iter().all(|op| op.weight().0 <= MAX_WEIGHT)
    }

    #[quickcheck]
    fn ops_reference_declared_queues(s: ConsistentHashScenario) -> bool {
        (1..=MAX_QUEUES).contains(&s.queues.len())
            && s.ops.iter().all(|op| op.queue() < s.queues.len())
    }

    #[quickcheck]
    fn unbinds_target_live_bindings(s: ConsistentHashScenario) -> bool {
        let mut live: HashSet<(usize, u32)> = HashSet::new();
        for op in &s.ops {
            match *op {
                BindOp::Bind { queue, weight } => {
                    live.insert((queue, weight.0));
                }
                BindOp::Unbind { queue, weight } => {
                    if !live.remove(&(queue, weight.0)) {
                        return false;
                    }
                }
            }
        }
        true
    }

    #[quickcheck]
    fn queue_names_are_unique(s: ConsistentHashScenario) -> bool {
        let names: HashSet<&String> = s.queues.iter().collect();
        names.len() == s.queues.len() && !s.exchange.is_empty() && !s.keys.is_empty()
    }
}
