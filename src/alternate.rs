//! Alternate-exchange edge cases outside the routing-graph DAG: AE cycles
//! and AEs naming an exchange that doesn't exist.

use crate::routing::AeSpelling;
use quickcheck::{Arbitrary, Gen};

const MAX_CYCLE: usize = 5;

fn spelling(g: &mut Gen) -> AeSpelling {
    *g.choose(&[AeSpelling::XAlternate, AeSpelling::Legacy])
        .unwrap()
}

/// Fanout exchanges with no bindings, each with AE → the next, the last
/// pointing back to the first. Length 1 is an exchange whose AE is itself.
#[derive(Clone, Debug)]
pub struct AeCycle {
    pub exchanges: Vec<(String, AeSpelling)>,
}

impl Arbitrary for AeCycle {
    fn arbitrary(g: &mut Gen) -> Self {
        let suffix = u64::arbitrary(g);
        let n = *g.choose(&(1..=MAX_CYCLE).collect::<Vec<_>>()).unwrap();
        let exchanges = (0..n)
            .map(|i| (format!("qc_aec_{suffix:x}_{i}"), spelling(g)))
            .collect();
        AeCycle { exchanges }
    }
}

/// A fanout exchange with no bindings whose AE names an undeclared exchange.
#[derive(Clone, Debug)]
pub struct MissingAe {
    pub exchange: String,
    pub missing: String,
    pub spelling: AeSpelling,
}

impl Arbitrary for MissingAe {
    fn arbitrary(g: &mut Gen) -> Self {
        let suffix = u64::arbitrary(g);
        MissingAe {
            exchange: format!("qc_aem_{suffix:x}"),
            missing: format!("qc_aem_missing_{suffix:x}"),
            spelling: spelling(g),
        }
    }
}

/// An exchange with no bindings that gets an AE from a policy and, maybe,
/// a different one from its own argument (which should win).
#[derive(Clone, Debug)]
pub struct PolicyAe {
    pub exchange: String,
    pub policy: String,
    pub policy_ae: String,
    pub policy_queue: String,
    pub arg_ae: String,
    pub arg_queue: String,
    /// `None` = no AE argument, only the policy.
    pub arg_spelling: Option<AeSpelling>,
}

impl Arbitrary for PolicyAe {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = format!("{:x}", u64::arbitrary(g));
        PolicyAe {
            exchange: format!("qc_aep_{s}"),
            policy: format!("qc_aep_policy_{s}"),
            policy_ae: format!("qc_aep_pae_{s}"),
            policy_queue: format!("qc_aep_pq_{s}"),
            arg_ae: format!("qc_aep_aae_{s}"),
            arg_queue: format!("qc_aep_aq_{s}"),
            arg_spelling: bool::arbitrary(g).then(|| spelling(g)),
        }
    }
}
