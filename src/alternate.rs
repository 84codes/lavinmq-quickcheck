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
