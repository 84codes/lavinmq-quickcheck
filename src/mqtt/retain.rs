//! Retained-message scenarios (MQTT 3.1.1 §3.3.1.3): a run of retained
//! publishes, then a new subscription.

use super::topic::{Subscription, TopicName};
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct RetainScenario {
    /// Retained publishes in order; an empty payload clears the topic.
    pub publishes: Vec<(String, Vec<u8>)>,
    /// Subscribed to after all publishes.
    pub filter: String,
}

impl RetainScenario {
    /// What the broker retains afterwards: the last non-empty payload per
    /// topic, unless a later empty one cleared it.
    pub fn retained(&self) -> BTreeMap<String, Vec<u8>> {
        let mut store = BTreeMap::new();
        for (topic, payload) in &self.publishes {
            if payload.is_empty() {
                store.remove(topic);
            } else {
                store.insert(topic.clone(), payload.clone());
            }
        }
        store
    }

    /// Are the filter and every topic ASCII?
    pub fn is_ascii(&self) -> bool {
        self.filter.is_ascii() && self.publishes.iter().all(|(t, _)| t.is_ascii())
    }

    /// The same scenario with `prefix/` before every topic and the filter,
    /// so cases don't see each other's retained messages. Matching is
    /// unchanged, except that no topic starts with `$` any more.
    pub fn with_prefix(&self, prefix: &str) -> RetainScenario {
        RetainScenario {
            publishes: self
                .publishes
                .iter()
                .map(|(t, p)| (format!("{prefix}/{t}"), p.clone()))
                .collect(),
            filter: format!("{prefix}/{}", self.filter),
        }
    }

    /// The retained messages a new subscription to `filter` receives.
    pub fn expected(&self, matches: fn(&str, &str) -> bool) -> BTreeMap<String, Vec<u8>> {
        let mut store = self.retained();
        store.retain(|topic, _| matches(&self.filter, topic));
        store
    }
}

/// Topics from one `Subscription` plus up to two more, reused so publishes
/// overwrite and clear each other; payloads are empty a quarter of the time.
impl Arbitrary for RetainScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = Subscription::arbitrary(g);
        let mut topics = vec![s.topic];
        for _ in 0..*g.choose(&[0, 1, 2]).unwrap() {
            topics.push(TopicName::arbitrary(g).0);
        }
        let n = *g.choose(&[1, 2, 3, 4]).unwrap();
        let publishes = (0..n)
            .map(|i| {
                let topic = g.choose(&topics).unwrap().clone();
                let payload = if *g.choose(&[0, 1, 2, 3]).unwrap() == 0 {
                    vec![]
                } else {
                    vec![i as u8 + 1]
                };
                (topic, payload)
            })
            .collect();
        RetainScenario {
            publishes,
            filter: s.filter,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::topic::matches;

    fn scenario(publishes: &[(&str, &[u8])], filter: &str) -> RetainScenario {
        RetainScenario {
            publishes: publishes
                .iter()
                .map(|(t, p)| (t.to_string(), p.to_vec()))
                .collect(),
            filter: filter.into(),
        }
    }

    /// Both outcomes must be common, or the broker test checks little.
    #[test]
    fn scenarios_often_deliver_something() {
        let mut g = Gen::new(100);
        let hits = (0..1000)
            .filter(|_| {
                !RetainScenario::arbitrary(&mut g)
                    .expected(matches)
                    .is_empty()
            })
            .count();
        assert!((250..=750).contains(&hits), "{hits}");
    }

    #[test]
    fn ascii_check_covers_filter_and_topics() {
        assert!(scenario(&[("a", b"1")], "+").is_ascii());
        assert!(!scenario(&[("a", b"1")], "€").is_ascii());
        assert!(!scenario(&[("a", b"1"), ("ü", b"")], "#").is_ascii());
    }

    #[test]
    fn last_publish_wins() {
        let s = scenario(&[("a", b"1"), ("b", b"2"), ("a", b"3")], "#");
        let want = BTreeMap::from([("a".into(), b"3".to_vec()), ("b".into(), b"2".to_vec())]);
        assert_eq!(s.retained(), want);
    }

    #[test]
    fn empty_payload_clears() {
        let s = scenario(&[("a", b"1"), ("a", b""), ("b", b"")], "#");
        assert!(s.retained().is_empty());
        let s = scenario(&[("a", b""), ("a", b"1")], "#");
        assert_eq!(s.retained().len(), 1);
    }

    #[test]
    fn expected_filters_by_match() {
        let s = scenario(&[("a/x", b"1"), ("b/x", b"2")], "a/+");
        let want = BTreeMap::from([("a/x".into(), b"1".to_vec())]);
        assert_eq!(s.expected(matches), want);
    }
}
