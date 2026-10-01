//! Headers exchange scenarios plus a model of LavinMQ's matching rules,
//! including where they differ from plain AMQP (see `lavinmq-quirks.md`).

use lapin::types::{AMQPValue, FieldTable, ShortString};
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;

pub const MAX_QUEUES: usize = 5;
pub const MAX_BINDINGS: usize = 3;
pub const MAX_MESSAGES: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchMode {
    All,
    Any,
}

impl MatchMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            MatchMode::All => "all",
            MatchMode::Any => "any",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    A,
    B,
    C,
}

impl Key {
    pub const ALL: [Key; 3] = [Key::A, Key::B, Key::C];

    fn as_str(&self) -> &'static str {
        match self {
            Key::A => "a",
            Key::B => "b",
            Key::C => "c",
        }
    }
}

/// Small value alphabet, chosen so that matches are common and the numbers
/// exercise cross-type equality. No `ShortString`: lapin tags it `s`, which
/// LavinMQ decodes as a 16-bit integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HValue {
    Str1,
    StrX,
    Int32One,
    Int64One,
    DoubleOne,
    True,
}

impl HValue {
    pub const ALL: [HValue; 6] = [
        HValue::Str1,
        HValue::StrX,
        HValue::Int32One,
        HValue::Int64One,
        HValue::DoubleOne,
        HValue::True,
    ];

    pub fn to_amqp(self) -> AMQPValue {
        match self {
            HValue::Str1 => AMQPValue::LongString("1".into()),
            HValue::StrX => AMQPValue::LongString("x".into()),
            HValue::Int32One => AMQPValue::LongInt(1),
            HValue::Int64One => AMQPValue::LongLongInt(1),
            HValue::DoubleOne => AMQPValue::Double(1.0),
            HValue::True => AMQPValue::Boolean(true),
        }
    }

    fn is_number(self) -> bool {
        matches!(
            self,
            HValue::Int32One | HValue::Int64One | HValue::DoubleOne
        )
    }

    /// LavinMQ compares decoded fields with Crystal `==`, under which
    /// numbers of different types are equal when their values are.
    fn lavinmq_eq(self, other: HValue) -> bool {
        self == other || (self.is_number() && other.is_number())
    }
}

fn to_table(pairs: &[(Key, HValue)]) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in pairs {
        t.insert(ShortString::from(k.as_str()), v.to_amqp());
    }
    t
}

#[derive(Clone, Debug)]
pub struct HBinding {
    pub queue: usize,
    pub x_match: Option<MatchMode>,
    pub pairs: Vec<(Key, HValue)>,
}

impl HBinding {
    pub fn arguments(&self) -> FieldTable {
        let mut t = to_table(&self.pairs);
        if let Some(m) = self.x_match {
            t.insert(
                ShortString::from("x-match"),
                AMQPValue::LongString(m.as_str().into()),
            );
        }
        t
    }
}

#[derive(Clone, Debug)]
pub struct HMessage {
    /// `None` = no headers property at all.
    pub headers: Option<Vec<(Key, HValue)>>,
}

impl HMessage {
    pub fn headers_table(&self) -> Option<FieldTable> {
        self.headers.as_deref().map(to_table)
    }
}

/// LavinMQ's headers-exchange match. `default` is the exchange's own
/// `x-match` argument, used by bindings that don't set one.
pub fn matches(b: &HBinding, default: Option<MatchMode>, m: &HMessage) -> bool {
    let headers = match &m.headers {
        Some(h) if !h.is_empty() => h,
        // No or empty headers: only bindings with completely empty
        // arguments match (an `x-match` entry alone counts as non-empty).
        _ => return b.x_match.is_none() && b.pairs.is_empty(),
    };
    let present =
        |&(k, v): &(Key, HValue)| headers.iter().any(|&(hk, hv)| hk == k && hv.lavinmq_eq(v));
    match b.x_match.or(default).unwrap_or(MatchMode::All) {
        MatchMode::All => b.pairs.iter().all(present),
        MatchMode::Any => b.pairs.iter().any(present),
    }
}

fn pick(g: &mut Gen, range: std::ops::RangeInclusive<usize>) -> usize {
    *g.choose(&range.collect::<Vec<_>>()).unwrap()
}

fn x_match(g: &mut Gen) -> Option<MatchMode> {
    *g.choose(&[None, Some(MatchMode::All), Some(MatchMode::Any)])
        .unwrap()
}

/// Each key at most once, each present with probability 1/2.
fn pairs(g: &mut Gen) -> Vec<(Key, HValue)> {
    let mut out = Vec::new();
    for k in Key::ALL {
        if bool::arbitrary(g) {
            out.push((k, *g.choose(&HValue::ALL).unwrap()));
        }
    }
    out
}

#[derive(Clone, Debug)]
pub struct HeadersScenario {
    pub exchange: String,
    /// The exchange's own `x-match` argument.
    pub exchange_x_match: Option<MatchMode>,
    pub queues: Vec<String>,
    pub bindings: Vec<HBinding>,
    pub messages: Vec<HMessage>,
}

impl HeadersScenario {
    /// For each message, the queues LavinMQ should route it to.
    pub fn expected(&self) -> Vec<BTreeSet<usize>> {
        self.messages
            .iter()
            .map(|m| {
                self.bindings
                    .iter()
                    .filter(|b| matches(b, self.exchange_x_match, m))
                    .map(|b| b.queue)
                    .collect()
            })
            .collect()
    }
}

impl Arbitrary for HeadersScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let suffix = u64::arbitrary(g);
        let n_queues = pick(g, 1..=MAX_QUEUES);
        let mut bindings = Vec::new();
        for queue in 0..n_queues {
            for _ in 0..pick(g, 1..=MAX_BINDINGS) {
                bindings.push(HBinding {
                    queue,
                    x_match: x_match(g),
                    pairs: pairs(g),
                });
            }
        }
        let messages = (0..pick(g, 1..=MAX_MESSAGES))
            .map(|_| HMessage {
                headers: match pick(g, 0..=3) {
                    0 => None,
                    1 => Some(Vec::new()),
                    _ => Some(pairs(g)),
                },
            })
            .collect();
        HeadersScenario {
            exchange: format!("qc_hx_{suffix:x}"),
            exchange_x_match: x_match(g),
            queues: (0..n_queues)
                .map(|i| format!("qc_hq_{suffix:x}_{i}"))
                .collect(),
            bindings,
            messages,
        }
    }
}

/// An `x-match` value LavinMQ rejects with 406: anything but the strings
/// `all` and `any`, including RabbitMQ's `all-with-x` / `any-with-x`.
#[derive(Clone, Debug)]
pub struct InvalidXMatch(pub AMQPValue);

impl Arbitrary for InvalidXMatch {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = |v: &str| AMQPValue::LongString(v.into());
        let options = [
            s("all-with-x"),
            s("any-with-x"),
            s("ALL"),
            s("Any"),
            s(""),
            s("all "),
            AMQPValue::LongInt(1),
            AMQPValue::Boolean(true),
        ];
        InvalidXMatch(g.choose(&options).unwrap().clone())
    }
}

#[cfg(test)]
mod model_tests {
    use super::*;
    use HValue::*;
    use MatchMode::*;

    fn binding(x_match: Option<MatchMode>, pairs: &[(Key, HValue)]) -> HBinding {
        HBinding {
            queue: 0,
            x_match,
            pairs: pairs.to_vec(),
        }
    }

    fn msg(pairs: &[(Key, HValue)]) -> HMessage {
        HMessage {
            headers: Some(pairs.to_vec()),
        }
    }

    const NO_HEADERS: HMessage = HMessage { headers: None };

    #[test]
    fn all_needs_every_pair() {
        let b = binding(Some(All), &[(Key::A, Str1), (Key::B, StrX)]);
        assert!(matches(&b, None, &msg(&[(Key::A, Str1), (Key::B, StrX)])));
        assert!(!matches(&b, None, &msg(&[(Key::A, Str1)])));
    }

    #[test]
    fn any_needs_one_pair() {
        let b = binding(Some(Any), &[(Key::A, Str1), (Key::B, StrX)]);
        assert!(matches(&b, None, &msg(&[(Key::B, StrX)])));
        assert!(!matches(&b, None, &msg(&[(Key::C, StrX)])));
    }

    #[test]
    fn values_must_be_equal() {
        let b = binding(Some(All), &[(Key::A, Str1)]);
        assert!(!matches(&b, None, &msg(&[(Key::A, StrX)])));
        assert!(!matches(&b, None, &msg(&[(Key::A, Int32One)])));
    }

    #[test]
    fn numbers_compare_by_value_across_types() {
        let b = binding(Some(All), &[(Key::A, Int32One)]);
        assert!(matches(&b, None, &msg(&[(Key::A, Int64One)])));
        assert!(matches(&b, None, &msg(&[(Key::A, DoubleOne)])));
        assert!(!matches(&b, None, &msg(&[(Key::A, True)])));
    }

    #[test]
    fn header_less_message_matches_only_empty_binding_args() {
        assert!(matches(&binding(None, &[]), None, &NO_HEADERS));
        assert!(matches(&binding(None, &[]), None, &msg(&[])));
        // `{x-match: all}` with no pairs: not empty args, so no match …
        assert!(!matches(&binding(Some(All), &[]), None, &NO_HEADERS));
        // … yet it matches any message that does have headers.
        assert!(matches(
            &binding(Some(All), &[]),
            None,
            &msg(&[(Key::A, True)])
        ));
    }

    #[test]
    fn binding_without_x_match_uses_exchange_default() {
        let b = binding(None, &[(Key::A, Str1), (Key::B, StrX)]);
        let m = msg(&[(Key::A, Str1)]);
        assert!(!matches(&b, None, &m));
        assert!(!matches(&b, Some(All), &m));
        assert!(matches(&b, Some(Any), &m));
    }

    #[test]
    fn empty_binding_with_any_default_skips_messages_with_headers() {
        let b = binding(None, &[]);
        assert!(matches(&b, Some(Any), &NO_HEADERS));
        assert!(!matches(&b, Some(Any), &msg(&[(Key::A, True)])));
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    fn unique_keys(pairs: &[(Key, HValue)]) -> bool {
        Key::ALL
            .iter()
            .all(|k| pairs.iter().filter(|(pk, _)| pk == k).count() <= 1)
    }

    #[quickcheck]
    fn tables_have_unique_keys(s: HeadersScenario) -> bool {
        s.bindings.iter().all(|b| unique_keys(&b.pairs))
            && s.messages
                .iter()
                .all(|m| m.headers.as_deref().is_none_or(unique_keys))
    }

    #[quickcheck]
    fn scenario_is_bounded(s: HeadersScenario) -> bool {
        (1..=MAX_QUEUES).contains(&s.queues.len())
            && (1..=MAX_MESSAGES).contains(&s.messages.len())
            && s.bindings.iter().all(|b| b.queue < s.queues.len())
            && (0..s.queues.len()).all(|q| {
                (1..=MAX_BINDINGS).contains(&s.bindings.iter().filter(|b| b.queue == q).count())
            })
    }

    #[quickcheck]
    fn invalid_x_match_is_never_all_or_any(x: InvalidXMatch) -> bool {
        !matches!(&x.0, AMQPValue::LongString(s) if s.as_bytes() == b"all" || s.as_bytes() == b"any")
    }
}
