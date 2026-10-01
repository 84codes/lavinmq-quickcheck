//! Headers exchange scenarios plus a model of LavinMQ's matching rules,
//! including where they differ from plain AMQP (see `lavinmq-quirks.md`).

use lapin::types::{AMQPValue, FieldTable, ShortString};

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
