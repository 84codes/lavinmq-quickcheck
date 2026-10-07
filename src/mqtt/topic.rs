//! MQTT topic names and topic filters (MQTT 3.1.1 §4.7).

use quickcheck::{Arbitrary, Gen};

/// Few, short levels so generated filters and topics often collide. Empty
/// levels and non-ASCII are legal; `$` only matters as the first character.
const LEVELS: &[&str] = &["a", "b", "c", "", "A", "ü", "x y", "€"];
const DOLLAR_LEVEL: &str = "$sys";

/// Does `filter` match `topic`, per MQTT 3.1.1 §4.7?
///
/// `#` also matches the parent level (`a/#` matches `a`), and a filter
/// starting with a wildcard never matches a topic starting with `$`.
pub fn matches(filter: &str, topic: &str) -> bool {
    !dollar(filter, topic) && walk(filter, topic, true)
}

/// How LavinMQ matches today (`lavinmq-quirks.md` #18): `#` needs at least
/// one more level.
pub fn lavinmq_matches(filter: &str, topic: &str) -> bool {
    !dollar(filter, topic) && walk(filter, topic, false)
}

/// A filter starting with a wildcard never matches a topic starting with
/// `$` (§4.7.2).
fn dollar(filter: &str, topic: &str) -> bool {
    topic.starts_with('$') && filter.starts_with(['#', '+'])
}

fn walk(filter: &str, topic: &str, hash_matches_parent: bool) -> bool {
    let mut topic = topic.split('/');
    for level in filter.split('/') {
        match (level, topic.next()) {
            ("#", t) => return t.is_some() || hash_matches_parent,
            (_, None) => return false,
            ("+", Some(_)) => {}
            (l, Some(t)) if l != t => return false,
            _ => {}
        }
    }
    topic.next().is_none()
}

/// Is `filter` a well-formed topic filter (§4.7.1)?
pub fn valid_filter(filter: &str) -> bool {
    let levels: Vec<&str> = filter.split('/').collect();
    let last = levels.len() - 1;
    !filter.is_empty()
        && !filter.contains('\0')
        && levels.iter().enumerate().all(|(i, l)| match *l {
            "#" => i == last,
            "+" => true,
            l => !l.contains(['#', '+']),
        })
}

/// A topic name valid for PUBLISH: non-empty, no wildcards, no NUL.
#[derive(Clone, Debug)]
pub struct TopicName(pub String);

impl Arbitrary for TopicName {
    fn arbitrary(g: &mut Gen) -> Self {
        TopicName(levels(g).join("/"))
    }
}

/// A topic and a filter derived from it by wildcarding, replacing,
/// truncating or extending levels, so it matches about half the time.
#[derive(Clone, Debug)]
pub struct Subscription {
    pub filter: String,
    pub topic: String,
}

impl Subscription {
    pub fn matches(&self) -> bool {
        matches(&self.filter, &self.topic)
    }
}

impl Arbitrary for Subscription {
    fn arbitrary(g: &mut Gen) -> Self {
        let topic = levels(g);
        let mut filter: Vec<String> = Vec::new();
        for level in &topic {
            match pick(g, 0..=9) {
                0 => {
                    filter.push("#".into());
                    break;
                }
                1 | 2 => filter.push("+".into()),
                3 => filter.push(level_word(g).into()),
                _ => filter.push(level.clone()),
            }
        }
        if filter.last().is_none_or(|l| l != "#") {
            match pick(g, 0..=5) {
                0 => filter.push("#".into()),
                1 => filter.push("+".into()),
                2 => {
                    filter.pop();
                }
                _ => {}
            }
        }
        if filter.is_empty() || filter == [""] {
            filter = vec!["#".into()];
        }
        Subscription {
            filter: filter.join("/"),
            topic: topic.join("/"),
        }
    }
}

/// A topic filter LavinMQ must reject: empty, NUL, or a wildcard that isn't
/// a whole level (`#` also only last).
#[derive(Clone, Debug)]
pub struct InvalidTopicFilter(pub String);

impl Arbitrary for InvalidTopicFilter {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut lv = levels(g);
        let i = pick(g, 0..=lv.len() - 1);
        // Non-empty, so `+` or `#` glued to it is never a whole level.
        let word = if lv[i].is_empty() { "a" } else { &lv[i] };
        let bad = match pick(g, 0..=5) {
            0 => return InvalidTopicFilter(String::new()),
            1 => format!("{word}#"),
            2 => format!("+{word}"),
            3 => format!("{word}\0"),
            4 => "##".into(),
            _ => {
                // `#` before the last level.
                lv.push(level_word(g).into());
                lv[i] = "#".into();
                return InvalidTopicFilter(lv.join("/"));
            }
        };
        lv[i] = bad;
        InvalidTopicFilter(lv.join("/"))
    }
}

/// A topic name LavinMQ must reject in PUBLISH: empty, a wildcard, or NUL.
#[derive(Clone, Debug)]
pub struct InvalidTopicName(pub String);

impl Arbitrary for InvalidTopicName {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut lv = levels(g);
        let i = pick(g, 0..=lv.len() - 1);
        let bad = *g.choose(&["", "#", "+", "\0"]).unwrap();
        if bad.is_empty() {
            return InvalidTopicName(String::new());
        }
        lv[i].push_str(bad);
        InvalidTopicName(lv.join("/"))
    }
}

/// 1–5 levels, the first sometimes `$`-prefixed, never the empty topic.
fn levels(g: &mut Gen) -> Vec<String> {
    let n = pick(g, 1..=5);
    let mut lv: Vec<String> = (0..n).map(|_| level_word(g).to_string()).collect();
    if pick(g, 0..=7) == 0 {
        lv[0] = DOLLAR_LEVEL.into();
    }
    if lv == [""] {
        lv[0] = "a".into();
    }
    lv
}

fn level_word(g: &mut Gen) -> &'static str {
    g.choose(LEVELS).unwrap()
}

fn pick(g: &mut Gen, range: std::ops::RangeInclusive<usize>) -> usize {
    *g.choose(&range.collect::<Vec<_>>()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn topic_names_are_valid(t: TopicName) -> bool {
        !t.0.is_empty() && !t.0.contains(['+', '#', '\0'])
    }

    #[quickcheck]
    fn subscriptions_have_valid_filters(s: Subscription) -> bool {
        valid_filter(&s.filter) && !s.topic.contains(['+', '#'])
    }

    #[quickcheck]
    fn invalid_filters_are_invalid(f: InvalidTopicFilter) -> bool {
        !valid_filter(&f.0)
    }

    #[quickcheck]
    fn invalid_topic_names_are_invalid(t: InvalidTopicName) -> bool {
        t.0.is_empty() || t.0.contains(['+', '#', '\0'])
    }

    /// Both outcomes must be common, or the broker tests check little.
    #[test]
    fn subscriptions_match_about_half_the_time() {
        let mut g = Gen::new(100);
        let hits = (0..1000)
            .filter(|_| Subscription::arbitrary(&mut g).matches())
            .count();
        assert!((250..=750).contains(&hits), "{hits}");
    }

    #[test]
    fn spec_examples_multi_level() {
        let f = "sport/tennis/player1/#";
        assert!(matches(f, "sport/tennis/player1"));
        assert!(matches(f, "sport/tennis/player1/ranking"));
        assert!(matches(f, "sport/tennis/player1/score/wimbledon"));
        assert!(matches("sport/#", "sport"));
        assert!(matches("#", "a/b/c"));
        assert!(!matches("sport/#", "sports"));
    }

    #[test]
    fn spec_examples_single_level() {
        assert!(matches("sport/tennis/+", "sport/tennis/player1"));
        assert!(!matches("sport/tennis/+", "sport/tennis/player1/ranking"));
        assert!(!matches("sport/+", "sport"));
        assert!(matches("sport/+", "sport/"));
        assert!(matches("+/+", "/finance"));
        assert!(matches("/+", "/finance"));
        assert!(!matches("+", "/finance"));
        assert!(matches("+/tennis/#", "sport/tennis"));
    }

    #[test]
    fn spec_examples_dollar_topics() {
        assert!(!matches("#", "$SYS/monitor/Clients"));
        assert!(!matches("+/monitor/Clients", "$SYS/monitor/Clients"));
        assert!(matches("$SYS/#", "$SYS/monitor/Clients"));
        assert!(matches("$SYS/monitor/+", "$SYS/monitor/Clients"));
    }

    #[test]
    fn lavinmq_hash_skips_parent_level() {
        assert!(!lavinmq_matches("sport/#", "sport"));
        assert!(lavinmq_matches("sport/#", "sport/"));
        assert!(lavinmq_matches("#", "sport"));
    }

    #[test]
    fn lavinmq_wildcards_skip_dollar_topics() {
        assert!(!lavinmq_matches("#", "$SYS/a"));
        assert!(!lavinmq_matches("+/a", "$SYS/a"));
        assert!(lavinmq_matches("$SYS/#", "$SYS/a"));
        assert!(lavinmq_matches("a/+", "a/$SYS"));
    }

    #[quickcheck]
    fn lavinmq_otherwise_follows_spec(s: Subscription) -> bool {
        let parent_level = s
            .filter
            .strip_suffix("/#")
            .is_some_and(|parent| matches(parent, &s.topic));
        parent_level || lavinmq_matches(&s.filter, &s.topic) == s.matches()
    }

    #[test]
    fn exact_match_is_bytewise() {
        assert!(matches("a//b", "a//b"));
        assert!(!matches("a/b", "a//b"));
        assert!(!matches("A", "a"));
    }

    #[test]
    fn valid_filters() {
        for f in ["#", "+", "a/#", "+/+", "/", "a//+", "+/tennis/#", "ü/€"] {
            assert!(valid_filter(f), "{f:?}");
        }
    }

    #[test]
    fn invalid_filters() {
        for f in ["", "a#", "a/#/b", "#/", "a+", "+a/b", "a/b+", "##", "a\0b"] {
            assert!(!valid_filter(f), "{f:?}");
        }
    }
}
