use quickcheck::{Arbitrary, Gen};

const VALID_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:";
const TOPIC_WORD_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
const RESERVED_PREFIX: &str = "amq.";

#[derive(Clone, Debug)]
pub struct QueueName(pub String);

impl Arbitrary for QueueName {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();

        loop {
            let name: String = (0..len)
                .map(|_| {
                    let &byte = g.choose(VALID_CHARS).unwrap();
                    byte as char
                })
                .collect();

            if !name.starts_with(RESERVED_PREFIX) {
                return QueueName(name);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoutingKey(pub String);

impl Arbitrary for RoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();

        let key: String = (0..len)
            .map(|_| {
                let &byte = g.choose(VALID_CHARS).unwrap();
                byte as char
            })
            .collect();

        RoutingKey(key)
    }
}

#[derive(Clone, Debug)]
pub struct TopicRoutingKey {
    pub routing_key: String,
    pub binding_pattern: String,
}

impl TopicRoutingKey {
    fn gen_word(g: &mut Gen) -> String {
        let max_len = g.size().clamp(1, 20);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        (0..len)
            .map(|_| {
                let &byte = g.choose(TOPIC_WORD_CHARS).unwrap();
                byte as char
            })
            .collect()
    }
}

impl Arbitrary for TopicRoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_words = g.size().clamp(1, 10);
        let num_words = *g.choose(&(1..=max_words).collect::<Vec<_>>()).unwrap();

        let words: Vec<String> = (0..num_words).map(|_| Self::gen_word(g)).collect();
        let routing_key = words.join(".");

        let mut binding_parts: Vec<String> = Vec::new();
        let mut i = 0;
        while i < words.len() {
            let choice = *g.choose(&[0u8, 1, 2, 3]).unwrap();
            match choice {
                0 => {
                    binding_parts.push(words[i].clone());
                    i += 1;
                }
                1 => {
                    binding_parts.push("*".to_string());
                    i += 1;
                }
                2 => {
                    let remaining = words.len() - i;
                    let skip = *g.choose(&(1..=remaining).collect::<Vec<_>>()).unwrap();
                    binding_parts.push("#".to_string());
                    i += skip;
                }
                _ => {
                    binding_parts.push(words[i].clone());
                    i += 1;
                }
            }
        }

        let binding_pattern = binding_parts.join(".");

        TopicRoutingKey {
            routing_key,
            binding_pattern,
        }
    }
}

// Re-export VALID_CHARS so other modules can build exchange-name / header-name generators
// against the same alphabet. Stays crate-internal — not pub-reexported from lib.rs.
pub(crate) const QUEUE_NAME_CHARS: &[u8] = VALID_CHARS;
pub(crate) const RESERVED_QUEUE_PREFIX: &str = RESERVED_PREFIX;

/// Lengths around the 255-byte short-string limit, plus some far past it.
pub const LONG_NAME_LENGTHS: &[usize] = &[254, 255, 256, 257, 300, 1000];

/// A queue-name-alphabet string of one of [`LONG_NAME_LENGTHS`], for
/// testing length validation. Not `amq.`-prefixed.
#[derive(Clone, Debug)]
pub struct LongName(pub String);

impl Arbitrary for LongName {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = *g.choose(LONG_NAME_LENGTHS).unwrap();
        loop {
            let name: String = (0..len)
                .map(|_| *g.choose(VALID_CHARS).unwrap() as char)
                .collect();
            if !name.starts_with(RESERVED_PREFIX) {
                return LongName(name);
            }
        }
    }
}

#[cfg(test)]
mod long_name_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn long_name_has_boundary_length_and_valid_chars(n: LongName) -> bool {
        LONG_NAME_LENGTHS.contains(&n.0.len())
            && n.0.bytes().all(|b| VALID_CHARS.contains(&b))
            && !n.0.starts_with(RESERVED_PREFIX)
    }

    #[test]
    fn long_name_covers_both_sides_of_the_limit() {
        let mut g = Gen::new(100);
        let lens: std::collections::HashSet<usize> = (0..500)
            .map(|_| LongName::arbitrary(&mut g).0.len())
            .collect();
        assert!(lens.contains(&255) && lens.contains(&256));
    }
}
