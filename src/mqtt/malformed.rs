//! Byte-level input that a MQTT 3.1.1 server must refuse, for the raw
//! socket tests.

use super::topic::TopicName;
use quickcheck::{Arbitrary, Gen};

/// Sequences that are not well-formed UTF-8, or encode a surrogate, which
/// §1.5.3 also forbids.
const BAD_UTF8: &[&[u8]] = &[
    &[0xff],
    &[0xc3],
    &[0xc0, 0x80],
    &[0xed, 0xa0, 0x80],
    &[0xf8, 0x88, 0x80, 0x80, 0x80],
    &[0x80],
];

/// A valid topic name with ill-formed UTF-8 spliced in.
#[derive(Clone, Debug)]
pub struct BadUtf8Topic(pub Vec<u8>);

impl Arbitrary for BadUtf8Topic {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut t = TopicName::arbitrary(g).0.into_bytes();
        // Splice at a char boundary, so the bad sequence stays bad.
        let at = *g
            .choose(
                &(0..=t.len())
                    .filter(|&i| i == t.len() || t[i] & 0xc0 != 0x80)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        t.splice(at..at, g.choose(BAD_UTF8).unwrap().iter().copied());
        BadUtf8Topic(t)
    }
}

/// A CONNECT protocol level other than 4 (MQTT 3.1.1).
#[derive(Clone, Debug)]
pub struct BadProtocolLevel(pub u8);

impl Arbitrary for BadProtocolLevel {
    fn arbitrary(g: &mut Gen) -> Self {
        // Neighbours (3 = MQTT 3.1, 5 = MQTT 5) are the likeliest mix-ups.
        let level = *g.choose(&[0, 1, 2, 3, 5, 6, 0x83, 0x84, 0xff]).unwrap();
        BadProtocolLevel(level)
    }
}

/// SUBSCRIBE fixed-header flags other than the required `0010` (§3.8.1).
#[derive(Clone, Debug)]
pub struct BadSubscribeFlags(pub u8);

impl Arbitrary for BadSubscribeFlags {
    fn arbitrary(g: &mut Gen) -> Self {
        let flags: Vec<u8> = (0..16).filter(|&f| f != 0b0010).collect();
        BadSubscribeFlags(*g.choose(&flags).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn bad_utf8_is_bad(t: BadUtf8Topic) -> bool {
        std::str::from_utf8(&t.0).is_err()
    }

    #[quickcheck]
    fn bad_protocol_level_is_not_4(l: BadProtocolLevel) -> bool {
        l.0 != 4
    }

    #[quickcheck]
    fn bad_subscribe_flags_are_a_nibble_but_not_2(f: BadSubscribeFlags) -> bool {
        f.0 < 16 && f.0 != 0b0010
    }
}
