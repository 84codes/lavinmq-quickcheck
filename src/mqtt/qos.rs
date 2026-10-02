//! MQTT QoS levels and how LavinMQ grants and delivers them.

use quickcheck::{Arbitrary, Gen};

/// A QoS level, 0–2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Qos(pub u8);

impl Arbitrary for Qos {
    fn arbitrary(g: &mut Gen) -> Self {
        Qos(*g.choose(&[0, 1, 2]).unwrap())
    }
}

impl Qos {
    /// What SUBACK grants for this requested QoS: LavinMQ has no QoS 2,
    /// so it downgrades to 1 (MQTT 3.1.1 §3.9.3 allows a lower grant).
    pub fn granted(self) -> Qos {
        Qos(self.0.min(1))
    }

    /// The QoS a message published at `self` is delivered with to a
    /// subscription granted `granted` (§3.8.4: the minimum of the two).
    pub fn delivered(self, granted: Qos) -> Qos {
        Qos(self.0.min(granted.0))
    }

    /// How LavinMQ delivers today (`lavinmq-quirks.md` #20): always at the
    /// granted QoS, so a QoS 0 publish can arrive at QoS 1.
    pub fn lavinmq_delivered(self, granted: Qos) -> Qos {
        granted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_at_most_one() {
        assert_eq!(Qos(0).granted(), Qos(0));
        assert_eq!(Qos(1).granted(), Qos(1));
        assert_eq!(Qos(2).granted(), Qos(1));
    }

    #[test]
    fn lavinmq_delivers_the_granted_qos() {
        assert_eq!(Qos(0).lavinmq_delivered(Qos(1)), Qos(1));
        assert_eq!(Qos(1).lavinmq_delivered(Qos(0)), Qos(0));
        assert_eq!(Qos(2).lavinmq_delivered(Qos(1)), Qos(1));
    }

    #[test]
    fn delivers_the_minimum() {
        assert_eq!(Qos(1).delivered(Qos(0)), Qos(0));
        assert_eq!(Qos(0).delivered(Qos(1)), Qos(0));
        assert_eq!(Qos(2).delivered(Qos(1)), Qos(1));
        assert_eq!(Qos(1).delivered(Qos(1)), Qos(1));
    }
}
