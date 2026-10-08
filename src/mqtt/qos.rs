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
    /// The QoS a message published at `self` is delivered with to a
    /// subscription granted `granted` (§3.8.4: the minimum of the two).
    pub fn delivered(self, granted: Qos) -> Qos {
        Qos(self.0.min(granted.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivers_the_minimum() {
        assert_eq!(Qos(1).delivered(Qos(0)), Qos(0));
        assert_eq!(Qos(0).delivered(Qos(1)), Qos(0));
        assert_eq!(Qos(2).delivered(Qos(1)), Qos(1));
        assert_eq!(Qos(1).delivered(Qos(1)), Qos(1));
    }
}
