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

/// Whether the broker under test supports QoS 2 (cloudamqp/lavinmq#2236).
/// Read from `QC_MQTT_QOS2` (`0` or `1`); unset means `0`, LavinMQ main.
pub fn qos2_from_env() -> bool {
    match std::env::var("QC_MQTT_QOS2").as_deref() {
        Err(_) | Ok("") | Ok("0") => false,
        Ok("1") => true,
        Ok(other) => panic!("QC_MQTT_QOS2 must be 0 or 1, not {other:?}"),
    }
}

impl Qos {
    /// What SUBACK grants for this requested QoS. Without QoS 2 support
    /// LavinMQ downgrades 2 to 1 (MQTT 3.1.1 §3.9.3 allows a lower grant).
    pub fn granted(self, qos2: bool) -> Qos {
        if qos2 { self } else { Qos(self.0.min(1)) }
    }

    /// The QoS a message published at `self` is delivered with to a
    /// subscription granted `granted` (§3.8.4: the minimum of the two).
    pub fn delivered(self, granted: Qos) -> Qos {
        Qos(self.0.min(granted.0))
    }

    /// How LavinMQ delivers. With QoS 2 support that's `delivered`. Without
    /// it, always at the granted QoS (`lavinmq-quirks.md` #20), so a QoS 0
    /// publish can arrive at QoS 1.
    pub fn lavinmq_delivered(self, granted: Qos, qos2: bool) -> Qos {
        if qos2 {
            self.delivered(granted)
        } else {
            granted
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants() {
        assert_eq!(Qos(0).granted(false), Qos(0));
        assert_eq!(Qos(1).granted(false), Qos(1));
        assert_eq!(Qos(2).granted(false), Qos(1));
        assert_eq!(Qos(2).granted(true), Qos(2));
    }

    #[test]
    fn lavinmq_delivers_the_granted_qos_without_qos2() {
        assert_eq!(Qos(0).lavinmq_delivered(Qos(1), false), Qos(1));
        assert_eq!(Qos(1).lavinmq_delivered(Qos(0), false), Qos(0));
        assert_eq!(Qos(2).lavinmq_delivered(Qos(1), false), Qos(1));
        assert_eq!(Qos(0).lavinmq_delivered(Qos(1), true), Qos(0));
    }

    #[test]
    fn delivers_the_minimum() {
        assert_eq!(Qos(1).delivered(Qos(0)), Qos(0));
        assert_eq!(Qos(0).delivered(Qos(1)), Qos(0));
        assert_eq!(Qos(2).delivered(Qos(1)), Qos(1));
        assert_eq!(Qos(1).delivered(Qos(1)), Qos(1));
    }
}
