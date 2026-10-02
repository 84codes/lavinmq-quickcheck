//! Clean vs persistent sessions (MQTT 3.1.1 §3.1.2.4): connect, subscribe,
//! go offline while messages are published, reconnect.

use quickcheck::{Arbitrary, Gen};

#[derive(Clone, Debug)]
pub struct SessionScenario {
    /// CleanSession on the first connection, which subscribes at QoS 1.
    pub first_clean: bool,
    /// Payloads published at QoS 1 while the client is offline.
    pub offline: Vec<Vec<u8>>,
    /// CleanSession on the reconnect.
    pub second_clean: bool,
}

impl SessionScenario {
    /// Session Present in the reconnect's CONNACK (§3.2.2.2): the session
    /// survives only if neither connection asked for a clean one.
    pub fn session_present(&self) -> bool {
        !self.first_clean && !self.second_clean
    }

    /// What the reconnected client receives, in order.
    pub fn delivered(&self) -> Vec<Vec<u8>> {
        if self.session_present() {
            self.offline.clone()
        } else {
            vec![]
        }
    }
}

impl Arbitrary for SessionScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = *g.choose(&[0, 1, 2, 3]).unwrap();
        SessionScenario {
            first_clean: bool::arbitrary(g),
            offline: (0..n).map(|_| Vec::arbitrary(g)).collect(),
            second_clean: bool::arbitrary(g),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scenario(first_clean: bool, second_clean: bool) -> SessionScenario {
        SessionScenario {
            first_clean,
            offline: vec![b"1".to_vec(), b"2".to_vec()],
            second_clean,
        }
    }

    #[test]
    fn persistent_twice_keeps_queued_messages() {
        let s = scenario(false, false);
        assert!(s.session_present());
        assert_eq!(s.delivered(), [b"1".to_vec(), b"2".to_vec()]);
    }

    #[test]
    fn any_clean_connection_discards_the_session() {
        for (a, b) in [(true, true), (true, false), (false, true)] {
            let s = scenario(a, b);
            assert!(!s.session_present(), "{a} {b}");
            assert!(s.delivered().is_empty(), "{a} {b}");
        }
    }
}
