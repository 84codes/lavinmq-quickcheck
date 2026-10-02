//! Hand-built MQTT 3.1.1 packets over a plain TCP socket, for input that
//! rumqttc refuses to send (wildcards in PUBLISH, bad UTF-8, bad flags…).

use super::client::{PORT, client_id};
use crate::tests::test_vhost;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);

pub const CONNACK: u8 = 0x20;
pub const PUBLISH: u8 = 0x30;
pub const PUBACK: u8 = 0x40;
pub const PUBREC: u8 = 0x50;
pub const SUBACK: u8 = 0x90;

#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// First byte of the fixed header, and the variable header + payload.
    Packet(u8, Vec<u8>),
    Closed,
    Timeout,
}

pub struct Raw(TcpStream);

impl Raw {
    pub async fn open() -> Raw {
        Raw(TcpStream::connect(("localhost", PORT)).await.unwrap())
    }

    /// Opens a socket and completes CONNECT/CONNACK for vhost `qc-<test>`.
    pub async fn connect(test: &str) -> Raw {
        let mut raw = Raw::open().await;
        raw.send(&connect_packet(test, client_id(test).as_bytes(), 4))
            .await;
        match raw.read().await {
            Read::Packet(CONNACK, body) if body == [0, 0] => raw,
            other => panic!("expected CONNACK accepted, got {other:?}"),
        }
    }

    pub async fn send(&mut self, bytes: &[u8]) {
        // The broker may already have closed the socket; read() reports it.
        let _ = self.0.write_all(bytes).await;
    }

    pub async fn read(&mut self) -> Read {
        match timeout(WAIT, self.read_packet()).await {
            Ok(Some((h, body))) => Read::Packet(h, body),
            Ok(None) => Read::Closed,
            Err(_) => Read::Timeout,
        }
    }

    async fn read_packet(&mut self) -> Option<(u8, Vec<u8>)> {
        let header = self.0.read_u8().await.ok()?;
        let (mut len, mut shift) = (0usize, 0);
        loop {
            let b = self.0.read_u8().await.ok()?;
            len |= ((b & 0x7f) as usize) << shift;
            shift += 7;
            if b & 0x80 == 0 {
                break;
            }
        }
        let mut body = vec![0; len];
        self.0.read_exact(&mut body).await.ok()?;
        Some((header, body))
    }
}

/// CONNECT with protocol name `MQTT`, clean session, keepalive 60 s, user
/// `qc-<test>:guest` / `guest`.
pub fn connect_packet(test: &str, client_id: &[u8], level: u8) -> Vec<u8> {
    let user = format!("{}:guest", test_vhost(test));
    let mut body = string(b"MQTT");
    body.extend([level, 0b1100_0010, 0, 60]);
    body.extend(string(client_id));
    body.extend(string(user.as_bytes()));
    body.extend(string(b"guest"));
    packet(0x10, &body)
}

/// PUBLISH with the given flags nibble (`dup<<3 | qos<<1 | retain`); a
/// packet id is included when the QoS bits are non-zero.
pub fn publish_packet(flags: u8, topic: &[u8], packet_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = string(topic);
    if flags & 0b0110 != 0 {
        body.extend(packet_id.to_be_bytes());
    }
    body.extend(payload);
    packet(PUBLISH | flags, &body)
}

/// SUBSCRIBE for one filter, with the given fixed-header flags (`0b0010`
/// is the only valid value).
pub fn subscribe_packet(flags: u8, packet_id: u16, filter: &[u8], qos: u8) -> Vec<u8> {
    let mut body = packet_id.to_be_bytes().to_vec();
    body.extend(string(filter));
    body.push(qos);
    packet(0x80 | flags, &body)
}

pub fn packet(first_byte: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first_byte];
    let mut len = body.len();
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        out.push(b);
        if len == 0 {
            break;
        }
    }
    out.extend(body);
    out
}

/// A UTF-8 string field: u16 length + bytes (not checked to be UTF-8).
pub fn string(s: &[u8]) -> Vec<u8> {
    let mut out = (s.len() as u16).to_be_bytes().to_vec();
    out.extend(s);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_length_encoding() {
        assert_eq!(packet(0xc0, &[]), [0xc0, 0]);
        assert_eq!(packet(0x30, &[0; 127])[..2], [0x30, 0x7f]);
        assert_eq!(packet(0x30, &[0; 128])[..3], [0x30, 0x80, 0x01]);
        assert_eq!(packet(0x30, &[0; 16_384])[..4], [0x30, 0x80, 0x80, 0x01]);
    }

    #[test]
    fn publish_has_packet_id_only_above_qos0() {
        assert_eq!(
            publish_packet(0, b"a", 7, b"x"),
            [0x30, 4, 0, 1, b'a', b'x']
        );
        assert_eq!(
            publish_packet(0b0010, b"a", 7, b""),
            [0x32, 5, 0, 1, b'a', 0, 7]
        );
    }
}
