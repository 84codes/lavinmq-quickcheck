//! rumqttc wrapper: one MQTT connection whose event loop runs in a task
//! and forwards every incoming packet to `Conn::next`.

use super::qos::Qos;
use crate::tests::test_vhost;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS, SubscribeReasonCode};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub const PORT: u16 = 1883;
const WAIT: Duration = Duration::from_secs(2);

static NEXT_ID: AtomicU32 = AtomicU32::new(0);

/// Options for a connection to vhost `qc-<test>` with a client id that is
/// unique in this run (a reused id kicks the other connection off).
pub fn options(test: &str) -> MqttOptions {
    let mut opts = MqttOptions::new(client_id(test), "localhost", PORT);
    opts.set_credentials(format!("{}:guest", test_vhost(test)), "guest");
    opts
}

/// `<test>-<n>`, unique in this run.
pub fn client_id(test: &str) -> String {
    format!("{test}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

pub fn qos(q: Qos) -> QoS {
    match q.0 {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

pub struct Conn {
    pub client: AsyncClient,
    /// Session Present from the CONNACK.
    pub session_present: bool,
    events: mpsc::UnboundedReceiver<Packet>,
    /// Packets that arrived while waiting for an ack, for `next`.
    pending: VecDeque<Packet>,
    eventloop: JoinHandle<()>,
}

/// Connects and waits for the CONNACK.
pub async fn connect(test: &str) -> Conn {
    connect_with(options(test)).await
}

pub async fn connect_with(opts: MqttOptions) -> Conn {
    let (client, mut eventloop) = AsyncClient::new(opts, 100);
    let (tx, events) = mpsc::unbounded_channel();
    let eventloop = tokio::spawn(async move {
        // An error ends the task; polling again would reconnect.
        while let Ok(event) = eventloop.poll().await {
            if let Event::Incoming(p) = event
                && tx.send(p).is_err()
            {
                break;
            }
        }
    });
    let mut conn = Conn {
        client,
        session_present: false,
        events,
        pending: VecDeque::new(),
        eventloop,
    };
    match conn.next().await {
        Some(Packet::ConnAck(ack)) => {
            conn.session_present = ack.session_present;
            conn
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }
}

impl Conn {
    /// The next incoming packet, or `None` on timeout or a closed connection.
    pub async fn next(&mut self) -> Option<Packet> {
        match self.pending.pop_front() {
            Some(p) => Some(p),
            None => self.recv().await,
        }
    }

    async fn recv(&mut self) -> Option<Packet> {
        timeout(WAIT, self.events.recv()).await.ok().flatten()
    }

    /// Waits for the first packet `want` accepts, keeping the others for
    /// `next`.
    async fn wait_for(&mut self, want: impl Fn(&Packet) -> bool) -> Option<Packet> {
        loop {
            match self.recv().await? {
                p if want(&p) => return Some(p),
                p => self.pending.push_back(p),
            }
        }
    }

    pub async fn subscribe(&mut self, filter: &str, qos: QoS) -> Vec<SubscribeReasonCode> {
        self.client.subscribe(filter, qos).await.unwrap();
        match self.wait_for(|p| matches!(p, Packet::SubAck(_))).await {
            Some(Packet::SubAck(ack)) => ack.return_codes,
            _ => panic!("no SUBACK for {filter:?}"),
        }
    }

    /// Publishes and, at QoS 1 or 2, waits for the PUBACK or PUBCOMP, so
    /// the broker has routed the message when this returns.
    pub async fn publish(&mut self, topic: &str, qos: QoS, retain: bool, payload: Vec<u8>) {
        self.client
            .publish(topic, qos, retain, payload)
            .await
            .unwrap();
        let want = match qos {
            QoS::AtMostOnce => return,
            QoS::AtLeastOnce => |p: &Packet| matches!(p, Packet::PubAck(_)),
            QoS::ExactlyOnce => |p: &Packet| matches!(p, Packet::PubComp(_)),
        };
        if self.wait_for(want).await.is_none() {
            panic!("no ack for {qos:?} publish to {topic:?}");
        }
    }

    /// Publishes received before the first one on `topic`.
    pub async fn publishes_until(&mut self, topic: &str) -> Vec<Publish> {
        let mut got = Vec::new();
        loop {
            match self.next().await {
                Some(Packet::Publish(p)) if p.topic == topic => return got,
                Some(Packet::Publish(p)) => got.push(p),
                Some(_) => {}
                None => panic!("never received a publish on {topic:?}"),
            }
        }
    }

    /// Sends DISCONNECT and waits until the broker has closed the socket.
    pub async fn disconnect(mut self) {
        let _ = self.client.disconnect().await;
        let closed = async { while self.events.recv().await.is_some() {} };
        timeout(WAIT, closed)
            .await
            .expect("broker kept the socket open");
    }

    /// Drops the socket without a DISCONNECT, like a crashed client.
    pub fn abort(self) {
        self.eventloop.abort();
    }
}
