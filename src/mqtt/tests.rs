use super::client::{client_id, connect, connect_with, options, qos};
use super::malformed::{BadProtocolLevel, BadSubscribeFlags, BadUtf8Topic};
use super::qos::Qos;
use super::raw::{
    CONNACK, PUBACK, PUBREC, Raw, Read, SUBACK, connect_packet, publish_packet, subscribe_packet,
};
use super::retain::RetainScenario;
use super::session::SessionScenario;
use super::topic::{
    InvalidTopicFilter, InvalidTopicName, Subscription, TopicName, lavinmq_matches,
};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use rumqttc::{LastWill, Packet, Publish, QoS, SubscribeReasonCode};
use std::collections::BTreeMap;
use tokio::runtime::Runtime;

/// Matches no generated filter except `#`-style ones, so it marks the end
/// of what a subscriber was sent.
const SENTINEL: &str = "qc-sentinel";

/// Subscribes to `filter`, publishes `payload` to `topic` from another
/// connection, and returns what the subscriber got on `topic`.
async fn route(test: &str, filter: &str, topic: &str, payload: &[u8]) -> Vec<Publish> {
    let mut sub = connect(test).await;
    sub.subscribe(filter, QoS::AtLeastOnce).await;
    sub.subscribe(SENTINEL, QoS::AtLeastOnce).await;
    let mut publisher = connect(test).await;
    publisher
        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
        .await;
    publisher
        .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
        .await;
    let got = sub.publishes_until(SENTINEL).await;
    sub.disconnect().await;
    publisher.disconnect().await;
    got.into_iter().filter(|p| p.topic == topic).collect()
}

#[quickcheck]
fn mqtt_wildcard_routing_matches_model(s: Subscription, payload: Vec<u8>) -> bool {
    Runtime::new().unwrap().block_on(async {
        let got = route(
            "mqtt_wildcard_routing_matches_model",
            &s.filter,
            &s.topic,
            &payload,
        )
        .await;
        if lavinmq_matches(&s.filter, &s.topic) {
            got.len() == 1 && got[0].payload == payload
        } else {
            got.is_empty()
        }
    })
}

/// `lavinmq-quirks.md` #18: `a/#` must also match `a` (MQTT 3.1.1 §4.7.1.2).
#[quickcheck]
#[ignore = "LavinMQ: # doesn't match the parent level; cloudamqp/lavinmq#2312"]
fn mqtt_hash_matches_parent_level(t: TopicName) -> bool {
    Runtime::new().unwrap().block_on(async {
        let filter = format!("{}/#", t.0);
        route("mqtt_hash_matches_parent_level", &filter, &t.0, b"x")
            .await
            .len()
            == 1
    })
}

/// `lavinmq-quirks.md` #18: a leading wildcard must not match a `$` topic
/// (MQTT 3.1.1 §4.7.2).
#[quickcheck]
#[ignore = "LavinMQ: wildcards match $ topics; cloudamqp/lavinmq#2313"]
fn mqtt_wildcards_skip_dollar_topics(t: TopicName, plus: bool) -> bool {
    Runtime::new().unwrap().block_on(async {
        let topic = format!("$sys/{}", t.0);
        let filter = if plus { "+/#" } else { "#" };
        route("mqtt_wildcards_skip_dollar_topics", filter, &topic, b"x")
            .await
            .is_empty()
    })
}

#[quickcheck]
fn mqtt_subscribe_grants_at_most_qos1(requested: Qos) -> bool {
    Runtime::new().unwrap().block_on(async {
        let mut conn = connect("mqtt_subscribe_grants_at_most_qos1").await;
        let codes = conn.subscribe("a", qos(requested)).await;
        conn.disconnect().await;
        codes == [SubscribeReasonCode::Success(qos(requested.granted()))]
    })
}

#[quickcheck]
fn mqtt_delivery_qos_is_the_minimum(requested: Qos, published: Qos) -> TestResult {
    const TEST: &str = "mqtt_delivery_qos_is_the_minimum";
    if published == Qos(2) {
        // LavinMQ can't complete a QoS 2 publish (quirk #19).
        return TestResult::discard();
    }
    Runtime::new().unwrap().block_on(async {
        let mut sub = connect(TEST).await;
        sub.subscribe("a", qos(requested)).await;
        sub.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        let mut publisher = connect(TEST).await;
        publisher
            .publish("a", qos(published), false, b"x".to_vec())
            .await;
        publisher
            .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
            .await;
        let got = sub.publishes_until(SENTINEL).await;
        sub.disconnect().await;
        publisher.disconnect().await;
        let want = qos(published.lavinmq_delivered(requested.granted()));
        TestResult::from_bool(got.len() == 1 && got[0].qos == want)
    })
}

/// `lavinmq-quirks.md` #20: delivery QoS must be the minimum of publish and
/// granted QoS (MQTT 3.1.1 §3.8.4).
#[quickcheck]
#[ignore = "LavinMQ: QoS 0 publishes are delivered at QoS 1; cloudamqp/lavinmq#2315"]
fn mqtt_qos0_publish_is_delivered_at_qos0(requested: Qos) -> bool {
    const TEST: &str = "mqtt_qos0_publish_is_delivered_at_qos0";
    Runtime::new().unwrap().block_on(async {
        let mut sub = connect(TEST).await;
        sub.subscribe("a", qos(requested)).await;
        sub.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        let mut publisher = connect(TEST).await;
        publisher
            .publish("a", QoS::AtMostOnce, false, b"x".to_vec())
            .await;
        publisher
            .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
            .await;
        let got = sub.publishes_until(SENTINEL).await;
        sub.disconnect().await;
        publisher.disconnect().await;
        got.len() == 1 && got[0].qos == QoS::AtMostOnce
    })
}

/// `lavinmq-quirks.md` #19: a QoS 2 PUBLISH must get a PUBREC
/// (MQTT 3.1.1 §4.3.3).
#[quickcheck]
#[ignore = "LavinMQ: QoS 2 PUBLISH gets PUBACK; cloudamqp/lavinmq#2314"]
fn mqtt_qos2_publish_gets_pubrec(t: TopicName, packet_id: u16) -> TestResult {
    if packet_id == 0 {
        return TestResult::discard();
    }
    Runtime::new().unwrap().block_on(async {
        let mut raw = Raw::connect("mqtt_qos2_publish_gets_pubrec").await;
        raw.send(&publish_packet(0b0100, t.0.as_bytes(), packet_id, b"x"))
            .await;
        let pubrec = Read::Packet(PUBREC, packet_id.to_be_bytes().to_vec());
        TestResult::from_bool(raw.read().await == pubrec)
    })
}

/// Publishes `s`'s retained messages, then subscribes and compares what
/// arrives with the model.
fn retained_matches_model(test: &str, s: RetainScenario) -> bool {
    // The retain store outlives a case, so each case gets its own topics.
    let s = s.with_prefix(&client_id("case"));
    Runtime::new().unwrap().block_on(async {
        let mut publisher = connect(test).await;
        for (topic, payload) in &s.publishes {
            publisher
                .publish(topic, QoS::AtLeastOnce, true, payload.clone())
                .await;
        }
        let mut sub = connect(test).await;
        sub.subscribe(&s.filter, QoS::AtLeastOnce).await;
        sub.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        publisher
            .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
            .await;
        let got = sub.publishes_until(SENTINEL).await;
        sub.disconnect().await;
        publisher.disconnect().await;
        let all_retained = got.iter().all(|p| p.retain);
        let got: BTreeMap<String, Vec<u8>> = got
            .into_iter()
            .map(|p| (p.topic, p.payload.to_vec()))
            .collect();
        all_retained && got == s.expected(lavinmq_matches)
    })
}

#[quickcheck]
fn mqtt_new_subscription_gets_retained(s: RetainScenario) -> TestResult {
    if !s.is_ascii() {
        // Non-ASCII topics break the retain store (quirk #21).
        return TestResult::discard();
    }
    TestResult::from_bool(retained_matches_model(
        "mqtt_new_subscription_gets_retained",
        s,
    ))
}

/// `lavinmq-quirks.md` #21: retained messages work on any UTF-8 topic.
#[quickcheck]
#[ignore = "LavinMQ: retain store mis-splits non-ASCII topics; cloudamqp/lavinmq#2316"]
fn mqtt_retained_non_ascii_topics(s: RetainScenario) -> bool {
    retained_matches_model("mqtt_retained_non_ascii_topics", s)
}

/// §3.3.1.3: a retained publish reaches existing subscriptions with the
/// RETAIN flag cleared.
#[quickcheck]
fn mqtt_live_delivery_clears_retain_flag(t: TopicName) -> bool {
    const TEST: &str = "mqtt_live_delivery_clears_retain_flag";
    let topic = format!("{}/{}", client_id("case"), t.0);
    Runtime::new().unwrap().block_on(async {
        let mut sub = connect(TEST).await;
        sub.subscribe(&topic, QoS::AtLeastOnce).await;
        sub.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        let mut publisher = connect(TEST).await;
        publisher
            .publish(&topic, QoS::AtLeastOnce, true, b"x".to_vec())
            .await;
        publisher
            .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
            .await;
        let got = sub.publishes_until(SENTINEL).await;
        // Clear it again.
        publisher
            .publish(&topic, QoS::AtLeastOnce, true, vec![])
            .await;
        sub.disconnect().await;
        publisher.disconnect().await;
        got.len() == 1 && !got[0].retain
    })
}

/// §3.1.2.5: the will is published iff the connection closes without a
/// DISCONNECT.
#[quickcheck]
fn mqtt_will_only_on_unclean_close(
    t: TopicName,
    payload: Vec<u8>,
    will_qos: Qos,
    graceful: bool,
) -> bool {
    const TEST: &str = "mqtt_will_only_on_unclean_close";
    let topic = format!("{}/{}", client_id("case"), t.0);
    Runtime::new().unwrap().block_on(async {
        let mut sub = connect(TEST).await;
        sub.subscribe(&topic, QoS::AtLeastOnce).await;
        sub.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        let mut opts = options(TEST);
        opts.set_last_will(LastWill::new(&topic, payload.clone(), qos(will_qos), false));
        let dying = connect_with(opts).await;
        let got = if graceful {
            // Closed by the broker, so any will is already routed.
            dying.disconnect().await;
            let mut publisher = connect(TEST).await;
            publisher
                .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
                .await;
            publisher.disconnect().await;
            sub.publishes_until(SENTINEL).await
        } else {
            dying.abort();
            match sub.next().await {
                Some(Packet::Publish(p)) => vec![p],
                _ => vec![],
            }
        };
        sub.disconnect().await;
        let wills: Vec<_> = got.iter().filter(|p| p.topic == topic).collect();
        if graceful {
            wills.is_empty()
        } else {
            wills.len() == 1 && wills[0].payload == payload && !wills[0].retain
        }
    })
}

#[quickcheck]
fn mqtt_session_persistence_matches_model(s: SessionScenario) -> bool {
    const TEST: &str = "mqtt_session_persistence_matches_model";
    let topic = client_id("case");
    let opts = options(TEST);
    let reconnect = |clean| {
        let mut o = opts.clone();
        o.set_clean_session(clean);
        connect_with(o)
    };
    Runtime::new().unwrap().block_on(async {
        let mut first = reconnect(s.first_clean).await;
        first.subscribe(&topic, QoS::AtLeastOnce).await;
        first.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        first.disconnect().await;
        let mut publisher = connect(TEST).await;
        for payload in &s.offline {
            publisher
                .publish(&topic, QoS::AtLeastOnce, false, payload.clone())
                .await;
        }
        let mut second = reconnect(s.second_clean).await;
        let present = second.session_present;
        // Resubscribing to the sentinel doesn't touch the other subscription.
        second.subscribe(SENTINEL, QoS::AtLeastOnce).await;
        publisher
            .publish(SENTINEL, QoS::AtLeastOnce, false, vec![])
            .await;
        let got = second.publishes_until(SENTINEL).await;
        second.disconnect().await;
        publisher.disconnect().await;
        // Drop any persistent session again.
        reconnect(true).await.disconnect().await;
        let got: Vec<Vec<u8>> = got.into_iter().map(|p| p.payload.to_vec()).collect();
        present == s.session_present() && got == s.delivered()
    })
}

/// Connects, sends `packet`, and checks that the broker closes the socket
/// without answering (§4.8: a protocol violation closes the connection).
fn closes_on(test: &str, packet: Vec<u8>) -> bool {
    Runtime::new().unwrap().block_on(async {
        let mut raw = Raw::connect(test).await;
        raw.send(&packet).await;
        raw.read().await == Read::Closed
    })
}

/// Control for the tests below: well-formed raw packets get normal
/// answers, so a close there is about the malformation.
#[quickcheck]
fn mqtt_raw_valid_packets_are_answered(t: TopicName, packet_id: u16) -> TestResult {
    if packet_id == 0 {
        return TestResult::discard();
    }
    Runtime::new().unwrap().block_on(async {
        let mut raw = Raw::connect("mqtt_raw_valid_packets_are_answered").await;
        let id = packet_id.to_be_bytes().to_vec();
        raw.send(&publish_packet(0b0010, t.0.as_bytes(), packet_id, b"x"))
            .await;
        let puback = raw.read().await == Read::Packet(PUBACK, id.clone());
        raw.send(&subscribe_packet(0b0010, packet_id, t.0.as_bytes(), 1))
            .await;
        let suback = raw.read().await == Read::Packet(SUBACK, [id, vec![1]].concat());
        TestResult::from_bool(puback && suback)
    })
}

/// §3.3.2.1, §4.7.3, §1.5.3: no wildcards, not empty, no U+0000.
#[quickcheck]
fn mqtt_publish_to_invalid_topic_closes(t: InvalidTopicName, qos1: bool) -> bool {
    let flags = if qos1 { 0b0010 } else { 0 };
    closes_on(
        "mqtt_publish_to_invalid_topic_closes",
        publish_packet(flags, t.0.as_bytes(), 1, b"x"),
    )
}

/// §1.5.3: ill-formed UTF-8 in a topic closes the connection.
#[quickcheck]
fn mqtt_publish_bad_utf8_topic_closes(t: BadUtf8Topic) -> bool {
    closes_on(
        "mqtt_publish_bad_utf8_topic_closes",
        publish_packet(0, &t.0, 1, b"x"),
    )
}

/// §3.3.1.2: both QoS bits set is malformed.
#[quickcheck]
fn mqtt_publish_qos3_closes(t: TopicName) -> bool {
    closes_on(
        "mqtt_publish_qos3_closes",
        publish_packet(0b0110, t.0.as_bytes(), 1, b"x"),
    )
}

/// §1.5.3 again, for a SUBSCRIBE filter.
#[quickcheck]
fn mqtt_subscribe_bad_utf8_filter_closes(t: BadUtf8Topic) -> bool {
    closes_on(
        "mqtt_subscribe_bad_utf8_filter_closes",
        subscribe_packet(0b0010, 1, &t.0, 1),
    )
}

/// §3.8.1: SUBSCRIBE's fixed-header flags must be `0010`.
#[quickcheck]
fn mqtt_subscribe_bad_flags_closes(f: BadSubscribeFlags) -> bool {
    closes_on(
        "mqtt_subscribe_bad_flags_closes",
        subscribe_packet(f.0, 1, b"a", 1),
    )
}

/// §4.7.1: a malformed filter is a protocol violation. Answering it with
/// a failure SUBACK (0x80) is also acceptable, granting it is not.
#[quickcheck]
fn mqtt_subscribe_invalid_filter_is_refused(f: InvalidTopicFilter) -> bool {
    Runtime::new().unwrap().block_on(async {
        let mut raw = Raw::connect("mqtt_subscribe_invalid_filter_is_refused").await;
        raw.send(&subscribe_packet(0b0010, 1, f.0.as_bytes(), 1))
            .await;
        match raw.read().await {
            Read::Closed => true,
            Read::Packet(SUBACK, body) => body == [0, 1, 0x80],
            _ => false,
        }
    })
}

/// §3.1.2.2: an unsupported protocol level gets CONNACK 0x01, then the
/// connection closes.
#[quickcheck]
fn mqtt_connect_bad_protocol_level_refused(l: BadProtocolLevel) -> bool {
    const TEST: &str = "mqtt_connect_bad_protocol_level_refused";
    Runtime::new().unwrap().block_on(async {
        let mut raw = Raw::open().await;
        raw.send(&connect_packet(TEST, client_id(TEST).as_bytes(), l.0))
            .await;
        raw.read().await == Read::Packet(CONNACK, vec![0, 1]) && raw.read().await == Read::Closed
    })
}

/// §3.1.0-2: a second CONNECT is a protocol violation.
#[test]
fn mqtt_second_connect_closes() {
    const TEST: &str = "mqtt_second_connect_closes";
    assert!(closes_on(
        TEST,
        connect_packet(TEST, client_id(TEST).as_bytes(), 4)
    ));
}

/// §3.1.0-1: the first packet must be CONNECT.
#[quickcheck]
fn mqtt_first_packet_must_be_connect(t: TopicName) -> bool {
    Runtime::new().unwrap().block_on(async {
        let mut raw = Raw::open().await;
        raw.send(&publish_packet(0, t.0.as_bytes(), 1, b"x")).await;
        raw.read().await == Read::Closed
    })
}
