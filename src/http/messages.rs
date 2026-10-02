//! Messages: AMQP `basic.publish`/`basic.get` vs HTTP
//! `POST /exchanges/{vhost}/{name}/publish` and `POST /queues/{vhost}/{name}/get`.

use crate::http::client::{self, encode_segment};
use crate::http::diff::{Outcome, equivalent};
use crate::http::message::{base64, json_safe_headers, properties_to_json};
use crate::names::{QueueName, RoutingKey};
use crate::properties::BasicPropertiesArgs;
use crate::tests::{connect, test_vhost};
use lapin::{
    BasicProperties,
    options::{BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions},
    publisher_confirm::Confirmation,
};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use serde_json::{Value, json};

/// Expirations shorter than this could expire on one side only.
const MIN_EXPIRATION_MS: u64 = 10_000;

#[derive(Clone, Debug)]
pub struct Message {
    pub exchange: String,
    pub routing_key: String,
    pub properties: BasicPropertiesArgs,
    pub payload: Vec<u8>,
    /// Whether the queue is bound, so the message routes.
    pub routed: bool,
}

impl Arbitrary for Message {
    fn arbitrary(g: &mut Gen) -> Self {
        Message {
            exchange: QueueName::arbitrary(g).0,
            routing_key: RoutingKey::arbitrary(g).0,
            properties: BasicPropertiesArgs::arbitrary(g),
            payload: Vec::arbitrary(g),
            routed: bool::arbitrary(g),
        }
    }
}

impl Message {
    /// The properties to send, or `None` if the case should be discarded:
    /// a timestamp above `i64::MAX` (quirk #12) or a short expiration.
    fn properties(&self) -> Option<BasicProperties> {
        let p = json_safe_headers(self.properties.apply());
        if p.timestamp().is_some_and(|t| t > i64::MAX as u64) {
            return None;
        }
        let expiration = p.expiration().as_ref().map(|e| e.as_str().parse::<u64>());
        if let Some(Ok(ms)) = expiration
            && ms < MIN_EXPIRATION_MS
        {
            return None;
        }
        Some(p)
    }

    fn setup(&self, vhost: &str, queues: &[&str]) {
        let v = encode_segment(vhost);
        let x = encode_segment(&self.exchange);
        let kind = if queues.len() > 1 { "fanout" } else { "direct" };
        let mut ok = vec![client::put(
            &format!("exchanges/{v}/{x}"),
            &json!({"type": kind}),
        )];
        for q in queues {
            let q = encode_segment(q);
            ok.push(client::put(&format!("queues/{v}/{q}"), &json!({})));
            if self.routed {
                ok.push(client::post(
                    &format!("bindings/{v}/e/{x}/q/{q}"),
                    &json!({"routing_key": self.routing_key}),
                ));
            }
        }
        assert!(ok.iter().all(|o| *o == Outcome::Ok), "setup failed: {ok:?}");
    }

    fn teardown(&self, vhost: &str, queues: &[&str]) {
        let v = encode_segment(vhost);
        let _ = client::delete(&format!("exchanges/{v}/{}", encode_segment(&self.exchange)));
        for q in queues {
            let _ = client::delete(&format!("queues/{v}/{}", encode_segment(q)));
        }
    }

    /// Publishes over AMQP (mandatory, confirmed). Returns whether it
    /// routed, or the reply code if the broker closed the channel.
    async fn publish_amqp(&self, ch: &lapin::Channel, props: BasicProperties) -> Result<bool, u16> {
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        let opts = BasicPublishOptions {
            mandatory: true,
            ..BasicPublishOptions::default()
        };
        let confirm = async {
            ch.basic_publish(
                &self.exchange,
                &self.routing_key,
                opts,
                &self.payload,
                props,
            )
            .await?
            .await
        };
        match confirm.await {
            Ok(c) => Ok(!matches!(
                c,
                Confirmation::Ack(Some(_)) | Confirmation::Nack(Some(_))
            )),
            Err(lapin::Error::ProtocolError(e)) => Err(e.get_id()),
            Err(e) => panic!("non-protocol AMQP error: {e:?}"),
        }
    }

    /// Publishes over HTTP. Returns whether it routed, or the status.
    fn publish_http(&self, vhost: &str, props: &Value) -> Result<bool, u16> {
        let path = format!(
            "exchanges/{}/{}/publish",
            encode_segment(vhost),
            encode_segment(&self.exchange)
        );
        let body = json!({
            "properties": props,
            "routing_key": self.routing_key,
            "payload": base64(&self.payload),
            "payload_encoding": "base64",
        });
        Ok(client::post_json(&path, &body)?["routed"] == json!(true))
    }
}

/// The next message in `queue` via HTTP `/get`, or `None` if the queue is
/// empty. Normalizes two quirks: `cluster_id` is spelled `reserved` (#11)
/// and the payload uses the standard base64 alphabet (#13).
fn get_http(vhost: &str, queue: &str) -> Option<Value> {
    let mut msg = get_http_raw(vhost, queue)?;
    let payload = msg["payload"].as_str()?.replace('-', "+").replace('_', "/");
    msg["payload"] = payload.into();
    let props = msg["properties"].as_object_mut()?;
    if let Some(v) = props.remove("reserved1") {
        props.insert("reserved".into(), v);
    }
    Some(msg)
}

/// The next message in `queue` via HTTP `/get`, as the API returns it.
fn get_http_raw(vhost: &str, queue: &str) -> Option<Value> {
    let path = format!(
        "queues/{}/{}/get",
        encode_segment(vhost),
        encode_segment(queue)
    );
    let body = json!({"count": 1, "ackmode": "get", "encoding": "base64"});
    Some(
        client::post_json(&path, &body)
            .ok()?
            .as_array()?
            .first()?
            .clone(),
    )
}

/// Publishing the same message over AMQP and over HTTP routes the same
/// way, and the queued messages read back identically.
#[quickcheck]
fn publish_parity(m: Message) -> TestResult {
    let Some(props) = m.properties() else {
        return TestResult::discard();
    };
    let json_props = properties_to_json(&props).expect("headers are JSON-safe");
    let amqp_vhost = test_vhost("publish_parity-amqp");
    let http_vhost = test_vhost("publish_parity-http");
    let queue = "q";
    m.setup(&amqp_vhost, &[queue]);
    m.setup(&http_vhost, &[queue]);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let amqp_routed = rt.block_on(async {
        let conn = connect("publish_parity-amqp").await;
        let ch = conn.create_channel().await.unwrap();
        let routed = m.publish_amqp(&ch, props).await;
        let _ = conn.close(200, "bye").await;
        routed
    });
    let http_routed = m.publish_http(&http_vhost, &json_props);
    let (amqp_msg, http_msg) = (get_http(&amqp_vhost, queue), get_http(&http_vhost, queue));

    let outcome = |r: Result<bool, u16>| r.map_or_else(Outcome::Err, |_| Outcome::Ok);
    let mut ok = equivalent(outcome(amqp_routed), outcome(http_routed));
    if amqp_routed.is_ok() && amqp_routed != http_routed {
        eprintln!("routed differs: amqp={amqp_routed:?} http={http_routed:?}");
        ok = false;
    }
    if !ok {
        eprintln!("publish outcomes differ: amqp={amqp_routed:?} http={http_routed:?}");
    }
    if amqp_msg != http_msg {
        eprintln!("message differs:\n  amqp={amqp_msg:?}\n  http={http_msg:?}");
        ok = false;
    }
    m.teardown(&amqp_vhost, &[queue]);
    m.teardown(&http_vhost, &[queue]);
    TestResult::from_bool(ok)
}

/// A message published once and read from two queues, once with AMQP
/// `basic.get` and once with HTTP `/get`, reads the same.
#[quickcheck]
fn get_parity(m: Message) -> TestResult {
    let m = Message { routed: true, ..m };
    let Some(props) = m.properties() else {
        return TestResult::discard();
    };
    let vhost = test_vhost("get_parity");
    let queues = ["via-amqp", "via-http"];
    m.setup(&vhost, &queues);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let amqp_msg = rt.block_on(async {
        let conn = connect("get_parity").await;
        let ch = conn.create_channel().await.unwrap();
        if m.publish_amqp(&ch, props).await.is_err() {
            return None;
        }
        let got = ch
            .basic_get(queues[0], BasicGetOptions { no_ack: true })
            .await
            .unwrap()
            .map(|g| {
                json!({
                    "payload_bytes": g.delivery.data.len(),
                    "redelivered": g.delivery.redelivered,
                    "exchange": g.delivery.exchange.as_str(),
                    "routing_key": g.delivery.routing_key.as_str(),
                    "message_count": g.message_count,
                    "properties": properties_to_json(&g.delivery.properties),
                    "payload": base64(&g.delivery.data),
                    "payload_encoding": "base64",
                })
            });
        let _ = conn.close(200, "bye").await;
        Some(got)
    });
    let Some(amqp_msg) = amqp_msg else {
        m.teardown(&vhost, &queues);
        return TestResult::discard();
    };
    let http_msg = get_http(&vhost, queues[1]);

    m.teardown(&vhost, &queues);
    if amqp_msg != http_msg {
        eprintln!("message differs:\n  amqp={amqp_msg:?}\n  http={http_msg:?}");
        return TestResult::failed();
    }
    TestResult::passed()
}

/// Declares `vhost`/`q` and publishes `properties` and `payload` to it over
/// HTTP through the default exchange.
fn publish_http_to_q(vhost: &str, properties: &Value, payload: &[u8]) -> Result<Value, u16> {
    let v = encode_segment(vhost);
    client::put(&format!("queues/{v}/q"), &json!({}));
    client::post_json(
        &format!("exchanges/{v}/amq.default/publish"),
        &json!({
            "properties": properties,
            "routing_key": "q",
            "payload": base64(payload),
            "payload_encoding": "base64",
        }),
    )
}

/// Known bug (`lavinmq-quirks.md` #11): `/get` spells `cluster_id`
/// `reserved1`, `/publish` only reads `reserved`, so properties read with
/// `/get` don't survive being published again.
#[quickcheck]
#[ignore]
fn get_properties_republish_unchanged(cluster_id: crate::properties::ClusterId) -> bool {
    let vhost = test_vhost("get_properties_republish_unchanged");
    let _ = publish_http_to_q(&vhost, &json!({"reserved": cluster_id.0}), b"");
    let first = get_http_raw(&vhost, "q").unwrap()["properties"].clone();
    let _ = publish_http_to_q(&vhost, &first, b"");
    let second = get_http_raw(&vhost, "q").unwrap()["properties"].clone();
    first == second
}

/// Known bug (`lavinmq-quirks.md` #12): timestamps above `i64::MAX` read
/// back negative from `/get`, and `/publish` rejects them.
#[quickcheck]
#[ignore]
fn large_timestamp_round_trips(t: u64) -> bool {
    let t = t | 1 << 63;
    let vhost = test_vhost("large_timestamp_round_trips");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let conn = connect("large_timestamp_round_trips").await;
        let ch = conn.create_channel().await.unwrap();
        client::put(&format!("queues/{}/q", encode_segment(&vhost)), &json!({}));
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        ch.basic_publish(
            "",
            "q",
            BasicPublishOptions::default(),
            b"",
            BasicProperties::default().with_timestamp(t),
        )
        .await
        .unwrap()
        .await
        .unwrap();
        let _ = conn.close(200, "bye").await;
    });
    let via_amqp = get_http_raw(&vhost, "q").unwrap()["properties"]["timestamp"] == json!(t);
    let via_http = publish_http_to_q(&vhost, &json!({"timestamp": t}), b"").is_ok();
    let _ = get_http_raw(&vhost, "q");
    via_amqp && via_http
}

/// Known bug (`lavinmq-quirks.md` #13): `/get` with `encoding: base64`
/// returns the URL-safe alphabet (`-_`), not the standard one (`+/`) that
/// `/publish` takes.
#[quickcheck]
#[ignore]
fn get_payload_is_standard_base64(payload: Vec<u8>) -> bool {
    let vhost = test_vhost("get_payload_is_standard_base64");
    let _ = publish_http_to_q(&vhost, &json!({}), &payload);
    get_http_raw(&vhost, "q").unwrap()["payload"] == json!(base64(&payload))
}
