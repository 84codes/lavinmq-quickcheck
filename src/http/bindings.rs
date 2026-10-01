//! Bindings: AMQP `queue.bind`/`exchange.bind` (and unbind) vs HTTP
//! `POST /bindings/{vhost}/e/{source}/{q|e}/{destination}` and `DELETE …/{props}`.

use crate::headers::{InvalidXMatch, Key, MatchMode};
use crate::http::client::{self, encode_segment};
use crate::http::diff::{Outcome, amqp_outcome, equivalent, equivalent_delete};
use crate::http::json::to_json;
use crate::names::{QueueName, RoutingKey, TopicRoutingKey};
use crate::tests::{connect, test_vhost};
use lapin::{
    Channel,
    options::{ExchangeBindOptions, ExchangeUnbindOptions, QueueBindOptions},
    types::{AMQPValue, FieldTable, ShortString},
};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use serde_json::{Value, json};

const MAX_OPS: usize = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dest {
    Queue,
    Exchange,
    /// A queue that was never declared.
    Missing,
}

#[derive(Clone, Debug)]
pub struct Bind {
    pub dest: Dest,
    pub routing_key: String,
    pub arguments: FieldTable,
}

#[derive(Clone, Debug)]
pub enum BindOp {
    Bind(Bind),
    Unbind(Bind),
}

/// A source exchange, a queue and an exchange to bind to, and the
/// bind/unbind operations to run. Routing keys and arguments come from
/// small pools so that unbinds usually hit an existing binding.
#[derive(Clone, Debug)]
pub struct BindingScenario {
    pub source: String,
    pub source_kind: &'static str,
    pub queue: String,
    pub exchange: String,
    pub ops: Vec<BindOp>,
}

fn headers_arguments(g: &mut Gen) -> FieldTable {
    let mut t = FieldTable::default();
    for _ in 0..*g.choose(&[0, 1, 2]).unwrap() {
        let key = *g.choose(&Key::ALL).unwrap();
        let value = g.choose(&crate::headers::HValue::ALL).unwrap().to_amqp();
        t.insert(ShortString::from(key.as_str()), value);
    }
    match g.choose(&[0, 1, 2]).unwrap() {
        0 => {}
        1 => {
            let mode = g.choose(&[MatchMode::All, MatchMode::Any]).unwrap();
            t.insert(
                ShortString::from("x-match"),
                AMQPValue::LongString(mode.as_str().into()),
            );
        }
        _ => {
            t.insert(ShortString::from("x-match"), InvalidXMatch::arbitrary(g).0);
        }
    }
    t
}

impl Arbitrary for BindingScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let keys = [
            String::new(),
            RoutingKey::arbitrary(g).0,
            RoutingKey::arbitrary(g).0,
            TopicRoutingKey::arbitrary(g).binding_pattern,
        ];
        let args = [
            FieldTable::default(),
            headers_arguments(g),
            headers_arguments(g),
        ];
        let len = *g.choose(&(1..=MAX_OPS).collect::<Vec<_>>()).unwrap();
        let ops = (0..len)
            .map(|_| {
                let b = Bind {
                    dest: *g
                        .choose(&[Dest::Queue, Dest::Queue, Dest::Exchange, Dest::Missing])
                        .unwrap(),
                    routing_key: g.choose(&keys).unwrap().clone(),
                    arguments: g.choose(&args).unwrap().clone(),
                };
                if bool::arbitrary(g) {
                    BindOp::Bind(b)
                } else {
                    BindOp::Unbind(b)
                }
            })
            .collect();
        BindingScenario {
            source: QueueName::arbitrary(g).0,
            source_kind: g.choose(&["direct", "fanout", "topic", "headers"]).unwrap(),
            queue: QueueName::arbitrary(g).0,
            exchange: QueueName::arbitrary(g).0,
            ops,
        }
    }
}

const MISSING: &str = "qc-missing-queue";

impl BindingScenario {
    fn dest_name(&self, dest: Dest) -> &str {
        match dest {
            Dest::Queue => &self.queue,
            Dest::Exchange => &self.exchange,
            Dest::Missing => MISSING,
        }
    }

    fn setup(&self, vhost: &str) {
        let v = encode_segment(vhost);
        let ok = [
            client::put(
                &format!("exchanges/{v}/{}", encode_segment(&self.source)),
                &json!({"type": self.source_kind}),
            ),
            client::put(
                &format!("exchanges/{v}/{}", encode_segment(&self.exchange)),
                &json!({"type": "fanout"}),
            ),
            client::put(
                &format!("queues/{v}/{}", encode_segment(&self.queue)),
                &json!({}),
            ),
        ];
        assert!(ok.iter().all(|o| *o == Outcome::Ok), "setup failed: {ok:?}");
    }

    fn teardown(&self, vhost: &str) {
        let v = encode_segment(vhost);
        let _ = client::delete(&format!("exchanges/{v}/{}", encode_segment(&self.source)));
        let _ = client::delete(&format!("exchanges/{v}/{}", encode_segment(&self.exchange)));
        let _ = client::delete(&format!("queues/{v}/{}", encode_segment(&self.queue)));
    }

    /// The source exchange's bindings, without `vhost`, sorted.
    fn bindings(&self, vhost: &str) -> Option<Vec<Value>> {
        let path = format!(
            "exchanges/{}/{}/bindings/source",
            encode_segment(vhost),
            encode_segment(&self.source)
        );
        let mut list: Vec<Value> = client::get(&path)?
            .as_array()?
            .iter()
            .map(|b| {
                let mut b = b.clone();
                b.as_object_mut()?.remove("vhost");
                Some(b)
            })
            .collect::<Option<_>>()?;
        list.sort_by_key(|b| b.to_string());
        Some(list)
    }

    fn binding_path(&self, vhost: &str, b: &Bind) -> String {
        let kind = if b.dest == Dest::Exchange { "e" } else { "q" };
        format!(
            "bindings/{}/e/{}/{kind}/{}",
            encode_segment(vhost),
            encode_segment(&self.source),
            encode_segment(self.dest_name(b.dest))
        )
    }

    fn http(&self, vhost: &str, op: &BindOp, arguments: &Value) -> Outcome {
        match op {
            BindOp::Bind(b) => client::post(
                &self.binding_path(vhost, b),
                &json!({"routing_key": b.routing_key, "arguments": arguments}),
            ),
            BindOp::Unbind(b) => {
                let path = self.binding_path(vhost, b);
                let props = client::get(&path)
                    .and_then(|list| {
                        list.as_array()?
                            .iter()
                            .find(|x| {
                                x["routing_key"] == json!(b.routing_key)
                                    && x["arguments"] == *arguments
                            })
                            .and_then(|x| x["properties_key"].as_str().map(String::from))
                    })
                    // No such binding. The bare key would match the
                    // argument-less binding with the same routing key, so
                    // only use it when the arguments are empty.
                    .unwrap_or_else(|| {
                        match (b.routing_key.as_str(), b.arguments.inner().is_empty()) {
                            ("", true) => "~".into(),
                            (rk, true) => rk.into(),
                            (rk, false) => format!("{rk}~qc-no-such-binding"),
                        }
                    });
                client::delete(&format!("{path}/{}", encode_segment(&props)))
            }
        }
    }

    async fn amqp(&self, ch: &Channel, op: &BindOp) -> Outcome {
        let (BindOp::Bind(b) | BindOp::Unbind(b)) = op;
        let (src, dst, rk, args) = (
            self.source.as_str(),
            self.dest_name(b.dest),
            b.routing_key.as_str(),
            b.arguments.clone(),
        );
        match (op, b.dest) {
            (BindOp::Bind(_), Dest::Exchange) => amqp_outcome(
                ch.exchange_bind(dst, src, rk, ExchangeBindOptions::default(), args)
                    .await,
            ),
            (BindOp::Bind(_), _) => amqp_outcome(
                ch.queue_bind(dst, src, rk, QueueBindOptions::default(), args)
                    .await,
            ),
            (BindOp::Unbind(_), Dest::Exchange) => amqp_outcome(
                ch.exchange_unbind(dst, src, rk, ExchangeUnbindOptions::default(), args)
                    .await,
            ),
            (BindOp::Unbind(_), _) => amqp_outcome(ch.queue_unbind(dst, src, rk, args).await),
        }
    }
}

/// Binding and unbinding over AMQP and over HTTP gives equivalent outcomes
/// and leaves the source exchange with the same bindings.
#[quickcheck]
fn binding_parity(s: BindingScenario) -> TestResult {
    let Some(bodies) = s
        .ops
        .iter()
        .map(|(BindOp::Bind(b) | BindOp::Unbind(b))| to_json(&b.arguments))
        .collect::<Option<Vec<_>>>()
    else {
        return TestResult::discard();
    };
    let amqp_vhost = test_vhost("binding_parity-amqp");
    let http_vhost = test_vhost("binding_parity-http");
    s.setup(&amqp_vhost);
    s.setup(&http_vhost);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let conn = rt.block_on(connect("binding_parity-amqp"));

    let mut ok = true;
    for (op, arguments) in s.ops.iter().zip(&bodies) {
        let amqp = rt.block_on(async {
            let ch = conn.create_channel().await.unwrap();
            s.amqp(&ch, op).await
        });
        let http = s.http(&http_vhost, op, arguments);
        let agree = match op {
            BindOp::Bind(_) => equivalent(amqp, http),
            BindOp::Unbind(_) => equivalent_delete(amqp, http),
        };
        if !agree {
            eprintln!("outcomes differ: amqp={amqp:?} http={http:?} op={op:?}");
            ok = false;
        }
    }

    let (amqp_state, http_state) = (s.bindings(&amqp_vhost), s.bindings(&http_vhost));
    if amqp_state != http_state {
        eprintln!("bindings differ: amqp={amqp_state:?} http={http_state:?}");
        ok = false;
    }

    s.teardown(&amqp_vhost);
    s.teardown(&http_vhost);
    let _ = rt.block_on(conn.close(200, "bye"));
    TestResult::from_bool(ok)
}

/// Routing keys built around `~`, the separator in `properties_key`.
#[derive(Clone, Debug)]
pub struct TildeKey(pub String);

impl Arbitrary for TildeKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let rk = RoutingKey::arbitrary(g).0;
        let options = [
            String::new(),
            "~".into(),
            format!("{rk}~"),
            format!("~{rk}"),
            rk,
        ];
        TildeKey(g.choose(&options).unwrap().clone())
    }
}

/// Known bug (`lavinmq-quirks.md` #10): routing keys `""` and `"~"` both
/// get `properties_key` `"~"`, so `DELETE …/{props}` can't tell them apart.
#[quickcheck]
#[ignore]
fn properties_keys_are_unique(keys: Vec<TildeKey>) -> bool {
    let v = encode_segment(&test_vhost("properties_keys_are_unique"));
    client::put(&format!("exchanges/{v}/x"), &json!({"type": "direct"}));
    client::put(&format!("queues/{v}/q"), &json!({}));
    for k in &keys {
        client::post(
            &format!("bindings/{v}/e/x/q/q"),
            &json!({"routing_key": k.0}),
        );
    }
    let list = client::get(&format!("bindings/{v}/e/x/q/q")).unwrap();
    let props: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["properties_key"].as_str().unwrap())
        .collect();
    let unique: std::collections::HashSet<_> = props.iter().collect();
    client::delete(&format!("queues/{v}/q"));
    unique.len() == props.len()
}
