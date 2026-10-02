//! HTTP-only input validation: inputs AMQP can't even express (short
//! strings over 255 bytes) or that only exist in JSON (wrong value types).
//! The HTTP API should reject them with 400 and create nothing.

use crate::delayed::{DeclareStyle, DelayedType, LongExchangeName, declare_args};
use crate::http::client::{self, encode_segment};
use crate::http::diff::Outcome;
use crate::http::exchanges::kind_name;
use crate::names::LongName;
use crate::tests::test_vhost;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use serde_json::{Value, json};

/// AMQP short strings are at most this many bytes.
const SHORT_STRING_MAX: usize = 255;

/// What a length-limited input should get: accepted up to the limit,
/// 400 past it.
fn expected(len: usize) -> Outcome {
    if len <= SHORT_STRING_MAX {
        Outcome::Ok
    } else {
        Outcome::Err(400)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Resource {
    Queue,
    Exchange,
}

impl Arbitrary for Resource {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[Resource::Queue, Resource::Exchange]).unwrap()
    }
}

impl Resource {
    fn path(self, vhost: &str, name: &str) -> String {
        let r = match self {
            Resource::Queue => "queues",
            Resource::Exchange => "exchanges",
        };
        format!("{r}/{}/{}", encode_segment(vhost), encode_segment(name))
    }

    fn minimal_body(self) -> Value {
        match self {
            Resource::Queue => json!({}),
            Resource::Exchange => json!({"type": "direct"}),
        }
    }
}

/// PUTs `body` and checks the outcome is `want` and that the resource
/// exists afterwards exactly when the PUT succeeded. Cleans up.
fn put_expecting(resource: Resource, vhost: &str, name: &str, body: &Value, want: Outcome) -> bool {
    let path = resource.path(vhost, name);
    let got = client::put(&path, body);
    let exists = client::get(&path).is_some();
    let _ = client::delete(&path);
    if got != want || exists != (got == Outcome::Ok) {
        eprintln!(
            "PUT {body} (name len {}): got {got:?}, exists={exists}, want {want:?}",
            name.len()
        );
        return false;
    }
    true
}

/// Queue and exchange names are accepted up to 255 bytes, rejected past it.
#[quickcheck]
fn name_length_validated(resource: Resource, name: LongName) -> bool {
    let vhost = test_vhost("name_length_validated");
    put_expecting(
        resource,
        &vhost,
        &name.0,
        &resource.minimal_body(),
        expected(name.0.len()),
    )
}

/// Known bug (`lavinmq-quirks.md` #5): over HTTP, a delayed exchange whose
/// internal queue name `amq.delayed-<name>` would exceed the limit gets 500
/// Internal Server Error instead of a 400.
#[quickcheck]
#[ignore]
fn delayed_long_name_is_bad_request(
    name: LongExchangeName,
    style: DeclareStyle,
    ty: DelayedType,
    delayed_field: bool,
) -> bool {
    let vhost = test_vhost("delayed_long_name_is_bad_request");
    let body = if delayed_field {
        // HTTP-only: the `delayed` body field instead of arguments.
        json!({"type": kind_name(ty.kind()), "delayed": true})
    } else {
        let (kind, args) = declare_args(style, ty);
        json!({"type": kind_name(kind), "arguments": crate::http::json::to_json(&args)})
    };
    put_expecting(
        Resource::Exchange,
        &vhost,
        &name.0,
        &body,
        Outcome::Err(400),
    )
}

/// Known bug (`lavinmq-quirks.md` #15): the HTTP API creates bindings with
/// routing keys over 255 bytes, which AMQP can't express and `/publish`
/// rejects, so nothing can ever route through them.
#[quickcheck]
#[ignore]
fn binding_key_length_validated(rk: LongName) -> bool {
    let v = encode_segment(&test_vhost("binding_key_length_validated"));
    client::put(&format!("exchanges/{v}/x"), &json!({"type": "direct"}));
    client::put(&format!("queues/{v}/q"), &json!({}));
    let got = client::post(
        &format!("bindings/{v}/e/x/q/q"),
        &json!({"routing_key": rk.0}),
    );
    let list = client::get(&format!("bindings/{v}/e/x/q/q")).unwrap();
    let exists = list
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["routing_key"] == json!(rk.0));
    let _ = client::delete(&format!("queues/{v}/q"));
    let want = expected(rk.0.len());
    if got != want || exists != (got == Outcome::Ok) {
        eprintln!(
            "bind rk len {}: got {got:?}, exists={exists}, want {want:?}",
            rk.0.len()
        );
        return false;
    }
    true
}

/// A short-string field of HTTP `/publish`. Header keys aren't generated:
/// see [`header_key_length_validated`].
#[derive(Clone, Copy, Debug)]
pub enum PublishField {
    RoutingKey,
    Property(&'static str),
    HeaderKey,
}

impl Arbitrary for PublishField {
    fn arbitrary(g: &mut Gen) -> Self {
        let props = [
            "content_type",
            "content_encoding",
            "correlation_id",
            "reply_to",
            "message_id",
            "type",
            "app_id",
            "reserved",
        ];
        if bool::arbitrary(g) {
            PublishField::RoutingKey
        } else {
            PublishField::Property(g.choose(&props).unwrap())
        }
    }
}

/// Publishes a message with `field` set to `value` through a fanout
/// exchange, bound to a queue if `routed`. Returns the outcome.
fn publish_with(test: &str, field: PublishField, value: &str, routed: bool) -> Outcome {
    let v = encode_segment(&test_vhost(test));
    let mut setup = vec![
        client::put(&format!("exchanges/{v}/x"), &json!({"type": "fanout"})),
        client::put(&format!("queues/{v}/q"), &json!({})),
    ];
    if routed {
        setup.push(client::post(
            &format!("bindings/{v}/e/x/q/q"),
            &json!({"routing_key": ""}),
        ));
    }
    assert!(
        setup.iter().all(|o| *o == Outcome::Ok),
        "setup failed: {setup:?}"
    );
    let mut body = json!({
        "routing_key": "k",
        "payload": "",
        "payload_encoding": "string",
        "properties": {},
    });
    match field {
        PublishField::RoutingKey => body["routing_key"] = json!(value),
        PublishField::Property(p) => body["properties"][p] = json!(value),
        PublishField::HeaderKey => body["properties"]["headers"] = json!({value: 1}),
    }
    let got = client::post_json(&format!("exchanges/{v}/x/publish"), &body)
        .map_or_else(Outcome::Err, |_| Outcome::Ok);
    let _ = client::delete(&format!("queues/{v}/q"));
    let _ = client::delete(&format!("exchanges/{v}/x"));
    got
}

/// `/publish` of a routed message accepts short-string fields up to 255
/// bytes and rejects longer ones.
#[quickcheck]
fn publish_field_length_validated(field: PublishField, value: LongName) -> bool {
    let got = publish_with("publish_field_length_validated", field, &value.0, true);
    let want = expected(value.0.len());
    if got != want {
        eprintln!(
            "{field:?} len {}: got {got:?}, want {want:?}",
            value.0.len()
        );
    }
    got == want
}

/// Known bug (`lavinmq-quirks.md` #15): `/publish` only checks short-string
/// lengths when it stores the message, so an unroutable message with
/// over-long fields gets 200 `{"routed": false}` instead of 400.
#[quickcheck]
#[ignore]
fn unroutable_publish_field_length_validated(field: PublishField, value: LongName) -> bool {
    let got = publish_with(
        "unroutable_publish_field_length_validated",
        field,
        &value.0,
        false,
    );
    let want = expected(value.0.len());
    if got != want {
        eprintln!(
            "{field:?} len {}: got {got:?}, want {want:?}",
            value.0.len()
        );
    }
    got == want
}

/// Known bug (`lavinmq-quirks.md` #17): `/publish` with a header key over
/// 255 bytes gets 500 Internal Server Error, routed or not.
#[quickcheck]
#[ignore]
fn header_key_length_validated(key: LongName, routed: bool) -> bool {
    let got = publish_with(
        "header_key_length_validated",
        PublishField::HeaderKey,
        &key.0,
        routed,
    );
    let want = expected(key.0.len());
    if got != want {
        eprintln!(
            "header key len {} (routed={routed}): got {got:?}, want {want:?}",
            key.0.len()
        );
    }
    got == want
}

/// A declare body with one field of the wrong JSON type.
#[derive(Clone, Debug)]
pub struct WrongType {
    pub resource: Resource,
    pub field: &'static str,
    pub value: Value,
}

impl Arbitrary for WrongType {
    fn arbitrary(g: &mut Gen) -> Self {
        let resource = Resource::arbitrary(g);
        let fields: &[&str] = match resource {
            Resource::Queue => &["durable", "auto_delete", "arguments"],
            Resource::Exchange => &["durable", "auto_delete", "internal", "arguments", "type"],
        };
        let field = *g.choose(fields).unwrap();
        let wrong: Vec<Value> = match field {
            "arguments" => vec![json!("x"), json!(1), json!([]), json!(true)],
            "type" => vec![json!(1), json!(true), json!([]), json!({})],
            _ => vec![json!("yes"), json!("true"), json!(1), json!([]), json!({})],
        };
        WrongType {
            resource,
            field,
            value: g.choose(&wrong).unwrap().clone(),
        }
    }
}

/// Known bug (`lavinmq-quirks.md` #16): declare bodies with a field of the
/// wrong JSON type (e.g. `"durable": "yes"`) are accepted.
#[quickcheck]
#[ignore]
fn wrong_json_types_rejected(w: WrongType) -> bool {
    let vhost = test_vhost("wrong_json_types_rejected");
    let mut body = w.resource.minimal_body();
    body[w.field] = w.value.clone();
    put_expecting(w.resource, &vhost, "wrong-type", &body, Outcome::Err(400))
}
