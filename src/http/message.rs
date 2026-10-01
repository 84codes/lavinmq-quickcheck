//! AMQP message properties as HTTP API JSON.

use crate::http::json::{to_json, value};
use lapin::BasicProperties;
use lapin::types::FieldTable;
use serde_json::{Map, Value};

/// `properties` as HTTP `/publish` takes them, or `None` if a header value
/// has no JSON form. `cluster_id` is spelled `reserved`.
pub fn properties_to_json(p: &BasicProperties) -> Option<Value> {
    let headers = match p.headers() {
        Some(h) => Some(to_json(h)?),
        None => None,
    };
    let mut m = Map::new();
    let mut put = |k: &str, v: Option<Value>| {
        if let Some(v) = v {
            m.insert(k.into(), v);
        }
    };
    let s = |v: &Option<lapin::types::ShortString>| v.as_ref().map(|s| Value::from(s.as_str()));
    put("content_type", s(p.content_type()));
    put("content_encoding", s(p.content_encoding()));
    put("headers", headers);
    put("delivery_mode", p.delivery_mode().map(Value::from));
    put("priority", p.priority().map(Value::from));
    put("correlation_id", s(p.correlation_id()));
    put("reply_to", s(p.reply_to()));
    put("expiration", s(p.expiration()));
    put("message_id", s(p.message_id()));
    put("timestamp", p.timestamp().map(Value::from));
    put("type", s(p.kind()));
    put("user_id", s(p.user_id()));
    put("app_id", s(p.app_id()));
    put("reserved", s(p.cluster_id()));
    Some(Value::Object(m))
}

/// `p` without the header entries whose values have no JSON form.
pub fn json_safe_headers(p: BasicProperties) -> BasicProperties {
    let Some(headers) = p.headers().clone() else {
        return p;
    };
    let mut safe = FieldTable::default();
    for (k, v) in headers.inner() {
        if value(v).is_some() {
            safe.insert(k.clone(), v.clone());
        }
    }
    p.with_headers(safe)
}

/// Standard base64 with padding, as HTTP `/get` returns payloads.
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | (b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::types::{AMQPValue, FieldTable};
    use serde_json::json;

    fn headers(entries: Vec<(&str, AMQPValue)>) -> FieldTable {
        let mut t = FieldTable::default();
        for (k, v) in entries {
            t.insert(k.into(), v);
        }
        t
    }

    #[test]
    fn base64_rfc4648_vectors() {
        let cases = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (input, want) in cases {
            assert_eq!(base64(input.as_bytes()), want);
        }
        assert_eq!(base64(&[0xfb, 0xff]), "+/8=");
    }

    #[test]
    fn empty_properties() {
        assert_eq!(
            properties_to_json(&BasicProperties::default()),
            Some(json!({}))
        );
    }

    #[test]
    fn all_fields() {
        let p = BasicProperties::default()
            .with_content_type("t".into())
            .with_content_encoding("e".into())
            .with_headers(headers(vec![("a", AMQPValue::LongInt(1))]))
            .with_delivery_mode(2)
            .with_priority(3)
            .with_correlation_id("c".into())
            .with_reply_to("r".into())
            .with_expiration("60000".into())
            .with_message_id("m".into())
            .with_timestamp(12)
            .with_type("ty".into())
            .with_user_id("guest".into())
            .with_app_id("ap".into())
            .with_cluster_id("cl".into());
        assert_eq!(
            properties_to_json(&p),
            Some(json!({
                "content_type": "t", "content_encoding": "e", "headers": {"a": 1},
                "delivery_mode": 2, "priority": 3, "correlation_id": "c",
                "reply_to": "r", "expiration": "60000", "message_id": "m",
                "timestamp": 12, "type": "ty", "user_id": "guest", "app_id": "ap",
                "reserved": "cl",
            }))
        );
    }

    #[test]
    fn non_json_header_gives_none() {
        let p =
            BasicProperties::default().with_headers(headers(vec![("t", AMQPValue::Timestamp(1))]));
        assert_eq!(properties_to_json(&p), None);
    }

    #[test]
    fn json_safe_headers_drops_only_non_json_entries() {
        let p = BasicProperties::default().with_headers(headers(vec![
            ("t", AMQPValue::Timestamp(1)),
            ("nan", AMQPValue::Double(f64::NAN)),
            ("ok", AMQPValue::LongInt(1)),
        ]));
        assert_eq!(
            properties_to_json(&json_safe_headers(p)),
            Some(json!({"headers": {"ok": 1}}))
        );
    }
}
