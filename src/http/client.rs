//! Thin ureq wrapper for the LavinMQ management API.

use crate::http::diff::Outcome;
use crate::tests::{MGMT, MGMT_AUTH};
use serde_json::Value;
use ureq::http::Response;
use ureq::{Agent, Body};

/// An agent that returns 4xx/5xx responses instead of erroring on them.
fn agent() -> Agent {
    Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into()
}

fn outcome(response: Result<Response<Body>, ureq::Error>) -> Outcome {
    let status = response.expect("HTTP request failed").status().as_u16();
    if (200..300).contains(&status) {
        Outcome::Ok
    } else {
        Outcome::Err(status)
    }
}

/// PUTs `body` to `path` (relative to `/api`).
pub fn put(path: &str, body: &Value) -> Outcome {
    outcome(
        agent()
            .put(format!("{MGMT}/{path}"))
            .header("Authorization", MGMT_AUTH)
            .header("Content-Type", "application/json")
            .send(body.to_string()),
    )
}

/// POSTs `body` to `path` (relative to `/api`).
pub fn post(path: &str, body: &Value) -> Outcome {
    outcome(
        agent()
            .post(format!("{MGMT}/{path}"))
            .header("Authorization", MGMT_AUTH)
            .header("Content-Type", "application/json")
            .send(body.to_string()),
    )
}

/// POSTs `body` to `path` (relative to `/api`) and returns the JSON
/// response, or the status if it isn't 2xx.
pub fn post_json(path: &str, body: &Value) -> Result<Value, u16> {
    let mut response = agent()
        .post(format!("{MGMT}/{path}"))
        .header("Authorization", MGMT_AUTH)
        .header("Content-Type", "application/json")
        .send(body.to_string())
        .expect("HTTP request failed");
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(status);
    }
    let body = response.body_mut().read_to_string().expect("HTTP body");
    Ok(serde_json::from_str(&body).expect("JSON response"))
}

/// GETs `path` (relative to `/api`); `None` unless it answers 200.
pub fn get(path: &str) -> Option<Value> {
    let mut response = agent()
        .get(format!("{MGMT}/{path}"))
        .header("Authorization", MGMT_AUTH)
        .call()
        .expect("HTTP request failed");
    if response.status() != 200 {
        return None;
    }
    let body = response.body_mut().read_to_string().ok()?;
    serde_json::from_str(&body).ok()
}

/// DELETEs `path` (relative to `/api`).
pub fn delete(path: &str) -> Outcome {
    outcome(
        agent()
            .delete(format!("{MGMT}/{path}"))
            .header("Authorization", MGMT_AUTH)
            .call(),
    )
}

/// Percent-encodes `s` as one URL path segment. Everything outside
/// `[A-Za-z0-9_~-]` is escaped, `.` included, so `.` and `..` are not
/// treated as relative path segments.
pub fn encode_segment(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    fn decode(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    #[quickcheck]
    fn encode_segment_round_trips(s: String) -> bool {
        decode(&encode_segment(&s)) == s
    }

    #[quickcheck]
    fn encode_segment_only_emits_unreserved_or_escapes(s: String) -> bool {
        encode_segment(&s)
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_~%".contains(&b))
    }

    #[test]
    fn encode_segment_escapes_dots() {
        assert_eq!(encode_segment(".."), "%2E%2E");
    }
}
