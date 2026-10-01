//! Comparing AMQP and HTTP outcomes for the same operation.

use serde_json::Value;

/// Result of one operation: success, or the AMQP reply code / HTTP status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Err(u16),
}

/// The outcome of an AMQP method call: its reply code if the broker
/// closed the channel or connection.
pub fn amqp_outcome<T>(result: Result<T, lapin::Error>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(lapin::Error::ProtocolError(e)) => Outcome::Err(e.get_id()),
        Err(e) => panic!("non-protocol AMQP error: {e:?}"),
    }
}

/// The HTTP status an AMQP reply code should surface as.
pub fn http_status_for(amqp_code: u16) -> u16 {
    match amqp_code {
        406 => 400, // PRECONDITION_FAILED -> Bad Request
        code => code,
    }
}

/// Whether an AMQP outcome and an HTTP outcome agree.
pub fn equivalent(amqp: Outcome, http: Outcome) -> bool {
    match (amqp, http) {
        (Outcome::Ok, Outcome::Ok) => true,
        (Outcome::Err(a), Outcome::Err(h)) => http_status_for(a) == h,
        _ => false,
    }
}

/// Like [`equivalent`], for deletes: AMQP deletes of something that isn't
/// there succeed (idempotent, as in RabbitMQ), HTTP answers 404.
pub fn equivalent_delete(amqp: Outcome, http: Outcome) -> bool {
    (amqp, http) == (Outcome::Ok, Outcome::Err(404)) || equivalent(amqp, http)
}

/// Keeps only the fields of a GET response that describe the declaration.
pub fn normalize(resource: &Value) -> Value {
    const KEEP: &[&str] = &[
        "name",
        "type",
        "durable",
        "auto_delete",
        "exclusive",
        "internal",
        "arguments",
        "effective_arguments",
    ];
    Value::Object(
        KEEP.iter()
            .filter_map(|&k| Some((k.to_string(), resource.get(k)?.clone())))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_reply_codes() {
        assert_eq!(http_status_for(406), 400);
        assert_eq!(http_status_for(404), 404);
        assert_eq!(http_status_for(403), 403);
    }

    #[test]
    fn equivalence() {
        assert!(equivalent(Outcome::Ok, Outcome::Ok));
        assert!(equivalent(Outcome::Err(406), Outcome::Err(400)));
        assert!(!equivalent(Outcome::Ok, Outcome::Err(400)));
        assert!(!equivalent(Outcome::Err(406), Outcome::Ok));
        assert!(!equivalent(Outcome::Err(403), Outcome::Err(400)));
    }

    #[test]
    fn delete_equivalence_allows_idempotent_amqp_delete() {
        assert!(equivalent_delete(Outcome::Ok, Outcome::Err(404)));
        assert!(equivalent_delete(Outcome::Ok, Outcome::Ok));
        assert!(equivalent_delete(Outcome::Err(406), Outcome::Err(400)));
        assert!(!equivalent_delete(Outcome::Err(404), Outcome::Ok));
        assert!(!equivalent_delete(Outcome::Ok, Outcome::Err(400)));
    }

    #[test]
    fn normalize_drops_stats_and_vhost() {
        let got = normalize(&json!({
            "name": "q", "vhost": "v", "type": "direct", "durable": true, "auto_delete": false,
            "exclusive": false, "internal": false, "messages": 3,
            "arguments": {"x-max-priority": 5},
            "effective_arguments": ["x-max-priority"],
        }));
        assert_eq!(
            got,
            json!({
                "name": "q", "type": "direct", "durable": true, "auto_delete": false,
                "exclusive": false, "internal": false,
                "arguments": {"x-max-priority": 5},
                "effective_arguments": ["x-max-priority"],
            })
        );
    }
}
