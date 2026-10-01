//! AMQP field tables as HTTP API JSON.

use lapin::types::{AMQPValue, FieldTable};
use serde_json::Value;

/// Converts `table` to a JSON object, or `None` if a value has no
/// natural JSON form (byte arrays, timestamps, decimals, f32, ...).
pub fn to_json(table: &FieldTable) -> Option<Value> {
    table
        .inner()
        .iter()
        .map(|(k, v)| Some((k.to_string(), value(v)?)))
        .collect::<Option<serde_json::Map<_, _>>>()
        .map(Value::Object)
}

/// One AMQP value as JSON, or `None` if it has no natural JSON form.
pub fn value(v: &AMQPValue) -> Option<Value> {
    Some(match v {
        AMQPValue::Boolean(b) => Value::from(*b),
        AMQPValue::ShortShortInt(n) => Value::from(*n),
        AMQPValue::ShortShortUInt(n) => Value::from(*n),
        AMQPValue::ShortInt(n) => Value::from(*n),
        AMQPValue::ShortUInt(n) => Value::from(*n),
        AMQPValue::LongInt(n) => Value::from(*n),
        AMQPValue::LongUInt(n) => Value::from(*n),
        AMQPValue::LongLongInt(n) => Value::from(*n),
        // NaN and infinities have no JSON form. No `Float` either: the
        // HTTP API reads JSON numbers as doubles, so an f32 can't round-trip.
        AMQPValue::Double(f) => Value::Number(serde_json::Number::from_f64(*f)?),
        AMQPValue::ShortString(s) => Value::from(s.as_str()),
        AMQPValue::LongString(s) => Value::from(String::from_utf8(s.as_bytes().to_vec()).ok()?),
        AMQPValue::FieldTable(t) => to_json(t)?,
        AMQPValue::FieldArray(a) => {
            Value::Array(a.as_slice().iter().map(value).collect::<Option<_>>()?)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lapin::types::{AMQPValue, ByteArray};
    use serde_json::json;

    fn table(entries: Vec<(&str, AMQPValue)>) -> FieldTable {
        let mut t = FieldTable::default();
        for (k, v) in entries {
            t.insert(k.into(), v);
        }
        t
    }

    #[test]
    fn converts_json_native_values() {
        let t = table(vec![
            ("i8", AMQPValue::ShortShortUInt(5)),
            ("i64", AMQPValue::LongLongInt(-7)),
            ("u32", AMQPValue::LongUInt(u32::MAX)),
            ("s", AMQPValue::LongString("x".into())),
            ("b", AMQPValue::Boolean(true)),
        ]);
        assert_eq!(
            to_json(&t),
            Some(json!({"i8": 5, "i64": -7, "u32": u32::MAX, "s": "x", "b": true}))
        );
    }

    #[test]
    fn converts_doubles() {
        let t = table(vec![("d", AMQPValue::Double(1.0))]);
        assert_eq!(to_json(&t), Some(json!({"d": 1.0})));
    }

    /// JSON numbers are doubles over HTTP, so an f32 can't round-trip.
    #[test]
    fn rejects_f32() {
        let t = table(vec![("f", AMQPValue::Float(0.5))]);
        assert_eq!(to_json(&t), None);
    }

    #[test]
    fn rejects_values_without_json_form() {
        let t = table(vec![(
            "ba",
            AMQPValue::ByteArray(ByteArray::from(vec![1u8])),
        )]);
        assert_eq!(to_json(&t), None);
    }
}
