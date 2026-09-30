//! Delayed-message exchange scenarios: an exchange declared either as
//! `x-delayed-message` + `x-delayed-type` or as a base type with
//! `x-delayed-exchange: true`, and `x-delay` header values that LavinMQ
//! can't read as a `u32` millisecond delay.

use crate::names::{QUEUE_NAME_CHARS, RoutingKey};
use lapin::ExchangeKind;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use quickcheck::{Arbitrary, Gen};

pub const ONE_HOUR_MS: u32 = 3_600_000;
/// Shortest exchange name whose internal queue `amq.delayed-<name>` exceeds
/// LavinMQ's 256-byte cap.
pub const MIN_LONG_NAME: usize = 245;

#[derive(Clone, Copy, Debug)]
pub enum DelayedType {
    Direct,
    Fanout,
    Topic,
}

impl DelayedType {
    fn as_str(&self) -> &'static str {
        match self {
            DelayedType::Direct => "direct",
            DelayedType::Fanout => "fanout",
            DelayedType::Topic => "topic",
        }
    }

    fn kind(&self) -> ExchangeKind {
        match self {
            DelayedType::Direct => ExchangeKind::Direct,
            DelayedType::Fanout => ExchangeKind::Fanout,
            DelayedType::Topic => ExchangeKind::Topic,
        }
    }
}

impl Arbitrary for DelayedType {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[DelayedType::Direct, DelayedType::Fanout, DelayedType::Topic])
            .unwrap()
    }
}

/// The two ways LavinMQ accepts to declare a delayed exchange.
#[derive(Clone, Copy, Debug)]
pub enum DeclareStyle {
    /// `x-delayed-message` + `x-delayed-type` (RabbitMQ plugin style).
    DelayedMessage,
    /// Base type + `x-delayed-exchange: true`.
    DelayedFlag,
}

impl Arbitrary for DeclareStyle {
    fn arbitrary(g: &mut Gen) -> Self {
        *g.choose(&[DeclareStyle::DelayedMessage, DeclareStyle::DelayedFlag])
            .unwrap()
    }
}

/// Exchange kind and arguments for declaring a delayed `ty` exchange.
pub fn declare_args(style: DeclareStyle, ty: DelayedType) -> (ExchangeKind, FieldTable) {
    let mut args = FieldTable::default();
    let kind = match style {
        DeclareStyle::DelayedMessage => {
            args.insert(
                ShortString::from("x-delayed-type"),
                AMQPValue::LongString(ty.as_str().into()),
            );
            ExchangeKind::Custom("x-delayed-message".into())
        }
        DeclareStyle::DelayedFlag => {
            args.insert(
                ShortString::from("x-delayed-exchange"),
                AMQPValue::Boolean(true),
            );
            ty.kind()
        }
    };
    (kind, args)
}

/// An `x-delay` value LavinMQ can't read as a `u32`. Read literally, each
/// would mean no delay (negative) or at least an hour.
#[derive(Clone, Debug)]
pub struct OddDelay(pub AMQPValue);

impl Arbitrary for OddDelay {
    fn arbitrary(g: &mut Gen) -> Self {
        let neg = |g: &mut Gen| -(u32::arbitrary(g) as i64) - 1;
        let hours = |g: &mut Gen| ONE_HOUR_MS as f64 * (1.0 + u16::arbitrary(g) as f64);
        let v = match *g.choose(&(0..=9).collect::<Vec<_>>()).unwrap() {
            0 => AMQPValue::ShortShortInt(neg(g).max(i8::MIN as i64) as i8),
            1 => AMQPValue::ShortInt(neg(g).max(i16::MIN as i64) as i16),
            2 => AMQPValue::LongInt(neg(g).max(i32::MIN as i64) as i32),
            3 => {
                let candidates = [
                    neg(g),
                    i64::MIN,
                    u32::MAX as i64 + 1 + u32::arbitrary(g) as i64,
                    i64::MAX,
                ];
                AMQPValue::LongLongInt(*g.choose(&candidates).unwrap())
            }
            4 => AMQPValue::Float(hours(g) as f32),
            5 => AMQPValue::Double(hours(g)),
            6 => AMQPValue::LongString(hours(g).to_string().into()),
            7 => AMQPValue::Boolean(bool::arbitrary(g)),
            8 => AMQPValue::Timestamp(u32::arbitrary(g) as u64),
            _ => {
                let mut t = FieldTable::default();
                t.insert(ShortString::from("ms"), AMQPValue::LongUInt(ONE_HOUR_MS));
                AMQPValue::FieldTable(t)
            }
        };
        OddDelay(v)
    }
}

/// Exchange name of `MIN_LONG_NAME..=255` bytes.
#[derive(Clone, Debug)]
pub struct LongExchangeName(pub String);

impl Arbitrary for LongExchangeName {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = *g
            .choose(&(MIN_LONG_NAME..=255).collect::<Vec<_>>())
            .unwrap();
        let name = (0..len)
            .map(|_| *g.choose(QUEUE_NAME_CHARS).unwrap() as char)
            .collect();
        LongExchangeName(name)
    }
}

#[derive(Clone, Debug)]
pub struct DelayedScenario {
    pub exchange: String,
    pub queue: String,
    pub ty: DelayedType,
    pub style: DeclareStyle,
    /// Used as both publish routing key and binding key.
    pub routing_key: String,
    pub delay: OddDelay,
}

impl Arbitrary for DelayedScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let suffix = u64::arbitrary(g);
        DelayedScenario {
            exchange: format!("qc_dx_{suffix:x}"),
            queue: format!("qc_dq_{suffix:x}"),
            ty: DelayedType::arbitrary(g),
            style: DeclareStyle::arbitrary(g),
            routing_key: RoutingKey::arbitrary(g).0,
            delay: OddDelay::arbitrary(g),
        }
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn odd_delay_is_never_a_valid_u32(d: OddDelay) -> bool {
        match d.0 {
            AMQPValue::ShortShortInt(v) => v < 0,
            AMQPValue::ShortInt(v) => v < 0,
            AMQPValue::LongInt(v) => v < 0,
            AMQPValue::LongLongInt(v) => v < 0 || v > u32::MAX as i64,
            AMQPValue::Float(v) => v >= ONE_HOUR_MS as f32,
            AMQPValue::Double(v) => v >= ONE_HOUR_MS as f64,
            AMQPValue::LongString(_)
            | AMQPValue::Boolean(_)
            | AMQPValue::Timestamp(_)
            | AMQPValue::FieldTable(_) => true,
            _ => false,
        }
    }

    #[quickcheck]
    fn long_exchange_name_overflows_internal_queue_name(n: LongExchangeName) -> bool {
        (MIN_LONG_NAME..=255).contains(&n.0.len())
            && n.0.bytes().all(|b| QUEUE_NAME_CHARS.contains(&b))
            && format!("amq.delayed-{}", n.0).len() > 256
    }
}
