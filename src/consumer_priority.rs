//! `x-priority` consumer argument values. LavinMQ accepts any AMQP integer
//! that fits an `i32` and rejects everything else with 406.

use crate::stream_offset::IntWidth;
use lapin::types::{AMQPValue, ByteArray, DecimalValue, FieldArray, FieldTable};
use quickcheck::{Arbitrary, Gen};

/// The value of an AMQP integer field, or `None` for any other type.
pub fn as_int(v: &AMQPValue) -> Option<i64> {
    match *v {
        AMQPValue::ShortShortInt(v) => Some(v as i64),
        AMQPValue::ShortShortUInt(v) => Some(v as i64),
        AMQPValue::ShortInt(v) => Some(v as i64),
        AMQPValue::ShortUInt(v) => Some(v as i64),
        AMQPValue::LongInt(v) => Some(v as i64),
        AMQPValue::LongUInt(v) => Some(v as i64),
        AMQPValue::LongLongInt(v) => Some(v),
        _ => None,
    }
}

/// An `x-priority` LavinMQ accepts: any integer width, value within `i32`.
#[derive(Clone, Debug)]
pub struct ValidPriority(pub AMQPValue);

impl Arbitrary for ValidPriority {
    fn arbitrary(g: &mut Gen) -> Self {
        let width = *g.choose(&IntWidth::ALL).unwrap();
        let (lo, hi) = width.range();
        let (lo, hi) = (lo.max(i32::MIN as i64), hi.min(i32::MAX as i64));
        let small = (lo.max(-10)..=hi.min(10)).collect::<Vec<_>>();
        let mid = *g.choose(&small).unwrap();
        let v = *g.choose(&[lo, hi, mid]).unwrap();
        ValidPriority(width.encode(v))
    }
}

/// An `x-priority` LavinMQ must reject with 406: an integer outside `i32`,
/// or any non-integer type. `ShortString` is left out because lapin encodes
/// it as `s`, which LavinMQ reads as a 16-bit integer.
#[derive(Clone, Debug)]
pub struct InvalidPriority(pub AMQPValue);

impl Arbitrary for InvalidPriority {
    fn arbitrary(g: &mut Gen) -> Self {
        let above = i32::MAX as i64 + 1;
        let below = i32::MIN as i64 - 1;
        let v = match *g.choose(&(0..=11).collect::<Vec<_>>()).unwrap() {
            0 => AMQPValue::LongLongInt(*g.choose(&[above, i64::MAX]).unwrap()),
            1 => AMQPValue::LongLongInt(*g.choose(&[below, i64::MIN]).unwrap()),
            2 => AMQPValue::LongUInt(*g.choose(&[above as u32, u32::MAX]).unwrap()),
            3 => AMQPValue::Float(f32::arbitrary(g)),
            4 => AMQPValue::Double(f64::arbitrary(g)),
            5 => AMQPValue::LongString(String::arbitrary(g).into()),
            6 => AMQPValue::Boolean(bool::arbitrary(g)),
            7 => AMQPValue::Timestamp(u32::arbitrary(g) as u64),
            8 => AMQPValue::FieldTable(FieldTable::default()),
            9 => AMQPValue::DecimalValue(DecimalValue {
                scale: u8::arbitrary(g),
                value: u32::arbitrary(g),
            }),
            10 => AMQPValue::ByteArray(ByteArray::from(Vec::<u8>::arbitrary(g))),
            _ => g
                .choose(&[
                    AMQPValue::Void,
                    AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::LongInt(1)])),
                ])
                .unwrap()
                .clone(),
        };
        InvalidPriority(v)
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn valid_priority_fits_i32(p: ValidPriority) -> bool {
        as_int(&p.0).is_some_and(|v| i32::try_from(v).is_ok())
    }

    #[quickcheck]
    fn invalid_priority_is_never_an_in_range_int(p: InvalidPriority) -> bool {
        as_int(&p.0).is_none_or(|v| i32::try_from(v).is_err())
    }
}
