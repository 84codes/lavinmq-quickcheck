//! `x-stream-offset` consumer argument for stream queues, plus a model of
//! which pre-published messages a consumer starting at that offset gets.
//!
//! LavinMQ numbers stream messages from 1: an empty stream has
//! `last_offset = 0` and the first message gets offset 1.

use lapin::types::AMQPValue;
use quickcheck::{Arbitrary, Gen};
use std::ops::Range;

pub const MAX_MESSAGES: usize = 50;

/// AMQP field-table integer encoding for an `x-stream-offset` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntWidth {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
}

impl IntWidth {
    pub const ALL: [IntWidth; 7] = [
        IntWidth::I8,
        IntWidth::U8,
        IntWidth::I16,
        IntWidth::U16,
        IntWidth::I32,
        IntWidth::U32,
        IntWidth::I64,
    ];

    pub fn range(&self) -> (i64, i64) {
        match self {
            IntWidth::I8 => (i8::MIN as i64, i8::MAX as i64),
            IntWidth::U8 => (0, u8::MAX as i64),
            IntWidth::I16 => (i16::MIN as i64, i16::MAX as i64),
            IntWidth::U16 => (0, u16::MAX as i64),
            IntWidth::I32 => (i32::MIN as i64, i32::MAX as i64),
            IntWidth::U32 => (0, u32::MAX as i64),
            IntWidth::I64 => (i64::MIN, i64::MAX),
        }
    }

    pub fn fits(&self, v: i64) -> bool {
        let (lo, hi) = self.range();
        (lo..=hi).contains(&v)
    }

    /// Encodes `v` in this width. Caller must ensure `self.fits(v)`.
    pub fn encode(&self, v: i64) -> AMQPValue {
        match self {
            IntWidth::I8 => AMQPValue::ShortShortInt(v as i8),
            IntWidth::U8 => AMQPValue::ShortShortUInt(v as u8),
            IntWidth::I16 => AMQPValue::ShortInt(v as i16),
            IntWidth::U16 => AMQPValue::ShortUInt(v as u16),
            IntWidth::I32 => AMQPValue::LongInt(v as i32),
            IntWidth::U32 => AMQPValue::LongUInt(v as u32),
            IntWidth::I64 => AMQPValue::LongLongInt(v),
        }
    }
}

/// Timestamps far enough from any message to have an exact expected result.
#[derive(Clone, Copy, Debug)]
pub enum TsExtreme {
    Epoch,
    FarFuture,
}

/// Year 3000, in seconds (LavinMQ decodes AMQP timestamps as Unix seconds).
const FAR_FUTURE_SECS: u64 = 32_503_680_000;

#[derive(Clone, Copy, Debug)]
pub enum StreamOffset {
    First,
    Next,
    Int(i64, IntWidth),
    Timestamp(TsExtreme),
}

impl StreamOffset {
    pub fn to_amqp(&self) -> AMQPValue {
        match *self {
            StreamOffset::First => AMQPValue::LongString("first".into()),
            StreamOffset::Next => AMQPValue::LongString("next".into()),
            StreamOffset::Int(v, w) => w.encode(v),
            StreamOffset::Timestamp(TsExtreme::Epoch) => AMQPValue::Timestamp(0),
            StreamOffset::Timestamp(TsExtreme::FarFuture) => AMQPValue::Timestamp(FAR_FUTURE_SECS),
        }
    }

    /// Indices (0-based) of the `m` pre-published messages a consumer
    /// starting here receives.
    pub fn expected(&self, m: usize) -> Range<usize> {
        let start = match *self {
            StreamOffset::First | StreamOffset::Timestamp(TsExtreme::Epoch) => 0,
            StreamOffset::Next | StreamOffset::Timestamp(TsExtreme::FarFuture) => m,
            StreamOffset::Int(k, _) if k < 0 => m - (k.unsigned_abs().min(m as u64) as usize),
            StreamOffset::Int(k, _) => (k.max(1) as u64 - 1).min(m as u64) as usize,
        };
        start..m
    }
}

impl Arbitrary for StreamOffset {
    fn arbitrary(g: &mut Gen) -> Self {
        match *g.choose(&[0, 1, 2, 3, 4]).unwrap() {
            0 => StreamOffset::First,
            1 => StreamOffset::Next,
            2 => StreamOffset::Timestamp(
                *g.choose(&[TsExtreme::Epoch, TsExtreme::FarFuture]).unwrap(),
            ),
            _ => {
                // Mostly small values around the message count, plus the
                // bounds of every width.
                let width = *g.choose(&IntWidth::ALL).unwrap();
                let (lo, hi) = width.range();
                let small = (-60..=60).filter(|v| width.fits(*v)).collect::<Vec<_>>();
                let v = if bool::arbitrary(g) {
                    *g.choose(&small).unwrap()
                } else {
                    *g.choose(&[lo, hi]).unwrap()
                };
                StreamOffset::Int(v, width)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamOffsetScenario {
    pub stream: String,
    /// Messages published before the consumer starts.
    pub messages: usize,
    pub offset: StreamOffset,
}

impl Arbitrary for StreamOffsetScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        StreamOffsetScenario {
            stream: format!("qc_stream_{:x}", u64::arbitrary(g)),
            messages: *g.choose(&(0..=MAX_MESSAGES).collect::<Vec<_>>()).unwrap(),
            offset: StreamOffset::arbitrary(g),
        }
    }
}

#[cfg(test)]
mod model_tests {
    use super::*;

    fn int(v: i64) -> StreamOffset {
        StreamOffset::Int(v, IntWidth::I64)
    }

    #[test]
    fn first_and_epoch_deliver_everything() {
        assert_eq!(StreamOffset::First.expected(5), 0..5);
        assert_eq!(StreamOffset::Timestamp(TsExtreme::Epoch).expected(5), 0..5);
    }

    #[test]
    fn next_and_far_future_deliver_nothing() {
        assert!(StreamOffset::Next.expected(5).is_empty());
        assert!(
            StreamOffset::Timestamp(TsExtreme::FarFuture)
                .expected(5)
                .is_empty()
        );
    }

    #[test]
    fn zero_and_one_start_at_first_message() {
        assert_eq!(int(0).expected(5), 0..5);
        assert_eq!(int(1).expected(5), 0..5);
    }

    #[test]
    fn positive_offset_skips_earlier_messages() {
        assert_eq!(int(3).expected(5), 2..5);
        assert_eq!(int(5).expected(5), 4..5);
    }

    #[test]
    fn offset_past_end_delivers_nothing() {
        assert!(int(6).expected(5).is_empty());
        assert!(int(i64::MAX).expected(5).is_empty());
    }

    #[test]
    fn negative_offset_delivers_last_n() {
        assert_eq!(int(-1).expected(5), 4..5);
        assert_eq!(int(-3).expected(5), 2..5);
    }

    #[test]
    fn negative_offset_clamps_to_first() {
        assert_eq!(int(-5).expected(5), 0..5);
        assert_eq!(int(-50).expected(5), 0..5);
        assert_eq!(int(i64::MIN).expected(5), 0..5);
    }

    #[test]
    fn empty_stream_delivers_nothing() {
        for o in [StreamOffset::First, int(0), int(-3), int(7)] {
            assert!(o.expected(0).is_empty());
        }
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn int_offset_fits_its_width(o: StreamOffset) -> bool {
        match o {
            StreamOffset::Int(v, w) => w.fits(v),
            _ => true,
        }
    }

    #[quickcheck]
    fn scenario_is_bounded(s: StreamOffsetScenario) -> bool {
        s.messages <= MAX_MESSAGES && s.stream.starts_with("qc_stream_")
    }
}
