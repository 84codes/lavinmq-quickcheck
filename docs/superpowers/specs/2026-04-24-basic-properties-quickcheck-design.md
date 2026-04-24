# BasicProperties QuickCheck — Design

Date: 2026-04-24

## Background

The crate currently exercises LavinMQ's queue-declaration arguments
(`queue-arguments.md`), its routing semantics (routing-graph spec), and
three round-trip smoke tests. It does not yet exercise AMQP's
`basic.publish` *message properties*.

Lapin exposes these as `BasicProperties`. The AMQP 0-9-1 frame carries 14
such properties; each has its own wire type and validation rules. This
spec adds property-based tests that generate arbitrary valid values for
each property (and random subsets of them) and publish messages to
LavinMQ, asserting only that the publish call and its confirm succeed.

## Goals

1. Generate broker-valid values for 13 of the 14 `BasicProperties`
   fields (all except `user_id`, see below).
2. For each property, a per-property test that publishes a message with
   exactly that one property set.
3. A combination test that publishes with a random subset of all 13
   properties set at once.
4. Declare-and-publish semantics — no consumer, no verification beyond
   "publish confirmed successfully".

## Non-goals

- `user_id` property. LavinMQ (like RabbitMQ) validates `user_id` against
  the authenticated user on the channel. The tests connect as `guest`;
  anything else gets rejected with `ACCESS_REFUSED` (403). That's an auth
  side-effect, not a property-wire-format behaviour. Skipping it keeps
  the spec focused.
- Nested / exotic `AMQPValue` variants inside `headers`. Flat scalars
  only (`Boolean`, signed/unsigned ints of each size, `Double`,
  `LongString`, `Timestamp`). Nested tables, field arrays, byte arrays,
  decimals, and `Void` are out of scope — they'd test our generator
  more than LavinMQ.
- Consuming the published messages. "Just publish, no verify" — we only
  assert that `basic_publish` (and the confirm that follows) returns
  `Ok`.
- Error-path coverage (invalid expiration formats, priority > 9 on
  priority queues, etc.) — generators produce only broker-valid values.
- Testing interactions between properties and queue features (e.g.,
  priority property with a priority queue vs. a classic queue).

## Target broker

LavinMQ on `amqp://localhost:5672`, same as the rest of the test suite.

## Architecture

```
src/
  lib.rs        — adds `pub mod properties;`
  properties.rs — per-property newtypes + Arbitrary impls + apply_to
                  helpers, plus a combined `BasicPropertiesArgs` type. ~250
                  lines.
  tests.rs      — one new helper (`publish_with_props`), one macro
                  (`single_prop_publish_test!`), 13 per-property tests,
                  plus one combined test. ~80 new lines.
```

The pattern mirrors the existing queue-arguments work: per-property
newtypes with `apply_to(BasicProperties) -> BasicProperties`, a combined
struct with `Option<T>` fields and `apply() -> BasicProperties`, and a
single macro that produces the per-property tests.

## Per-property newtypes

Each newtype wraps a single value that maps to one of lapin's
`BasicProperties` setters. Every newtype has:

- `#[derive(Clone, Debug)]`
- `pub fn apply_to(&self, props: BasicProperties) -> BasicProperties` —
  invokes the corresponding `with_*` setter on lapin's builder.
- `impl Arbitrary` producing broker-valid values.

### Property set

| Newtype | AMQP field | Generator | Wire type |
| --- | --- | --- | --- |
| `ContentType(String)` | content_type | 1–255 chars from `QUEUE_NAME_CHARS`, no `amq.` prefix restriction (it's a free string, not a name) | short-string |
| `ContentEncoding(String)` | content_encoding | Same alphabet, 1–64 chars | short-string |
| `Headers(FieldTable)` | headers | 0–10 entries; each key is 1–32 chars from header-name alphabet; each value is uniformly one of 12 scalar `AMQPValue` variants (see below) | field-table |
| `DeliveryMode(u8)` | delivery_mode | Uniformly `1` (transient) or `2` (persistent) | octet |
| `Priority(u8)` | priority | Uniformly `0..=9` | octet |
| `CorrelationId(String)` | correlation_id | 1–64 chars from `QUEUE_NAME_CHARS` | short-string |
| `ReplyTo(String)` | reply_to | 1–64 chars from `QUEUE_NAME_CHARS` | short-string |
| `Expiration(String)` | expiration | Stringified `u32` ms (e.g. `"60000"`) — LavinMQ parses this as an integer | short-string |
| `MessageId(String)` | message_id | 1–64 chars from `QUEUE_NAME_CHARS` | short-string |
| `MessageTimestamp(u64)` | timestamp | any `u64` | timestamp |
| `MessageKind(String)` | kind | 1–64 chars from `QUEUE_NAME_CHARS`. Named `kind` because `type` is a reserved Rust keyword — it maps to the AMQP `type` property. | short-string |
| `AppId(String)` | app_id | 1–64 chars from `QUEUE_NAME_CHARS` | short-string |
| `ClusterId(String)` | cluster_id | 1–64 chars from `QUEUE_NAME_CHARS` | short-string. Deprecated by AMQP but still wire-legal. |

### Headers scalar value variants

`Headers::arbitrary` picks each value uniformly from:

- `AMQPValue::Boolean(bool)`
- `AMQPValue::ShortShortInt(i8)`
- `AMQPValue::ShortShortUInt(u8)`
- `AMQPValue::ShortInt(i16)`
- `AMQPValue::ShortUInt(u16)`
- `AMQPValue::LongInt(i32)`
- `AMQPValue::LongUInt(u32)`
- `AMQPValue::LongLongInt(i64)`
- `AMQPValue::LongLongUInt(u64)`
- `AMQPValue::Double(f64)` — via `f64::arbitrary`, which includes NaN/Inf; LavinMQ encodes them as IEEE 754 bytes and should accept any
- `AMQPValue::LongString(String)` — random valid-char string
- `AMQPValue::Timestamp(u64)`

12 variants; a 13th (`Void`) is pointless to generate and rarely seen in
practice; explicitly skipped along with `DecimalValue`, `ByteArray`,
`FieldArray`, `FieldTable`.

### Reused alphabets

- `QUEUE_NAME_CHARS` (crate-internal in `src/names.rs`) — the ASCII
  character set already used by `QueueName` and `RoutingKey`. Covers
  letters, digits, `-_.:`. Safe across AMQP short-strings.
- Header name alphabet — the same set as `DeduplicationHeader`:
  `[A-Za-z0-9_-]`, 1–32 chars. Header names don't allow dots in most
  real-world brokers, so we use the stricter set.

## Combined type

```rust
pub struct BasicPropertiesArgs {
    pub content_type: Option<ContentType>,
    pub content_encoding: Option<ContentEncoding>,
    pub headers: Option<Headers>,
    pub delivery_mode: Option<DeliveryMode>,
    pub priority: Option<Priority>,
    pub correlation_id: Option<CorrelationId>,
    pub reply_to: Option<ReplyTo>,
    pub expiration: Option<Expiration>,
    pub message_id: Option<MessageId>,
    pub timestamp: Option<MessageTimestamp>,
    pub kind: Option<MessageKind>,
    pub app_id: Option<AppId>,
    pub cluster_id: Option<ClusterId>,
}

impl BasicPropertiesArgs {
    pub fn apply(&self) -> BasicProperties {
        let mut props = BasicProperties::default();
        if let Some(v) = &self.content_type { props = v.apply_to(props); }
        if let Some(v) = &self.content_encoding { props = v.apply_to(props); }
        if let Some(v) = &self.headers { props = v.apply_to(props); }
        if let Some(v) = &self.delivery_mode { props = v.apply_to(props); }
        if let Some(v) = &self.priority { props = v.apply_to(props); }
        if let Some(v) = &self.correlation_id { props = v.apply_to(props); }
        if let Some(v) = &self.reply_to { props = v.apply_to(props); }
        if let Some(v) = &self.expiration { props = v.apply_to(props); }
        if let Some(v) = &self.message_id { props = v.apply_to(props); }
        if let Some(v) = &self.timestamp { props = v.apply_to(props); }
        if let Some(v) = &self.kind { props = v.apply_to(props); }
        if let Some(v) = &self.app_id { props = v.apply_to(props); }
        if let Some(v) = &self.cluster_id { props = v.apply_to(props); }
        props
    }
}

impl Arbitrary for BasicPropertiesArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        BasicPropertiesArgs {
            content_type: Option::arbitrary(g),
            content_encoding: Option::arbitrary(g),
            headers: Option::arbitrary(g),
            delivery_mode: Option::arbitrary(g),
            priority: Option::arbitrary(g),
            correlation_id: Option::arbitrary(g),
            reply_to: Option::arbitrary(g),
            expiration: Option::arbitrary(g),
            message_id: Option::arbitrary(g),
            timestamp: Option::arbitrary(g),
            kind: Option::arbitrary(g),
            app_id: Option::arbitrary(g),
            cluster_id: Option::arbitrary(g),
        }
    }
}
```

Every field independent — no coupling constraints like the DLRK/DLX
pairing in `ClassicQueueArgs`.

## Test harness

### Helper

One new async helper in `src/tests.rs`, placed near the existing
`declare_classic_ok`:

```rust
async fn publish_with_props(
    channel: &lapin::Channel,
    name: &str,
    props: BasicProperties,
) -> bool {
    let declared = channel
        .queue_declare(name, classic_queue_opts(), FieldTable::default())
        .await
        .is_ok();
    if !declared {
        return false;
    }
    let res = channel
        .basic_publish(
            "",                                   // default exchange
            name,                                 // routing key == queue name
            BasicPublishOptions::default(),
            b"",
            props,
        )
        .await;
    let ok = match res {
        Ok(confirm) => confirm.await.is_ok(),
        Err(_) => false,
    };
    let _ = channel
        .queue_delete(name, QueueDeleteOptions::default())
        .await;
    ok
}
```

Queue declared, message published to default exchange with routing key
= queue name, publish confirm awaited, queue deleted. Matches the
existing round-trip pattern minus the consume step.

### Per-property tests

One `#[quickcheck]` per newtype, via a macro:

```rust
macro_rules! single_prop_publish_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, prop: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let props = prop.apply_to(BasicProperties::default());
                publish_with_props(&channel, &name.0, props).await
            })
        }
    };
}

single_prop_publish_test!(publish_with_content_type, ContentType);
single_prop_publish_test!(publish_with_content_encoding, ContentEncoding);
single_prop_publish_test!(publish_with_headers, Headers);
single_prop_publish_test!(publish_with_delivery_mode, DeliveryMode);
single_prop_publish_test!(publish_with_priority, Priority);
single_prop_publish_test!(publish_with_correlation_id, CorrelationId);
single_prop_publish_test!(publish_with_reply_to, ReplyTo);
single_prop_publish_test!(publish_with_expiration, Expiration);
single_prop_publish_test!(publish_with_message_id, MessageId);
single_prop_publish_test!(publish_with_timestamp, MessageTimestamp);
single_prop_publish_test!(publish_with_kind, MessageKind);
single_prop_publish_test!(publish_with_app_id, AppId);
single_prop_publish_test!(publish_with_cluster_id, ClusterId);
```

### Combined test

```rust
#[quickcheck]
fn publish_with_combined_properties(name: QueueName, args: BasicPropertiesArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        publish_with_props(&channel, &name.0, args.apply()).await
    })
}
```

### Test count

| Flavour | Count |
| --- | --- |
| Per-property | 13 |
| Combined | 1 |
| **New total** | **14** |

Plus the existing 41 tests, unchanged. Post-implementation suite size:
**55**.

## Failure interpretation

| Failing test | Meaning |
| --- | --- |
| A per-property test fails | Generator produces a value LavinMQ rejects on the wire; either the generator is wrong, lapin encoded it unexpectedly, or LavinMQ has stricter validation than documented. |
| The combined test fails | Same as above, plus potentially an interaction between two properties. QuickCheck will shrink to a minimal failing subset. |
| Publish confirm hangs | LavinMQ accepted the publish but never confirmed — likely a broker bug or a lost connection. Not expected. |

## Out of scope (recap)

- `user_id` (auth-validated).
- Nested / exotic header value types.
- Consuming or verifying received messages.
- Invalid-value rejection tests.
- Cross-feature interactions (property × queue type).
- `AGENTS.md` / `lavinmq-quirks.md` updates (none anticipated).
