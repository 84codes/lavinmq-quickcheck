# BasicProperties QuickCheck Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add publish-only property-based tests for 13 of 14 AMQP `BasicProperties` fields (skipping auth-gated `user_id`): one per-property test for each field, plus one combination test that publishes with a random subset of all 13 set at once.

**Architecture:** A new `src/properties.rs` module holds per-property newtypes (each with `Arbitrary` + `apply_to(BasicProperties) -> BasicProperties`) and a `BasicPropertiesArgs` combined type with `Option<T>` fields. `src/tests.rs` gets one new helper (`publish_with_props`), one macro (`single_prop_publish_test!`), 13 per-property test invocations, and one combined test. Tests declare an auto-delete queue per iteration, publish one empty-body message to it via the default exchange, await publish confirm, and assert `Ok`.

**Tech Stack:** Rust 2024, `quickcheck` + `quickcheck_macros`, `lapin` 2.x, `tokio` runtime.

**Spec:** `docs/superpowers/specs/2026-04-24-basic-properties-quickcheck-design.md`

**Prerequisite:** LavinMQ on `localhost:5672`.

---

## Task 1: Module scaffold + first property (ContentType)

Sets up `src/properties.rs`, the `publish_with_props` helper in `src/tests.rs`, the `single_prop_publish_test!` macro, and `ContentType` as the first newtype. TDD loop: failing test, red phase, implementation, green phase.

**Files:**
- Create: `src/properties.rs`
- Modify: `src/lib.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Write the failing test invocation at the bottom of `src/tests.rs`**

At the very bottom of the file (after all existing macros and tests), append:

```rust
// ---------------------------------------------------------------------------
// BasicProperties publish-only tests
// ---------------------------------------------------------------------------

use crate::properties::ContentType;

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
            "",
            name,
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
```

- [ ] **Step 2: Run the tests to verify they fail at compile time**

Run: `cargo check --tests`
Expected: compile error, `ContentType` not defined; also `crate::properties` doesn't exist.

- [ ] **Step 3: Create `src/properties.rs` with `ContentType`**

```rust
//! Arbitrary generators for AMQP `BasicProperties` fields. Each newtype
//! wraps a single value, exposes `apply_to(BasicProperties) -> BasicProperties`
//! that calls lapin's chainable setter, and implements `Arbitrary` with
//! broker-valid values.

use lapin::BasicProperties;
use lapin::types::ShortString;
use quickcheck::{Arbitrary, Gen};

use crate::names::QUEUE_NAME_CHARS;

/// Max length for short-string-valued properties (spec: up to 255 bytes).
/// We cap at 64 for ease of reading in failure reports.
const SHORT_STRING_MAX: usize = 64;

/// Generates 1..=max characters uniformly from `QUEUE_NAME_CHARS`.
fn short_string(g: &mut Gen, max: usize) -> String {
    let len = *g.choose(&(1..=max).collect::<Vec<_>>()).unwrap();
    (0..len)
        .map(|_| {
            let &byte = g.choose(QUEUE_NAME_CHARS).unwrap();
            byte as char
        })
        .collect()
}

/// `content_type` message property.
#[derive(Clone, Debug)]
pub struct ContentType(pub String);

impl ContentType {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_content_type(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ContentType {
    fn arbitrary(g: &mut Gen) -> Self {
        ContentType(short_string(g, SHORT_STRING_MAX))
    }
}
```

- [ ] **Step 4: Register the module in `src/lib.rs`**

Add `pub mod properties;` in alphabetical order. The final module block becomes:

```rust
pub mod arguments;
pub mod combined;
pub mod names;
pub mod properties;
pub mod routing;

pub use names::{QueueName, RoutingKey, TopicRoutingKey};

#[cfg(test)]
mod tests;
```

- [ ] **Step 5: Run the new test**

Run: `cargo test --lib publish_with_content_type`
Expected: PASS against LavinMQ on `localhost:5672`. Runs ~100 quickcheck iterations in a few seconds.

- [ ] **Step 6: Run the full suite to catch regressions**

Run: `cargo test`
Expected: 42 tests pass (41 prior + 1 new).

- [ ] **Step 7: Commit**

```bash
git add src/properties.rs src/lib.rs src/tests.rs
git commit -m "Add BasicProperties test scaffold + ContentType"
```

---

## Task 2: Remaining short-string properties (7 newtypes)

Adds `ContentEncoding`, `CorrelationId`, `ReplyTo`, `MessageId`, `MessageKind`, `AppId`, `ClusterId` — all share the same short-string shape as `ContentType`.

**Files:**
- Modify: `src/properties.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Append the seven newtypes to `src/properties.rs`**

```rust
/// `content_encoding` message property.
#[derive(Clone, Debug)]
pub struct ContentEncoding(pub String);

impl ContentEncoding {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_content_encoding(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ContentEncoding {
    fn arbitrary(g: &mut Gen) -> Self {
        ContentEncoding(short_string(g, SHORT_STRING_MAX))
    }
}

/// `correlation_id` message property.
#[derive(Clone, Debug)]
pub struct CorrelationId(pub String);

impl CorrelationId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_correlation_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for CorrelationId {
    fn arbitrary(g: &mut Gen) -> Self {
        CorrelationId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `reply_to` message property. Opaque — broker does not validate as a queue name.
#[derive(Clone, Debug)]
pub struct ReplyTo(pub String);

impl ReplyTo {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_reply_to(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ReplyTo {
    fn arbitrary(g: &mut Gen) -> Self {
        ReplyTo(short_string(g, SHORT_STRING_MAX))
    }
}

/// `message_id` message property.
#[derive(Clone, Debug)]
pub struct MessageId(pub String);

impl MessageId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_message_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for MessageId {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `type` message property. Named `MessageKind` because `type` is a
/// reserved Rust keyword.
#[derive(Clone, Debug)]
pub struct MessageKind(pub String);

impl MessageKind {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_kind(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for MessageKind {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageKind(short_string(g, SHORT_STRING_MAX))
    }
}

/// `app_id` message property.
#[derive(Clone, Debug)]
pub struct AppId(pub String);

impl AppId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_app_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for AppId {
    fn arbitrary(g: &mut Gen) -> Self {
        AppId(short_string(g, SHORT_STRING_MAX))
    }
}

/// `cluster_id` message property. Deprecated by AMQP but still wire-legal.
#[derive(Clone, Debug)]
pub struct ClusterId(pub String);

impl ClusterId {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_cluster_id(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for ClusterId {
    fn arbitrary(g: &mut Gen) -> Self {
        ClusterId(short_string(g, SHORT_STRING_MAX))
    }
}
```

- [ ] **Step 2: Append the seven test invocations to `src/tests.rs`**

Extend the existing import line at the bottom of `src/tests.rs` from `use crate::properties::ContentType;` to:

```rust
use crate::properties::{
    AppId, ClusterId, ContentEncoding, ContentType, CorrelationId, MessageId, MessageKind,
    ReplyTo,
};
```

Then below `single_prop_publish_test!(publish_with_content_type, ContentType);`, append:

```rust
single_prop_publish_test!(publish_with_content_encoding, ContentEncoding);
single_prop_publish_test!(publish_with_correlation_id, CorrelationId);
single_prop_publish_test!(publish_with_reply_to, ReplyTo);
single_prop_publish_test!(publish_with_message_id, MessageId);
single_prop_publish_test!(publish_with_kind, MessageKind);
single_prop_publish_test!(publish_with_app_id, AppId);
single_prop_publish_test!(publish_with_cluster_id, ClusterId);
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test --lib publish_with_`
Expected: 8 tests pass (ContentType + 7 new).

- [ ] **Step 4: Run the full suite**

Run: `cargo test`
Expected: 49 tests pass (41 prior + 8 new).

- [ ] **Step 5: Commit**

```bash
git add src/properties.rs src/tests.rs
git commit -m "Add short-string BasicProperties newtypes + per-prop tests"
```

---

## Task 3: Numeric and special-format properties (4 newtypes)

Adds `DeliveryMode`, `Priority`, `MessageTimestamp`, `Expiration`.

**Files:**
- Modify: `src/properties.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Append the four newtypes to `src/properties.rs`**

```rust
/// `delivery_mode` — `1` (transient) or `2` (persistent).
#[derive(Clone, Debug)]
pub struct DeliveryMode(pub u8);

impl DeliveryMode {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_delivery_mode(self.0)
    }
}

impl Arbitrary for DeliveryMode {
    fn arbitrary(g: &mut Gen) -> Self {
        DeliveryMode(if bool::arbitrary(g) { 1 } else { 2 })
    }
}

/// `priority` — 0..=9 (conservative bound; brokers accept higher but
/// priority queues typically use 0-9).
#[derive(Clone, Debug)]
pub struct Priority(pub u8);

impl Priority {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_priority(self.0)
    }
}

impl Arbitrary for Priority {
    fn arbitrary(g: &mut Gen) -> Self {
        Priority(*g.choose(&(0u8..=9).collect::<Vec<_>>()).unwrap())
    }
}

/// `timestamp` — arbitrary u64 unix timestamp.
#[derive(Clone, Debug)]
pub struct MessageTimestamp(pub u64);

impl MessageTimestamp {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_timestamp(self.0)
    }
}

impl Arbitrary for MessageTimestamp {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageTimestamp(u64::arbitrary(g))
    }
}

/// `expiration` — short string parsed as a stringified integer millisecond value.
#[derive(Clone, Debug)]
pub struct Expiration(pub String);

impl Expiration {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_expiration(ShortString::from(self.0.clone()))
    }
}

impl Arbitrary for Expiration {
    fn arbitrary(g: &mut Gen) -> Self {
        // LavinMQ parses this as an integer number of milliseconds.
        // Generate any u32 formatted as decimal.
        Expiration(format!("{}", u32::arbitrary(g)))
    }
}
```

- [ ] **Step 2: Append the four test invocations to `src/tests.rs`**

Extend the `use crate::properties::{...}` import list:

```rust
use crate::properties::{
    AppId, ClusterId, ContentEncoding, ContentType, CorrelationId, DeliveryMode, Expiration,
    MessageId, MessageKind, MessageTimestamp, Priority, ReplyTo,
};
```

Append below the existing `single_prop_publish_test!` invocations:

```rust
single_prop_publish_test!(publish_with_delivery_mode, DeliveryMode);
single_prop_publish_test!(publish_with_priority, Priority);
single_prop_publish_test!(publish_with_timestamp, MessageTimestamp);
single_prop_publish_test!(publish_with_expiration, Expiration);
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test --lib "publish_with_(delivery_mode|priority|timestamp|expiration)"`
Expected: 4 new tests pass.

- [ ] **Step 4: Run the full suite**

Run: `cargo test`
Expected: 53 tests pass (49 prior + 4 new).

- [ ] **Step 5: Commit**

```bash
git add src/properties.rs src/tests.rs
git commit -m "Add numeric BasicProperties newtypes + per-prop tests"
```

---

## Task 4: Headers property (flat-scalar FieldTable)

Adds `Headers` — a FieldTable with 0–10 entries, each value uniformly one of 12 scalar `AMQPValue` variants.

**Files:**
- Modify: `src/properties.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Append `Headers` to `src/properties.rs`**

```rust
use lapin::types::{AMQPValue, FieldTable, LongString};

const HEADER_NAME_CHARS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

fn header_name(g: &mut Gen) -> String {
    let len = *g.choose(&(1..=32usize).collect::<Vec<_>>()).unwrap();
    (0..len)
        .map(|_| {
            let &byte = g.choose(HEADER_NAME_CHARS).unwrap();
            byte as char
        })
        .collect()
}

fn arbitrary_scalar_value(g: &mut Gen) -> AMQPValue {
    // Twelve uniform variants; one is picked per entry.
    let variant = *g.choose(&(0u8..12).collect::<Vec<_>>()).unwrap();
    match variant {
        0 => AMQPValue::Boolean(bool::arbitrary(g)),
        1 => AMQPValue::ShortShortInt(i8::arbitrary(g)),
        2 => AMQPValue::ShortShortUInt(u8::arbitrary(g)),
        3 => AMQPValue::ShortInt(i16::arbitrary(g)),
        4 => AMQPValue::ShortUInt(u16::arbitrary(g)),
        5 => AMQPValue::LongInt(i32::arbitrary(g)),
        6 => AMQPValue::LongUInt(u32::arbitrary(g)),
        7 => AMQPValue::LongLongInt(i64::arbitrary(g)),
        8 => AMQPValue::LongLongUInt(u64::arbitrary(g)),
        9 => AMQPValue::Double(f64::arbitrary(g)),
        10 => {
            let s = short_string(g, SHORT_STRING_MAX);
            AMQPValue::LongString(LongString::from(s))
        }
        _ => AMQPValue::Timestamp(u64::arbitrary(g)),
    }
}

/// `headers` message property. Flat scalars only — no nested tables, arrays,
/// or exotic types (Void / DecimalValue / ByteArray / FieldArray / FieldTable).
#[derive(Clone, Debug)]
pub struct Headers(pub FieldTable);

impl Headers {
    pub fn apply_to(&self, props: BasicProperties) -> BasicProperties {
        props.with_headers(self.0.clone())
    }
}

impl Arbitrary for Headers {
    fn arbitrary(g: &mut Gen) -> Self {
        let n_entries = *g.choose(&(0usize..=10).collect::<Vec<_>>()).unwrap();
        let mut table = FieldTable::default();
        for _ in 0..n_entries {
            let key = header_name(g);
            let value = arbitrary_scalar_value(g);
            table.insert(key.into(), value);
        }
        Headers(table)
    }
}
```

- [ ] **Step 2: Append the test invocation to `src/tests.rs`**

Extend the `use crate::properties::{...}` import list to add `Headers`:

```rust
use crate::properties::{
    AppId, ClusterId, ContentEncoding, ContentType, CorrelationId, DeliveryMode, Expiration,
    Headers, MessageId, MessageKind, MessageTimestamp, Priority, ReplyTo,
};
```

Append below the existing `single_prop_publish_test!` invocations:

```rust
single_prop_publish_test!(publish_with_headers, Headers);
```

- [ ] **Step 3: Run the new test**

Run: `cargo test --lib publish_with_headers`
Expected: PASS. Note: `f64::arbitrary` produces NaN/Inf occasionally — these are valid IEEE 754 bit patterns and LavinMQ must accept them.

- [ ] **Step 4: Run the full suite**

Run: `cargo test`
Expected: 54 tests pass (53 prior + 1 new).

- [ ] **Step 5: Commit**

```bash
git add src/properties.rs src/tests.rs
git commit -m "Add Headers BasicProperties newtype + per-prop test"
```

---

## Task 5: Combined `BasicPropertiesArgs` + combination test

**Files:**
- Modify: `src/properties.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Append `BasicPropertiesArgs` to `src/properties.rs`**

```rust
#[derive(Clone, Debug)]
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
        if let Some(v) = &self.content_type {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.content_encoding {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.headers {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.delivery_mode {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.priority {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.correlation_id {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.reply_to {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.expiration {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.message_id {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.timestamp {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.kind {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.app_id {
            props = v.apply_to(props);
        }
        if let Some(v) = &self.cluster_id {
            props = v.apply_to(props);
        }
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

- [ ] **Step 2: Append the combined test to `src/tests.rs`**

Extend the import to add `BasicPropertiesArgs`:

```rust
use crate::properties::{
    AppId, BasicPropertiesArgs, ClusterId, ContentEncoding, ContentType, CorrelationId,
    DeliveryMode, Expiration, Headers, MessageId, MessageKind, MessageTimestamp, Priority,
    ReplyTo,
};
```

Append at the bottom:

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

- [ ] **Step 3: Run the new test**

Run: `cargo test --lib publish_with_combined_properties`
Expected: PASS.

- [ ] **Step 4: Run the full suite**

Run: `cargo test`
Expected: 55 tests pass (54 prior + 1 new).

- [ ] **Step 5: Commit**

```bash
git add src/properties.rs src/tests.rs
git commit -m "Add BasicPropertiesArgs combined type + combination test"
```

---

## Task 6: Final polish

**Files:**
- Modify: anywhere fmt/clippy flags

- [ ] **Step 1: Run `cargo fmt`**

Run: `cargo fmt`
Expected: either no diff or cosmetic changes.

- [ ] **Step 2: Run `cargo clippy --all-targets -- -D warnings`**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean. If any warnings fire, read the lint and fix the underlying code rather than silencing with `#[allow(...)]`.

- [ ] **Step 3: Run the full test suite once more**

Run: `cargo test`
Expected: all 55 tests pass.

- [ ] **Step 4: Commit any polish changes**

```bash
git add -A
git commit -m "Format and lint cleanup"
```

(If there are no changes, skip the commit.)

---

## Self-Review Notes

**Spec coverage:**

- File layout (spec §"Architecture"): Task 1 creates `src/properties.rs`, wires into `lib.rs`, adds the test helper and macro in `src/tests.rs`. ✅
- Per-property newtypes (spec §"Property set"):
  - `ContentType` — Task 1 ✅
  - `ContentEncoding`, `CorrelationId`, `ReplyTo`, `MessageId`, `MessageKind`, `AppId`, `ClusterId` — Task 2 ✅
  - `DeliveryMode`, `Priority`, `MessageTimestamp`, `Expiration` — Task 3 ✅
  - `Headers` (flat scalars, 12 variants) — Task 4 ✅
- Combined type `BasicPropertiesArgs` (spec §"Combined type") — Task 5 ✅
- `publish_with_props` helper using default exchange + per-iteration auto-delete queue (spec §"Helper") — Task 1 ✅
- `single_prop_publish_test!` macro (spec §"Per-property tests") — Task 1 ✅
- 13 per-property invocations — Tasks 1, 2, 3, 4 ✅
- Combined test (spec §"Combined test") — Task 5 ✅
- Skip `user_id` (spec §"Non-goals") — confirmed absent from all tasks ✅
- Flat scalar headers only (spec §"Non-goals") — `arbitrary_scalar_value` in Task 4 deliberately picks only 12 scalar variants ✅

**Type consistency:**

- `apply_to(&self, BasicProperties) -> BasicProperties` on every per-property newtype — consistent across Tasks 1-4.
- `apply(&self) -> BasicProperties` on `BasicPropertiesArgs` — consistent with spec.
- `short_string(&mut Gen, usize)` and `SHORT_STRING_MAX` helper from Task 1 are reused by Tasks 2, 3 (Expiration uses `u32::arbitrary` instead of the helper, which is correct — stringified integer has a different format).
- `HEADER_NAME_CHARS` and `header_name` live in `properties.rs` (Task 4) rather than being imported from `arguments.rs`. That's an intentional duplicate — keeps the modules decoupled, at the cost of ~10 lines of duplicated alphabet.
- Macro `single_prop_publish_test!` takes `($fn_name:ident, $ty:ty)` — consistent across all 13 invocations.
- `use crate::properties::{...}` import grows by a few names per task — each task shows the final form of the import list.

**Placeholders:** none.
