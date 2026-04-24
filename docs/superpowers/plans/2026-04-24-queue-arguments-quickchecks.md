# Queue-Arguments QuickChecks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the crate with QuickCheck-based `Arbitrary` generators and integration tests for every `x-*` queue argument LavinMQ accepts on `queue.declare`, covering per-argument, combination, and rejection test flavours.

**Architecture:** Split the existing single-file crate into focused modules (`names.rs`, `arguments.rs`, `combined.rs`, `tests.rs`). Per-argument newtypes expose `Arbitrary` + `insert_into(&mut FieldTable)`. Combined structs with `Option<T>` fields produce random subsets. All tests are declare-only and run against a real LavinMQ broker on `localhost:5672`.

**Tech Stack:** Rust 2024 edition, `quickcheck` + `quickcheck_macros`, `lapin` (async AMQP client), `tokio` runtime. Target broker: LavinMQ.

**Spec:** `docs/superpowers/specs/2026-04-24-queue-arguments-quickchecks-design.md`

**Prerequisite:** A LavinMQ container running on `localhost:5672`:
```sh
docker run -d --rm -p 5672:5672 cloudamqp/lavinmq
```

---

## Task 1: Split existing code into modules

**Files:**
- Create: `src/names.rs`
- Create: `src/tests.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create `src/names.rs` with the existing type definitions**

Move the current contents of `src/lib.rs` (lines 1–109 — constants and `Arbitrary` impls for `QueueName`, `RoutingKey`, `TopicRoutingKey`) into a new file `src/names.rs`. Keep everything `pub`.

```rust
// src/names.rs
use quickcheck::{Arbitrary, Gen};

const VALID_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:";
const TOPIC_WORD_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
const RESERVED_PREFIX: &str = "amq.";

#[derive(Clone, Debug)]
pub struct QueueName(pub String);

impl Arbitrary for QueueName {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();

        loop {
            let name: String = (0..len)
                .map(|_| {
                    let &byte = g.choose(VALID_CHARS).unwrap();
                    byte as char
                })
                .collect();

            if !name.starts_with(RESERVED_PREFIX) {
                return QueueName(name);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoutingKey(pub String);

impl Arbitrary for RoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();

        let key: String = (0..len)
            .map(|_| {
                let &byte = g.choose(VALID_CHARS).unwrap();
                byte as char
            })
            .collect();

        RoutingKey(key)
    }
}

#[derive(Clone, Debug)]
pub struct TopicRoutingKey {
    pub routing_key: String,
    pub binding_pattern: String,
}

impl TopicRoutingKey {
    fn gen_word(g: &mut Gen) -> String {
        let max_len = g.size().clamp(1, 20);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        (0..len)
            .map(|_| {
                let &byte = g.choose(TOPIC_WORD_CHARS).unwrap();
                byte as char
            })
            .collect()
    }
}

impl Arbitrary for TopicRoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_words = g.size().clamp(1, 10);
        let num_words = *g.choose(&(1..=max_words).collect::<Vec<_>>()).unwrap();

        let words: Vec<String> = (0..num_words).map(|_| Self::gen_word(g)).collect();
        let routing_key = words.join(".");

        let mut binding_parts: Vec<String> = Vec::new();
        let mut i = 0;
        while i < words.len() {
            let choice = *g.choose(&[0u8, 1, 2, 3]).unwrap();
            match choice {
                0 => {
                    binding_parts.push(words[i].clone());
                    i += 1;
                }
                1 => {
                    binding_parts.push("*".to_string());
                    i += 1;
                }
                2 => {
                    let remaining = words.len() - i;
                    let skip = *g.choose(&(1..=remaining).collect::<Vec<_>>()).unwrap();
                    binding_parts.push("#".to_string());
                    i += skip;
                }
                _ => {
                    binding_parts.push(words[i].clone());
                    i += 1;
                }
            }
        }

        let binding_pattern = binding_parts.join(".");

        TopicRoutingKey {
            routing_key,
            binding_pattern,
        }
    }
}

// Re-export VALID_CHARS so other modules can build exchange-name / header-name generators
// against the same alphabet. Stays crate-internal — not pub-reexported from lib.rs.
pub(crate) const QUEUE_NAME_CHARS: &[u8] = VALID_CHARS;
pub(crate) const RESERVED_QUEUE_PREFIX: &str = RESERVED_PREFIX;
```

- [ ] **Step 2: Create `src/tests.rs` with the existing round-trip tests**

Move the existing `#[cfg(test)] mod tests` block (lines 111–352 of the old `lib.rs`) into `src/tests.rs`. Change the `use super::{QueueName, RoutingKey, TopicRoutingKey};` line to `use crate::names::{QueueName, RoutingKey, TopicRoutingKey};`. Keep everything else identical.

```rust
// src/tests.rs
// (Module is cfg-gated by `#[cfg(test)] mod tests;` in lib.rs — no inner
// attribute needed here.)

use crate::names::{QueueName, RoutingKey, TopicRoutingKey};
use futures_lite::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{
        BasicConsumeOptions, BasicPublishOptions, QueueBindOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::FieldTable,
};
use quickcheck_macros::quickcheck;

// <paste the three existing tests (round_trip, direct_exchange_round_trip,
//  topic_exchange_round_trip) from the old lib.rs tests module verbatim>
```

- [ ] **Step 3: Rewrite `src/lib.rs` as a thin re-export module**

Replace the entire contents of `src/lib.rs` with:

```rust
//! Property-based testing helpers for AMQP (LavinMQ) interactions.
//!
//! Provides `Arbitrary` implementations for AMQP names and queue arguments,
//! together with integration tests that exercise them against a real LavinMQ
//! broker.

pub mod names;

pub use names::{QueueName, RoutingKey, TopicRoutingKey};

#[cfg(test)]
mod tests;
```

- [ ] **Step 4: Run `cargo check` to verify the module split compiles**

Run: `cargo check`
Expected: clean build, no warnings about unused imports.

- [ ] **Step 5: Run existing tests to verify behaviour is unchanged**

Run: `cargo test`
Expected: the three existing tests (`round_trip`, `direct_exchange_round_trip`, `topic_exchange_round_trip`) pass against a running LavinMQ instance.

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs src/names.rs src/tests.rs
git commit -m "Split lib.rs into names/tests modules"
```

---

## Task 2: Update AGENTS.md for LavinMQ

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: Change broker references from RabbitMQ to LavinMQ**

Replace the "External Dependencies (Runtime)" section so it reads:

```markdown
## External Dependencies (Runtime)

Tests require a **LavinMQ server** running on `amqp://localhost:5672` with default credentials. Without it, `cargo test` will fail with connection errors. You can start one with:

```sh
docker run -d --rm -p 5672:5672 cloudamqp/lavinmq
```
```

Replace the "Project Overview" section's first sentence so it reads:

```markdown
`amqp-quickcheck` is a Rust library that provides [QuickCheck](https://crates.io/crates/quickcheck) `Arbitrary` implementations for AMQP types, enabling property-based testing of AMQP interactions against LavinMQ. Currently implements generators for queue names, routing keys, topic routing keys, and the full set of LavinMQ queue-declaration arguments.
```

Replace the "Project Structure" block so it reads:

```markdown
## Project Structure

```
src/
  lib.rs        — Library root. Re-exports the public API.
  names.rs      — Arbitrary impls for QueueName, RoutingKey, TopicRoutingKey.
  arguments.rs  — Arbitrary impls for individual x-* queue arguments.
  combined.rs   — Arbitrary impls for combined argument sets per queue type.
  tests.rs      — #[cfg(test)] integration tests against a real broker.
Cargo.toml      — Package manifest (edition 2024).
```
```

Replace the "Gotchas" list's first item so it reads:

```markdown
1. **LavinMQ required for tests** — Tests are not unit tests; they hit a real LavinMQ broker. CI must provision LavinMQ.
```

- [ ] **Step 2: Commit**

```bash
git add AGENTS.md
git commit -m "Point AGENTS.md at LavinMQ"
```

---

## Task 3: Add the `connect_channel` test helper

**Files:**
- Modify: `src/tests.rs`

- [ ] **Step 1: Add a shared async connection helper at the top of the tests module**

Insert after the `use` block in `src/tests.rs`:

```rust
async fn connect_channel() -> lapin::Channel {
    let conn = Connection::connect("amqp://localhost:5672", ConnectionProperties::default())
        .await
        .expect("Failed to connect to LavinMQ");
    conn.create_channel()
        .await
        .expect("Failed to create channel")
}

fn classic_queue_opts() -> QueueDeclareOptions {
    QueueDeclareOptions {
        auto_delete: true,
        ..QueueDeclareOptions::default()
    }
}

fn stream_queue_opts() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    }
}
```

Then refactor the three existing tests to use `connect_channel()` in place of their inline `Connection::connect(...).create_channel()` calls. Replace the first seven-or-so lines of each test's async block with:

```rust
let channel = connect_channel().await;
```

- [ ] **Step 2: Run the existing tests to make sure the refactor is clean**

Run: `cargo test`
Expected: the three existing tests still pass.

- [ ] **Step 3: Commit**

```bash
git add src/tests.rs
git commit -m "Add connect_channel + queue-opts helpers in tests"
```

---

## Task 4: Integer-valued argument newtypes (8 arguments)

Adds `MaxLength`, `MaxLengthBytes`, `MessageTtl`, `Expires`, `DeliveryLimit`, `ConsumerTimeout`, `CacheSize`, `CacheTtl` — all unsigned-integer args in the shared Classic/Priority set.

**Files:**
- Create: `src/arguments.rs`
- Modify: `src/lib.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Create `src/arguments.rs` with the eight integer newtypes**

```rust
// src/arguments.rs
use lapin::types::{AMQPValue, FieldTable, ShortString};
use quickcheck::{Arbitrary, Gen};

/// `x-max-length` — max number of messages.
#[derive(Clone, Debug)]
pub struct MaxLength(pub u32);

impl MaxLength {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-length"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for MaxLength {
    fn arbitrary(g: &mut Gen) -> Self {
        MaxLength(u32::arbitrary(g))
    }
}

/// `x-max-length-bytes` — max total byte size of messages.
#[derive(Clone, Debug)]
pub struct MaxLengthBytes(pub u64);

impl MaxLengthBytes {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-length-bytes"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for MaxLengthBytes {
    fn arbitrary(g: &mut Gen) -> Self {
        MaxLengthBytes(u64::arbitrary(g))
    }
}

/// `x-message-ttl` — per-message TTL in milliseconds.
#[derive(Clone, Debug)]
pub struct MessageTtl(pub u32);

impl MessageTtl {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-message-ttl"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for MessageTtl {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageTtl(u32::arbitrary(g))
    }
}

/// `x-expires` — queue-level idle TTL in milliseconds (must be ≥ 1).
#[derive(Clone, Debug)]
pub struct Expires(pub u32);

impl Expires {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-expires"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for Expires {
    fn arbitrary(g: &mut Gen) -> Self {
        // Spec requires >= 1.
        let v = u32::arbitrary(g).saturating_add(1);
        Expires(v)
    }
}

/// `x-delivery-limit` — max redelivery attempts before dead-lettering.
#[derive(Clone, Debug)]
pub struct DeliveryLimit(pub u32);

impl DeliveryLimit {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-delivery-limit"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for DeliveryLimit {
    fn arbitrary(g: &mut Gen) -> Self {
        DeliveryLimit(u32::arbitrary(g))
    }
}

/// `x-consumer-timeout` — max consumer idle time in milliseconds.
#[derive(Clone, Debug)]
pub struct ConsumerTimeout(pub u32);

impl ConsumerTimeout {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-consumer-timeout"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for ConsumerTimeout {
    fn arbitrary(g: &mut Gen) -> Self {
        ConsumerTimeout(u32::arbitrary(g))
    }
}

/// `x-cache-size` — dedup cache capacity.
#[derive(Clone, Debug)]
pub struct CacheSize(pub u32);

impl CacheSize {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-cache-size"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for CacheSize {
    fn arbitrary(g: &mut Gen) -> Self {
        CacheSize(u32::arbitrary(g))
    }
}

/// `x-cache-ttl` — dedup cache entry TTL in milliseconds.
#[derive(Clone, Debug)]
pub struct CacheTtl(pub u32);

impl CacheTtl {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-cache-ttl"),
            AMQPValue::LongLongInt(self.0 as i64),
        );
    }
}

impl Arbitrary for CacheTtl {
    fn arbitrary(g: &mut Gen) -> Self {
        CacheTtl(u32::arbitrary(g))
    }
}
```

- [ ] **Step 2: Re-export the module from `src/lib.rs`**

Add to `src/lib.rs` after `pub mod names;`:

```rust
pub mod arguments;
```

- [ ] **Step 3: Add a declare-only helper in `src/tests.rs`**

Insert below the `stream_queue_opts()` helper:

```rust
async fn declare_classic_ok(channel: &lapin::Channel, name: &str, args: FieldTable) -> bool {
    let res = channel
        .queue_declare(name, classic_queue_opts(), args)
        .await
        .is_ok();
    // Cleanup (best-effort; ignore errors — channel may already be closed on failure).
    let _ = channel
        .queue_delete(name, QueueDeleteOptions::default())
        .await;
    res
}
```

- [ ] **Step 4: Add per-argument tests for all eight integer newtypes**

Append to `src/tests.rs`:

```rust
use crate::arguments::{
    CacheSize, CacheTtl, ConsumerTimeout, DeliveryLimit, Expires, MaxLength, MaxLengthBytes,
    MessageTtl,
};

macro_rules! single_arg_classic_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, arg: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let mut table = FieldTable::default();
                arg.insert_into(&mut table);
                declare_classic_ok(&channel, &name.0, table).await
            })
        }
    };
}

single_arg_classic_test!(declare_with_max_length, MaxLength);
single_arg_classic_test!(declare_with_max_length_bytes, MaxLengthBytes);
single_arg_classic_test!(declare_with_message_ttl, MessageTtl);
single_arg_classic_test!(declare_with_expires, Expires);
single_arg_classic_test!(declare_with_delivery_limit, DeliveryLimit);
single_arg_classic_test!(declare_with_consumer_timeout, ConsumerTimeout);
single_arg_classic_test!(declare_with_cache_size, CacheSize);
single_arg_classic_test!(declare_with_cache_ttl, CacheTtl);
```

- [ ] **Step 5: Run the new tests**

Run: `cargo test declare_with_`
Expected: all eight new tests pass against LavinMQ. The three existing round-trip tests are unaffected.

- [ ] **Step 6: Commit**

```bash
git add src/arguments.rs src/lib.rs src/tests.rs
git commit -m "Add integer queue-argument newtypes and per-arg tests"
```

---

## Task 5: `Overflow` enum-valued argument

Adds `x-overflow` — a string from the set `{drop-head, reject-publish}`.

**Files:**
- Modify: `src/arguments.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add `OverflowKind` and `Overflow` to `src/arguments.rs`**

Append:

```rust
#[derive(Clone, Debug)]
pub enum OverflowKind {
    DropHead,
    RejectPublish,
}

impl OverflowKind {
    fn as_str(&self) -> &'static str {
        match self {
            OverflowKind::DropHead => "drop-head",
            OverflowKind::RejectPublish => "reject-publish",
        }
    }
}

/// `x-overflow` — behaviour when a length limit is hit.
#[derive(Clone, Debug)]
pub struct Overflow(pub OverflowKind);

impl Overflow {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-overflow"),
            AMQPValue::LongString(self.0.as_str().into()),
        );
    }
}

impl Arbitrary for Overflow {
    fn arbitrary(g: &mut Gen) -> Self {
        let kind = if bool::arbitrary(g) {
            OverflowKind::DropHead
        } else {
            OverflowKind::RejectPublish
        };
        Overflow(kind)
    }
}
```

- [ ] **Step 2: Add the per-argument test**

Append to `src/tests.rs`:

```rust
use crate::arguments::Overflow;

single_arg_classic_test!(declare_with_overflow, Overflow);
```

- [ ] **Step 3: Run the new test**

Run: `cargo test declare_with_overflow`
Expected: passes against LavinMQ.

- [ ] **Step 4: Commit**

```bash
git add src/arguments.rs src/tests.rs
git commit -m "Add Overflow queue argument + per-arg test"
```

---

## Task 6: Boolean argument newtypes (2 arguments)

Adds `SingleActiveConsumer` and `MessageDeduplication`.

**Files:**
- Modify: `src/arguments.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add the two boolean newtypes to `src/arguments.rs`**

```rust
/// `x-single-active-consumer` — only one consumer active at a time.
#[derive(Clone, Debug)]
pub struct SingleActiveConsumer(pub bool);

impl SingleActiveConsumer {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-single-active-consumer"),
            AMQPValue::Boolean(self.0),
        );
    }
}

impl Arbitrary for SingleActiveConsumer {
    fn arbitrary(g: &mut Gen) -> Self {
        SingleActiveConsumer(bool::arbitrary(g))
    }
}

/// `x-message-deduplication` — enable dedup on this queue.
#[derive(Clone, Debug)]
pub struct MessageDeduplication(pub bool);

impl MessageDeduplication {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-message-deduplication"),
            AMQPValue::Boolean(self.0),
        );
    }
}

impl Arbitrary for MessageDeduplication {
    fn arbitrary(g: &mut Gen) -> Self {
        MessageDeduplication(bool::arbitrary(g))
    }
}
```

- [ ] **Step 2: Add per-argument tests**

Append to `src/tests.rs`:

```rust
use crate::arguments::{MessageDeduplication, SingleActiveConsumer};

single_arg_classic_test!(declare_with_single_active_consumer, SingleActiveConsumer);
single_arg_classic_test!(declare_with_message_deduplication, MessageDeduplication);
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test "declare_with_(single_active_consumer|message_deduplication)"`
Expected: both pass.

- [ ] **Step 4: Commit**

```bash
git add src/arguments.rs src/tests.rs
git commit -m "Add boolean queue arguments + per-arg tests"
```

---

## Task 7: String-valued name arguments (3 arguments)

Adds `DeadLetterExchange`, `DeadLetterRoutingKey`, `DeduplicationHeader`.

**Files:**
- Modify: `src/arguments.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add the three string newtypes to `src/arguments.rs`**

```rust
use crate::names::{QUEUE_NAME_CHARS, RESERVED_QUEUE_PREFIX, RoutingKey};

/// `x-dead-letter-exchange` — exchange dead-letters are republished to.
#[derive(Clone, Debug)]
pub struct DeadLetterExchange(pub String);

impl DeadLetterExchange {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-dead-letter-exchange"),
            AMQPValue::LongString(self.0.clone().into()),
        );
    }
}

impl Arbitrary for DeadLetterExchange {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        loop {
            let name: String = (0..len)
                .map(|_| {
                    let &byte = g.choose(QUEUE_NAME_CHARS).unwrap();
                    byte as char
                })
                .collect();
            if !name.starts_with(RESERVED_QUEUE_PREFIX) {
                return DeadLetterExchange(name);
            }
        }
    }
}

/// `x-dead-letter-routing-key` — routing key used when dead-lettering.
#[derive(Clone, Debug)]
pub struct DeadLetterRoutingKey(pub RoutingKey);

impl DeadLetterRoutingKey {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-dead-letter-routing-key"),
            AMQPValue::LongString(self.0.0.clone().into()),
        );
    }
}

impl Arbitrary for DeadLetterRoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        DeadLetterRoutingKey(RoutingKey::arbitrary(g))
    }
}

/// `x-deduplication-header` — message-header name carrying the dedup key.
#[derive(Clone, Debug)]
pub struct DeduplicationHeader(pub String);

const HEADER_NAME_CHARS: &[u8] =
    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

impl DeduplicationHeader {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-deduplication-header"),
            AMQPValue::LongString(self.0.clone().into()),
        );
    }
}

impl Arbitrary for DeduplicationHeader {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 64);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        let name: String = (0..len)
            .map(|_| {
                let &byte = g.choose(HEADER_NAME_CHARS).unwrap();
                byte as char
            })
            .collect();
        DeduplicationHeader(name)
    }
}
```

- [ ] **Step 2: Add per-argument tests**

Append to `src/tests.rs`:

```rust
use crate::arguments::{DeadLetterExchange, DeadLetterRoutingKey, DeduplicationHeader};

single_arg_classic_test!(declare_with_dead_letter_exchange, DeadLetterExchange);
single_arg_classic_test!(declare_with_dead_letter_routing_key, DeadLetterRoutingKey);
single_arg_classic_test!(declare_with_deduplication_header, DeduplicationHeader);
```

- [ ] **Step 3: Run the new tests**

Run: `cargo test "declare_with_(dead_letter|deduplication_header)"`
Expected: all three pass.

- [ ] **Step 4: Commit**

```bash
git add src/arguments.rs src/tests.rs
git commit -m "Add string queue arguments + per-arg tests"
```

---

## Task 8: `MaxPriority` + priority-queue per-arg test

Adds the `x-max-priority` argument, whose presence turns a classic queue into a priority queue. Declaring with only this argument is effectively the per-arg test for priority queues.

**Files:**
- Modify: `src/arguments.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add `MaxPriority` to `src/arguments.rs`**

```rust
/// `x-max-priority` — turns the queue into a priority queue (0..=255).
#[derive(Clone, Debug)]
pub struct MaxPriority(pub u8);

impl MaxPriority {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-priority"),
            AMQPValue::ShortShortUInt(self.0),
        );
    }
}

impl Arbitrary for MaxPriority {
    fn arbitrary(g: &mut Gen) -> Self {
        MaxPriority(u8::arbitrary(g))
    }
}
```

- [ ] **Step 2: Add the per-argument test**

Append to `src/tests.rs`:

```rust
use crate::arguments::MaxPriority;

single_arg_classic_test!(declare_with_max_priority, MaxPriority);
```

(The classic-queue opts are still correct: a priority queue is declared exactly like a classic queue, just with `x-max-priority` present.)

- [ ] **Step 3: Run the test**

Run: `cargo test declare_with_max_priority`
Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/arguments.rs src/tests.rs
git commit -m "Add MaxPriority + priority queue per-arg test"
```

---

## Task 9: `MaxAge` + stream-queue per-arg test

Adds `x-max-age` (stream-only, duration string `"<n><unit>"`) and the first stream-queue test path.

**Files:**
- Modify: `src/arguments.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add `MaxAge` to `src/arguments.rs`**

```rust
/// `x-max-age` — stream retention by age. Format: N + unit, where
/// unit ∈ {Y, M, D, h, m, s}. Example: "7D", "12h".
#[derive(Clone, Debug)]
pub struct MaxAge(pub String);

const MAX_AGE_UNITS: &[char] = &['Y', 'M', 'D', 'h', 'm', 's'];

impl MaxAge {
    pub fn insert_into(&self, table: &mut FieldTable) {
        table.insert(
            ShortString::from("x-max-age"),
            AMQPValue::LongString(self.0.clone().into()),
        );
    }
}

impl Arbitrary for MaxAge {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = *g.choose(&(1u32..=999).collect::<Vec<_>>()).unwrap();
        let unit = *g.choose(MAX_AGE_UNITS).unwrap();
        MaxAge(format!("{}{}", n, unit))
    }
}
```

- [ ] **Step 2: Add a stream declare helper in `src/tests.rs`**

Insert below `declare_classic_ok`:

```rust
async fn declare_stream_ok(channel: &lapin::Channel, name: &str, mut args: FieldTable) -> bool {
    args.insert(
        lapin::types::ShortString::from("x-queue-type"),
        lapin::types::AMQPValue::LongString("stream".into()),
    );
    let res = channel
        .queue_declare(name, stream_queue_opts(), args)
        .await
        .is_ok();
    let _ = channel
        .queue_delete(name, QueueDeleteOptions::default())
        .await;
    res
}
```

- [ ] **Step 3: Add the per-argument stream test**

Append to `src/tests.rs`:

```rust
use crate::arguments::MaxAge;

macro_rules! single_arg_stream_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, arg: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let mut table = FieldTable::default();
                arg.insert_into(&mut table);
                declare_stream_ok(&channel, &name.0, table).await
            })
        }
    };
}

single_arg_stream_test!(declare_stream_with_max_age, MaxAge);
```

- [ ] **Step 4: Run the test**

Run: `cargo test declare_stream_with_max_age`
Expected: passes against LavinMQ.

- [ ] **Step 5: Commit**

```bash
git add src/arguments.rs src/tests.rs
git commit -m "Add MaxAge + stream-queue per-arg test"
```

---

## Task 10: `ClassicQueueArgs` + combination test

Covers the "random subset of classic/priority arguments" combination flavour.

**Files:**
- Create: `src/combined.rs`
- Modify: `src/lib.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Create `src/combined.rs` with `ClassicQueueArgs`**

```rust
// src/combined.rs
use lapin::types::FieldTable;
use quickcheck::{Arbitrary, Gen};

use crate::arguments::{
    CacheSize, CacheTtl, ConsumerTimeout, DeadLetterExchange, DeadLetterRoutingKey,
    DeduplicationHeader, DeliveryLimit, Expires, MaxLength, MaxLengthBytes, MessageDeduplication,
    MessageTtl, Overflow, SingleActiveConsumer,
};

#[derive(Clone, Debug)]
pub struct ClassicQueueArgs {
    pub max_length: Option<MaxLength>,
    pub max_length_bytes: Option<MaxLengthBytes>,
    pub overflow: Option<Overflow>,
    pub message_ttl: Option<MessageTtl>,
    pub expires: Option<Expires>,
    pub dead_letter_exchange: Option<DeadLetterExchange>,
    pub dead_letter_routing_key: Option<DeadLetterRoutingKey>,
    pub delivery_limit: Option<DeliveryLimit>,
    pub consumer_timeout: Option<ConsumerTimeout>,
    pub single_active_consumer: Option<SingleActiveConsumer>,
    pub message_deduplication: Option<MessageDeduplication>,
    pub deduplication_header: Option<DeduplicationHeader>,
    pub cache_size: Option<CacheSize>,
    pub cache_ttl: Option<CacheTtl>,
}

impl ClassicQueueArgs {
    pub fn apply(&self, table: &mut FieldTable) {
        if let Some(a) = &self.max_length {
            a.insert_into(table);
        }
        if let Some(a) = &self.max_length_bytes {
            a.insert_into(table);
        }
        if let Some(a) = &self.overflow {
            a.insert_into(table);
        }
        if let Some(a) = &self.message_ttl {
            a.insert_into(table);
        }
        if let Some(a) = &self.expires {
            a.insert_into(table);
        }
        if let Some(a) = &self.dead_letter_exchange {
            a.insert_into(table);
        }
        if let Some(a) = &self.dead_letter_routing_key {
            a.insert_into(table);
        }
        if let Some(a) = &self.delivery_limit {
            a.insert_into(table);
        }
        if let Some(a) = &self.consumer_timeout {
            a.insert_into(table);
        }
        if let Some(a) = &self.single_active_consumer {
            a.insert_into(table);
        }
        if let Some(a) = &self.message_deduplication {
            a.insert_into(table);
        }
        if let Some(a) = &self.deduplication_header {
            a.insert_into(table);
        }
        if let Some(a) = &self.cache_size {
            a.insert_into(table);
        }
        if let Some(a) = &self.cache_ttl {
            a.insert_into(table);
        }
    }
}

impl Arbitrary for ClassicQueueArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        ClassicQueueArgs {
            max_length: Option::arbitrary(g),
            max_length_bytes: Option::arbitrary(g),
            overflow: Option::arbitrary(g),
            message_ttl: Option::arbitrary(g),
            expires: Option::arbitrary(g),
            dead_letter_exchange: Option::arbitrary(g),
            dead_letter_routing_key: Option::arbitrary(g),
            delivery_limit: Option::arbitrary(g),
            consumer_timeout: Option::arbitrary(g),
            single_active_consumer: Option::arbitrary(g),
            message_deduplication: Option::arbitrary(g),
            deduplication_header: Option::arbitrary(g),
            cache_size: Option::arbitrary(g),
            cache_ttl: Option::arbitrary(g),
        }
    }
}
```

- [ ] **Step 2: Register the module in `src/lib.rs`**

Add after `pub mod arguments;`:

```rust
pub mod combined;
```

- [ ] **Step 3: Add the combination test**

Append to `src/tests.rs`:

```rust
use crate::combined::ClassicQueueArgs;

#[quickcheck]
fn declare_classic_with_combined_args(name: QueueName, args: ClassicQueueArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        args.apply(&mut table);
        declare_classic_ok(&channel, &name.0, table).await
    })
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test declare_classic_with_combined_args`
Expected: passes against LavinMQ.

- [ ] **Step 5: Commit**

```bash
git add src/combined.rs src/lib.rs src/tests.rs
git commit -m "Add ClassicQueueArgs combined type + combination test"
```

---

## Task 11: `PriorityQueueArgs` + combination test

**Files:**
- Modify: `src/combined.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add `PriorityQueueArgs` to `src/combined.rs`**

Append:

```rust
use crate::arguments::MaxPriority;

#[derive(Clone, Debug)]
pub struct PriorityQueueArgs {
    pub max_priority: MaxPriority,
    pub classic: ClassicQueueArgs,
}

impl PriorityQueueArgs {
    pub fn apply(&self, table: &mut FieldTable) {
        self.max_priority.insert_into(table);
        self.classic.apply(table);
    }
}

impl Arbitrary for PriorityQueueArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        PriorityQueueArgs {
            max_priority: MaxPriority::arbitrary(g),
            classic: ClassicQueueArgs::arbitrary(g),
        }
    }
}
```

- [ ] **Step 2: Add the combination test**

Append to `src/tests.rs`:

```rust
use crate::combined::PriorityQueueArgs;

#[quickcheck]
fn declare_priority_with_combined_args(name: QueueName, args: PriorityQueueArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        args.apply(&mut table);
        declare_classic_ok(&channel, &name.0, table).await
    })
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test declare_priority_with_combined_args`
Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/combined.rs src/tests.rs
git commit -m "Add PriorityQueueArgs combined type + combination test"
```

---

## Task 12: `StreamQueueArgs` + combination test

**Files:**
- Modify: `src/combined.rs`
- Modify: `src/tests.rs`

- [ ] **Step 1: Add `StreamQueueArgs` to `src/combined.rs`**

Append:

```rust
use crate::arguments::MaxAge;

#[derive(Clone, Debug)]
pub struct StreamQueueArgs {
    pub max_length: Option<MaxLength>,
    pub max_length_bytes: Option<MaxLengthBytes>,
    pub max_age: Option<MaxAge>,
}

impl StreamQueueArgs {
    /// Inserts all set arguments PLUS x-queue-type: "stream".
    pub fn apply(&self, table: &mut FieldTable) {
        table.insert(
            lapin::types::ShortString::from("x-queue-type"),
            lapin::types::AMQPValue::LongString("stream".into()),
        );
        if let Some(a) = &self.max_length {
            a.insert_into(table);
        }
        if let Some(a) = &self.max_length_bytes {
            a.insert_into(table);
        }
        if let Some(a) = &self.max_age {
            a.insert_into(table);
        }
    }
}

impl Arbitrary for StreamQueueArgs {
    fn arbitrary(g: &mut Gen) -> Self {
        StreamQueueArgs {
            max_length: Option::arbitrary(g),
            max_length_bytes: Option::arbitrary(g),
            max_age: Option::arbitrary(g),
        }
    }
}
```

- [ ] **Step 2: Add the combination test**

Append to `src/tests.rs`. Because `StreamQueueArgs::apply` already inserts `x-queue-type`, the test uses `stream_queue_opts()` directly rather than `declare_stream_ok` (which would double-insert the key).

```rust
use crate::combined::StreamQueueArgs;

#[quickcheck]
fn declare_stream_with_combined_args(name: QueueName, args: StreamQueueArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        args.apply(&mut table);
        let ok = channel
            .queue_declare(&name.0, stream_queue_opts(), table)
            .await
            .is_ok();
        let _ = channel
            .queue_delete(&name.0, QueueDeleteOptions::default())
            .await;
        ok
    })
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test declare_stream_with_combined_args`
Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/combined.rs src/tests.rs
git commit -m "Add StreamQueueArgs combined type + combination test"
```

---

## Task 13: Stream rejection tests (7 arguments)

Verify that LavinMQ rejects each of the seven forbidden-on-stream arguments with `PRECONDITION_FAILED` (AMQP reply code 406).

**Files:**
- Modify: `src/tests.rs`

- [ ] **Step 1: Add a rejection helper to `src/tests.rs`**

Insert below `declare_stream_ok`:

```rust
/// Returns true iff `queue_declare` fails with AMQP reply code 406 (PRECONDITION_FAILED).
/// Uses a string-based check against the error's `Debug` output rather than matching on
/// lapin's error enum variants, which change shape between 2.x versions. The Debug repr
/// reliably contains either "406" (the numeric code) or "PRECONDITION_FAILED" (the name)
/// for this broker-originated channel-close error.
async fn declare_stream_rejects(
    channel: &lapin::Channel,
    name: &str,
    mut args: FieldTable,
) -> bool {
    args.insert(
        lapin::types::ShortString::from("x-queue-type"),
        lapin::types::AMQPValue::LongString("stream".into()),
    );
    match channel
        .queue_declare(name, stream_queue_opts(), args)
        .await
    {
        Ok(_) => {
            // Unexpected success — clean up so we don't leak.
            let _ = channel
                .queue_delete(name, QueueDeleteOptions::default())
                .await;
            false
        }
        Err(err) => {
            let msg = format!("{err:?}");
            msg.contains("406") || msg.contains("PRECONDITION_FAILED")
        }
    }
}
```

- [ ] **Step 2: Add a macro and the seven rejection tests**

Append to `src/tests.rs`:

```rust
macro_rules! stream_rejection_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, arg: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let mut table = FieldTable::default();
                arg.insert_into(&mut table);
                declare_stream_rejects(&channel, &name.0, table).await
            })
        }
    };
}

stream_rejection_test!(stream_rejects_dead_letter_exchange, DeadLetterExchange);
stream_rejection_test!(stream_rejects_dead_letter_routing_key, DeadLetterRoutingKey);
stream_rejection_test!(stream_rejects_expires, Expires);
stream_rejection_test!(stream_rejects_delivery_limit, DeliveryLimit);
stream_rejection_test!(stream_rejects_overflow, Overflow);
stream_rejection_test!(stream_rejects_single_active_consumer, SingleActiveConsumer);
stream_rejection_test!(stream_rejects_max_priority, MaxPriority);
```

- [ ] **Step 3: Run the rejection tests**

Run: `cargo test stream_rejects_`
Expected: all seven pass. Each iteration must observe a 406 `PRECONDITION_FAILED` from LavinMQ.

- [ ] **Step 4: Run the full test suite to catch regressions**

Run: `cargo test`
Expected: all tests pass — the 3 original round-trip tests, 16 per-argument tests, 3 combination tests, and 7 rejection tests (29 total).

- [ ] **Step 5: Commit**

```bash
git add src/tests.rs
git commit -m "Add stream rejection tests for forbidden queue arguments"
```

---

## Task 14: Final polish

**Files:**
- Modify: source files as needed

- [ ] **Step 1: Run `cargo fmt`**

Run: `cargo fmt`
Expected: no diff, or minor formatting touch-ups applied.

- [ ] **Step 2: Run `cargo clippy`**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean. Fix any warnings flagged (most likely unused imports or needless-clones).

- [ ] **Step 3: Run the full test suite once more**

Run: `cargo test`
Expected: all 29 tests pass.

- [ ] **Step 4: Commit any polish changes**

```bash
git add -A
git commit -m "Format and lint cleanup"
```

(If there are no changes, skip the commit.)

---

## Self-Review Notes

- **Spec coverage:**
  - Per-argument tests: 14 shared × 1 + MaxPriority + MaxAge = 16 ✅ (Tasks 4–9)
  - Combination tests: ClassicQueueArgs, PriorityQueueArgs, StreamQueueArgs = 3 ✅ (Tasks 10–12)
  - Rejection tests: 7 stream-forbidden args ✅ (Task 13)
  - Module split ✅ (Task 1)
  - AGENTS.md update ✅ (Task 2)
  - LavinMQ target, broker URL unchanged ✅
  - Out-of-scope items (policy args, MQTT, stream consumer args, invalid values, behavioural verification) are correctly absent from the plan.
- **Type consistency:** `insert_into(&self, &mut FieldTable)` signature is used everywhere. `apply(&self, &mut FieldTable)` is the combined-struct equivalent. `declare_classic_ok` / `declare_stream_ok` / `declare_stream_rejects` all take `(&Channel, &str, FieldTable)` and return `bool`. Macro names (`single_arg_classic_test`, `single_arg_stream_test`, `stream_rejection_test`) are consistent.
- **Placeholders:** None. Every code step shows actual code. The one adjustment note in Task 13 Step 1 (on lapin error shape) gives a concrete `grep` command to resolve if lapin's internals differ from the expected shape.
