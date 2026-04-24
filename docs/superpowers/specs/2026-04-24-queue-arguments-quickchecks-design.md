# Queue-Arguments QuickChecks — Design

Date: 2026-04-24

## Background

`amqp-quickcheck` currently provides `Arbitrary` impls for `QueueName`,
`RoutingKey`, and `TopicRoutingKey`, with integration tests that round-trip
messages through a real broker on `amqp://localhost:5672`.

`queue-arguments.md` documents every `x-*` queue argument LavinMQ recognises
on `queue.declare`. This spec extends the crate with QuickCheck-based
tests that exercise those arguments against a real LavinMQ broker.

## Goals

1. Generate broker-valid values for every `x-*` queue argument in
   `queue-arguments.md` (Classic, Priority, Stream).
2. Cover three test flavours:
   - **Per-argument**: declare a queue with exactly one `x-*` argument set.
   - **Combination**: declare with a random subset of compatible arguments.
   - **Rejection**: declare a stream with a forbidden argument and verify
     it fails with `PRECONDITION_FAILED`.
4. Update project docs to reflect that tests target LavinMQ, not RabbitMQ.

## Non-goals

- Behavioural verification of argument effects (e.g., actually triggering
  `x-overflow` by publishing past `x-max-length`). Tests are **declare-only**.
- Policy-only arguments (`federation-upstream*`) — they require the
  management API, not `queue.declare`.
- MQTT session queue (`x-queue-type: "mqtt"`) — internally declared only.
- `x-delay` — a message header, not a queue argument.
- Stream consumer arguments (`x-stream-offset`, `x-stream-filter`, etc.) —
  passed on `basic.consume`, not queue declare. Out of scope for this pass.
- Testing invalid / out-of-range values — generators produce only
  broker-valid values.

## Target broker

LavinMQ on `amqp://localhost:5672` with default credentials. `AGENTS.md`
will be updated accordingly (currently says RabbitMQ).

## File layout

Existing `src/lib.rs` is split into focused modules:

```
src/
  lib.rs        — crate docs + re-exports of the public API
  names.rs      — QueueName, RoutingKey, TopicRoutingKey (moved verbatim)
  arguments.rs  — per-argument newtypes + Arbitrary impls + insert_into helpers
  combined.rs   — ClassicQueueArgs, PriorityQueueArgs, StreamQueueArgs
  tests.rs      — #[cfg(test)] mod tests: connect_channel() helper +
                  per-argument, combination, and rejection tests
```

Each file stays ≲400 lines. The existing three round-trip tests in
`lib.rs` move to `tests.rs` unchanged.

## Per-argument types

One newtype per argument. Each has:
- `Arbitrary` producing broker-valid values.
- `fn insert_into(&self, table: &mut lapin::types::FieldTable)` that inserts
  the correctly-typed `AMQPValue` under the right `x-*` key.

### Shared by Classic + Priority queues

| Newtype | Value generation | AMQP key |
| --- | --- | --- |
| `MaxLength(u32)` | any `u32` | `x-max-length` |
| `MaxLengthBytes(u64)` | any `u64` | `x-max-length-bytes` |
| `Overflow(OverflowKind)` | enum `DropHead` \| `RejectPublish` → string | `x-overflow` |
| `MessageTtl(u32)` | any `u32` ms | `x-message-ttl` |
| `Expires(u32)` | `1..=u32::MAX` ms | `x-expires` |
| `DeadLetterExchange(String)` | 1–255 chars from `VALID_CHARS` (same alphabet as `QueueName`), not starting with `amq.` | `x-dead-letter-exchange` |
| `DeadLetterRoutingKey(RoutingKey)` | reuses existing `RoutingKey` | `x-dead-letter-routing-key` |
| `DeliveryLimit(u32)` | any `u32` | `x-delivery-limit` |
| `ConsumerTimeout(u32)` | any `u32` ms | `x-consumer-timeout` |
| `SingleActiveConsumer(bool)` | any `bool` | `x-single-active-consumer` |
| `MessageDeduplication(bool)` | any `bool` | `x-message-deduplication` |
| `DeduplicationHeader(String)` | 1–64 chars from `[A-Za-z0-9_-]` (conservative AMQP header-name alphabet) | `x-deduplication-header` |
| `CacheSize(u32)` | any `u32` | `x-cache-size` |
| `CacheTtl(u32)` | any `u32` ms | `x-cache-ttl` |

### Priority-only

| Newtype | Value generation | AMQP key |
| --- | --- | --- |
| `MaxPriority(u8)` | `0..=255` | `x-max-priority` |

### Stream-only

| Newtype | Value generation | AMQP key |
| --- | --- | --- |
| `MaxAge(String)` | number (1..=999) + unit from `Y` / `M` / `D` / `h` / `m` / `s`, e.g. `"7D"`, `"12h"`, `"1M"` | `x-max-age` |

`MaxLength` and `MaxLengthBytes` are reused on streams. Stream declarations
also insert `x-queue-type: "stream"` (not modelled as a newtype — handled
by `StreamQueueArgs::apply`).

### AMQP wire types

- Integer arguments → whichever of lapin's `AMQPValue` integer variants
  LavinMQ accepts for that key (e.g. `LongLongInt` for counts, `LongUInt`
  for millisecond durations). The exact choice per key is an implementation
  detail; LavinMQ parses flexibly.
- Boolean arguments → `AMQPValue::Boolean`.
- String arguments → `AMQPValue::LongString`.

## Combined types

Each field is `Option<T>`; `Arbitrary` uses the derived `Option::arbitrary`,
giving roughly a 50/50 mix per field. `apply(&self, &mut FieldTable)`
inserts only `Some` fields.

```rust
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

pub struct PriorityQueueArgs {
    pub max_priority: MaxPriority,   // required — makes it a priority queue
    pub classic: ClassicQueueArgs,   // all classic args legal on priority
}

pub struct StreamQueueArgs {
    pub max_length: Option<MaxLength>,
    pub max_length_bytes: Option<MaxLengthBytes>,
    pub max_age: Option<MaxAge>,
}
```

`StreamQueueArgs::apply` inserts `x-queue-type: "stream"` unconditionally.

## Test flavours

All live in `src/tests.rs` as `#[quickcheck]` functions. Each spins up its
own `tokio::runtime::Runtime` (existing pattern), connects to LavinMQ via
a shared helper:

```rust
async fn connect_channel() -> lapin::Channel { /* amqp://localhost:5672 */ }
```

### Per-argument tests

One test per argument. Declares a queue with exactly that one argument set,
asserts declare succeeds, deletes the queue. Example shape:

```rust
#[quickcheck]
fn declare_with_max_length(name: QueueName, arg: MaxLength) -> bool {
    let mut table = FieldTable::default();
    arg.insert_into(&mut table);
    // declare classic queue with table, assert ok, delete
}
```

- Shared args (14): classic queue (auto-delete, not durable).
- `MaxPriority`: priority queue (same declare options, just with the arg).
- `MaxAge`: stream queue — `durable: true`, no auto-delete, no exclusive;
  explicit `queue_delete` at end.

### Combination tests

One per queue family — QuickCheck covers subsets of varying size automatically.

```rust
#[quickcheck]
fn declare_classic_with_combined_args(name: QueueName, args: ClassicQueueArgs) -> bool;
#[quickcheck]
fn declare_priority_with_combined_args(name: QueueName, args: PriorityQueueArgs) -> bool;
#[quickcheck]
fn declare_stream_with_combined_args(name: QueueName, args: StreamQueueArgs) -> bool;
```

Each applies all `Some` fields, declares, asserts success, deletes.

### Rejection tests (stream-only)

One per rejected-on-stream argument (per `queue-arguments.md §Rejected`):

- `x-dead-letter-exchange`
- `x-dead-letter-routing-key`
- `x-expires`
- `x-delivery-limit`
- `x-overflow`
- `x-single-active-consumer`
- `x-max-priority`

Each test declares a stream queue with that argument and expects the
declaration to fail with AMQP reply code `406` (`PRECONDITION_FAILED`).
Because failed declares close the channel, each `#[quickcheck]` iteration
opens a fresh channel via `connect_channel()`. Returning `false` (or getting
any other error shape) is a test failure.

### Cleanup

- Classic / Priority: `auto_delete: true` plus explicit `queue_delete` at
  the end (matches existing pattern).
- Stream: `auto_delete` is incompatible with streams, so tests explicitly
  `queue_delete` in both success and negative tests. Panics on
  QuickCheck-failing iterations may leak queues — acceptable trade-off;
  those signal real bugs worth investigating anyway.

### Test count

| Flavour | Count |
| --- | --- |
| Per-argument | ~16 (14 shared + 1 priority + 1 stream) |
| Combination | 3 |
| Rejection | 7 |
| **New total** | **~26** |

Plus the 3 existing round-trip tests, unchanged (moved to `tests.rs`).

## AGENTS.md updates

- Project overview: "tests target LavinMQ" (not RabbitMQ).
- External dependencies: Docker command updated to the public LavinMQ
  image (`cloudamqp/lavinmq` or equivalent).
- Project structure: reflect the new `names.rs` / `arguments.rs` /
  `combined.rs` / `tests.rs` split.
- Testing approach: note the three test flavours and the
  `connect_channel()` helper.

## Open implementation details (to resolve during coding)

- Exact `AMQPValue` integer variant per key (`LongLongInt` vs `LongUInt` vs
  `ShortInt`). LavinMQ parses flexibly; we pick whichever lapin's
  `FieldTable` produces most naturally, verified by a smoke declare.
- Whether to expose `insert_into` on the combined structs via a trait or a
  plain inherent method. Leaning toward a plain method — we have only three
  consumers and no polymorphism need.
