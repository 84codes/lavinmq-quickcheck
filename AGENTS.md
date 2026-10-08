# AGENTS.md

## Project Overview

`lavinmq-quickcheck` is a Rust library that provides [QuickCheck](https://crates.io/crates/quickcheck) `Arbitrary` implementations for AMQP and MQTT 3.1.1 types, enabling property-based testing of LavinMQ. `src/http/` reuses them to check that the HTTP management API behaves like AMQP. Currently implements generators for queue names, routing keys, topic routing keys, the full set of LavinMQ queue-declaration arguments, `BasicProperties` fields, acyclic routing topologies (exchanges, queues, bindings, dead-lettering), consistent-hash exchange scenarios, stream consumer `x-stream-offset` values, delayed-message exchanges, consumer `x-priority` values, headers-exchange routing, and MQTT topics/filters, QoS, retained messages, sessions and malformed packets (`src/mqtt/`).

## Commands

```sh
# Build
cargo build

# Run all tests (requires a running LavinMQ instance on localhost:5672 and :1883)
cargo test

# Check without building
cargo check

# Format
cargo fmt

# Lint
cargo clippy
```

## Release / CI binary

`.github/workflows/release.yml` builds the test suite as one static musl executable (`cargo test --release --no-run --target x86_64-unknown-linux-musl`). It smoke-tests the binary against a LavinMQ container and, on `v*` tags, publishes it to a GitHub Release. `docs/lavinmq-ci.md` has example jobs for running it in LavinMQ's CI. To build locally you need a musl C compiler for `ring` (`musl-gcc` from `musl-tools`).

## External Dependencies (Runtime)

Tests require a **LavinMQ server** running on `amqp://localhost:5672` with default credentials (`guest:guest`), its management HTTP API on `http://localhost:15672` (used by the policy tests), and MQTT on `localhost:1883`. Without it, `cargo test` will fail with connection errors. You can start one with:

```sh
docker run -d --rm -p 5672:5672 -p 15672:15672 -p 1883:1883 cloudamqp/lavinmq
```

## Project Structure

```
src/
  lib.rs        — Library root. Re-exports the public API.
  headers.rs    — Arbitrary headers-exchange scenarios (exchange/binding x-match, header-less messages, cross-typed values), LavinMQ matching model, invalid x-match values.
  names.rs      — Arbitrary impls for QueueName, RoutingKey, TopicRoutingKey, LongName (254–257/300/1000 bytes, for length validation).
  alternate.rs  — Arbitrary AE edge cases: AE cycles, AE naming a missing exchange, AE argument vs policy.
  arguments.rs  — Arbitrary impls for individual x-* queue arguments.
  combined.rs   — Arbitrary impls for combined argument sets per queue type.
  consistent_hash.rs — Arbitrary x-consistent-hash scenarios: ring/jump, x-hash-on, weighted bind/unbind ops (weight ≤ 100), keys.
  properties.rs — Arbitrary newtypes for BasicProperties fields (each with apply_to), plus BasicPropertiesArgs.
  stream_offset.rs — Arbitrary x-stream-offset (first/next/int in every AMQP int width/timestamp extremes) + expected-delivery model.
  consumer_priority.rs — Arbitrary x-priority consumer-argument values: valid (any int width within i32) and invalid (out of range / non-int).
  delayed.rs    — Arbitrary delayed-exchange scenarios (both declare styles), odd x-delay values, over-long exchange names.
  routing.rs    — Arbitrary Topology: fanout exchanges with optional alternate exchange, queues with optional DLX, bindings in shuffled order; guaranteed acyclic. simulate() models LavinMQ's order-dependent AE semantics.
  tests.rs      — #[cfg(test)] integration tests against a real broker.
  http/         — #[cfg(test)] differential tests: same operation over AMQP (vhost qc-<fn>-amqp) and the HTTP API (qc-<fn>-http); outcomes must agree (diff::equivalent maps 406→400) and the resources must GET equal after normalize().
    client.rs   — ureq wrapper (put/get/delete) + encode_segment (escapes `.` too).
    diff.rs     — Outcome, AMQP→HTTP status mapping, equivalent_delete() (AMQP delete of a missing thing is ok, HTTP 404), normalize().
    json.rs     — FieldTable → JSON (None for values JSON can't express, incl. f32).
    message.rs  — BasicProperties → /publish JSON, json_safe_headers(), base64().
    messages.rs — publish_parity (AMQP vs HTTP publish, read via /get) and get_parity (basic.get vs /get) + ignored quirks #11–#13.
    harness.rs  — declare_parity(): runs 1–3 declarations of one name on both sides and compares.
    queues.rs   — queue.declare vs PUT /queues parity (+ ignored amq. prefix quirk #9).
    validation.rs — HTTP-only input validation (no AMQP side: lapin can't send >255-byte short strings): name/routing-key/property/header-key lengths via LongName, delayed names past the internal-queue limit, wrong JSON types; ignored quirks #5, #15–#17.
    bindings.rs — bind/unbind parity (queue + exchange destinations, missing dest, headers args) + ignored properties_key quirk #10.
    exchanges.rs — exchange.declare vs PUT /exchanges parity (plain, headers x-match, consistent-hash, delayed, AE) + ignored internal-redeclare quirk #23.
  mqtt/         — MQTT 3.1.1. Generators + models are public; client.rs, raw.rs, tests.rs are #[cfg(test)].
    topic.rs    — TopicName, Subscription (filter derived from a topic, ~50% match), InvalidTopicName/Filter; matches() (spec) vs lavinmq_matches() (quirk #18).
    qos.rs      — Qos 0–2; delivered() (spec min, §3.8.4).
    retain.rs   — RetainScenario (retained publish/clear runs + a filter), with_prefix() for per-case isolation.
    session.rs  — SessionScenario (clean/persistent connect, offline publishes, reconnect).
    malformed.rs — BadUtf8Topic, BadProtocolLevel, BadSubscribeFlags.
    client.rs   — rumqttc wrapper: Conn drives the event loop in a task; subscribe/publish wait for acks, publishes_until(sentinel), disconnect() waits for the socket to close, abort() for unclean close.
    raw.rs      — hand-encoded packets over TCP for what rumqttc won't send.
    tests.rs    — broker tests (rumqttc + raw), plus ignored quirks #18, #21.
Cargo.toml      — Package manifest (edition 2024).
```

This is a library crate (`lib.rs` is the root — no binary target).

## Key Dependencies

| Crate              | Role                                         | Scope          |
|--------------------|----------------------------------------------|----------------|
| `quickcheck`       | Property-based testing framework + `Arbitrary` trait | Runtime dep |
| `quickcheck_macros`| `#[quickcheck]` proc macro for test functions | Dev only       |
| `lapin`            | Async AMQP client (`BasicProperties` types; tests) | Runtime dep |
| `rumqttc`          | Async MQTT client (`default-features = false`: no TLS) | Dev only |
| `tokio`            | Async runtime (multi-thread + macros)        | Dev only       |
| `futures-lite`     | `StreamExt` for consuming AMQP messages      | Dev only       |
| `ureq`             | Sync HTTP client for the management API (policies, `src/http/`) | Dev only |
| `serde_json`       | JSON for `src/http/` (`float_roundtrip`: the default parser can be 1 ULP off) | Dev only |

## Code Patterns

### Arbitrary Implementations

Types follow the newtype pattern: `pub struct QueueName(pub String)`. The `Arbitrary` impl lives on the newtype and uses `Gen` directly — not `Arbitrary` on inner types — to enforce domain constraints:

- Valid character set defined as a `const` byte slice (`VALID_CHARS`).
- Reserved prefixes (e.g., `amq.`) are rejected via a retry loop.
- Length clamped to AMQP limits (1–255).

When adding new types, follow the same pattern:
1. Define a newtype wrapper with `#[derive(Clone, Debug)]`.
2. Implement `Arbitrary` with domain-valid generation logic.
3. Add integration tests that round-trip through LavinMQ.

### Testing Approach

Tests are **integration-style property-based tests** in `src/tests.rs` (declared as `#[cfg(test)] mod tests;` in `lib.rs`). They:

- Use `#[quickcheck]` macro (not `#[test]`) to run property checks.
- Accept generated types (`QueueName`, `Vec<u8>`, etc.) as function parameters.
- Return `bool` — QuickCheck treats `false` as a failure and shrinks inputs.
- Spin up a `tokio::runtime::Runtime` manually inside each test (since `#[quickcheck]` doesn't support `async fn`).
- Perform a full AMQP round-trip: declare queue → publish → consume → verify → delete queue.
- Use `auto_delete: true` on queues for cleanup safety.
- Run in a **per-test-function vhost** `qc-<fn name>`: `connect_channel("<fn name>")` / `test_vhost(..)` delete and recreate it on first use in a run. Tests run in parallel against one broker, and vhosts keep their names, policies and policy side effects (e.g. `lavinmq-quirks.md` #7) from leaking between tests. The `qc-*` vhosts stay on the broker after a run.

### Async in Sync Tests

Because `#[quickcheck]` requires synchronous functions returning `bool`, each test creates its own `tokio::runtime::Runtime` via `Runtime::new().unwrap()` and calls `rt.block_on(async { ... })`. This is intentional — do not try to use `#[tokio::test]` with `#[quickcheck]`.

## Naming Conventions

- Types: `PascalCase` newtypes wrapping standard types (e.g., `QueueName(String)`).
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `VALID_CHARS`, `RESERVED_PREFIX`).
- Inner field is `pub` (tuple struct `.0` access pattern).

## Gotchas

1. **LavinMQ required for tests** — Tests are not unit tests; they hit a real LavinMQ broker. CI must provision LavinMQ.
2. **Edition 2024** — This crate uses Rust edition `2024`. Ensure your toolchain is recent enough (`rustup update`).
3. **Retry loop in `Arbitrary`** — `QueueName::arbitrary` uses a `loop` to reject reserved prefixes. This is safe because the probability of generating `amq.` prefix is vanishingly small, but it's technically unbounded.
4. **No `shrink` implementation** — The `Arbitrary` impl only defines `arbitrary`, not `shrink`. QuickCheck will use default shrinking on the inner `String`, which may produce invalid names during shrink. If shrink-generated names cause test failures unrelated to the property, consider implementing `shrink` with domain constraints.
5. **Ignored tests document LavinMQ bugs** — `#[ignore]`d tests assert the *desired* broker behaviour for known bugs (see `lavinmq-quirks.md`). Run them with `cargo test -- --ignored`; one passing means the bug is fixed and the `#[ignore]` can go.
6. **MQTT test isolation** — The vhost comes from the username (`qc-<fn>:guest`, see `client::options`). Client ids must be unique (`client::client_id`), or a new connection kicks off the old one. Retained messages outlive a case, so retain/will tests put a unique `case-<n>/` level in front of their topics. To know a subscriber got everything, tests publish to `SENTINEL` last and read up to it (one session queue, so order holds).
7. **Printed failures are shrunk** — quickcheck prints the input *after* shrinking. For an intermittent failure, that's just whatever happened to fail again while shrinking, not the input that failed first. To diagnose one, temporarily log the first failing input from inside the test, along with what the broker actually holds at that moment (e.g. queue depths from the management API). Also keep waiting past the timeout, to tell a late message from a lost one. That's how the `odd_x_delay_delivers_immediately` "flake" turned out to be `lavinmq-quirks.md` #22, not the delay values.
