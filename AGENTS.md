# AGENTS.md

## Project Overview

`amqp-quickcheck` is a Rust library that provides [QuickCheck](https://crates.io/crates/quickcheck) `Arbitrary` implementations for AMQP types, enabling property-based testing of AMQP interactions against LavinMQ. Currently implements generators for queue names, routing keys, topic routing keys, the full set of LavinMQ queue-declaration arguments, `BasicProperties` fields, acyclic routing topologies (exchanges, queues, bindings, dead-lettering), consistent-hash exchange scenarios, stream consumer `x-stream-offset` values, and delayed-message exchanges.

## Commands

```sh
# Build
cargo build

# Run all tests (requires a running LavinMQ instance on localhost:5672)
cargo test

# Check without building
cargo check

# Format
cargo fmt

# Lint
cargo clippy
```

## External Dependencies (Runtime)

Tests require a **LavinMQ server** running on `amqp://localhost:5672` with default credentials (`guest:guest`), and its management HTTP API on `http://localhost:15672` (used by the policy tests). Without it, `cargo test` will fail with connection errors. You can start one with:

```sh
docker run -d --rm -p 5672:5672 -p 15672:15672 cloudamqp/lavinmq
```

## Project Structure

```
src/
  lib.rs        — Library root. Re-exports the public API.
  names.rs      — Arbitrary impls for QueueName, RoutingKey, TopicRoutingKey.
  alternate.rs  — Arbitrary AE edge cases: AE cycles, AE naming a missing exchange, AE argument vs policy.
  arguments.rs  — Arbitrary impls for individual x-* queue arguments.
  combined.rs   — Arbitrary impls for combined argument sets per queue type.
  consistent_hash.rs — Arbitrary x-consistent-hash scenarios: ring/jump, x-hash-on, weighted bind/unbind ops (weight ≤ 100), keys.
  properties.rs — Arbitrary newtypes for BasicProperties fields (each with apply_to), plus BasicPropertiesArgs.
  stream_offset.rs — Arbitrary x-stream-offset (first/next/int in every AMQP int width/timestamp extremes) + expected-delivery model.
  delayed.rs    — Arbitrary delayed-exchange scenarios (both declare styles), odd x-delay values, over-long exchange names.
  routing.rs    — Arbitrary Topology: fanout exchanges with optional alternate exchange, queues with optional DLX, bindings in shuffled order; guaranteed acyclic. simulate() models LavinMQ's order-dependent AE semantics.
  tests.rs      — #[cfg(test)] integration tests against a real broker.
Cargo.toml      — Package manifest (edition 2024).
```

This is a library crate (`lib.rs` is the root — no binary target).

## Key Dependencies

| Crate              | Role                                         | Scope          |
|--------------------|----------------------------------------------|----------------|
| `quickcheck`       | Property-based testing framework + `Arbitrary` trait | Runtime dep |
| `quickcheck_macros`| `#[quickcheck]` proc macro for test functions | Dev only       |
| `lapin`            | Async AMQP client (`BasicProperties` types; tests) | Runtime dep |
| `tokio`            | Async runtime (multi-thread + macros)        | Dev only       |
| `futures-lite`     | `StreamExt` for consuming AMQP messages      | Dev only       |
| `ureq`             | Sync HTTP client for the management API (policies) | Dev only |

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
6. **Known flake, not yet investigated: `odd_x_delay_delivers_immediately`** — It sometimes fails in the full parallel suite, even with policy tests skipped, but has never failed when run alone. Policy churn doesn't affect the delayed exchange (0/30 lost in a stress check). One suspect is the 2 s arrival timeout under load. Parked on 2026-10-01 until the suite runs green again.
