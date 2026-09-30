# AGENTS.md

## Project Overview

`amqp-quickcheck` is a Rust library that provides [QuickCheck](https://crates.io/crates/quickcheck) `Arbitrary` implementations for AMQP types, enabling property-based testing of AMQP interactions against LavinMQ. Currently implements generators for queue names, routing keys, topic routing keys, the full set of LavinMQ queue-declaration arguments, `BasicProperties` fields, acyclic routing topologies (exchanges, queues, bindings, dead-lettering), and consistent-hash exchange scenarios.

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

Tests require a **LavinMQ server** running on `amqp://localhost:5672` with default credentials. Without it, `cargo test` will fail with connection errors. You can start one with:

```sh
docker run -d --rm -p 5672:5672 cloudamqp/lavinmq
```

## Project Structure

```
src/
  lib.rs        — Library root. Re-exports the public API.
  names.rs      — Arbitrary impls for QueueName, RoutingKey, TopicRoutingKey.
  arguments.rs  — Arbitrary impls for individual x-* queue arguments.
  combined.rs   — Arbitrary impls for combined argument sets per queue type.
  consistent_hash.rs — Arbitrary x-consistent-hash scenarios: ring/jump, x-hash-on, weighted bind/unbind ops (weight ≤ 100), keys.
  properties.rs — Arbitrary newtypes for BasicProperties fields (each with apply_to), plus BasicPropertiesArgs.
  routing.rs    — Arbitrary Topology: fanout exchanges, queues with optional DLX, bindings; guaranteed acyclic.
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
