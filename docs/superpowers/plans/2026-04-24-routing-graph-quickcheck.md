# Routing-Graph QuickCheck Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a property-based test that generates arbitrary DAG routing topologies (fanout exchanges + queues with optional dead-letter references), applies them to LavinMQ, publishes one probe message at the entry point, and asserts the observed per-queue ack counts match a pure-Rust simulator's prediction.

**Architecture:** A new `src/routing.rs` module holds the `Topology` data model, its `Arbitrary` impl (always produces a valid DAG), and a pure-Rust `simulate()` reference function. The existing `src/tests.rs` gains the integration test plus a set of async helpers (declare fanout, declare queue with args, apply binding, spawn consumer, publish probe, wait for quiescence, cleanup).

**Tech Stack:** Rust 2024, `quickcheck` + `quickcheck_macros`, `lapin` 2.x, `tokio` runtime, `std::sync::atomic::AtomicU32` for per-queue counters.

**Spec:** `docs/superpowers/specs/2026-04-24-routing-graph-quickcheck-design.md`

**Prerequisite:** LavinMQ on `localhost:5672` (same as the rest of the test suite).

---

## Task 1: Topology data model + module wiring

**Files:**
- Create: `src/routing.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create `src/routing.rs` with type definitions only**

```rust
// src/routing.rs
//! Routing-graph topology model: fanout exchanges, queues with optional
//! dead-letter references, and forward-only bindings between them.
//!
//! Every topology produced by the Arbitrary impl is a DAG: nodes are
//! ordered (exchanges first, then queues) and every edge points strictly
//! later in that order. This guarantees simulation terminates and keeps
//! real LavinMQ runs from looping indefinitely.

#[derive(Clone, Debug)]
pub struct Topology {
    /// All fanout exchanges. Index 0 is the publish point.
    pub exchanges: Vec<ExchangeNode>,
    pub queues: Vec<QueueNode>,
    pub bindings: Vec<Binding>,
}

#[derive(Clone, Debug)]
pub struct ExchangeNode {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct QueueNode {
    pub name: String,
    /// Optional dead-letter exchange (as an exchange index).
    pub dlx: Option<usize>,
    pub action: QueueAction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueAction {
    /// Consumer acks the delivery; the message terminates at this queue.
    Ack,
    /// Consumer nacks with requeue=false; LavinMQ routes to the DLX, or
    /// drops the message if no DLX is set.
    Reject,
}

#[derive(Clone, Copy, Debug)]
pub enum Binding {
    ExchangeToExchange { src: usize, dst: usize },
    ExchangeToQueue { src: usize, dst: usize },
}
```

- [ ] **Step 2: Register the module in `src/lib.rs`**

Add `pub mod routing;` directly after the existing `pub mod names;` / `pub mod arguments;` / `pub mod combined;` block. Keep alphabetical ordering so `cargo fmt` is happy. The lib.rs block becomes:

```rust
pub mod arguments;
pub mod combined;
pub mod names;
pub mod routing;

pub use names::{QueueName, RoutingKey, TopicRoutingKey};

#[cfg(test)]
mod tests;
```

- [ ] **Step 3: Run `cargo check` to verify the module compiles**

Run: `cargo check`
Expected: clean compile. Any new dead_code warnings on the new types are expected (they'll be used in Task 2).

- [ ] **Step 4: Commit**

```bash
git add src/routing.rs src/lib.rs
git commit -m "Add Topology data model for routing-graph tests"
```

---

## Task 2: Reference simulator with unit tests (TDD)

**Files:**
- Modify: `src/routing.rs`

- [ ] **Step 1: Write the failing unit tests at the bottom of `src/routing.rs`**

Append:

```rust
#[cfg(test)]
mod simulator_tests {
    use super::*;
    use std::collections::HashMap;

    fn ex(name: &str) -> ExchangeNode {
        ExchangeNode { name: name.into() }
    }

    fn qn(name: &str, dlx: Option<usize>, action: QueueAction) -> QueueNode {
        QueueNode { name: name.into(), dlx, action }
    }

    #[test]
    fn empty_graph_delivers_nothing() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![],
            bindings: vec![],
        };
        assert_eq!(simulate(&topo), HashMap::new());
    }

    #[test]
    fn single_exchange_to_ack_queue() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![Binding::ExchangeToQueue { src: 0, dst: 0 }],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn fanout_to_two_queues() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![
                qn("q0", None, QueueAction::Ack),
                qn("q1", None, QueueAction::Ack),
            ],
            bindings: vec![
                Binding::ExchangeToQueue { src: 0, dst: 0 },
                Binding::ExchangeToQueue { src: 0, dst: 1 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1), (1, 1)]));
    }

    #[test]
    fn exchange_to_exchange_chain() {
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![
                Binding::ExchangeToExchange { src: 0, dst: 1 },
                Binding::ExchangeToQueue { src: 1, dst: 0 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 1)]));
    }

    #[test]
    fn reject_with_dlx_routes_through_to_ack_queue() {
        // e0 → q0 (Reject, DLX=e1); e1 → q1 (Ack). Result: q1 gets 1.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![
                qn("q0", Some(1), QueueAction::Reject),
                qn("q1", None, QueueAction::Ack),
            ],
            bindings: vec![
                Binding::ExchangeToQueue { src: 0, dst: 0 },
                Binding::ExchangeToQueue { src: 1, dst: 1 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(1, 1)]));
    }

    #[test]
    fn reject_without_dlx_drops_message() {
        let topo = Topology {
            exchanges: vec![ex("e0")],
            queues: vec![qn("q0", None, QueueAction::Reject)],
            bindings: vec![Binding::ExchangeToQueue { src: 0, dst: 0 }],
        };
        assert_eq!(simulate(&topo), HashMap::new());
    }

    #[test]
    fn multi_path_convergence_duplicates_at_target() {
        // e0 → e1 → q0   (path A)
        // e0 → q0        (path B)
        // q0 gets 2 copies.
        let topo = Topology {
            exchanges: vec![ex("e0"), ex("e1")],
            queues: vec![qn("q0", None, QueueAction::Ack)],
            bindings: vec![
                Binding::ExchangeToExchange { src: 0, dst: 1 },
                Binding::ExchangeToQueue { src: 1, dst: 0 },
                Binding::ExchangeToQueue { src: 0, dst: 0 },
            ],
        };
        assert_eq!(simulate(&topo), HashMap::from([(0, 2)]));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib simulator_tests`
Expected: all seven tests fail to compile — `simulate` is not yet defined.

- [ ] **Step 3: Implement `simulate` and the private `Node` helper**

Insert, between the type definitions at the top of `src/routing.rs` and the `#[cfg(test)] mod simulator_tests { ... }` block:

```rust
use std::collections::HashMap;

/// Predicts the per-queue ack count for one message published at
/// `exchanges[0]`. Keyed by queue index. Queues with zero acks are absent.
pub fn simulate(topo: &Topology) -> HashMap<usize, u32> {
    let mut delivered: HashMap<usize, u32> = HashMap::new();
    let mut stack: Vec<Node> = vec![Node::Exchange(0)];
    while let Some(node) = stack.pop() {
        match node {
            Node::Exchange(ex) => {
                for b in &topo.bindings {
                    match *b {
                        Binding::ExchangeToExchange { src, dst } if src == ex => {
                            stack.push(Node::Exchange(dst));
                        }
                        Binding::ExchangeToQueue { src, dst } if src == ex => {
                            stack.push(Node::Queue(dst));
                        }
                        _ => {}
                    }
                }
            }
            Node::Queue(q) => match topo.queues[q].action {
                QueueAction::Ack => {
                    *delivered.entry(q).or_insert(0) += 1;
                }
                QueueAction::Reject => {
                    if let Some(dlx) = topo.queues[q].dlx {
                        stack.push(Node::Exchange(dlx));
                    }
                    // else: LavinMQ drops the message; nothing to record.
                }
            },
        }
    }
    delivered
}

enum Node {
    Exchange(usize),
    Queue(usize),
}
```

- [ ] **Step 4: Run the tests again to verify they pass**

Run: `cargo test --lib simulator_tests`
Expected: `test result: ok. 7 passed; 0 failed`.

- [ ] **Step 5: Commit**

```bash
git add src/routing.rs
git commit -m "Add routing-graph reference simulator with unit tests"
```

---

## Task 3: Arbitrary impl for Topology + DAG invariant test

**Files:**
- Modify: `src/routing.rs`

- [ ] **Step 1: Add the Arbitrary import and impl**

At the top of `src/routing.rs`, add:

```rust
use quickcheck::{Arbitrary, Gen};
```

Then insert this block below `impl` and above the existing `#[cfg(test)]` module:

```rust
impl Arbitrary for Topology {
    fn arbitrary(g: &mut Gen) -> Self {
        // 1. Pick sizes clamped into [1, 8], size-driven.
        let n_ex = *g
            .choose(&(1..=g.size().clamp(1, 8)).collect::<Vec<_>>())
            .unwrap();
        let n_q = *g
            .choose(&(1..=g.size().clamp(1, 8)).collect::<Vec<_>>())
            .unwrap();

        // 2. Per-iteration random suffix so names don't collide with
        //    earlier (possibly leaked) iterations on the broker.
        let suffix: u64 = u64::arbitrary(g);

        let exchanges: Vec<ExchangeNode> = (0..n_ex)
            .map(|i| ExchangeNode {
                name: format!("qc_ex_{:x}_{}", suffix, i),
            })
            .collect();

        let queues: Vec<QueueNode> = (0..n_q)
            .map(|i| {
                let dlx = if bool::arbitrary(g) {
                    Some(*g.choose(&(0..n_ex).collect::<Vec<_>>()).unwrap())
                } else {
                    None
                };
                let action = if bool::arbitrary(g) {
                    QueueAction::Ack
                } else {
                    QueueAction::Reject
                };
                QueueNode {
                    name: format!("qc_q_{:x}_{}", suffix, i),
                    dlx,
                    action,
                }
            })
            .collect();

        // 3. Forward-only bindings. 50/50 per candidate edge.
        let mut bindings = Vec::new();
        for src in 0..n_ex {
            for dst in (src + 1)..n_ex {
                if bool::arbitrary(g) {
                    bindings.push(Binding::ExchangeToExchange { src, dst });
                }
            }
            for dst in 0..n_q {
                if bool::arbitrary(g) {
                    bindings.push(Binding::ExchangeToQueue { src, dst });
                }
            }
        }

        Topology { exchanges, queues, bindings }
    }
}
```

- [ ] **Step 2: Add property tests for the DAG invariant and simulator termination**

Append to `src/routing.rs` (keep the existing `simulator_tests` module as-is):

```rust
#[cfg(test)]
mod generator_tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn generated_topology_is_a_dag(topo: Topology) -> bool {
        // Every binding points forward in topo order, and every index is
        // in-bounds for the declared node counts.
        for b in &topo.bindings {
            match *b {
                Binding::ExchangeToExchange { src, dst } => {
                    if src >= dst {
                        return false;
                    }
                    if dst >= topo.exchanges.len() {
                        return false;
                    }
                }
                Binding::ExchangeToQueue { src, dst } => {
                    if src >= topo.exchanges.len() || dst >= topo.queues.len() {
                        return false;
                    }
                }
            }
        }
        // Every DLX reference is an in-bounds exchange.
        for q in &topo.queues {
            if let Some(dlx) = q.dlx {
                if dlx >= topo.exchanges.len() {
                    return false;
                }
            }
        }
        true
    }

    #[quickcheck]
    fn simulate_terminates_on_any_generated_topology(topo: Topology) -> bool {
        // simulate() is guaranteed to return on any DAG. If it hangs,
        // either the generator has a bug or simulate() is wrong.
        let _ = simulate(&topo);
        true
    }
}
```

Note: `quickcheck_macros` is already a `[dev-dependencies]` entry — no Cargo.toml change needed. Because these are pure-logic tests they run in the library test target alongside the existing broker-hitting tests, but they don't touch the network.

- [ ] **Step 3: Run the new tests**

Run: `cargo test --lib generator_tests`
Expected: `test result: ok. 2 passed; 0 failed`. Each runs ~100 quickcheck iterations in a few ms.

- [ ] **Step 4: Run the full library unit-test filter to confirm nothing regressed**

Run: `cargo test --lib simulator_tests generator_tests`
Expected: `test result: ok. 9 passed; 0 failed`.

- [ ] **Step 5: Commit**

```bash
git add src/routing.rs
git commit -m "Add Arbitrary for Topology + DAG invariant property tests"
```

---

## Task 4: Integration test + async helpers

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/tests.rs`

- [ ] **Step 0: Enable tokio's `time` feature**

The `wait_for_total` helper calls `tokio::time::sleep(...)`. That lives
behind the `time` feature flag, which isn't currently enabled.

Edit `/home/mange/workspace/amqp-quickcheck/Cargo.toml`, changing the
existing tokio line:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

to:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
```

Then run `cargo check` and expect a clean build (the feature flip just
exposes `tokio::time::*`; nothing else changes).

- [ ] **Step 1: Add the new imports to the top of `src/tests.rs`**

Insert these imports into the existing `use` section (alphabetically, keep `cargo fmt` happy):

```rust
use crate::routing::{Binding, QueueAction, QueueNode, Topology, simulate};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
```

Also extend the existing `use lapin::{...}` block to include the extra items the helpers need. The final block:

```rust
use lapin::{
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        ExchangeBindOptions, ExchangeDeclareOptions, ExchangeDeleteOptions,
        QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable, ShortString},
};
```

- [ ] **Step 2: Append the async helpers at the bottom of `src/tests.rs`**

Append (below all existing tests/macros, in that order):

```rust
// ---------------------------------------------------------------------------
// Routing-graph test harness helpers (used by `routing_graph_delivers_expected`)
// ---------------------------------------------------------------------------

async fn declare_fanout(channel: &lapin::Channel, name: &str) {
    channel
        .exchange_declare(
            name,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare fanout exchange");
}

async fn declare_queue_for_routing(channel: &lapin::Channel, name: &str, args: FieldTable) {
    channel
        .queue_declare(name, QueueDeclareOptions::default(), args)
        .await
        .expect("Failed to declare queue");
}

async fn apply_binding(channel: &lapin::Channel, topo: &Topology, b: &Binding) {
    match *b {
        Binding::ExchangeToExchange { src, dst } => {
            channel
                .exchange_bind(
                    &topo.exchanges[dst].name, // destination
                    &topo.exchanges[src].name, // source
                    "",
                    ExchangeBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .expect("Failed to bind exchange to exchange");
        }
        Binding::ExchangeToQueue { src, dst } => {
            channel
                .queue_bind(
                    &topo.queues[dst].name,
                    &topo.exchanges[src].name,
                    "",
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .expect("Failed to bind queue to exchange");
        }
    }
}

/// Starts a consumer on `queue` that acks (if `Ack`) or nacks-requeue-false
/// (if `Reject`) every delivery it receives. The counter is incremented
/// ONLY for Ack events — Reject events are message transitions, already
/// modelled by the simulator's graph traversal. Spawns the consumer loop
/// onto the current tokio runtime; the task terminates when the consumer
/// stream ends (e.g. on channel close or queue deletion).
async fn spawn_consumer(channel: &lapin::Channel, queue: &QueueNode, counter: Arc<AtomicU32>) {
    let mut consumer = channel
        .basic_consume(
            &queue.name,
            &format!("consumer_{}", queue.name),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to start consumer");
    let action = queue.action;
    tokio::spawn(async move {
        while let Some(delivery_result) = consumer.next().await {
            let delivery = match delivery_result {
                Ok(d) => d,
                Err(_) => break,
            };
            match action {
                QueueAction::Ack => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
                QueueAction::Reject => {
                    let _ = delivery
                        .nack(BasicNackOptions {
                            requeue: false,
                            ..BasicNackOptions::default()
                        })
                        .await;
                }
            }
        }
    });
}

async fn publish_probe(channel: &lapin::Channel, exchange: &str) {
    channel
        .basic_publish(
            exchange,
            "",
            BasicPublishOptions::default(),
            b"probe",
            BasicProperties::default(),
        )
        .await
        .expect("Failed to publish probe")
        .await
        .expect("Failed to confirm publish");
}

/// Polls the sum of all counters every 5 ms; returns once the total
/// reaches `expected_total` or the timeout elapses. Returns immediately
/// when `expected_total == 0` (nothing to wait for).
async fn wait_for_total(counters: &[Arc<AtomicU32>], expected_total: u32, timeout: Duration) {
    if expected_total == 0 {
        return;
    }
    let start = std::time::Instant::now();
    loop {
        let total: u32 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        if total >= expected_total {
            return;
        }
        if start.elapsed() >= timeout {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn cleanup_topology(channel: &lapin::Channel, topo: &Topology) {
    for q in &topo.queues {
        let _ = channel
            .queue_delete(&q.name, QueueDeleteOptions::default())
            .await;
    }
    for ex in &topo.exchanges {
        let _ = channel
            .exchange_delete(&ex.name, ExchangeDeleteOptions::default())
            .await;
    }
}
```

- [ ] **Step 3: Append the property test at the bottom of `src/tests.rs`**

```rust
#[quickcheck]
fn routing_graph_delivers_expected(topo: Topology) -> bool {
    let expected: HashMap<usize, u32> = simulate(&topo);
    let expected_total: u32 = expected.values().sum();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;

        // 1. Declare exchanges.
        for ex in &topo.exchanges {
            declare_fanout(&channel, &ex.name).await;
        }

        // 2. Declare queues (with x-dead-letter-exchange if DLX is set).
        for q in &topo.queues {
            let mut args = FieldTable::default();
            if let Some(dlx) = q.dlx {
                args.insert(
                    ShortString::from("x-dead-letter-exchange"),
                    AMQPValue::LongString(topo.exchanges[dlx].name.clone().into()),
                );
            }
            declare_queue_for_routing(&channel, &q.name, args).await;
        }

        // 3. Apply bindings.
        for b in &topo.bindings {
            apply_binding(&channel, &topo, b).await;
        }

        // 4. Spawn per-queue consumers.
        let counters: Vec<Arc<AtomicU32>> = (0..topo.queues.len())
            .map(|_| Arc::new(AtomicU32::new(0)))
            .collect();
        for (i, q) in topo.queues.iter().enumerate() {
            spawn_consumer(&channel, q, counters[i].clone()).await;
        }

        // 5. Publish one probe at exchange 0.
        publish_probe(&channel, &topo.exchanges[0].name).await;

        // 6. Wait for quiescence (500 ms deadline).
        wait_for_total(&counters, expected_total, Duration::from_millis(500)).await;

        // 7. Collect observed counts.
        let actual: HashMap<usize, u32> = counters
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                let n = c.load(Ordering::Relaxed);
                (n > 0).then_some((i, n))
            })
            .collect();

        // 8. Cleanup (best-effort; runs even on mismatch so later iterations
        //    don't trip over leftover state).
        cleanup_topology(&channel, &topo).await;

        actual == expected
    })
}
```

- [ ] **Step 4: Run just the new property test**

Run: `cargo test --lib routing_graph_delivers_expected`
Expected: PASS. Runs ~100 quickcheck iterations. Some iterations exercise 1-node graphs (fast, trivially pass); larger iterations take up to ~100 ms each. Full test should finish in ~30 s on a typical machine.

- [ ] **Step 5: Run the full test suite to confirm no regression**

Run: `cargo test`
Expected: all 30 tests pass (3 existing round-trip + 16 per-argument + 3 combined + 7 rejection + 1 new routing = 30, plus the 9 pure-logic unit tests from Tasks 2+3 which also run in the default target).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/tests.rs
git commit -m "Add routing-graph property test + async test harness"
```

---

## Task 5: Final polish

**Files:**
- Modify: any source files fmt/clippy flags

- [ ] **Step 1: Run `cargo fmt`**

Run: `cargo fmt`
Expected: either no diff, or minor cosmetic changes. If cargo fmt modifies files, stage them for the polish commit.

- [ ] **Step 2: Run `cargo clippy --all-targets -- -D warnings`**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean. If anything surfaces, read the lint; the most likely suspects are:
- Needless `.clone()` around `topo.exchanges[dlx].name` — can `.into()` without cloning where ownership works.
- `collapsible_if` or similar style lints.
- Unused imports if a helper ended up unused — verify first whether the helper is actually used before removing.
Fix each lint before re-running. Do NOT add `#[allow(...)]` attributes to silence them.

- [ ] **Step 3: Run the full test suite once more**

Run: `cargo test`
Expected: all 30 property tests + 9 pure-logic unit tests pass.

- [ ] **Step 4: Commit any polish changes**

```bash
git add -A
git commit -m "Format and lint cleanup"
```

If there are no changes, skip the commit.

---

## Self-Review Notes

**Spec coverage:**

- Topology model (spec §"Topology model"): Task 1 ✅
- Arbitrary with DAG invariant (spec §"Arbitrary generation"): Task 3 ✅
- Reference simulator (spec §"Reference simulator"): Task 2 ✅
- Applier + verification harness (spec §"Applier / verification harness"): Task 4 ✅
- Per-queue AtomicU32 counter incremented only on Ack (spec §"Applier" + helper table): Task 4 Step 2 `spawn_consumer` — counter only increments in the Ack arm, intentionally ✅
- `wait_for_total` with active polling + early exit when expected is 0 (spec §"Applier"): Task 4 Step 2 ✅
- Cleanup runs before return (spec §"Applier"): Task 4 Step 3, Step 8 ✅
- File layout ~200 lines routing.rs + ~150 lines tests.rs (spec §"Architecture"): fits in the plan ✅
- Follow-ups are intentionally absent (spec §"Follow-ups"): not implemented in this plan ✅

**Type consistency:**

- `Topology` / `ExchangeNode` / `QueueNode` / `Binding` / `QueueAction` are defined in Task 1 and used consistently throughout.
- `simulate(&Topology) -> HashMap<usize, u32>` — defined in Task 2 Step 3, called in Task 4 Step 3. Matches.
- `spawn_consumer(&lapin::Channel, &QueueNode, Arc<AtomicU32>)` — defined in Task 4 Step 2, called in Task 4 Step 3. Matches.
- `wait_for_total(&[Arc<AtomicU32>], u32, Duration)` — defined and called consistently.
- Consumer only increments the counter on Ack (not Reject). Simulator likewise only records Ack events. The property assertion `actual == expected` is therefore a valid multiset comparison.

**Placeholders:** None.
