# Routing-Graph QuickCheck — Design

Date: 2026-04-24

## Background

The crate currently has per-argument and per-queue-family property tests
for queue declarations. It doesn't yet exercise LavinMQ's *routing*
behaviour end-to-end — exchange-to-exchange bindings, exchange-to-queue
bindings, and dead-letter chains combined into arbitrary topologies.

This spec extends the suite with a property-based test that generates a
random routing graph, applies it to LavinMQ, publishes a message at the
entry point, and verifies that the messages observed at each queue match
what a pure-Rust simulator predicts.

## Goals

1. Generate arbitrary but valid LavinMQ routing graphs — fanout exchanges,
   queues with optional dead-letter-exchange references, and bindings
   between them.
2. Apply a generated topology to LavinMQ (declare exchanges, queues,
   bindings; start consumers).
3. Publish a single probe message at the entry point and observe the
   resulting per-queue ack counts.
4. Compare observed counts to a reference simulator's prediction. Any
   mismatch fails the property, with QuickCheck's shrinking producing a
   minimal reproducing topology.

## Non-goals

- Non-fanout exchange types (direct, topic, headers). Deferred — fanout
  alone already covers the user-requested shape ("arbitrary combinations
  of routing") without pulling in routing-key / pattern semantics.
- Cycles in the routing graph. The entire graph (bindings + dead-letter
  transitions) is required to be a DAG, because LavinMQ has no usable
  hop-bound for consumer-rejection-triggered loops (`x-delivery-limit`
  counts redeliveries, not dead-letter events). Deferred.
- Non-rejection dead-lettering triggers (TTL, delivery-limit, overflow).
  Rejection-driven DL gives deterministic, fast tests.
- Multiple concurrent messages / multiple publish points per iteration.
  One probe per iteration keeps the model and simulator simple.
- Verifying broker-internal details like `x-death` header contents.

## Target broker

LavinMQ on `amqp://localhost:5672`, same as the rest of the test suite.

## Architecture

```
src/
  lib.rs       — adds `pub mod routing;`
  routing.rs   — topology model + Arbitrary impl + simulator (pure Rust, ~200 lines)
  tests.rs     — one new `#[quickcheck]` test + helper fns (~150 added lines)
```

`routing.rs` is pure data + pure function, with no broker contact.
`tests.rs` hosts the test harness that applies a topology to LavinMQ.

## Topology model

```rust
pub struct Topology {
    pub exchanges: Vec<ExchangeNode>, // all fanout; index 0 is the publish point
    pub queues: Vec<QueueNode>,
    pub bindings: Vec<Binding>,
}

pub struct ExchangeNode {
    pub name: String,
}

pub struct QueueNode {
    pub name: String,
    pub dlx: Option<usize>,   // exchange index; DLX for this queue
    pub action: QueueAction,
}

pub enum QueueAction {
    Ack,     // consumer acks; message terminates here
    Reject,  // consumer nacks with requeue=false, triggering dead-letter
}

pub enum Binding {
    ExchangeToExchange { src: usize, dst: usize }, // dst > src in topo order
    ExchangeToQueue    { src: usize, dst: usize },
}
```

**DAG invariant (cutpoint scheme):** exchanges are laid out at positions
`0..n_ex`. Each queue is assigned a "cutpoint" `k ∈ [0, n_ex]` (not
stored on `QueueNode` — it lives only in the generator). The cutpoint
carves the exchange axis into two halves for that queue:

- E→Q bindings can come only from exchanges with index `< k` (the
  "upstream" of the queue).
- The queue's DLX (if set) must be an exchange with index `≥ k` (the
  "downstream").

When a rejected message leaves the queue via DLX it therefore jumps
strictly forward in exchange-index space — from `< k` to `≥ k`. It can
never reach an upstream exchange and never loops. The E→E binding graph
is forward-only (`src < dst`), so all edges in the unfolded routing
graph point forward. Termination is guaranteed.

A naive "DLX = any exchange" scheme fails here: `Q₀ bound from E₀,
Q₀.dlx = E₀` closes an immediate cycle. The cutpoint scheme eliminates
that by construction.

## Arbitrary generation

```rust
impl Arbitrary for Topology {
    fn arbitrary(g: &mut Gen) -> Self {
        let n_ex = *g.choose(&(1..=g.size().clamp(1, 8)).collect::<Vec<_>>()).unwrap();
        let n_q  = *g.choose(&(1..=g.size().clamp(1, 8)).collect::<Vec<_>>()).unwrap();

        let suffix: u64 = u64::arbitrary(g);

        let exchanges: Vec<ExchangeNode> = (0..n_ex)
            .map(|i| ExchangeNode { name: format!("qc_ex_{:x}_{}", suffix, i) })
            .collect();

        let mut queues: Vec<QueueNode> = Vec::with_capacity(n_q);
        let mut bindings: Vec<Binding> = Vec::new();

        for qi in 0..n_q {
            // Each queue gets a cutpoint k ∈ [0, n_ex]. Upstream bindings
            // must have src < k, DLX (if set) must have idx ≥ k.
            let cutpoint = *g.choose(&(0..=n_ex).collect::<Vec<_>>()).unwrap();

            let dlx = if cutpoint < n_ex && bool::arbitrary(g) {
                Some(*g.choose(&(cutpoint..n_ex).collect::<Vec<_>>()).unwrap())
            } else {
                None
            };

            let action = if bool::arbitrary(g) {
                QueueAction::Ack
            } else {
                QueueAction::Reject
            };

            queues.push(QueueNode {
                name: format!("qc_q_{:x}_{}", suffix, qi),
                dlx,
                action,
            });

            for src in 0..cutpoint {
                if bool::arbitrary(g) {
                    bindings.push(Binding::ExchangeToQueue { src, dst: qi });
                }
            }
        }

        // E→E bindings: forward-only in exchange index.
        for src in 0..n_ex {
            for dst in (src + 1)..n_ex {
                if bool::arbitrary(g) {
                    bindings.push(Binding::ExchangeToExchange { src, dst });
                }
            }
        }

        Topology { exchanges, queues, bindings }
    }
}
```

- Size bounds: `1..=8` for each node type, driven by `g.size()`. A typical
  run generates ~3 exchanges + ~3 queues, with larger topologies appearing
  occasionally for coverage.
- Per-test-run suffix: a `u64::arbitrary` value rendered hex so collisions
  between iterations are astronomically unlikely.
- Shrinking: no custom `shrink`. Default `Arbitrary` shrinking on `Vec`s
  produces smaller graphs (fewer bindings, fewer queues/exchanges); default
  shrinking on `Option` produces `None` → drops DLX. This usually reduces
  a failing topology to a minimal reproducer without custom logic.
- The publish point is always exchange 0. It may or may not have outbound
  edges; if not, the message goes nowhere and the simulator expects zero
  deliveries. That's a valid (though uninteresting) test case.

## Reference simulator

Pure function, no I/O. Two broker semantics are load-bearing for the
model (both confirmed empirically against LavinMQ by the integration
test):

1. **Per-routing-pass deduplication.** Within one delivery of a single
   published message, LavinMQ (like RabbitMQ) visits each exchange at
   most once and each queue at most once. Diamond paths
   (`E₀ → E₁ → E₂` and `E₀ → E₂`) deliver to `E₂` once, not twice.
   Similarly, two `E → Q` bindings from different exchanges to the same
   queue still deliver one copy.
2. **Dead-lettering is a fresh routing pass.** When a queue rejects, the
   DLX receives a newly-published message with its own empty visited
   sets — so it can re-reach exchanges and queues that the original pass
   already visited.

```rust
use std::collections::{HashMap, HashSet};

pub fn simulate(topo: &Topology) -> HashMap<usize, u32> {
    let mut delivered: HashMap<usize, u32> = HashMap::new();
    // Each entry is a pending (or in-progress) routing event: the
    // starting exchange plus the exchanges/queues already visited on
    // that event's pass.
    let mut work: Vec<(usize, HashSet<usize>, HashSet<usize>)> =
        vec![(0, HashSet::new(), HashSet::new())];
    while let Some((start_ex, mut vis_ex, mut vis_q)) = work.pop() {
        let mut stack: Vec<Node> = vec![Node::Exchange(start_ex)];
        while let Some(node) = stack.pop() {
            match node {
                Node::Exchange(ex) => {
                    if !vis_ex.insert(ex) { continue; }
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
                Node::Queue(q) => {
                    if !vis_q.insert(q) { continue; }
                    match topo.queues[q].action {
                        QueueAction::Ack => *delivered.entry(q).or_insert(0) += 1,
                        QueueAction::Reject => {
                            if let Some(dlx) = topo.queues[q].dlx {
                                // DL starts a fresh pass with empty visited sets.
                                work.push((dlx, HashSet::new(), HashSet::new()));
                            }
                        }
                    }
                }
            }
        }
    }
    delivered
}

enum Node { Exchange(usize), Queue(usize) }
```

Fanout semantics: a single arrival at an exchange produces one
downstream traversal per outbound binding, constrained by the visited
sets above. The DAG invariant (cutpoint scheme) plus per-pass dedup
guarantees termination — no hop cap required.

## Applier / verification harness

In `src/tests.rs`:

```rust
#[quickcheck]
fn routing_graph_delivers_expected(topo: Topology) -> bool {
    let expected = simulate(&topo);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;

        // 1. Declare exchanges (fanout, non-durable, non-auto-delete).
        for ex in &topo.exchanges {
            declare_fanout(&channel, &ex.name).await;
        }

        // 2. Declare queues. If a queue has a DLX, set x-dead-letter-exchange.
        for q in &topo.queues {
            let mut args = FieldTable::default();
            if let Some(dlx) = q.dlx {
                args.insert(
                    ShortString::from("x-dead-letter-exchange"),
                    AMQPValue::LongString(topo.exchanges[dlx].name.clone().into()),
                );
            }
            declare_queue(&channel, &q.name, args).await;
        }

        // 3. Apply bindings.
        for b in &topo.bindings {
            apply_binding(&channel, &topo, b).await;
        }

        // 4. Start per-queue consumers; each increments a counter and
        //    acks or nacks-requeue-false per its action.
        let counters: Vec<Arc<AtomicU32>> =
            (0..topo.queues.len()).map(|_| Arc::new(AtomicU32::new(0))).collect();
        for (i, q) in topo.queues.iter().enumerate() {
            spawn_consumer(&channel, q, counters[i].clone()).await;
        }

        // 5. Publish one probe at exchange 0.
        publish(&channel, &topo.exchanges[0].name, b"probe").await;

        // 6. Wait for quiescence: poll until total Ack counts equal the
        //    simulator's expected total, bounded by 500ms.
        let expected_total: u32 = expected.values().sum();
        wait_for_total(&counters, expected_total, Duration::from_millis(500)).await;

        // 7. Compare per-queue counts (queues with zero acks omitted from
        //    both HashMaps so the equality is a multiset comparison).
        let actual: HashMap<usize, u32> = counters.iter().enumerate()
            .filter_map(|(i, c)| {
                let n = c.load(Ordering::Relaxed);
                (n > 0).then_some((i, n))
            })
            .collect();

        cleanup(&channel, &topo).await;
        actual == expected
    })
}
```

Helper responsibilities:

| Helper           | Responsibility |
| ---------------- | -------------- |
| `declare_fanout` | `exchange_declare` with type `"fanout"`, non-durable, not auto-delete. |
| `declare_queue`  | `queue_declare` with the provided argument table, non-auto-delete. |
| `apply_binding`  | `queue_bind` or `exchange_bind` with empty routing key. |
| `spawn_consumer` | `basic_consume` with a unique tag; spawns a tokio task that processes each delivery: increment counter, then `basic_ack` (Ack) or `basic_nack(requeue=false)` (Reject). |
| `publish`        | `basic_publish` with empty routing key to the entry exchange; awaits publish confirm. |
| `wait_for_total` | Polls the sum of counters every 5 ms; returns as soon as it equals `expected_total` or when the 500 ms deadline elapses. If `expected_total == 0`, returns immediately. |
| `cleanup`        | Best-effort: cancel consumer tasks, `queue_delete` each queue, `exchange_delete` each exchange. |

### Why rejection, not TTL, drives dead-lettering

Rejection (`basic_nack` with `requeue=false`) triggers a DL event within
microseconds — a hop in the topology is as fast as the broker's routing
table lookup. TTL-based DL would add the TTL to every test iteration's
wall time. With rejection, a typical iteration finishes in <50 ms.

### Why `wait_for_total` uses active polling

- Graphs are tiny; settling finishes in a few ms when there's no bug.
- When there IS a bug (message lost, duplicated, mis-routed), the 500 ms
  deadline surfaces it quickly with a useful failure message.
- When no delivery is expected, the test skips the wait entirely.

## Failure modes and what they tell us

| Observed | Interpretation |
| --- | --- |
| Actual count < expected | LavinMQ lost or mis-routed a copy; or our declarations / consumer logic is wrong. |
| Actual count > expected | LavinMQ duplicated a copy (broker bug) or our simulator is wrong — most commonly, a missing dedup case somewhere. |
| Test times out at 500 ms | Messages are stuck in a cycle we failed to prevent, or LavinMQ is hanging. Graph should be reproduced manually. |
| Test leaks queues/exchanges on failure | `cleanup` didn't run. Acceptable — per-test-run name suffixes prevent cross-iteration pollution. |

## Test count

One new property test: `routing_graph_delivers_expected`. Brings the suite
total from 29 to 30.

## Follow-ups (explicit non-goals here)

- Direct / topic / headers exchange types. Would require routing-key /
  pattern modelling in the simulator.
- Cyclic routing graphs. Would require either LavinMQ loop-detection
  research or a per-message hop counter mechanism.
- TTL- and delivery-limit-triggered dead-lettering within the same graph.
- Multiple concurrent probe messages / multiple publish points.
