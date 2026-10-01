# LavinMQ Quirks

Broker behaviours discovered while running this crate's property-based tests
against LavinMQ that aren't (or aren't fully) documented in
[`queue-arguments.md`](./queue-arguments.md). Each entry explains what we
observed, how it deviates from the documented contract, and how this crate
works around it.

## 1. `x-dead-letter-routing-key` requires `x-dead-letter-exchange`

**Observed:** Declaring a queue with `x-dead-letter-routing-key` set but no
`x-dead-letter-exchange` fails with `PRECONDITION_FAILED` (AMQP reply code
406) and the channel closes.

**Where the docs disagree:** `queue-arguments.md` describes the two
arguments independently and doesn't state that the routing key requires an
exchange. RabbitMQ (the nearest reference implementation) accepts this
combination — it silently ignores the routing key when no DLX is set.
LavinMQ treats it as an error instead.

**Why:** An orphan routing key has no destination, so LavinMQ's
`queue.declare` validation rejects it up front rather than letting the
argument become a no-op.

**How this crate works around it:**

- `tests::declare_with_dead_letter_routing_key` is hand-rolled (not using
  the `single_arg_classic_test!` macro) so it can set a fixed DLX
  (`amq.direct`) alongside the generated routing key.
- `ClassicQueueArgs::apply` in `src/combined.rs` gates the routing-key
  insertion on the exchange also being set:
  ```rust
  if let (Some(a), Some(_)) = (&self.dead_letter_routing_key, &self.dead_letter_exchange) {
      a.insert_into(table);
  }
  ```
  When the random subset drops DLX, DLRK is dropped with it — keeping
  combined-argument declarations accepted.

**Consequence for the rejection tests:** The Task 13 stream-rejection test
`stream_rejects_dead_letter_routing_key` can trip on either rejection
reason (stream forbids DL args, or DLRK-without-DLX). Both surface as 406
PRECONDITION_FAILED, so the declare-only test passes either way.

## 2. `x-cache-size` + `x-message-deduplication: true` can OOM the broker

**Observed:** Declaring a queue with both `x-message-deduplication: true`
and a large `x-cache-size` (millions, depending on host RAM) crashes the
LavinMQ process — empty HTTP response from the management interface, the
AMQP connection closes abruptly, and any broker state in memory (e.g.
transient queues from other tests in the same run) is lost. Reproduced on
LavinMQ 2.7.0.

**Where the docs disagree:** `queue-arguments.md` describes `x-cache-size`
as "number of recent dedup keys to remember" with no documented upper
bound. It doesn't mention that the cache is pre-allocated when
deduplication is enabled.

**Why:** When `x-message-deduplication: true`, LavinMQ eagerly allocates
the dedup cache at declare time — so `x-cache-size: 2_000_000_000` is an
immediate request for ~billions of cache slots. The allocator gives up and
the broker terminates.

**Per-argument scope:** `x-cache-size` *alone* (without dedup enabled)
is stored without pre-allocation. Our `declare_with_cache_size` per-arg
test runs with the full `u32` range and doesn't crash the broker — only
the combined test (`declare_classic_with_combined_args`) exposes the
interaction.

**How this crate works around it:**

`CacheSize::arbitrary` in `src/arguments.rs` caps generated values at
`100_000`:

```rust
impl Arbitrary for CacheSize {
    fn arbitrary(g: &mut Gen) -> Self {
        // Cap at 100_000 to avoid triggering OOM crashes in LavinMQ when the
        // dedup cache is actually allocated (i.e. when x-message-deduplication
        // is also set). The broker allocates the cache up front; very large
        // values (> ~500 M on a typical test host) cause it to crash.
        CacheSize(u32::arbitrary(g) % 100_000)
    }
}
```

100_000 is well inside any reasonable production cache size and comfortably
below the allocation threshold. If LavinMQ starts allocating the cache
lazily or capping it internally, this limit can be lifted.

**Operational note:** If you see tests from an earlier run of this crate
leaving LavinMQ unresponsive, this quirk is a likely cause. Restarting the
broker restores normal operation.

## 3. Routing dedups visited exchanges and queues per delivery pass

**Observed:** When a single message is routed through fanout exchanges
with multiple paths, LavinMQ visits each exchange and each queue **at
most once** within the pass. Concretely:

- Diamond path `E₀ → E₁ → E₂` combined with `E₀ → E₂` produces one
  delivery through `E₂`, not two.
- Two independent `E → Q` bindings (e.g. `E₀ → Q` and `E₁ → Q`) from
  fanout sources that are both reachable deliver one copy to `Q`, not
  two.

Dead-lettering, by contrast, is a **fresh routing pass**: the DLX
receives a newly-published message with empty visited sets, so it can
re-reach exchanges and queues that the original pass already visited.

**Where the docs disagree:** Not really a LavinMQ-specific quirk —
RabbitMQ behaves the same way. But it's easy to forget when modelling
fanout semantics: a naive "one copy per binding, no dedup" reference
model over-counts every time a fan-in convergence happens.

**How we found it:** The routing-graph property test
(`src/tests.rs::routing_graph_delivers_expected`) compares observed
per-queue ack counts against a pure-Rust simulator. An initial version
of the simulator that pushed one copy per binding with no dedup
disagreed with LavinMQ on most non-trivial graphs. QuickCheck shrunk
the failing cases down to minimal diamond and fan-in reproducers, which
pinpointed the behaviour.

**How the simulator models it:**

- Each routing pass maintains its own `HashSet<usize>` of visited
  exchanges and queues. Revisits within a pass are skipped (before
  fan-out and before ack accounting).
- When a queue rejects with a DLX, a new routing event is queued with
  fresh empty visited sets.

See `src/routing.rs::simulate` for the implementation.

## 4. Unreadable `x-delay` values mean "no delay"

**Observed:** On a delayed exchange, any `x-delay` header LavinMQ can't
convert to a `u32` is silently treated as `0`, and the message is
delivered immediately. That covers negative integers of any width,
integers above `u32::MAX`, floats and doubles (even ≥ 1 h), strings like
`"3600000"`, booleans, timestamps and tables. Nothing is rejected.
Reproduced on LavinMQ 2.10.0.

**Why:** `SegmentPosition.make` reads the delay as
`headers["x-delay"]?.try { |v| v.as?(Int).try(&.to_u32) } || 0u32 rescue 0u32`.
A non-`Int` gives `nil → 0`, and an out-of-range `Int` raises
`OverflowError`, which the `rescue` turns into `0`. The message still goes
through the internal delayed queue, because the exchange only checks
whether an `x-delay` header is *present*.

**How this crate covers it:** `odd_x_delay_delivers_immediately` in
`src/tests.rs` publishes an `OddDelay` (`src/delayed.rs`) and expects
delivery within 2 s. Each odd value, read literally, would mean either no
delay or at least an hour.

## 5. Delayed exchange with a long name aborts the connection

**Observed:** Declaring a delayed exchange whose name is 245–255 bytes
aborts the client's whole TCP connection (lapin reports
`IOError(ConnectionAborted)`), not just the channel. The broker itself
stays healthy and no exchange is left behind. Reproduced on LavinMQ
2.10.0.

**Why:** The internal queue is named `amq.delayed-<exchange>` (12-byte
prefix). `DelayedExchangeQueue.create` does
`raise "Exchange name too long" if q_name.bytesize > MAX_NAME_LENGTH`
(256). That is a plain Crystal exception, not an AMQP channel error, so
it escapes as a connection-level failure.

**Expected:** A channel-level error (e.g. 406 `PRECONDITION_FAILED`),
with the connection still usable.

**How this crate covers it:** `delayed_exchange_long_name_is_clean_error`
asserts the expected behaviour and is `#[ignore]`d until LavinMQ is
fixed. Run it with `cargo test -- --ignored`.

**Upstream:** [cloudamqp/lavinmq#2297](https://github.com/cloudamqp/lavinmq/issues/2297)

## 6. Alternate exchanges fire per routing pass, not per exchange

**Observed:** LavinMQ hands a message to an exchange's alternate exchange
only if the **whole routing pass** has found no queue yet when that
exchange finishes walking its bindings. Bindings are walked in insertion
order, so the outcome depends on binding order. Reproduced on LavinMQ
2.10.0.

Two cases where this differs from RabbitMQ:

1. **Sibling order.** `e0` is bound to `q0` and to `e1`. `e1` has no
   bindings and an AE leading to `q1`. If `e0 → q0` was bound first,
   `q0` is already found when `e1` is visited, so `e1`'s AE is skipped
   and only `q0` gets the message. If `e0 → e1` was bound first, the AE
   fires and both get it. RabbitMQ fires `e1`'s AE either way.
2. **Exchange-to-exchange bindings.** `e0` (AE → `e2` → `q0`) is bound
   only to `e1`, which has no bindings. LavinMQ finds no queue, so
   `e0`'s AE fires and `q0` gets the message. RabbitMQ counts `e1` as a
   destination of `e0`, so `e0`'s AE does not fire.

**Why:** `Exchange#find_queues` shares one `queues` set across the pass
(including exchange-to-exchange hops) and checks
`if queues.empty? && (ae_name = alternate_exchange)` after its own
bindings. RabbitMQ's `process_alternate` only looks at what that
exchange's own `route` returned, where exchange destinations count.

**How this crate models it:** `simulate` in `src/routing.rs` walks
bindings depth-first in insertion order, and fires an AE iff the pass's
found-queue list is still empty. The generator shuffles binding order so
both sibling orders get exercised. The unit tests
`sub_exchange_ae_skipped_when_sibling_found_a_queue_first`,
`sub_exchange_ae_fires_when_visited_before_sibling_queue` and
`ae_fires_when_e2e_subtree_reaches_no_queue` pin the behaviour down.

## 7. Consistent-hash exchange with `x-algorithm` stops routing after any policy change

**Observed:** Creating or deleting *any* policy in the vhost, even one
that matches nothing, makes a consistent-hash exchange declared with
`x-algorithm` route nothing. Publishes are unroutable (`312 NO_ROUTE`),
yet the bindings are still listed. Deleting the policy doesn't bring
routing back. Without `x-algorithm` the exchange is unaffected.
Reproduced on LavinMQ 2.10.0 with both `ring` and `jump`.

**Why:** Every policy add or delete re-applies policies to every
exchange, which ends in `handle_arguments`.
`ConsistentHashExchange#handle_arguments` replaces `@hasher` with a new,
empty one whenever `x-algorithm` is set. `@bindings` is kept, so the API
still lists the bindings.

**Consequence for this crate:** Any test that changes policies
(`ae_argument_beats_policy`) breaks `consistent_hash_is_deterministic`
if the two run at the same time against the same vhost. The failures
look like "the first copies were delivered, everything after vanished".
Each test function now runs in its own vhost, so that no longer
happens. `consistent_hash_survives_policy_change` reproduces the bug on
purpose and is `#[ignore]`d until it's fixed.

**Upstream:** [cloudamqp/lavinmq#2300](https://github.com/cloudamqp/lavinmq/issues/2300)

## 8. Headers exchange matching

Observed on LavinMQ 2.10.0 and modelled by `matches` in `src/headers.rs`.
The property `headers_routing_matches_model` checks the model against the
broker, and mutation checks confirmed that LavinMQ really behaves this way
on each point below.

1. **Header-less messages match only bindings with completely empty
   arguments.** A message with no or empty headers matches a binding iff
   the binding's argument table is empty. An `x-match` entry counts, so a
   binding of just `{x-match: all}` *doesn't* match a header-less message,
   but *does* match every message that has at least one header (`all`
   over zero pairs). The same binding gives opposite answers depending on
   whether unrelated headers are present.
2. **Exchange-level `x-match` default.** A binding without `x-match`
   uses the exchange's own `x-match` argument (default `all`). Combined
   with point 1: on an exchange declared with `x-match: any`, a binding
   with empty arguments matches header-less messages but *no* message
   with headers. AMQP 0-9-1 only defines `x-match` as a binding argument.
3. **Numbers compare by value across types.** Values are compared with
   Crystal `==` after decoding, so `LongInt(1)`, `LongLongInt(1)` and
   `Double(1.0)` all match each other. Strings compare by bytes, and
   booleans only match booleans.
4. **No `all-with-x` / `any-with-x`.** Only `all` and `any` (exact,
   lowercase) are accepted, on bind and on exchange declare. Anything else,
   including RabbitMQ 3.10+'s `all-with-x` / `any-with-x`, fails with 406.
   Covered by `invalid_x_match_rejected_on_declare` / `_on_bind`.

**Upstream:** point 1 is [cloudamqp/lavinmq#2301](https://github.com/cloudamqp/lavinmq/issues/2301), point 2 is [cloudamqp/lavinmq#2302](https://github.com/cloudamqp/lavinmq/issues/2302).

**Generator note:** the value alphabet leaves out `ShortString`, because
lapin tags it `s` and LavinMQ decodes `s` as a 16-bit integer.
