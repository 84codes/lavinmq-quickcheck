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
