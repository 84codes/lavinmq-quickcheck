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

**Over HTTP:** `PUT /api/exchanges/{vhost}/{name}` for the same names
answers `500 Internal Server Error`, whichever way the exchange is made
delayed (`x-delayed-message`, `x-delayed-exchange: true`, or the
HTTP-only `"delayed": true` body field). Nothing is left behind.
`delayed_long_name_is_bad_request` (in `src/http/validation.rs`) asserts
a 400 and is `#[ignore]`d.

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

## 9. HTTP API rejects reserved `amq.` names with 400, not 403

**Observed:** Declaring a queue or exchange named `amq.*` over AMQP fails
with 403 `ACCESS_REFUSED`. The same declaration over the HTTP API
(`PUT /api/queues/{vhost}/{name}`, `PUT /api/exchanges/{vhost}/{name}`)
answers `400 Bad Request` with the same reason text. Reproduced on
LavinMQ 2.10.0.

**Expected:** `403 Forbidden`, matching AMQP. The HTTP API already
answers 403 to binding to the default exchange, which AMQP also refuses
with 403.

**How this crate covers it:** `reserved_prefix_parity` (in
`src/http/queues.rs`) asserts the AMQP and HTTP outcomes agree and is
`#[ignore]`d until LavinMQ is fixed.

## 10. Binding `properties_key` collides for routing keys `""` and `"~"`

**Observed:** The HTTP API identifies a binding by its `properties_key`:
the routing key (or `~` if it is empty), plus `~<base64 of the
arguments>` if it has arguments. A binding with routing key `""` and one
with routing key `"~"` (same source, destination, no arguments) both get
`properties_key` `"~"`. `DELETE /api/bindings/{vhost}/e/{x}/q/{q}/~`
removes the `""` binding first. You can't target the `"~"` binding until
it's the only one left. Reproduced on LavinMQ 2.10.0.

**Expected:** Each binding of a source/destination pair has its own
`properties_key`, e.g. by escaping `~` in the routing key.

**How this crate covers it:** `properties_keys_are_unique` (in
`src/http/bindings.rs`) is `#[ignore]`d until LavinMQ is fixed.
`RoutingKey` never generates `~`, so `binding_parity` doesn't hit it.

## 11. `/get` and `/publish` spell `cluster_id` differently

**Observed:** `POST /api/exchanges/{vhost}/{name}/publish` reads the
`cluster_id` property from `properties.reserved`, as the OpenAPI spec says.
`POST /api/queues/{vhost}/{name}/get` returns it as `properties.reserved1`.
`/publish` silently ignores `reserved1`, so republishing a message read
with `/get` drops its `cluster_id`. Reproduced on LavinMQ 2.10.0.

**Expected:** One spelling in both directions.

**How this crate covers it:** `get_properties_republish_unchanged` (in
`src/http/messages.rs`) is `#[ignore]`d. `publish_parity`/`get_parity`
rename `reserved1` to `reserved` before comparing.

## 12. Timestamps above `i64::MAX` over HTTP

**Observed:** The AMQP `timestamp` property is an unsigned 64-bit integer.
LavinMQ stores one above `i64::MAX` fine over AMQP, but `/get` renders it
as a negative number (`u64::MAX` reads back as `-1`). `/publish` rejects
such a timestamp with `400`. Reproduced on LavinMQ 2.10.0.

**Expected:** The unsigned value in both directions.

**How this crate covers it:** `large_timestamp_round_trips` (in
`src/http/messages.rs`) is `#[ignore]`d. `publish_parity`/`get_parity`
discard cases with such timestamps.

## 13. `/get` returns URL-safe base64

**Observed:** `POST /api/queues/{vhost}/{name}/get` with
`"encoding": "base64"` returns payloads in the URL-safe base64 alphabet
(`-` and `_`). `/publish` with `"payload_encoding": "base64"` takes the
standard alphabet (`+` and `/`), and a standard decoder fails on what
`/get` returns. Reproduced on LavinMQ 2.10.0.

**Expected:** The standard alphabet (RFC 4648 §4), as `/publish` takes it.

**How this crate covers it:** `get_payload_is_standard_base64` (in
`src/http/messages.rs`) is `#[ignore]`d. `publish_parity`/`get_parity`
translate the payload to the standard alphabet before comparing.

## 15. HTTP accepts over-long short strings it never encodes

**Observed:** AMQP short strings (routing keys, most message properties)
are at most 255 bytes. The HTTP API only enforces that when it encodes the
value into a message:

- `POST /api/bindings/{vhost}/e/{x}/q/{q}` creates a binding whose routing
  key is longer than 255 bytes. AMQP can't express that binding, and a
  routed `/publish` with that key gets 400, so nothing can ever match it.
- `/publish` of an *unroutable* message with an over-long routing key or
  property (`message_id`, `correlation_id`, ...) answers 200
  `{"routed": false}`. The same message routed gets 400 "Short string too
  long, max 255".

Reproduced on LavinMQ 2.10.0.

**Expected:** 400 for any short-string field over 255 bytes, before
routing.

**How this crate covers it:** `binding_key_length_validated` and
`unroutable_publish_field_length_validated` (in `src/http/validation.rs`)
are `#[ignore]`d. `publish_field_length_validated` covers the routed case,
which works.

## 16. HTTP declare bodies coerce wrong JSON types to `false` / `{}`

**Observed:** In `PUT /api/queues/...` and `PUT /api/exchanges/...`, a
`durable`, `auto_delete` or `internal` that isn't a JSON boolean is
accepted (201) and stored as `false`. That includes the string `"true"`,
so `{"durable": "true"}` silently creates a non-durable queue. An
`arguments` that isn't an object is accepted and stored as `{}`. Only an
exchange's `type` is type-checked (400). Reproduced on LavinMQ 2.10.0.

**Expected:** 400 for a field of the wrong type, like `type` gets.

**How this crate covers it:** `wrong_json_types_rejected` (in
`src/http/validation.rs`) is `#[ignore]`d.

## 17. `/publish` with a header key over 255 bytes is a 500

**Observed:** A `properties.headers` key longer than 255 bytes (a short
string in AMQP field tables) makes `/publish` answer `500 Internal Server
Error`, routed or not. Reproduced on LavinMQ 2.10.0.

**Expected:** 400, like an over-long routing key on a routed message.

**How this crate covers it:** `header_key_length_validated` (in
`src/http/validation.rs`) is `#[ignore]`d.

## 18. MQTT wildcard matching

Observed on LavinMQ 2.10.0 and modelled by `lavinmq_matches` in
`src/mqtt/topic.rs`. The property `mqtt_wildcard_routing_matches_model`
checks the model against the broker. Apart from these two points, LavinMQ
matches the spec model `matches`.

1. **`#` doesn't match the parent level.** `sport/#` doesn't match
   `sport`. MQTT 3.1.1 §4.7.1.2 says it must ("sport/tennis/player1/#"
   matches "sport/tennis/player1"). `sport/#` does match `sport/`, whose
   last level is empty. Only the filter `#` on its own matches every
   topic.
2. **Wildcards match `$` topics.** `#` and `+/…` match topics that start
   with `$`, such as `$sys/x`. §4.7.2 says a filter starting with a
   wildcard must not match them. LavinMQ has no `$SYS` tree, so this only
   matters for clients that publish to `$` topics themselves.

**Upstream:** point 1 is [cloudamqp/lavinmq#2312](https://github.com/cloudamqp/lavinmq/issues/2312), point 2 is [cloudamqp/lavinmq#2313](https://github.com/cloudamqp/lavinmq/issues/2313).

**How this crate covers it:** `mqtt_hash_matches_parent_level` and
`mqtt_wildcards_skip_dollar_topics` (in `src/mqtt/tests.rs`) assert the
spec behaviour and are `#[ignore]`d.

## 19. MQTT QoS 2 PUBLISH gets a PUBACK

**Observed:** LavinMQ has no QoS 2. A SUBSCRIBE asking for QoS 2 is
granted QoS 1, which §3.9.3 allows. But a PUBLISH at QoS 2 is answered
with a PUBACK (the QoS 1 ack), and the PUBREL a client then sends closes
the connection. Clients that implement QoS 2 never see the PUBREC they
wait for. With rumqttc the publish just never completes. Reproduced on
LavinMQ 2.10.0.

**Expected:** §4.3.3: the receiver of a QoS 2 PUBLISH must answer PUBREC,
then PUBCOMP to the PUBREL. MQTT 3.1.1 has no way to refuse QoS 2, so a
server without it should still run the handshake, even if it then
delivers at QoS 1.

**Upstream:** [cloudamqp/lavinmq#2314](https://github.com/cloudamqp/lavinmq/issues/2314).
Fixed by [cloudamqp/lavinmq#2236](https://github.com/cloudamqp/lavinmq/pull/2236),
which adds QoS 2 and grants it on SUBSCRIBE.

**How this crate covers it:** `mqtt_qos2_publish_gets_pubrec` (raw
socket). `mqtt_subscribe_grants_requested_qos` expects a QoS 2 grant, and
`mqtt_delivery_qos_is_the_minimum` publishes at QoS 2 too.

## 20. MQTT QoS 0 publishes are delivered at QoS 1

**Observed:** A message is always delivered at the subscription's
granted QoS, whatever QoS it was published at. A QoS 0 publish therefore
reaches a QoS 1 subscription as a QoS 1 message, with a packet id the
client has to PUBACK. Reproduced on LavinMQ 2.10.0.

**Expected:** §3.8.4: delivery QoS is the minimum of the publish QoS and
the granted QoS (`Qos::delivered`).

**Upstream:** [cloudamqp/lavinmq#2315](https://github.com/cloudamqp/lavinmq/issues/2315).

Fixed by [cloudamqp/lavinmq#2236](https://github.com/cloudamqp/lavinmq/pull/2236).

**How this crate covers it:** `mqtt_delivery_qos_is_the_minimum` checks
`Qos::delivered`, and `mqtt_qos0_publish_is_delivered_at_qos0` the QoS 0
case on its own.

## 21. MQTT retained messages break on non-ASCII topics

**Observed:** With multi-byte UTF-8 in a topic, a new subscription can
miss retained messages it matches. For example, a message retained on
`p/€/€` isn't sent to a new subscription to `p/€/+`, but is sent for
`p/€/€` or `p/#`. Live routing of the same topics works. Reproduced on
LavinMQ 2.10.0.

**Cause (from the LavinMQ source):** the retain store's topic tree splits
topics with `StringTokenIterator`, which compares `Char::Reader#pos` (a
byte offset) with `String#size` (a char count) and slices by char index
with byte offsets. Once a multi-byte character has been read, tokens are
cut in the wrong places and iteration stops early. Live routing uses
`BytesTokenIterator`, which works on bytes throughout.

**Upstream:** [cloudamqp/lavinmq#2316](https://github.com/cloudamqp/lavinmq/issues/2316).

**How this crate covers it:** `mqtt_new_subscription_gets_retained`
discards scenarios with non-ASCII topics or filters.
`mqtt_retained_non_ascii_topics` runs the same property on all of them
and is `#[ignore]`d.

## 22. Topic routing keys lose a trailing empty word

**Observed:** On a topic exchange, a routing key ending in `.` never
matches, not even a binding with the identical key. `a.`, `a.b.`, `.` and
`a..` don't route to bindings with the same key. Nor does `a.` route to
`a.*` or `*.*`. Leading and middle empty words work (`.a`, `a..b`,
`a.*.c` vs `a..c`). Reproduced on LavinMQ 2.10.0.

**Why:** binding keys are split with `String#split(".")`, which keeps the
trailing `""`, so `a.` is `["a", ""]`. Routing keys are walked with
`RkIterator`, which stops when nothing is left after the last dot, so it
yields only `a`. The word counts differ.

**Expected:** RabbitMQ splits both keys with
`binary:split(Key, <<".">>, [global])`, which keeps trailing empty parts,
so `a.` is two words on both sides.

**Upstream:** [cloudamqp/lavinmq#2310](https://github.com/cloudamqp/lavinmq/issues/2310).

**How this crate covers it:** `topic_trailing_empty_word_routes` (in
`src/tests.rs`) is `#[ignore]`d. This was the cause of the "flaky"
`odd_x_delay_delivers_immediately`: `DelayedScenario` uses `RoutingKey`,
which can end in `.`. On a topic-typed delayed exchange such a message was
released immediately and then dropped as unroutable. The generator now
never gives a topic scenario a trailing `.`.

## 23. HTTP redeclare of an internal exchange is rejected

**Observed:** `PUT /api/exchanges/{vhost}/{name}` on an existing internal
exchange, with identical settings, answers 400 "Not allowed to publish to
internal exchange". Any exchange type. Over AMQP the same redeclare is a
plain `declare-ok`. Reproduced on LavinMQ 2.10.0.

**Why:** the PUT handler checks `e.internal?` after matching the existing
exchange. That check looks copied from the `/publish` handler, and
nothing is published here.

**Expected:** 204, as for a non-internal exchange.

**Upstream:** [cloudamqp/lavinmq#2319](https://github.com/cloudamqp/lavinmq/issues/2319).

**How this crate covers it:** `internal_exchange_redeclare` (in
`src/http/exchanges.rs`) is `#[ignore]`d. `exchange_declare_parity`
discards cases with more than one internal declaration.
