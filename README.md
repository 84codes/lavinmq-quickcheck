# lavinmq-quickcheck

Property-based tests for [LavinMQ](https://github.com/cloudamqp/lavinmq),
built on [QuickCheck](https://crates.io/crates/quickcheck).

The crate provides `Arbitrary` generators for AMQP entities, arguments and
routing setups, and for MQTT 3.1.1 topics, sessions and malformed packets,
plus a suite of tests that runs them against a real broker.
Where routing logic is involved, the tests compare what LavinMQ does with
a small pure-Rust model of what it should do. The suite has found several
LavinMQ bugs. Each is written up in [`lavinmq-quirks.md`](lavinmq-quirks.md).

## What's covered

| Area | Generators | What the broker tests check |
| --- | --- | --- |
| Names | `QueueName`, `RoutingKey`, `TopicRoutingKey` | Publish/consume round trips via the default, direct and topic exchanges |
| Queue arguments | Every LavinMQ `x-*` queue argument, alone and combined per queue type (classic, priority, stream) | Valid arguments are accepted. Streams reject arguments they don't support |
| Message properties | Every `BasicProperties` field, plus a combined set | Publishing with any of them succeeds |
| Routing graphs | Fanout exchanges, exchange-to-exchange bindings, dead-lettering, alternate exchanges, all acyclic | Per-queue delivery counts match `routing::simulate` |
| Alternate exchanges | Alternate-exchange loops, an alternate exchange that doesn't exist, argument vs. policy | Loops end, unroutable mandatory messages are returned, the argument wins over a policy |
| Consistent-hash exchange | `ring`/`jump`, `x-hash-on`, weighted bind/unbind sequences | The same key always lands on the same queue |
| Stream offsets | `x-stream-offset` as `first`, `next`, integers (every AMQP int width, negative = last N), timestamps | Delivered messages match the offset model |
| Delayed-message exchange | Both declare styles, `x-delay` values that can't be read as a delay, over-long names | Such `x-delay` values mean "deliver now". Over-long names fail cleanly (ignored test, see below) |
| Consumer priority | `x-priority` values, valid and invalid | In-range integers are accepted, everything else gets 406 |
| Headers exchange | Exchange and binding `x-match`, messages without headers, values of mixed types | Routing matches `headers::matches`. Invalid `x-match` gets 406 |
| MQTT routing | Topic/filter pairs (`+`, `#`, empty levels, `$` topics, non-ASCII), QoS 0–2 | Delivery matches `mqtt::topic::lavinmq_matches`. Granted and delivered QoS match `mqtt::qos` |
| MQTT retain, will, sessions | Retained publish/clear sequences, wills, clean/persistent reconnects | New subscriptions get the latest retained message. Wills only fire on an unclean close. Queued messages and Session Present follow `mqtt::session` |
| MQTT malformed packets | Bad topic names and filters, ill-formed UTF-8, QoS 3, bad SUBSCRIBE flags, bad protocol levels | Sent over a raw socket. The broker closes the connection, or refuses with CONNACK 0x01 / SUBACK 0x80 |

[`queue-arguments.md`](queue-arguments.md) is a reference for the arguments
LavinMQ accepts.

## Requirements

- Rust (edition 2024, so a recent stable toolchain).
- A LavinMQ broker on `localhost:5672` (AMQP) and `localhost:1883`
  (MQTT), with the management API on `localhost:15672` and the default
  `guest:guest` user:

  ```sh
  docker run -d --rm -p 5672:5672 -p 15672:15672 -p 1883:1883 cloudamqp/lavinmq
  ```

These addresses are hard-coded.

## Running the tests

```sh
cargo test                                         # whole suite (~45 s)
cargo test headers                                 # only tests whose name contains "headers"
cargo test -- --ignored                            # only the known-bug tests
QUICKCHECK_TESTS=20 cargo test                     # fewer cases per property
```

Tests run in parallel against one broker. Each test function gets its own
vhost, `qc-<test name>`, which is deleted and recreated the first time the
test uses it. That stops one test's policies or queue names from affecting
another. MQTT tests reach their vhost through the username
(`qc-<test name>:guest`). The `qc-*` vhosts stay on the broker after a run.

Tests that don't need a broker (the routing, stream-offset and
headers-matching models, and checks on what the generators produce) run as
ordinary unit tests in the same suite.

### Known LavinMQ bugs

Tests marked `#[ignore]` describe what LavinMQ *should* do for a bug that's
still open. They fail until the bug is fixed:

| Test | Upstream issue |
| --- | --- |
| `delayed_exchange_long_name_is_clean_error` | [cloudamqp/lavinmq#2297](https://github.com/cloudamqp/lavinmq/issues/2297) |
| `consistent_hash_survives_policy_change` | [cloudamqp/lavinmq#2300](https://github.com/cloudamqp/lavinmq/issues/2300) |
| `topic_trailing_empty_word_routes` | [cloudamqp/lavinmq#2310](https://github.com/cloudamqp/lavinmq/issues/2310) |
| `mqtt_hash_matches_parent_level` | [cloudamqp/lavinmq#2312](https://github.com/cloudamqp/lavinmq/issues/2312) |
| `mqtt_wildcards_skip_dollar_topics` | [cloudamqp/lavinmq#2313](https://github.com/cloudamqp/lavinmq/issues/2313) |
| `mqtt_retained_non_ascii_topics` | [cloudamqp/lavinmq#2316](https://github.com/cloudamqp/lavinmq/issues/2316) |
| `internal_exchange_redeclare` | [cloudamqp/lavinmq#2319](https://github.com/cloudamqp/lavinmq/issues/2319) |

Two headers-exchange inconsistencies are filed as
[#2301](https://github.com/cloudamqp/lavinmq/issues/2301) and
[#2302](https://github.com/cloudamqp/lavinmq/issues/2302). Until they're
resolved, the headers model mirrors LavinMQ's current behaviour.

## Using the generators

Generators follow the newtype pattern. Each wraps a value that's valid for
the broker, and arguments can add themselves to a `FieldTable`:

```rust
use lavinmq_quickcheck::QueueName;
use lavinmq_quickcheck::arguments::MaxLength;
use lapin::types::FieldTable;
use quickcheck_macros::quickcheck;

#[quickcheck]
fn declare_with_max_length(name: QueueName, max_len: MaxLength) -> bool {
    let mut args = FieldTable::default();
    max_len.insert_into(&mut args);
    // declare `name.0` with `args` against your broker…
    true
}
```

## Running in CI without Rust

[`.github/workflows/release.yml`](.github/workflows/release.yml) compiles the
whole suite into one static (musl) executable. It smoke-tests that binary
against LavinMQ and, on `v*` tags, publishes it to a GitHub Release.
Another project's CI can download and run it with no Rust toolchain:

```sh
./lavinmq-quickcheck-tests-x86_64-linux
```

[`docs/lavinmq-ci.md`](docs/lavinmq-ci.md) has ready-made jobs for LavinMQ's
GitHub Actions setup. To build the binary locally you also need a musl C
compiler (`musl-gcc`, from `musl-tools`) for `ring`.

## Layout

```
src/
  names.rs            queue names, routing keys
  arguments.rs        x-* queue arguments
  combined.rs         combined arguments per queue type
  properties.rs       BasicProperties fields
  routing.rs          routing-graph generator + simulate()
  alternate.rs        alternate-exchange edge cases
  consistent_hash.rs  consistent-hash exchange setups
  stream_offset.rs    x-stream-offset values + offset model
  delayed.rs          delayed-message exchange setups
  consumer_priority.rs x-priority values
  headers.rs          headers exchange setups + matching model
  mqtt/               MQTT 3.1.1: topics + matching, QoS, retain, sessions,
                      malformed input; rumqttc and raw-socket tests
  tests.rs            the suite that runs against the broker
lavinmq-quirks.md     LavinMQ behaviour found by these tests
queue-arguments.md    reference for LavinMQ queue/consumer arguments
docs/lavinmq-ci.md    running the suite in LavinMQ's CI
```

See [`AGENTS.md`](AGENTS.md) for conventions when adding generators or tests.
