# LavinMQ Queue Arguments

A reference of every `x-*` argument that LavinMQ recognizes when declaring a
queue (via `queue.declare` arguments or an equivalent policy), grouped by the
queue type they apply to.

LavinMQ supports three AMQP queue types plus one internal type used by the
MQTT bridge:

| Type               | How to declare                                  | Durable only? |
| ------------------ | ----------------------------------------------- | ------------- |
| **Classic**        | default — no `x-queue-type`, no `x-max-priority`| no            |
| **Priority**       | set `x-max-priority` (0–255)                    | no            |
| **Stream**         | `x-queue-type: "stream"`                        | yes (required)|
| **MQTT session**   | `x-queue-type: "mqtt"` — internal, not user-declared | yes      |

Unknown `x-queue-type` values are logged and fall back to a classic queue
(`src/lavinmq/queue_factory.cr:58`).

---

## Arguments shared by Classic and Priority queues

These are validated in `src/lavinmq/amqp/queue/queue.cr:36-47`. They can also
be set via a policy (policy keys drop the `x-` prefix). When the same setting
is provided by both the queue arguments and a policy, the policy wins for
values that are stricter than the argument.

### Size / length limits

| Argument               | Type         | Default | Description |
| ---------------------- | ------------ | ------- | ----------- |
| `x-max-length`         | integer ≥ 0  | —       | Max number of messages allowed. Over limit → apply `x-overflow`. |
| `x-max-length-bytes`   | integer ≥ 0  | —       | Max total byte size of messages. Over limit → apply `x-overflow`. |
| `x-overflow`           | string       | `drop-head` | Behavior when a limit is reached. Allowed: `drop-head` (remove oldest), `reject-publish` (NACK new publishes). Parsed in `src/lavinmq/amqp/queue/queue.cr:297-301`. |

### Time-based expiry

| Argument          | Type              | Default | Description |
| ----------------- | ----------------- | ------- | ----------- |
| `x-message-ttl`   | integer ≥ 0 (ms)  | —       | Per-message TTL. Expired messages are discarded (or dead-lettered). `0` = effectively expire immediately. |
| `x-expires`       | integer ≥ 1 (ms)  | —       | Queue-level TTL. If the queue is idle for this long it is deleted. |

### Dead-lettering

Defined in `src/lavinmq/amqp/argument/dead_lettering.cr`.

| Argument                      | Type   | Default | Description |
| ----------------------------- | ------ | ------- | ----------- |
| `x-dead-letter-exchange`      | string | —       | Exchange dead messages are republished to (on expiry, reject, or delivery-limit exceeded). |
| `x-dead-letter-routing-key`   | string | —       | Routing key used when dead-lettering. If omitted, the message's original routing key is used. |

### Delivery / consumer control

| Argument                    | Type              | Default                        | Description |
| --------------------------- | ----------------- | ------------------------------ | ----------- |
| `x-delivery-limit`          | integer ≥ 0       | —                              | Max redelivery attempts before the message is dead-lettered. `0` = unlimited. |
| `x-consumer-timeout`        | integer ≥ 0 (ms)  | `Config.instance.consumer_timeout` | Max idle time for a consumer before its connection is closed. `0` disables. |
| `x-single-active-consumer`  | boolean           | `false`                        | If true, only one consumer is active at a time; the rest are on standby. |

### Message deduplication

Deduplication is opt-in and requires a header to identify duplicates.

| Argument                  | Type              | Default | Description |
| ------------------------- | ----------------- | ------- | ----------- |
| `x-message-deduplication` | boolean           | `false` | Enable deduplication on this queue. |
| `x-deduplication-header`  | string            | —       | Name of the message header that carries the dedup key. |
| `x-cache-size`            | integer ≥ 0       | —       | Number of recent dedup keys to remember. |
| `x-cache-ttl`             | integer ≥ 0 (ms)  | —       | TTL for cached dedup keys. |

---

## Priority-queue-only arguments

Defined in `src/lavinmq/amqp/queue/priority_queue.cr:12-21`. Presence of
`x-max-priority` is what switches a queue from classic to priority
(`src/lavinmq/queue_factory.cr:46-48`).

| Argument         | Type            | Default | Description |
| ---------------- | --------------- | ------- | ----------- |
| `x-max-priority` | integer 0–255   | —       | Required to create a priority queue. Messages with higher priority are delivered first; within a priority level FIFO applies. |

A priority queue accepts all the classic-queue arguments above *in addition*
to `x-max-priority`.

---

## Stream-queue arguments

Declared with `x-queue-type: "stream"`. Streams must be durable; they may not
be exclusive or auto-delete
(`src/lavinmq/amqp/stream/stream.cr:12-13`).

### Accepted

| Argument              | Type                  | Default | Description |
| --------------------- | --------------------- | ------- | ----------- |
| `x-queue-type`        | string (`stream`)     | —       | Selects the stream queue type. |
| `x-max-length`        | integer ≥ 0           | —       | Retention trigger by message count. Oldest segment(s) removed once exceeded. |
| `x-max-length-bytes`  | integer ≥ 0           | —       | Retention trigger by total byte size. |
| `x-max-age`           | string duration       | —       | Retention trigger by age. Format: number + unit, where unit is one of `Y` (year), `M` (month), `D` (day), `h` (hour), `m` (minute), `s` (second). Examples: `"7D"`, `"12h"`, `"1M"`. Validated in `src/lavinmq/amqp/stream/stream.cr:34-36`. |

### Rejected

Passing any of these to a stream raises `PRECONDITION_FAILED`
(`src/lavinmq/amqp/stream/stream.cr:19-33`):

- `x-dead-letter-exchange`
- `x-dead-letter-routing-key`
- `x-expires`
- `x-delivery-limit`
- `x-overflow`
- `x-single-active-consumer`
- `x-max-priority`

Not applicable to streams (silently ignored, since streams don't remove
messages on ack): `x-message-ttl`, `x-consumer-timeout`, dedup arguments.

### Stream consumer arguments

These are passed on **`basic.consume`**, not on queue declaration. They only
apply to consumers of stream queues.

| Argument                              | Type                                              | Default  | Description |
| ------------------------------------- | ------------------------------------------------- | -------- | ----------- |
| `x-stream-offset`                     | integer \| timestamp \| `first` \| `next` \| `last` | `next` | Where to start reading. Integers are absolute offsets, numbered from 1 (`0` also means the first message; above the last offset behaves like `next`). Negative `-N` (2.10+) starts at the last N messages, clamped to the first. Timestamps seek by time and are decoded as Unix **seconds**. `last` is the first message of the *last segment*, not the last message. See `src/lavinmq/amqp/stream/stream_consumer.cr:23-68`. |
| `x-stream-automatic-offset-tracking`  | boolean                                           | `true` (except for `amq.ctag-*` consumer tags) | Server-side tracking of consumer offset for crash recovery. |
| `x-stream-filter`                     | string \| table \| array                          | —        | Bloom-filter-based message filter. Accepts a comma-separated string, a JSON map, or an array of filter specs (including geo-spatial filters). See `src/lavinmq/amqp/stream/filters/filter.cr`. |
| `x-filter-match-type`                 | string (`all` \| `any`)                           | `all`    | Whether all filters must match or any single one. |
| `x-stream-match-unfiltered`           | boolean                                           | `false`  | If true, messages without the filter header are still delivered. |

### Consumer priority (`x-priority`)

Also passed on **`basic.consume`**, for classic and priority queues.

| Argument     | Type                        | Default | Description |
| ------------ | --------------------------- | ------- | ----------- |
| `x-priority` | integer (any width) fitting `i32` | `0` | A consumer only gets messages when every consumer with a higher priority is out of prefetch capacity. Equal priorities share. Out-of-range integers and non-integers (including an explicit void/null) → `406 PRECONDITION_FAILED`. Rejected on stream consumers. Ignored on single-active-consumer queues. See `src/lavinmq/amqp/consumer.cr` (`consumer_priority`, `wait_for_priority_consumers`). |

---

## Policy-only arguments

These cannot be set directly on a queue; they are applied via policy.

| Policy key                  | Description |
| --------------------------- | ----------- |
| `federation-upstream`       | Binds the queue to a single federation upstream. |
| `federation-upstream-set`   | Binds the queue to a named set of upstreams. |

Applied in `src/lavinmq/amqp/queue/queue.cr:324-329`.

---

## MQTT session queue

Declared internally with `x-queue-type: "mqtt"` when an MQTT client session
is created. It is not something client code creates directly, but the value
is listed here for completeness (`src/lavinmq/queue_factory.cr:54-56`).

---

## Headers LavinMQ sets automatically

Not queue arguments, but good to know — these appear on messages as a side
effect of queue behavior:

| Header                  | Set by                    | Meaning |
| ----------------------- | ------------------------- | ------- |
| `x-delivery-count`      | queue, on redelivery      | Increment-per-redeliver counter; present only when `x-delivery-limit` is set and count > 0 (`src/lavinmq/amqp/queue/queue.cr:784`). |
| `x-death` / `x-first-death-*` | dead-lettering       | Standard dead-letter history. |
| `x-stream-offset`       | stream consumer delivery  | The message's offset within the stream. |
| `x-stream-filter-value` | stream filtering          | Used during filter evaluation. |

`x-delay` on a *published* message is honored by the delayed-message exchange
— it is a message header, not a queue argument.

---

## Quick reference: what applies where

| Argument                        | Classic | Priority | Stream |
| ------------------------------- | :-----: | :------: | :----: |
| `x-max-length`                  | ✅      | ✅       | ✅     |
| `x-max-length-bytes`            | ✅      | ✅       | ✅     |
| `x-max-age`                     | ❌      | ❌       | ✅     |
| `x-message-ttl`                 | ✅      | ✅       | ❌     |
| `x-expires`                     | ✅      | ✅       | ❌     |
| `x-overflow`                    | ✅      | ✅       | ❌     |
| `x-dead-letter-exchange`        | ✅      | ✅       | ❌     |
| `x-dead-letter-routing-key`     | ✅      | ✅       | ❌     |
| `x-delivery-limit`              | ✅      | ✅       | ❌     |
| `x-consumer-timeout`            | ✅      | ✅       | ❌     |
| `x-single-active-consumer`      | ✅      | ✅       | ❌     |
| `x-max-priority`                | ❌      | ✅ (req) | ❌     |
| `x-message-deduplication` and friends | ✅ | ✅     | ❌     |
| `x-queue-type`                  | n/a     | n/a      | ✅ (req)|

> Note: LavinMQ does **not** implement RabbitMQ's quorum queues,
> `x-queue-mode`, `x-queue-leader-locator`, or stream `x-stream-*-bytes`
> tuning arguments. If you're porting from RabbitMQ docs, check here first.
