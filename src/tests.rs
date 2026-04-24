use crate::names::{QueueName, RoutingKey, TopicRoutingKey};
use crate::routing::{Binding, QueueAction, QueueNode, Topology, simulate};
use futures_lite::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        ExchangeBindOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions,
        QueueDeclareOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable, ShortString},
};
use quickcheck_macros::quickcheck;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

async fn connect_channel() -> lapin::Channel {
    let conn = Connection::connect("amqp://localhost:5672", ConnectionProperties::default())
        .await
        .expect("Failed to connect to LavinMQ");
    conn.create_channel()
        .await
        .expect("Failed to create channel")
}

fn classic_queue_opts() -> QueueDeclareOptions {
    QueueDeclareOptions {
        auto_delete: true,
        ..QueueDeclareOptions::default()
    }
}

fn stream_queue_opts() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..QueueDeclareOptions::default()
    }
}

async fn declare_classic_ok(channel: &lapin::Channel, name: &str, args: FieldTable) -> bool {
    let res = channel
        .queue_declare(name, classic_queue_opts(), args)
        .await
        .is_ok();
    // Cleanup (best-effort; ignore errors — channel may already be closed on failure).
    let _ = channel
        .queue_delete(name, QueueDeleteOptions::default())
        .await;
    res
}

async fn declare_stream_ok(channel: &lapin::Channel, name: &str, mut args: FieldTable) -> bool {
    args.insert(
        lapin::types::ShortString::from("x-queue-type"),
        lapin::types::AMQPValue::LongString("stream".into()),
    );
    let res = channel
        .queue_declare(name, stream_queue_opts(), args)
        .await
        .is_ok();
    let _ = channel
        .queue_delete(name, QueueDeleteOptions::default())
        .await;
    res
}

/// Returns true iff `queue_declare` fails with AMQP reply code 406 (PRECONDITION_FAILED).
/// Uses a string-based check against the error's `Debug` output rather than matching on
/// lapin's error enum variants, which change shape between 2.x versions. The Debug repr
/// reliably contains either "406" (the numeric code) or "PRECONDITION_FAILED" (the name)
/// for this broker-originated channel-close error.
async fn declare_stream_rejects(
    channel: &lapin::Channel,
    name: &str,
    mut args: FieldTable,
) -> bool {
    args.insert(
        lapin::types::ShortString::from("x-queue-type"),
        lapin::types::AMQPValue::LongString("stream".into()),
    );
    match channel.queue_declare(name, stream_queue_opts(), args).await {
        Ok(_) => {
            // Unexpected success — clean up so we don't leak.
            let _ = channel
                .queue_delete(name, QueueDeleteOptions::default())
                .await;
            false
        }
        Err(err) => {
            let msg = format!("{err:?}");
            msg.contains("406") || msg.contains("PRECONDITION_FAILED")
        }
    }
}

#[quickcheck]
fn round_trip(name: QueueName, payload: Vec<u8>) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;

        let queue_opts = QueueDeclareOptions {
            auto_delete: true,
            ..QueueDeclareOptions::default()
        };
        channel
            .queue_declare(&name.0, queue_opts, FieldTable::default())
            .await
            .expect("Failed to declare queue");

        channel
            .basic_publish(
                "",
                &name.0,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .expect("Failed to publish")
            .await
            .expect("Failed to confirm publish");

        let mut consumer = channel
            .basic_consume(
                &name.0,
                "test-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to start consumer");

        let delivery = consumer
            .next()
            .await
            .expect("Consumer stream ended unexpectedly")
            .expect("Failed to receive delivery");

        delivery
            .ack(lapin::options::BasicAckOptions::default())
            .await
            .expect("Failed to ack");

        let matches = delivery.data == payload;

        channel
            .queue_delete(&name.0, QueueDeleteOptions::default())
            .await
            .expect("Failed to delete queue");

        matches
    })
}

#[quickcheck]
fn direct_exchange_round_trip(routing_key: RoutingKey, payload: Vec<u8>) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;

        let queue_opts = QueueDeclareOptions {
            auto_delete: true,
            ..QueueDeclareOptions::default()
        };
        let queue = channel
            .queue_declare("", queue_opts, FieldTable::default())
            .await
            .expect("Failed to declare queue");
        let queue_name = queue.name().as_str();

        channel
            .queue_bind(
                queue_name,
                "amq.direct",
                &routing_key.0,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to bind queue");

        channel
            .basic_publish(
                "amq.direct",
                &routing_key.0,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .expect("Failed to publish")
            .await
            .expect("Failed to confirm publish");

        let mut consumer = channel
            .basic_consume(
                queue_name,
                "test-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to start consumer");

        let delivery = consumer
            .next()
            .await
            .expect("Consumer stream ended unexpectedly")
            .expect("Failed to receive delivery");

        delivery
            .ack(lapin::options::BasicAckOptions::default())
            .await
            .expect("Failed to ack");

        let matches = delivery.data == payload;

        channel
            .queue_delete(queue_name, QueueDeleteOptions::default())
            .await
            .expect("Failed to delete queue");

        matches
    })
}

#[quickcheck]
fn topic_exchange_round_trip(topic: TopicRoutingKey, payload: Vec<u8>) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;

        let queue_opts = QueueDeclareOptions {
            auto_delete: true,
            ..QueueDeclareOptions::default()
        };
        let queue = channel
            .queue_declare("", queue_opts, FieldTable::default())
            .await
            .expect("Failed to declare queue");
        let queue_name = queue.name().as_str();

        channel
            .queue_bind(
                queue_name,
                "amq.topic",
                &topic.binding_pattern,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to bind queue");

        channel
            .basic_publish(
                "amq.topic",
                &topic.routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .expect("Failed to publish")
            .await
            .expect("Failed to confirm publish");

        let mut consumer = channel
            .basic_consume(
                queue_name,
                "test-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to start consumer");

        let delivery = consumer
            .next()
            .await
            .expect("Consumer stream ended unexpectedly")
            .expect("Failed to receive delivery");

        delivery
            .ack(lapin::options::BasicAckOptions::default())
            .await
            .expect("Failed to ack");

        let matches = delivery.data == payload;

        channel
            .queue_delete(queue_name, QueueDeleteOptions::default())
            .await
            .expect("Failed to delete queue");

        matches
    })
}

use crate::arguments::{
    CacheSize, CacheTtl, ConsumerTimeout, DeadLetterExchange, DeadLetterRoutingKey,
    DeduplicationHeader, DeliveryLimit, Expires, MaxLength, MaxLengthBytes, MaxPriority,
    MessageDeduplication, MessageTtl, Overflow, SingleActiveConsumer,
};

macro_rules! single_arg_classic_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, arg: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let mut table = FieldTable::default();
                arg.insert_into(&mut table);
                declare_classic_ok(&channel, &name.0, table).await
            })
        }
    };
}

single_arg_classic_test!(declare_with_max_length, MaxLength);
single_arg_classic_test!(declare_with_max_length_bytes, MaxLengthBytes);
single_arg_classic_test!(declare_with_message_ttl, MessageTtl);
single_arg_classic_test!(declare_with_expires, Expires);
single_arg_classic_test!(declare_with_delivery_limit, DeliveryLimit);
single_arg_classic_test!(declare_with_consumer_timeout, ConsumerTimeout);
single_arg_classic_test!(declare_with_cache_size, CacheSize);
single_arg_classic_test!(declare_with_cache_ttl, CacheTtl);
single_arg_classic_test!(declare_with_overflow, Overflow);
single_arg_classic_test!(declare_with_single_active_consumer, SingleActiveConsumer);
single_arg_classic_test!(declare_with_message_deduplication, MessageDeduplication);
single_arg_classic_test!(declare_with_dead_letter_exchange, DeadLetterExchange);
single_arg_classic_test!(declare_with_deduplication_header, DeduplicationHeader);

// LavinMQ rejects `x-dead-letter-routing-key` when `x-dead-letter-exchange`
// isn't also set (an orphan routing key has nowhere to route). Set a fixed
// DLX alongside so the per-arg test exercises only the routing-key value.
#[quickcheck]
fn declare_with_dead_letter_routing_key(name: QueueName, arg: DeadLetterRoutingKey) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        table.insert(
            lapin::types::ShortString::from("x-dead-letter-exchange"),
            lapin::types::AMQPValue::LongString("amq.direct".into()),
        );
        arg.insert_into(&mut table);
        declare_classic_ok(&channel, &name.0, table).await
    })
}

single_arg_classic_test!(declare_with_max_priority, MaxPriority);

use crate::arguments::MaxAge;

macro_rules! single_arg_stream_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, arg: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let mut table = FieldTable::default();
                arg.insert_into(&mut table);
                declare_stream_ok(&channel, &name.0, table).await
            })
        }
    };
}

single_arg_stream_test!(declare_stream_with_max_age, MaxAge);

use crate::combined::ClassicQueueArgs;

#[quickcheck]
fn declare_classic_with_combined_args(name: QueueName, args: ClassicQueueArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        args.apply(&mut table);
        declare_classic_ok(&channel, &name.0, table).await
    })
}

use crate::combined::PriorityQueueArgs;

#[quickcheck]
fn declare_priority_with_combined_args(name: QueueName, args: PriorityQueueArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        args.apply(&mut table);
        declare_classic_ok(&channel, &name.0, table).await
    })
}

use crate::combined::StreamQueueArgs;

#[quickcheck]
fn declare_stream_with_combined_args(name: QueueName, args: StreamQueueArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel().await;
        let mut table = FieldTable::default();
        args.apply(&mut table);
        let ok = channel
            .queue_declare(&name.0, stream_queue_opts(), table)
            .await
            .is_ok();
        let _ = channel
            .queue_delete(&name.0, QueueDeleteOptions::default())
            .await;
        ok
    })
}

macro_rules! stream_rejection_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, arg: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel().await;
                let mut table = FieldTable::default();
                arg.insert_into(&mut table);
                declare_stream_rejects(&channel, &name.0, table).await
            })
        }
    };
}

stream_rejection_test!(stream_rejects_dead_letter_exchange, DeadLetterExchange);
stream_rejection_test!(stream_rejects_dead_letter_routing_key, DeadLetterRoutingKey);
stream_rejection_test!(stream_rejects_expires, Expires);
stream_rejection_test!(stream_rejects_delivery_limit, DeliveryLimit);
stream_rejection_test!(stream_rejects_overflow, Overflow);
stream_rejection_test!(stream_rejects_single_active_consumer, SingleActiveConsumer);
stream_rejection_test!(stream_rejects_max_priority, MaxPriority);

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
