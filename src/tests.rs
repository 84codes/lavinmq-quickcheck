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
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Vhosts already recreated in this test run.
static FRESH_VHOSTS: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Returns the vhost `qc-<test>` that isolates one test function from the
/// others running in parallel. Its first use in a run deletes and
/// recreates it, so leftovers from earlier runs are gone. The cases within
/// one test function run sequentially and share it.
fn test_vhost(test: &str) -> String {
    let vhost = format!("qc-{test}");
    let mut fresh = FRESH_VHOSTS.lock().unwrap();
    if fresh.get_or_insert_with(HashSet::new).insert(vhost.clone()) {
        let _ = ureq::delete(format!("{MGMT}/vhosts/{vhost}"))
            .header("Authorization", MGMT_AUTH)
            .call();
        ureq::put(format!("{MGMT}/vhosts/{vhost}"))
            .header("Authorization", MGMT_AUTH)
            .send_empty()
            .expect("Failed to create test vhost");
    }
    vhost
}

async fn connect(test: &str) -> Connection {
    let uri = format!("amqp://localhost:5672/{}", test_vhost(test));
    Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("Failed to connect to LavinMQ")
}

async fn connect_channel(test: &str) -> lapin::Channel {
    let conn = connect(test).await;
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
        let channel = connect_channel("round_trip").await;

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
        let channel = connect_channel("direct_exchange_round_trip").await;

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
        let channel = connect_channel("topic_exchange_round_trip").await;

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
                let channel = connect_channel(stringify!($fn_name)).await;
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
        let channel = connect_channel("declare_with_dead_letter_routing_key").await;
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
                let channel = connect_channel(stringify!($fn_name)).await;
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
        let channel = connect_channel("declare_classic_with_combined_args").await;
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
        let channel = connect_channel("declare_priority_with_combined_args").await;
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
        let channel = connect_channel("declare_stream_with_combined_args").await;
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
                let channel = connect_channel(stringify!($fn_name)).await;
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

async fn declare_fanout(channel: &lapin::Channel, name: &str, args: FieldTable) {
    channel
        .exchange_declare(
            name,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            args,
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
        let channel = connect_channel("routing_graph_delivers_expected").await;

        // 1. Declare exchanges.
        for ex in &topo.exchanges {
            let mut args = FieldTable::default();
            if let Some(ae) = ex.ae {
                args.insert(
                    ShortString::from(ae.spelling.key()),
                    AMQPValue::LongString(topo.exchanges[ae.target].name.clone().into()),
                );
            }
            declare_fanout(&channel, &ex.name, args).await;
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

// ---------------------------------------------------------------------------
// BasicProperties publish-only tests
// ---------------------------------------------------------------------------

use crate::properties::{
    AppId, BasicPropertiesArgs, ClusterId, ContentEncoding, ContentType, CorrelationId,
    DeliveryMode, Expiration, Headers, MessageId, MessageKind, MessageTimestamp, Priority, ReplyTo,
};

async fn publish_with_props(channel: &lapin::Channel, name: &str, props: BasicProperties) -> bool {
    let declared = channel
        .queue_declare(name, classic_queue_opts(), FieldTable::default())
        .await
        .is_ok();
    if !declared {
        return false;
    }
    let res = channel
        .basic_publish("", name, BasicPublishOptions::default(), b"", props)
        .await;
    let ok = match res {
        Ok(confirm) => confirm.await.is_ok(),
        Err(_) => false,
    };
    let _ = channel
        .queue_delete(name, QueueDeleteOptions::default())
        .await;
    ok
}

macro_rules! single_prop_publish_test {
    ($fn_name:ident, $ty:ty) => {
        #[quickcheck]
        fn $fn_name(name: QueueName, prop: $ty) -> bool {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let channel = connect_channel(stringify!($fn_name)).await;
                let props = prop.apply_to(BasicProperties::default());
                publish_with_props(&channel, &name.0, props).await
            })
        }
    };
}

single_prop_publish_test!(publish_with_content_type, ContentType);
single_prop_publish_test!(publish_with_content_encoding, ContentEncoding);
single_prop_publish_test!(publish_with_correlation_id, CorrelationId);
single_prop_publish_test!(publish_with_reply_to, ReplyTo);
single_prop_publish_test!(publish_with_message_id, MessageId);
single_prop_publish_test!(publish_with_kind, MessageKind);
single_prop_publish_test!(publish_with_app_id, AppId);
single_prop_publish_test!(publish_with_cluster_id, ClusterId);
single_prop_publish_test!(publish_with_delivery_mode, DeliveryMode);
single_prop_publish_test!(publish_with_priority, Priority);
single_prop_publish_test!(publish_with_timestamp, MessageTimestamp);
single_prop_publish_test!(publish_with_expiration, Expiration);
single_prop_publish_test!(publish_with_headers, Headers);

#[quickcheck]
fn publish_with_combined_properties(name: QueueName, args: BasicPropertiesArgs) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("publish_with_combined_properties").await;
        publish_with_props(&channel, &name.0, args.apply()).await
    })
}

// ---------------------------------------------------------------------------
// Consistent-hash exchange tests
// ---------------------------------------------------------------------------

use crate::consistent_hash::{BindOp, ConsistentHashScenario, HashAlgorithm};
use lapin::options::{BasicGetOptions, ConfirmSelectOptions};

async fn setup_consistent_hash(channel: &lapin::Channel, s: &ConsistentHashScenario) {
    let mut args = FieldTable::default();
    s.algorithm.insert_into(&mut args);
    s.hash_on.insert_into(&mut args);
    channel
        .exchange_declare(
            &s.exchange,
            ExchangeKind::Custom("x-consistent-hash".into()),
            ExchangeDeclareOptions::default(),
            args,
        )
        .await
        .expect("Failed to declare consistent-hash exchange");
    for q in &s.queues {
        declare_queue_for_routing(channel, q, FieldTable::default()).await;
    }
    for op in &s.ops {
        let queue = &s.queues[op.queue()];
        let rk = op.weight().routing_key();
        match op {
            BindOp::Bind { .. } => channel
                .queue_bind(
                    queue,
                    &s.exchange,
                    &rk,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .expect("Failed to bind"),
            BindOp::Unbind { .. } => channel
                .queue_unbind(queue, &s.exchange, &rk, FieldTable::default())
                .await
                .expect("Failed to unbind"),
        }
    }
}

/// Drains every queue, returning for each (key, copy) the queue indices
/// that received it.
async fn drain_hits(channel: &lapin::Channel, s: &ConsistentHashScenario) -> Vec<[Vec<usize>; 2]> {
    let mut hits = vec![[Vec::new(), Vec::new()]; s.keys.len()];
    for (qi, q) in s.queues.iter().enumerate() {
        while let Some(msg) = channel
            .basic_get(q, BasicGetOptions { no_ack: true })
            .await
            .expect("Failed to basic_get")
        {
            let tag = String::from_utf8(msg.delivery.data).unwrap();
            let (key, copy) = tag.split_once(':').unwrap();
            hits[key.parse::<usize>().unwrap()][copy.parse::<usize>().unwrap()].push(qi);
        }
    }
    hits
}

async fn cleanup_consistent_hash(channel: &lapin::Channel, s: &ConsistentHashScenario) {
    for q in &s.queues {
        let _ = channel.queue_delete(q, QueueDeleteOptions::default()).await;
    }
    let _ = channel
        .exchange_delete(&s.exchange, ExchangeDeleteOptions::default())
        .await;
}

#[quickcheck]
fn consistent_hash_is_deterministic(s: ConsistentHashScenario) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("consistent_hash_is_deterministic").await;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("Failed to enable confirms");
        setup_consistent_hash(&channel, &s).await;

        // Publish every key twice; confirms mean routing is done before drain.
        for (i, key) in s.keys.iter().enumerate() {
            let mut props = BasicProperties::default();
            if let (Some(h), Some(v)) = (&s.hash_on.0, &key.header) {
                let mut headers = FieldTable::default();
                headers.insert(
                    ShortString::from(h.as_str()),
                    AMQPValue::LongString(v.clone().into()),
                );
                props = props.with_headers(headers);
            }
            for copy in 0..2 {
                channel
                    .basic_publish(
                        &s.exchange,
                        &key.routing_key,
                        BasicPublishOptions::default(),
                        format!("{i}:{copy}").as_bytes(),
                        props.clone(),
                    )
                    .await
                    .expect("Failed to publish")
                    .await
                    .expect("Failed to confirm publish");
            }
        }

        let hits = drain_hits(&channel, &s).await;
        cleanup_consistent_hash(&channel, &s).await;
        hits.iter().all(|[a, b]| a == b)
    })
}

// ---------------------------------------------------------------------------
// Stream consumer x-stream-offset tests
// ---------------------------------------------------------------------------

use crate::stream_offset::StreamOffsetScenario;
use lapin::options::BasicQosOptions;

const SENTINEL: &[u8] = b"sentinel";

async fn publish_confirmed(channel: &lapin::Channel, queue: &str, payload: &[u8]) {
    channel
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await
        .expect("Failed to publish")
        .await
        .expect("Failed to confirm publish");
}

/// Consumes (and acks) until the sentinel, returning the indices before it.
async fn consume_until_sentinel(consumer: &mut lapin::Consumer) -> Vec<usize> {
    let mut seen = Vec::new();
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.expect("Failed to receive delivery");
        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("Failed to ack");
        if delivery.data == SENTINEL {
            break;
        }
        seen.push(String::from_utf8(delivery.data).unwrap().parse().unwrap());
    }
    seen
}

#[quickcheck]
fn stream_offset_delivers_expected(s: StreamOffsetScenario) -> bool {
    let expected: Vec<usize> = s.offset.expected(s.messages).collect();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("stream_offset_delivers_expected").await;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("Failed to enable confirms");
        channel
            .basic_qos(100, BasicQosOptions::default())
            .await
            .expect("Failed to set prefetch");

        let mut args = FieldTable::default();
        args.insert(
            ShortString::from("x-queue-type"),
            AMQPValue::LongString("stream".into()),
        );
        channel
            .queue_declare(&s.stream, stream_queue_opts(), args)
            .await
            .expect("Failed to declare stream");

        for i in 0..s.messages {
            publish_confirmed(&channel, &s.stream, i.to_string().as_bytes()).await;
        }

        let mut consume_args = FieldTable::default();
        consume_args.insert(ShortString::from("x-stream-offset"), s.offset.to_amqp());
        let mut consumer = channel
            .basic_consume(
                &s.stream,
                &format!("{}-consumer", s.stream),
                BasicConsumeOptions::default(),
                consume_args,
            )
            .await
            .expect("Failed to consume");

        // The consumer's start offset is resolved before consume-ok, so the
        // sentinel always lands after it.
        publish_confirmed(&channel, &s.stream, SENTINEL).await;

        let seen = tokio::time::timeout(
            Duration::from_secs(5),
            consume_until_sentinel(&mut consumer),
        )
        .await
        .expect("Timed out waiting for sentinel");

        let _ = channel
            .queue_delete(&s.stream, QueueDeleteOptions::default())
            .await;
        seen == expected
    })
}

// ---------------------------------------------------------------------------
// Delayed-message exchange tests
// ---------------------------------------------------------------------------

use crate::delayed::{DelayedScenario, LongExchangeName, declare_args};

#[quickcheck]
fn odd_x_delay_delivers_immediately(s: DelayedScenario) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("odd_x_delay_delivers_immediately").await;
        let (kind, args) = declare_args(s.style, s.ty);
        channel
            .exchange_declare(&s.exchange, kind, ExchangeDeclareOptions::default(), args)
            .await
            .expect("Failed to declare delayed exchange");
        declare_queue_for_routing(&channel, &s.queue, FieldTable::default()).await;
        channel
            .queue_bind(
                &s.queue,
                &s.exchange,
                &s.routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to bind");

        let mut headers = FieldTable::default();
        headers.insert(ShortString::from("x-delay"), s.delay.0.clone());
        channel
            .basic_publish(
                &s.exchange,
                &s.routing_key,
                BasicPublishOptions::default(),
                b"delayed",
                BasicProperties::default().with_headers(headers),
            )
            .await
            .expect("Failed to publish");

        let mut consumer = channel
            .basic_consume(
                &s.queue,
                "delayed-consumer",
                BasicConsumeOptions {
                    no_ack: true,
                    ..BasicConsumeOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("Failed to consume");
        let arrived = tokio::time::timeout(Duration::from_secs(2), consumer.next())
            .await
            .is_ok_and(|d| d.is_some_and(|d| d.is_ok_and(|d| d.data == b"delayed")));

        let _ = channel
            .queue_delete(&s.queue, QueueDeleteOptions::default())
            .await;
        let _ = channel
            .exchange_delete(&s.exchange, ExchangeDeleteOptions::default())
            .await;
        arrived
    })
}

/// Desired: a delayed exchange whose internal queue name would exceed
/// LavinMQ's 256-byte cap is refused with a channel-level error, and the
/// connection stays usable. Ignored: LavinMQ currently aborts the whole
/// connection instead (see `lavinmq-quirks.md` #5). Run with `--ignored`.
#[quickcheck]
#[ignore = "LavinMQ aborts the connection; cloudamqp/lavinmq#2297"]
fn delayed_exchange_long_name_is_clean_error(name: LongExchangeName) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let conn = connect("delayed_exchange_long_name_is_clean_error").await;
        let channel = conn
            .create_channel()
            .await
            .expect("Failed to create channel");
        let (kind, args) = declare_args(
            crate::delayed::DeclareStyle::DelayedFlag,
            crate::delayed::DelayedType::Direct,
        );
        let declared = channel
            .exchange_declare(&name.0, kind, ExchangeDeclareOptions::default(), args)
            .await;
        if declared.is_ok() {
            let _ = channel
                .exchange_delete(&name.0, ExchangeDeleteOptions::default())
                .await;
            return false;
        }
        let Ok(probe) = conn.create_channel().await else {
            return false;
        };
        probe
            .exchange_declare(
                "amq.direct",
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    passive: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .is_ok()
    })
}

// ---------------------------------------------------------------------------
// Alternate-exchange edge cases
// ---------------------------------------------------------------------------

use crate::alternate::{AeCycle, MissingAe};
use lapin::publisher_confirm::Confirmation;

/// Publishes a mandatory message (confirms on) and returns the reply code
/// of the basic.return, or `None` if the message was routed.
async fn publish_mandatory(channel: &lapin::Channel, exchange: &str) -> Option<u16> {
    let confirm = channel
        .basic_publish(
            exchange,
            "",
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            b"probe",
            BasicProperties::default(),
        )
        .await
        .expect("Failed to publish")
        .await
        .expect("Failed to confirm publish");
    match confirm {
        Confirmation::Ack(Some(ret)) | Confirmation::Nack(Some(ret)) => Some(ret.reply_code),
        _ => None,
    }
}

async fn declare_with_ae(channel: &lapin::Channel, name: &str, ae: &str, key: &str) {
    let mut args = FieldTable::default();
    args.insert(ShortString::from(key), AMQPValue::LongString(ae.into()));
    declare_fanout(channel, name, args).await;
}

#[quickcheck]
fn ae_cycle_terminates_and_returns(c: AeCycle) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("ae_cycle_terminates_and_returns").await;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("Failed to enable confirms");
        let n = c.exchanges.len();
        for (i, (name, spelling)) in c.exchanges.iter().enumerate() {
            let next = &c.exchanges[(i + 1) % n].0;
            declare_with_ae(&channel, name, next, spelling.key()).await;
        }
        let code = tokio::time::timeout(
            Duration::from_secs(2),
            publish_mandatory(&channel, &c.exchanges[0].0),
        )
        .await;
        for (name, _) in &c.exchanges {
            let _ = channel
                .exchange_delete(name, ExchangeDeleteOptions::default())
                .await;
        }
        matches!(code, Ok(Some(312)))
    })
}

#[quickcheck]
fn missing_ae_returns_mandatory(m: MissingAe) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("missing_ae_returns_mandatory").await;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("Failed to enable confirms");
        declare_with_ae(&channel, &m.exchange, &m.missing, m.spelling.key()).await;
        let code = publish_mandatory(&channel, &m.exchange).await;
        let _ = channel
            .exchange_delete(&m.exchange, ExchangeDeleteOptions::default())
            .await;
        code == Some(312)
    })
}

use crate::alternate::PolicyAe;

const MGMT: &str = "http://localhost:15672/api";
/// `guest:guest`, base64.
const MGMT_AUTH: &str = "Basic Z3Vlc3Q6Z3Vlc3Q=";

fn put_ae_policy(vhost: &str, name: &str, exchange: &str, ae: &str) {
    let body = format!(
        r#"{{"pattern":"^{exchange}$","apply-to":"exchanges","priority":10,"definition":{{"alternate-exchange":"{ae}"}}}}"#
    );
    ureq::put(format!("{MGMT}/policies/{vhost}/{name}"))
        .header("Authorization", MGMT_AUTH)
        .header("Content-Type", "application/json")
        .send(body)
        .expect("Failed to create policy");
}

fn delete_policy(vhost: &str, name: &str) {
    let _ = ureq::delete(format!("{MGMT}/policies/{vhost}/{name}"))
        .header("Authorization", MGMT_AUTH)
        .call();
}

/// Polls until LavinMQ reports `policy` as applied to `exchange`.
async fn wait_for_policy(vhost: &str, exchange: &str, policy: &str) -> bool {
    let needle = format!(r#""policy":"{policy}""#);
    for _ in 0..200 {
        let applied = ureq::get(format!("{MGMT}/exchanges/{vhost}/{exchange}"))
            .header("Authorization", MGMT_AUTH)
            .call()
            .ok()
            .and_then(|mut r| r.body_mut().read_to_string().ok())
            .is_some_and(|b| b.contains(&needle));
        if applied {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn queue_len(channel: &lapin::Channel, queue: &str) -> usize {
    let mut n = 0;
    while channel
        .basic_get(queue, BasicGetOptions { no_ack: true })
        .await
        .expect("Failed to basic_get")
        .is_some()
    {
        n += 1;
    }
    n
}

#[quickcheck]
fn ae_argument_beats_policy(p: PolicyAe) -> bool {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel("ae_argument_beats_policy").await;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("Failed to enable confirms");
        for (ex, q) in [(&p.policy_ae, &p.policy_queue), (&p.arg_ae, &p.arg_queue)] {
            declare_fanout(&channel, ex, FieldTable::default()).await;
            declare_queue_for_routing(&channel, q, FieldTable::default()).await;
            channel
                .queue_bind(
                    q,
                    ex,
                    "",
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .expect("Failed to bind");
        }
        match p.arg_spelling {
            Some(sp) => declare_with_ae(&channel, &p.exchange, &p.arg_ae, sp.key()).await,
            None => declare_fanout(&channel, &p.exchange, FieldTable::default()).await,
        }
        let vhost = test_vhost("ae_argument_beats_policy");
        put_ae_policy(&vhost, &p.policy, &p.exchange, &p.policy_ae);
        let applied = wait_for_policy(&vhost, &p.exchange, &p.policy).await;

        publish_probe(&channel, &p.exchange).await;
        let got = (
            queue_len(&channel, &p.policy_queue).await,
            queue_len(&channel, &p.arg_queue).await,
        );

        delete_policy(&vhost, &p.policy);
        for q in [&p.policy_queue, &p.arg_queue] {
            let _ = channel.queue_delete(q, QueueDeleteOptions::default()).await;
        }
        for ex in [&p.exchange, &p.policy_ae, &p.arg_ae] {
            let _ = channel
                .exchange_delete(ex, ExchangeDeleteOptions::default())
                .await;
        }
        let expected = if p.arg_spelling.is_some() {
            (0, 1)
        } else {
            (1, 0)
        };
        applied && got == expected
    })
}

/// Desired: re-applying policies leaves a consistent-hash exchange's
/// routing intact. Ignored: with `x-algorithm` set, LavinMQ replaces the
/// hasher and routing breaks (see `lavinmq-quirks.md` #7).
#[quickcheck]
#[ignore = "consistent-hash loses routing on policy change; cloudamqp/lavinmq#2300"]
fn consistent_hash_survives_policy_change(algorithm: HashAlgorithm) -> bool {
    let test = "consistent_hash_survives_policy_change";
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = connect_channel(test).await;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("Failed to enable confirms");
        let mut args = FieldTable::default();
        algorithm.insert_into(&mut args);
        channel
            .exchange_declare(
                "qc_ch_policy",
                ExchangeKind::Custom("x-consistent-hash".into()),
                ExchangeDeclareOptions::default(),
                args,
            )
            .await
            .expect("Failed to declare consistent-hash exchange");
        declare_queue_for_routing(&channel, "qc_ch_policy_q", FieldTable::default()).await;
        channel
            .queue_bind(
                "qc_ch_policy_q",
                "qc_ch_policy",
                "10",
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("Failed to bind");

        let before = publish_mandatory(&channel, "qc_ch_policy").await;
        let vhost = test_vhost(test);
        put_ae_policy(&vhost, "qc_unrelated", "qc_matches_nothing", "x");
        // Policy application is asynchronous; there's no resource to poll.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let after = publish_mandatory(&channel, "qc_ch_policy").await;

        delete_policy(&vhost, "qc_unrelated");
        let _ = channel
            .queue_delete("qc_ch_policy_q", QueueDeleteOptions::default())
            .await;
        let _ = channel
            .exchange_delete("qc_ch_policy", ExchangeDeleteOptions::default())
            .await;
        before.is_none() && after.is_none()
    })
}
