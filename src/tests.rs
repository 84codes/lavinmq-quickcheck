use crate::names::{QueueName, RoutingKey, TopicRoutingKey};
use futures_lite::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{
        BasicConsumeOptions, BasicPublishOptions, QueueBindOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::FieldTable,
};
use quickcheck_macros::quickcheck;

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
    CacheSize, CacheTtl, ConsumerTimeout, DeliveryLimit, DeadLetterExchange,
    DeadLetterRoutingKey, DeduplicationHeader, Expires, MaxLength, MaxLengthBytes,
    MaxPriority, MessageDeduplication, MessageTtl, Overflow, SingleActiveConsumer,
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
