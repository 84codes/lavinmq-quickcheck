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
