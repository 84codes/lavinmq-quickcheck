use quickcheck::{Arbitrary, Gen};

const VALID_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:";
const TOPIC_WORD_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
const RESERVED_PREFIX: &str = "amq.";

#[derive(Clone, Debug)]
pub struct QueueName(pub String);

impl Arbitrary for QueueName {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();

        loop {
            let name: String = (0..len)
                .map(|_| {
                    let &byte = g.choose(VALID_CHARS).unwrap();
                    byte as char
                })
                .collect();

            if !name.starts_with(RESERVED_PREFIX) {
                return QueueName(name);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoutingKey(pub String);

impl Arbitrary for RoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_len = g.size().clamp(1, 255);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();

        let key: String = (0..len)
            .map(|_| {
                let &byte = g.choose(VALID_CHARS).unwrap();
                byte as char
            })
            .collect();

        RoutingKey(key)
    }
}

#[derive(Clone, Debug)]
pub struct TopicRoutingKey {
    pub routing_key: String,
    pub binding_pattern: String,
}

impl TopicRoutingKey {
    fn gen_word(g: &mut Gen) -> String {
        let max_len = g.size().clamp(1, 20);
        let len = *g.choose(&(1..=max_len).collect::<Vec<_>>()).unwrap();
        (0..len)
            .map(|_| {
                let &byte = g.choose(TOPIC_WORD_CHARS).unwrap();
                byte as char
            })
            .collect()
    }
}

impl Arbitrary for TopicRoutingKey {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_words = g.size().clamp(1, 10);
        let num_words = *g.choose(&(1..=max_words).collect::<Vec<_>>()).unwrap();

        let words: Vec<String> = (0..num_words).map(|_| Self::gen_word(g)).collect();
        let routing_key = words.join(".");

        let mut binding_parts: Vec<String> = Vec::new();
        let mut i = 0;
        while i < words.len() {
            let choice = *g.choose(&[0u8, 1, 2, 3]).unwrap();
            match choice {
                0 => {
                    binding_parts.push(words[i].clone());
                    i += 1;
                }
                1 => {
                    binding_parts.push("*".to_string());
                    i += 1;
                }
                2 => {
                    let remaining = words.len() - i;
                    let skip = *g.choose(&(1..=remaining).collect::<Vec<_>>()).unwrap();
                    binding_parts.push("#".to_string());
                    i += skip;
                }
                _ => {
                    binding_parts.push(words[i].clone());
                    i += 1;
                }
            }
        }

        let binding_pattern = binding_parts.join(".");

        TopicRoutingKey {
            routing_key,
            binding_pattern,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{QueueName, RoutingKey, TopicRoutingKey};
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

    #[quickcheck]
    fn round_trip(name: QueueName, payload: Vec<u8>) -> bool {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let conn =
                Connection::connect("amqp://localhost:5672", ConnectionProperties::default())
                    .await
                    .expect("Failed to connect to RabbitMQ");

            let channel = conn
                .create_channel()
                .await
                .expect("Failed to create channel");

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
            let conn =
                Connection::connect("amqp://localhost:5672", ConnectionProperties::default())
                    .await
                    .expect("Failed to connect to RabbitMQ");

            let channel = conn
                .create_channel()
                .await
                .expect("Failed to create channel");

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
            let conn =
                Connection::connect("amqp://localhost:5672", ConnectionProperties::default())
                    .await
                    .expect("Failed to connect to RabbitMQ");

            let channel = conn
                .create_channel()
                .await
                .expect("Failed to create channel");

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
}
