//! Queue declarations: AMQP `queue.declare` vs HTTP `PUT /queues/{vhost}/{name}`.

use crate::QueueName;
use crate::combined::{ClassicQueueArgs, PriorityQueueArgs, StreamQueueArgs};
use crate::http::client::{self, encode_segment};
use crate::http::diff::{amqp_outcome, equivalent};
use crate::http::harness::declare_parity;
use crate::http::json::to_json;
use crate::tests::{connect, test_vhost};
use lapin::{options::QueueDeclareOptions, types::FieldTable};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use serde_json::json;

/// One declaration of a queue: durability plus arguments of any queue type.
#[derive(Clone, Debug)]
pub struct QueueDecl {
    pub durable: bool,
    pub arguments: FieldTable,
}

impl Arbitrary for QueueDecl {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut arguments = FieldTable::default();
        match g.choose(&[0, 1, 2]).unwrap() {
            0 => ClassicQueueArgs::arbitrary(g).apply(&mut arguments),
            1 => PriorityQueueArgs::arbitrary(g).apply(&mut arguments),
            _ => StreamQueueArgs::arbitrary(g).apply(&mut arguments),
        }
        QueueDecl {
            durable: bool::arbitrary(g),
            arguments,
        }
    }
}

fn queue_path(vhost: &str, name: &str) -> String {
    format!("queues/{}/{}", encode_segment(vhost), encode_segment(name))
}

/// Declaring the same queue (once, or redeclared with other settings) over
/// AMQP and over HTTP gives equivalent outcomes and the same resulting queue.
#[quickcheck]
fn queue_declare_parity(name: QueueName, decls: Vec<QueueDecl>) -> TestResult {
    declare_parity(
        "queue_declare_parity",
        "queues",
        &name.0,
        &decls,
        async |ch, d: &QueueDecl| {
            let opts = QueueDeclareOptions {
                durable: d.durable,
                ..QueueDeclareOptions::default()
            };
            ch.queue_declare(&name.0, opts, d.arguments.clone()).await
        },
        |d| {
            Some(json!({
                "durable": d.durable,
                "auto_delete": false,
                "arguments": to_json(&d.arguments)?,
            }))
        },
    )
}

/// Known bug (`lavinmq-quirks.md` #9): HTTP answers 400 to a reserved
/// `amq.` prefix where AMQP answers 403 ACCESS_REFUSED.
#[quickcheck]
#[ignore]
fn reserved_prefix_parity(name: QueueName) -> bool {
    let name: String = format!("amq.{}", name.0).chars().take(255).collect();
    let amqp_vhost = "reserved_prefix_parity-amqp";
    let http_vhost = test_vhost("reserved_prefix_parity-http");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let amqp = rt.block_on(async {
        let conn = connect(amqp_vhost).await;
        let ch = conn.create_channel().await.unwrap();
        let r = ch
            .queue_declare(&name, QueueDeclareOptions::default(), FieldTable::default())
            .await;
        let _ = conn.close(200, "bye").await;
        amqp_outcome(r)
    });
    let http = client::put(&queue_path(&http_vhost, &name), &json!({}));
    equivalent(amqp, http)
}
