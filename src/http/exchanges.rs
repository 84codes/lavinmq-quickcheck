//! Exchange declarations: AMQP `exchange.declare` vs HTTP
//! `PUT /exchanges/{vhost}/{name}`.

use crate::QueueName;
use crate::consistent_hash::{HashAlgorithm, HashOn};
use crate::delayed::{DeclareStyle, DelayedType, declare_args};
use crate::headers::{InvalidXMatch, MatchMode};
use crate::http::harness::{MAX_DECLS, declare_parity};
use crate::http::json::to_json;
use lapin::{
    ExchangeKind,
    options::ExchangeDeclareOptions,
    types::{AMQPValue, FieldTable, ShortString},
};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use serde_json::json;

/// One declaration of an exchange.
#[derive(Clone, Debug)]
pub struct ExchangeDecl {
    pub kind: String,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: FieldTable,
}

pub fn kind_name(kind: ExchangeKind) -> String {
    match kind {
        ExchangeKind::Custom(s) => s,
        ExchangeKind::Direct => "direct".into(),
        ExchangeKind::Fanout => "fanout".into(),
        ExchangeKind::Headers => "headers".into(),
        ExchangeKind::Topic => "topic".into(),
    }
}

impl Arbitrary for ExchangeDecl {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut arguments = FieldTable::default();
        let kind = match g.choose(&[0, 1, 2, 3, 4]).unwrap() {
            0 => g
                .choose(&["direct", "fanout", "topic"])
                .unwrap()
                .to_string(),
            1 => {
                let x_match = match g.choose(&[0, 1, 2]).unwrap() {
                    0 => None,
                    1 => Some(AMQPValue::LongString(
                        g.choose(&[MatchMode::All, MatchMode::Any])
                            .unwrap()
                            .as_str()
                            .into(),
                    )),
                    _ => Some(InvalidXMatch::arbitrary(g).0),
                };
                if let Some(v) = x_match {
                    arguments.insert(ShortString::from("x-match"), v);
                }
                "headers".into()
            }
            2 => {
                HashAlgorithm::arbitrary(g).insert_into(&mut arguments);
                HashOn::arbitrary(g).insert_into(&mut arguments);
                "x-consistent-hash".into()
            }
            3 => {
                let (kind, args) =
                    declare_args(DeclareStyle::arbitrary(g), DelayedType::arbitrary(g));
                arguments = args;
                kind_name(kind)
            }
            _ => "x-bogus".into(),
        };
        if bool::arbitrary(g) {
            arguments.insert(
                ShortString::from("alternate-exchange"),
                AMQPValue::LongString(QueueName::arbitrary(g).0.into()),
            );
        }
        ExchangeDecl {
            kind,
            durable: bool::arbitrary(g),
            auto_delete: bool::arbitrary(g),
            internal: bool::arbitrary(g),
            arguments,
        }
    }
}

async fn declare(ch: &lapin::Channel, name: &str, d: &ExchangeDecl) -> lapin::Result<()> {
    let opts = ExchangeDeclareOptions {
        durable: d.durable,
        auto_delete: d.auto_delete,
        internal: d.internal,
        ..ExchangeDeclareOptions::default()
    };
    ch.exchange_declare(
        name,
        ExchangeKind::Custom(d.kind.clone()),
        opts,
        d.arguments.clone(),
    )
    .await
}

fn body(d: &ExchangeDecl) -> Option<serde_json::Value> {
    Some(json!({
        "type": d.kind,
        "durable": d.durable,
        "auto_delete": d.auto_delete,
        "internal": d.internal,
        "arguments": to_json(&d.arguments)?,
    }))
}

/// Declaring the same exchange (once, or redeclared with other settings)
/// over AMQP and over HTTP gives equivalent outcomes and the same exchange.
/// Redeclaring an internal exchange is left to `internal_exchange_redeclare`
/// (quirk #23).
#[quickcheck]
fn exchange_declare_parity(name: QueueName, decls: Vec<ExchangeDecl>) -> TestResult {
    if decls.iter().take(MAX_DECLS).filter(|d| d.internal).count() > 1 {
        return TestResult::discard();
    }
    declare_parity(
        "exchange_declare_parity",
        "exchanges",
        &name.0,
        &decls,
        async |ch, d| declare(ch, &name.0, d).await,
        body,
    )
}

/// Known bug (`lavinmq-quirks.md` #23): re-PUTting an existing internal
/// exchange, with identical settings, answers 400 "Not allowed to publish
/// to internal exchange". AMQP redeclares it fine.
#[quickcheck]
#[ignore]
fn internal_exchange_redeclare(name: QueueName, decl: ExchangeDecl) -> TestResult {
    let decl = ExchangeDecl {
        internal: true,
        ..decl
    };
    declare_parity(
        "internal_exchange_redeclare",
        "exchanges",
        &name.0,
        &[decl.clone(), decl],
        async |ch, d| declare(ch, &name.0, d).await,
        body,
    )
}
