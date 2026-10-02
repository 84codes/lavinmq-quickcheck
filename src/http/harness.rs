//! Runs a sequence of declarations over AMQP and over HTTP, side by side.

use crate::http::client::{self, encode_segment};
use crate::http::diff::{amqp_outcome, equivalent, normalize};
use crate::tests::{connect, test_vhost};
use quickcheck::TestResult;
use serde_json::Value;
use std::fmt::Debug;

/// Declarations per case: enough to cover redeclares.
pub const MAX_DECLS: usize = 3;

/// Declares `name` once per entry of `decls` over AMQP (vhost
/// `qc-<test>-amqp`) and over HTTP (`PUT /<resource>/qc-<test>-http/<name>`).
/// Passes if every pair of outcomes is equivalent and both resources GET
/// equal afterwards. Discards the case if a declaration has no JSON body.
pub fn declare_parity<D: Debug, T>(
    test: &str,
    resource: &str,
    name: &str,
    decls: &[D],
    amqp: impl AsyncFn(&lapin::Channel, &D) -> Result<T, lapin::Error>,
    body: impl Fn(&D) -> Option<Value>,
) -> TestResult {
    let decls = &decls[..decls.len().min(MAX_DECLS)];
    let Some(bodies) = decls.iter().map(body).collect::<Option<Vec<_>>>() else {
        return TestResult::discard();
    };
    if decls.is_empty() {
        return TestResult::discard();
    }
    let amqp_vhost = test_vhost(&format!("{test}-amqp"));
    let http_vhost = test_vhost(&format!("{test}-http"));
    let path = |vhost: &str| {
        format!(
            "{resource}/{}/{}",
            encode_segment(vhost),
            encode_segment(name)
        )
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let conn = rt.block_on(connect(&format!("{test}-amqp")));

    let mut ok = true;
    for (decl, body) in decls.iter().zip(&bodies) {
        let amqp_result = rt.block_on(async {
            let ch = conn.create_channel().await.unwrap();
            amqp_outcome(amqp(&ch, decl).await)
        });
        let http_result = client::put(&path(&http_vhost), body);
        if !equivalent(amqp_result, http_result) {
            eprintln!("outcomes differ: amqp={amqp_result:?} http={http_result:?} decl={decl:?}");
            ok = false;
        }
    }

    let amqp_state = client::get(&path(&amqp_vhost)).map(|r| normalize(&r));
    let http_state = client::get(&path(&http_vhost)).map(|r| normalize(&r));
    if amqp_state != http_state {
        eprintln!("state differs: amqp={amqp_state:?} http={http_state:?}");
        ok = false;
    }

    let _ = client::delete(&path(&amqp_vhost));
    let _ = client::delete(&path(&http_vhost));
    let _ = rt.block_on(conn.close(200, "bye"));
    TestResult::from_bool(ok)
}
