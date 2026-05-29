//! Acceptance test for PRD-agorabus-multi-prefix-subscribe (AC1).
//!
//! Project: agorabus (cli)
//! AC1 description: a client that calls `subscribe()` twice with disjoint
//! prefixes must receive events matching EITHER prefix. Pre-0.4 the daemon
//! held a single `Option<String>` slot, so the second Subscribe silently
//! overwrote the first and only the last prefix's events were delivered.
//!
//! Also guards backward-compat (a single subscribe still works) and the
//! match-all empty-prefix case.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::doc_markdown, clippy::indexing_slicing, clippy::cast_lossless, clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::missing_panics_doc, clippy::many_single_char_names, clippy::as_conversions, clippy::panic, clippy::needless_pass_by_value, clippy::similar_names, clippy::tests_outside_test_module, clippy::needless_borrow)]

mod common;

use agorabus::Client;
use common::DaemonHandle;
use std::collections::BTreeSet;
use std::time::Duration;

/// Read `n` events off `sub`, each bounded by a timeout so a regression
/// (dropped event) fails fast instead of hanging the test. Returns the set
/// of topics seen.
async fn collect_topics(sub: &mut Client, n: usize) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    for _ in 0..n {
        let ev = tokio::time::timeout(Duration::from_secs(5), sub.next_event())
            .await
            .expect("timed out waiting for a subscribed event")
            .expect("next_event io")
            .expect("stream closed before event arrived");
        seen.insert(ev.topic);
    }
    seen
}

#[test]
fn acceptance_multi_prefix() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        // --- AC1: two disjoint prefixes, both must be live. ---
        let mut sub = Client::connect(&h.socket).await.unwrap();
        sub.announce("multi-sub", 1, "/tmp/sub", "subscribing").await.unwrap();
        sub.subscribe("wm.stt.").await.unwrap();
        sub.subscribe("wm.audio.").await.unwrap();

        let mut pubr = Client::connect(&h.socket).await.unwrap();
        pubr.announce("multi-pub", 2, "/tmp/pub", "publishing").await.unwrap();
        // One event per subscribed prefix, plus one that matches NEITHER
        // (must be filtered out, so we only ever expect 2 to arrive).
        pubr.publish("wm.stt.final", serde_json::json!({"n": 1})).await.unwrap();
        pubr.publish("wm.audio.reload", serde_json::json!({"n": 2})).await.unwrap();
        pubr.publish("wm.brain.thought", serde_json::json!({"n": 3})).await.unwrap();

        let seen = collect_topics(&mut sub, 2).await;
        assert!(
            seen.contains("wm.stt.final"),
            "first prefix lost (last-subscribe-wins regression): {seen:?}"
        );
        assert!(
            seen.contains("wm.audio.reload"),
            "second prefix not delivered: {seen:?}"
        );
        assert!(
            !seen.contains("wm.brain.thought"),
            "unsubscribed topic leaked through: {seen:?}"
        );

        h.shutdown().await;
    });
}

#[test]
fn single_prefix_unchanged() {
    // AC5 spirit: a single subscribe() behaves exactly as before.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        let mut sub = Client::connect(&h.socket).await.unwrap();
        sub.announce("single-sub", 1, "/tmp/sub", "subscribing").await.unwrap();
        sub.subscribe("wm.stt.").await.unwrap();

        let mut pubr = Client::connect(&h.socket).await.unwrap();
        pubr.announce("single-pub", 2, "/tmp/pub", "publishing").await.unwrap();
        pubr.publish("wm.audio.reload", serde_json::json!({"n": 1})).await.unwrap();
        pubr.publish("wm.stt.final", serde_json::json!({"n": 2})).await.unwrap();

        // Only the matching topic should arrive; reading one event yields it.
        let seen = collect_topics(&mut sub, 1).await;
        assert!(seen.contains("wm.stt.final"), "single-prefix match failed: {seen:?}");
        assert!(!seen.contains("wm.audio.reload"), "non-matching topic leaked: {seen:?}");

        h.shutdown().await;
    });
}

#[test]
fn empty_prefix_matches_all() {
    // Backward-compat: subscribe("") preserves the pre-0.4 "match all" path.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        let mut sub = Client::connect(&h.socket).await.unwrap();
        sub.announce("all-sub", 1, "/tmp/sub", "subscribing").await.unwrap();
        sub.subscribe("").await.unwrap();

        let mut pubr = Client::connect(&h.socket).await.unwrap();
        pubr.announce("all-pub", 2, "/tmp/pub", "publishing").await.unwrap();
        pubr.publish("anything.at.all", serde_json::json!({"n": 1})).await.unwrap();

        let seen = collect_topics(&mut sub, 1).await;
        assert!(seen.contains("anything.at.all"), "empty prefix should match all: {seen:?}");

        h.shutdown().await;
    });
}
