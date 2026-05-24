//! Regression test: a one-shot client that reuses an existing long-lived
//! peer's session_id must not wipe the long-lived peer's record on
//! disconnect. The daemon tracks owner-connection per session_id; a guest
//! announce that hits an already-owned session_id is allowed (so publish
//! and heartbeat over that connection still work) but does NOT take
//! ownership and therefore does not remove the record on close.
//!
//! Originally observed when a publish CLI was invoked with
//! `--session-id <existing subscriber>`; the publish's announce overwrote
//! the subscriber's peer record and the subsequent disconnect deleted it,
//! leaving the long-lived subscriber's connection alive but orphaned in
//! `peers` query results.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use agorabus::Client;
use common::DaemonHandle;
use std::time::Duration;

#[test]
fn one_shot_guest_does_not_evict_long_lived_owner() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        // Long-lived subscriber claims session id "shared-sid".
        let mut sub = Client::connect(&h.socket).await.unwrap();
        sub.announce("shared-sid", 1, "/tmp/sub", "subscriber")
            .await
            .unwrap();
        let r = sub.subscribe("").await.unwrap();
        assert!(r.ok);

        // Sanity: peers shows the subscriber.
        let mut probe = Client::connect(&h.socket).await.unwrap();
        probe
            .announce("probe-1", 99, "/tmp", "probe")
            .await
            .unwrap();
        let ps = probe.peers().await.unwrap();
        drop(probe); // close so it doesn't pollute later snapshots
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            ps.iter().any(|p| p.session_id == "shared-sid"),
            "subscriber should be in peers before guest connects"
        );

        // Guest connection reuses the subscriber's session_id and then
        // disconnects (the publish CLI pattern).
        {
            let mut guest = Client::connect(&h.socket).await.unwrap();
            guest
                .announce("shared-sid", 2, "/tmp/guest", "publisher")
                .await
                .unwrap();
            let _ = guest
                .publish("hello", serde_json::json!({"v": 1}))
                .await
                .unwrap();
            // guest dropped here -> connection closes
        }

        // Settle.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The long-lived subscriber's peer record must still be present.
        let mut probe2 = Client::connect(&h.socket).await.unwrap();
        probe2
            .announce("probe-2", 100, "/tmp", "probe")
            .await
            .unwrap();
        let ps2 = probe2.peers().await.unwrap();
        let surviving = ps2.iter().find(|p| p.session_id == "shared-sid");
        assert!(
            surviving.is_some(),
            "guest disconnect must not evict the long-lived owner. peers: {ps2:?}"
        );
        // And the subscriber must still receive events (its connection
        // wasn't disturbed by the guest's announce-overwrite either).
        let ev = tokio::time::timeout(Duration::from_secs(2), sub.next_event())
            .await
            .expect("subscriber receives the guest's publish")
            .unwrap()
            .unwrap();
        assert_eq!(ev.topic, "hello");

        h.shutdown().await;
    });
}
