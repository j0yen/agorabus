//! Acceptance test for PRD-agorabus-client-reconnect AC1 + AC2.
//!
//! Project: agorabus (cli, rust-extend)
//! AC1 description: With a daemon running and a `subscribe` client attached,
//!   killing and relaunching the daemon results in the *same* client process
//!   re-appearing in `agorabus peers` (same session_id) within
//!   reconnect-cap-ms + bind_time, without the client process exiting.
//! AC2 description: A `publish` to the subscribed prefix issued *after* the
//!   daemon relaunch is delivered to the reconnected client and appended to
//!   its output sink.
//!
//! These two ACs are tested together because they share one fixture: a
//! long-lived reconnecting subscriber across a single daemon bounce.
//!
//! The reconnecting subscriber runs in-process via
//! `agorabus::reconnect_subscribe`, exercising the *same* connect → announce
//! → subscribe → stream loop that `agorabus subscribe` drives. Events are
//! collected through an mpsc channel which stands in for the ndjson output
//! sink the SessionStart hook redirects to.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use std::time::Duration;

use agorabus::{Client, ReconnectConfig, reconnect_subscribe};
use common::DaemonHandle;

/// Poll `peers` until a peer with `session_id` appears, or `deadline` elapses.
async fn wait_for_peer(socket: &std::path::Path, session_id: &str, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(mut c) = Client::connect(socket).await {
            // The probe connection itself must announce before it can query.
            let _ = c.announce("probe", std::process::id(), "/tmp", "probe").await;
            if let Ok(peers) = c.peers().await {
                if peers.iter().any(|p| p.session_id == session_id) {
                    return true;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[test]
fn acceptance_reconnect_survives_restart() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Generous heartbeat timeout so the subscriber is never pruned for
        // quietness during the test window.
        let mut h = DaemonHandle::start_with_timeout(Duration::from_secs(60)).await;
        let socket = h.socket.clone();
        let sid = "reconnect-survivor";
        let prefix = "shared.";

        // Channel stands in for the ndjson output sink.
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel();

        // Fast, bounded backoff so the test reconnects quickly.
        let cfg = ReconnectConfig {
            base_ms: 20,
            cap_ms: 200,
            max_attempts: 0, // unbounded — survive the bounce
        };

        // Spawn the long-lived reconnecting subscriber. It must NOT exit when
        // the daemon dies; it reconnects and keeps streaming.
        let sub_socket = socket.clone();
        let sub = tokio::spawn(async move {
            reconnect_subscribe(
                &sub_socket,
                sid,
                std::process::id(),
                "/tmp/reconnect-test",
                "reconnect-test",
                prefix,
                0,    // max_events = unbounded
                true, // reconnect = ON
                cfg,
                move |ev| {
                    // Forward every event to the sink; stop only if the sink
                    // is gone (receiver dropped → test finished).
                    ev_tx.send(ev).is_ok()
                },
            )
            .await
        });

        // --- Phase 1: subscriber attaches on first connect. ---
        assert!(
            wait_for_peer(&socket, sid, Duration::from_secs(5)).await,
            "subscriber should appear in peers before the bounce"
        );

        // --- Phase 2: bounce the daemon (kill + relaunch on the same path). ---
        h.stop_only().await;
        // Brief gap so the subscriber actually observes EOF and enters its
        // reconnect backoff before the daemon comes back.
        tokio::time::sleep(Duration::from_millis(100)).await;
        h.restart_with_timeout(Duration::from_secs(60)).await;

        // --- AC1: same session_id re-appears in peers after the bounce. ---
        // Window: cap_ms (200) + bind/probe slack. 5 s is generous.
        assert!(
            wait_for_peer(&socket, sid, Duration::from_secs(5)).await,
            "AC1: subscriber should re-appear in peers within cap+bind after restart"
        );

        // The subscriber task must still be alive (did NOT exit on the bounce).
        assert!(
            !sub.is_finished(),
            "AC1: subscriber process must not exit on daemon death"
        );

        // --- AC2: a publish AFTER the relaunch reaches the reconnected sub. ---
        // Publish via a fresh announced client.
        let mut pubc = Client::connect(&socket).await.expect("publisher connect");
        pubc.announce("publisher", std::process::id(), "/tmp", "pub")
            .await
            .expect("publisher announce");

        // The reconnected subscriber may not have finished its re-subscribe at
        // the instant peers showed it; retry the publish a few times until the
        // event is delivered to the sink.
        let topic = "shared.post_restart";
        let marker = serde_json::json!({"phase": "post_restart", "n": 42});

        let mut delivered = false;
        for _ in 0..50 {
            pubc.publish(topic, marker.clone())
                .await
                .expect("publish ok");
            // Drain any events that have arrived.
            let wait = tokio::time::timeout(Duration::from_millis(100), ev_rx.recv()).await;
            if let Ok(Some(ev)) = wait {
                if ev.topic == topic && ev.data == marker {
                    delivered = true;
                    break;
                }
            }
        }

        assert!(
            delivered,
            "AC2: post-restart publish must be delivered to the reconnected subscriber's sink"
        );

        // Tear down: drop the receiver so the subscriber's on_event returns
        // false on the next event (or abort the task), then shut the daemon.
        drop(ev_rx);
        sub.abort();
        let _ = sub.await;
        h.shutdown().await;
    });
}
