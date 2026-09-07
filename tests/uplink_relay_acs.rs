//! Acceptance tests for PRD-agorabus-nats-uplink: the bidirectional relay
//! and its loop-prevention guarantee, exercised against a real throwaway
//! `nats-server` (AC12: skip, don't pass vacuously, when absent) and two
//! real agorabus daemons.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::similar_names
)]

mod common;

use agorabus::{Client, UplinkConfig, UplinkStatus};
use common::DaemonHandle;
use common::nats_harness::{NatsServer, nats_server_available, skip_no_nats_server};
use std::time::Duration;
use tokio::time::{Instant, timeout};

/// Spin up a throwaway nats-server plus two uplinked daemons (`testa` /
/// `testb`), and wait until both report `Connected` before returning.
async fn setup_pair() -> (NatsServer, DaemonHandle, DaemonHandle) {
    let nats = NatsServer::spawn().await;

    let cfg_a = UplinkConfig {
        enabled: true,
        url: nats.url(),
        node: "testa".to_string(),
        fleet_presence_interval_ms: 60_000,
    };
    let cfg_b = UplinkConfig {
        enabled: true,
        url: nats.url(),
        node: "testb".to_string(),
        fleet_presence_interval_ms: 60_000,
    };
    let daemon_a = DaemonHandle::start_with_uplink(cfg_a).await;
    let daemon_b = DaemonHandle::start_with_uplink(cfg_b).await;

    wait_connected(&daemon_a.socket, "testa-wait").await;
    wait_connected(&daemon_b.socket, "testb-wait").await;
    // Small settle margin: the subscribe command is enqueued before we ever
    // observe `Connected`, but give the server a moment to register it
    // before the test starts publishing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    (nats, daemon_a, daemon_b)
}

async fn wait_connected(socket: &std::path::Path, sid: &str) {
    let mut client = Client::connect(socket).await.unwrap();
    client.announce(sid, 1, "/tmp", "wait-connected").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = client.uplink_status().await.unwrap();
        if matches!(status, UplinkStatus::Connected { .. }) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("uplink never reported connected for {sid}: {status:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// AC6: a client publish on local topic `T` arrives on NATS subject
/// `wm.bus.T`, wrapped in an envelope carrying `origin_node` and the
/// original JSON payload.
#[test]
fn ac6_publish_wraps_envelope_on_nats_subject() {
    if !nats_server_available() {
        skip_no_nats_server("ac6_publish_wraps_envelope_on_nats_subject");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (nats, daemon_a, daemon_b) = setup_pair().await;

        // Raw NATS subscriber watching the mirrored subject directly.
        let raw = async_nats::connect(nats.url()).await.unwrap();
        let mut raw_sub = raw.subscribe("wm.bus.ac6.topic").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut publisher = Client::connect(&daemon_a.socket).await.unwrap();
        publisher
            .announce("ac6-pub", 1, "/tmp", "pub")
            .await
            .unwrap();
        publisher
            .publish("ac6.topic", serde_json::json!({"hello": "fleet"}))
            .await
            .unwrap();

        use futures::StreamExt as _;
        let msg = timeout(Duration::from_secs(5), raw_sub.next())
            .await
            .expect("timed out waiting for NATS relay")
            .expect("subscription closed unexpectedly");

        let env: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!(env["origin_node"], "testa");
        assert_eq!(env["data"], serde_json::json!({"hello": "fleet"}));

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
    });
}

/// AC7: two uplinked daemons on one nats-server — a publish via A is
/// received exactly once by a subscriber attached to B.
#[test]
fn ac7_cross_daemon_delivery_exactly_once() {
    if !nats_server_available() {
        skip_no_nats_server("ac7_cross_daemon_delivery_exactly_once");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (_nats, daemon_a, daemon_b) = setup_pair().await;

        let mut sub = Client::connect(&daemon_b.socket).await.unwrap();
        sub.announce("ac7-sub", 1, "/tmp", "sub").await.unwrap();
        sub.subscribe("ac7.topic").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut publisher = Client::connect(&daemon_a.socket).await.unwrap();
        publisher
            .announce("ac7-pub", 1, "/tmp", "pub")
            .await
            .unwrap();
        publisher
            .publish("ac7.topic", serde_json::json!({"n": 1}))
            .await
            .unwrap();

        let ev = timeout(Duration::from_secs(5), sub.next_event())
            .await
            .expect("timed out waiting for cross-daemon delivery")
            .unwrap()
            .expect("subscription EOF");
        assert_eq!(ev.topic, "ac7.topic");
        assert_eq!(ev.data, serde_json::json!({"n": 1}));

        // Exactly once: no second delivery should follow.
        let second = timeout(Duration::from_millis(300), sub.next_event()).await;
        assert!(second.is_err(), "expected exactly one delivery, got a second");

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
    });
}

/// AC8: a 10-message burst published via A is never echoed back to a
/// subscriber on A (self-origin drop), and B's subscriber sees each message
/// exactly once (no loop amplification).
#[test]
fn ac8_no_self_echo_and_no_amplification() {
    if !nats_server_available() {
        skip_no_nats_server("ac8_no_self_echo_and_no_amplification");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (_nats, daemon_a, daemon_b) = setup_pair().await;

        let mut sub_a = Client::connect(&daemon_a.socket).await.unwrap();
        sub_a.announce("ac8-sub-a", 1, "/tmp", "sub").await.unwrap();
        sub_a.subscribe("ac8.burst").await.unwrap();

        let mut sub_b = Client::connect(&daemon_b.socket).await.unwrap();
        sub_b.announce("ac8-sub-b", 1, "/tmp", "sub").await.unwrap();
        sub_b.subscribe("ac8.burst").await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut publisher = Client::connect(&daemon_a.socket).await.unwrap();
        publisher
            .announce("ac8-pub", 1, "/tmp", "pub")
            .await
            .unwrap();
        for i in 0..10 {
            publisher
                .publish("ac8.burst", serde_json::json!({"i": i}))
                .await
                .unwrap();
        }

        // B must see exactly 10 (one per message, no amplification).
        let mut count_b = 0;
        loop {
            match timeout(Duration::from_millis(800), sub_b.next_event()).await {
                Ok(Ok(Some(_ev))) => count_b += 1,
                _ => break,
            }
        }
        assert_eq!(count_b, 10, "B should see exactly one delivery per message");

        // A's own local subscriber sees the 10 *local* publishes (that's the
        // normal local-bus behavior, unrelated to the uplink) but must NOT
        // see any additional uplink echo of its own messages beyond that.
        let mut count_a = 0;
        loop {
            match timeout(Duration::from_millis(800), sub_a.next_event()).await {
                Ok(Ok(Some(_ev))) => count_a += 1,
                _ => break,
            }
        }
        assert_eq!(
            count_a, 10,
            "A should see its own 10 local publishes and no uplink echo on top"
        );

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
    });
}
