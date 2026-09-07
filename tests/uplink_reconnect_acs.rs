//! Acceptance test for PRD-agorabus-nats-uplink AC5: local pub/sub keeps
//! working throughout a leaf outage, and the uplink reconnects within 30s
//! of the leaf coming back.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use agorabus::{Client, UplinkConfig, UplinkStatus};
use common::DaemonHandle;
use common::nats_harness::{NatsServer, nats_server_available, skip_no_nats_server};
use std::time::Duration;
use tokio::time::Instant;

/// AC5: given a connected uplink, when the nats-server stops and later
/// restarts, local pub/sub keeps working throughout and the uplink
/// reconnects within 30s (observed via status polling, mirroring what
/// `doctor` would report).
#[test]
fn ac5_local_bus_survives_leaf_outage_and_uplink_reconnects() {
    if !nats_server_available() {
        skip_no_nats_server("ac5_local_bus_survives_leaf_outage_and_uplink_reconnects");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut nats = NatsServer::spawn().await;
        let uplink = UplinkConfig {
            enabled: true,
            url: nats.url(),
            node: "reconnecttest".to_string(),
            fleet_presence_interval_ms: 60_000,
        };
        let daemon = DaemonHandle::start_with_uplink(uplink).await;

        let mut status_client = Client::connect(&daemon.socket).await.unwrap();
        status_client
            .announce("status-query", 1, "/tmp", "status")
            .await
            .unwrap();

        // Wait for initial connect.
        wait_for(&mut status_client, |s| matches!(s, UplinkStatus::Connected { .. }), 10).await;

        // Local bus sanity check #1, before the outage.
        assert_local_bus_works(&daemon.socket, "pre-outage").await;

        // Kill the leaf.
        nats.stop();
        wait_for(&mut status_client, |s| matches!(s, UplinkStatus::Reconnecting { .. }), 10).await;

        // Local bus must keep working *during* the outage.
        assert_local_bus_works(&daemon.socket, "during-outage").await;

        // Bring the leaf back and require reconnect within 30s (AC5).
        nats.restart().await;
        let start = Instant::now();
        wait_for(&mut status_client, |s| matches!(s, UplinkStatus::Connected { .. }), 30).await;
        assert!(
            start.elapsed() <= Duration::from_secs(30),
            "uplink must reconnect within 30s of the leaf returning"
        );

        // Local bus sanity check #2, after recovery.
        assert_local_bus_works(&daemon.socket, "post-recovery").await;

        daemon.shutdown().await;
    });
}

async fn wait_for(
    client: &mut Client,
    pred: impl Fn(&UplinkStatus) -> bool,
    timeout_secs: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let status = client.uplink_status().await.unwrap();
        if pred(&status) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("uplink status never matched predicate within {timeout_secs}s: {status:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_local_bus_works(socket: &std::path::Path, phase: &str) {
    let mut sub = Client::connect(socket).await.unwrap();
    sub.announce(&format!("local-sub-{phase}"), 1, "/tmp", "sub")
        .await
        .unwrap();
    sub.subscribe("local.check").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut publisher = Client::connect(socket).await.unwrap();
    publisher
        .announce(&format!("local-pub-{phase}"), 1, "/tmp", "pub")
        .await
        .unwrap();
    let reply = publisher
        .publish("local.check", serde_json::json!({"phase": phase}))
        .await
        .unwrap();
    assert!(reply.ok, "local publish must succeed during phase {phase}");

    let ev = tokio::time::timeout(Duration::from_secs(2), sub.next_event())
        .await
        .unwrap_or_else(|_| panic!("local delivery timed out during phase {phase}"))
        .unwrap()
        .unwrap_or_else(|| panic!("local subscriber EOF during phase {phase}"));
    assert_eq!(ev.data, serde_json::json!({"phase": phase}));
}
