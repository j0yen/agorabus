//! Acceptance tests for PRD-agorabus-nats-uplink AC9/AC10: remote peers
//! relayed over the uplink show up (tagged with `node`) in
//! `agorabus peers --fleet`, expire within the peer TTL once their daemon
//! goes away, and never leak into the plain (non-`--fleet`) `peers` output.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use agorabus::{Client, UplinkConfig, UplinkStatus};
use agorabus::fleet::collect_fleet_peers_from_bus;
use common::DaemonHandle;
use common::nats_harness::{NatsServer, nats_server_available, skip_no_nats_server};
use std::time::Duration;
use tokio::time::Instant;

/// Fast presence refresh + a short TTL so the test doesn't need to wait
/// out the production 300s default.
const PRESENCE_INTERVAL_MS: u64 = 100;
const TEST_TTL_SECS: u64 = 1;

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

/// AC9: B's peers appear on A's `peers --fleet` tagged `node=testb`; once
/// B's daemon is killed, they disappear within the peer TTL.
#[test]
fn ac9_remote_peer_visible_then_expires_after_daemon_dies() {
    if !nats_server_available() {
        skip_no_nats_server("ac9_remote_peer_visible_then_expires_after_daemon_dies");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let nats = NatsServer::spawn().await;
        let cfg_a = UplinkConfig {
            enabled: true,
            url: nats.url(),
            node: "testa".to_string(),
            fleet_presence_interval_ms: PRESENCE_INTERVAL_MS,
        };
        let cfg_b = UplinkConfig {
            enabled: true,
            url: nats.url(),
            node: "testb".to_string(),
            fleet_presence_interval_ms: PRESENCE_INTERVAL_MS,
        };
        let daemon_a = DaemonHandle::start_with_uplink(cfg_a).await;
        let mut daemon_b = DaemonHandle::start_with_uplink(cfg_b).await;

        wait_connected(&daemon_a.socket, "wait-a").await;
        wait_connected(&daemon_b.socket, "wait-b").await;

        // A long-lived local peer on B, so the presence broadcaster has
        // something to announce.
        let mut peer_on_b = Client::connect(&daemon_b.socket).await.unwrap();
        peer_on_b
            .announce("b-local-peer", 42, "/on/b", "hanging-around")
            .await
            .unwrap();

        // Wait for at least one presence broadcast cycle to land on A.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let remote = collect_fleet_peers_from_bus(&daemon_a.socket, "ac9-query", TEST_TTL_SECS)
                .await;
            if remote.iter().any(|p| p.node.as_deref() == Some("testb")) {
                break;
            }
            if Instant::now() >= deadline {
                panic!("B's peer never showed up in A's --fleet view");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Kill B's daemon entirely (models "B's daemon is killed").
        daemon_b.stop_only().await;
        drop(peer_on_b);

        // No further presence refreshes arrive; after TTL + a couple of
        // refresh intervals, the remote peer must be gone.
        tokio::time::sleep(Duration::from_secs(TEST_TTL_SECS) + Duration::from_millis(500)).await;
        let remote_after =
            collect_fleet_peers_from_bus(&daemon_a.socket, "ac9-query-after", TEST_TTL_SECS).await;
        assert!(
            !remote_after.iter().any(|p| p.node.as_deref() == Some("testb")),
            "B's peer should have expired within the TTL, still present: {remote_after:?}"
        );

        daemon_a.shutdown().await;
    });
}

/// AC10: plain `peers` (no `--fleet`) never includes remote peers — the
/// library-level equivalent of the CLI's default (non-`--fleet`) path,
/// which never touches the fleet-collection function at all.
#[test]
fn ac10_plain_peers_excludes_remote() {
    if !nats_server_available() {
        skip_no_nats_server("ac10_plain_peers_excludes_remote");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let nats = NatsServer::spawn().await;
        let cfg_a = UplinkConfig {
            enabled: true,
            url: nats.url(),
            node: "testa".to_string(),
            fleet_presence_interval_ms: PRESENCE_INTERVAL_MS,
        };
        let cfg_b = UplinkConfig {
            enabled: true,
            url: nats.url(),
            node: "testb".to_string(),
            fleet_presence_interval_ms: PRESENCE_INTERVAL_MS,
        };
        let daemon_a = DaemonHandle::start_with_uplink(cfg_a).await;
        let daemon_b = DaemonHandle::start_with_uplink(cfg_b).await;
        wait_connected(&daemon_a.socket, "wait-a").await;
        wait_connected(&daemon_b.socket, "wait-b").await;

        let mut peer_on_a = Client::connect(&daemon_a.socket).await.unwrap();
        peer_on_a
            .announce("a-local-peer", 7, "/on/a", "local")
            .await
            .unwrap();
        let _peer_on_b = {
            let mut c = Client::connect(&daemon_b.socket).await.unwrap();
            c.announce("b-local-peer", 8, "/on/b", "local").await.unwrap();
            c
        };

        // Let a presence cycle or two pass so B's presence is definitely
        // flowing over the uplink and landing on A's local bus.
        tokio::time::sleep(Duration::from_millis(400)).await;

        let mut query = Client::connect(&daemon_a.socket).await.unwrap();
        query
            .announce("ac10-query", 1, "/tmp", "query")
            .await
            .unwrap();
        let local_peers = query.peers().await.unwrap();
        assert!(
            local_peers.iter().all(|p| p.node.is_none()),
            "plain peers() must never include a remote (node=Some(..)) entry: {local_peers:?}"
        );
        assert!(
            local_peers.iter().any(|p| p.session_id == "a-local-peer"),
            "the genuinely local peer must still be present"
        );

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
    });
}
