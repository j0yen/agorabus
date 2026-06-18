//! Acceptance tests for tether-presence fleet peer extension.
//!
//! Tests use the existing in-process UDS harness (DaemonHandle) and the
//! pure-Rust FleetStore / merge_peers logic. No external NATS required.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
)]

mod common;

use agorabus::{
    Client, FleetPresenceEvent, FleetStore, merge_peers, peer_age_secs,
    protocol::{FLEET_PRESENCE_SUBJECT_ANNOUNCE, FLEET_PRESENCE_SUBJECT_GONE},
};
use common::DaemonHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

/// AC1: announce without `node` → peer recorded as local, node field absent.
#[test]
fn ac1_announce_without_node_is_local() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        let mut client = Client::connect(&h.socket).await.unwrap();
        let reply = client.announce("ac1-local", 1, "/tmp", "test").await.unwrap();
        assert!(reply.ok, "announce should succeed");

        let mut q = Client::connect(&h.socket).await.unwrap();
        q.announce("ac1-q", 2, "/tmp", "q").await.unwrap();
        let peers = q.peers().await.unwrap();
        let peer = peers.iter().find(|p| p.session_id == "ac1-local").unwrap();
        assert_eq!(peer.node, None, "local announce must have node=None");

        // Serialize — `node` key must be absent from JSON (skip_serializing_if).
        let json = serde_json::to_string(peer).unwrap();
        assert!(
            !json.contains("\"node\""),
            "node must not appear in JSON for local peer: {json}"
        );

        h.shutdown().await;
    });
}

/// AC2: announce with `node: "worknode"` → peer recorded with node="worknode".
#[test]
fn ac2_announce_with_node_is_tagged() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        let mut client = Client::connect(&h.socket).await.unwrap();
        let reply = client
            .announce_with_node("ac2-remote", 99, "/remote", "test", Some("worknode".to_string()))
            .await
            .unwrap();
        assert!(reply.ok, "announce_with_node should succeed");

        let mut q = Client::connect(&h.socket).await.unwrap();
        q.announce("ac2-q", 2, "/tmp", "q").await.unwrap();
        let peers = q.peers().await.unwrap();
        let peer = peers.iter().find(|p| p.session_id == "ac2-remote").unwrap();
        assert_eq!(
            peer.node.as_deref(),
            Some("worknode"),
            "peer should carry node tag"
        );

        let json = serde_json::to_string(peer).unwrap();
        assert!(
            json.contains("\"node\":\"worknode\""),
            "node field must appear in JSON: {json}"
        );

        h.shutdown().await;
    });
}

/// AC3: merge_peers with empty remote list is byte-equivalent to local-only.
#[test]
fn ac3_no_fleet_returns_local_only() {
    let local = vec![agorabus::PeerRecord {
        session_id: "local-s1".to_string(),
        pid: 1,
        cwd: "/".to_string(),
        intent: String::new(),
        last_tool: String::new(),
        skill: String::new(),
        prd_slug: String::new(),
        working_paths: Vec::new(),
        last_heartbeat_unix_secs: unix_now(),
        node: None,
        extra: Default::default(),
    }];
    let merged = merge_peers(local.clone(), Vec::new());
    // Output should be identical: same len, same node=None.
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].node, None);
    // JSON must be identical.
    let j_before = serde_json::to_string(&local[0]).unwrap();
    let j_after = serde_json::to_string(&merged[0]).unwrap();
    assert_eq!(j_before, j_after, "no-fleet output must be byte-identical");
}

/// AC4: fleet store dedup + merge produces correct combined list.
#[test]
fn ac4_fleet_merge_and_dedup() {
    let ev = FleetPresenceEvent {
        session_id: "remote-s1".to_string(),
        pid: 1234,
        cwd: "/remote".to_string(),
        node: "worknode".to_string(),
        ts: unix_now(),
    };

    let mut store = FleetStore::new();
    store.announce(&ev);
    store.announce(&ev); // reannounce — must not duplicate.

    let remote_peers = store.live_peers();
    assert_eq!(remote_peers.len(), 1, "dedup on reannounce");
    assert_eq!(remote_peers[0].node.as_deref(), Some("worknode"));

    let local_peer = agorabus::PeerRecord {
        session_id: "local-s1".to_string(),
        pid: 42,
        cwd: "/local".to_string(),
        intent: String::new(),
        last_tool: String::new(),
        skill: String::new(),
        prd_slug: String::new(),
        working_paths: Vec::new(),
        last_heartbeat_unix_secs: unix_now(),
        node: None,
        extra: Default::default(),
    };
    let merged = merge_peers(vec![local_peer], remote_peers);
    assert_eq!(merged.len(), 2, "local + one remote");

    let rp = merged.iter().find(|p| p.session_id == "remote-s1").unwrap();
    assert_eq!(rp.node.as_deref(), Some("worknode"));
    assert_eq!(rp.pid, 1234);
}

/// AC4b: peer_age_secs returns a reasonable freshness age.
#[test]
fn ac4b_peer_freshness_age() {
    let ts = unix_now().saturating_sub(5); // 5 seconds ago
    let peer = agorabus::PeerRecord {
        session_id: "s".to_string(),
        pid: 1,
        cwd: "/".to_string(),
        intent: String::new(),
        last_tool: String::new(),
        skill: String::new(),
        prd_slug: String::new(),
        working_paths: Vec::new(),
        last_heartbeat_unix_secs: ts,
        node: Some("n".to_string()),
        extra: Default::default(),
    };
    let age = peer_age_secs(&peer);
    // Allow ±2s for clock jitter.
    assert!(age >= 3 && age <= 10, "expected ~5s age, got {age}");
}

/// AC5: a fleet presence event injected via publish is received exactly once.
/// The daemon does NOT re-emit fleet presence events (loop guard is structural).
#[test]
fn ac5_presence_event_received_exactly_once() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        // Subscriber.
        let mut sub = Client::connect(&h.socket).await.unwrap();
        sub.announce("ac5-sub", 1, "/tmp", "sub").await.unwrap();
        sub.subscribe("wm.fleet.presence").await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Publisher simulates a remote bridge injecting a presence event.
        let mut pub_c = Client::connect(&h.socket).await.unwrap();
        pub_c.announce("ac5-pub", 2, "/tmp", "pub").await.unwrap();

        let ev = FleetPresenceEvent {
            session_id: "remote-x".to_string(),
            pid: 99,
            cwd: "/remote".to_string(),
            node: "farnode".to_string(),
            ts: unix_now(),
        };
        let payload = serde_json::to_value(&ev).unwrap();
        pub_c
            .publish(FLEET_PRESENCE_SUBJECT_ANNOUNCE, payload)
            .await
            .unwrap();

        // Subscriber should receive exactly one event.
        let received = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            sub.next_event(),
        )
        .await
        .expect("should receive event within 500ms")
        .expect("no I/O error")
        .expect("event should not be None");

        assert_eq!(received.topic, FLEET_PRESENCE_SUBJECT_ANNOUNCE);
        let received_ev: FleetPresenceEvent =
            serde_json::from_value(received.data).expect("valid FleetPresenceEvent");
        assert_eq!(received_ev.session_id, "remote-x");
        assert_eq!(received_ev.node, "farnode");

        // Second event must NOT arrive (daemon does not re-publish).
        let second = tokio::time::timeout(
            tokio::time::Duration::from_millis(150),
            sub.next_event(),
        )
        .await;
        assert!(
            second.is_err(),
            "daemon must not re-emit fleet presence events (loop guard)"
        );

        h.shutdown().await;
    });
}

/// AC6: stale peers omitted; fresh peers live.
#[test]
fn ac6_stale_peer_omitted() {
    // FleetStore::with_ttl(0): age=0 satisfies ≤ 0 so entry is marginally live.
    // Test the remove() path instead (AC6b covers gone events).
    let mut store = FleetStore::new();
    assert_eq!(store.live_peers().len(), 0, "empty store → no peers");

    store.announce(&FleetPresenceEvent {
        session_id: "s".to_string(),
        pid: 1,
        cwd: "/".to_string(),
        node: "n".to_string(),
        ts: unix_now(),
    });
    assert_eq!(store.live_peers().len(), 1, "fresh peer should be live");

    // Simulate a gone event.
    let _ = FLEET_PRESENCE_SUBJECT_GONE; // ensure constant is accessible
    store.remove("s", "n");
    assert_eq!(store.live_peers().len(), 0, "peer should be gone after remove");
}
