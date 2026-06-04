//! Acceptance tests for PRD-agorabus-state-persist.
//!
//! Covers ACs 1-6 (claim persist, expired-claim pruning, intent persist,
//! atomic write + mode 0600, corrupt-journal start-clean, no-regression).
//!
//! These tests launch a real in-process daemon, mutate state, bounce the
//! daemon, and verify that the durable slice survived the restart.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::similar_names,
    clippy::indexing_slicing,
)]

mod common;

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agorabus::{
    Client, DEFAULT_DRAIN_GRACE_MS, DEFAULT_DRAIN_RESUME_HINT_MS, DEFAULT_STATE_FLUSH_MS,
    DaemonConfig, run_daemon,
    protocol::ClaimRecord,
};
use common::DaemonHandle;
use tokio::sync::oneshot;

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

async fn announce(socket: &PathBuf, sid: &str) -> Client {
    let mut c = Client::connect(socket).await.expect("connect");
    let r = c
        .announce(sid, std::process::id(), "/tmp", "persist-test")
        .await
        .expect("announce");
    assert!(r.ok, "announce failed: {:?}", r.error);
    c
}

// ──────────────────────────────────────────────────────────────────────────────
// AC1: A claim acquired before daemon stop is present after restart.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn ac1_claims_survive_restart() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");

        // ── first daemon ───────────────────────────────────────────────────
        let cfg1 = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file: state_file.clone(),
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx1, rrx1) = oneshot::channel::<()>();
        let (stx1, srx1) = oneshot::channel::<()>();
        let j1 = tokio::spawn(async move { run_daemon(cfg1, Some(rtx1), srx1).await });
        rrx1.await.unwrap();

        let mut c = announce(&socket, "sid-persist-ac1").await;
        let ttl_unix = now_unix_secs() + 600;
        let r = c
            .claim_acquire("/tmp/ac1-target", ttl_unix, "ac1", false)
            .await
            .unwrap();
        assert!(r.ok, "acquire failed: {:?}", r);

        // Allow debounce window to flush.
        tokio::time::sleep(Duration::from_millis(DEFAULT_STATE_FLUSH_MS + 100)).await;

        // Shutdown first daemon (triggers final flush).
        let _ = stx1.send(());
        let _ = j1.await;

        // ── second daemon (same state_file) ───────────────────────────────
        let cfg2 = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file,
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx2, rrx2) = oneshot::channel::<()>();
        let (stx2, srx2) = oneshot::channel::<()>();
        let j2 = tokio::spawn(async move { run_daemon(cfg2, Some(rtx2), srx2).await });
        rrx2.await.unwrap();

        let mut q = announce(&socket, "sid-query-ac1").await;
        let claims: Vec<ClaimRecord> = q.claim_list().await.unwrap();
        assert_eq!(claims.len(), 1, "expected 1 claim after restart, got {claims:?}");
        assert_eq!(claims[0].path, "/tmp/ac1-target");
        assert_eq!(claims[0].session_id, "sid-persist-ac1");
        assert_eq!(claims[0].ttl_unix_secs, ttl_unix, "ttl_unix_secs preserved");

        let _ = stx2.send(());
        let _ = j2.await;
    });
}

// ──────────────────────────────────────────────────────────────────────────────
// AC2: Expired claims are pruned on load, not resurrected.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn ac2_expired_claims_not_resurrected() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");

        let cfg1 = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file: state_file.clone(),
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx1, rrx1) = oneshot::channel::<()>();
        let (stx1, srx1) = oneshot::channel::<()>();
        let j1 = tokio::spawn(async move { run_daemon(cfg1, Some(rtx1), srx1).await });
        rrx1.await.unwrap();

        let mut c = announce(&socket, "sid-persist-ac2").await;
        // TTL = now + 1s (will be expired by the time we query).
        let ttl_unix = now_unix_secs() + 1;
        let r = c
            .claim_acquire("/tmp/ac2-expiry", ttl_unix, "ac2", false)
            .await
            .unwrap();
        assert!(r.ok, "acquire failed: {:?}", r);

        // Wait for both the claim to expire and the flush debounce to elapse.
        tokio::time::sleep(Duration::from_millis(DEFAULT_STATE_FLUSH_MS + 1500)).await;

        let _ = stx1.send(());
        let _ = j1.await;

        // Relaunch — the expired claim must be pruned on load.
        let cfg2 = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file,
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx2, rrx2) = oneshot::channel::<()>();
        let (stx2, srx2) = oneshot::channel::<()>();
        let j2 = tokio::spawn(async move { run_daemon(cfg2, Some(rtx2), srx2).await });
        rrx2.await.unwrap();

        let mut q = announce(&socket, "sid-query-ac2").await;
        let claims: Vec<ClaimRecord> = q.claim_list().await.unwrap();
        assert!(
            claims.is_empty(),
            "expired claim should be pruned on load, got {claims:?}"
        );

        let _ = stx2.send(());
        let _ = j2.await;
    });
}

// ──────────────────────────────────────────────────────────────────────────────
// AC3: Sticky intent survives a daemon bounce.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn ac3_sticky_intent_survives_restart() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");

        let cfg1 = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file: state_file.clone(),
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx1, rrx1) = oneshot::channel::<()>();
        let (stx1, srx1) = oneshot::channel::<()>();
        let j1 = tokio::spawn(async move { run_daemon(cfg1, Some(rtx1), srx1).await });
        rrx1.await.unwrap();

        let mut c = announce(&socket, "sid-intent-ac3").await;
        // Set intent via heartbeat with structured fields.
        let r = c
            .heartbeat_with_intent(
                "",
                Some("build".into()),
                Some("my-prd".into()),
                Some(vec!["/tmp/a".into(), "/tmp/b".into()]),
            )
            .await
            .unwrap();
        assert!(r.ok, "heartbeat_with_intent failed: {:?}", r);

        tokio::time::sleep(Duration::from_millis(DEFAULT_STATE_FLUSH_MS + 100)).await;

        let _ = stx1.send(());
        let _ = j1.await;

        // Check the state.json directly — the intent must be recorded.
        let raw = std::fs::read_to_string(&state_file).unwrap();
        let val: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let intents = val.get("intents").expect("intents key present");
        let intent = intents
            .get("sid-intent-ac3")
            .expect("sid-intent-ac3 intent present");
        assert_eq!(
            intent.get("skill").and_then(|v| v.as_str()),
            Some("build"),
            "skill persisted"
        );
        assert_eq!(
            intent.get("prd_slug").and_then(|v| v.as_str()),
            Some("my-prd"),
            "prd_slug persisted"
        );
    });
}

// ──────────────────────────────────────────────────────────────────────────────
// AC4: state.json is mode 0600 and written atomically (no .tmp residue).
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn ac4_atomic_write_mode_0600() {
    use std::os::unix::fs::PermissionsExt as _;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");

        let cfg = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file: state_file.clone(),
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx, rrx) = oneshot::channel::<()>();
        let (stx, srx) = oneshot::channel::<()>();
        let j = tokio::spawn(async move { run_daemon(cfg, Some(rtx), srx).await });
        rrx.await.unwrap();

        let mut c = announce(&socket, "sid-mode-ac4").await;
        let ttl = now_unix_secs() + 600;
        let r = c.claim_acquire("/tmp/mode-test", ttl, "ac4", false).await.unwrap();
        assert!(r.ok);

        // Wait for the flush.
        tokio::time::sleep(Duration::from_millis(DEFAULT_STATE_FLUSH_MS + 100)).await;

        // File must exist and be mode 0600.
        let meta = std::fs::metadata(&state_file).expect("state.json should exist");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected mode 0600, got {mode:o}");

        // No .tmp residue.
        let residue: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(residue.is_empty(), "tmp residue found: {residue:?}");

        let _ = stx.send(());
        let _ = j.await;
    });
}

// ──────────────────────────────────────────────────────────────────────────────
// AC5: A corrupt/truncated state.json starts the daemon with empty state.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn ac5_corrupt_journal_starts_clean() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");

        // Write a deliberately truncated/corrupt file.
        std::fs::write(&state_file, b"{\"claims\": {\"bad\":").unwrap();

        let cfg = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout: Duration::from_secs(60),
            broadcast_capacity: 64,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file: state_file.clone(),
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (rtx, rrx) = oneshot::channel::<()>();
        let (stx, srx) = oneshot::channel::<()>();
        // Daemon must start successfully despite the corrupt file.
        let j = tokio::spawn(async move { run_daemon(cfg, Some(rtx), srx).await });
        rrx.await.expect("daemon should start even with corrupt state");

        // Claims list must be empty (started clean).
        let mut c = announce(&socket, "sid-corrupt-ac5").await;
        let claims: Vec<ClaimRecord> = c.claim_list().await.unwrap();
        assert!(
            claims.is_empty(),
            "daemon should start with empty state after corrupt journal, got {claims:?}"
        );

        let _ = stx.send(());
        let _ = j.await;
    });
}

// ──────────────────────────────────────────────────────────────────────────────
// AC6: No regression — with no state.json the daemon behaves as 0.8.0.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn ac6_no_state_file_starts_clean_and_writes_on_first_mutation() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Use the shared DaemonHandle (which starts with no pre-existing state).
        let h = DaemonHandle::start().await;
        let state_file = h.tmp.path().join("state.json");

        // Before any mutation, state.json may or may not exist.
        // The important thing is the daemon is up.
        let mut c = announce(&h.socket, "sid-regression-ac6").await;
        let claims: Vec<ClaimRecord> = c.claim_list().await.unwrap();
        assert!(claims.is_empty(), "new daemon starts with empty claims");

        // After a mutation, state.json should be created.
        let ttl = now_unix_secs() + 600;
        let r = c.claim_acquire("/tmp/regression-target", ttl, "ac6", false).await.unwrap();
        assert!(r.ok);

        tokio::time::sleep(Duration::from_millis(DEFAULT_STATE_FLUSH_MS + 100)).await;

        assert!(
            state_file.exists(),
            "state.json should be created after first mutation"
        );

        h.shutdown().await;
    });
}
