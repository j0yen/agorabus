//! Acceptance tests for PRD-chord-claim ACs 1-7.
//!
//! Project: agorabus (rust-extend, v0.1 -> v0.2 via PRD-chord-claim)
//! Covers daemon-level behaviors. ACs 8-9 (CLI fail-open + --wait) live in
//! `tests/claim_cli_acs.rs` since they exercise the CLI binary as a
//! subprocess. AC10 is a meta-AC (version bump + CHANGELOG) verified at
//! archive time via the manifest `verification` field, not at runtime.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::needless_borrow,
    clippy::similar_names,
    clippy::indexing_slicing,
    clippy::tests_outside_test_module
)]

mod common;

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agorabus::{Client, protocol::ClaimRecord};
use common::DaemonHandle;

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn canonical(p: &std::path::Path) -> String {
    std::fs::canonicalize(p)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

async fn announce_client(socket: &PathBuf, sid: &str) -> Client {
    let mut c = Client::connect(socket).await.expect("connect");
    let reply = c
        .announce(sid, std::process::id(), "/tmp", "claim-test")
        .await
        .expect("announce ok");
    assert!(reply.ok, "announce returned ok=false: {:?}", reply.error);
    c
}

#[test]
fn claim_ac1_acquire_returns_ok_with_canonicalized_path() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        let mut c = announce_client(&h.socket, "sid-A").await;
        let now = now_unix_secs();
        let ttl_unix = now + 60;
        let reply = c
            .claim_acquire(&canon, ttl_unix, "ac1", false)
            .await
            .unwrap();
        assert!(reply.ok, "expected ok, got {:?}", reply);
        let data = reply.data.expect("payload present");
        assert_eq!(
            data.get("path").and_then(|v| v.as_str()).unwrap(),
            canon.as_str(),
            "path echoed back canonicalized"
        );
        let echoed_ttl = data.get("ttl_unix_secs").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(echoed_ttl, ttl_unix, "ttl_unix_secs echoed verbatim");
        assert!(PathBuf::from(&canon).is_absolute(), "path is absolute");

        h.shutdown().await;
    });
}

#[test]
fn claim_ac2_list_returns_claim_and_json_parses() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        let mut a = announce_client(&h.socket, "sid-A").await;
        let ttl_unix = now_unix_secs() + 60;
        let r = a.claim_acquire(&canon, ttl_unix, "ac2", false).await.unwrap();
        assert!(r.ok);

        let mut q = announce_client(&h.socket, "sid-listquery").await;
        let claims: Vec<ClaimRecord> = q.claim_list().await.unwrap();
        assert_eq!(claims.len(), 1, "exactly one claim, got {claims:?}");
        let only = &claims[0];
        assert_eq!(only.path, canon);
        assert_eq!(only.session_id, "sid-A");
        assert_eq!(only.ttl_unix_secs, ttl_unix);

        // JSON re-serialization round-trips (json --format equivalent).
        let json = serde_json::to_string(&claims).unwrap();
        let parsed: Vec<ClaimRecord> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, claims);

        h.shutdown().await;
    });
}

#[test]
fn claim_ac3_conflict_from_different_session() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        let mut a = announce_client(&h.socket, "sid-A").await;
        let ttl_unix = now_unix_secs() + 60;
        assert!(a.claim_acquire(&canon, ttl_unix, "ac3", false).await.unwrap().ok);

        let mut b = announce_client(&h.socket, "sid-B").await;
        let reply = b.claim_acquire(&canon, ttl_unix, "ac3-b", false).await.unwrap();
        assert!(!reply.ok, "expected conflict, got ok");
        assert_eq!(reply.error.as_deref(), Some("claim_conflict"));
        let detail = reply.data.expect("detail present on conflict");
        assert_eq!(detail.get("holder").and_then(|v| v.as_str()), Some("sid-A"));
        assert_eq!(
            detail.get("expires_unix").and_then(|v| v.as_u64()),
            Some(ttl_unix)
        );

        h.shutdown().await;
    });
}

#[test]
fn claim_ac4_renewal_on_same_session() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        let mut a = announce_client(&h.socket, "sid-A").await;
        let ttl1 = now_unix_secs() + 30;
        let r1 = a.claim_acquire(&canon, ttl1, "first", false).await.unwrap();
        assert!(r1.ok);

        let ttl2 = now_unix_secs() + 120;
        let r2 = a.claim_acquire(&canon, ttl2, "renew", false).await.unwrap();
        assert!(r2.ok, "renewal should succeed, got {:?}", r2);
        let echoed = r2
            .data
            .as_ref()
            .and_then(|d| d.get("ttl_unix_secs"))
            .and_then(|v| v.as_u64())
            .unwrap();
        assert!(
            echoed >= ttl1,
            "renewal ttl_unix_secs {echoed} must be >= original {ttl1}"
        );
        assert_eq!(echoed, ttl2);

        // List confirms the in-table record carries the renewed ttl.
        let claims = a.claim_list().await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].ttl_unix_secs, ttl2);

        h.shutdown().await;
    });
}

#[test]
fn claim_ac5_release_is_idempotent() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        let mut a = announce_client(&h.socket, "sid-A").await;
        let ttl_unix = now_unix_secs() + 60;
        assert!(a.claim_acquire(&canon, ttl_unix, "ac5", false).await.unwrap().ok);

        let r1 = a.claim_release(&canon).await.unwrap();
        assert!(r1.ok);
        let released1 = r1
            .data
            .as_ref()
            .and_then(|d| d.get("released"))
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(released1, "first release should report released:true");

        let r2 = a.claim_release(&canon).await.unwrap();
        assert!(r2.ok, "second release still returns ok");
        let released2 = r2
            .data
            .as_ref()
            .and_then(|d| d.get("released"))
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(!released2, "second release should report released:false");

        // No active claims remain.
        let claims = a.claim_list().await.unwrap();
        assert!(claims.is_empty());

        h.shutdown().await;
    });
}

#[test]
fn claim_ac6_force_overrides_and_publishes_release_before_acquire() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        // Subscriber on a separate connection BEFORE the force-evict, so we
        // capture both the synthetic claim.release (for sid-A) and the
        // claim.acquire (for sid-B) in order.
        let mut watcher = announce_client(&h.socket, "sid-watcher").await;
        let _ = watcher.subscribe("claim.").await.unwrap();

        let mut a = announce_client(&h.socket, "sid-A").await;
        let ttl_unix = now_unix_secs() + 60;
        assert!(a.claim_acquire(&canon, ttl_unix, "by-A", false).await.unwrap().ok);

        // Wait for sid-A's initial claim.acquire to land on the watcher so
        // we know the bus has flushed before the force-evict.
        let ev = tokio::time::timeout(Duration::from_secs(2), watcher.next_event())
            .await
            .expect("first event in time")
            .unwrap()
            .expect("event present");
        assert_eq!(ev.topic, "claim.acquire");
        assert_eq!(ev.data.get("session_id").and_then(|v| v.as_str()), Some("sid-A"));

        let mut b = announce_client(&h.socket, "sid-B").await;
        let ttl_b = now_unix_secs() + 90;
        let r = b.claim_acquire(&canon, ttl_b, "force-B", true).await.unwrap();
        assert!(r.ok, "force acquire should succeed, got {:?}", r);

        // The next two events should be: claim.release for sid-A, then
        // claim.acquire for sid-B (in that order — daemon publishes release
        // first per PRD §Conflict resolution force semantics).
        let ev1 = tokio::time::timeout(Duration::from_secs(2), watcher.next_event())
            .await
            .expect("release in time")
            .unwrap()
            .expect("event present");
        assert_eq!(ev1.topic, "claim.release");
        assert_eq!(
            ev1.data.get("session_id").and_then(|v| v.as_str()),
            Some("sid-A"),
            "release names the evicted session"
        );

        let ev2 = tokio::time::timeout(Duration::from_secs(2), watcher.next_event())
            .await
            .expect("acquire in time")
            .unwrap()
            .expect("event present");
        assert_eq!(ev2.topic, "claim.acquire");
        assert_eq!(
            ev2.data.get("session_id").and_then(|v| v.as_str()),
            Some("sid-B"),
            "acquire names the new holder"
        );

        // List shows sid-B holds the claim now.
        let claims = b.claim_list().await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].session_id, "sid-B");

        h.shutdown().await;
    });
}

#[test]
fn claim_ac7_ttl_expiry_is_silently_pruned_on_read() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("foo");
        std::fs::write(&target, "").unwrap();
        let canon = canonical(&target);

        let mut a = announce_client(&h.socket, "sid-A").await;
        // ttl=1 second → set ttl_unix to now+1.
        let ttl_unix = now_unix_secs() + 1;
        assert!(a.claim_acquire(&canon, ttl_unix, "ac7", false).await.unwrap().ok);

        // Sleep until we're strictly past the ttl (daemon prunes c.ttl_unix_secs <= now).
        tokio::time::sleep(Duration::from_millis(2100)).await;

        let claims = a.claim_list().await.unwrap();
        assert!(claims.is_empty(), "expired claim should be pruned, got {claims:?}");

        h.shutdown().await;
    });
}
