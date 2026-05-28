//! Acceptance tests for PRD-chord-intent-rich (iter-1 scope).
//!
//! Covers:
//! - AC1: protocol back-compat. Existing
//!   `{"op":"heartbeat","tool":"Bash"}` continues to work; daemon replies
//!   `{"ok":true}` and the peer record updates `last_heartbeat` without
//!   touching skill/prd_slug/working_paths.
//! - AC2 (partial): A heartbeat carrying the new fields populates them on
//!   the peer record; a subsequent heartbeat that omits them leaves the
//!   prior values in place (sticky); a heartbeat with an explicit empty
//!   value clears.
//! - AC5: `working_paths` length > 8 is rejected with
//!   `{"ok":false,"error":"too_many_paths"}`.
//!
//! AC3/AC4 (CLI subcommands) and AC7 (version+changelog) are out of
//! scope for this iteration and live in later iters.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use agorabus::Client;
use common::DaemonHandle;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

async fn announce_and_query(socket: &std::path::Path, sid: &str) -> agorabus::PeerRecord {
    let mut q = Client::connect(socket).await.unwrap();
    q.announce("chord-querier", 99, "/tmp", "q").await.unwrap();
    let peers = q.peers().await.unwrap();
    peers
        .into_iter()
        .find(|p| p.session_id == sid)
        .unwrap_or_else(|| panic!("peer {sid} not present"))
}

#[test]
fn ac1_legacy_heartbeat_preserves_intent_fields() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start_with_timeout(Duration::from_secs(60)).await;

        // Open a long-lived connection for the subject peer.
        let stream = UnixStream::connect(&h.socket).await.unwrap();
        let (r, mut w) = stream.into_split();
        let mut reader = BufReader::new(r).lines();

        // Announce.
        let mut buf = serde_json::to_vec(&serde_json::json!({
            "op": "announce",
            "session_id": "ac1-legacy",
            "pid": 1u32,
            "cwd": "/tmp/ac1",
            "intent": ""
        }))
        .unwrap();
        buf.push(b'\n');
        w.write_all(&buf).await.unwrap();
        w.flush().await.unwrap();
        let _ = reader.next_line().await.unwrap().unwrap();

        // Legacy single-field heartbeat.
        let mut hb = serde_json::to_vec(&serde_json::json!({
            "op": "heartbeat",
            "tool": "Bash"
        }))
        .unwrap();
        hb.push(b'\n');
        w.write_all(&hb).await.unwrap();
        w.flush().await.unwrap();
        let line = reader.next_line().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["ok"], serde_json::Value::Bool(true));

        let rec = announce_and_query(&h.socket, "ac1-legacy").await;
        assert_eq!(rec.last_tool, "Bash");
        assert!(rec.skill.is_empty(), "skill untouched: {:?}", rec.skill);
        assert!(rec.prd_slug.is_empty(), "prd_slug untouched");
        assert!(rec.working_paths.is_empty(), "working_paths untouched");
        assert!(rec.last_heartbeat_unix_secs > 0);

        drop(w);
        drop(reader);
        h.shutdown().await;
    });
}

#[test]
fn ac2_new_fields_set_then_sticky_then_cleared() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start_with_timeout(Duration::from_secs(60)).await;

        let stream = UnixStream::connect(&h.socket).await.unwrap();
        let (r, mut w) = stream.into_split();
        let mut reader = BufReader::new(r).lines();

        let mut buf = serde_json::to_vec(&serde_json::json!({
            "op": "announce",
            "session_id": "ac2-sticky",
            "pid": 1u32,
            "cwd": "/tmp/ac2",
            "intent": ""
        }))
        .unwrap();
        buf.push(b'\n');
        w.write_all(&buf).await.unwrap();
        w.flush().await.unwrap();
        let _ = reader.next_line().await.unwrap().unwrap();

        // (a) Set all three fields.
        let mut hb = serde_json::to_vec(&serde_json::json!({
            "op": "heartbeat",
            "tool": "",
            "skill": "/build",
            "prd_slug": "chord-intent-rich",
            "working_paths": ["/home/jsy/wintermute/agorabus"]
        }))
        .unwrap();
        hb.push(b'\n');
        w.write_all(&hb).await.unwrap();
        w.flush().await.unwrap();
        let line = reader.next_line().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["ok"], serde_json::Value::Bool(true));

        let rec = announce_and_query(&h.socket, "ac2-sticky").await;
        assert_eq!(rec.skill, "/build");
        assert_eq!(rec.prd_slug, "chord-intent-rich");
        assert_eq!(rec.working_paths, vec!["/home/jsy/wintermute/agorabus"]);

        // (b) Sticky: heartbeat that omits the new fields retains prior values.
        let mut hb2 = serde_json::to_vec(&serde_json::json!({
            "op": "heartbeat",
            "tool": "Bash"
        }))
        .unwrap();
        hb2.push(b'\n');
        w.write_all(&hb2).await.unwrap();
        w.flush().await.unwrap();
        let _ = reader.next_line().await.unwrap().unwrap();

        let rec = announce_and_query(&h.socket, "ac2-sticky").await;
        assert_eq!(rec.last_tool, "Bash");
        assert_eq!(rec.skill, "/build", "sticky skill");
        assert_eq!(rec.prd_slug, "chord-intent-rich", "sticky prd_slug");
        assert_eq!(
            rec.working_paths,
            vec!["/home/jsy/wintermute/agorabus"],
            "sticky working_paths"
        );

        // (c) Explicit empty clears.
        let mut hb3 = serde_json::to_vec(&serde_json::json!({
            "op": "heartbeat",
            "tool": "",
            "skill": "",
            "prd_slug": "",
            "working_paths": []
        }))
        .unwrap();
        hb3.push(b'\n');
        w.write_all(&hb3).await.unwrap();
        w.flush().await.unwrap();
        let _ = reader.next_line().await.unwrap().unwrap();

        let rec = announce_and_query(&h.socket, "ac2-sticky").await;
        assert!(rec.skill.is_empty(), "cleared skill");
        assert!(rec.prd_slug.is_empty(), "cleared prd_slug");
        assert!(rec.working_paths.is_empty(), "cleared working_paths");

        drop(w);
        drop(reader);
        h.shutdown().await;
    });
}

#[test]
fn ac5_working_paths_cap_rejected() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start_with_timeout(Duration::from_secs(60)).await;

        let stream = UnixStream::connect(&h.socket).await.unwrap();
        let (r, mut w) = stream.into_split();
        let mut reader = BufReader::new(r).lines();

        let mut buf = serde_json::to_vec(&serde_json::json!({
            "op": "announce",
            "session_id": "ac5-overflow",
            "pid": 1u32,
            "cwd": "/tmp/ac5",
            "intent": ""
        }))
        .unwrap();
        buf.push(b'\n');
        w.write_all(&buf).await.unwrap();
        w.flush().await.unwrap();
        let _ = reader.next_line().await.unwrap().unwrap();

        // Nine paths (one over the cap) → rejected.
        let nine: Vec<String> = (0..9).map(|i| format!("/tmp/p{i}")).collect();
        let mut hb = serde_json::to_vec(&serde_json::json!({
            "op": "heartbeat",
            "tool": "",
            "working_paths": nine
        }))
        .unwrap();
        hb.push(b'\n');
        w.write_all(&hb).await.unwrap();
        w.flush().await.unwrap();
        let line = reader.next_line().await.unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["ok"], serde_json::Value::Bool(false));
        assert_eq!(v["error"], serde_json::Value::String("too_many_paths".into()));

        // Eight paths (at the cap) → accepted.
        let eight: Vec<String> = (0..8).map(|i| format!("/tmp/p{i}")).collect();
        let mut hb2 = serde_json::to_vec(&serde_json::json!({
            "op": "heartbeat",
            "tool": "",
            "working_paths": eight
        }))
        .unwrap();
        hb2.push(b'\n');
        w.write_all(&hb2).await.unwrap();
        w.flush().await.unwrap();
        let line2 = reader.next_line().await.unwrap().unwrap();
        let v2: serde_json::Value = serde_json::from_str(&line2).unwrap();
        assert_eq!(v2["ok"], serde_json::Value::Bool(true));

        let rec = announce_and_query(&h.socket, "ac5-overflow").await;
        assert_eq!(rec.working_paths.len(), 8);

        drop(w);
        drop(reader);
        h.shutdown().await;
    });
}
