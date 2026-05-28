//! CLI-level acceptance tests for PRD-chord-intent-rich ACs 3, 4, 6.
//!
//! These exercise the `agorabus intent {set,list}` subcommands through the
//! built binary (CARGO_BIN_EXE_agorabus), not just the daemon API.
//!
//! - AC3: `intent set --session-id sid-X --skill /build` writes a single
//!   heartbeat carrying `skill="/build"` and leaves `tool` unset; a
//!   subsequent `peers` shows `skill` populated for sid-X.
//! - AC4: with three peers where two have intent set, `intent list` returns
//!   exactly those two, each projected to `session_id` + the set intent
//!   fields only (no pid/cwd/last_tool). The third peer is omitted.
//! - AC6: fail-open — `intent list` with no daemon exits 0 and emits `[]`;
//!   `intent set` with no daemon exits 0 silently.
//!
//! `intent set` is one-shot (connect → announce → heartbeat → disconnect).
//! Because the daemon removes a peer record when its *owning* connection
//! closes, the subject session must already be held open by a long-lived
//! owner connection — the real-world case (a session is on the bus; `intent
//! set` updates it as a guest). The tests hold those owner Clients in scope.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use std::path::PathBuf;
use std::process::Command;

use agorabus::Client;
use common::DaemonHandle;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_agorabus"))
}

/// Open a long-lived owner connection for `sid` and keep it alive by
/// returning the Client (caller holds it in scope).
async fn own(socket: &std::path::Path, sid: &str) -> Client {
    let mut c = Client::connect(socket).await.unwrap();
    c.announce(sid, 4242, "/tmp", "owner").await.unwrap();
    c
}

#[test]
fn ac3_intent_set_populates_skill_and_leaves_tool_unset() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        // Long-lived owner for sid-X so the record survives the one-shot
        // `intent set` guest connection.
        let _owner = own(&h.socket, "sid-X").await;

        // intent set --session-id sid-X --skill /build
        let out = Command::new(bin())
            .args([
                "--socket",
                h.socket.to_str().unwrap(),
                "intent",
                "set",
                "--session-id",
                "sid-X",
                "--skill",
                "/build",
            ])
            .output()
            .expect("spawn intent set");
        assert!(
            out.status.success(),
            "intent set non-zero: stderr={:?}",
            String::from_utf8_lossy(&out.stderr)
        );
        let reply: serde_json::Value =
            serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim())
                .expect("intent set emits a JSON reply");
        assert_eq!(reply.get("ok").and_then(serde_json::Value::as_bool), Some(true));

        // peers must show skill populated for sid-X and last_tool still empty.
        let mut q = Client::connect(&h.socket).await.unwrap();
        q.announce("ac3-q", 7, "/tmp", "q").await.unwrap();
        let peers = q.peers().await.unwrap();
        let rec = peers
            .into_iter()
            .find(|p| p.session_id == "sid-X")
            .expect("sid-X present");
        assert_eq!(rec.skill, "/build", "skill should be set by intent set");
        assert_eq!(rec.last_tool, "", "tool must be left unset (AC3)");
        assert_eq!(rec.prd_slug, "", "prd_slug not set");
        assert!(rec.working_paths.is_empty(), "working_paths not set");

        h.shutdown().await;
    });
}

#[test]
fn ac4_intent_list_filters_and_projects() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;

        // Three live peers; only A and B will get intent.
        let _a = own(&h.socket, "sid-A").await;
        let _b = own(&h.socket, "sid-B").await;
        let _c = own(&h.socket, "sid-C").await;

        // A gets a skill; B gets a prd + paths; C stays bare.
        let set = |args: &[&str]| {
            let mut full = vec!["--socket", h.socket.to_str().unwrap(), "intent", "set"];
            full.extend_from_slice(args);
            let out = Command::new(bin()).args(&full).output().expect("spawn set");
            assert!(out.status.success(), "set failed: {args:?}");
        };
        set(&["--session-id", "sid-A", "--skill", "/dream"]);
        set(&[
            "--session-id",
            "sid-B",
            "--prd",
            "recall-daemon",
            "--paths",
            "~/wintermute/recall,~/wintermute/autobuilder",
        ]);

        // intent list
        let out = Command::new(bin())
            .args([
                "--socket",
                h.socket.to_str().unwrap(),
                "intent",
                "list",
            ])
            .output()
            .expect("spawn intent list");
        assert!(out.status.success(), "intent list non-zero");
        let listed: Vec<serde_json::Value> =
            serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim())
                .expect("intent list emits a JSON array");

        // Exactly two peers (A and B); C omitted; the throwaway query id absent.
        assert_eq!(listed.len(), 2, "expected exactly A and B, got {listed:?}");
        let by_sid = |sid: &str| {
            listed
                .iter()
                .find(|v| v.get("session_id").and_then(serde_json::Value::as_str) == Some(sid))
                .unwrap_or_else(|| panic!("{sid} missing from {listed:?}"))
        };

        let a = by_sid("sid-A");
        assert_eq!(a.get("skill").and_then(serde_json::Value::as_str), Some("/dream"));
        // Projection: only session_id + set intent fields. No pid/cwd/last_tool,
        // and no prd_slug/working_paths for A (unset).
        let a_keys: Vec<&String> = a.as_object().unwrap().keys().collect();
        assert_eq!(a_keys.len(), 2, "A should have session_id+skill only: {a}");
        assert!(a.get("pid").is_none() && a.get("cwd").is_none(), "no pid/cwd leak: {a}");

        let b = by_sid("sid-B");
        assert_eq!(
            b.get("prd_slug").and_then(serde_json::Value::as_str),
            Some("recall-daemon")
        );
        let b_paths = b.get("working_paths").and_then(serde_json::Value::as_array).unwrap();
        assert_eq!(b_paths.len(), 2, "B working_paths: {b}");
        assert!(b.get("skill").is_none(), "B has no skill set: {b}");
        assert!(by_sid_opt(&listed, "sid-C").is_none(), "C must be omitted");

        h.shutdown().await;
    });
}

fn by_sid_opt<'a>(listed: &'a [serde_json::Value], sid: &str) -> Option<&'a serde_json::Value> {
    listed
        .iter()
        .find(|v| v.get("session_id").and_then(serde_json::Value::as_str) == Some(sid))
}

#[test]
fn ac6_intent_failopen_no_daemon() {
    let tmp = tempfile::tempdir().unwrap();
    let bogus = tmp.path().join("nope.sock");
    assert!(!bogus.exists());

    // intent list: exit 0, stdout == "[]".
    let out = Command::new(bin())
        .args(["--socket", bogus.to_str().unwrap(), "intent", "list"])
        .output()
        .expect("spawn list");
    assert!(out.status.success(), "list exit non-zero on missing daemon");
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "[]");

    // intent set: exit 0, stdout empty (silent).
    let out = Command::new(bin())
        .args([
            "--socket",
            bogus.to_str().unwrap(),
            "intent",
            "set",
            "--session-id",
            "sid-X",
            "--skill",
            "/build",
        ])
        .output()
        .expect("spawn set");
    assert!(out.status.success(), "set exit non-zero on missing daemon");
    assert!(
        out.stdout.is_empty(),
        "set stdout should be empty on fail-open, got {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
}
