//! Acceptance tests for PRD-chord-claim ACs 8-9: CLI-level surface.
//!
//! AC8 (fail-open with no daemon) and AC9 (--wait flag, client-side) both
//! exercise the binary's run_claim path, not just the daemon. Spawn the
//! binary via CARGO_BIN_EXE_agorabus and observe stdout + exit code.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::DaemonHandle;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_agorabus"))
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[test]
fn claim_ac8_failopen_no_daemon() {
    // Point at a socket path that does NOT exist.
    let tmp = tempfile::tempdir().unwrap();
    let bogus_sock = tmp.path().join("nope.sock");
    assert!(
        !bogus_sock.exists(),
        "fixture: bogus socket must not exist"
    );

    // acquire: exit 0, no JSON on stdout (per PRD §AC8).
    let out = Command::new(bin())
        .args([
            "--socket",
            bogus_sock.to_str().unwrap(),
            "claim",
            "acquire",
            "--session-id",
            "sid-A",
            "/tmp/ac8-target",
            "--ttl",
            "5",
        ])
        .output()
        .expect("spawn acquire");
    assert!(
        out.status.success(),
        "acquire exit non-zero on missing daemon: status={:?} stderr={:?}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.is_empty(),
        "acquire stdout should be empty on fail-open, got {:?}",
        String::from_utf8_lossy(&out.stdout)
    );

    // release: same — exit 0, no JSON on stdout.
    let out = Command::new(bin())
        .args([
            "--socket",
            bogus_sock.to_str().unwrap(),
            "claim",
            "release",
            "--session-id",
            "sid-A",
            "/tmp/ac8-target",
        ])
        .output()
        .expect("spawn release");
    assert!(out.status.success(), "release exit non-zero on missing daemon");
    assert!(
        out.stdout.is_empty(),
        "release stdout should be empty on fail-open, got {:?}",
        String::from_utf8_lossy(&out.stdout)
    );

    // list: exit 0, stdout == "[]\n" (matches peers fail-open).
    let out = Command::new(bin())
        .args([
            "--socket",
            bogus_sock.to_str().unwrap(),
            "claim",
            "list",
        ])
        .output()
        .expect("spawn list");
    assert!(out.status.success(), "list exit non-zero on missing daemon");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(stdout.trim(), "[]", "list stdout = {stdout:?}");
}

#[test]
fn claim_ac9_wait_either_succeeds_or_times_out() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let h = DaemonHandle::start().await;
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("ac9-target");
        std::fs::write(&target, "").unwrap();
        let canon = std::fs::canonicalize(&target).unwrap();

        // sid-A acquires via the CLI (5-second TTL) and holds.
        let ttl = 5;
        let out = Command::new(bin())
            .args([
                "--socket",
                h.socket.to_str().unwrap(),
                "claim",
                "acquire",
                "--session-id",
                "sid-A",
                canon.to_str().unwrap(),
                "--ttl",
                &ttl.to_string(),
                "--reason",
                "ac9-holder",
            ])
            .output()
            .expect("spawn A acquire");
        assert!(out.status.success(), "sid-A acquire failed: {:?}", out);

        // sid-B tries acquire with --wait 2; sid-A doesn't release.
        // Expectation: command exits non-zero (claim_conflict) with timed_out:true
        // in the conflict detail. This is the conservative branch of AC9.
        let started = std::time::Instant::now();
        let out = Command::new(bin())
            .args([
                "--socket",
                h.socket.to_str().unwrap(),
                "claim",
                "acquire",
                "--session-id",
                "sid-B",
                canon.to_str().unwrap(),
                "--ttl",
                "60",
                "--wait",
                "2",
            ])
            .output()
            .expect("spawn B acquire --wait");
        let elapsed = started.elapsed();

        // The CLI exits 1 when reply.ok is false (claim_conflict). Stdout is
        // the JSON reply, including a `timed_out:true` flag in `data`.
        assert!(
            !out.status.success(),
            "B should not have succeeded while A holds — got {out:?}"
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let reply: serde_json::Value = serde_json::from_str(stdout.trim())
            .unwrap_or_else(|e| panic!("parse JSON reply: {e}; stdout={stdout:?}"));
        assert_eq!(
            reply.get("ok").and_then(|v| v.as_bool()),
            Some(false),
            "ok=false expected, reply={reply}"
        );
        assert_eq!(
            reply.get("error").and_then(|v| v.as_str()),
            Some("claim_conflict")
        );
        let timed_out = reply
            .get("data")
            .and_then(|d| d.get("timed_out"))
            .and_then(|v| v.as_bool());
        assert_eq!(
            timed_out,
            Some(true),
            "expected detail.timed_out=true; reply={reply}"
        );
        // Wait honored — must have blocked at least ~1.5s before bailing.
        assert!(
            elapsed >= Duration::from_millis(1500),
            "expected to block ~2s, elapsed {elapsed:?}"
        );

        // Companion path: sid-A releases mid-wait, sid-B's wait succeeds.
        // Re-acquire as sid-A (renewal).
        let ttl_unix = now_unix_secs() + 30;
        let out = Command::new(bin())
            .args([
                "--socket",
                h.socket.to_str().unwrap(),
                "claim",
                "acquire",
                "--session-id",
                "sid-A",
                canon.to_str().unwrap(),
                "--ttl",
                "30",
            ])
            .output()
            .expect("re-acquire A");
        assert!(out.status.success(), "renew failed: {:?}", out);
        let _ = ttl_unix; // documents intent; daemon picks its own from --ttl

        // Spawn sid-B's --wait 5 in the background, then release A after 500ms.
        let socket = h.socket.clone();
        let canon_str = canon.to_string_lossy().into_owned();
        let b_handle = std::thread::spawn(move || {
            Command::new(bin())
                .args([
                    "--socket",
                    socket.to_str().unwrap(),
                    "claim",
                    "acquire",
                    "--session-id",
                    "sid-B",
                    &canon_str,
                    "--ttl",
                    "60",
                    "--wait",
                    "5",
                ])
                .output()
                .expect("spawn B --wait 5")
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        let out = Command::new(bin())
            .args([
                "--socket",
                h.socket.to_str().unwrap(),
                "claim",
                "release",
                "--session-id",
                "sid-A",
                canon.to_str().unwrap(),
            ])
            .output()
            .expect("release A");
        assert!(out.status.success(), "A release failed: {:?}", out);

        let b_out = b_handle.join().expect("B thread join");
        assert!(
            b_out.status.success(),
            "B --wait should have acquired post-release: stdout={:?} stderr={:?}",
            String::from_utf8_lossy(&b_out.stdout),
            String::from_utf8_lossy(&b_out.stderr)
        );
        let b_stdout = String::from_utf8_lossy(&b_out.stdout);
        let b_reply: serde_json::Value = serde_json::from_str(b_stdout.trim())
            .unwrap_or_else(|e| panic!("parse B reply: {e}; stdout={b_stdout:?}"));
        assert_eq!(b_reply.get("ok").and_then(|v| v.as_bool()), Some(true));

        h.shutdown().await;
    });
}
