//! Acceptance test for AC1 (MUST).
//!
//! Project: agorabus (cli)
//! AC description: Given the evidence below, When the root cause is fixed,
//! Then `repo-health.sh compute` no longer lists `main-ci-red` among
//! `agorabus`'s alarms on the next tick.
//!
//! `repo-health.sh` itself lives outside this repo (build-skill) and its
//! verdict depends on >=7 days of live GitHub Actions history — neither is
//! assertable inside `cargo test`. What IS assertable, and is the actual
//! mechanism diagnosed as the root cause (PRD-agorabus-health-main-ci-red-
//! 20260918's Evidence: `failing_job=autobuilder gate`), is that
//! `scripts/audit.sh` — the CI job step that was failing — exits 0 with no
//! BLOCKING findings. This test asserts that necessary condition as the
//! nearest in-repo, deterministic, CI-runnable proxy for the AC above; it is
//! not sufficient on its own to prove repo-health.sh's verdict (a separate,
//! CI-step-red or 7-day-window cause could still exist), so it deliberately
//! does not claim to.
//! Test predicate: tests/acceptance_ac1.rs

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::Command;

#[test]
fn acceptance_ac1() {
    let repo_root = env!("CARGO_MANIFEST_DIR");
    let out = Command::new("bash")
        .arg("scripts/audit.sh")
        .current_dir(repo_root)
        .output()
        .expect("scripts/audit.sh should run");

    assert!(
        out.status.success(),
        "scripts/audit.sh exited {:?} (blocking bad-rust-audit findings present); \
         this is the gate that was causing main-ci-red — stdout={} stderr={}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
