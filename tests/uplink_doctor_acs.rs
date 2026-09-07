//! Acceptance tests for PRD-agorabus-nats-uplink: `agorabus doctor` surfaces
//! the uplink connection state (AC3, AC4) without ever changing doctor's
//! exit code (AC11).
//!
//! These drive the real compiled `agorabus` binary as a subprocess (not the
//! in-process `Client`) so the CLI wiring in `main.rs` is exercised
//! end-to-end, pointed at an in-process test daemon via `--socket`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module
)]

mod common;

use agorabus::UplinkConfig;
use common::DaemonHandle;
use common::nats_harness::{NatsServer, nats_server_available, skip_no_nats_server};
use std::process::Command;
use std::time::Duration;

fn agorabus_bin() -> &'static str {
    env!("CARGO_BIN_EXE_agorabus")
}

fn run_doctor(socket: &std::path::Path) -> (String, i32) {
    let out = Command::new(agorabus_bin())
        .args(["--socket", socket.to_str().unwrap(), "doctor", "--format", "text"])
        .output()
        .expect("run agorabus doctor");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    (stdout, out.status.code().unwrap_or(-1))
}

/// AC3: uplink enabled + a reachable nats-server → doctor output includes
/// `uplink: connected` and the url.
#[test]
fn ac3_doctor_reports_connected() {
    if !nats_server_available() {
        skip_no_nats_server("ac3_doctor_reports_connected");
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let nats = NatsServer::spawn().await;
        let uplink = UplinkConfig {
            enabled: true,
            url: nats.url(),
            node: "doctortest".to_string(),
            fleet_presence_interval_ms: 60_000,
        };
        let daemon = DaemonHandle::start_with_uplink(uplink).await;

        // Poll doctor until it reports connected (background reconnect takes
        // a beat to complete the handshake).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (stdout, _code) = run_doctor(&daemon.socket);
            if stdout.contains("uplink: connected") {
                assert!(
                    stdout.contains(&nats.url()),
                    "doctor output should include the leaf url: {stdout}"
                );
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("doctor never reported connected; last output:\n{stdout}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        daemon.shutdown().await;
    });
}

/// AC4: uplink enabled but no nats-server listening → the daemon starts and
/// serves the local bus normally; doctor reports `uplink: reconnecting`; no
/// panic, no exit.
#[test]
fn ac4_doctor_reports_reconnecting_when_leaf_absent() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // A URL that (almost certainly) nothing is listening on.
        let uplink = UplinkConfig {
            enabled: true,
            url: "nats://127.0.0.1:4".to_string(), // port 4 is not a NATS port
            node: "doctortest2".to_string(),
            fleet_presence_interval_ms: 60_000,
        };
        let daemon = DaemonHandle::start_with_uplink(uplink).await;

        // Give the background connector a moment to fail and report.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (stdout, _code) = run_doctor(&daemon.socket);
        assert!(
            stdout.contains("uplink: reconnecting"),
            "expected reconnecting state, got: {stdout}"
        );

        // The local bus must still be usable — announce + publish succeeds.
        let mut client = agorabus::Client::connect(&daemon.socket).await.unwrap();
        let reply = client.announce("ac4-local", 1, "/tmp", "test").await.unwrap();
        assert!(reply.ok, "local bus must stay usable while uplink is down");

        daemon.shutdown().await;
    });
}

/// AC11: doctor's exit code is unchanged across every uplink state
/// (disabled / connected / reconnecting) — uplink lines are informational
/// only, never affect the staleness verdict's exit code.
#[test]
fn ac11_exit_code_unaffected_by_uplink_state() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Disabled.
        let disabled = DaemonHandle::start_with_uplink(UplinkConfig::default()).await;
        let (stdout_disabled, code_disabled) = run_doctor(&disabled.socket);
        assert!(stdout_disabled.contains("uplink: disabled"));

        // Reconnecting (no leaf).
        let uplink = UplinkConfig {
            enabled: true,
            url: "nats://127.0.0.1:4".to_string(),
            node: "doctortest3".to_string(),
            fleet_presence_interval_ms: 60_000,
        };
        let reconnecting = DaemonHandle::start_with_uplink(uplink).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (stdout_reconnecting, code_reconnecting) = run_doctor(&reconnecting.socket);
        assert!(stdout_reconnecting.contains("uplink: reconnecting"));

        // Same daemon-discovery path (no separate `agorabus daemon` OS
        // process backs either in-process test daemon), so the staleness
        // verdict — and therefore the exit code — must be identical
        // regardless of the uplink line printed above it.
        assert_eq!(
            code_disabled, code_reconnecting,
            "doctor exit code must not depend on uplink state"
        );

        disabled.shutdown().await;
        reconnecting.shutdown().await;
    });
}
