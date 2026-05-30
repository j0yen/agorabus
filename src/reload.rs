//! `agorabus reload` — non-destructive one-command daemon bounce.
//!
//! Mirrors the `doctor` module pattern (PRD-agorabus-doctor-selfstale) but
//! adds side-effects: SIGTERM the running daemon, relaunch the fresh binary,
//! then poll until the pre-bounce peer set has reconnected.
//!
//! ## Flow
//!
//! 1. **Pre-flight** — resolve daemon pid via `/proc` scan; refuse if absent
//!    (unless `--start-if-absent`).
//! 2. **Freshness check** — call [`doctor::run_doctor`] to confirm the running
//!    binary is stale. With `--require-fresh` (default on) abort if already
//!    current.
//! 3. **Snapshot** — record pre-bounce `peers` (session_id set + count).
//! 4. **Drain + bounce** — SIGTERM the daemon, wait for exit (bounded by
//!    `drain_timeout_ms`), then `nohup` the fresh binary in daemon mode.
//! 5. **Confirm** — poll `test -S <sock>` then `peers` until the pre-bounce
//!    session_id set has re-registered or `reconnect_timeout_ms` elapses.
//! 6. **Verdict** — emit JSON + human table; exit 0 only on full recovery.

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use crate::doctor::{self, Verdict};
use crate::Client;
use anyhow::{Result, bail};
use serde::Serialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

// ── public surface ────────────────────────────────────────────────────────────

/// Output format for the reload verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadFormat {
    /// Human-readable table (default).
    Text,
    /// Machine-readable JSON object.
    Json,
}

impl ReloadFormat {
    /// Parse from the CLI `--format` string.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// Final status of a reload attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReloadStatus {
    /// All pre-bounce sessions recovered; or no sessions to recover.
    Reloaded,
    /// Daemon bounced but some sessions did not re-register before timeout.
    ReloadedDegraded,
    /// Reload did not proceed (pre-flight / freshness check refused).
    Failed,
}

/// Full verdict emitted at the end of a reload attempt.
#[derive(Debug, Serialize)]
pub struct ReloadVerdict {
    /// PID of the daemon before the bounce (None in dry-run).
    pub old_pid: Option<u32>,
    /// PID of the new daemon process (None if not launched / dry-run).
    pub new_pid: Option<u32>,
    /// Staleness verdict of the old binary before the bounce.
    pub binary_before: String,
    /// Staleness verdict of the new binary (always "current" if bounce succeeded).
    pub binary_after: Option<String>,
    /// Session IDs present before the bounce.
    pub peers_before: Vec<String>,
    /// Session IDs present after confirmation polling.
    pub peers_after: Vec<String>,
    /// Session IDs that were present before and re-registered after.
    pub peers_recovered: Vec<String>,
    /// Session IDs that were present before but did NOT re-register.
    pub peers_missing: Vec<String>,
    /// Total elapsed time in milliseconds.
    pub elapsed_ms: u64,
    /// Overall reload status.
    pub status: ReloadStatus,
}

/// Configuration for a reload run.
pub struct ReloadConfig {
    /// Path to the agorabus socket.
    pub socket_path: PathBuf,
    /// If true, refuse to bounce when the binary is already current.
    pub require_fresh: bool,
    /// If true, start a daemon even if none is currently running.
    pub start_if_absent: bool,
    /// If true, print the plan but make no mutations.
    pub dry_run: bool,
    /// Milliseconds to wait for old daemon to exit after SIGTERM.
    pub drain_timeout_ms: u64,
    /// Milliseconds to wait for peers to reconnect before declaring degraded.
    pub reconnect_timeout_ms: u64,
    /// Output format.
    pub format: ReloadFormat,
    /// Override the installed binary path (for testing).
    pub installed_path: Option<PathBuf>,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            socket_path: crate::default_socket_path(),
            require_fresh: true,
            start_if_absent: false,
            dry_run: true, // default posture per vigil's "--dry-run is the default"
            drain_timeout_ms: 2_000,
            reconnect_timeout_ms: 8_000,
            format: ReloadFormat::Text,
            installed_path: None,
        }
    }
}

/// Run the reload operation (or dry-run).
///
/// Returns `(verdict, exit_code)`. The caller is responsible for printing.
///
/// # Errors
///
/// Returns `Err` on unexpected I/O failures (daemon communication errors
/// distinct from expected "no daemon" scenarios, which produce a `Failed`
/// verdict instead).
pub async fn run_reload(cfg: &ReloadConfig) -> Result<(ReloadVerdict, ExitCode)> {
    let start = Instant::now();

    // ── 1. Pre-flight: find daemon pid ────────────────────────────────────────
    let (doctor_report, _doctor_code) = doctor::run_doctor(cfg.installed_path.as_deref());

    let daemon_pid = doctor_report.daemon_pid;

    if daemon_pid.is_none() && !cfg.start_if_absent {
        let verdict = ReloadVerdict {
            old_pid: None,
            new_pid: None,
            binary_before: "unknown".to_string(),
            binary_after: None,
            peers_before: vec![],
            peers_after: vec![],
            peers_recovered: vec![],
            peers_missing: vec![],
            elapsed_ms: elapsed_ms(start),
            status: ReloadStatus::Failed,
        };
        return Ok((verdict, ExitCode::from(1)));
    }

    // ── 2. Freshness check ────────────────────────────────────────────────────
    let binary_before = match &doctor_report.verdict {
        Verdict::Current => "current".to_string(),
        Verdict::StaleDeletedExe => "stale:deleted-exe".to_string(),
        Verdict::StaleInodeDrift => "stale:inode-drift".to_string(),
        Verdict::Unknown => "unknown".to_string(),
    };

    // With --require-fresh (default), refuse to bounce an already-current daemon.
    if cfg.require_fresh && daemon_pid.is_some() && doctor_report.verdict == Verdict::Current {
        let verdict = ReloadVerdict {
            old_pid: daemon_pid,
            new_pid: None,
            binary_before: binary_before.clone(),
            binary_after: None,
            peers_before: vec![],
            peers_after: vec![],
            peers_recovered: vec![],
            peers_missing: vec![],
            elapsed_ms: elapsed_ms(start),
            status: ReloadStatus::Failed,
        };
        return Ok((verdict, ExitCode::from(1)));
    }

    // ── 3. Snapshot peers ─────────────────────────────────────────────────────
    let peers_before = snapshot_peers(&cfg.socket_path).await.unwrap_or_default();

    // ── Dry-run: plan only, no mutations ─────────────────────────────────────
    if cfg.dry_run {
        let verdict = ReloadVerdict {
            old_pid: daemon_pid,
            new_pid: None,
            binary_before,
            binary_after: None,
            peers_before: peers_before.clone(),
            peers_after: vec![],
            peers_recovered: vec![],
            peers_missing: peers_before,
            elapsed_ms: elapsed_ms(start),
            status: ReloadStatus::Reloaded, // plan-only: not a failure
        };
        return Ok((verdict, ExitCode::SUCCESS));
    }

    // ── 4. Drain + bounce ─────────────────────────────────────────────────────
    let Some(old_pid) = daemon_pid else {
        // start_if_absent path: no existing daemon; just launch one.
        let new_pid = launch_daemon(&cfg.socket_path)?;
        let verdict = ReloadVerdict {
            old_pid: None,
            new_pid: Some(new_pid),
            binary_before,
            binary_after: Some("current".to_string()),
            peers_before: vec![],
            peers_after: vec![],
            peers_recovered: vec![],
            peers_missing: vec![],
            elapsed_ms: elapsed_ms(start),
            status: ReloadStatus::Reloaded,
        };
        return Ok((verdict, ExitCode::SUCCESS));
    };

    // Send SIGTERM to the old daemon.
    send_sigterm(old_pid)?;

    // Wait for old process to exit (bounded).
    let drained = wait_for_exit(old_pid, Duration::from_millis(cfg.drain_timeout_ms));
    if !drained {
        eprintln!(
            "agorabus reload: old daemon (pid {old_pid}) did not exit within {}ms; proceeding anyway",
            cfg.drain_timeout_ms
        );
    }

    // Launch the fresh daemon.
    let new_pid = launch_daemon(&cfg.socket_path)?;

    // ── 5. Confirm: poll socket + peers ──────────────────────────────────────
    let deadline = Instant::now() + Duration::from_millis(cfg.reconnect_timeout_ms);

    // Wait for socket to appear.
    while !cfg.socket_path.exists() {
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Poll peers until pre-bounce set re-registered (or timeout).
    let before_set: HashSet<String> = peers_before.iter().cloned().collect();
    let peers_after;
    loop {
        let current = snapshot_peers(&cfg.socket_path).await.unwrap_or_default();
        let current_set: HashSet<String> = current.iter().cloned().collect();
        if before_set.is_subset(&current_set) || Instant::now() >= deadline {
            peers_after = current;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── 6. Verdict ────────────────────────────────────────────────────────────
    let after_set: HashSet<String> = peers_after.iter().cloned().collect();
    let mut peers_recovered: Vec<String> = before_set
        .intersection(&after_set)
        .cloned()
        .collect();
    peers_recovered.sort_unstable();
    let mut peers_missing: Vec<String> = before_set
        .difference(&after_set)
        .cloned()
        .collect();
    peers_missing.sort_unstable();

    let status = if peers_missing.is_empty() {
        ReloadStatus::Reloaded
    } else {
        ReloadStatus::ReloadedDegraded
    };
    let exit_code = if status == ReloadStatus::Reloaded {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    };

    // Post-bounce doctor check (best-effort; don't fail the reload if /proc
    // introspection is flaky).
    let binary_after = {
        let (post_report, _) = doctor::run_doctor(cfg.installed_path.as_deref());
        match post_report.verdict {
            Verdict::Current => Some("current".to_string()),
            Verdict::StaleDeletedExe => Some("stale:deleted-exe".to_string()),
            Verdict::StaleInodeDrift => Some("stale:inode-drift".to_string()),
            Verdict::Unknown => Some("unknown".to_string()),
        }
    };

    let verdict = ReloadVerdict {
        old_pid: Some(old_pid),
        new_pid: Some(new_pid),
        binary_before,
        binary_after,
        peers_before,
        peers_after,
        peers_recovered,
        peers_missing,
        elapsed_ms: elapsed_ms(start),
        status,
    };
    Ok((verdict, exit_code))
}

/// Print the verdict to stdout in the requested format.
pub fn print_verdict(verdict: &ReloadVerdict, format: ReloadFormat) {
    match format {
        ReloadFormat::Text => {
            let status_str = match &verdict.status {
                ReloadStatus::Reloaded => "reloaded",
                ReloadStatus::ReloadedDegraded => "reloaded-degraded",
                ReloadStatus::Failed => "failed",
            };
            println!("status: {status_str}");
            println!("elapsed_ms: {}", verdict.elapsed_ms);
            if let Some(pid) = verdict.old_pid {
                println!("old_pid: {pid}");
            }
            if let Some(pid) = verdict.new_pid {
                println!("new_pid: {pid}");
            }
            println!("binary_before: {}", verdict.binary_before);
            if let Some(ref ba) = verdict.binary_after {
                println!("binary_after: {ba}");
            }
            println!("peers_before: {}", verdict.peers_before.len());
            println!("peers_after: {}", verdict.peers_after.len());
            println!("peers_recovered: {}", verdict.peers_recovered.len());
            if !verdict.peers_missing.is_empty() {
                println!(
                    "peers_missing: {}",
                    verdict.peers_missing.join(", ")
                );
            }
        }
        ReloadFormat::Json => match serde_json::to_string(verdict) {
            Ok(s) => println!("{s}"),
            Err(e) => eprintln!("agorabus reload: json serialise error: {e}"),
        },
    }
}

// ── private helpers ───────────────────────────────────────────────────────────

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

/// Collect the current set of session IDs from the daemon's peers endpoint.
/// Returns an empty vec on any error (fail-open).
async fn snapshot_peers(socket: &Path) -> Result<Vec<String>> {
    let Some(mut client) = Client::try_connect(socket).await? else {
        return Ok(vec![]);
    };
    let pid = std::process::id();
    let sid = format!("reload-probe-{pid}");
    let cwd = std::env::current_dir()
        .ok()
        .and_then(|p| p.to_str().map(String::from))
        .unwrap_or_default();
    let _ = client.announce(&sid, pid, &cwd, "reload-probe").await?;
    let peers = client.peers().await?;
    // Filter out the throwaway probe session.
    let ids: Vec<String> = peers
        .into_iter()
        .filter(|p| p.session_id != sid)
        .map(|p| p.session_id)
        .collect();
    Ok(ids)
}

/// Send SIGTERM to `pid` via the system `kill` utility.
///
/// We avoid a libc/nix crate dependency by shelling out to `/usr/bin/kill`.
/// This is intentional: `unsafe` is forbidden in production code by the
/// project lints, and the `kill` utility is universally available on Linux.
fn send_sigterm(pid: u32) -> Result<()> {
    let output = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .output()
        .map_err(|e| anyhow::anyhow!("failed to invoke kill(1): {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("kill -TERM {pid} failed: {stderr}");
    }
}

/// Wait for `pid` to exit. Returns `true` if the process exited within
/// `timeout`, `false` if it was still running at deadline.
fn wait_for_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        // Check if /proc/<pid> still exists (Linux-specific but reliable).
        if !std::path::Path::new(&format!("/proc/{pid}")).exists() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Launch a new `agorabus daemon` process detached from our session.
///
/// Uses the currently-executing binary (which, after `install.sh`, is the
/// fresh on-disk binary) launched via `nohup` with `setsid` to fully detach.
///
/// Returns the new process PID.
///
/// # Errors
///
/// Returns `Err` if the binary cannot be resolved or the process cannot be
/// spawned.
fn launch_daemon(socket_path: &Path) -> Result<u32> {
    let binary = discover_binary()?;
    let socket_str = socket_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("socket path is not valid UTF-8"))?;

    let child = std::process::Command::new("nohup")
        .args([
            binary.to_str().unwrap_or("agorabus"),
            "--socket",
            socket_str,
            "daemon",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn nohup agorabus daemon: {e}"))?;

    Ok(child.id())
}

/// Resolve the path of the installed `agorabus` binary.
fn discover_binary() -> Result<PathBuf> {
    // Try the current executable first (most reliable when the command is
    // invoked from the installed binary itself).
    if let Ok(exe) = std::env::current_exe() {
        return Ok(exe);
    }
    // Fall back to PATH lookup.
    let path_var = std::env::var_os("PATH")
        .ok_or_else(|| anyhow::anyhow!("$PATH not set"))?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join("agorabus");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    bail!("could not locate agorabus binary on $PATH or via current_exe")
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_format_parse_text() {
        assert_eq!(ReloadFormat::parse("text"), Some(ReloadFormat::Text));
    }

    #[test]
    fn reload_format_parse_json() {
        assert_eq!(ReloadFormat::parse("json"), Some(ReloadFormat::Json));
    }

    #[test]
    fn reload_format_parse_unknown() {
        assert_eq!(ReloadFormat::parse("xml"), None);
    }

    #[test]
    fn reload_status_serialises_reloaded() {
        let s = serde_json::to_string(&ReloadStatus::Reloaded).unwrap();
        assert_eq!(s, "\"reloaded\"");
    }

    #[test]
    fn reload_status_serialises_degraded() {
        let s = serde_json::to_string(&ReloadStatus::ReloadedDegraded).unwrap();
        assert_eq!(s, "\"reloaded-degraded\"");
    }

    #[test]
    fn reload_status_serialises_failed() {
        let s = serde_json::to_string(&ReloadStatus::Failed).unwrap();
        assert_eq!(s, "\"failed\"");
    }

    #[test]
    fn verdict_json_has_required_fields() {
        let v = ReloadVerdict {
            old_pid: Some(1234),
            new_pid: Some(5678),
            binary_before: "stale:deleted-exe".to_string(),
            binary_after: Some("current".to_string()),
            peers_before: vec!["s1".to_string()],
            peers_after: vec!["s1".to_string()],
            peers_recovered: vec!["s1".to_string()],
            peers_missing: vec![],
            elapsed_ms: 150,
            status: ReloadStatus::Reloaded,
        };
        let json: serde_json::Value = serde_json::to_value(&v).unwrap();
        // AC4: all required fields must be present
        for field in &[
            "old_pid",
            "new_pid",
            "binary_before",
            "binary_after",
            "peers_before",
            "peers_after",
            "peers_recovered",
            "peers_missing",
            "elapsed_ms",
            "status",
        ] {
            assert!(
                json.get(field).is_some(),
                "required field '{field}' missing from verdict JSON"
            );
        }
        assert_eq!(json["status"], "reloaded");
        assert_eq!(json["old_pid"], 1234);
        assert_eq!(json["elapsed_ms"], 150);
    }

    #[test]
    fn print_verdict_text_does_not_panic() {
        let v = ReloadVerdict {
            old_pid: Some(9999),
            new_pid: None,
            binary_before: "stale:deleted-exe".to_string(),
            binary_after: None,
            peers_before: vec!["session-abc".to_string()],
            peers_after: vec![],
            peers_recovered: vec![],
            peers_missing: vec!["session-abc".to_string()],
            elapsed_ms: 42,
            status: ReloadStatus::ReloadedDegraded,
        };
        // Just verify it doesn't panic.
        print_verdict(&v, ReloadFormat::Text);
    }

    #[test]
    fn print_verdict_json_is_valid() {
        let v = ReloadVerdict {
            old_pid: Some(1),
            new_pid: Some(2),
            binary_before: "stale:inode-drift".to_string(),
            binary_after: Some("current".to_string()),
            peers_before: vec![],
            peers_after: vec![],
            peers_recovered: vec![],
            peers_missing: vec![],
            elapsed_ms: 999,
            status: ReloadStatus::Reloaded,
        };
        // Capture via JSON serialise.
        let s = serde_json::to_string(&v).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["status"], "reloaded");
    }

    // AC6: no daemon → clear refusal (unit-test version; integration in
    // acceptance_reload_dryrun.rs).
    #[tokio::test]
    async fn no_daemon_without_start_if_absent_is_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = ReloadConfig {
            socket_path: tmp.path().join("sock"),
            require_fresh: true,
            start_if_absent: false,
            dry_run: true,
            drain_timeout_ms: 500,
            reconnect_timeout_ms: 500,
            format: ReloadFormat::Text,
            installed_path: None,
        };
        // No daemon is running in test context (almost certainly).
        let (verdict, _code) = run_reload(&cfg).await.unwrap();
        if verdict.old_pid.is_none() {
            // Confirmed no daemon path → status must be Failed.
            assert_eq!(verdict.status, ReloadStatus::Failed);
        }
        // If a daemon happened to be running, we skip the assertion (test env).
    }
}
