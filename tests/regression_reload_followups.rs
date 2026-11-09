//! Regression tests for the A1472 follow-ups (A1475, A1476, A1479 items 1-3).
//!
//! Every case uses a private `HOME`, `XDG_RUNTIME_DIR` and socket inside its
//! own temp dir, and kills only the pids it started.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use agorabus::doctor::find_daemon_pid_for;
use agorabus::reload::{
    check_pid_cmdline, recheck_daemon_pid,
    Bounce, ReloadConfig, ReloadFormat, ReloadStatus, run_reload, snapshot_peers_within,
    start_daemon_if_lock_free,
};

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_agorabus"))
}

/// Per-case sandbox: private HOME / `XDG_RUNTIME_DIR` and socket path.
struct Sandbox {
    tmp: tempfile::TempDir,
    socket: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("run/sock");
        for d in ["home", "xdg", "run"] {
            std::fs::create_dir_all(tmp.path().join(d)).unwrap();
        }
        Self { tmp, socket }
    }

    fn lock(&self) -> PathBuf {
        self.tmp.path().join("run/sock.lock")
    }

    fn daemon_args(&self) -> Vec<String> {
        let p = |n: &str| self.tmp.path().join(n).to_string_lossy().into_owned();
        vec![
            "--state-file".into(),
            p("state.json"),
            "--uplink-config".into(),
            p("no-uplink.toml"),
            "--drain-grace-ms".into(),
            "50".into(),
        ]
    }

    fn cmd(&self) -> Command {
        let mut cmd = Command::new(bin());
        cmd.env("HOME", self.tmp.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.tmp.path().join("xdg"))
            .arg("--socket")
            .arg(&self.socket);
        cmd
    }

    /// Daemon with the global flag after the subcommand:
    /// `agorabus daemon --socket X ...`.
    fn daemon_cmd_flag_after(&self) -> Command {
        let mut cmd = Command::new(bin());
        cmd.env("HOME", self.tmp.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.tmp.path().join("xdg"))
            .arg("daemon")
            .arg("--socket")
            .arg(&self.socket)
            .args(self.daemon_args())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        cmd
    }

    fn daemon_cmd(&self) -> Command {
        let mut cmd = self.cmd();
        cmd.arg("daemon")
            .args(self.daemon_args())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        cmd
    }
}

/// Pids this test started; each is killed by pid on drop.
#[derive(Default)]
struct Pids(Vec<u32>);

impl Pids {
    fn track(&mut self, pid: u32) {
        self.0.extend([pid]);
    }
}

impl Drop for Pids {
    fn drop(&mut self) {
        for pid in &self.0 {
            let _ = Command::new("kill")
                .arg("-KILL")
                .arg(pid.to_string())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn spawn_tracked(cmd: &mut Command, pids: &mut Pids) -> u32 {
    let mut child = cmd.spawn().unwrap();
    let pid = child.id();
    pids.track(pid);
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    pid
}

fn wait_socket(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::os::unix::net::UnixStream::connect(socket).is_err() {
        assert!(Instant::now() < deadline, "no socket {}", socket.display());
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn pids_mentioning(needle: &str) -> Vec<u32> {
    let mut out = Vec::new();
    for e in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        if String::from_utf8_lossy(&raw).contains(needle) && pid != std::process::id() {
            out.extend([pid]);
        }
    }
    out
}

fn config(sb: &Sandbox) -> ReloadConfig {
    ReloadConfig {
        socket_path: sb.socket.clone(),
        require_fresh: false,
        start_if_absent: true,
        dry_run: false,
        drain_timeout_ms: 500,
        reconnect_timeout_ms: 1_000,
        format: ReloadFormat::Json,
        installed_path: None,
        build: false,
        cloudbuild_path: None,
    }
}

/// A1475: two daemons on private sockets; each socket resolves to its own
/// daemon, not to the smallest pid.
#[test]
fn discovery_picks_the_daemon_serving_the_socket() {
    let a = Sandbox::new();
    let b = Sandbox::new();
    let mut pids = Pids::default();
    let pa = spawn_tracked(&mut a.daemon_cmd(), &mut pids);
    let pb = spawn_tracked(&mut b.daemon_cmd(), &mut pids);
    wait_socket(&a.socket);
    wait_socket(&b.socket);
    assert_eq!(find_daemon_pid_for(&a.socket), Some(pa));
    assert_eq!(find_daemon_pid_for(&b.socket), Some(pb));
    assert_eq!(find_daemon_pid_for(&a.tmp.path().join("run/other")), None);
}

/// A1476: a held owner lock with no discovered daemon launches nothing.
#[test]
fn start_if_absent_with_held_lock_does_not_launch() {
    let sb = Sandbox::new();
    let held = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(sb.lock())
        .unwrap();
    rustix::fs::flock(&held, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();

    let mut pids = Pids::default();
    let out = start_daemon_if_lock_free(
        &sb.socket,
        Duration::from_millis(400),
        &bin(),
        &sb.daemon_args(),
    );
    if let Bounce::Launched(pid) = out {
        pids.track(pid);
    }
    assert_eq!(out, Bounce::LockHeld);
    std::thread::sleep(Duration::from_millis(300));
    let sock = sb.socket.to_string_lossy().into_owned();
    assert!(pids_mentioning(&sock).is_empty(), "nothing launched");
}

/// A1479 item 1: `--apply` performs the reload; without it the run is a plan.
#[test]
fn reload_apply_flag_is_not_a_dry_run() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();

    let plan = sb
        .cmd()
        .args(["reload", "--start-if-absent", "--no-require-fresh", "--format", "json"])
        .output()
        .unwrap();
    let plan: serde_json::Value = serde_json::from_slice(&plan.stdout).unwrap();
    assert!(plan["new_pid"].is_null(), "default run is a plan: {plan}");
    assert!(std::os::unix::net::UnixStream::connect(&sb.socket).is_err());

    let out = sb
        .cmd()
        .args([
            "reload",
            "--start-if-absent",
            "--apply",
            "--no-require-fresh",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!("json: {e}; stderr: {}", String::from_utf8_lossy(&out.stderr))
    });
    if let Some(pid) = v["new_pid"].as_u64() {
        pids.track(u32::try_from(pid).unwrap());
    }
    assert!(v["new_pid"].is_u64(), "apply launched a daemon: {v}");
    assert_eq!(v["status"], "reloaded");
    wait_socket(&sb.socket);
}

/// A1479 item 2: a peer that accepts but never answers cannot hang the
/// snapshot, directly or through a (dry-run) reload.
#[test]
fn snapshot_against_silent_peer_returns_within_bound() {
    let sb = Sandbox::new();
    let _silent = std::os::unix::net::UnixListener::bind(&sb.socket).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let t0 = Instant::now();
    let peers = rt
        .block_on(snapshot_peers_within(&sb.socket, Duration::from_millis(300)))
        .unwrap();
    assert!(peers.is_empty());
    assert!(t0.elapsed() < Duration::from_secs(3), "{:?}", t0.elapsed());

    let mut cfg = config(&sb);
    cfg.dry_run = true;
    let t0 = Instant::now();
    let done = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), run_reload(&cfg)).await
    });
    assert!(done.is_ok(), "dry-run reload hung on a silent peer");
    assert!(t0.elapsed() < Duration::from_secs(10), "{:?}", t0.elapsed());
}

/// A1479 item 3: the no-peers path is `Reloaded` only if the new daemon
/// answers on its socket. The launched process here is this test binary, which
/// exits at once without binding.
#[test]
fn no_peers_reload_with_unbound_daemon_is_not_reloaded() {
    let sb = Sandbox::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (verdict, _code) = rt.block_on(run_reload(&config(&sb))).unwrap();
    if let Some(pid) = verdict.new_pid {
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(pid.to_string())
            .stderr(Stdio::null())
            .status();
    }
    assert_eq!(verdict.status, ReloadStatus::Failed, "{verdict:?}");
}

/// A1475 gate correction: `agorabus daemon --socket X` (flag after the
/// subcommand) is attributed to X, never to the default socket.
#[test]
fn discovery_reads_socket_flag_after_daemon() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let pid = spawn_tracked(&mut sb.daemon_cmd_flag_after(), &mut pids);
    wait_socket(&sb.socket);
    assert_eq!(find_daemon_pid_for(&sb.socket), Some(pid));
    assert_ne!(find_daemon_pid_for(&agorabus::default_socket_path()), Some(pid));
}

/// A1475 gate correction: the pre-signal re-check spares a pid that now serves
/// another socket, is not a daemon, or is gone.
#[test]
fn recheck_refuses_pid_serving_other_socket() {
    let a = Sandbox::new();
    let other = a.tmp.path().join("run/other");
    let mut pids = Pids::default();
    let pid = spawn_tracked(&mut a.daemon_cmd_flag_after(), &mut pids);
    wait_socket(&a.socket);
    assert!(recheck_daemon_pid(pid, &a.socket).is_ok());
    let err = recheck_daemon_pid(pid, &other).unwrap_err();
    assert!(err.contains(&pid.to_string()), "{err}");

    let sleeper = spawn_tracked(Command::new("sleep").arg("30"), &mut pids);
    assert!(recheck_daemon_pid(sleeper, &a.socket).unwrap_err().contains(&sleeper.to_string()));
}

#[test]
fn check_pid_cmdline_cases() {
    let sock = Path::new("/p/x/sock");
    assert!(check_pid_cmdline(7, sock, Some(b"agorabus\0daemon\0--socket\0/p/x/sock\0")).is_ok());
    assert!(check_pid_cmdline(7, sock, Some(b"agorabus\0--socket=/p/x/sock\0daemon\0")).is_ok());
    let e = check_pid_cmdline(7, sock, Some(b"agorabus\0daemon\0--socket\0/p/y/sock\0")).unwrap_err();
    assert!(e.contains("pid 7"), "{e}");
    assert!(check_pid_cmdline(7, sock, Some(b"sleep\030\0")).is_err());
    assert!(check_pid_cmdline(7, sock, None).unwrap_err().contains("pid 7"));
}
