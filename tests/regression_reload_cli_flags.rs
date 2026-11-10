//! Regression tests for A1532 round 3: `agorabus reload --apply` and
//! `--no-require-fresh` must exist so the bounce path is reachable from the
//! CLI. Every case uses a private `HOME`, `XDG_RUNTIME_DIR` and socket inside
//! its own temp dir and kills only the pids it started.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::panic
)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use agorabus::Client;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_agorabus"))
}

struct Sandbox {
    tmp: tempfile::TempDir,
    socket: PathBuf,
    pids: Vec<u32>,
}

impl Sandbox {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        for d in ["home", "xdg", "run"] {
            std::fs::create_dir_all(tmp.path().join(d)).unwrap();
        }
        let socket = tmp.path().join("run/sock");
        Self {
            tmp,
            socket,
            pids: vec![],
        }
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(bin());
        c.env("HOME", self.tmp.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.tmp.path().join("xdg"))
            .arg("--socket")
            .arg(&self.socket);
        c
    }

    fn start_daemon(&mut self) -> u32 {
        let mut c = self.cmd();
        c.args(["daemon", "--drain-grace-ms", "50"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child: Child = c.spawn().unwrap();
        let pid = child.id();
        self.pids.push(pid);
        std::thread::spawn(move || {
            let _ = child.wait();
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while !answers(&self.socket) {
            assert!(Instant::now() < deadline, "daemon never answered");
            std::thread::sleep(Duration::from_millis(50));
        }
        pid
    }

    fn reload(&self, args: &[&str]) -> Output {
        self.cmd().arg("reload").args(args).output().unwrap()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        for pid in &self.pids {
            let _ = Command::new("kill")
                .arg("-KILL")
                .arg(pid.to_string())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn answers(socket: &Path) -> bool {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let attempt = async {
            let mut c = Client::connect(socket).await.ok()?;
            Some(c.announce("reload-flags", 1, "/tmp", "").await.ok()?.ok)
        };
        tokio::time::timeout(Duration::from_secs(2), attempt)
            .await
            .ok()
            .flatten()
            .unwrap_or(false)
    })
}

fn field(out: &Output, key: &str) -> Option<u32> {
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find_map(|l| l.strip_prefix(key)?.trim().parse().ok())
}

fn alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[test]
fn reload_without_apply_keeps_daemon_and_prints_plan() {
    let mut sb = Sandbox::new();
    let old = sb.start_daemon();
    let out = sb.reload(&["--no-require-fresh"]);
    assert!(out.status.success(), "{out:?}");
    assert_eq!(field(&out, "old_pid:"), Some(old), "{out:?}");
    assert_eq!(field(&out, "new_pid:"), None, "{out:?}");
    assert!(alive(old), "dry-run must leave the daemon running");
    assert!(answers(&sb.socket));
}

#[test]
fn reload_apply_replaces_daemon_and_new_pid_serves() {
    let mut sb = Sandbox::new();
    let old = sb.start_daemon();
    let out = sb.reload(&["--apply", "--no-require-fresh"]);
    assert!(out.status.success(), "{out:?}");
    let new = field(&out, "new_pid:").unwrap_or_else(|| panic!("no new_pid: {out:?}"));
    sb.pids.push(new);
    assert_ne!(new, old);
    assert!(!alive(old), "old daemon {old} should be gone");
    assert!(alive(new));
    assert!(answers(&sb.socket), "new daemon serves the scratch socket");
}

#[test]
fn apply_with_dry_run_is_a_usage_error() {
    let sb = Sandbox::new();
    let out = sb.reload(&["--apply", "--dry-run"]);
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("cannot be used with"),
        "{out:?}"
    );
}
