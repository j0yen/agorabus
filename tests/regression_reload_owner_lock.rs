//! Regression tests for A1472: `reload` must not launch a new daemon into a
//! held owner lock.
//!
//! After signalling the old daemon, reload waits for the owner lock
//! (`<socket>.lock`) to be free before launching. If the lock is still held
//! at the bound, nothing is launched and the old daemon keeps serving.
//!
//! The cases call `reload::bounce_daemon` (the part of reload after the old
//! pid is known) directly: the pid discovery in `run_reload` scans all of
//! `/proc` for any `agorabus daemon`, so a CLI-level test could signal a real
//! bus. Every case uses a private `HOME`, `XDG_RUNTIME_DIR` and socket inside
//! its own temp dir and kills only the pids it started.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use agorabus::Client;
use agorabus::reload::{Bounce, BounceOpts, bounce_daemon};

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
        std::fs::create_dir_all(tmp.path().join("home")).unwrap();
        std::fs::create_dir_all(tmp.path().join("xdg")).unwrap();
        std::fs::create_dir_all(tmp.path().join("run")).unwrap();
        Self { tmp, socket }
    }

    fn lock(&self) -> PathBuf {
        self.tmp.path().join("run/sock.lock")
    }

    /// Daemon arguments after `daemon`, so a launched daemon touches nothing
    /// outside the sandbox.
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

    fn daemon_cmd(&self) -> Command {
        let mut cmd = Command::new(bin());
        cmd.env("HOME", self.tmp.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.tmp.path().join("xdg"))
            .arg("--socket")
            .arg(&self.socket)
            .arg("daemon")
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

/// Start `cmd`; a thread reaps it so `/proc/<pid>` vanishes when it exits
/// (reload's drain wait looks for that, as it would for a non-child).
fn spawn_reaped(cmd: &mut Command, pids: &mut Pids) -> u32 {
    let mut child: Child = cmd.spawn().unwrap();
    let pid = child.id();
    pids.track(pid);
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    pid
}

fn process_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

fn wait_dead(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while process_alive(pid) {
        assert!(Instant::now() < deadline, "pid {pid} did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// One announce round-trip through the bus at `socket`.
fn answers(socket: &Path) -> bool {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let attempt = async {
            let mut c = Client::connect(socket).await.ok()?;
            let reply = c.announce("reload-lock-test", 1, "/tmp", "").await.ok()?;
            Some(reply.ok)
        };
        tokio::time::timeout(Duration::from_secs(2), attempt)
            .await
            .ok()
            .flatten()
            .unwrap_or(false)
    })
}

fn wait_answers(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !answers(socket) {
        assert!(
            Instant::now() < deadline,
            "no answer on {}",
            socket.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Pids whose cmdline mentions `needle` (reads cmdline only).
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

fn hold_lock(path: &Path) -> std::fs::File {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    rustix::fs::flock(&f, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    f
}

/// (a) Normal reload: the old daemon exits within drain, the new one binds
/// and answers.
#[test]
fn a_normal_reload_new_daemon_binds() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let old = spawn_reaped(&mut sb.daemon_cmd(), &mut pids);
    wait_answers(&sb.socket);

    let out = bounce_daemon(
        &sb.socket,
        old,
        &BounceOpts {
            drain_timeout: Duration::from_secs(5),
            extra_wait: Duration::from_secs(5),
            binary: &bin(),
            extra_args: &sb.daemon_args(),
        },
    )
    .unwrap();
    let Bounce::Launched(new) = out else {
        panic!("expected Launched, got {out:?}");
    };
    pids.track(new);
    assert_ne!(new, old);
    assert!(!process_alive(old), "old daemon gone");
    wait_answers(&sb.socket);
    assert!(process_alive(new), "new daemon running");
}

/// (b) Slow drain: the old daemon ignores SIGTERM and the lock is held past
/// `drain_timeout`, released before the bound. Reload waits, then launches.
#[test]
fn b_slow_drain_waits_for_lock_then_launches() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    // Stands in for the old daemon: ignores TERM until its stdin closes.
    let mut old_child = Command::new("sh")
        .args(["-c", "trap '' TERM; read x"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let old = old_child.id();
    pids.track(old);
    let stdin = old_child.stdin.take().unwrap();
    std::thread::spawn(move || {
        let _ = old_child.wait();
    });

    let lock = hold_lock(&sb.lock());
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(900));
        drop(lock);
        drop(stdin);
    });

    let t0 = Instant::now();
    let out = bounce_daemon(
        &sb.socket,
        old,
        &BounceOpts {
            drain_timeout: Duration::from_millis(200),
            extra_wait: Duration::from_secs(8),
            binary: &bin(),
            extra_args: &sb.daemon_args(),
        },
    )
    .unwrap();
    let waited = t0.elapsed();
    release.join().unwrap();
    let Bounce::Launched(new) = out else {
        panic!("expected Launched, got {out:?}");
    };
    pids.track(new);
    assert!(
        waited >= Duration::from_millis(800),
        "waited past drain_timeout for the lock: {waited:?}"
    );
    wait_answers(&sb.socket);
}

/// (c) The lock is never released within the bound while a listener serves
/// the path: nothing is launched, the socket inode and listener are intact.
#[test]
fn c_lock_never_released_does_not_launch() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let listener = std::os::unix::net::UnixListener::bind(&sb.socket).unwrap();
    let before = std::fs::symlink_metadata(&sb.socket).unwrap();
    let _lock = hold_lock(&sb.lock());
    let old = spawn_reaped(Command::new("sleep").arg("30"), &mut pids);

    let out = bounce_daemon(
        &sb.socket,
        old,
        &BounceOpts {
            drain_timeout: Duration::from_millis(200),
            extra_wait: Duration::from_millis(1000),
            binary: &bin(),
            extra_args: &sb.daemon_args(),
        },
    )
    .unwrap();
    assert_eq!(out, Bounce::LockHeld);
    wait_dead(old);

    let sock = sb.socket.to_string_lossy().into_owned();
    assert!(
        pids_mentioning(&sock).is_empty(),
        "no process started on the socket: {:?}",
        pids_mentioning(&sock)
    );
    let after = std::fs::symlink_metadata(&sb.socket).unwrap();
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    listener.set_nonblocking(true).unwrap();
    while listener.accept().is_ok() {}
    let _client = std::os::unix::net::UnixStream::connect(&sb.socket).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while listener.accept().is_err() {
        assert!(Instant::now() < deadline, "listener still serves the path");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// (d) A pre-existing 0644 lock file is 0600 after a daemon start.
#[test]
fn d_preexisting_lock_file_becomes_0600() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    std::fs::write(sb.lock(), b"").unwrap();
    std::fs::set_permissions(sb.lock(), std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(std::fs::metadata(sb.lock()).unwrap().mode() & 0o777, 0o644);

    spawn_reaped(&mut sb.daemon_cmd(), &mut pids);
    wait_answers(&sb.socket);
    assert_eq!(std::fs::metadata(sb.lock()).unwrap().mode() & 0o777, 0o600);
}
