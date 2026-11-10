//! Regression tests for A1455: one live owner per daemon socket path.
//!
//! The daemon takes `flock(LOCK_EX|LOCK_NB)` on `<socket>.lock` for its whole
//! lifetime and connect-probes the socket before any unlink or bind. A second
//! daemon on a live path exits 0 with the owner's socket untouched; an
//! unknown probe result fails closed; shutdown unlinks the path only while it
//! still names the inode this daemon bound.
//!
//! Every case spawns the binary built from this crate with a private `HOME`,
//! `XDG_RUNTIME_DIR` and `--socket` inside its own temp dir, and kills only
//! the pids it spawned.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

use agorabus::Client;

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
        Self { tmp, socket }
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(bin());
        cmd.env("HOME", self.tmp.path().join("home"))
            .env("XDG_RUNTIME_DIR", self.tmp.path().join("xdg"))
            .arg("--socket")
            .arg(&self.socket)
            .arg("daemon")
            .arg("--state-file")
            .arg(self.tmp.path().join("state.json"))
            .arg("--drain-grace-ms")
            .arg("50")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        cmd
    }

    fn spawn(&self) -> Daemon {
        Daemon {
            child: Some(self.command().spawn().unwrap()),
        }
    }
}

/// A daemon this test spawned; killed by pid on drop.
struct Daemon {
    child: Option<Child>,
}

impl Daemon {
    fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    fn running(&mut self) -> bool {
        self.child.as_mut().unwrap().try_wait().unwrap().is_none()
    }

    /// Wait for the process to exit on its own; panics after `timeout`.
    fn wait_exit(mut self, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        while self.running() {
            assert!(Instant::now() < deadline, "pid {} did not exit", self.pid());
            std::thread::sleep(Duration::from_millis(20));
        }
        self.child.take().unwrap().wait_with_output().unwrap()
    }

    fn sigterm(&self) {
        let st = Command::new("kill")
            .arg("-TERM")
            .arg(self.pid().to_string())
            .status()
            .unwrap();
        assert!(st.success());
    }

    fn sigkill(&mut self) {
        let child = self.child.as_mut().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
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
            let reply = c.announce("owner-test", 1, "/tmp", "").await.ok()?;
            Some(reply.ok)
        };
        tokio::time::timeout(Duration::from_secs(2), attempt)
            .await
            .ok()
            .flatten()
            .unwrap_or(false)
    })
}

/// Poll until the bus at `socket` answers; panics after 10 s.
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

fn inode(path: &Path) -> (u64, u64) {
    let m = std::fs::symlink_metadata(path).unwrap();
    (m.dev(), m.ino())
}

/// The path still routes to `listener`: a fresh connect is accepted by it.
/// (Inode numbers are reused after unlink, so an equal inode alone does not
/// prove the original socket survived.)
fn listener_owns_path(listener: &std::os::unix::net::UnixListener, path: &Path) -> bool {
    listener.set_nonblocking(true).unwrap();
    // Drain earlier connects (e.g. the daemon's own probe) so only the fresh
    // one below can satisfy the accept.
    while listener.accept().is_ok() {}
    let Ok(_client) = std::os::unix::net::UnixStream::connect(path) else {
        return false;
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if listener.accept().is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

fn stderr_lines(out: &Output) -> Vec<String> {
    String::from_utf8_lossy(&out.stderr)
        .lines()
        .map(str::to_owned)
        .collect()
}

fn assert_code(status: ExitStatus, want: i32) {
    assert_eq!(status.code(), Some(want), "exit status {status:?}");
}

const EXIT_WAIT: Duration = Duration::from_secs(10);

/// (a) No socket: the daemon binds and answers.
#[test]
fn a_no_socket_binds_and_answers() {
    let sb = Sandbox::new();
    let mut d = sb.spawn();
    wait_answers(&sb.socket);
    assert!(d.running());
    assert!(
        sb.tmp.path().join("run/sock.lock").exists(),
        "owner lock file"
    );
}

/// (b) + (h) Live owner: a second daemon exits 0 with one log line; the
/// owner's socket inode, pid and answers are unchanged after that exit.
#[test]
fn b_h_second_daemon_exits_zero_owner_untouched() {
    let sb = Sandbox::new();
    let mut owner = sb.spawn();
    wait_answers(&sb.socket);
    let before = inode(&sb.socket);
    let owner_pid = owner.pid();

    let out = sb.spawn().wait_exit(EXIT_WAIT);
    assert_code(out.status, 0);
    let lines = stderr_lines(&out);
    assert_eq!(lines.len(), 1, "one log line: {lines:?}");
    assert!(lines[0].contains("another daemon holds"), "{lines:?}");

    assert!(owner.running(), "owner pid {owner_pid} still running");
    assert_eq!(owner.pid(), owner_pid);
    assert_eq!(inode(&sb.socket), before, "socket inode unchanged");
    assert!(answers(&sb.socket), "owner still answers");
}

/// (c) Older-style owner: a plain listener bound with no lock. The new
/// daemon exits 0 and the listener's inode is unchanged.
#[test]
fn c_unlocked_live_listener_is_respected() {
    let sb = Sandbox::new();
    std::fs::create_dir_all(sb.socket.parent().unwrap()).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&sb.socket).unwrap();
    let before = inode(&sb.socket);

    let out = sb.spawn().wait_exit(EXIT_WAIT);
    assert_code(out.status, 0);
    let lines = stderr_lines(&out);
    assert_eq!(lines.len(), 1, "one log line: {lines:?}");
    assert!(
        lines[0].contains("live daemon already answers"),
        "{lines:?}"
    );
    assert_eq!(inode(&sb.socket), before, "listener inode unchanged");
    assert!(
        listener_owns_path(&listener, &sb.socket),
        "listener still serves the path"
    );
}

/// (d) Stale socket file, no process: the daemon unlinks it and binds.
#[test]
fn d_stale_socket_is_replaced() {
    let sb = Sandbox::new();
    std::fs::create_dir_all(sb.socket.parent().unwrap()).unwrap();
    drop(std::os::unix::net::UnixListener::bind(&sb.socket).unwrap());
    assert!(
        !answers(&sb.socket),
        "fixture: stale socket has no listener"
    );

    // The stale file cannot answer, so an answer proves a fresh bind. Its
    // inode number may be reused by the new socket, so it is not compared.
    let mut d = sb.spawn();
    wait_answers(&sb.socket);
    assert!(d.running());
}

fn assert_refused(sb: &Sandbox, reason: &str) {
    let out = sb.spawn().wait_exit(EXIT_WAIT);
    assert_ne!(
        out.status.code(),
        Some(0),
        "non-zero exit: {:?}",
        out.status
    );
    assert!(out.status.code().is_some(), "exited, not signalled");
    let lines = stderr_lines(&out);
    assert_eq!(lines.len(), 1, "one log line: {lines:?}");
    assert!(lines[0].contains(reason), "reason {reason:?} in {lines:?}");
}

/// (e1) UNKNOWN: a regular file at the socket path fails closed, untouched.
#[test]
fn e1_regular_file_fails_closed() {
    let sb = Sandbox::new();
    std::fs::create_dir_all(sb.socket.parent().unwrap()).unwrap();
    std::fs::write(&sb.socket, b"not a socket").unwrap();
    let before = inode(&sb.socket);
    assert_refused(&sb, "is not a socket");
    assert_eq!(inode(&sb.socket), before);
    assert_eq!(std::fs::read(&sb.socket).unwrap(), b"not a socket");
}

/// (e2) UNKNOWN: a directory at the socket path fails closed, untouched.
#[test]
fn e2_directory_fails_closed() {
    let sb = Sandbox::new();
    std::fs::create_dir_all(&sb.socket).unwrap();
    std::fs::write(sb.socket.join("keep"), b"x").unwrap();
    let before = inode(&sb.socket);
    assert_refused(&sb, "is not a socket");
    assert_eq!(inode(&sb.socket), before);
    assert!(sb.socket.join("keep").exists());
}

/// (e3) UNKNOWN: a self-referencing symlink makes the probe fail with ELOOP,
/// an error other than `NotFound` / `ConnectionRefused`; fails closed, untouched.
#[test]
fn e3_probe_error_fails_closed() {
    let sb = Sandbox::new();
    std::fs::create_dir_all(sb.socket.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&sb.socket, &sb.socket).unwrap();
    let before = inode(&sb.socket);
    assert_refused(&sb, "probing");
    assert_eq!(inode(&sb.socket), before);
    assert!(
        std::fs::symlink_metadata(&sb.socket)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

/// (f) Five daemons launched together, no owner: exactly one live owner,
/// four exit 0, one socket inode.
#[test]
fn f_five_concurrent_starts_one_owner() {
    let sb = Sandbox::new();
    let mut all: Vec<Daemon> = (0..5).map(|_| sb.spawn()).collect();
    wait_answers(&sb.socket);
    let first = inode(&sb.socket);

    let deadline = Instant::now() + EXIT_WAIT;
    loop {
        let live = all.iter_mut().map(Daemon::running).filter(|r| *r).count();
        if live == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "{live} daemons still running");
        std::thread::sleep(Duration::from_millis(20));
    }
    let owner_idx = all.iter_mut().position(Daemon::running).unwrap();
    let mut owner = all.remove(owner_idx);
    for d in all {
        let out = d.wait_exit(EXIT_WAIT);
        assert_code(out.status, 0);
        assert_eq!(stderr_lines(&out).len(), 1);
    }
    assert!(owner.running());
    assert_eq!(inode(&sb.socket), first, "one inode");
    assert!(answers(&sb.socket));
}

/// (g) Owner killed with SIGKILL: the kernel releases the lock; the next start binds.
#[test]
fn g_after_sigkill_next_start_binds() {
    let sb = Sandbox::new();
    let mut owner = sb.spawn();
    wait_answers(&sb.socket);
    owner.sigkill();
    assert!(sb.socket.exists(), "socket file left behind by SIGKILL");
    assert!(!answers(&sb.socket), "killed owner no longer answers");

    let mut next = sb.spawn();
    wait_answers(&sb.socket);
    assert!(next.running());
}

/// (h2) Shutdown unlinks the path only while it names this daemon's inode:
/// an owner whose path was re-bound by someone else leaves it in place.
#[test]
fn h2_shutdown_leaves_foreign_socket_in_place() {
    let sb = Sandbox::new();
    let owner = sb.spawn();
    wait_answers(&sb.socket);
    std::fs::remove_file(&sb.socket).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&sb.socket).unwrap();
    let foreign = inode(&sb.socket);

    owner.sigterm();
    let out = owner.wait_exit(EXIT_WAIT);
    assert_code(out.status, 0);
    assert_eq!(inode(&sb.socket), foreign, "foreign socket kept");
    assert!(
        listener_owns_path(&listener, &sb.socket),
        "foreign listener still serves the path"
    );
}

/// (h3) A clean shutdown still removes the daemon's own socket.
#[test]
fn h3_shutdown_removes_own_socket() {
    let sb = Sandbox::new();
    let owner = sb.spawn();
    wait_answers(&sb.socket);
    owner.sigterm();
    let out = owner.wait_exit(EXIT_WAIT);
    assert_code(out.status, 0);
    assert!(!sb.socket.exists(), "own socket removed");
}
