//! Regression tests for A1532: a reload that cannot finish must never leave
//! the bus without a serving daemon.
//!
//! `bounce_daemon` signals the old daemon, whose shutdown arm removes the
//! socket at once and then holds the owner lock through its drain grace. If
//! the lock outlives the wait bound, reload used to return `LockHeld` with no
//! socket and no successor (a clean exit 0 is not restarted by
//! `Restart=on-failure`). Every case uses a private `HOME`, `XDG_RUNTIME_DIR`
//! and socket inside its own temp dir and kills only the pids it started.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::tests_outside_test_module,
    clippy::panic,
    clippy::indexing_slicing
)]

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

/// Old daemon with a drain grace far longer than the wait bound: on SIGTERM it
/// drops the socket and keeps the owner lock while it "drains".
fn wedged_daemon(sb: &Sandbox) -> Command {
    let mut cmd = Command::new(bin());
    let p = |n: &str| sb.tmp.path().join(n).to_string_lossy().into_owned();
    cmd.env("HOME", sb.tmp.path().join("home"))
        .env("XDG_RUNTIME_DIR", sb.tmp.path().join("xdg"))
        .arg("--socket")
        .arg(&sb.socket)
        .args(["daemon", "--state-file"])
        .arg(p("state.json"))
        .args(["--uplink-config"])
        .arg(p("no-uplink.toml"))
        .args(["--drain-grace-ms", "120000"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

/// The owner lock outlives `drain_timeout + extra_wait`: afterwards a daemon
/// must still serve the socket (or reload must not have signalled at all).
#[test]
fn lock_held_past_bound_leaves_a_serving_daemon() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let old = spawn_reaped(&mut wedged_daemon(&sb), &mut pids);
    wait_answers(&sb.socket);

    let out = bounce_daemon(
        &sb.socket,
        old,
        &BounceOpts {
            drain_timeout: Duration::from_millis(200),
            extra_wait: Duration::from_millis(600),
            binary: &bin(),
            extra_args: &sb.daemon_args(),
            home: Some(&sb.tmp.path().join("home")),
        },
    )
    .unwrap();
    if let Bounce::Launched(new) | Bounce::NotOwner(new) = out {
        pids.track(new);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !answers(&sb.socket) {
        assert!(
            Instant::now() < deadline,
            "bounce returned {out:?} and no daemon serves {}",
            sb.socket.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A successful `Launched` pid is the daemon that holds the owner lock.
#[test]
fn launched_pid_is_the_serving_daemon() {
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
            home: Some(&sb.tmp.path().join("home")),
        },
    )
    .unwrap();
    if let Bounce::NotOwner(p) = out {
        pids.track(p);
    }
    let Bounce::Launched(new) = out else {
        panic!("expected Launched, got {out:?}");
    };
    pids.track(new);
    assert!(process_alive(new), "launched pid {new} is alive at return");
    assert!(answers(&sb.socket), "a daemon already serves at return");
}
