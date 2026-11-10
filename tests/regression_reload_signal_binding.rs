//! Regression tests for A1532 round 2: reload signals only the daemon that
//! owns the socket being reloaded, identified by lock holder and process
//! identity, and never through a `kill` program found on PATH. Every case
//! uses a private `HOME`, `XDG_RUNTIME_DIR` and scratch sockets, and kills
//! only the pids it started.

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


fn sleeper(shell: &str, pids: &mut Pids) -> u32 {
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(shell)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    spawn_reaped(&mut cmd, pids)
}

fn opts<'a>(bin: &'a Path, args: &'a [String], home: &'a Path) -> BounceOpts<'a> {
    BounceOpts {
        drain_timeout: Duration::from_millis(300),
        extra_wait: Duration::from_millis(600),
        binary: bin,
        extra_args: args,
        home: Some(home),
    }
}

/// (a) The only agorabus daemon serves socket A; a reload of socket B that is
/// handed A's pid must not signal it.
#[test]
fn reload_of_socket_b_never_signals_the_daemon_on_socket_a() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let a = spawn_reaped(&mut sb.daemon_cmd(), &mut pids);
    wait_answers(&sb.socket);
    let socket_b = sb.tmp.path().join("run/sock-b");
    let b = bin();
    let args = sb.daemon_args();
    let home = sb.tmp.path().join("home");
    let out = bounce_daemon(&socket_b, a, &opts(&b, &args, &home));
    if let Ok(Bounce::Launched(new) | Bounce::NotOwner(new)) = out {
        pids.track(new);
    }
    std::thread::sleep(Duration::from_millis(500));
    assert!(process_alive(a), "daemon A (pid {a}) was signalled by a reload of socket B: {out:?}");
    assert!(answers(&sb.socket), "daemon A no longer serves its socket: {out:?}");
    assert!(out.is_err(), "reload of an unowned socket must refuse, got {out:?}");
}

/// (b) Processes whose cmdline merely contains the word agorabus are never
/// signalled, even when named as the pid to bounce.
#[test]
fn unrelated_process_naming_agorabus_is_never_signalled() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let sh = sleeper("sleep 30; true # agorabus", &mut pids);
    let log = sb.tmp.path().join("agorabus.log");
    std::fs::write(&log, "").unwrap();
    let tail = {
        let mut cmd = Command::new("tail");
        cmd.arg("-f")
            .arg(&log)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        spawn_reaped(&mut cmd, &mut pids)
    };
    std::thread::sleep(Duration::from_millis(300));
    let b = bin();
    let args = sb.daemon_args();
    let home = sb.tmp.path().join("home");
    for pid in [sh, tail] {
        let out = bounce_daemon(&sb.socket, pid, &opts(&b, &args, &home));
        if let Ok(Bounce::Launched(new) | Bounce::NotOwner(new)) = out {
            pids.track(new);
        }
        std::thread::sleep(Duration::from_millis(300));
        assert!(process_alive(pid), "unrelated pid {pid} was signalled: {out:?}");
    }
}
