//! Regression test for A1532 round 2: reload sends its signals through a pidfd,
//! never through a `kill` program found on PATH. Single-test binary because it
//! sets `PATH` for the whole process.

#![allow(
    unsafe_code,
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
            let _ = Command::new("/usr/bin/kill")
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


/// A fake `kill` first on PATH records any invocation; a reload of a live
/// daemon must never run it.
#[test]
fn reload_never_runs_a_kill_program_from_path() {
    let sb = Sandbox::new();
    let mut pids = Pids::default();
    let old = spawn_reaped(&mut sb.daemon_cmd(), &mut pids);
    wait_answers(&sb.socket);
    let fake = sb.tmp.path().join("fakebin");
    std::fs::create_dir_all(&fake).unwrap();
    let marker = sb.tmp.path().join("kill-was-run");
    let script = fake.join("kill");
    std::fs::write(
        &script,
        format!("#!/bin/sh\necho \"$@\" >> {}\nexit 0\n", marker.display()),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = format!("{}:{}", fake.display(), std::env::var("PATH").unwrap_or_default());
    // SAFETY: single-test binary; nothing else reads the environment concurrently.
    unsafe {
        std::env::set_var("PATH", path);
    }
    let b = bin();
    let args = sb.daemon_args();
    let res = bounce_daemon(
        &sb.socket,
        old,
        &BounceOpts {
            drain_timeout: Duration::from_millis(300),
            extra_wait: Duration::from_millis(1500),
            binary: &b,
            extra_args: &args,
            home: Some(&sb.tmp.path().join("home")),
        },
    );
    let detail = format!("{res:?}");
    if let Ok(Bounce::Launched(n) | Bounce::NotOwner(n)) = res {
        pids.track(n);
    }
    assert!(
        !marker.exists(),
        "reload ran a kill program from PATH ({}); result {detail}",
        std::fs::read_to_string(&marker).unwrap_or_default()
    );
    assert!(answers(&sb.socket), "no daemon serves after reload: {detail}");
}
