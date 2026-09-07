//! Test harness: spawn a throwaway `nats-server` on a random localhost port
//! for integration tests that need a real NATS transport
//! (PRD-agorabus-nats-uplink).
//!
//! AC12: if `nats-server` is not on `$PATH`, tests using this harness must
//! skip with an explicit `skipped: nats-server not on PATH` message instead
//! of passing vacuously.

#![allow(dead_code)]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Returns true if `nats-server` is discoverable on `$PATH`.
pub fn nats_server_available() -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join("nats-server").is_file()))
        .unwrap_or(false)
}

/// Print the AC12 skip message and return, for tests that need a real
/// `nats-server` and find none on `$PATH`.
pub fn skip_no_nats_server(test_name: &str) {
    println!("skipped: nats-server not on PATH ({test_name})");
}

/// A running throwaway `nats-server` bound to a fixed localhost port chosen
/// by the harness at spawn time.
pub struct NatsServer {
    child: Option<Child>,
    port: u16,
}

impl NatsServer {
    /// Spawn a fresh `nats-server` on a random free port and wait for it to
    /// accept connections. Panics if `nats-server` is not on PATH — callers
    /// must check [`nats_server_available`] first and skip instead (AC12).
    pub async fn spawn() -> Self {
        let port = free_port();
        let mut me = Self { child: None, port };
        me.spawn_on_current_port().await;
        me
    }

    /// This server's `nats://127.0.0.1:<port>` URL.
    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    /// Kill the current `nats-server` process. Leaves the port reserved so a
    /// subsequent [`Self::restart`] rebinds the same URL (models a leaf
    /// bounce for AC5's reconnect test).
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Restart `nats-server` on the same port after a prior [`Self::stop`].
    pub async fn restart(&mut self) {
        // Give the OS a moment to release the just-killed listener before
        // rebinding the same port.
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.spawn_on_current_port().await;
    }

    async fn spawn_on_current_port(&mut self) {
        let child = Command::new("nats-server")
            .args(["-p", &self.port.to_string(), "-a", "127.0.0.1"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn nats-server");
        self.child = Some(child);

        // Wait for the port to accept connections before returning.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("nats-server did not become ready on port {}", self.port);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Reserve a free TCP port by binding then immediately dropping a listener.
/// Small TOCTOU race in principle; acceptable for a test harness.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}
