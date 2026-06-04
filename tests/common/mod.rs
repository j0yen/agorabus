//! Shared test fixtures: spin up an in-process daemon on a per-test temp
//! socket, with a `Drop` guard that cleanly shuts it down.

#![allow(dead_code)] // shared across many test files; some helpers are file-local in use
#![allow(unreachable_pub)] // test-mod re-exported across many integration crates

use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::oneshot;

use agorabus::{
    DaemonConfig, DEFAULT_DRAIN_GRACE_MS, DEFAULT_DRAIN_RESUME_HINT_MS, DEFAULT_STATE_FLUSH_MS,
    run_daemon,
};

pub struct DaemonHandle {
    pub socket: PathBuf,
    pub tmp: tempfile::TempDir,
    pub shutdown: Option<oneshot::Sender<()>>,
    pub join: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl DaemonHandle {
    pub async fn start_with_timeout(heartbeat_timeout: Duration) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");
        let cfg = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout,
            broadcast_capacity: 256,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file,
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join =
            tokio::spawn(async move { run_daemon(cfg, Some(ready_tx), shutdown_rx).await });
        ready_rx.await.expect("daemon ready");
        Self {
            socket,
            tmp,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    pub async fn start() -> Self {
        Self::start_with_timeout(Duration::from_secs(60)).await
    }

    /// Start with custom drain parameters (PRD-agorabus-drain-notice tests).
    pub async fn start_with_drain(
        heartbeat_timeout: Duration,
        drain_grace_ms: u64,
        drain_resume_hint_ms: u64,
    ) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket = tmp.path().join("sock");
        let state_file = tmp.path().join("state.json");
        let cfg = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout,
            broadcast_capacity: 256,
            drain_grace_ms,
            drain_resume_hint_ms,
            state_file,
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join =
            tokio::spawn(async move { run_daemon(cfg, Some(ready_tx), shutdown_rx).await });
        ready_rx.await.expect("daemon ready");
        Self {
            socket,
            tmp,
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }

    /// Stop the running daemon (drops the listener / closes peer sockets) but
    /// keep the `tmp` dir and socket *path* alive so a fresh daemon can be
    /// bound on the same path. Models a daemon bounce for reconnect tests.
    pub async fn stop_only(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }

    /// Relaunch a fresh daemon on the *same* socket path as a prior
    /// `stop_only`. The daemon removes the stale socket file on bind. Must be
    /// called after `stop_only`; reuses `self.socket` / `self.tmp`.
    pub async fn restart_with_timeout(&mut self, heartbeat_timeout: Duration) {
        let state_file = self.tmp.path().join("state.json");
        let cfg = DaemonConfig {
            socket_path: self.socket.clone(),
            heartbeat_timeout,
            broadcast_capacity: 256,
            drain_grace_ms: DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file,
            state_flush_ms: DEFAULT_STATE_FLUSH_MS,
        };
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let join =
            tokio::spawn(async move { run_daemon(cfg, Some(ready_tx), shutdown_rx).await });
        ready_rx.await.expect("daemon ready after restart");
        self.shutdown = Some(shutdown_tx);
        self.join = Some(join);
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
        // self.tmp is dropped when self drops at end of scope.
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // join is dropped without awaiting; OK in Drop.
    }
}
