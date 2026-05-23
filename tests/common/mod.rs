//! Shared test fixtures: spin up an in-process daemon on a per-test temp
//! socket, with a `Drop` guard that cleanly shuts it down.

#![allow(dead_code)] // shared across many test files; some helpers are file-local in use
#![allow(unreachable_pub)] // test-mod re-exported across many integration crates

use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::oneshot;

use agorabus::{DaemonConfig, run_daemon};

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
        let cfg = DaemonConfig {
            socket_path: socket.clone(),
            heartbeat_timeout,
            broadcast_capacity: 256,
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
