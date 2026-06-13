//! [`ClaimGuard`]: a lifetime-bound handle that acquires an advisory claim,
//! auto-renews it before TTL expiry, and releases it on drop or explicit
//! [`ClaimGuard::release`].
//!
//! # Usage
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use std::path::Path;
//! use agorabus::claim_guard::ClaimGuard;
//! use agorabus::Client;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let sock = Path::new("/run/agorabus/sock");
//! let mut client = Client::connect(sock).await?;
//! let _ = client.announce("my-daemon", std::process::id(), "/", "hold example").await?;
//! let guard = ClaimGuard::hold(client, sock, "my-daemon", "/dev/audio", Duration::from_secs(30)).await?;
//! // ... do work ...
//! guard.release().await?;
//! # Ok(())
//! # }
//! ```

#![allow(
    clippy::future_not_send, // same reasoning as client.rs
)]

use crate::Client;
use anyhow::{Context as _, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

/// Compute the current UNIX wall-clock time in whole seconds.
fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A lifetime-bound handle on an agorabus advisory claim.
///
/// Created via [`ClaimGuard::hold`]. Renews the claim at approximately
/// `ttl/3` before each expiry. On [`Drop`] issues a best-effort
/// [`ClaimRelease`] and cancels the renew task.
///
/// For a graceful, awaitable release (e.g. from a SIGTERM handler) use
/// [`ClaimGuard::release`] instead of relying on `Drop`.
///
/// [`ClaimRelease`]: crate::protocol::ClientMessage::ClaimRelease
pub struct ClaimGuard {
    path: String,
    client: Arc<Mutex<Client>>,
    ttl: Duration,
    renew_task: Option<JoinHandle<()>>,
    cancel_tx: Option<oneshot::Sender<()>>,
}

impl ClaimGuard {
    /// Acquire `path` and return a guard that auto-renews the lease.
    ///
    /// `socket_path` and `session_id` are used to open a second connection
    /// for the background renewal task (each connection is tied to one
    /// session). The primary `client` must already have called `announce`
    /// with `session_id`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the initial [`ClaimAcquire`] fails or if the bus
    /// returns `ok: false`.
    ///
    /// [`ClaimAcquire`]: crate::protocol::ClientMessage::ClaimAcquire
    pub async fn hold(
        mut client: Client,
        socket_path: &Path,
        session_id: &str,
        path: &str,
        ttl: Duration,
    ) -> Result<Self> {
        let ttl_unix_secs = now_unix_secs() + ttl.as_secs().max(1);
        let reply = client
            .claim_acquire(path, ttl_unix_secs, "claim-guard", false)
            .await
            .context("claim_acquire on hold")?;
        if !reply.ok {
            return Err(anyhow::anyhow!(
                "claim_acquire failed: {}",
                reply.error.unwrap_or_else(|| "(no error)".into())
            ));
        }

        let client = Arc::new(Mutex::new(client));
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let renew_task = tokio::spawn(renew_loop(
            Arc::clone(&client),
            path.to_string(),
            session_id.to_string(),
            socket_path.to_path_buf(),
            ttl,
            cancel_rx,
        ));

        Ok(Self {
            path: path.to_string(),
            client,
            ttl,
            renew_task: Some(renew_task),
            cancel_tx: Some(cancel_tx),
        })
    }

    /// Explicitly release the claim and await confirmation.
    ///
    /// Cancels the renew task, issues a `ClaimRelease`, and consumes the
    /// guard. Prefer this over relying on `Drop` in SIGTERM handlers so the
    /// release is awaited and confirmed before the process exits.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the `ClaimRelease` request fails at the transport
    /// level. A `released: false` payload (idempotent, no active claim) is
    /// treated as success.
    pub async fn release(mut self) -> Result<()> {
        self.cancel_renew();
        // Lock scope ends before function returns so the guard is dropped early.
        #[allow(clippy::significant_drop_tightening)]
        self.client
            .lock()
            .await
            .claim_release(&self.path)
            .await
            .context("ClaimGuard explicit release")
            .map(|_| ())
    }

    /// Cancel the background renew task (idempotent).
    fn cancel_renew(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
        if let Some(jh) = self.renew_task.take() {
            jh.abort();
        }
    }

    /// The claimed path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The configured TTL for this guard (used to compute the renew interval).
    #[must_use]
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl Drop for ClaimGuard {
    /// Best-effort `ClaimRelease`. Cancels the renew task and spawns a
    /// fire-and-forget release. This cannot be awaited; for a guaranteed
    /// release, use [`ClaimGuard::release`].
    fn drop(&mut self) {
        self.cancel_renew();
        let client = Arc::clone(&self.client);
        let path = self.path.clone();
        tokio::spawn(async move {
            // Best-effort: ignore errors. The Mutex guard is used inline and
            // dropped at the end of the statement to avoid significant-drop lint.
            #[allow(clippy::significant_drop_tightening)]
            let _ = client.lock().await.claim_release(&path).await;
        });
    }
}

/// Background loop: sleep until `ttl * 2/3` has elapsed, then re-acquire the
/// claim. If the bus is unreachable, log the failure and retry on the next
/// tick rather than abandoning the claim.
///
/// Exits cleanly when `cancel_rx` fires.
#[allow(clippy::too_many_arguments)]
async fn renew_loop(
    client: Arc<Mutex<Client>>,
    path: String,
    session_id: String,
    socket_path: PathBuf,
    ttl: Duration,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    // Sleep for ttl * 2/3 before the first renewal attempt.
    // Use integer arithmetic (multiply by 2, divide by 3) to avoid float-arithmetic lint.
    let renew_interval = ttl
        .checked_mul(2)
        .and_then(|d| d.checked_div(3))
        .unwrap_or(ttl)
        .max(Duration::from_millis(10));

    loop {
        let sleep = tokio::time::sleep(renew_interval);
        tokio::select! {
            _ = &mut cancel_rx => {
                // Guard was dropped or released — exit cleanly.
                return;
            }
            () = sleep => {}
        }

        let ttl_unix_secs = now_unix_secs() + ttl.as_secs().max(1);
        let result = {
            let mut guard = client.lock().await;
            guard
                .claim_acquire(&path, ttl_unix_secs, "claim-guard-renew", false)
                .await
        };

        match result {
            Ok(reply) if reply.ok => {
                // Renewed successfully; continue.
            }
            Ok(reply) => {
                // Bus returned ok:false — log and retry on next tick.
                // This can happen transiently if the bus restarted and the
                // session was evicted; try reconnecting on the next iteration.
                let err_tag = reply.error.unwrap_or_else(|| "(no error tag)".into());
                #[allow(clippy::print_stderr)]
                {
                    eprintln!(
                        "[claim-guard] renew failed for {path}: {err_tag}; will retry via new connection"
                    );
                }
                // Try to reconnect and re-acquire on the next tick.
                if let Err(e) = try_reconnect_renew(
                    &client,
                    &socket_path,
                    &session_id,
                    &path,
                    ttl_unix_secs,
                )
                .await
                {
                    #[allow(clippy::print_stderr)]
                    {
                        eprintln!("[claim-guard] reconnect-renew also failed for {path}: {e}");
                    }
                }
            }
            Err(e) => {
                // Transport error — bus likely unreachable. Log and retry.
                #[allow(clippy::print_stderr)]
                {
                    eprintln!("[claim-guard] renew transport error for {path}: {e}; will retry via new connection");
                }
                if let Err(e2) = try_reconnect_renew(
                    &client,
                    &socket_path,
                    &session_id,
                    &path,
                    ttl_unix_secs,
                )
                .await
                {
                    #[allow(clippy::print_stderr)]
                    {
                        eprintln!("[claim-guard] reconnect-renew also failed for {path}: {e2}");
                    }
                }
            }
        }
    }
}

/// Attempt to open a new connection to the bus, re-announce, and re-acquire
/// the claim. Replaces the broken connection inside the shared `Arc<Mutex<Client>>`.
async fn try_reconnect_renew(
    client: &Arc<Mutex<Client>>,
    socket_path: &Path,
    session_id: &str,
    path: &str,
    ttl_unix_secs: u64,
) -> Result<()> {
    let mut new_client = Client::connect(socket_path)
        .await
        .context("claim-guard reconnect")?;
    new_client
        .announce(session_id, std::process::id(), "/", "claim-guard-renew")
        .await
        .context("claim-guard reconnect announce")?;
    let reply = new_client
        .claim_acquire(path, ttl_unix_secs, "claim-guard-reconnect-renew", false)
        .await
        .context("claim-guard reconnect acquire")?;
    if !reply.ok {
        return Err(anyhow::anyhow!(
            "reconnect claim_acquire failed: {}",
            reply.error.unwrap_or_else(|| "(no error)".into())
        ));
    }
    // Swap the broken client for the healthy one.
    // The guard is dropped at end of statement (not end of function), so no significant-drop lint.
    #[allow(clippy::significant_drop_tightening)]
    {
        *client.lock().await = new_client;
    }
    Ok(())
}
