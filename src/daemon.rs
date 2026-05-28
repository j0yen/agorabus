//! UDS daemon: accepts connections, tracks peers, routes pub/sub events.
//!
//! Concurrency model: a single tokio runtime; per-connection task; an
//! in-memory `Mutex<BusState>` shared across tasks. Throughput is fine for the
//! single-host advisory use case (low double-digit peers).

#![allow(
    // nursery: write_json_line is generic over a non-Send `&T`. The actual
    // call-sites pass &Reply / &ServerEvent which are Send+Sync; the bound
    // is intentionally minimal at the function level.
    clippy::future_not_send,
    // pedantic: handle_line dispatches across 6 ClientMessage variants.
    // Splitting per-variant helpers obscures the state-machine shape.
    clippy::too_many_lines,
    // nursery: explicit match-arm-then-bail is clearer than an if-let-else
    // pyramid when the bail message names the error tag we serialized.
    clippy::single_match_else,
    // pedantic: BTreeMap::default() vs Default::default() is purely stylistic.
    clippy::default_trait_access,
)]

use crate::protocol::{ClaimRecord, ClientMessage, PeerRecord, Reply, ServerEvent};
use anyhow::{Context as _, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, broadcast};

/// Daemon configuration.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Path to the UDS socket file. Parent directory is created with mode
    /// 0700 if missing; the socket file is chmod'd to 0600 after bind.
    pub socket_path: PathBuf,
    /// Heartbeat timeout. A peer with no message for this duration is pruned
    /// from the peers list on the next `peers` query.
    pub heartbeat_timeout: Duration,
    /// Capacity of the pub/sub broadcast channel. Slow subscribers that lag
    /// past this depth are dropped from that publish (Lagged variant).
    pub broadcast_capacity: usize,
}

impl DaemonConfig {
    /// Build a config with the default socket path and 60s heartbeat timeout.
    #[must_use]
    pub fn defaults() -> Self {
        Self {
            socket_path: crate::default_socket_path(),
            heartbeat_timeout: Duration::from_secs(crate::DEFAULT_HEARTBEAT_TIMEOUT_SECS),
            broadcast_capacity: 1024,
        }
    }
}

#[derive(Debug)]
struct BusState {
    // session_id -> record
    peers: HashMap<String, PeerRecord>,
    // session_id -> id of the connection that originally announced (and
    // therefore owns) this peer record. A guest connection that re-announces
    // the same session_id (e.g. a one-shot `publish --session-id <existing>`)
    // does not take ownership and does not get to remove the record on
    // disconnect. Without this, any one-shot client reusing a long-lived
    // peer's session_id would wipe the long-lived peer's record.
    peer_owners: HashMap<String, u64>,
    // canonical_path -> active claim. In-memory only; dropped on daemon
    // restart per PRD-chord-claim §State persistence.
    claims: HashMap<String, ClaimRecord>,
}

impl BusState {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
            peer_owners: HashMap::new(),
            claims: HashMap::new(),
        }
    }

    // Drop any claim whose ttl_unix_secs <= now. Called before any
    // read/write against the claims table so expired entries never leak.
    fn prune_expired_claims(&mut self, now: u64) {
        self.claims.retain(|_, c| c.ttl_unix_secs > now);
    }
}

/// One pub/sub message routed through the daemon-wide broadcast channel.
#[derive(Debug, Clone, Serialize)]
struct BroadcastMsg {
    topic: String,
    data: serde_json::Value,
    from: String,
}

/// Run the daemon until the listener errors or `shutdown` resolves.
///
/// `ready_tx` (optional) fires once the socket is bound and chmod'd; tests use
/// this to avoid racing connect-before-listen.
///
/// # Errors
///
/// Returns the error from creating parent dirs, binding, or chmod'ing the
/// socket. Accept-loop errors are logged but do not terminate the daemon.
pub async fn run_daemon(
    config: DaemonConfig,
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    let socket_path = &config.socket_path;

    if let Some(parent) = socket_path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating parent dir {}", parent.display()))?;
            set_mode(parent, 0o700)?;
        }
    }

    // Remove stale socket file if present (from a crashed previous daemon).
    if tokio::fs::metadata(socket_path).await.is_ok() {
        let _ = tokio::fs::remove_file(socket_path).await;
    }

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("binding UDS at {}", socket_path.display()))?;
    set_mode(socket_path, 0o600)?;

    if let Some(tx) = ready_tx {
        let _ = tx.send(());
    }

    let state = Arc::new(Mutex::new(BusState::new()));
    let (bcast_tx, _) = broadcast::channel::<BroadcastMsg>(config.broadcast_capacity);
    let heartbeat_timeout = config.heartbeat_timeout;
    let conn_counter = AtomicU64::new(0);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                let _ = tokio::fs::remove_file(socket_path).await;
                return Ok(());
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let state = Arc::clone(&state);
                        let bcast = bcast_tx.clone();
                        let conn_id = conn_counter.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, state, bcast, heartbeat_timeout, conn_id).await {
                                // Per-connection errors are noisy in normal operation
                                // (clients disconnect). Drop without polluting stdout/stderr.
                                let _ = e;
                            }
                        });
                    }
                    Err(_e) => {
                        // accept() error: tiny backoff to avoid a tight failure loop.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: UnixStream,
    state: Arc<Mutex<BusState>>,
    bcast: broadcast::Sender<BroadcastMsg>,
    heartbeat_timeout: Duration,
    conn_id: u64,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    let mut session_id: Option<String> = None;
    let mut subscribed_prefix: Option<String> = None;
    let mut bcast_rx: Option<broadcast::Receiver<BroadcastMsg>> = None;

    loop {
        // If subscribed, multiplex between client lines and broadcast events.
        if let Some(rx) = bcast_rx.as_mut() {
            tokio::select! {
                line = reader.next_line() => {
                    match line {
                        Ok(Some(l)) => {
                            handle_line(
                                &l,
                                &mut session_id,
                                &mut subscribed_prefix,
                                &mut bcast_rx,
                                &state,
                                &bcast,
                                &mut write_half,
                                heartbeat_timeout,
                                conn_id,
                            ).await?;
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                ev = rx.recv() => {
                    match ev {
                        Ok(msg) => {
                            let prefix = subscribed_prefix.clone().unwrap_or_default();
                            if topic_matches(&prefix, &msg.topic) {
                                let event = ServerEvent { topic: msg.topic, data: msg.data, from: msg.from };
                                write_json_line(&mut write_half, &event).await?;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Lagged: best effort; continue.
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        } else {
            match reader.next_line().await {
                Ok(Some(line)) => {
                    handle_line(
                        &line,
                        &mut session_id,
                        &mut subscribed_prefix,
                        &mut bcast_rx,
                        &state,
                        &bcast,
                        &mut write_half,
                        heartbeat_timeout,
                        conn_id,
                    )
                    .await?;
                }
                Ok(None) | Err(_) => break,
            }
        }
    }

    // Connection closed: drop the peer record only if this connection owns
    // it. A guest connection (one that re-announced an already-owned
    // session_id) does not delete the long-lived owner's record.
    if let Some(sid) = session_id {
        let mut st = state.lock().await;
        if st.peer_owners.get(&sid).copied() == Some(conn_id) {
            st.peers.remove(&sid);
            st.peer_owners.remove(&sid);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_line<W>(
    line: &str,
    session_id: &mut Option<String>,
    subscribed_prefix: &mut Option<String>,
    bcast_rx: &mut Option<broadcast::Receiver<BroadcastMsg>>,
    state: &Arc<Mutex<BusState>>,
    bcast: &broadcast::Sender<BroadcastMsg>,
    write_half: &mut W,
    heartbeat_timeout: Duration,
    conn_id: u64,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let msg: ClientMessage = match serde_json::from_str(trimmed) {
        Ok(m) => m,
        Err(_) => {
            write_json_line(write_half, &Reply::error("malformed_json")).await?;
            // Close the connection per AC7.
            anyhow::bail!("malformed_json");
        }
    };

    if session_id.is_none() {
        // First message must be announce.
        match &msg {
            ClientMessage::Announce {
                session_id: sid,
                pid,
                cwd,
                intent,
            } => {
                {
                    let mut st = state.lock().await;
                    // Only claim ownership and (re)insert the peer record if
                    // no other connection currently owns this session_id.
                    // Otherwise this connection becomes a guest: the existing
                    // peer record is left untouched, and the guest's eventual
                    // disconnect will not remove it.
                    if !st.peer_owners.contains_key(sid) {
                        let record = PeerRecord {
                            session_id: sid.clone(),
                            pid: *pid,
                            cwd: cwd.clone(),
                            intent: intent.clone(),
                            last_tool: String::new(),
                            last_heartbeat_unix_secs: now_unix_secs(),
                            extra: Default::default(),
                        };
                        st.peers.insert(sid.clone(), record);
                        st.peer_owners.insert(sid.clone(), conn_id);
                    }
                }
                *session_id = Some(sid.clone());
                write_json_line(write_half, &Reply::ok()).await?;
                return Ok(());
            }
            _ => {
                write_json_line(write_half, &Reply::error("announce_required")).await?;
                anyhow::bail!("announce_required");
            }
        }
    }

    // session_id is set from here on.
    let sid = session_id.clone().unwrap_or_default();

    match msg {
        ClientMessage::Announce { .. } => {
            write_json_line(write_half, &Reply::error("already_announced")).await?;
        }
        ClientMessage::Update { cwd, intent } => {
            let mut st = state.lock().await;
            if let Some(rec) = st.peers.get_mut(&sid) {
                if let Some(c) = cwd {
                    rec.cwd = c;
                }
                if let Some(i) = intent {
                    rec.intent = i;
                }
                rec.last_heartbeat_unix_secs = now_unix_secs();
            }
            drop(st);
            write_json_line(write_half, &Reply::ok()).await?;
        }
        ClientMessage::Heartbeat { tool } => {
            let mut st = state.lock().await;
            if let Some(rec) = st.peers.get_mut(&sid) {
                rec.last_heartbeat_unix_secs = now_unix_secs();
                if !tool.is_empty() {
                    rec.last_tool = tool;
                }
            }
            drop(st);
            write_json_line(write_half, &Reply::ok()).await?;
        }
        ClientMessage::Publish { topic, data } => {
            let _ = bcast.send(BroadcastMsg {
                topic,
                data,
                from: sid.clone(),
            });
            // Also refresh heartbeat: publishing counts as activity.
            let mut st = state.lock().await;
            if let Some(rec) = st.peers.get_mut(&sid) {
                rec.last_heartbeat_unix_secs = now_unix_secs();
            }
            drop(st);
            write_json_line(write_half, &Reply::ok()).await?;
        }
        ClientMessage::Subscribe { prefix } => {
            *subscribed_prefix = Some(prefix);
            *bcast_rx = Some(bcast.subscribe());
            write_json_line(write_half, &Reply::ok()).await?;
        }
        ClientMessage::Peers {} => {
            let now = now_unix_secs();
            let timeout_secs = heartbeat_timeout.as_secs();
            let mut st = state.lock().await;
            // Prune stale.
            st.peers.retain(|_, rec| {
                now.saturating_sub(rec.last_heartbeat_unix_secs) <= timeout_secs
            });
            let snapshot: Vec<PeerRecord> = st.peers.values().cloned().collect();
            drop(st);
            let value = serde_json::to_value(&snapshot).unwrap_or(serde_json::Value::Null);
            write_json_line(write_half, &Reply::ok_with(value)).await?;
        }
        ClientMessage::ClaimAcquire {
            path,
            ttl_unix_secs,
            reason,
            force,
        } => {
            let now = now_unix_secs();
            let mut st = state.lock().await;
            st.prune_expired_claims(now);
            // Conflict iff path is held by a different session AND not forcing.
            let existing = st.claims.get(&path).cloned();
            let conflict = match &existing {
                Some(c) if c.session_id != sid && !force => Some(c.clone()),
                _ => None,
            };
            if let Some(c) = conflict {
                drop(st);
                let detail = serde_json::json!({
                    "holder": c.session_id,
                    "expires_unix": c.ttl_unix_secs,
                    "reason": c.reason,
                });
                let reply = Reply {
                    ok: false,
                    error: Some("claim_conflict".into()),
                    data: Some(detail),
                };
                write_json_line(write_half, &reply).await?;
                return Ok(());
            }
            // If force-evicting a foreign holder, publish their release first.
            if let Some(prev) = existing {
                if prev.session_id != sid {
                    let _ = bcast.send(BroadcastMsg {
                        topic: "claim.release".into(),
                        data: serde_json::json!({
                            "path": prev.path,
                            "session_id": prev.session_id,
                        }),
                        from: sid.clone(),
                    });
                }
            }
            let rec = ClaimRecord {
                path: path.clone(),
                session_id: sid.clone(),
                ttl_unix_secs,
                acquired_unix_secs: now,
                reason: reason.clone(),
            };
            st.claims.insert(path.clone(), rec);
            // Heartbeat-touch since we used the bus.
            if let Some(p) = st.peers.get_mut(&sid) {
                p.last_heartbeat_unix_secs = now;
            }
            drop(st);
            let _ = bcast.send(BroadcastMsg {
                topic: "claim.acquire".into(),
                data: serde_json::json!({
                    "path": path,
                    "session_id": sid,
                    "ttl_unix_secs": ttl_unix_secs,
                    "reason": reason,
                }),
                from: sid.clone(),
            });
            let payload = serde_json::json!({
                "path": path,
                "ttl_unix_secs": ttl_unix_secs,
            });
            write_json_line(write_half, &Reply::ok_with(payload)).await?;
        }
        ClientMessage::ClaimRelease { path } => {
            let now = now_unix_secs();
            let mut st = state.lock().await;
            st.prune_expired_claims(now);
            // Only release if held by this same session. Other sessions'
            // claims are not affected by a release call from us.
            let released = match st.claims.get(&path) {
                Some(c) if c.session_id == sid => {
                    st.claims.remove(&path);
                    true
                }
                _ => false,
            };
            if let Some(p) = st.peers.get_mut(&sid) {
                p.last_heartbeat_unix_secs = now;
            }
            drop(st);
            if released {
                let _ = bcast.send(BroadcastMsg {
                    topic: "claim.release".into(),
                    data: serde_json::json!({
                        "path": path,
                        "session_id": sid,
                    }),
                    from: sid.clone(),
                });
            }
            let payload = serde_json::json!({"released": released});
            write_json_line(write_half, &Reply::ok_with(payload)).await?;
        }
        ClientMessage::ClaimList {} => {
            let now = now_unix_secs();
            let mut st = state.lock().await;
            st.prune_expired_claims(now);
            let snapshot: Vec<ClaimRecord> = st.claims.values().cloned().collect();
            drop(st);
            let value = serde_json::to_value(&snapshot).unwrap_or(serde_json::Value::Null);
            write_json_line(write_half, &Reply::ok_with(value)).await?;
        }
    }
    Ok(())
}

async fn write_json_line<W, T>(w: &mut W, value: &T) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let mut buf = serde_json::to_vec(value).context("serializing reply")?;
    buf.push(b'\n');
    w.write_all(&buf).await.context("write reply")?;
    w.flush().await.context("flush reply")?;
    Ok(())
}

fn topic_matches(prefix: &str, topic: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    topic.starts_with(prefix)
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let perm = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(path, perm)
        .with_context(|| format!("chmod {mode:o} {}", path.display()))?;
    Ok(())
}
