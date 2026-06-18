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

use crate::persist::{DurableState, StickyIntent, default_state_path, load as load_state, save as save_state};
use crate::protocol::{ClaimRecord, ClientMessage, DrainNotice, PeerRecord, Reply, ServerEvent};
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
use tokio::task::JoinSet;

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
    /// Grace period given to subscriber writes after the drain notice is sent,
    /// before the daemon proceeds to exit regardless. Best-effort: a stuck
    /// subscriber cannot wedge a roll (PRD-agorabus-drain-notice §AC3).
    /// Default: 200 ms.
    pub drain_grace_ms: u64,
    /// Value of `resume_after_ms` embedded in the drain notice. Relayed to
    /// subscribers as a reconnect backoff hint; the daemon does not compute it.
    /// Default: 3000 ms.
    pub drain_resume_hint_ms: u64,
    /// Path to the durable state file. Defaults to `~/.cache/agorabus/state.json`.
    /// Overridable via `--state-file` CLI flag.
    pub state_file: PathBuf,
    /// Debounce window in milliseconds for state-flush writes. A burst of
    /// mutations within this window coalesces into a single write. Default: 250 ms.
    pub state_flush_ms: u64,
}

impl DaemonConfig {
    /// Build a config with the default socket path and 60s heartbeat timeout.
    #[must_use]
    pub fn defaults() -> Self {
        Self {
            socket_path: crate::default_socket_path(),
            heartbeat_timeout: Duration::from_secs(crate::DEFAULT_HEARTBEAT_TIMEOUT_SECS),
            broadcast_capacity: 1024,
            drain_grace_ms: crate::DEFAULT_DRAIN_GRACE_MS,
            drain_resume_hint_ms: crate::DEFAULT_DRAIN_RESUME_HINT_MS,
            state_file: default_state_path(),
            state_flush_ms: crate::DEFAULT_STATE_FLUSH_MS,
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
    // canonical_path -> active claim. Durable: persisted to state.json on
    // each mutation (PRD-agorabus-state-persist).
    claims: HashMap<String, ClaimRecord>,
    // session_id -> sticky intent (skill / prd_slug / working_paths).
    // Durable: persisted alongside claims.
    intents: HashMap<String, StickyIntent>,
}

impl BusState {
    /// Construct from a rehydrated [`DurableState`]. Peers and peer_owners
    /// start empty — they are populated as peers re-announce after restart.
    fn from_durable(durable: DurableState) -> Self {
        Self {
            peers: HashMap::new(),
            peer_owners: HashMap::new(),
            claims: durable.claims,
            intents: durable.intents,
        }
    }

    // Drop any claim whose ttl_unix_secs <= now. Called before any
    // read/write against the claims table so expired entries never leak.
    fn prune_expired_claims(&mut self, now: u64) {
        self.claims.retain(|_, c| c.ttl_unix_secs > now);
    }

    /// Extract the durable slice for serialization.
    fn to_durable(&self) -> DurableState {
        DurableState {
            claims: self.claims.clone(),
            intents: self.intents.clone(),
        }
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

    // Rehydrate durable state from the journal, then prune expired claims.
    let durable = load_state(&config.state_file).unwrap_or_else(|e| {
        eprintln!("agorabus: could not read state file: {e:#}; starting empty");
        DurableState::default()
    });
    let mut bus_state = BusState::from_durable(durable);
    bus_state.prune_expired_claims(now_unix_secs());
    let state = Arc::new(Mutex::new(bus_state));

    // Persist-flush channel. Connection tasks send () when the durable slice
    // changes; the persist task debounces and flushes.
    let (persist_tx, mut persist_rx) = tokio::sync::mpsc::channel::<()>(64);
    let state_for_persist = Arc::clone(&state);
    let state_file_for_persist = config.state_file.clone();
    let flush_debounce = Duration::from_millis(config.state_flush_ms);

    // Spawn the persist writer as a separate task. It drains pending signals
    // with a debounce window, then writes the current durable state.
    let persist_task = tokio::spawn(async move {
        loop {
            match persist_rx.recv().await {
                Some(()) => {
                    // Debounce: drain any additional signals that arrive within
                    // the flush window, then do a single write.
                    tokio::time::sleep(flush_debounce).await;
                    while persist_rx.try_recv().is_ok() {}

                    let durable = state_for_persist.lock().await.to_durable();
                    if let Err(e) = save_state(&state_file_for_persist, &durable) {
                        eprintln!("agorabus: state-flush error: {e:#}");
                    }
                }
                None => break, // channel closed → exit
            }
        }
    });

    if let Some(tx) = ready_tx {
        let _ = tx.send(());
    }

    let (bcast_tx, _) = broadcast::channel::<BroadcastMsg>(config.broadcast_capacity);
    // Drain-notice channel: capacity = 1; each connection task subscribes and
    // writes the DrainNotice to its socket when a drain fires. Capacity of 16
    // is more than enough for the subscriber count we target.
    let (drain_tx, _) = broadcast::channel::<DrainNotice>(16);
    let heartbeat_timeout = config.heartbeat_timeout;
    let drain_grace_ms = config.drain_grace_ms;
    let drain_resume_hint_ms = config.drain_resume_hint_ms;
    let conn_counter = AtomicU64::new(0);
    // Track per-connection tasks so we can abort them on shutdown, ensuring
    // that subscriber UnixStreams are closed and clients receive EOF promptly.
    let mut conn_tasks: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                let _ = tokio::fs::remove_file(socket_path).await;

                // Broadcast the drain notice to every subscribed connection.
                // Best-effort: if there are no active subscriber receivers the
                // send returns Err(NoReceivers), which is fine.
                let notice = DrainNotice::new(drain_resume_hint_ms);
                let _ = drain_tx.send(notice);

                // Give subscriber tasks a bounded grace period to flush the
                // drain notice to their sockets before we abort them.
                // This satisfies AC3: drain must never wedge shutdown — we
                // abort unconditionally after the grace window.
                tokio::time::sleep(Duration::from_millis(drain_grace_ms)).await;

                // Abort all live connection tasks so their UnixStreams are
                // dropped. Without this, subscriber tasks block in
                // `reader.next_line()` indefinitely — they never see EOF and
                // never trigger reconnect logic.
                conn_tasks.abort_all();
                // Give tasks a brief moment to actually drop (not required for
                // correctness, but avoids spurious address-already-in-use
                // races when the daemon bounces on the same socket path).
                while conn_tasks.join_next().await.is_some() {}

                // Final flush: write the current durable state to the journal
                // before exit, capturing any mutations that arrived after the
                // last debounced flush.
                {
                    let durable = state.lock().await.to_durable();
                    if let Err(e) = save_state(&config.state_file, &durable) {
                        eprintln!("agorabus: final state-flush error: {e:#}");
                    }
                }

                // Shut down the persist task cleanly.
                drop(persist_tx);
                let _ = persist_task.await;

                return Ok(());
            }
            // Reap finished connection tasks to avoid unbounded growth.
            Some(_) = conn_tasks.join_next() => {}
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let state = Arc::clone(&state);
                        let bcast = bcast_tx.clone();
                        let drain_rx = drain_tx.subscribe();
                        let persist = persist_tx.clone();
                        let conn_id = conn_counter.fetch_add(1, Ordering::Relaxed);
                        conn_tasks.spawn(async move {
                            if let Err(e) = handle_connection(
                                stream, state, bcast, drain_rx, persist, heartbeat_timeout, conn_id,
                            ).await {
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
    mut drain_rx: broadcast::Receiver<DrainNotice>,
    persist: tokio::sync::mpsc::Sender<()>,
    heartbeat_timeout: Duration,
    conn_id: u64,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    let mut session_id: Option<String> = None;
    let mut subscribed_prefixes: Vec<String> = Vec::new();
    let mut bcast_rx: Option<broadcast::Receiver<BroadcastMsg>> = None;

    loop {
        // If subscribed, multiplex between client lines, broadcast events, and
        // the drain notice channel.
        if let Some(rx) = bcast_rx.as_mut() {
            tokio::select! {
                line = reader.next_line() => {
                    match line {
                        Ok(Some(l)) => {
                            handle_line(
                                &l,
                                &mut session_id,
                                &mut subscribed_prefixes,
                                &mut bcast_rx,
                                &state,
                                &bcast,
                                &persist,
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
                            if topic_matches(&subscribed_prefixes, &msg.topic) {
                                let event = ServerEvent { topic: msg.topic, data: msg.data, from: msg.from };
                                write_json_line(&mut write_half, &event).await?;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Lagged: best effort; fall through to next iteration.
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                drain = drain_rx.recv() => {
                    // Drain notice: write it to the subscriber best-effort and
                    // return. The main loop will abort us if we stall, so no
                    // timeout is needed here — the grace window is enforced
                    // centrally in `run_daemon` (AC3).
                    if let Ok(notice) = drain {
                        // Ignore write errors: the subscriber may have already
                        // disconnected. The important thing is we attempted the
                        // send before exit.
                        let _ = write_json_line(&mut write_half, &notice).await;
                    }
                    break;
                }
            }
        } else {
            tokio::select! {
                line = reader.next_line() => {
                    match line {
                        Ok(Some(line)) => {
                            handle_line(
                                &line,
                                &mut session_id,
                                &mut subscribed_prefixes,
                                &mut bcast_rx,
                                &state,
                                &bcast,
                                &persist,
                                &mut write_half,
                                heartbeat_timeout,
                                conn_id,
                            )
                            .await?;
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                drain = drain_rx.recv() => {
                    // Drain notice on a non-subscribed connection: the client
                    // is a one-shot (announce/peers/publish). These clients
                    // complete before shutdown; nothing to send. Just exit.
                    let _ = drain;
                    break;
                }
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

// Cognitive complexity is high by design — this is the single-entry message
// router that dispatches every `ClientMessage` variant. Splitting it per-
// variant would hide the announce-first invariant and obscure the shared
// state borrow ordering. Audit changes here carefully.
#[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
async fn handle_line<W>(
    line: &str,
    session_id: &mut Option<String>,
    subscribed_prefixes: &mut Vec<String>,
    bcast_rx: &mut Option<broadcast::Receiver<BroadcastMsg>>,
    state: &Arc<Mutex<BusState>>,
    bcast: &broadcast::Sender<BroadcastMsg>,
    persist: &tokio::sync::mpsc::Sender<()>,
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
                node,
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
                            skill: String::new(),
                            prd_slug: String::new(),
                            working_paths: Vec::new(),
                            last_heartbeat_unix_secs: now_unix_secs(),
                            // Preserve the node tag from the announce op.
                            // `None` means local (backward-compatible, AC2).
                            node: node.clone(),
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
        ClientMessage::Heartbeat {
            tool,
            skill,
            prd_slug,
            working_paths,
        } => {
            if let Some(ref paths) = working_paths
                && paths.len() > crate::protocol::MAX_WORKING_PATHS
            {
                write_json_line(write_half, &Reply::error("too_many_paths")).await?;
                return Ok(());
            }
            // Track whether the durable intent slice changed so we know
            // whether to trigger a persist flush.
            let mut intent_changed = false;
            let mut st = state.lock().await;
            if let Some(rec) = st.peers.get_mut(&sid) {
                rec.last_heartbeat_unix_secs = now_unix_secs();
                if !tool.is_empty() {
                    rec.last_tool = tool;
                }
                if let Some(ref s) = skill {
                    if rec.skill != *s {
                        intent_changed = true;
                    }
                    rec.skill = s.clone();
                }
                if let Some(ref p) = prd_slug {
                    if rec.prd_slug != *p {
                        intent_changed = true;
                    }
                    rec.prd_slug = p.clone();
                }
                if let Some(ref paths) = working_paths {
                    if rec.working_paths != *paths {
                        intent_changed = true;
                    }
                    rec.working_paths = paths.clone();
                }
                // Update the durable intents map to mirror what's in the peer record.
                if intent_changed {
                    let intent = StickyIntent {
                        skill: rec.skill.clone(),
                        prd_slug: rec.prd_slug.clone(),
                        working_paths: rec.working_paths.clone(),
                    };
                    if intent.is_empty() {
                        st.intents.remove(&sid);
                    } else {
                        st.intents.insert(sid.clone(), intent);
                    }
                }
            }
            drop(st);
            if intent_changed {
                let _ = persist.try_send(());
            }
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
            // Append on each Subscribe so multiple subscribe() calls accumulate
            // prefixes (pre-0.4 this slot held a single Option and the last
            // Subscribe silently overwrote all prior ones). Dedup to keep the
            // match cheap; first Subscribe also wires up the broadcast receiver,
            // and later ones must not replace it (that would reset the channel
            // position and drop in-flight events).
            if !subscribed_prefixes.contains(&prefix) {
                subscribed_prefixes.push(prefix);
            }
            if bcast_rx.is_none() {
                *bcast_rx = Some(bcast.subscribe());
            }
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
            // Signal persist flush: claims changed.
            let _ = persist.try_send(());
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
                // Signal persist flush: claims changed.
                let _ = persist.try_send(());
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

fn topic_matches(prefixes: &[String], topic: &str) -> bool {
    // No prefixes registered, or any empty-string prefix, means "match all"
    // (preserves the pre-0.4 `unwrap_or_default()` + empty-prefix semantics).
    if prefixes.is_empty() {
        return true;
    }
    prefixes.iter().any(|p| p.is_empty() || topic.starts_with(p))
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
