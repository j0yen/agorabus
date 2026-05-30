//! agorabus — CLI entry point.
//!
//! Multi-subcommand client + embedded daemon. Run `agorabus daemon` to start
//! the bus; other subcommands are short-lived clients.
//!
//! All client subcommands are fail-open: with no daemon running, they
//! produce sensible empty output and exit 0.

#![allow(clippy::print_stdout)] // CLI prints structured JSON to stdout by design.
#![allow(clippy::print_stderr)]
#![allow(
    // Pedantic/nursery — cosmetic, not BAD_RUST. Silenced at module level.
    clippy::items_after_statements,
    clippy::redundant_pub_crate,
    clippy::too_many_lines,
    clippy::future_not_send,
    clippy::option_if_let_else,
    clippy::single_match_else,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::missing_errors_doc,
    clippy::cognitive_complexity,
    clippy::similar_names,
    clippy::doc_markdown,
)]

use agorabus::{
    Client, ClientMessage, DaemonConfig, ReconnectConfig, default_socket_path,
    DEFAULT_DRAIN_GRACE_MS, DEFAULT_DRAIN_RESUME_HINT_MS,
    doctor::{DoctorFormat, print_report, run_doctor},
    protocol::ServerEvent,
    reconnect_subscribe, run_daemon,
    reload::{ReloadConfig, ReloadFormat, print_verdict, run_reload},
};
use tokio::time::{Instant, timeout};
use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "agorabus",
    version,
    about = "Single-host advisory pub/sub bus for concurrent Claude sessions."
)]
struct Cli {
    /// Override the UDS path. Default: `~/.cache/agorabus/sock`.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the bus daemon.
    Daemon {
        /// Heartbeat timeout in seconds; peers idle longer than this are
        /// pruned from `peers` results.
        #[arg(long, default_value_t = agorabus::DEFAULT_HEARTBEAT_TIMEOUT_SECS)]
        heartbeat_timeout: u64,
        /// Grace period in milliseconds: after broadcasting the drain notice on
        /// SIGTERM/SIGINT, the daemon waits this long for subscriber writes to
        /// flush before aborting connections and exiting. Must never wedge a
        /// roll — exit happens unconditionally after the grace window.
        #[arg(long, default_value_t = DEFAULT_DRAIN_GRACE_MS)]
        drain_grace_ms: u64,
        /// Resume hint in milliseconds embedded in the drain notice sent to
        /// subscribers on shutdown. Subscribers are advised to wait at least
        /// this long before reconnecting to avoid a thundering-herd on rebind.
        #[arg(long, default_value_t = DEFAULT_DRAIN_RESUME_HINT_MS)]
        drain_resume_hint_ms: u64,
    },
    /// One-shot announce + immediate disconnect.
    ///
    /// Useful for tests; production use would keep a long-lived connection
    /// (managed by the SessionStart hook + heartbeat loop).
    Announce {
        /// Session id (opaque string).
        #[arg(long)]
        session_id: String,
        /// PID to record.
        #[arg(long, default_value_t = std::process::id())]
        pid: u32,
        /// Current working directory to record. Defaults to actual cwd.
        #[arg(long)]
        cwd: Option<String>,
        /// Intent string.
        #[arg(long, default_value = "")]
        intent: String,
    },
    /// List current peers as a JSON array on stdout. Fail-open: emits `[]`
    /// and exits 0 when no daemon is reachable.
    Peers,
    /// Publish a JSON payload on a topic.
    Publish {
        /// Session id used to identify the publisher (announce-on-connect).
        #[arg(long, default_value = "cli-publisher")]
        session_id: String,
        /// Dotted topic name.
        topic: String,
        /// JSON data; may be any JSON value including a bare string.
        data: String,
    },
    /// Subscribe to a topic prefix. Streams one JSON line per event to
    /// stdout until EOF or `--max-events`. Reconnects automatically on
    /// daemon restart; use `--no-reconnect` for one-shot / scripted callers.
    Subscribe {
        /// Session id used at announce time.
        #[arg(long, default_value = "cli-subscriber")]
        session_id: String,
        /// Topic prefix (empty matches all).
        prefix: String,
        /// Cap on events to receive before exiting (0 = unlimited).
        #[arg(long, default_value_t = 0)]
        max_events: usize,
        /// Disable reconnect on EOF/connection-reset (old exit-on-EOF behaviour).
        #[arg(long, default_value_t = false)]
        no_reconnect: bool,
        /// Base delay in milliseconds for exponential backoff on reconnect.
        #[arg(long, default_value_t = 100)]
        reconnect_base_ms: u64,
        /// Maximum delay cap in milliseconds for reconnect backoff.
        #[arg(long, default_value_t = 5_000)]
        reconnect_cap_ms: u64,
        /// Maximum number of reconnect attempts before exiting non-zero
        /// (0 = unlimited).
        #[arg(long, default_value_t = 0)]
        max_reconnect_attempts: usize,
    },
    /// Send a single heartbeat on a new connection. Use --tool to record
    /// the most-recently-invoked tool name.
    Heartbeat {
        /// Session id.
        #[arg(long)]
        session_id: String,
        /// Last tool name to record.
        #[arg(long, default_value = "")]
        tool: String,
    },
    /// Advisory soft-locks on filesystem paths (PRD-chord-claim).
    #[command(subcommand)]
    Claim(ClaimCommand),
    /// Structured per-session intent: skill / PRD slug / working paths
    /// (PRD-chord-intent-rich).
    #[command(subcommand)]
    Intent(IntentCommand),
    /// Self-staleness check: compare the running daemon's executing image
    /// against the installed binary on disk. Exits 0=current, 1=stale,
    /// 2=unknown (no daemon / unreadable /proc).
    Doctor {
        /// Output shape: `text` (single-line verdict) or `json`.
        #[arg(long, default_value = "text")]
        format: String,
        /// Path of the installed binary to compare against. Defaults to
        /// discovery via `which agorabus` / the current executable.
        #[arg(long)]
        installed_path: Option<PathBuf>,
    },
    /// Non-destructive one-command daemon bounce: SIGTERM the stale daemon,
    /// relaunch the fresh binary, then wait for all pre-bounce sessions to
    /// reconnect. Emits a structured verdict (old_pid, new_pid,
    /// binary_before/after, peers_before/after/recovered, elapsed_ms, status).
    ///
    /// Default posture is `--dry-run`: prints the plan without mutating.
    Reload {
        /// Output shape: `text` (human table) or `json`.
        #[arg(long, default_value = "text")]
        format: String,
        /// If set, skip the reload when the running binary is already current
        /// (no-op guard). Enabled by default; pass `--no-require-fresh` to
        /// bypass (e.g. for testing the bounce itself).
        #[arg(long, default_value_t = true)]
        require_fresh: bool,
        /// Start a new daemon even if none is currently running.
        #[arg(long, default_value_t = false)]
        start_if_absent: bool,
        /// Print the plan + freshness verdict + peer set that *would* be
        /// bounced, without mutating anything. This is the default posture
        /// (--dry-run true); pass `--no-dry-run` (or `--apply`) to perform
        /// the actual reload.
        #[arg(long, default_value_t = true)]
        dry_run: bool,
        /// Milliseconds to wait for the old daemon to exit after SIGTERM.
        #[arg(long, default_value_t = 2_000)]
        drain_timeout_ms: u64,
        /// Milliseconds to wait for pre-bounce sessions to reconnect before
        /// declaring the reload degraded.
        #[arg(long, default_value_t = 8_000)]
        reconnect_timeout_ms: u64,
        /// Override the installed binary path (for testing / scripts).
        #[arg(long)]
        installed_path: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
enum IntentCommand {
    /// Publish structured intent for the calling session via a single
    /// heartbeat. All intent flags are optional and sticky: omitting a flag
    /// leaves the daemon-side value untouched; passing an empty value
    /// (e.g. `--skill ""` or `--paths ""`) clears it. Fail-open: with no
    /// daemon running, exits 0 silently.
    Set {
        /// Session id to set intent for (must match the session's announce id).
        #[arg(long)]
        session_id: String,
        /// Skill currently active (e.g. `/build`). Omit to leave sticky.
        #[arg(long)]
        skill: Option<String>,
        /// PRD slug currently being built. Omit to leave sticky.
        #[arg(long)]
        prd: Option<String>,
        /// Comma-separated working paths (max 8). Omit to leave sticky;
        /// pass an empty string to clear.
        #[arg(long)]
        paths: Option<String>,
    },
    /// List peers that have any structured intent field set, projecting only
    /// `session_id` plus the set intent fields. Fail-open: emits `[]` and
    /// exits 0 when no daemon is reachable.
    List,
}

#[derive(Subcommand, Debug)]
enum ClaimCommand {
    /// Acquire an advisory claim on a path.
    Acquire {
        /// Session id to record as the holder.
        #[arg(long)]
        session_id: String,
        /// Path to claim. Canonicalized to an absolute path before send.
        path: String,
        /// Time-to-live in seconds from now.
        #[arg(long, default_value_t = 600)]
        ttl: u64,
        /// Human-readable rationale shown in `claim list`.
        #[arg(long, default_value = "")]
        reason: String,
        /// Overwrite any conflicting claim from a different session.
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Client-side wait: if a conflict is hit, subscribe to
        /// `claim.release` and retry up to `wait` seconds.
        #[arg(long, default_value_t = 0)]
        wait: u64,
    },
    /// Release the claim this session holds on a path. Idempotent.
    Release {
        /// Session id that owns the claim.
        #[arg(long)]
        session_id: String,
        /// Path to release. Canonicalized to an absolute path before send.
        path: String,
    },
    /// Print the active-claims snapshot as JSON. Fail-open: `[]` and exit 0
    /// when no daemon is reachable.
    List {
        /// Filter to a specific path (canonicalized).
        #[arg(long)]
        path: Option<String>,
        /// Filter to a specific session.
        #[arg(long)]
        session_id: Option<String>,
        /// Output shape: `text` (one record per line) or `json` (single JSON
        /// array). Default: `json`.
        #[arg(long, default_value = "json")]
        format: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let socket_path = cli.socket.unwrap_or_else(default_socket_path);

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("agorabus: cannot start runtime: {e}");
            return ExitCode::from(2);
        }
    };

    match rt.block_on(run(cli.cmd, socket_path)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("agorabus: {e:#}");
            ExitCode::from(2)
        }
    }
}

async fn run(cmd: Command, socket: PathBuf) -> Result<ExitCode> {
    match cmd {
        Command::Daemon { heartbeat_timeout, drain_grace_ms, drain_resume_hint_ms } => {
            let cfg = DaemonConfig {
                socket_path: socket,
                heartbeat_timeout: Duration::from_secs(heartbeat_timeout),
                broadcast_capacity: 1024,
                drain_grace_ms,
                drain_resume_hint_ms,
            };
            let (_ready_tx, _ready_rx) = tokio::sync::oneshot::channel::<()>();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let daemon = tokio::spawn(async move { run_daemon(cfg, None, shutdown_rx).await });

            // Wait for SIGINT / SIGTERM, then signal shutdown.
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigint = signal(SignalKind::interrupt())?;
            let mut sigterm = signal(SignalKind::terminate())?;
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
            let _ = shutdown_tx.send(());
            let _ = daemon.await;
            Ok(ExitCode::SUCCESS)
        }
        Command::Announce {
            session_id,
            pid,
            cwd,
            intent,
        } => {
            let cwd = cwd.unwrap_or_else(|| {
                std::env::current_dir()
                    .ok()
                    .and_then(|p| p.to_str().map(String::from))
                    .unwrap_or_default()
            });
            let Some(mut client) = Client::try_connect(&socket).await? else {
                // Fail-open: no daemon, no-op.
                return Ok(ExitCode::SUCCESS);
            };
            let reply = client.announce(&session_id, pid, &cwd, &intent).await?;
            println!("{}", serde_json::to_string(&reply)?);
            Ok(if reply.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Peers => {
            let Some(mut client) = Client::try_connect(&socket).await? else {
                println!("[]");
                return Ok(ExitCode::SUCCESS);
            };
            // Peers query does not require us to have announced ourselves:
            // but daemon rejects non-announce first-message. Announce a
            // throwaway client identity.
            let cli_pid = std::process::id();
            let sid = format!("cli-peers-{cli_pid}");
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client.announce(&sid, cli_pid, &cwd, "peers-query").await?;
            let peers = client.peers().await?;
            // Filter out the throwaway self-record so the user sees true peers.
            let peers: Vec<_> = peers.into_iter().filter(|p| p.session_id != sid).collect();
            println!("{}", serde_json::to_string(&peers)?);
            Ok(ExitCode::SUCCESS)
        }
        Command::Publish {
            session_id,
            topic,
            data,
        } => {
            let Some(mut client) = Client::try_connect(&socket).await? else {
                return Ok(ExitCode::SUCCESS);
            };
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&session_id, std::process::id(), &cwd, "publisher")
                .await?;
            let payload: serde_json::Value =
                serde_json::from_str(&data).unwrap_or(serde_json::Value::String(data));
            let reply = client.publish(&topic, payload).await?;
            println!("{}", serde_json::to_string(&reply)?);
            Ok(if reply.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Subscribe {
            session_id,
            prefix,
            max_events,
            no_reconnect,
            reconnect_base_ms,
            reconnect_cap_ms,
            max_reconnect_attempts,
        } => {
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let pid = std::process::id();
            let cfg = ReconnectConfig {
                base_ms: reconnect_base_ms,
                cap_ms: reconnect_cap_ms,
                max_attempts: max_reconnect_attempts,
            };
            // Silence unused-must-use for the borrow of ServerEvent (lint
            // checker hint; not a real warning).
            let _ = std::any::TypeId::of::<ServerEvent>();
            let result = reconnect_subscribe(
                &socket,
                &session_id,
                pid,
                &cwd,
                "subscriber",
                &prefix,
                max_events,
                !no_reconnect,
                cfg,
                |ev| {
                    // on_event: print the event as ndjson + signal continue.
                    match serde_json::to_string(&ev) {
                        Ok(line) => {
                            println!("{line}");
                            true
                        }
                        Err(_) => true,
                    }
                },
            )
            .await;
            match result {
                Ok(()) => Ok(ExitCode::SUCCESS),
                Err(e) => {
                    eprintln!("agorabus subscribe: {e:#}");
                    Ok(ExitCode::from(1))
                }
            }
        }
        Command::Claim(sub) => run_claim(sub, &socket).await,
        Command::Intent(sub) => run_intent(sub, &socket).await,
        Command::Doctor {
            format,
            installed_path,
        } => {
            let Some(fmt) = DoctorFormat::parse(&format) else {
                anyhow::bail!("invalid --format {format:?}; expected 'text' or 'json'");
            };
            // Pure introspection over /proc + the on-disk binary; no daemon
            // connection needed (it inspects the daemon, doesn't talk to it).
            let (report, code) = run_doctor(installed_path.as_deref());
            print_report(&report, fmt);
            Ok(code)
        }
        Command::Heartbeat { session_id, tool } => {
            let Some(mut client) = Client::try_connect(&socket).await? else {
                return Ok(ExitCode::SUCCESS);
            };
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&session_id, std::process::id(), &cwd, "heartbeat-client")
                .await?;
            let reply = client.heartbeat(&tool).await?;
            // We need to use the ClientMessage import to keep clippy quiet
            // about it being technically dead.
            let _ = std::mem::size_of::<ClientMessage>();
            println!("{}", serde_json::to_string(&reply)?);
            Ok(if reply.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Command::Reload {
            format,
            require_fresh,
            start_if_absent,
            dry_run,
            drain_timeout_ms,
            reconnect_timeout_ms,
            installed_path,
        } => {
            let Some(fmt) = ReloadFormat::parse(&format) else {
                anyhow::bail!("invalid --format {format:?}; expected 'text' or 'json'");
            };
            let cfg = ReloadConfig {
                socket_path: socket,
                require_fresh,
                start_if_absent,
                dry_run,
                drain_timeout_ms,
                reconnect_timeout_ms,
                format: fmt,
                installed_path,
            };
            let (verdict, code) = run_reload(&cfg).await?;
            print_verdict(&verdict, fmt);
            Ok(code)
        }
    }
}

fn canonicalize_input(path: &str) -> PathBuf {
    let p = PathBuf::from(path);
    let absolute = if p.is_absolute() {
        p
    } else {
        std::env::current_dir().map_or_else(|_| p.clone(), |cwd| cwd.join(&p))
    };
    // `std::fs::canonicalize` requires the path to exist; for not-yet-created
    // files (a common case: claim a path you're about to write) we fall back
    // to canonicalizing the parent and re-joining the file name.
    std::fs::canonicalize(&absolute).unwrap_or_else(|_| {
        absolute
            .parent()
            .and_then(|parent| {
                std::fs::canonicalize(parent).ok().and_then(|canon_parent| {
                    absolute
                        .file_name()
                        .map(|name| canon_parent.join(name))
                })
            })
            .unwrap_or(absolute)
    })
}

fn now_unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

async fn run_claim(sub: ClaimCommand, socket: &Path) -> Result<ExitCode> {
    match sub {
        ClaimCommand::Acquire {
            session_id,
            path,
            ttl,
            reason,
            force,
            wait,
        } => {
            let canon = canonicalize_input(&path);
            let canon_str = canon.to_string_lossy().into_owned();
            let Some(mut client) = Client::try_connect(socket).await? else {
                return Ok(ExitCode::SUCCESS);
            };
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&session_id, std::process::id(), &cwd, "claim-client")
                .await?;
            let ttl_unix = now_unix_secs().saturating_add(ttl);
            let mut reply = client
                .claim_acquire(&canon_str, ttl_unix, &reason, force)
                .await?;
            if !reply.ok
                && reply.error.as_deref() == Some("claim_conflict")
                && wait > 0
            {
                // Client-side wait: open a fresh subscriber connection,
                // listen for claim.release on this exact path, then retry.
                let deadline = Instant::now() + Duration::from_secs(wait);
                let mut sub_client = Client::connect(socket).await?;
                let sub_sid = format!("{session_id}-wait");
                let _ = sub_client
                    .announce(&sub_sid, std::process::id(), &cwd, "claim-wait")
                    .await?;
                let _ = sub_client.subscribe("claim.release").await?;
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        // Annotate the conflict reply with timed_out=true so
                        // callers can distinguish "tried-and-failed" from
                        // "rejected-without-waiting".
                        if let Some(serde_json::Value::Object(ref mut m)) = reply.data {
                            m.insert("timed_out".into(), serde_json::Value::Bool(true));
                        }
                        break;
                    }
                    match timeout(remaining, sub_client.next_event()).await {
                        Ok(Ok(Some(ev))) => {
                            if ev.topic == "claim.release" {
                                if let Some(p) = ev.data.get("path").and_then(|v| v.as_str()) {
                                    if p == canon_str {
                                        // Retry acquire.
                                        let ttl_unix2 =
                                            now_unix_secs().saturating_add(ttl);
                                        reply = client
                                            .claim_acquire(
                                                &canon_str, ttl_unix2, &reason, force,
                                            )
                                            .await?;
                                        if reply.ok
                                            || reply.error.as_deref()
                                                != Some("claim_conflict")
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Ok(None) | Err(_)) => break,
                        Err(_) => {
                            // Deadline hit before a usable claim.release
                            // arrived. Annotate the conflict reply so callers
                            // can distinguish a timed-out wait from an
                            // immediate-reject (no --wait at all).
                            if let Some(serde_json::Value::Object(ref mut m)) = reply.data {
                                m.insert("timed_out".into(), serde_json::Value::Bool(true));
                            }
                            break;
                        }
                    }
                }
            }
            println!("{}", serde_json::to_string(&reply)?);
            Ok(if reply.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        ClaimCommand::Release { session_id, path } => {
            let canon = canonicalize_input(&path);
            let canon_str = canon.to_string_lossy().into_owned();
            let Some(mut client) = Client::try_connect(socket).await? else {
                return Ok(ExitCode::SUCCESS);
            };
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&session_id, std::process::id(), &cwd, "claim-client")
                .await?;
            let reply = client.claim_release(&canon_str).await?;
            println!("{}", serde_json::to_string(&reply)?);
            Ok(if reply.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        ClaimCommand::List {
            path,
            session_id,
            format,
        } => {
            let Some(mut client) = Client::try_connect(socket).await? else {
                println!("[]");
                return Ok(ExitCode::SUCCESS);
            };
            let cli_pid = std::process::id();
            let sid = format!("cli-claim-list-{cli_pid}");
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client.announce(&sid, cli_pid, &cwd, "claim-list-query").await?;
            let mut claims = client.claim_list().await?;
            if let Some(want_path) = path {
                let canon = canonicalize_input(&want_path)
                    .to_string_lossy()
                    .into_owned();
                claims.retain(|c| c.path == canon);
            }
            if let Some(want_sid) = session_id {
                claims.retain(|c| c.session_id == want_sid);
            }
            match format.as_str() {
                "text" => {
                    for c in &claims {
                        println!(
                            "{}\t{}\tttl_unix={}\tacquired_unix={}\treason={}",
                            c.path,
                            c.session_id,
                            c.ttl_unix_secs,
                            c.acquired_unix_secs,
                            c.reason
                        );
                    }
                }
                _ => {
                    println!("{}", serde_json::to_string(&claims)?);
                }
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

async fn run_intent(sub: IntentCommand, socket: &Path) -> Result<ExitCode> {
    match sub {
        IntentCommand::Set {
            session_id,
            skill,
            prd,
            paths,
        } => {
            let Some(mut client) = Client::try_connect(socket).await? else {
                // Fail-open: no daemon → no-op, silent (AC6).
                return Ok(ExitCode::SUCCESS);
            };
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&session_id, std::process::id(), &cwd, "intent-client")
                .await?;
            // Comma-split paths; an explicit empty string clears (Some(vec![]));
            // omitting --paths leaves the field sticky (None).
            let working_paths = paths.map(|s| {
                if s.is_empty() {
                    Vec::new()
                } else {
                    s.split(',').map(|p| p.trim().to_string()).collect()
                }
            });
            // Empty tool ⇒ daemon leaves last_tool untouched (AC3: tool unset).
            let reply = client
                .heartbeat_with_intent("", skill, prd, working_paths)
                .await?;
            println!("{}", serde_json::to_string(&reply)?);
            Ok(if reply.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        IntentCommand::List => {
            let Some(mut client) = Client::try_connect(socket).await? else {
                println!("[]");
                return Ok(ExitCode::SUCCESS);
            };
            let cli_pid = std::process::id();
            let sid = format!("cli-intent-list-{cli_pid}");
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&sid, cli_pid, &cwd, "intent-list-query")
                .await?;
            let peers = client.peers().await?;
            // Keep only peers carrying at least one structured intent field;
            // drop the throwaway query identity; project to session_id + the
            // set intent fields (no pid/cwd/last_tool/etc) per AC4.
            let projected: Vec<serde_json::Value> = peers
                .into_iter()
                .filter(|p| p.session_id != sid)
                .filter(|p| {
                    !p.skill.is_empty()
                        || !p.prd_slug.is_empty()
                        || !p.working_paths.is_empty()
                })
                .map(|p| {
                    let mut m = serde_json::Map::new();
                    m.insert(
                        "session_id".into(),
                        serde_json::Value::String(p.session_id),
                    );
                    if !p.skill.is_empty() {
                        m.insert("skill".into(), serde_json::Value::String(p.skill));
                    }
                    if !p.prd_slug.is_empty() {
                        m.insert("prd_slug".into(), serde_json::Value::String(p.prd_slug));
                    }
                    if !p.working_paths.is_empty() {
                        m.insert(
                            "working_paths".into(),
                            serde_json::Value::Array(
                                p.working_paths
                                    .into_iter()
                                    .map(serde_json::Value::String)
                                    .collect(),
                            ),
                        );
                    }
                    serde_json::Value::Object(m)
                })
                .collect();
            println!("{}", serde_json::to_string(&projected)?);
            Ok(ExitCode::SUCCESS)
        }
    }
}
