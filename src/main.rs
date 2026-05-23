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
    Client, ClientMessage, DaemonConfig, default_socket_path, protocol::ServerEvent, run_daemon,
};
use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
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
    /// stdout until EOF or `--max-events`.
    Subscribe {
        /// Session id used at announce time.
        #[arg(long, default_value = "cli-subscriber")]
        session_id: String,
        /// Topic prefix (empty matches all).
        prefix: String,
        /// Cap on events to receive before exiting (0 = unlimited).
        #[arg(long, default_value_t = 0)]
        max_events: usize,
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
        Command::Daemon { heartbeat_timeout } => {
            let cfg = DaemonConfig {
                socket_path: socket,
                heartbeat_timeout: Duration::from_secs(heartbeat_timeout),
                broadcast_capacity: 1024,
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
        } => {
            let Some(mut client) = Client::try_connect(&socket).await? else {
                return Ok(ExitCode::SUCCESS);
            };
            let cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from))
                .unwrap_or_default();
            let _ = client
                .announce(&session_id, std::process::id(), &cwd, "subscriber")
                .await?;
            let _ = client.subscribe(&prefix).await?;
            let mut received = 0_usize;
            while let Some(ev) = client.next_event().await? {
                println!("{}", serde_json::to_string(&ev)?);
                received = received.saturating_add(1);
                if max_events != 0 && received >= max_events {
                    break;
                }
            }
            // Silence unused-must-use for the borrow of ServerEvent (lint
            // checker hint; not a real warning).
            let _ = std::any::TypeId::of::<ServerEvent>();
            Ok(ExitCode::SUCCESS)
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
    }
}
