//! Client: connects to a daemon socket and exchanges line-framed JSON.
//!
//! All client constructors are fail-open-friendly via [`Client::try_connect`]:
//! the caller decides whether to treat connect failure as "no bus / no peers"
//! (the CLI's `peers` subcommand does this) or as a hard error.

#![allow(
    // Same reasoning as daemon.rs: `T: Serialize` generic over &T is not
    // Sync-bounded; call sites all pass concrete Send+Sync types.
    clippy::future_not_send,
)]

use crate::protocol::{ClaimRecord, ClientMessage, PeerRecord, Reply, ServerEvent};
use anyhow::{Context as _, Result, anyhow};
use std::path::Path;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

/// A line received from the daemon on a subscribed connection: either a
/// streaming broadcast [`ServerEvent`] or a one-shot [`Reply`] to a request
/// the client sent (e.g. a periodic heartbeat). Long-lived subscribe loops
/// use this to demultiplex the two on the same wire.
#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum InboundLine {
    /// Routed broadcast event from a publisher.
    Event(ServerEvent),
    /// Reply to a request the client sent over this connection.
    Reply(Reply),
}

/// Send a single [`ClientMessage::Heartbeat`] over a raw write half. Used
/// by long-lived subscribe loops to refresh the daemon's
/// `last_heartbeat_unix_secs` without a reply read (the reply is consumed
/// out-of-band by [`Client::next_event`], which skips [`InboundLine::Reply`]).
///
/// # Errors
///
/// Returns `Err` on serialization failure or write/flush I/O failure.
pub async fn send_heartbeat(write: &mut OwnedWriteHalf, tool: &str) -> Result<()> {
    let msg = ClientMessage::Heartbeat {
        tool: tool.to_string(),
        skill: None,
        prd_slug: None,
        working_paths: None,
    };
    let mut buf = serde_json::to_vec(&msg).context("serializing heartbeat")?;
    buf.push(b'\n');
    write.write_all(&buf).await.context("send_heartbeat write")?;
    write.flush().await.context("send_heartbeat flush")?;
    Ok(())
}

/// A connected bus client.
pub struct Client {
    write: tokio::net::unix::OwnedWriteHalf,
    reader: tokio::io::Lines<BufReader<OwnedReadHalf>>,
}

impl Client {
    /// Connect to the daemon at `socket_path`. Returns `Ok(None)` if the
    /// socket does not exist or refuses (fail-open). Returns `Err` only on
    /// non-bus errors (e.g. transient I/O after connect).
    ///
    /// # Errors
    ///
    /// Returns `Err` for unexpected I/O failures during connect. Fail-open
    /// classes (file-not-found, connection-refused) map to `Ok(None)`.
    pub async fn try_connect(socket_path: &Path) -> Result<Option<Self>> {
        match UnixStream::connect(socket_path).await {
            Ok(stream) => Ok(Some(Self::from_stream(stream))),
            Err(e) => {
                use std::io::ErrorKind;
                match e.kind() {
                    ErrorKind::NotFound | ErrorKind::ConnectionRefused => Ok(None),
                    _ => Err(e).context(format!(
                        "connecting to bus at {}",
                        socket_path.display()
                    )),
                }
            }
        }
    }

    /// Strict connect — propagate any error.
    ///
    /// # Errors
    ///
    /// Returns any underlying I/O error from `UnixStream::connect`.
    pub async fn connect(socket_path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket_path).await.with_context(|| {
            format!("connecting to bus at {}", socket_path.display())
        })?;
        Ok(Self::from_stream(stream))
    }

    fn from_stream(stream: UnixStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            write: write_half,
            reader: BufReader::new(read_half).lines(),
        }
    }

    /// Send a message, expecting a single one-line `Reply`. Returns the
    /// parsed Reply.
    ///
    /// # Errors
    ///
    /// Returns `Err` on I/O failure, on the daemon closing the connection
    /// before replying, or on a malformed reply.
    pub async fn request(&mut self, msg: &ClientMessage) -> Result<Reply> {
        self.send_line(msg).await?;
        self.read_reply().await
    }

    /// Send the initial announce + return the reply.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn announce(
        &mut self,
        session_id: &str,
        pid: u32,
        cwd: &str,
        intent: &str,
    ) -> Result<Reply> {
        let msg = ClientMessage::Announce {
            session_id: session_id.to_string(),
            pid,
            cwd: cwd.to_string(),
            intent: intent.to_string(),
        };
        self.request(&msg).await
    }

    /// Send a heartbeat with the optional last-tool name.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn heartbeat(&mut self, tool: &str) -> Result<Reply> {
        let msg = ClientMessage::Heartbeat {
            tool: tool.to_string(),
            skill: None,
            prd_slug: None,
            working_paths: None,
        };
        self.request(&msg).await
    }

    /// Send a heartbeat carrying optional structured intent fields. Pass
    /// `Some("")` (or `Some(vec![])`) to explicitly clear a field;
    /// `None` leaves the daemon-side value sticky.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn heartbeat_with_intent(
        &mut self,
        tool: &str,
        skill: Option<String>,
        prd_slug: Option<String>,
        working_paths: Option<Vec<String>>,
    ) -> Result<Reply> {
        let msg = ClientMessage::Heartbeat {
            tool: tool.to_string(),
            skill,
            prd_slug,
            working_paths,
        };
        self.request(&msg).await
    }

    /// Ask for the current peers snapshot.
    ///
    /// # Errors
    ///
    /// Returns `Err` on I/O failure or if the reply payload does not decode
    /// as a peer list.
    pub async fn peers(&mut self) -> Result<Vec<PeerRecord>> {
        let reply = self.request(&ClientMessage::Peers {}).await?;
        if !reply.ok {
            return Err(anyhow!(
                "peers query failed: {}",
                reply.error.unwrap_or_else(|| "(no error tag)".into())
            ));
        }
        let data = reply.data.unwrap_or(serde_json::Value::Array(vec![]));
        let peers: Vec<PeerRecord> =
            serde_json::from_value(data).context("decoding peers payload")?;
        Ok(peers)
    }

    /// Publish on a topic; returns the one-shot reply.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn publish(&mut self, topic: &str, data: serde_json::Value) -> Result<Reply> {
        self.request(&ClientMessage::Publish {
            topic: topic.to_string(),
            data,
        })
        .await
    }

    /// Acquire an advisory claim on `path` (already canonicalized).
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn claim_acquire(
        &mut self,
        path: &str,
        ttl_unix_secs: u64,
        reason: &str,
        force: bool,
    ) -> Result<Reply> {
        self.request(&ClientMessage::ClaimAcquire {
            path: path.to_string(),
            ttl_unix_secs,
            reason: reason.to_string(),
            force,
        })
        .await
    }

    /// Release the claim this session holds on `path`. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn claim_release(&mut self, path: &str) -> Result<Reply> {
        self.request(&ClientMessage::ClaimRelease {
            path: path.to_string(),
        })
        .await
    }

    /// Snapshot of all currently-active claims.
    ///
    /// # Errors
    ///
    /// Returns `Err` on I/O failure or if the reply payload does not decode
    /// as a claim list.
    pub async fn claim_list(&mut self) -> Result<Vec<ClaimRecord>> {
        let reply = self.request(&ClientMessage::ClaimList {}).await?;
        if !reply.ok {
            return Err(anyhow!(
                "claim_list query failed: {}",
                reply.error.unwrap_or_else(|| "(no error tag)".into())
            ));
        }
        let data = reply.data.unwrap_or(serde_json::Value::Array(vec![]));
        let claims: Vec<ClaimRecord> =
            serde_json::from_value(data).context("decoding claim_list payload")?;
        Ok(claims)
    }

    /// Subscribe to a prefix. The initial Reply is returned, after which
    /// `next_event` yields one [`ServerEvent`] per publish.
    ///
    /// # Errors
    ///
    /// Returns any error from [`Self::request`].
    pub async fn subscribe(&mut self, prefix: &str) -> Result<Reply> {
        self.request(&ClientMessage::Subscribe {
            prefix: prefix.to_string(),
        })
        .await
    }

    /// Read the next streaming `ServerEvent`. Returns `Ok(None)` on EOF.
    /// Silently skips any [`InboundLine::Reply`] lines (e.g. replies to
    /// heartbeats interleaved with broadcast events on a subscribed wire).
    ///
    /// # Errors
    ///
    /// Returns `Err` on I/O failure or on a line that decodes as neither
    /// a `ServerEvent` nor a `Reply`.
    pub async fn next_event(&mut self) -> Result<Option<ServerEvent>> {
        loop {
            match self.reader.next_line().await? {
                Some(line) => {
                    let parsed: InboundLine = serde_json::from_str(&line)
                        .context("decoding inbound line")?;
                    match parsed {
                        InboundLine::Event(ev) => return Ok(Some(ev)),
                        InboundLine::Reply(_) => continue,
                    }
                }
                None => return Ok(None),
            }
        }
    }

    /// Consume the client and return the raw `(write_half, line_reader)`
    /// halves. Useful for subscribe loops that want to interleave reads
    /// with a periodic heartbeat task running on a separate tokio task.
    #[must_use]
    pub fn into_halves(self) -> (OwnedWriteHalf, tokio::io::Lines<BufReader<OwnedReadHalf>>) {
        (self.write, self.reader)
    }

    async fn send_line<T: serde::Serialize>(&mut self, value: &T) -> Result<()> {
        let mut buf = serde_json::to_vec(value).context("serializing client msg")?;
        buf.push(b'\n');
        self.write.write_all(&buf).await.context("send_line")?;
        self.write.flush().await.context("send_line flush")?;
        Ok(())
    }

    async fn read_reply(&mut self) -> Result<Reply> {
        match self.reader.next_line().await? {
            Some(line) => {
                let r: Reply = serde_json::from_str(&line).context("decoding Reply")?;
                Ok(r)
            }
            None => Err(anyhow!("daemon closed connection before replying")),
        }
    }
}
