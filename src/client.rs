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

use crate::protocol::{ClientMessage, PeerRecord, Reply, ServerEvent};
use anyhow::{Context as _, Result, anyhow};
use std::path::Path;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::OwnedReadHalf;

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
    ///
    /// # Errors
    ///
    /// Returns `Err` on I/O failure or on a malformed event line.
    pub async fn next_event(&mut self) -> Result<Option<ServerEvent>> {
        match self.reader.next_line().await? {
            Some(line) => {
                let ev: ServerEvent =
                    serde_json::from_str(&line).context("decoding ServerEvent")?;
                Ok(Some(ev))
            }
            None => Ok(None),
        }
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
