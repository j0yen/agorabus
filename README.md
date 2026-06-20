# agorabus

An advisory presence and pub/sub bus that lets concurrent Claude sessions on one machine see each other and coordinate, over a Unix-domain socket.

## Why it exists

Two Claude sessions running on the same laptop are blind to each other. They share files — `settings.json`, the recall DB — and they overwrite each other's edits. They redo each other's work because neither knows the other exists. The fix doesn't need a message broker or a network service; it needs a place for co-located sessions to announce themselves, watch for peers, and pass small coordination events. That's agorabus: a single daemon on a Unix socket, advisory by design — it tells sessions about each other and lends out soft locks, but it never forces anyone to wait.

The trust model is single-user: one person, one machine, sessions that trust each other. The socket is `0600` under a `0700` directory, and that's the whole boundary.

## Install

```sh
git clone --depth 1 https://github.com/j0yen/agorabus.git
cd agorabus
./install.sh
```

`install.sh` runs `cargo install --path . --locked`, so the `agorabus` binary lands in `~/.cargo/bin/`. Needs `cargo` / `rustc` 1.88+ and `git`. There's also a one-liner that does the same:

```sh
curl -fsSL https://raw.githubusercontent.com/j0yen/agorabus/main/install.sh | bash
```

## Quickstart

Start the daemon, then look at who's connected:

```sh
agorabus daemon &                       # UDS at ~/.cache/agorabus/sock
agorabus peers                          # JSON array of current peers
```

`peers` is fail-open: with no daemon running it prints `[]` and exits 0, so a SessionStart hook that calls it never breaks a session. Announce, publish, and subscribe round out the core:

```sh
agorabus announce --session s1 --intent "building mqo-mcp"
agorabus subscribe --session s1 --prefix shared. &     # streams matching events, one JSON line each
agorabus publish --session s2 --topic shared.lock --data '{"file":"settings.json"}'
```

A subscriber survives a daemon bounce and re-registers its `session_id` on its own; pass `--no-reconnect` for one-shot scripted callers.

## Subcommands

| Command | What it does |
|---------|--------------|
| `daemon` | start the bus daemon (heartbeat timeout, drain grace, durable-state path are all flags) |
| `announce` | one-shot announce + disconnect (mostly for tests) |
| `peers` | list current peers as JSON; `--fleet` merges remote peers from `wm.fleet.presence.*` |
| `publish` | publish a JSON payload on a dotted topic |
| `subscribe` | stream events for a topic prefix, one JSON line each; auto-reconnects |
| `heartbeat` | send a single heartbeat; `--tool` records the last tool invoked |
| `claim` | advisory soft-locks on filesystem paths — `acquire` / `release` / `list` |
| `intent` | structured per-session intent (active skill, PRD slug, working paths) — `set` / `list` |
| `doctor` | compare the running daemon's image against the installed binary (0 current, 1 stale, 2 unknown) |
| `reload` | non-destructive daemon bounce; defaults to `--dry-run` |

### Claims

A claim is an advisory soft-lock on a path with a TTL — a way for one session to say "I'm editing `settings.json`, hold off." It's advisory: nothing enforces it, peers are expected to honor it.

```sh
agorabus claim acquire --session s1 --path ~/.claude/settings.json --ttl 30 --reason "editing hooks"
agorabus claim list
agorabus claim release --session s1 --path ~/.claude/settings.json
```

`--force` overwrites a conflicting claim from another session; `--wait N` subscribes to `claim.release` and retries for up to N seconds instead of failing immediately.

### Reload

`agorabus reload` rolls a running daemon without dropping its peers: it resolves the daemon pid, checks binary freshness, snapshots the peer set, SIGTERMs the old daemon, relaunches the fresh binary, and polls until the pre-bounce sessions reconnect. It emits a structured verdict — `{old_pid, new_pid, binary_before, binary_after, peers_before, peers_after, peers_recovered, peers_missing, elapsed_ms, status}`. The default posture is `--dry-run`, which prints the plan without touching anything; pass `--apply` (or `--no-dry-run`) to perform it. With `--build`, it recompiles through `cloudbuild.sh` first — never in-process cargo — and aborts without touching the daemon if the build fails.

## The ClaimGuard handle

For Rust callers, `ClaimGuard` ties a claim to a value's lifetime: it acquires the claim, auto-renews the TTL lease in the background, and releases on drop or via an explicit `release()`.

```rust
use agorabus::{Client, default_socket_path};
use std::time::Duration;

let socket = default_socket_path();
let mut client = Client::connect(&socket).await?;
client.announce("my-daemon", std::process::id(), "/", "hold-example").await?;
let guard = client.hold_claim("/dev/audio", Duration::from_secs(30), &socket, "my-daemon").await?;
// ... do work ...
guard.release().await?;  // or just drop(guard) for best-effort release
```

A renew failure (bus unreachable) is logged and retried on a fresh connection, so a transient bus blip never permanently drops the claim.

## How it works

Newline-delimited JSON over the socket. A client's first message must be an `announce` carrying `session_id`, `pid`, and `cwd`; the daemon records the peer and replies `{"ok":true}`. After that the client can `heartbeat`, `publish`, `subscribe`, claim paths, or set intent. Peers idle past the heartbeat timeout (default 60s) are pruned. On SIGTERM the daemon broadcasts `{"op":"bus.draining","resume_after_ms":N}` and waits a grace window for subscriber writes to flush before exiting. Claims and sticky intents are journaled to `~/.cache/agorabus/state.json` and rehydrated on restart, so a reload doesn't lose them.

## Where it fits

agorabus is the single-host coordination layer of the wintermute fleet. Local peers live on the UDS; with `--fleet`, `peers` merges remote peers carried over NATS `wm.fleet.presence.*` subjects (via agorabus-nats-bridge), each tagged with its node and a freshness age. It was built through the [autobuilder](https://github.com/j0yen/autobuilder) pipeline and originally lived as a subdirectory of the [wintermute](https://github.com/j0yen/wintermute) monorepo; this is a standalone snapshot.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
