# agorabus

> Concurrent Claude sessions on the same laptop are mutually blind, leading to clobbered shared files (settings.json, recall DB) and redundant work.

## Install

### One-liner

```sh
curl -fsSL https://raw.githubusercontent.com/j0yen/agorabus/main/install.sh | bash
```

### Manual

```sh
git clone --depth 1 https://github.com/j0yen/agorabus.git
cd agorabus
./install.sh
```

Installs the `agorabus` binary via `cargo install --path . --locked`. Requires `cargo` / `rustc 1.85+` and `git`. Built binary lands in `~/.cargo/bin/`.

## Why

Concurrent Claude sessions on the same laptop are mutually blind, leading to clobbered shared files (settings.json, recall DB) and redundant work. agorabus provides an advisory presence+pub/sub substrate over a Unix-domain socket so co-located sessions can announce themselves, see peers, and exchange low-volume coordination events.

## Build

```sh
cargo build --release
```

Produces `target/release/agorabus`. Symlink into `~/.local/bin/` if you want it on `$PATH`.

## Usage

```sh
agorabus --help
```

## Audience

Joe Yen running multiple concurrent Claude Code sessions on a single Linux laptop (single-user trust model). The CLI is invoked by SessionStart/Stop hooks and by Claude itself during a session for ad-hoc peer queries.

## Acceptance criteria

This project was scaffolded from a PRD via the `autobuilder` pipeline. The MUST-level acceptance criteria are:

- **AC1**: `agorabus daemon` starts a UDS server at ~/.cache/agorabus/sock (or path from --socket), creates parent dirs with 0700, sets the socket file mode to 0600, and accepts at least one client connection.
- **AC2**: Newline-delimited JSON protocol: the first message from a client must be an `announce` op carrying session_id, pid, cwd; the daemon replies with {"ok":true} and records the peer. Non-announce first messages get {"ok":false,"error":"annou...
- **AC3**: `agorabus peers --socket <path>` returns a JSON array of currently-connected peers with their announce records (session_id, pid, cwd, intent, last_heartbeat).
- **AC4**: Heartbeat semantics: a client may send {"op":"heartbeat","tool":"..."} at any time; the daemon updates the peer's last_heartbeat. Peers with no heartbeat for more than the configured timeout (default 60s, override via --heartbeat-timeout...
- **AC5**: Pub/sub: a client subscribed via {"op":"subscribe","prefix":"shared."} receives every subsequent {"op":"publish","topic":"shared.X","data":...} whose topic begins with that prefix. Streamed as one JSON object per line on the subscriber's...
- **AC6**: Fail-open client: `agorabus peers` with no daemon running exits 0 and emits `[]` on stdout (PRD risk-mitigation requirement). All client subcommands treat connection-refused / no-socket as 'no peers / no bus' rather than as a hard error.

Each AC has a matching integration test under `tests/acceptance_ac<n>.rs`.

## Provenance

Built via the [`autobuilder`](https://github.com/j0yen/autobuilder) pipeline (PRD intake -> intent-card -> scaffold -> iterate-and-prove). Originally consolidated as a subdir of the [`wintermute`](https://github.com/j0yen/wintermute) monorepo; this standalone repo is a fresh-init snapshot for easier consumption and distribution.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
