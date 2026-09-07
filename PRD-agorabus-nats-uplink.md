# PRD: agorabus-nats-uplink — one bus across the fleet

- Status: building
- Lane: carbon 2026-09-07T02:41:53Z
- build_target: rust-extend
- build_into: /home/jsy/wintermute/agorabus
- build_priority: high
- build_version_bump: minor
- publish: j0yen/agorabus
- PM: Joe
- Drafted: 2026-09-06
- Vision: visions/fleet-autonomy.md
- Engineering target: extend ~/wintermute/agorabus in place (v0.12.0 → v0.13.0); the daemon gains an optional NATS uplink so local pub/sub reaches every fleet node through the existing nats-leaf → hub topology; the never-built wm-busbridge idea retires.

## TL;DR

agorabus is per-machine (UDS socket); NATS is the fleet transport (leaf per node → `nats.service` on hub :7422), but nothing connects them — the wm-busbridge unit is masked and empty, and no bridge repo exists. Merge them: the agorabus daemon optionally connects to the local NATS leaf and relays topics both ways, so `agorabus publish agent.activity …` on carbon is heard by a subscriber on redbaron with no new binary, no new daemon, and no new secrets (the leaf already holds hub credentials).

## Problem

Cross-host coordination today needs a second system. `agorabus peers` sees only local sessions; fleet presence topics (`wm.fleet.presence.announce/gone`) are defined in protocol.rs but never leave the machine. The planned bridge (wm-busbridge) was parked before it was built. Sessions on different nodes coordinate through SSH and git instead of the bus.

## Design constraints

- The uplink connects to the **local leaf** (default `nats://127.0.0.1:4222`), never directly to hub — the leaf owns hub auth, so agorabus carries no credentials.
- Local-only operation is untouched: no uplink config → daemon behaves exactly as v0.12.0. A dead leaf degrades to local-only with background reconnect (reuse the reconnect.rs backoff pattern).
- Relayed messages travel in an envelope carrying `origin_node` (default: hostname, lowercased) so loops are impossible: a daemon drops incoming messages whose origin is itself and never re-uplinks a message it received from the uplink.
- Subject mapping is mechanical: local topic `T` ⇄ NATS subject `wm.bus.T`; the daemon subscribes to `wm.bus.>`.

## Requirements

P0 (build these)
1. Parse optional uplink config at `~/.config/agorabus/uplink.toml` (`enabled`, `url`, `node`; defaults false / `nats://127.0.0.1:4222` / lowercased hostname). Absent file = uplink off.
2. Daemon connects to the uplink when enabled; relays local publishes to `wm.bus.<topic>` and delivers incoming `wm.bus.>` messages to local subscribers of the matching topic.
3. Loop prevention via `origin_node` envelope: self-origin messages dropped, uplink-received messages never re-uplinked.
4. Relay presence: local peer announce/gone republished on the uplink; remote peers tracked and shown in `agorabus peers` with a `node` field (local peers keep existing fields unchanged).
5. `agorabus doctor` reports uplink state (`disabled` / `connected <url>` / `reconnecting <err>`); uplink trouble is report-only and never fails doctor while the local bus is healthy.
6. Graceful degradation and reconnect: leaf down at start or mid-run → local bus unaffected, uplink retries with capped backoff.

P1
7. `agorabus peers --fleet` filters to remote peers; remote peers expire on TTL like local ones.
8. Integration harness spawns a throwaway `nats-server` (random port) plus two daemons (distinct UDS paths, node names `testa`/`testb`) — the round-trip and dedupe ACs run against real transport, not mocks.

P2
9. JetStream durable delivery for offline nodes — out of scope here; note it in README as the next step.

## Acceptance criteria

1. P0 — Given no uplink.toml exists, When the daemon starts and the pre-existing test suite runs, Then behavior matches v0.12.0 and all prior tests pass (uplink off by default).
2. P0 — Given an uplink.toml with any subset of enabled/url/node, When the daemon parses it, Then present fields are honored and missing ones take the documented defaults; Given a malformed file, When parsed, Then one error is logged and the daemon falls back to uplink-off rather than crashing.
3. P0 — Given uplink enabled and a reachable nats-server, When `agorabus doctor` runs, Then its output includes `uplink: connected` and the url.
4. P0 — Given uplink enabled and no nats-server listening, When the daemon starts, Then the local bus serves normally and doctor reports `uplink: reconnecting`; no panic, no exit.
5. P0 — Given a connected uplink, When the nats-server stops and later restarts, Then local pub/sub keeps working throughout and the uplink reconnects within 30s (observed via doctor polling in test).
6. P0 — Given a connected uplink, When a client publishes on local topic `T`, Then a message arrives on NATS subject `wm.bus.T` wrapped in an envelope containing `origin_node` and the original JSON payload.
7. P0 — Given two uplinked daemons A and B on one test nats-server, When a client publishes via A, Then a subscriber attached to B receives the payload exactly once.
8. P0 — Given the same two-daemon harness, When A publishes a 10-message burst, Then a subscriber on A receives no uplink echo of A's own messages and B's per-message delivery count stays 1 (no loop amplification).
9. P0 — Given both daemons connected, When `agorabus peers` runs on A, Then B's peers appear tagged `node=testb`; When B's daemon is killed, Then those peers disappear within the peer TTL.
10. P1 — Given remote peers are known, When `agorabus peers --fleet` runs, Then only remote peers are listed; When plain `peers` runs, Then local-peer output is byte-compatible with v0.12.0 apart from the added `node` field.
11. P0 — Given any uplink state (disabled, connected, reconnecting), When `agorabus doctor` runs, Then its exit code is unchanged by the uplink state (uplink lines are informational).
12. P1 — Given `nats-server` is absent from PATH, When the integration tests run, Then they skip with an explicit `skipped: nats-server not on PATH` message instead of passing vacuously; Given it is present, Then the harness spawns a real throwaway server on a random port.
13. P0 — Given the finished diff, When `cargo test` and clippy run, Then the version is 0.13.0, the suite is fully green, and clippy is clean on the diff.
14. P1 — Given the finished build, When the README is read, Then it documents uplink.toml, the `wm.bus.<topic>` subject scheme, the envelope, and the leaf-owns-credentials model.

## Non-goals

- JetStream durability / replay for offline nodes (follow-on PRD).
- Direct hub connections or credential handling inside agorabus.
- Deleting the masked wm-busbridge units from nodes (one-line ops cleanup, done by hand at deploy).
- Changing the wire protocol for local UDS clients.

## Deploy notes

Per node running nats-leaf (carbon, redbaron, ryzen7): write `~/.config/agorabus/uplink.toml` with `enabled = true` and the node name, then `agorabus reload` (or let agorabus-restart.path pick up the installed binary). Hub runs no agorabus. Verify with `agorabus doctor` on two nodes and one cross-node `agorabus publish` observed on the other node.
