# devlink

A small local bridge/relay server: one Node process that links a controller
to a client-side script over a dual transport (WebSocket + HTTP long-poll).
**Current state: full planned surface built** — transport (1A), pre-flight
gate (1B), execute + client status (1C), and the structure / remote / script /
GUI / interaction waves. Every op runs through the same gate.

## Pre-flight gate (1B)

Every op passes through `sendOp`, and every `sendOp` first scans the op +
payload against `crash_blacklist.json`. A match is refused with
`blocked_by_preflight` and **never reaches the bridge**. The list is
versioned: `version` + `last_modified` live in the file, every change is
recorded in `crash_blacklist.changelog.md`, and `/api/status` surfaces
`blacklist_version` so a stale or silently-edited list is visible.
The file is re-read automatically when it changes (mtime check); a malformed
file at startup is fatal — a broken safety gate never silently turns off.

## Run it

1. `start.bat` (installs deps on first run, generates `.dl-secret`).
2. Load the client bridge from your host environment:
   `loadstring(game:HttpGet("http://127.0.0.1:28432/bridge.lua"))()`
3. The bridge connects on its own and reconnects after session drops.

## Shape

One Node process, two listeners, two trust models:

| Channel | Bind | Port | Trust model |
|---|---|---|---|
| API (controller) | Tailscale-facing | 28431 | Pre-shared secret in `.dl-secret` → handshake → session token |
| Bridge (client) | **127.0.0.1 only** | 28432 | Trust-by-binding; per-session nonce (a nonce, not a credential) |

- Protocol version `1.0` on both sides of the handshake; a major mismatch is
  refused with both versions named.
- Heartbeat 3s; a session is declared dead after 10s of silence — only then
  can a fresh handshake replace it. A bridge that still holds its nonce can
  resume instantly.
- Single active session in v1.
- Startup is fail-fast: if either listener cannot bind, the process exits
  naming the listener (port + interface) instead of running half-alive.
- If a call is in flight when the bridge dies, the caller gets
  `unknown_outcome` — never a guessed "failed".

## Ops

All ops: `POST /api/op/<name>` with a JSON payload (Bearer token required).
Plus `GET /api/status`, `POST /api/session/ping`, and the snapshot trio
`POST /api/session/snapshot` · `POST /api/session/compare` ·
`POST /api/session/restore` (schema v1; restore touches only values the
snapshot recorded as tracked — best-effort, verify with a compare).

Bridge ops (54): ping, test_hang, execute, client_status, players, processes,
replication, resolve_path, get_children, get_services, find_instances,
walk_tree, read_properties, find_by_property, find_by_tag, find_by_attribute,
scan_proximity, scan_nil, hidden_property, list_remotes, map_remote_topology,
spy_remote, remote_handlers, fire_remote, running_scripts, loaded_modules,
decompile_deep, script_env, roblox_env, inspect_closure, debug_info, scan_gc,
scan_registry, unc_check, sandbox, gui_tree, screen_text, world_to_screen,
screenshot, video, console, files_deep, click_button, fire_signal,
interact_prompts, send_chat, move, set_position, camera, set_player,
get_tracked, restore_state, anti_afk, join_game.

Notes on honesty: `spy_remote` is READ-ONLY and passive (extra signal
listeners; no hooks) — it sees incoming RemoteEvent/BindableEvent traffic
only. `screenshot`, `video`, and `processes` answer `supported: false` with a
reason instead of faking output; executor-dependent ops (decompile, getgc…)
do the same when the client lacks the capability.

## Tests

`npm test` — spawns the real server + a fake bridge and checks the protocol:
handshake auth (API secret + bridge version), session rules, nonce rejection,
dead-session replacement, dual transport (WebSocket + HTTP long-poll),
in-flight `unknown_outcome`, and fail-fast startup per listener.

## Security notes

- `.dl-secret` stays on this machine (shared once with the controller over
  the tailnet) and is never committed (see `.gitignore`).
- The bridge port must never be exposed beyond localhost. There is no
  "remote bridge" mode; adding one is a separate decision, not a setting.
