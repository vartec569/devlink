# devlink

A small local bridge/relay server: one Node process that links a controller
to a client-side script over a dual transport (WebSocket + HTTP long-poll).
**Current state: Phase 1A — transport foundation only.** No application
tools yet; 1B adds a pre-flight safety filter, 1C adds the first real ops.

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
