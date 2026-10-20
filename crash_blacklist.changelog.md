# crash_blacklist changelog

Every change to `crash_blacklist.json` bumps its `version`, updates
`last_modified`, and adds one entry here: what changed, when, and why.
The server surfaces the current version in `/api/status` so a stale or
silently-edited list is visible instead of mysterious.

## v1 — 2026-10-20
- Created alongside the Phase 1B pre-flight gate.
- Seed entry `empty-infinite-loop` (regex): an empty `while true do end`
  loop freezes the client — the canonical accidental-crash pattern.
