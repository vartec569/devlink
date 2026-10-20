// devlink — server (Phase 1A: transport foundation)
// Two listeners, two trust models (see plan doc):
//   API    : DL_API_PORT (default 28431), bind DL_API_BIND (default 0.0.0.0).
//            Trust = pre-shared secret (DL_SECRET / .dl-secret). Handshake issues a session token.
//   Bridge : DL_BRIDGE_PORT (default 28432), bind 127.0.0.1 ONLY (trust-by-binding).
//            Handshake issues a per-session nonce (a nonce, NOT a credential).
// Protocol: protocol_version "1.0"; heartbeat 3s, dead after 10s; single active session.
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PROTOCOL_VERSION = "1.0";
const SERVER_VERSION = "0.1.0";
const HEARTBEAT_MS = 3000;
const DEAD_AFTER_MS = 10000;

const API_PORT = parseInt(process.env.DL_API_PORT || "28431", 10);
const API_BIND = process.env.DL_API_BIND || "0.0.0.0";
const BRIDGE_PORT = parseInt(process.env.DL_BRIDGE_PORT || "28432", 10);
const BRIDGE_BIND = process.env.DL_BRIDGE_BIND || "127.0.0.1";

// ---------- pre-shared API secret ----------
function loadSecret() {
  if (process.env.DL_SECRET) return process.env.DL_SECRET.trim();
  const f = path.join(__dirname, ".dl-secret");
  try {
    const s = fs.readFileSync(f, "utf8").trim();
    if (s) return s;
  } catch (_) { /* fall through to generate */ }
  const s = crypto.randomBytes(24).toString("hex");
  fs.writeFileSync(f, s + "\n", { mode: 0o600 });
  console.log("[devlink] generated new API secret in .dl-secret (keep this file private)");
  return s;
}
const API_SECRET = loadSecret();
const apiTokens = new Set(); // issued API session tokens (in-memory; die with the process)

function secretEquals(a, b) {
  const ba = Buffer.from(String(a || ""));
  const bb = Buffer.from(String(b || ""));
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

// ---------- session state (v1: exactly ONE active bridge session) ----------
let session = null; // {id, nonce, protocolVersion, protocolNote, client, transport, ws, queue, pollWaiters, connectedAt, lastSeen, nonceStrikes}
let requestCounter = 0;
const pending = new Map(); // request_id -> {resolve, timer, op, startedAt}

// ---------- pre-flight crash blacklist (Phase 1B) ----------
// crash_blacklist.json is versioned + changelogged (see crash_blacklist.changelog.md).
// The gate sits in sendOp — the single funnel every op passes through — so a
// blocked payload NEVER reaches the bridge. Reload is by mtime check: editing
// the file takes effect without a restart; a malformed edit keeps the last
// good list and logs loudly; a malformed file at STARTUP is fatal (fail-fast:
// a corrupt safety file must never silently disable the gate).
const BLACKLIST_PATH = process.env.DL_BLACKLIST || path.join(__dirname, "crash_blacklist.json");
let blacklist = { version: 0, last_modified: null, entries: [], compiled: [] };
let blacklistMtime = -1;

function loadBlacklist(initial) {
  let stat;
  try { stat = fs.statSync(BLACKLIST_PATH); } catch (_) {
    if (initial) console.log("[devlink] no crash_blacklist.json found — pre-flight gate running with an EMPTY list");
    blacklist = { version: 0, last_modified: null, entries: [], compiled: [] };
    blacklistMtime = -1;
    return;
  }
  if (stat.mtimeMs === blacklistMtime) return;
  const parsed = JSON.parse(fs.readFileSync(BLACKLIST_PATH, "utf8"));
  if (typeof parsed.version !== "number" || !Array.isArray(parsed.entries)) {
    throw new Error("crash_blacklist.json: expected {version: number, entries: [...]}");
  }
  const compiled = parsed.entries.map((e) => {
    if (!e.id || !e.kind || !e.pattern) throw new Error("crash_blacklist.json: every entry needs id, kind, pattern");
    return { ...e, re: e.kind === "regex" ? new RegExp(e.pattern) : null };
  });
  blacklist = { version: parsed.version, last_modified: parsed.last_modified || null, entries: parsed.entries, compiled };
  blacklistMtime = stat.mtimeMs;
  console.log(`[devlink] crash blacklist loaded: v${blacklist.version} (${compiled.length} entries, modified ${blacklist.last_modified})`);
}
try {
  loadBlacklist(true);
} catch (e) {
  console.error(`[devlink] FATAL: crash_blacklist.json is malformed (${e.message}). Refusing to start with a broken safety gate.`);
  process.exit(1);
}

function preflight(op, payload) {
  try { loadBlacklist(false); } catch (e) {
    console.error(`[devlink] ERROR: crash_blacklist.json reload failed (${e.message}) — keeping v${blacklist.version}`);
  }
  if (!blacklist.compiled.length) return null;
  const text = op + "\n" + JSON.stringify(payload || {});
  for (const e of blacklist.compiled) {
    if (e.kind === "op" && e.pattern === op) return e;
    if (e.kind === "substring" && text.includes(e.pattern)) return e;
    if (e.kind === "regex" && e.re.test(text)) return e;
  }
  return null;
}

function nowMs() { return Date.now(); }

function resolvePending(id, value) {
  const p = pending.get(id);
  if (!p) return false;
  pending.delete(id);
  clearTimeout(p.timer);
  p.resolve(value);
  return true;
}

function killSession(reason) {
  if (!session) return;
  const s = session;
  session = null;
  // Every in-flight call becomes unknown_outcome — NOT error, NOT success.
  for (const [id] of [...pending]) {
    resolvePending(id, { ok: false, outcome: "unknown_outcome", reason: "bridge_lost:" + reason });
  }
  for (const w of s.pollWaiters.splice(0)) { try { w([]); } catch (_) {} }
  if (s.ws) { try { s.ws.close(); } catch (_) {} }
  console.log(`[devlink] session ${s.id} dead (${reason})`);
}

function parseVersion(v) {
  const m = /^(\d+)\.(\d+)$/.exec(String(v || ""));
  return m ? { major: +m[1], minor: +m[2] } : null;
}

// Shared hello logic for WS + HTTP transports.
// body: {protocol_version, client, session_nonce?}  ->  {status, json}
function handleHello(body) {
  const clientVer = parseVersion(body && body.protocol_version);
  const serverVer = parseVersion(PROTOCOL_VERSION);
  if (!clientVer) {
    return { status: 400, json: { refused: true, reason: "missing_or_bad_protocol_version", server_protocol_version: PROTOCOL_VERSION } };
  }
  if (clientVer.major !== serverVer.major) {
    return { status: 426, json: { refused: true, reason: "protocol_version_mismatch", server_protocol_version: PROTOCOL_VERSION, client_protocol_version: body.protocol_version } };
  }
  // Resume path: same session presenting its nonce = same bridge reconnecting (teleport/re-inject).
  if (session && body.session_nonce && body.session_nonce === session.nonce) {
    session.lastSeen = nowMs();
    return { status: 200, json: { session_id: session.id, session_nonce: session.nonce, protocol_version: PROTOCOL_VERSION, resumed: true } };
  }
  if (session) {
    if (nowMs() - session.lastSeen <= DEAD_AFTER_MS) {
      return { status: 409, json: { refused: true, reason: "session_already_active", server_protocol_version: PROTOCOL_VERSION } };
    }
    killSession("replaced_after_death");
  }
  const s = {
    id: "sess-" + crypto.randomBytes(4).toString("hex"),
    nonce: crypto.randomBytes(16).toString("hex"),
    protocolVersion: body.protocol_version,
    protocolNote: clientVer.minor !== serverVer.minor ? `minor version differs (client ${body.protocol_version}, server ${PROTOCOL_VERSION})` : null,
    client: String(body.client || "unknown"),
    transport: null,
    ws: null,
    queue: [],
    pollWaiters: [],
    connectedAt: nowMs(),
    lastSeen: nowMs(),
    nonceStrikes: 0,
  };
  session = s;
  console.log(`[devlink] session ${s.id} connected (client: ${s.client}, protocol ${s.protocolVersion})`);
  return { status: 200, json: { session_id: s.id, session_nonce: s.nonce, protocol_version: PROTOCOL_VERSION, resumed: false } };
}

function checkNonce(s, nonce) {
  if (!s) return false;
  if (nonce && nonce === s.nonce) { s.lastSeen = nowMs(); return true; }
  s.nonceStrikes += 1;
  console.log(`[devlink] bad session nonce from bridge (strike ${s.nonceStrikes})`);
  if (s.nonceStrikes >= 3) killSession("bad_nonce");
  return false;
}

function deliverToBridge(msg) {
  const s = session;
  if (!s) return false;
  if (s.ws && s.ws.readyState === 1) {
    s.ws.send(JSON.stringify(msg));
    return true;
  }
  s.queue.push(msg);
  for (const w of s.pollWaiters.splice(0)) { try { w(s.queue.splice(0)); } catch (_) {} }
  return true;
}

// Timeout classes (locked in the plan): fast reads 5s, standard ops 15s,
// heavy analysis 60s, screenshot 20s, video = duration + 30s buffer.
const OP_TIMEOUTS = {
  ping: 5000, client_status: 5000,
  execute: 15000, fire_remote: 15000,
  decompile_deep: 60000, scan_gc: 60000, scan_registry: 60000, files_deep: 60000,
  screenshot: 20000,
};
function opTimeout(op) { return OP_TIMEOUTS[op] || 15000; }

// Send an op to the bridge and await its result under the failure taxonomy.
// timeoutMs per tool class (fast reads 5s, standard 15s, heavy 60s).
function sendOp(op, payload, timeoutMs) {
  return new Promise((resolve) => {
    const hit = preflight(op, payload);
    if (hit) {
      console.log(`[devlink] pre-flight BLOCKED op "${op}" by blacklist entry "${hit.id}" (v${blacklist.version})`);
      resolve({ ok: false, outcome: "blocked_by_preflight", entry: hit.id, reason: hit.reason });
      return;
    }
    if (!session) { resolve({ ok: false, outcome: "no_session" }); return; }
    const id = "req-" + (++requestCounter) + "-" + crypto.randomBytes(3).toString("hex");
    const startedAt = nowMs();
    const timer = setTimeout(() => {
      resolvePending(id, { ok: false, outcome: "timeout", op, timeout_ms: timeoutMs });
    }, timeoutMs);
    pending.set(id, { resolve, timer, op, startedAt });
    deliverToBridge({ type: "op", request_id: id, op, payload: payload || {}, timeout_ms: timeoutMs });
  });
}

function handleOpResult(msg) {
  const p = pending.get(msg.request_id);
  if (!p) return; // response with no matching request_id = discarded, never processed
  const latency = nowMs() - p.startedAt;
  if (msg.ok) resolvePending(msg.request_id, { ok: true, data: msg.data || {}, latency_ms: latency });
  else resolvePending(msg.request_id, { ok: false, outcome: "op_error", error: String(msg.error || "unknown bridge error"), latency_ms: latency });
}

// ---------- heartbeat / dead-session sweep (3s beat, dead at 10s, both channels) ----------
setInterval(() => {
  if (!session) return;
  if (session.ws && session.ws.readyState === 1) { try { session.ws.ping(); } catch (_) {} }
  if (nowMs() - session.lastSeen > DEAD_AFTER_MS) killSession("heartbeat_timeout");
}, 1000);

// ---------- helpers ----------
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => { data += c; if (data.length > 1024 * 1024) req.destroy(); });
    req.on("end", () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on("error", reject);
  });
}
function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}

// ---------- API server (Tailscale-facing; trust = pre-shared secret) ----------
const apiServer = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://localhost");
    if (req.method === "POST" && url.pathname === "/api/handshake") {
      const body = await readBody(req);
      if (!secretEquals(body.secret, API_SECRET)) { sendJson(res, 401, { ok: false, error: "bad_secret" }); return; }
      const token = crypto.randomBytes(24).toString("hex");
      apiTokens.add(token);
      sendJson(res, 200, { ok: true, token, protocol_version: PROTOCOL_VERSION, server_version: SERVER_VERSION });
      return;
    }
    const auth = /^Bearer\s+(.+)$/.exec(req.headers.authorization || "");
    if (!auth || !apiTokens.has(auth[1])) { sendJson(res, 401, { ok: false, error: "unauthorized" }); return; }

    if (req.method === "GET" && url.pathname === "/api/status") {
      sendJson(res, 200, {
        ok: true,
        server_version: SERVER_VERSION,
        protocol_version: PROTOCOL_VERSION,
        uptime_s: Math.floor(process.uptime()),
        heartbeat: { interval_ms: HEARTBEAT_MS, dead_after_ms: DEAD_AFTER_MS },
        blacklist: { version: blacklist.version, last_modified: blacklist.last_modified, entries: blacklist.entries.length },
        listeners: {
          api: { bind: API_BIND, port: API_PORT },
          bridge: { bind: BRIDGE_BIND, port: BRIDGE_PORT },
        },
        session: session ? {
          id: session.id,
          client: session.client,
          transport: session.transport,
          alive: nowMs() - session.lastSeen <= DEAD_AFTER_MS,
          connected_at: new Date(session.connectedAt).toISOString(),
          last_seen_ago_s: Math.round((nowMs() - session.lastSeen) / 100) / 10,
          protocol_version: session.protocolVersion,
          protocol_note: session.protocolNote,
        } : null,
      });
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/session/ping") {
      const result = await sendOp("ping", {}, 5000); // fast-read class budget
      sendJson(res, 200, result);
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/session/test-hang") {
      // Diagnostic op (bridge sleeps payload.ms then replies). Exists to prove
      // the timeout class and unknown_outcome live; not an application tool.
      const body = await readBody(req);
      const ms = Math.max(0, Math.min(60000, parseInt(body.ms || "10000", 10) || 0));
      const note = typeof body.note === "string" ? body.note.slice(0, 500) : undefined;
      const result = await sendOp("test_hang", note !== undefined ? { ms, note } : { ms }, 5000);
      sendJson(res, 200, result);
      return;
    }
    // Generic op endpoint (Phase 1C+): every bridge op is reachable here.
    // Timeout class comes from OP_TIMEOUTS; every call passes the pre-flight
    // gate inside sendOp — execute included, from its first day (Principle P4).
    const opMatch = /^\/api\/op\/([a-z0-9_]+)$/.exec(url.pathname);
    if (req.method === "POST" && opMatch) {
      const body = await readBody(req);
      const result = await sendOp(opMatch[1], body || {}, opTimeout(opMatch[1]));
      sendJson(res, 200, result);
      return;
    }
    sendJson(res, 404, { ok: false, error: "not_found" });
  } catch (e) {
    sendJson(res, 400, { ok: false, error: "bad_request" });
  }
});

// ---------- Bridge server (127.0.0.1 ONLY; trust = the bind itself) ----------
const bridgeLuaPath = path.join(__dirname, "bridge.lua");
const bridgeServer = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://localhost");
    if (req.method === "GET" && url.pathname === "/bridge.lua") {
      let code = "";
      try { code = fs.readFileSync(bridgeLuaPath, "utf8"); } catch (_) { /* served below as 404 */ }
      if (!code) { sendJson(res, 404, { ok: false, error: "bridge_lua_missing" }); return; }
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" });
      res.end(code);
      return;
    }
    if (req.method === "POST" && url.pathname === "/bridge/hello") {
      const body = await readBody(req);
      const r = handleHello(body);
      if (r.status === 200 && session) session.transport = "poll";
      sendJson(res, r.status, r.json);
      return;
    }
    if (req.method === "GET" && url.pathname === "/bridge/poll") {
      const s = session;
      if (!s || url.searchParams.get("session_id") !== s.id || !checkNonce(s, url.searchParams.get("nonce"))) {
        sendJson(res, 403, { ok: false, error: "bad_session_or_nonce" });
        return;
      }
      s.transport = "poll";
      if (s.queue.length) { sendJson(res, 200, { messages: s.queue.splice(0) }); return; }
      const messages = await new Promise((resolve) => {
        const waiter = (msgs) => resolve(msgs);
        s.pollWaiters.push(waiter);
        setTimeout(() => {
          const i = s.pollWaiters.indexOf(waiter);
          if (i >= 0) { s.pollWaiters.splice(i, 1); resolve([]); }
        }, 25000);
      });
      if (session === s) s.lastSeen = nowMs();
      sendJson(res, 200, { messages });
      return;
    }
    if (req.method === "POST" && url.pathname === "/bridge/result") {
      const body = await readBody(req);
      const s = session;
      if (!s || body.session_id !== s.id || !checkNonce(s, body.nonce)) {
        sendJson(res, 403, { ok: false, error: "bad_session_or_nonce" });
        return;
      }
      handleOpResult(body);
      sendJson(res, 200, { ok: true });
      return;
    }
    sendJson(res, 404, { ok: false, error: "not_found" });
  } catch (e) {
    sendJson(res, 400, { ok: false, error: "bad_request" });
  }
});

const wss = new WebSocketServer({ noServer: true });
bridgeServer.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname !== "/bridge") { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
});

wss.on("connection", (ws) => {
  let helloDone = false;
  const helloTimer = setTimeout(() => { if (!helloDone) { try { ws.close(); } catch (_) {} } }, 5000);
  ws.on("pong", () => { if (session && session.ws === ws) session.lastSeen = nowMs(); });
  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(String(raw)); } catch (_) { return; }
    if (!helloDone) {
      if (msg.type !== "hello") { try { ws.close(); } catch (_) {} return; }
      const r = handleHello(msg);
      if (r.status !== 200) {
        ws.send(JSON.stringify({ type: "hello_refused", ...r.json }));
        try { ws.close(); } catch (_) {}
        return;
      }
      helloDone = true;
      clearTimeout(helloTimer);
      session.ws = ws;
      session.transport = "ws";
      // Flush anything queued (e.g. ops issued while on poll fallback).
      for (const m of session.queue.splice(0)) ws.send(JSON.stringify(m));
      ws.send(JSON.stringify({ type: "hello_ack", ...r.json }));
      return;
    }
    if (!session || session.ws !== ws) return;
    if (!checkNonce(session, msg.nonce)) {
      ws.send(JSON.stringify({ type: "error", error: "bad_nonce" }));
      return;
    }
    if (msg.type === "op_result") handleOpResult(msg);
    else if (msg.type === "heartbeat") { /* lastSeen already updated by checkNonce */ }
  });
  ws.on("close", () => {
    if (session && session.ws === ws) { session.ws = null; session.transport = null; }
    // In-flight ops on this socket are genuinely unknowable now (the Lua
    // environment that ran them is gone or teleporting) — resolve them as
    // unknown_outcome immediately. The SESSION survives for resume until the
    // heartbeat sweep declares it dead.
    for (const [id] of [...pending]) {
      resolvePending(id, { ok: false, outcome: "unknown_outcome", reason: "bridge_lost:ws_closed" });
    }
  });
});

// ---------- startup: FAIL FAST, naming the listener that failed ----------
let apiUp = false, bridgeUp = false;
function fatalBind(which, bind, port, err) {
  console.error(`[devlink] FATAL: ${which} listener failed to bind ${bind}:${port} — ${err.code || err.message}. Refusing to start partially.`);
  process.exit(1);
}
apiServer.on("error", (e) => fatalBind("API (Tailscale-facing)", API_BIND, API_PORT, e));
bridgeServer.on("error", (e) => fatalBind("Bridge (localhost)", BRIDGE_BIND, BRIDGE_PORT, e));

apiServer.listen(API_PORT, API_BIND, () => {
  apiUp = true;
  console.log(`[devlink] API listener: http://${API_BIND}:${API_PORT} (pre-shared secret auth)`);
  maybeReady();
});
bridgeServer.listen(BRIDGE_PORT, BRIDGE_BIND, () => {
  bridgeUp = true;
  console.log(`[devlink] Bridge listener: http://${BRIDGE_BIND}:${BRIDGE_PORT} (localhost only, session nonce)`);
  maybeReady();
});
function maybeReady() {
  if (apiUp && bridgeUp) console.log(`[devlink] devlink ${SERVER_VERSION} ready (protocol ${PROTOCOL_VERSION}).`);
}
