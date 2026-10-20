// devlink 1A — local protocol tests with a fake bridge.
// Spawns the real server, then verifies: API secret handshake, bridge
// protocol_version refusal, single-session rules, nonce rejection,
// round-trips over BOTH transports, unknown_outcome on bridge death,
// and fail-fast startup for a second instance.
"use strict";

const { spawn } = require("child_process");
const path = require("path");
const WebSocket = require("ws");

const API = "http://127.0.0.1:28431";
const BRIDGE = "http://127.0.0.1:28432";
const SECRET = "test-secret-123";

let passed = 0, failed = 0;
function check(name, cond, extra) {
  if (cond) { passed++; console.log("  PASS", name); }
  else { failed++; console.log("  FAIL", name, extra ? "— " + JSON.stringify(extra) : ""); }
}
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function api(pathname, opts = {}, token) {
  const res = await fetch(API + pathname, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}), ...(opts.headers || {}) },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  let json = null;
  try { json = await res.json(); } catch (_) {}
  return { status: res.status, json };
}

function startServer(extraEnv = {}) {
  const child = spawn("node", [path.join(__dirname, "..", "server.js")], {
    env: { ...process.env, DL_SECRET: SECRET, ...extraEnv },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.out = "";
  child.err = "";
  child.stdout.on("data", (d) => { child.out += d; });
  child.stderr.on("data", (d) => { child.err += d; });
  return child;
}
async function waitReady(child, ms = 8000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    if (child.out.includes("ready")) return true;
    if (child.exitCode !== null) return false;
    await sleep(100);
  }
  return false;
}

function wsHello(version, nonce) {
  return new Promise((resolve) => {
    const ws = new WebSocket("ws://127.0.0.1:28432/bridge");
    const msgs = [];
    ws.on("open", () => {
      ws.send(JSON.stringify({ type: "hello", protocol_version: version, client: "fake-bridge", ...(nonce ? { session_nonce: nonce } : {}) }));
    });
    ws.on("message", (raw) => {
      const m = JSON.parse(String(raw));
      msgs.push(m);
      if (m.type === "hello_ack" || m.type === "hello_refused") resolve({ ws, msgs, first: m });
    });
    ws.on("error", () => resolve({ ws, msgs, first: null }));
    setTimeout(() => resolve({ ws, msgs, first: null }), 4000);
  });
}

(async () => {
  console.log("== devlink 1A protocol tests ==");
  const server = startServer();
  const ready = await waitReady(server);
  check("server starts and reports ready", ready, server.out + server.err);
  if (!ready) { console.log(`\n${passed} passed, ${failed} failed`); server.kill(); process.exit(1); }

  // --- API auth ---
  let r = await api("/api/handshake", { method: "POST", body: { secret: "wrong" } });
  check("API handshake rejects wrong secret (401)", r.status === 401, r);
  r = await api("/api/handshake", { method: "POST", body: { secret: SECRET } });
  check("API handshake accepts pre-shared secret", r.status === 200 && !!r.json.token, r);
  const token = r.json.token;
  r = await api("/api/status");
  check("status without token is 401", r.status === 401, r);
  r = await api("/api/status", {}, token);
  check("status with token: no session yet", r.status === 200 && r.json.session === null, r.json);
  check("status surfaces blacklist version + last_modified + entry count",
    r.json.blacklist && r.json.blacklist.version === 1 && r.json.blacklist.last_modified === "2026-10-20" && r.json.blacklist.entries >= 1, r.json.blacklist);

  // --- Bridge: protocol version refusal ---
  const refused = await wsHello("2.0");
  check("bridge hello with protocol 2.0 refused, versions named",
    refused.first && refused.first.type === "hello_refused" &&
    refused.first.reason === "protocol_version_mismatch" &&
    refused.first.server_protocol_version === "1.0" &&
    refused.first.client_protocol_version === "2.0", refused.first);
  try { refused.ws.close(); } catch (_) {}

  // --- Bridge: WS session ---
  const b1 = await wsHello("1.0");
  check("bridge hello 1.0 accepted (session + nonce issued)",
    b1.first && b1.first.type === "hello_ack" && !!b1.first.session_id && !!b1.first.session_nonce, b1.first);
  const nonce1 = b1.first.session_nonce;

  const second = await wsHello("1.0");
  check("second fresh hello while session alive refused (session_already_active)",
    second.first && second.first.type === "hello_refused" && second.first.reason === "session_already_active", second.first);
  try { second.ws.close(); } catch (_) {}

  r = await api("/api/status", {}, token);
  check("status shows live WS session", r.json.session && r.json.session.alive === true && r.json.session.transport === "ws", r.json.session);

  // --- Round trip: API ping -> WS bridge -> result ---
  const pingPromise = api("/api/session/ping", { method: "POST", body: {} }, token);
  const opMsg = await new Promise((resolve) => {
    b1.ws.on("message", (raw) => { const m = JSON.parse(String(raw)); if (m.type === "op") resolve(m); });
    setTimeout(() => resolve(null), 4000);
  });
  check("bridge received ping op with request_id", !!opMsg && opMsg.op === "ping" && !!opMsg.request_id, opMsg);

  // Forged result with wrong nonce must NOT resolve the call.
  let pingResolved = false;
  pingPromise.then(() => { pingResolved = true; });
  b1.ws.send(JSON.stringify({ type: "op_result", request_id: opMsg.request_id, nonce: "forged", ok: true, data: { pong: "fake" } }));
  await sleep(400);
  check("forged result (bad nonce) does not resolve the call", pingResolved === false);
  b1.ws.send(JSON.stringify({ type: "op_result", request_id: opMsg.request_id, nonce: nonce1, ok: true, data: { pong: true, place_id: 123 } }));
  const pingRes = await pingPromise;
  check("real result resolves ping (ok, pong, latency)",
    pingRes.json.ok === true && pingRes.json.data.pong === true && typeof pingRes.json.latency_ms === "number", pingRes.json);

  // --- test_hang: short sleep resolves; long sleep hits the timeout class ---
  const hangOkPromise = api("/api/session/test-hang", { method: "POST", body: { ms: 300 } }, token);
  const hangOp = await new Promise((resolve) => {
    b1.ws.on("message", (raw) => { const m = JSON.parse(String(raw)); if (m.type === "op" && m.op === "test_hang") resolve(m); });
    setTimeout(() => resolve(null), 4000);
  });
  check("bridge received test_hang op", !!hangOp, hangOp);
  setTimeout(() => {
    b1.ws.send(JSON.stringify({ type: "op_result", request_id: hangOp.request_id, nonce: nonce1, ok: true, data: { slept_ms: 300 } }));
  }, 300);
  const hangOk = await hangOkPromise;
  check("test_hang 300ms resolves ok", hangOk.json.ok === true && hangOk.json.data.slept_ms === 300, hangOk.json);

  const hangSlowPromise = api("/api/session/test-hang", { method: "POST", body: { ms: 8000 } }, token);
  await new Promise((resolve) => {
    b1.ws.on("message", (raw) => { const m = JSON.parse(String(raw)); if (m.type === "op" && m.op === "test_hang") resolve(m); });
    setTimeout(resolve, 4000);
  }); // fake bridge deliberately stays silent
  const hangSlow = await hangSlowPromise;
  check("test_hang 8000ms -> timeout outcome at the 5s class budget",
    hangSlow.json.ok === false && hangSlow.json.outcome === "timeout", hangSlow.json);

  // --- Pre-flight blacklist (1B): blocked payload never reaches the bridge ---
  const opsSeen = [];
  b1.ws.on("message", (raw) => { const m = JSON.parse(String(raw)); if (m.type === "op") opsSeen.push(m); });
  const blockedRes = await api("/api/session/test-hang", { method: "POST", body: { ms: 50, note: "while true do end" } }, token);
  check("blacklisted payload -> blocked_by_preflight with entry id",
    blockedRes.json.ok === false && blockedRes.json.outcome === "blocked_by_preflight" && blockedRes.json.entry === "empty-infinite-loop", blockedRes.json);
  await sleep(600);
  check("blocked payload NEVER reached the bridge",
    !opsSeen.some((m) => m.payload && m.payload.note === "while true do end"), opsSeen.map((m) => m.op));

  const benignPromise = api("/api/session/test-hang", { method: "POST", body: { ms: 50, note: "hello world" } }, token);
  const benignOp = await new Promise((resolve) => {
    const iv = setInterval(() => {
      const found = opsSeen.find((m) => m.payload && m.payload.note === "hello world");
      if (found) { clearInterval(iv); resolve(found); }
    }, 50);
    setTimeout(() => { clearInterval(iv); resolve(null); }, 4000);
  });
  check("benign payload passes the gate and reaches the bridge", !!benignOp, opsSeen.length);
  if (benignOp) {
    b1.ws.send(JSON.stringify({ type: "op_result", request_id: benignOp.request_id, nonce: nonce1, ok: true, data: { slept_ms: 50 } }));
  }
  const benignRes = await benignPromise;
  check("benign test_hang resolves ok", benignRes.json.ok === true, benignRes.json);

  // --- 1C: generic op endpoint + execute passes the SAME pre-flight gate (P4) ---
  const execPromise = api("/api/op/execute", { method: "POST", body: { code: "return 67" } }, token);
  const execOp = await new Promise((resolve) => {
    const iv = setInterval(() => {
      const found = opsSeen.find((m) => m.op === "execute" && m.payload && m.payload.code === "return 67");
      if (found) { clearInterval(iv); resolve(found); }
    }, 50);
    setTimeout(() => { clearInterval(iv); resolve(null); }, 4000);
  });
  check("generic endpoint routed execute to the bridge", !!execOp, opsSeen.map((m) => m.op));
  if (execOp) {
    b1.ws.send(JSON.stringify({ type: "op_result", request_id: execOp.request_id, nonce: nonce1, ok: true, data: { ok: true, returns: [67], prints: [] } }));
  }
  const execRes = await execPromise;
  check("execute round-trip via generic endpoint", execRes.json.ok === true && execRes.json.data.returns[0] === 67, execRes.json);

  const execBlocked = await api("/api/op/execute", { method: "POST", body: { code: "while true do end" } }, token);
  check("execute with blacklisted code is blocked by pre-flight (P4)",
    execBlocked.json.ok === false && execBlocked.json.outcome === "blocked_by_preflight", execBlocked.json);
  await sleep(400);
  check("blocked execute never reached the bridge",
    !opsSeen.some((m) => m.op === "execute" && m.payload && m.payload.code === "while true do end"), opsSeen.length);

  // --- Snapshots (schema v1): capture, compare, schema refusal, restore boundary ---
  async function answerNextOp(opName, data) {
    const found = await new Promise((resolve) => {
      const iv = setInterval(() => {
        const m = opsSeen.find((x) => x.op === opName && !x.__answered);
        if (m) { m.__answered = true; clearInterval(iv); resolve(m); }
      }, 40);
      setTimeout(() => { clearInterval(iv); resolve(null); }, 4000);
    });
    if (found) b1.ws.send(JSON.stringify({ type: "op_result", request_id: found.request_id, nonce: nonce1, ok: true, data }));
    return found;
  }
  const statusData = (ws) => ({ place_id: 123, job_id: "job-1", position: [10.04, 70, -353.02], humanoid: { health: 100, max_health: 100, walkspeed: ws, jump_power: 50, rig: "Enum.HumanoidRigType.R15" } });
  const snap1Promise = api("/api/session/snapshot", { method: "POST", body: { label: "test-a" } }, token);
  await answerNextOp("client_status", statusData(16));
  await answerNextOp("get_tracked", { tracked_keys: ["position", "humanoid.WalkSpeed"] });
  const snap1 = await snap1Promise;
  check("snapshot schema v1: rounded position, clean rig, tracked values filled",
    snap1.json.ok === true && snap1.json.snapshot.schema_version === 1 &&
    snap1.json.snapshot.player.position[0] === 10 && snap1.json.snapshot.player.rig === "R15" &&
    snap1.json.snapshot.tracked["humanoid.WalkSpeed"] === 16, snap1.json.snapshot);

  const snap2Promise = api("/api/session/snapshot", { method: "POST", body: {} }, token);
  await answerNextOp("client_status", statusData(24));
  await answerNextOp("get_tracked", { tracked_keys: ["position", "humanoid.WalkSpeed"] });
  const snap2 = await snap2Promise;
  const cmp = await api("/api/session/compare", { method: "POST", body: { a: snap1.json.snapshot.snapshot_id, b: snap2.json.snapshot.snapshot_id } }, token);
  check("compare diffs by path (walkspeed 16 -> 24)",
    cmp.json.ok === true && cmp.json.identical === false &&
    cmp.json.diffs.some((d) => d.path === "player.walkspeed" && d.from === 16 && d.to === 24), cmp.json);
  const cmpBad = await api("/api/session/compare", { method: "POST", body: { a: snap1.json.snapshot, b: { ...snap1.json.snapshot, schema_version: 2 } } }, token);
  check("compare refuses a different schema_version",
    cmpBad.json.ok === false && cmpBad.json.outcome === "schema_mismatch", cmpBad.json);

  const restorePromise = api("/api/session/restore", { method: "POST", body: { snapshot_id: snap1.json.snapshot.snapshot_id } }, token);
  const restoreOp = await answerNextOp("restore_state", { applied: ["position", "humanoid.WalkSpeed"] });
  check("restore sends ONLY tracked values (position + WalkSpeed 16)",
    !!restoreOp && restoreOp.payload.humanoid && restoreOp.payload.humanoid.WalkSpeed === 16 && Array.isArray(restoreOp.payload.position), restoreOp && restoreOp.payload);
  const restoreRes = await restorePromise;
  check("restore reply carries the best-effort note",
    restoreRes.json.ok === true && /best-effort/.test(restoreRes.json.note || ""), restoreRes.json);

  // --- unknown_outcome: bridge dies mid-call ---
  const ping2 = api("/api/session/ping", { method: "POST", body: {} }, token);
  await new Promise((resolve) => {
    b1.ws.on("message", (raw) => { const m = JSON.parse(String(raw)); if (m.type === "op") resolve(m); });
    setTimeout(resolve, 4000);
  });
  b1.ws.terminate(); // bridge vanishes without answering
  const ping2Res = await ping2;
  check("bridge death mid-call -> unknown_outcome (not error/success)",
    ping2Res.json.ok === false && ping2Res.json.outcome === "unknown_outcome", ping2Res.json);

  // Session must be declared dead after the 10s window, then status is clean.
  await sleep(11000);
  r = await api("/api/status", {}, token);
  check("session declared dead after 10s silence", r.json.session === null, r.json.session);

  // --- Poll transport session ---
  let res = await fetch(BRIDGE + "/bridge/hello", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ protocol_version: "1.0", client: "fake-bridge-poll" }),
  });
  const hello2 = await res.json();
  check("poll hello accepted after old session died", res.status === 200 && !!hello2.session_id && !!hello2.session_nonce, hello2);

  res = await fetch(`${BRIDGE}/bridge/poll?session_id=${hello2.session_id}&nonce=wrong`);
  check("poll with bad nonce rejected (403)", res.status === 403, res.status);

  // Resume with the session nonce (same bridge reconnecting) must succeed.
  res = await fetch(BRIDGE + "/bridge/hello", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ protocol_version: "1.0", client: "fake-bridge-poll", session_nonce: hello2.session_nonce }),
  });
  const resumed = await res.json();
  check("hello with session nonce resumes same session", res.status === 200 && resumed.resumed === true && resumed.session_id === hello2.session_id, resumed);

  // Round trip over long-poll.
  const ping3 = api("/api/session/ping", { method: "POST", body: {} }, token);
  await sleep(200); // let the op land in the queue
  res = await fetch(`${BRIDGE}/bridge/poll?session_id=${hello2.session_id}&nonce=${hello2.session_nonce}`);
  const pollBody = await res.json();
  const op3 = (pollBody.messages || []).find((m) => m.type === "op");
  check("poll delivered the queued ping op", !!op3, pollBody);
  res = await fetch(BRIDGE + "/bridge/result", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: hello2.session_id, nonce: hello2.session_nonce, request_id: op3.request_id, ok: true, data: { pong: true, via: "poll" } }),
  });
  const ping3Res = await ping3;
  check("poll round-trip resolves ping", ping3Res.json.ok === true && ping3Res.json.data.via === "poll", ping3Res.json);

  // bridge.lua is served on the bridge port.
  res = await fetch(BRIDGE + "/bridge.lua");
  const luaText = await res.text();
  check("bridge.lua served (contains protocol version)", res.status === 200 && luaText.includes('PROTOCOL_VERSION = "1.0"'), res.status);

  // --- Fail-fast: a blocked listener must kill startup, naming port + interface ---
  // (Test each listener separately with a dummy occupant — with both ports
  // taken, whichever bind loses the race reports first and masks the other.)
  server.kill();
  await sleep(300);
  const net = require("net");
  async function failFastCase(occupyPort, expectParts, label) {
    const dummy = net.createServer();
    await new Promise((r) => dummy.listen(occupyPort, occupyPort === 28432 ? "127.0.0.1" : "0.0.0.0", r));
    const inst = startServer();
    const exited = await new Promise((resolve) => {
      const t = setTimeout(() => resolve(false), 6000);
      inst.on("exit", (code) => { clearTimeout(t); resolve(code !== 0 && code !== null); });
    });
    const combined = inst.out + inst.err;
    check(`fail-fast when ${label} is occupied: exits non-zero`, exited, combined);
    check(`fail-fast message names ${label} port + interface`,
      combined.includes("FATAL") && expectParts.every((p) => combined.includes(p)), combined);
    dummy.close();
    await sleep(300);
  }
  await failFastCase(28431, ["28431", "0.0.0.0", "API"], "API port 28431");
  await failFastCase(28432, ["28432", "127.0.0.1", "Bridge"], "bridge port 28432");

  // --- Fail-fast: malformed crash blacklist at startup ---
  const fs = require("fs");
  const badPath = path.join(__dirname, "bad-blacklist.json");
  fs.writeFileSync(badPath, "{ this is not json");
  const badInst = startServer({ DL_BLACKLIST: badPath });
  const badExited = await new Promise((resolve) => {
    const t = setTimeout(() => resolve(false), 6000);
    badInst.on("exit", (code) => { clearTimeout(t); resolve(code !== 0 && code !== null); });
  });
  const badOut = badInst.out + badInst.err;
  check("malformed crash_blacklist.json at startup is fatal (gate never silently off)", badExited, badOut);
  check("fatal message names crash_blacklist.json", badOut.includes("crash_blacklist"), badOut);
  try { fs.unlinkSync(badPath); } catch (_) {}

  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error("test runner crashed:", e); process.exit(1); });
