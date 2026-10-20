-- devlink bridge (Phase 1A: transport)
-- Served by the devlink server at http://127.0.0.1:28432/bridge.lua
-- Autoexec stub (one line):
--   loadstring(game:HttpGet("http://127.0.0.1:28432/bridge.lua"))()
--
-- Trust model: this channel is localhost-only; the session nonce issued at
-- handshake is a session nonce (prevents cross-session mixups), not a credential.
-- Protocol "1.0". Heartbeat: server declares the session dead after 10s silent.

local PROTOCOL_VERSION = "1.0"
local HOST = "127.0.0.1"
local PORT = 28432
local BASE = "http://" .. HOST .. ":" .. PORT

local HttpService = game:GetService("HttpService")
local Players = game:GetService("Players")

local function log(...)
	print("[devlink]", ...)
end
-- warn exists in the client runtime but not in every Luau runtime — never depend on it.
local warnf = (typeof(warn) == "function" and warn) or print

-- Persist session across re-executions in the same client (resume after teleport).
local env = nil
if getgenv then
	local ok, g = pcall(getgenv)
	if ok and type(g) == "table" then env = g end
end
env = env or {} -- fallback: no cross-execution persistence, still functional
env.__devlinkBridge = env.__devlinkBridge or {}
local S = env.__devlinkBridge
if S.aliveAt and (os.clock() - S.aliveAt) < 5 then
	warnf("[devlink] already running — new instance exits")
	return
end
S.aliveAt = os.clock()

local function clientName()
	local ok, name = pcall(function()
		if identifyclient then return (identifyexecutor()) end
		return "unknown"
	end)
	return ok and tostring(name) or "unknown"
end

local function httpRequest(req)
	-- Executor HTTP function (request / http_request / syn.request). Returns {StatusCode, Body} or nil.
	local fn = (typeof(request) == "function" and request)
		or (typeof(http_request) == "function" and http_request)
		or (syn and typeof(syn.request) == "function" and syn.request)
	if not fn then return nil end
	local ok, res = pcall(fn, req)
	if not ok then return nil end
	return res
end

-- ============ op execution with watchdog ============
-- Every op runs in its own thread. Deadline = op timeout + 10s grace (looser
-- than the server-side timeout, per plan). If the op finishes after the
-- session/transport is gone, the result is DISCARDED and logged as done —
-- never as an error. Non-idempotent ops are never cancelled by the watchdog.
local running = {} -- request_id -> {started, abandoned}

local function sendResult(requestId, ok, dataOrErr)
	local msg = {
		type = "op_result",
		request_id = requestId,
		nonce = S.nonce,
		ok = ok,
	}
	if ok then msg.data = dataOrErr else msg.error = tostring(dataOrErr) end
	if S.wsOpen and S.ws then
		local sent = pcall(function() S.ws:Send(HttpService:JSONEncode(msg)) end)
		if sent then return true end
	end
	-- Poll transport: POST the result.
	local res = httpRequest({
		Url = BASE .. "/bridge/result",
		Method = "POST",
		Headers = { ["Content-Type"] = "application/json" },
		Body = HttpService:JSONEncode({
			session_id = S.sessionId,
			nonce = S.nonce,
			request_id = requestId,
			ok = ok,
			data = ok and dataOrErr or nil,
			error = (not ok) and tostring(dataOrErr) or nil,
		}),
	})
	return res ~= nil
end

local OPS = {}
OPS.ping = function(_payload)
	local lp = Players.LocalPlayer
	return {
		pong = true,
		place_id = game.PlaceId,
		job_id = game.JobId,
		player = lp and lp.Name or nil,
	}
end
-- Phase 1C adds OPS.execute. Nothing else belongs here in 1A.

local function runOp(msg)
	local requestId = msg.request_id
	local opFn = OPS[msg.op]
	if not opFn then
		sendResult(requestId, false, "unknown op: " .. tostring(msg.op))
		return
	end
	local timeoutS = ((msg.timeout_ms or 15000) / 1000)
	running[requestId] = { started = os.clock(), abandoned = false }
	-- Watchdog: after timeout + 10s, mark abandoned. The op is NOT cancelled;
	-- when it finishes, its result is discarded + logged (see below).
	task.spawn(function()
		task.wait(timeoutS + 10)
		local r = running[requestId]
		if r then r.abandoned = true end
	end)
	task.spawn(function()
		local ok, result = pcall(opFn, msg.payload or {})
		local r = running[requestId]
		running[requestId] = nil
		local abandoned = r and r.abandoned
		local gone = (not S.sessionId) or (not S.wsOpen and not S.pollMode)
		if abandoned or gone then
			log(("op %s done, result discarded"):format(requestId))
			return
		end
		sendResult(requestId, ok, result)
	end)
end

local function handleMessage(msg)
	if msg.type == "op" then
		runOp(msg)
	elseif msg.type == "hello_refused" then
		log("handshake refused:", msg.reason or "?", "(server protocol:", msg.server_protocol_version or "?", ")")
	end
end

-- ============ handshake ============
local function helloBody()
	return {
		protocol_version = PROTOCOL_VERSION,
		client = clientName(),
		session_nonce = S.nonce, -- resume if the server still holds our session
	}
end

-- ============ WebSocket transport (primary) ============
local function wsConnect()
	if typeof(WebSocket) ~= "table" or typeof(WebSocket.connect) ~= "function" then
		return false
	end
	local ok, ws = pcall(WebSocket.connect, ("ws://%s:%d/bridge"):format(HOST, PORT))
	if not ok or not ws then return false end
	S.ws = ws
	S.wsOpen = true
	S.pollMode = false

	ws.OnMessage:Connect(function(raw)
		local okDec, msg = pcall(HttpService.JSONDecode, HttpService, raw)
		if not okDec or type(msg) ~= "table" then return end
		if msg.type == "hello_ack" then
			S.sessionId = msg.session_id
			S.nonce = msg.session_nonce
			S.aliveAt = os.clock()
			log("connected via WebSocket, session", msg.session_id, msg.resumed and "(resumed)" or "")
		else
			handleMessage(msg)
		end
	end)
	ws.OnClose:Connect(function()
		S.wsOpen = false
		S.ws = nil
		log("WebSocket closed — will reconnect / fall back to polling")
	end)

	local hello = helloBody()
	hello.type = "hello"
	local sent = pcall(function() ws:Send(HttpService:JSONEncode(hello)) end)
	if not sent then S.wsOpen = false; return false end

	-- App-level heartbeat every 3s (server also pings at WS level).
	task.spawn(function()
		while S.wsOpen and S.ws == ws do
			task.wait(3)
			S.aliveAt = os.clock()
			if S.nonce then
				pcall(function() ws:Send(HttpService:JSONEncode({ type = "heartbeat", nonce = S.nonce })) end)
			end
		end
	end)
	return true
end

-- ============ HTTP long-poll transport (fallback) ============
local function pollLoop()
	-- Handshake over HTTP.
	local res = httpRequest({
		Url = BASE .. "/bridge/hello",
		Method = "POST",
		Headers = { ["Content-Type"] = "application/json" },
		Body = HttpService:JSONEncode(helloBody()),
	})
	if not res or res.StatusCode ~= 200 then
		log("poll handshake failed:", res and res.StatusCode or "no http function")
		return false
	end
	local ack = HttpService:JSONDecode(res.Body)
	S.sessionId = ack.session_id
	S.nonce = ack.session_nonce
	S.pollMode = true
	log("connected via long-poll, session", ack.session_id, ack.resumed and "(resumed)" or "")
	while S.pollMode and S.sessionId do
		S.aliveAt = os.clock()
		local url = ("%s/bridge/poll?session_id=%s&nonce=%s"):format(BASE, S.sessionId, S.nonce)
		local r = httpRequest({ Url = url, Method = "GET" })
		if not r then task.wait(2)
		elseif r.StatusCode == 200 then
			local body = HttpService:JSONDecode(r.Body)
			for _, msg in ipairs(body.messages or {}) do handleMessage(msg) end
		elseif r.StatusCode == 403 then
			log("poll rejected (session gone) — re-handshake needed")
			S.sessionId = nil
			S.pollMode = false
			return true -- signal: loop back to reconnect
		else
			task.wait(1)
		end
	end
	return true
end

-- ============ main reconnect loop (backoff 1,2,4,8,16,30s) ============
local function hasAnyTransport()
	if typeof(WebSocket) == "table" and typeof(WebSocket.connect) == "function" then return true end
	if typeof(request) == "function" or typeof(http_request) == "function" then return true end
	if syn and typeof(syn.request) == "function" then return true end
	return false
end

if not hasAnyTransport() then
	warnf("[devlink] no WebSocket and no HTTP function found — this is not a supported client environment, bridge exits")
	return
end

task.spawn(function()
	local backoff = 1
	while true do
		S.aliveAt = os.clock()
		local connected = wsConnect()
		if connected then
			backoff = 1
			while S.wsOpen do task.wait(1); S.aliveAt = os.clock() end
		end
		if not S.wsOpen then
			if httpRequest({ Url = BASE .. "/bridge.lua", Method = "GET" }) then
				local ok = pollLoop()
				if ok then backoff = 1 end
			end
		end
		task.wait(backoff)
		backoff = math.min(backoff * 2, 30)
	end
end)

log("bridge started (protocol " .. PROTOCOL_VERSION .. ", client " .. clientName() .. ")")
