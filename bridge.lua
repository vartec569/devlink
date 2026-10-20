-- devlink bridge (Phase 1A: transport)
-- Served by the devlink server at http://127.0.0.1:28432/bridge.lua
-- Autoexec stub (one line):
--   loadstring(game:HttpGet("http://127.0.0.1:28432/bridge.lua"))()
--
-- Trust model: this channel is localhost-only; the session nonce issued at
-- handshake is a session nonce (prevents cross-session mixups), not a credential.
-- Protocol "1.0". Heartbeat: server declares the session dead after 10s silent.
--
-- Debug knob (set from the client console, read live):
--   getgenv().__devlinkTransport = "poll"  -> force the long-poll fallback
--   getgenv().__devlinkTransport = "ws"    -> force back to WebSocket
--   getgenv().__devlinkTransport = nil     -> auto (default)

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

-- ============ shared serializer (JSON-safe, depth/width capped) ============
local function serialize(value, depth, seen)
	depth = depth or 2
	local t = typeof(value)
	if t == "nil" or t == "boolean" or t == "number" then return value end
	if t == "string" then
		if #value > 500 then return value:sub(1, 500) .. "…[truncated]" end
		return value
	end
	if t == "Instance" then
		local ok, full = pcall(function() return value:GetFullName() end)
		return { __type = "Instance", class = value.ClassName, name = value.Name, path = ok and full or value.Name }
	end
	if t == "Vector3" then return { __type = "Vector3", x = value.X, y = value.Y, z = value.Z } end
	if t == "CFrame" then
		return { __type = "CFrame", x = value.Position.X, y = value.Position.Y, z = value.Position.Z }
	end
	if t == "EnumItem" then return { __type = "Enum", value = tostring(value) } end
	if t == "table" then
		if depth <= 0 then return { __type = "table", note = "depth limit" } end
		seen = seen or {}
		if seen[value] then return { __type = "table", note = "cycle" } end
		seen[value] = true
		local out = {}
		if #value > 0 then
			for i, v in ipairs(value) do
				if i > 50 then out[51] = "…[truncated]" break end
				out[i] = serialize(v, depth - 1, seen)
			end
		else
			local count = 0
			for k, v in pairs(value) do
				count += 1
				if count > 50 then out.__truncated = true break end
				out[tostring(k)] = serialize(v, depth - 1, seen)
			end
		end
		seen[value] = nil
		return out
	end
	return { __type = t, value = tostring(value) }
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
-- Diagnostic op: sleeps, then replies. Exists to prove timeout classes,
-- unknown_outcome, and the watchdog's "done, result discarded" path live.
OPS.test_hang = function(payload)
	local ms = tonumber(payload and payload.ms) or 10000
	task.wait(ms / 1000)
	return { slept_ms = ms }
end

-- Phase 1C: arbitrary Luau in the client (the research escape hatch — bound by
-- the same principles + the server-side pre-flight gate as every other op).
OPS.execute = function(payload)
	local code = payload and payload.code
	if type(code) ~= "string" then error("execute requires payload.code (string)") end
	local prints = {}
	local function capture(...)
		if #prints >= 200 then return end
		local parts = {}
		for i = 1, select("#", ...) do parts[#parts + 1] = tostring(select(i, ...)) end
		prints[#prints + 1] = table.concat(parts, " ")
	end
	local chunk, loadErr = loadstring(code)
	if not chunk then
		return { ok = false, error = "loadstring: " .. tostring(loadErr), prints = prints }
	end
	if setfenv then
		local base = (getgenv and getgenv()) or _G
		local env = setmetatable({ print = capture, warn = capture }, { __index = base })
		pcall(setfenv, chunk, env)
	end
	local results = { pcall(chunk) }
	local okCall = table.remove(results, 1)
	if not okCall then
		return { ok = false, error = tostring(results[1]), prints = prints }
	end
	local rets = {}
	for i, v in ipairs(results) do rets[i] = serialize(v, 2) end
	return { ok = true, returns = rets, prints = prints }
end

OPS.client_status = function(_payload)
	local lp = Players.LocalPlayer
	local out = {
		place_id = game.PlaceId,
		job_id = game.JobId,
		client = clientName(),
		protocol = PROTOCOL_VERSION,
	}
	if lp then
		out.player = { name = lp.Name, user_id = lp.UserId, display_name = lp.DisplayName }
		local char = lp.Character
		if char then
			local hum = char:FindFirstChildOfClass("Humanoid")
			local hrp = char:FindFirstChild("HumanoidRootPart")
			if hum then
				out.humanoid = { health = hum.Health, max_health = hum.MaxHealth, walkspeed = hum.WalkSpeed, jump_power = hum.JumpPower }
			end
			if hrp then out.position = { hrp.Position.X, hrp.Position.Y, hrp.Position.Z } end
		end
	end
	return out
end

-- ============ Wave: structure search ============
local function resolveInstance(path)
	if type(path) ~= "string" or path == "" or path == "game" then return game end
	local segments = string.split(path, ".")
	local node = game
	local i = 1
	if segments[1] == "game" then i = 2 end
	if segments[i] == "LocalPlayer" then
		node = Players.LocalPlayer
		i += 1
	elseif segments[i] == "workspace" then
		node = workspace
		i += 1
	end
	for j = i, #segments do
		if not node then return nil end
		local child = node:FindFirstChild(segments[j])
		if not child and node == game then
			local ok, svc = pcall(function() return game:GetService(segments[j]) end)
			if ok then child = svc end
		end
		node = child
	end
	return node
end

-- Iterative DFS; fn(node) returning true stops the walk early.
local function walkDescendants(root, fn, budget)
	budget = budget or 200000
	local stack = { root }
	local visited = 0
	while #stack > 0 do
		local node = table.remove(stack)
		visited += 1
		if visited > budget then return visited, true end
		if fn(node) then return visited, false end
		local ok, children = pcall(function() return node:GetChildren() end)
		if ok then
			for k = #children, 1, -1 do stack[#stack + 1] = children[k] end
		end
	end
	return visited, false
end

OPS.resolve_path = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then return { found = false } end
	return { found = true, instance = serialize(inst, 1) }
end

OPS.get_children = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	local out = {}
	for _, child in ipairs(inst:GetChildren()) do
		out[#out + 1] = serialize(child, 1)
		if #out >= 500 then break end
	end
	return { count = #out, children = out }
end

OPS.get_services = function(_payload)
	local names = { "Workspace", "Players", "ReplicatedStorage", "ReplicatedFirst", "ServerStorage", "ServerScriptService", "StarterGui", "StarterPack", "StarterPlayer", "Lighting", "Teams", "SoundService", "Chat", "LocalizationService", "TweenService", "RunService", "UserInputService", "ContextActionService", "ProximityPromptService", "CollectionService", "HttpService", "TeleportService", "MarketplaceService", "DataStoreService", "MessagingService", "MemoryStoreService", "PathfindingService", "PhysicsService", "TextService", "GuiService", "VirtualInputManager", "LogService", "Debris", "InsertService", "MaterialService", "AvatarEditorService", "PolicyService", "SocialService", "VoiceChatService", "AnalyticsService" }
	local out = {}
	for _, name in ipairs(names) do
		local ok, svc = pcall(function() return game:GetService(name) end)
		if ok and svc then out[#out + 1] = serialize(svc, 1) end
	end
	return { count = #out, services = out }
end

OPS.find_instances = function(payload)
	local root = resolveInstance(payload and payload.root) or game
	local classWanted = payload and payload.class
	local nameWanted = payload and payload.name
	local limit = (payload and payload.limit) or 100
	local results = {}
	local budgetHit = false
	local _, truncated = walkDescendants(root, function(node)
		if classWanted and not node:IsA(classWanted) then return false end
		if nameWanted and not string.find(string.lower(node.Name), string.lower(nameWanted), 1, true) then return false end
		results[#results + 1] = serialize(node, 1)
		return #results >= limit
	end)
	budgetHit = truncated
	return { count = #results, truncated = #results >= limit or budgetHit, results = results }
end

OPS.walk_tree = function(payload)
	local root = resolveInstance(payload and payload.root) or game
	local maxDepth = (payload and payload.max_depth) or 3
	local maxNodes = (payload and payload.max_nodes) or 500
	local count = 0
	local function build(node, depth)
		count += 1
		local entry = serialize(node, 1)
		if depth < maxDepth and count < maxNodes then
			local kids = {}
			local ok, children = pcall(function() return node:GetChildren() end)
			if ok then
				for _, child in ipairs(children) do
					if count >= maxNodes then break end
					kids[#kids + 1] = build(child, depth + 1)
				end
			end
			entry.children = kids
		end
		return entry
	end
	local tree = build(root, 0)
	return { nodes = count, tree = tree }
end

OPS.read_properties = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	local out = {}
	for _, prop in ipairs((payload and payload.props) or {}) do
		local ok, value = pcall(function() return inst[prop] end)
		if ok then out[prop] = serialize(value, 2) else out[prop] = { __error = tostring(value) } end
	end
	return { properties = out }
end

OPS.find_by_property = function(payload)
	local root = resolveInstance(payload and payload.root) or game
	local prop = payload and payload.prop
	local equals = payload and payload.equals
	local classWanted = payload and payload.class
	local limit = (payload and payload.limit) or 100
	if not prop then error("find_by_property requires payload.prop") end
	local results = {}
	walkDescendants(root, function(node)
		if classWanted and not node:IsA(classWanted) then return false end
		local ok, value = pcall(function() return node[prop] end)
		if not ok then return false end
		local match
		if type(equals) == "number" and type(value) == "number" then match = value == equals
		else match = tostring(value) == tostring(equals) end
		if match then
			results[#results + 1] = serialize(node, 1)
			return #results >= limit
		end
		return false
	end)
	return { count = #results, results = results }
end

OPS.find_by_tag = function(payload)
	local tag = payload and payload.tag
	if not tag then error("find_by_tag requires payload.tag") end
	local cs = game:GetService("CollectionService")
	local out = {}
	for _, inst in ipairs(cs:GetTagged(tag)) do
		out[#out + 1] = serialize(inst, 1)
		if #out >= ((payload and payload.limit) or 100) then break end
	end
	return { count = #out, results = out }
end

OPS.find_by_attribute = function(payload)
	local root = resolveInstance(payload and payload.root) or game
	local attr = payload and payload.attribute
	if not attr then error("find_by_attribute requires payload.attribute") end
	local hasEquals = payload and payload.equals ~= nil
	local limit = (payload and payload.limit) or 100
	local results = {}
	walkDescendants(root, function(node)
		local ok, value = pcall(function() return node:GetAttribute(attr) end)
		if ok and value ~= nil then
			if not hasEquals or tostring(value) == tostring(payload.equals) then
				results[#results + 1] = serialize(node, 1)
				return #results >= limit
			end
		end
		return false
	end)
	return { count = #results, results = results }
end

OPS.scan_proximity = function(payload)
	local lp = Players.LocalPlayer
	local char = lp and lp.Character
	local hrp = char and char:FindFirstChild("HumanoidRootPart")
	if not hrp then error("no character/HumanoidRootPart (not spawned?)") end
	local radius = (payload and payload.radius) or 50
	local origin = hrp.Position
	local prompts = {}
	walkDescendants(workspace, function(node)
		if node:IsA("ProximityPrompt") then
			local parent = node.Parent
			local pos
			if parent and parent:IsA("BasePart") then pos = parent.Position
			elseif parent and parent:IsA("Model") and parent.PrimaryPart then pos = parent.PrimaryPart.Position end
			if pos then
				local dist = (pos - origin).Magnitude
				if dist <= radius then
					prompts[#prompts + 1] = {
						path = node:GetFullName(),
						action_text = node.ActionText,
						object_text = node.ObjectText,
						distance = math.floor(dist * 10) / 10,
						hold_duration = node.HoldDuration,
						max_activation_distance = node.MaxActivationDistance,
						enabled = node.Enabled,
					}
				end
			end
		end
		return false
	end)
	table.sort(prompts, function(a, b) return a.distance < b.distance end)
	return { count = #prompts, radius = radius, prompts = prompts }
end

OPS.scan_nil = function(_payload)
	if typeof(getnilinstances) ~= "function" then
		return { supported = false, reason = "getnilinstances not available in this client" }
	end
	local out = {}
	for _, inst in ipairs(getnilinstances()) do
		out[#out + 1] = serialize(inst, 1)
		if #out >= 200 then break end
	end
	return { supported = true, count = #out, results = out }
end

OPS.hidden_property = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	if typeof(gethiddenproperty) ~= "function" then
		return { supported = false, reason = "gethiddenproperty not available in this client" }
	end
	local ok, value = pcall(gethiddenproperty, inst, payload and payload.prop)
	if not ok then error(tostring(value)) end
	return { supported = true, value = serialize(value, 2) }
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
	while S.pollMode and S.sessionId and env.__devlinkTransport ~= "ws" do
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
		if env.__devlinkTransport ~= "poll" then
			local connected = wsConnect()
			if connected then
				backoff = 1
				while S.wsOpen and env.__devlinkTransport ~= "poll" do
					task.wait(1); S.aliveAt = os.clock()
				end
				-- Transport was forced to poll while WS was open: close and fall through.
				if S.wsOpen and S.ws then pcall(function() S.ws:Close() end) end
			end
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
