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

-- ============ Wave: remotes & read-only spy ============
local REMOTE_CLASSES = { RemoteEvent = true, RemoteFunction = true, BindableEvent = true, BindableFunction = true }

local function collectRemotes(root)
	local found = {}
	walkDescendants(root or game, function(node)
		if REMOTE_CLASSES[node.ClassName] then
			found[#found + 1] = node
		end
		return false
	end)
	return found
end

OPS.list_remotes = function(payload)
	local root = resolveInstance(payload and payload.root) or game
	local out = {}
	for _, inst in ipairs(collectRemotes(root)) do
		out[#out + 1] = { name = inst.Name, class = inst.ClassName, path = inst:GetFullName() }
	end
	table.sort(out, function(a, b) return a.path < b.path end)
	return { count = #out, remotes = out }
end

OPS.map_remote_topology = function(payload)
	local root = resolveInstance(payload and payload.root) or game
	local groups = {}
	local totals = { RemoteEvent = 0, RemoteFunction = 0, BindableEvent = 0, BindableFunction = 0, all = 0 }
	for _, inst in ipairs(collectRemotes(root)) do
		local top = inst:GetFullName():split(".")[2] or "game"
		groups[top] = groups[top] or { events = 0, functions = 0, bindables = 0 }
		local g = groups[top]
		if inst.ClassName == "RemoteEvent" then g.events += 1
		elseif inst.ClassName == "RemoteFunction" then g.functions += 1
		else g.bindables += 1 end
		totals[inst.ClassName] += 1
		totals.all += 1
	end
	return { totals = totals, groups = groups }
end

-- Read-only spy (point-2, Davin's decision): PASSIVE listeners only.
-- RemoteEvent/BindableEvent signals accept extra Connect listeners without
-- touching the game's own handlers. No hooks, no namecall interception:
-- outgoing FireServer calls and RemoteFunction invocations are therefore NOT
-- observable here — by design, not by omission.
local function spyState()
	S.spy = S.spy or { active = false, installed_at = nil, last_event_at = nil, event_count = 0, dropped = 0, entries = {}, connections = {} }
	return S.spy
end

OPS.spy_remote = function(payload)
	local action = (payload and payload.action) or "status"
	local spy = spyState()
	if action == "stop" then
		for _, conn in ipairs(spy.connections) do pcall(function() conn:Disconnect() end) end
		spy.connections = {}
		spy.active = false
		return { active = false, event_count = spy.event_count }
	end
	if action == "status" then
		return {
			active = spy.active,
			installed_at = spy.installed_at,
			last_event_at = spy.last_event_at,
			event_count = spy.event_count,
			dropped = spy.dropped,
			buffered = #spy.entries,
			coverage = "incoming RemoteEvent + BindableEvent only (passive listeners); outgoing calls and RemoteFunction invokes are not observable without hooks and stay unobserved",
		}
	end
	if action == "read" then
		local limit = (payload and payload.limit) or 100
		local nameFilter = payload and payload.name_contains
		local out = {}
		for _, e in ipairs(spy.entries) do
			if not nameFilter or string.find(e.name, nameFilter, 1, true) then
				out[#out + 1] = e
			end
		end
		while #out > limit do table.remove(out, 1) end
		return { count = #out, total = spy.event_count, entries = out }
	end
	if action == "start" then
		if spy.active then return { active = true, note = "already running", event_count = spy.event_count } end
		spy.entries = {}
		spy.event_count = 0
		spy.dropped = 0
		spy.installed_at = os.date("!%Y-%m-%dT%H:%M:%SZ")
		local connected = 0
		for _, inst in ipairs(collectRemotes(game)) do
			local signal
			if inst.ClassName == "RemoteEvent" then signal = inst.OnClientEvent
			elseif inst.ClassName == "BindableEvent" then signal = inst.Event end
			if signal then
				local ok, conn = pcall(function()
					return signal:Connect(function(...)
						local args = {}
						for i = 1, select("#", ...) do args[i] = serialize(select(i, ...), 1) end
						spy.event_count += 1
						spy.last_event_at = os.date("!%Y-%m-%dT%H:%M:%SZ")
						spy.entries[#spy.entries + 1] = {
							t = spy.last_event_at,
							name = inst.Name,
							class = inst.ClassName,
							path = inst:GetFullName(),
							args = args,
						}
						if #spy.entries > 500 then
							table.remove(spy.entries, 1)
							spy.dropped += 1
						end
					end)
				end)
				if ok and conn then
					spy.connections[#spy.connections + 1] = conn
					connected += 1
				end
			end
		end
		spy.active = true
		return { active = true, listeners = connected, note = "passive listeners only — game handlers untouched" }
	end
	error("spy_remote: unknown action " .. tostring(action))
end

OPS.remote_handlers = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	local out = { class = inst.ClassName, path = inst:GetFullName() }
	if inst.ClassName == "RemoteFunction" then
		local ok, cb = pcall(function() return inst.OnClientInvoke end)
		out.callback_set = ok and cb ~= nil or false
	elseif inst.ClassName == "BindableFunction" then
		local ok, cb = pcall(function() return inst.OnInvoke end)
		out.callback_set = ok and cb ~= nil or false
	end
	out.getconnections_supported = typeof(getconnections) == "function"
	if out.getconnections_supported then
		local signal
		if inst.ClassName == "RemoteEvent" then signal = inst.OnClientEvent
		elseif inst.ClassName == "BindableEvent" then signal = inst.Event end
		if signal then
			local ok, conns = pcall(getconnections, signal)
			if ok and conns then out.connections = #conns end
		end
	end
	return out
end

local function decodeArg(arg)
	if type(arg) == "table" and arg.__path then
		return resolveInstance(arg.__path)
	end
	return arg
end

OPS.fire_remote = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	local args = {}
	for i, a in ipairs((payload and payload.args) or {}) do args[i] = decodeArg(a) end
	if inst.ClassName == "RemoteEvent" then
		inst:FireServer(table.unpack(args))
		return { fired = true, class = inst.ClassName }
	elseif inst.ClassName == "RemoteFunction" then
		local results = { inst:InvokeServer(table.unpack(args)) }
		local rets = {}
		for i, v in ipairs(results) do rets[i] = serialize(v, 2) end
		return { fired = true, class = inst.ClassName, returns = rets }
	elseif inst.ClassName == "BindableEvent" then
		inst:Fire(table.unpack(args))
		return { fired = true, class = inst.ClassName }
	elseif inst.ClassName == "BindableFunction" then
		local results = { inst:Invoke(table.unpack(args)) }
		local rets = {}
		for i, v in ipairs(results) do rets[i] = serialize(v, 2) end
		return { fired = true, class = inst.ClassName, returns = rets }
	end
	error("not a remote: " .. inst.ClassName)
end

-- ============ Wave: scripts & VM introspection (read-only) ============
local function scriptList()
	if typeof(getscripts) == "function" then
		local ok, list = pcall(getscripts)
		if ok and list then return list, "getscripts" end
	end
	local out = {}
	walkDescendants(game, function(node)
		if node:IsA("LuaSourceContainer") then out[#out + 1] = node end
		return false
	end)
	return out, "walk (client-visible only)"
end

OPS.running_scripts = function(payload)
	local list, source = scriptList()
	local out = {}
	for _, s in ipairs(list) do
		out[#out + 1] = { name = s.Name, class = s.ClassName, path = s:GetFullName() }
		if #out >= ((payload and payload.limit) or 500) then break end
	end
	return { count = #out, via = source, scripts = out }
end

OPS.loaded_modules = function(payload)
	local out = {}
	if typeof(getloadedmodules) == "function" then
		local ok, list = pcall(getloadedmodules)
		if ok and list then
			for _, m in ipairs(list) do
				out[#out + 1] = { name = m.Name, class = m.ClassName, path = m:GetFullName() }
				if #out >= ((payload and payload.limit) or 500) then break end
			end
			return { count = #out, via = "getloadedmodules", modules = out }
		end
	end
	walkDescendants(game, function(node)
		if node.ClassName == "ModuleScript" then
			out[#out + 1] = { name = node.Name, class = node.ClassName, path = node:GetFullName() }
		end
		return #out >= ((payload and payload.limit) or 500)
	end)
	return { count = #out, via = "walk (client-visible only)", modules = out }
end

local function tryDecompile(inst)
	if typeof(decompile) ~= "function" then return nil, "decompile not available in this client" end
	local ok, src = pcall(decompile, inst)
	if not ok then return nil, tostring(src) end
	return src, nil
end

OPS.decompile_deep = function(payload)
	local mode = (payload and payload.mode) or "single"
	if mode == "single" then
		local inst = resolveInstance(payload and payload.path)
		if not inst then error("path not found: " .. tostring(payload and payload.path)) end
		local src, err = tryDecompile(inst)
		if not src then
			local out = { supported = false, reason = err }
			if typeof(getscriptbytecode) == "function" then
				local ok, bc = pcall(getscriptbytecode, inst)
				if ok and bc then out.bytecode = { available = true, size = #bc } end
			end
			return out
		end
		return { supported = true, path = inst:GetFullName(), source = src }
	end
	if mode == "search" or mode == "comms_map" then
		local query = payload and payload.query
		if mode == "search" and not query then error("decompile_deep search requires payload.query") end
		local list = scriptList()
		local maxScan = (payload and payload.max_scan) or 50
		local matches = {}
		local scanned = 0
		for _, s in ipairs(list) do
			if scanned >= maxScan then break end
			scanned += 1
			local src = tryDecompile(s)
			if src then
				local lineNo = 0
				for line in string.gmatch(src, "[^\r\n]+") do
					lineNo += 1
					local hit
					if mode == "search" then
						hit = string.find(line, query, 1, true) ~= nil
					else
						hit = string.find(line, "FireServer", 1, true) ~= nil or string.find(line, "InvokeServer", 1, true) ~= nil
					end
					if hit then
						matches[#matches + 1] = { script = s:GetFullName(), line = lineNo, text = line:sub(1, 300) }
						if #matches >= 200 then break end
					end
				end
			end
			if #matches >= 200 then break end
		end
		return { scanned = scanned, matches = matches, note = mode == "comms_map" and "call sites extracted from decompiled sources" or nil }
	end
	error("decompile_deep: unknown mode " .. tostring(mode))
end

OPS.script_env = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	if typeof(getsenv) ~= "function" then return { supported = false, reason = "getsenv not available in this client" } end
	local ok, env = pcall(getsenv, inst)
	if not ok then error(tostring(env)) end
	local keys = {}
	local values = {}
	for k, v in pairs(env) do
		keys[#keys + 1] = tostring(k)
		if type(v) ~= "function" then values[tostring(k)] = serialize(v, 1) end
		if #keys >= 300 then break end
	end
	table.sort(keys)
	return { supported = true, keys = keys, values = values }
end

OPS.roblox_env = function(_payload)
	local function keyNames(t)
		if type(t) ~= "table" then return nil end
		local out = {}
		for k in pairs(t) do
			out[#out + 1] = tostring(k)
			if #out >= 300 then break end
		end
		table.sort(out)
		return out
	end
	local out = {}
	if typeof(getgenv) == "function" then local ok, g = pcall(getgenv) if ok then out.genv_keys = keyNames(g) end end
	if typeof(getrenv) == "function" then local ok, r = pcall(getrenv) if ok then out.renv_keys = keyNames(r) end end
	return out
end

OPS.inspect_closure = function(payload)
	local name = payload and payload.name
	if not name then error("inspect_closure requires payload.name (a global function name)") end
	local fn
	if typeof(getgenv) == "function" then local ok, g = pcall(getgenv) if ok and g then fn = g[name] end end
	if fn == nil and typeof(getrenv) == "function" then local ok, r = pcall(getrenv) if ok and r then fn = r[name] end end
	if fn == nil then fn = _G[name] end
	if type(fn) ~= "function" then return { found = false } end
	local out = { found = true }
	if typeof(getinfo) == "function" then
		local ok, info = pcall(getinfo, fn)
		if ok and info then out.info = serialize(info, 1) end
	elseif debug and debug.info then
		local ok, src, nm, np, va = pcall(debug.info, fn, "sna")
		if ok then out.info = { source = src, name = nm, numparams = np, is_vararg = va } end
	end
	return out
end

OPS.debug_info = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	local out = { class = inst.ClassName, path = inst:GetFullName() }
	if typeof(getscripthash) == "function" then
		local ok, h = pcall(getscripthash, inst)
		if ok then out.hash = h end
	end
	if typeof(getscriptbytecode) == "function" then
		local ok, bc = pcall(getscriptbytecode, inst)
		if ok and bc then out.bytecode_size = #bc end
	end
	out.decompile_supported = typeof(decompile) == "function"
	return out
end

OPS.scan_gc = function(payload)
	if typeof(getgc) ~= "function" then return { supported = false, reason = "getgc not available in this client" } end
	local ok, gc = pcall(getgc, true)
	if not ok then error(tostring(gc)) end
	local counts = {}
	local samples = {}
	local contains = payload and payload.contains
	local limit = (payload and payload.limit) or 50
	for _, v in ipairs(gc) do
		local t = typeof(v)
		counts[t] = (counts[t] or 0) + 1
		if contains and t == "function" and #samples < limit then
			if typeof(getinfo) == "function" then
				local okInfo, info = pcall(getinfo, v)
				if okInfo and info and info.name and string.find(info.name, contains, 1, true) then
					samples[#samples + 1] = { name = info.name, source = info.source or info.short_src }
				end
			end
		end
	end
	return { supported = true, total = #gc, counts = counts, samples = samples }
end

OPS.scan_registry = function(_payload)
	if typeof(getreg) ~= "function" then return { supported = false, reason = "getreg not available in this client" } end
	local ok, reg = pcall(getreg)
	if not ok then error(tostring(reg)) end
	local keys = {}
	for k in pairs(reg) do
		keys[#keys + 1] = tostring(k)
		if #keys >= 200 then break end
	end
	return { supported = true, key_count = #keys, keys_sample = keys }
end

OPS.unc_check = function(_payload)
	local probes = { "getgenv", "getrenv", "getgc", "getreg", "getscripts", "getloadedmodules", "getsenv", "gethui", "getinstances", "getnilinstances", "gethiddenproperty", "getconnections", "getinfo", "decompile", "getscriptbytecode", "getscripthash", "loadstring", "setfenv", "getfenv", "fireproximityprompt", "firetouchinterest", "fireclickdetector", "writefile", "readfile", "listfiles", "makefolder", "delfile", "setclipboard", "queue_on_teleport", "identifyexecutor", "HttpGet", "request", "WebSocket" }
	local present, missing = {}, {}
	for _, name in ipairs(probes) do
		local v = _G[name]
		if v == nil and typeof(getgenv) == "function" then local ok, g = pcall(getgenv) if ok and g then v = g[name] end end
		if v ~= nil then present[#present + 1] = name else missing[#missing + 1] = name end
	end
	return { present = present, missing = missing, percent = math.floor(#present / #probes * 100 + 0.5) }
end

OPS.sandbox = function(_payload)
	local function has(name)
		local v = _G[name]
		if v == nil and typeof(getgenv) == "function" then local ok, g = pcall(getgenv) if ok and g then v = g[name] end end
		return v ~= nil
	end
	return {
		client = clientName(),
		has_loadstring = typeof(loadstring) == "function",
		has_setfenv = typeof(setfenv) == "function",
		has_getfenv = typeof(getfenv) == "function",
		has_request = typeof(request) == "function" or typeof(http_request) == "function" or (syn ~= nil and typeof(syn.request) == "function"),
		has_websocket = typeof(WebSocket) == "table",
		has_syn = syn ~= nil,
		has_krnl = krnl ~= nil,
		user_id = Players.LocalPlayer and Players.LocalPlayer.UserId or nil,
	}
end

-- ============ Wave: GUI, screen, console, files, interaction ============
OPS.gui_tree = function(payload)
	local rootPath = (payload and payload.root) or "LocalPlayer.PlayerGui"
	local sub = { root = rootPath, max_depth = (payload and payload.max_depth) or 3, max_nodes = (payload and payload.max_nodes) or 500 }
	return OPS.walk_tree(sub)
end

OPS.screen_text = function(payload)
	local gui = Players.LocalPlayer and Players.LocalPlayer:FindFirstChildOfClass("PlayerGui")
	if not gui then return { count = 0, texts = {} } end
	local out = {}
	walkDescendants(gui, function(node)
		if node:IsA("TextLabel") or node:IsA("TextButton") or node:IsA("TextBox") then
			if node.Text and node.Text ~= "" then
				out[#out + 1] = { path = node:GetFullName(), class = node.ClassName, text = node.Text:sub(1, 300) }
			end
		end
		return #out >= ((payload and payload.limit) or 300)
	end)
	return { count = #out, texts = out }
end

OPS.world_to_screen = function(payload)
	local cam = workspace.CurrentCamera
	if not cam then error("no CurrentCamera") end
	local pos
	if payload and payload.path then
		local inst = resolveInstance(payload.path)
		if not inst then error("path not found") end
		if inst:IsA("BasePart") then pos = inst.Position
		elseif inst:IsA("Model") and inst.PrimaryPart then pos = inst.PrimaryPart.Position
		else error("instance has no position") end
	else
		pos = Vector3.new(payload.x or 0, payload.y or 0, payload.z or 0)
	end
	local v, onScreen = cam:WorldToViewportPoint(pos)
	return { on_screen = onScreen, x = v.X, y = v.Y, depth = v.Z }
end

OPS.screenshot = function(_payload)
	return { supported = false, reason = "no client screenshot API in this environment (checked: no screenshot/takescreenshot global)" }
end

OPS.video = function(_payload)
	return { supported = false, reason = "no client video capture API in this environment" }
end

OPS.console = function(payload)
	local log = game:GetService("LogService")
	local ok, history = pcall(function() return log:GetLogHistory() end)
	if not ok then error(tostring(history)) end
	local limit = (payload and payload.limit) or 100
	local out = {}
	local start = math.max(1, #history - limit + 1)
	for i = start, #history do
		local e = history[i]
		out[#out + 1] = { message = tostring(e.message), kind = tostring(e.messageType), time = e.timestamp }
	end
	return { count = #out, entries = out }
end

OPS.files_deep = function(payload)
	local action = (payload and payload.action) or "list"
	if action == "list" then
		if typeof(listfiles) ~= "function" then return { supported = false, reason = "listfiles not available" } end
		local ok, files = pcall(listfiles, (payload and payload.path) or "")
		if not ok then error(tostring(files)) end
		return { supported = true, files = files }
	end
	if action == "read" then
		if typeof(readfile) ~= "function" then return { supported = false, reason = "readfile not available" } end
		local ok, content = pcall(readfile, payload and payload.path)
		if not ok then error(tostring(content)) end
		if #content > 100000 then content = content:sub(1, 100000) .. "…[truncated]" end
		return { supported = true, content = content }
	end
	if action == "write" then
		if typeof(writefile) ~= "function" then return { supported = false, reason = "writefile not available" } end
		local ok, err = pcall(writefile, payload and payload.path, (payload and payload.content) or "")
		if not ok then error(tostring(err)) end
		return { supported = true, written = true }
	end
	if action == "grep" then
		if typeof(listfiles) ~= "function" or typeof(readfile) ~= "function" or typeof(isfile) ~= "function" then
			return { supported = false, reason = "listfiles/readfile/isfile not available" }
		end
		local query = payload and payload.query
		if not query then error("files_deep grep requires payload.query") end
		local root = (payload and payload.path) or ""
		local matches = {}
		local scanned = 0
		local function scan(folder)
			if scanned >= 200 or #matches >= 200 then return end
			local ok, items = pcall(listfiles, folder)
			if not ok then return end
			for _, item in ipairs(items) do
				if scanned >= 200 or #matches >= 200 then break end
				local okFile, isF = pcall(isfile, item)
				if okFile and isF then
					scanned += 1
					local okRead, content = pcall(readfile, item)
					if okRead and content then
						local lineNo = 0
						for line in string.gmatch(content, "[^\r\n]+") do
							lineNo += 1
							if string.find(line, query, 1, true) then
								matches[#matches + 1] = { file = item, line = lineNo, text = line:sub(1, 300) }
								if #matches >= 200 then break end
							end
						end
					end
				elseif okFile and not isF then
					scan(item)
				end
			end
		end
		scan(root)
		return { supported = true, scanned_files = scanned, matches = matches }
	end
	error("files_deep: unknown action " .. tostring(action))
end

local function localHumanoid()
	local lp = Players.LocalPlayer
	local char = lp and lp.Character
	return char and char:FindFirstChildOfClass("Humanoid"), char
end

OPS.click_button = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	if typeof(firesignal) ~= "function" then return { supported = false, reason = "firesignal not available in this client" } end
	local signalName = (payload and payload.signal) or "Activated"
	local ok, err = pcall(firesignal, inst[signalName])
	if not ok then error(tostring(err)) end
	return { supported = true, fired = signalName }
end

OPS.fire_signal = function(payload)
	local inst = resolveInstance(payload and payload.path)
	if not inst then error("path not found: " .. tostring(payload and payload.path)) end
	if typeof(firesignal) ~= "function" then return { supported = false, reason = "firesignal not available in this client" } end
	if not (payload and payload.signal) then error("fire_signal requires payload.signal") end
	local args = (payload and payload.args) or {}
	local ok, err = pcall(firesignal, inst[payload.signal], table.unpack(args))
	if not ok then error(tostring(err)) end
	return { supported = true, fired = payload.signal }
end

OPS.interact_prompts = function(payload)
	if typeof(fireproximityprompt) ~= "function" then return { supported = false, reason = "fireproximityprompt not available in this client" } end
	local target
	if payload and payload.path then
		target = resolveInstance(payload.path)
	else
		local scan = OPS.scan_proximity({ radius = (payload and payload.radius) or 30 })
		if scan.count == 0 then return { fired = false, reason = "no prompt in radius" } end
		target = resolveInstance(scan.prompts[1].path)
	end
	if not target then error("prompt not found") end
	local ok, err = pcall(fireproximityprompt, target)
	if not ok then error(tostring(err)) end
	return { fired = true, path = target:GetFullName() }
end

OPS.send_chat = function(payload)
	local message = payload and payload.message
	if type(message) ~= "string" then error("send_chat requires payload.message") end
	local tcs = game:GetService("TextChatService")
	local channel = tcs and tcs:FindFirstChild("TextChannels") and tcs.TextChannels:FindFirstChild("RBXGeneral")
	if channel then
		local ok, err = pcall(function() channel:SendAsync(message) end)
		if ok then return { sent = true, via = "TextChatService.RBXGeneral" } end
		return { sent = false, reason = tostring(err) }
	end
	local rep = game:GetService("ReplicatedStorage")
	local say = rep and rep:FindFirstChild("SayMessageRequest", true)
	if say then
		say:FireServer(message, "All")
		return { sent = true, via = "legacy SayMessageRequest" }
	end
	return { sent = false, reason = "no chat channel found" }
end

OPS.move = function(payload)
	local hum, char = localHumanoid()
	if not hum then error("no humanoid") end
	local dest
	if payload and payload.path then
		local inst = resolveInstance(payload.path)
		if not inst then error("path not found") end
		if inst:IsA("BasePart") then dest = inst.Position
		elseif inst:IsA("Model") and inst.PrimaryPart then dest = inst.PrimaryPart.Position
		else error("target has no position") end
	elseif payload and payload.x then
		dest = Vector3.new(payload.x, payload.y or 0, payload.z or 0)
	else
		error("move requires payload.path or payload.x/y/z")
	end
	hum:MoveTo(dest)
	return { moving = true, destination = { dest.X, dest.Y, dest.Z } }
end

OPS.set_position = function(payload)
	local _, char = localHumanoid()
	local hrp = char and char:FindFirstChild("HumanoidRootPart")
	if not hrp then error("no HumanoidRootPart") end
	local before = hrp.Position
	hrp.CFrame = CFrame.new(payload.x or before.X, payload.y or before.Y, payload.z or before.Z)
	return { set = true, from = { before.X, before.Y, before.Z }, to = { hrp.Position.X, hrp.Position.Y, hrp.Position.Z } }
end

OPS.camera = function(payload)
	local cam = workspace.CurrentCamera
	if not cam then error("no CurrentCamera") end
	if (payload and payload.mode) == "set" then
		if payload.look_at and payload.position then
			cam.CFrame = CFrame.lookAt(
				Vector3.new(payload.position[1], payload.position[2], payload.position[3]),
				Vector3.new(payload.look_at[1], payload.look_at[2], payload.look_at[3]))
		end
		if payload.fov then cam.FieldOfView = payload.fov end
		return { set = true }
	end
	local pos = cam.CFrame.Position
	local look = cam.CFrame.LookVector
	return {
		position = { pos.X, pos.Y, pos.Z },
		look = { look.X, look.Y, look.Z },
		fov = cam.FieldOfView,
		camera_type = tostring(cam.CameraType),
	}
end

OPS.set_player = function(payload)
	local hum = localHumanoid()
	if not hum then error("no humanoid") end
	S.tracked = S.tracked or {}
	local applied = {}
	for _, prop in ipairs({ "WalkSpeed", "JumpPower", "Health" }) do
		local key = string.lower(prop)
		local value = payload and (payload[key] or payload[prop])
		if value ~= nil then
			hum[prop] = value
			S.tracked["humanoid." .. prop] = true
			applied[#applied + 1] = prop
		end
	end
	return { applied = applied }
end

OPS.get_tracked = function(_payload)
	local keys = {}
	for k in pairs(S.tracked or {}) do keys[#keys + 1] = k end
	if OPS and true then keys[#keys + 1] = "position" end
	table.sort(keys)
	return { tracked_keys = keys }
end

OPS.restore_state = function(payload)
	local applied = {}
	if payload and payload.position then
		local _, char = localHumanoid()
		local hrp = char and char:FindFirstChild("HumanoidRootPart")
		if hrp then
			hrp.CFrame = CFrame.new(payload.position[1], payload.position[2], payload.position[3])
			applied[#applied + 1] = "position"
		end
	end
	if payload and payload.humanoid then
		local hum = localHumanoid()
		if hum then
			for prop, value in pairs(payload.humanoid) do
				local ok = pcall(function() hum[prop] = value end)
				if ok then applied[#applied + 1] = "humanoid." .. prop end
			end
		end
	end
	return { applied = applied, note = "best-effort restore — verify with a fresh snapshot compare" }
end

OPS.anti_afk = function(_payload)
	local hum, char = localHumanoid()
	if not hum then error("no humanoid") end
	local hrp = char and char:FindFirstChild("HumanoidRootPart")
	hum.Jump = true
	if hrp then
		local look = hrp.CFrame.LookVector
		hum:MoveTo(hrp.Position + look * 2)
	end
	return { nudged = true, via = "humanoid jump+move (no VirtualUser)" }
end

OPS.join_game = function(payload)
	local placeId = payload and payload.place_id
	if not placeId then error("join_game requires payload.place_id") end
	local ts = game:GetService("TeleportService")
	local lp = Players.LocalPlayer
	local ok, err
	if payload.job_id and typeof(TeleportOptions) == "Instance" then
		local opts = Instance.new("TeleportOptions")
		local okSet = pcall(function() opts:SetJobId(payload.job_id) end)
		if okSet then
			ok, err = pcall(function() return ts:TeleportAsync(placeId, { lp }, opts) end)
		end
	end
	if not ok then
		ok, err = pcall(function() return ts:TeleportAsync(placeId, { lp }) end)
	end
	if not ok then error(tostring(err)) end
	return { teleporting = true, place_id = placeId }
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
