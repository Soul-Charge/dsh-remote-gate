// dsh-remote-gate: 在 dsh web 实例进程内开放一个 localhost HTTP gate，
// 供 Windows 宿主机的 harness-bridge 以"和本人使用等效"的方式驱动 dsh：
// 建会话（cwd=工作区）/ 发消息（followup）/ 异步审批（approval/request waterfall）/ 中断 / 标题。
// 远控会话与用户在 WebUI 里开的是同一实例的同一注册表 —— web UI 实时可见、可续聊。
//
// 安全：只监听 127.0.0.1；Bearer token 存 ~/.dsh/remote-gate-token（0600，缺失自动生成）。
// 审批兜底：gate 侧超时默认 rejected（fail-closed），与 bridge 侧 TTL 双保险。
//
// API（全部 JSON；除 /health 外都要 Authorization: Bearer <token>）：
//   GET  /health
//   GET  /sessions                     → 远控会话列表（含 status/last_reply）
//   GET  /all_sessions                 → 实例内全部会话（id/cwd/createdAt/title，跨工作区）
//   POST /session   {cwd, task?, session_id?} → 建会话（带 task 则直接 followup + 写标题）
//   POST /session/:id/send    {text}   → followup（运行中自动排队，等价 bridge 队列语义）
//   GET  /session/:id/events?after=N   → {events, last_seq, status, last_reply}
//   POST /session/:id/approve {key, ok} → 裁决待审批（key=事件里给的 approval key）
//   POST /session/:id/stop             → agent.cancel("user")
//   POST /session/:id/title  {title}   → session/title 事件（web UI 列表名）
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { createUserMessage } from "@deepseek-ai/dsh-llm";

const HOME = os.homedir();
const ENV = (k, d) => process.env[k] ?? d;

// 从 ~/.dsh/settings.yaml 读 agent-default-model（与 WebUI 的默认模型同源，等效关键）
function readDefaultModel(home) {
  try {
    const yaml = fs.readFileSync(path.join(home, ".dsh", "settings.yaml"), "utf8");
    const m = yaml.match(/agent-default-model:\s*\n((?:[ \t]+.*\n?)+)/);
    if (!m) return {};
    const block = m[1];
    const pick = (k) => {
      const mm = block.match(new RegExp("^[ \t]+" + k + ":[ \t]*(.+)$", "m"));
      return mm ? mm[1].trim() : undefined;
    };
    const provider = pick("provider");
    const model = pick("model");
    return provider && model ? { provider, model } : {};
  } catch {
    return {};
  }
}
function loadOrCreateToken(file) {
  try {
    const t = fs.readFileSync(file, "utf8").trim();
    if (t) return t;
  } catch {}
  const t = crypto.randomBytes(24).toString("hex");
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, t + "\n", { mode: 0o600 });
  return t;
}

const textOf = (content) => {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map((b) => (typeof b === "string" ? b : b?.text ?? "")).join("");
  }
  return content?.text ?? "";
};

const briefInput = (name, input) => {
  try {
    if (input === undefined || input === null) return "";
    if (name === "bash" && typeof input?.command === "string") return input.command.slice(0, 160);
    return JSON.stringify(input).slice(0, 160);
  } catch {
    return "";
  }
};

// cordis 服务注入：tools 立即可用；agents/sessions 在部分组合里较晚就绪，
// 用响应式 ctx.inject 等待（web profile 下 webServer/agent 服务随后就位）
export const inject = ['tools']

export function apply(ctx, config = {}) {
  console.log("[dsh-remote-gate] apply() entered, waiting for agents/sessions services")
  const port = Number(config.port ?? ENV("DSH_REMOTE_GATE_PORT", 3081));
  const approvalTimeoutSec = Number(config.approvalTimeoutSec ?? ENV("DSH_REMOTE_GATE_APPROVAL_TIMEOUT", 300));
  const tokenFile = String(config.tokenFile ?? ENV("DSH_REMOTE_GATE_TOKEN_FILE", path.join(HOME, ".dsh", "remote-gate-token")));
  const token = loadOrCreateToken(tokenFile);
  const maxEvents = 500;

  // 服务引用：响应式注入就绪后赋值（agents/sessions 不在插件根 ctx 上）
  let svc = null
  // sid -> {taskText, cwd, status, seq, events[], pending: Map(key->{resolve}), lastReply, createdAt}
  const remote = new Map();
  // callId -> {sid, toolName, input}（tools/pre-execute 时登记，approval/request 关联输入用）
  const callSite = new Map();

  const emit = (sid, type, data) => {
    const st = remote.get(sid);
    if (!st) return;
    st.seq += 1;
    st.events.push({ seq: st.seq, ts: Date.now(), type, data });
    if (st.events.length > maxEvents) st.events.splice(0, st.events.length - maxEvents);
  };

  // ---- 观测：持久会话事件镜像（token 流不镜像，只取锚点与边界）----
  ctx.on("session/event", (session, event) => {
    const sid = session?.id;
    const st = sid ? remote.get(sid) : undefined;
    if (!st) return;
    const type = event?.type ?? "?";
    const data = event?.data ?? {};
    console.log(`[gate:event] ${sid.slice(0, 14)} ${type} ${JSON.stringify(data).slice(0, 200)}`);
    if (type === "turn/start") {
      st.status = "running";
      emit(sid, "turn_started", {});
    } else if (type === "turn/end") {
      st.status = "idle";
      const err = data?.reason?.kind === "error" ? (data.reason.error?.message ?? "unknown") : "";
      if (err) emit(sid, "turn_failed", { error: String(err).slice(0, 300) });
      else emit(sid, "turn_completed", { aborted: data?.aborted === true });
    } else if (type === "assistant/message") {
      const text = textOf(data?.message?.content);
      if (text) {
        st.lastReply = text;
        emit(sid, "assistant_message", { text: String(text).slice(0, 8000) });
      }
    } else if (type === "tool/call") {
      emit(sid, "tool_call", {
        callId: data?.callId ?? "", tool: data?.name ?? "?",
        brief: briefInput(data?.name, data?.arguments),
      });
    } else if (type === "tool/result") {
      emit(sid, "tool_result", {
        callId: data?.callId ?? "",
        ok: data?.status === undefined ? true : data.status === "success",
        brief: textOf(data?.result?.content ?? data?.content).slice(0, 200),
      });
    } else if (type === "approval/asked") {
      emit(sid, "approval_asked", { tool: data?.toolName ?? "?", reason: String(data?.reason ?? "").slice(0, 200) });
    }
  });

  // ---- 观测：pre-execute 登记调用入参（approval/request 不带参数，靠 callId 关联）----
  ctx.on("tools/pre-execute", (exec, next) => {
    const sid = exec?.agent?.session?.id ?? exec?.agent?.id;
    if (sid && remote.has(sid) && exec?.callId) {
      callSite.set(exec.callId, { sid, toolName: exec.name, input: exec.arguments });
    }
    return next();
  });

  ctx.on("tools/result", (exec) => {
    if (exec?.callId) callSite.delete(exec.callId);
  });

  // ---- 审批 waterfall：远控会话的请求不 next()，异步等 bridge 裁决；非远控一律 next() 给 UI ----
  ctx.on("approval/request", (request, next) => {
    const callId = request?.callId;
    const site = callId ? callSite.get(callId) : undefined;
    const sid = request?.agent?.session?.id ?? request?.sessionId ?? site?.sid;
    if (!sid || !remote.has(sid)) return next();
    const st = remote.get(sid);
    const key = callId ?? `req-${crypto.randomUUID().slice(0, 8)}`;
    const toolName = request?.toolName ?? site?.toolName ?? "?";
    const reason = String(request?.reason ?? "").slice(0, 300);
    emit(sid, "approval_requested", { key, tool: toolName, reason, brief: briefInput(toolName, site?.input) });
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (st.pending.has(key)) {
          st.pending.delete(key);
          emit(sid, "approval_decided", { key, outcome: "rejected", auto: true });
          resolve("rejected");
        }
      }, approvalTimeoutSec * 1000);
      st.pending.set(key, {
        resolve: (outcome) => {
          clearTimeout(timer);
          st.pending.delete(key);
          emit(sid, "approval_decided", { key, outcome, auto: false });
          resolve(outcome);
        },
      });
    });
  }, { prepend: true });

  // ---- HTTP gate ----
  const json = (res, code, obj) => {
    const body = JSON.stringify(obj);
    res.writeHead(code, { "content-type": "application/json; charset=utf-8" });
    res.end(body);
  };
  const readBody = (req) => new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (c) => { raw += c; if (raw.length > 1e6) req.destroy(); });
    req.on("end", () => {
      try { resolve(raw ? JSON.parse(raw) : {}); } catch (e) { reject(new Error("bad json")); }
    });
    req.on("error", reject);
  });

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      const parts = url.pathname.split("/").filter(Boolean);
      if ((req.headers.authorization ?? "") !== `Bearer ${token}`) {
        return json(res, 401, { error: "bad token" });
      }

      if (req.method === "GET" && url.pathname === "/health") {
        return json(res, 200, { ok: true, gate: "dsh-remote-gate", remote: remote.size });
      }

      if (req.method === "GET" && url.pathname === "/sessions") {
        const sessions = [...remote.entries()].map(([id, st]) => ({
          id, cwd: st.cwd, title: st.taskText, status: st.status,
          created_at: st.createdAt, last_reply: (st.lastReply ?? "").slice(0, 400),
        }));
        return json(res, 200, { sessions });
      }

      if (req.method === "GET" && url.pathname === "/all_sessions") {
        const sessions = (svc?.sessions?.list?.() ?? []).map((s) => ({
          id: s.id, cwd: s.header?.cwd ?? "", created_at: s.header?.createdAt ?? 0,
          title: s.title?.title ?? "",
        }));
        return json(res, 200, { sessions });
      }

      if (req.method === "POST" && url.pathname === "/session") {
        const body = await readBody(req);
        const resumeId = String(body.resume_session_id ?? "").trim();
        const task = String(body.task ?? "").trim();
        let sessionId;
        let cwd;
        let handle;
        if (!svc) return json(res, 503, { error: "gate services not ready" });
        if (resumeId) {
          // 续聊：加载持久会话（与 WebUI 里点开同一条会话等效）
          handle = await svc.agents.resume({ resumeSessionId: resumeId });
          sessionId = resumeId;
          cwd = handle.agent?.session?.header?.cwd ?? "";
          if (remote.has(sessionId)) return json(res, 200, { id: sessionId, status: remote.get(sessionId).status, resumed: true });
        } else {
          cwd = String(body.cwd ?? "").trim();
          if (!cwd) return json(res, 400, { error: "cwd required" });
          sessionId = String(body.session_id ?? "").trim() || `remote-${crypto.randomUUID()}`;
          const dm = { ...readDefaultModel(HOME), ...(body.provider ? { provider: String(body.provider) } : {}), ...(body.model ? { model: String(body.model) } : {}) };
          const agentOptions = dm.provider && dm.model ? { provider: dm.provider, model: dm.model } : undefined;
          handle = await svc.agents.create({
            sessionId, meta: { cwd }, agentOptions,
            setup: async (agentCtx) => {
              const preset = await svc.presets.mount(agentCtx);
              console.log(`[dsh-remote-gate] session ${sessionId.slice(0, 14)} preset=${preset?.id ?? "?"}`);
            },
          });
        }
        remote.set(sessionId, {
          taskText: task, cwd, status: task ? "running" : "idle",
          seq: 0, events: [], pending: new Map(), lastReply: "", createdAt: Date.now(),
        });
        // 归组：registry 只在启动时做一次 cwd 索引，此后新建会话必须显式 attach，
        // 否则即使 cwd 匹配已注册工作区也会挂"未分组"（WebUI 手动开聊天时由 UI 代做）
        try {
          const ws = await svc.workspaceRegistry.resolveByPath(cwd);
          ws.attachSession(sessionId);
          console.log(`[dsh-remote-gate] session ${sessionId.slice(0, 14)} attached to workspace ${ws.title ?? ""}`);
        } catch (e) {
          console.log(`[dsh-remote-gate] session ${sessionId.slice(0, 14)} stayed ungrouped (${String(e?.message ?? e).slice(0, 80)})`);
        }
        if (!resumeId || task) {
          try {
            handle.agent.session.append("session/title", {
              title: task.slice(0, 60) || "远控会话",
              messageSeqs: [], source: { kind: "user" },
            });
          } catch {}
        }
        if (task) {
          handle.agent.followup(createUserMessage({
            content: [{ type: "text", text: task }],
            source: { kind: "user" },
          }));
        }
        return json(res, 200, { id: sessionId, status: task ? "running" : "idle", resumed: Boolean(resumeId) });
      }

      if (parts[0] === "session" && parts[1]) {
        const sid = parts[1];
        const st = remote.get(sid);
        const action = parts[2] ?? "";

        if (req.method === "GET" && action === "") {
          if (!st) return json(res, 404, { error: "no such remote session" });
          return json(res, 200, {
            id: sid, cwd: st.cwd, title: st.taskText, status: st.status,
            last_reply: (st.lastReply ?? "").slice(0, 400), pending: [...st.pending.keys()],
          });
        }

        if (req.method === "GET" && action === "events") {
          if (!st) return json(res, 404, { error: "no such remote session" });
          // 兼容 after / after_seq 两种参数名（bridge 适配器用 after_seq）
          const after = Number(url.searchParams.get("after") ?? url.searchParams.get("after_seq") ?? 0);
          return json(res, 200, {
            events: st.events.filter((e) => e.seq > after),
            last_seq: st.seq, status: st.status,
            last_reply: (st.lastReply ?? "").slice(0, 400),
          });
        }

        if (req.method === "POST" && action === "send") {
          if (!st) return json(res, 404, { error: "no such remote session" });
          if (!svc) return json(res, 503, { error: "gate services not ready" });
          const agent = svc.agents.get(sid);
          if (!agent) return json(res, 404, { error: "agent not live" });
          const body = await readBody(req);
          const text = String(body.text ?? "").trim();
          if (!text) return json(res, 400, { error: "text required" });
          const running = st.status === "running";
          agent.followup(createUserMessage({
            content: [{ type: "text", text }], source: { kind: "user" },
          }));
          if (!running) st.status = "running";
          return json(res, 200, { ok: true, queued: running });
        }

        if (req.method === "POST" && action === "approve") {
          if (!st) return json(res, 404, { error: "no such remote session" });
          const body = await readBody(req);
          const key = String(body.key ?? "");
          const pending = st.pending.get(key);
          if (!pending) return json(res, 404, { error: "no pending approval for key" });
          pending.resolve(body.ok ? "allowed-once" : "rejected");
          return json(res, 200, { ok: true });
        }

        if (req.method === "POST" && action === "stop") {
          if (!st) return json(res, 404, { error: "no such remote session" });
          if (!svc) return json(res, 503, { error: "gate services not ready" });
          const agent = svc.agents.get(sid);
          try { agent?.cancel?.("user"); } catch {}
          for (const [key, p] of st.pending) { p.resolve("rejected"); }
          st.pending.clear();
          st.status = "idle";
          return json(res, 200, { ok: true });
        }

        if (req.method === "POST" && action === "title") {
          if (!st) return json(res, 404, { error: "no such remote session" });
          const body = await readBody(req);
          const title = String(body.title ?? "").trim().slice(0, 60);
          if (!title) return json(res, 400, { error: "title required" });
          svc?.sessions.get(sid)?.append("session/title", {
            title, messageSeqs: [], source: { kind: "user" },
          });
          return json(res, 200, { ok: true });
        }
      }

      return json(res, 404, { error: "not found" });
    } catch (e) {
      return json(res, 500, { error: String(e?.message ?? e) });
    }
  });

  ctx.inject(['agents', 'sessions', 'workspaceRegistry', 'agentPresets'], (sctx) => {
    svc = { agents: sctx.agents, sessions: sctx.sessions,
            workspaceRegistry: sctx.workspaceRegistry, presets: sctx.agentPresets }
    console.log("[dsh-remote-gate] agents/sessions ready, starting gate")
    server.listen(port, "127.0.0.1", () => {
      console.log(`[dsh-remote-gate] listening on 127.0.0.1:${port} (token: ${tokenFile})`);
    });
  })

  return () => new Promise((resolve) => server.close(() => resolve()));
}
