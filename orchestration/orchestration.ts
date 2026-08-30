/**
 * Orchestration: keeping track of work that was spawned.
 *
 * Extracted from favorite-router.ts along with routing. This half answers
 * "what did we start, is it still alive, and did its result ever reach the
 * parent" — which is unrelated to which model a task should use, and was only
 * in the same file by accident.
 *
 * Three concerns live here:
 *  - the ledger of spawns, bindings and terminal states
 *  - the watchdog that re-injects a completion an idle parent never received
 *  - the live-tasks view (`running_tasks`)
 *
 * The watchdog needs the host's session API, so unlike model-routing.ts this
 * module is not host-free; the pieces that can be pure (event parsing, child
 * summarisation, formatting) take their inputs as plain data so they stay
 * testable without a host.
 */
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { spawn } from "node:child_process"
import { claimCompletionDelivery, recordCompletionDelivered, recordCompletionDeliveryFailed, recordNativeSessionLineage, recordNotification, recordTerminal, trackedChildren, expireExecutionLeases, pendingCompletionEvidence, readLedger, recordSpawn, recordSpawnResult, type CompletionEvidence } from "./orchestration-ledger"
import { injectCompletion } from "./watchdog-inject"
import { detectProviderFailure, failureMessage, type ProviderFailure } from "../usage/usage-reached"
import { blockLane } from "../models/capacity-registry"
import { capResetAt, nextHealthyFallback, quotaLaneNotice, rememberFailoverNotice, spawnLane, forceUsageCollectOnCap } from "../models/model-routing"
import { usageCache } from "../usage/usage-lib"

export async function deliverPendingCompletion(sessionApi: { synthetic?: Function }, completion: CompletionEvidence, file?: string): Promise<boolean> {
  const pending = pendingCompletionEvidence(completion.parentID, file).find((entry) => entry.idempotencyKey === completion.idempotencyKey)
  if (!pending) return true
  if (!claimCompletionDelivery(pending, file)) return true
  if (!await injectCompletion(sessionApi, pending)) {
    recordCompletionDeliveryFailed(pending.parentID, pending.idempotencyKey, file)
    return false
  }
  return recordCompletionDelivered(pending.parentID, pending.idempotencyKey, file)
}

const HERE = dirname(fileURLToPath(import.meta.url))
const CONFIG_ROOT = HERE

// ── watchdog: re-inject lost background-subagent completions ────────────────
// When a Task subagent finishes, the parent (orchestrator) session normally
// receives a synthetic result via task.ts injectBackgroundResult. That path is
// unreliable: delivery "steer" waits for a provider-turn boundary, and an idle
// parent has none (steer-into-idle-parent is never promoted). This watchdog
// re-injects a structured synthetic completion. Delivery acknowledgement is
// journaled separately, so transient host failures remain replayable. No timer
// injects user prompts or "Continue" into children.
//
// State lives on globalThis (Symbol.for) so plugin hot-reloads do NOT stack
// subscriptions or dedup sets: a reloaded module reuses the running watch.

// Key kept from the router era on purpose: it identifies the single running
// watch across a hot-reload. Renaming it would let an old and a new watchdog
// both register and double-inject every subagent result.
const WATCHDOG_STATE_KEY = Symbol.for("opencode-config.favorite-router.watchdog")

type WatchdogState = {
  installed: boolean
  notified: Set<string>
  notifying: Set<string>
  controller: AbortController
  sessionApi: Record<string, Function> | undefined
  leaseTimer?: ReturnType<typeof setInterval>
}

export function watchdogState(): WatchdogState {
  const g = globalThis as { [WATCHDOG_STATE_KEY]?: WatchdogState }
  if (!g[WATCHDOG_STATE_KEY]) {
    g[WATCHDOG_STATE_KEY] = { installed: false, notified: new Set(), notifying: new Set(), controller: new AbortController(), sessionApi: undefined }
  }
  return g[WATCHDOG_STATE_KEY]
}

function eventSessionID(event: unknown): { type?: string; sessionID?: string; idle: boolean } {
  const evt = (event ?? {}) as { type?: unknown; data?: unknown; properties?: unknown }
  const type = typeof evt.type === "string" ? evt.type : undefined
  const data = (evt.data ?? evt.properties ?? {}) as Record<string, unknown>
  const sessionID = typeof data.sessionID === "string" ? data.sessionID : undefined
  let idle = false
  // V2 execution lifecycle (current runtime): terminal when an execution ends.
  if (type === "session.execution.succeeded" || type === "session.execution.failed" || type === "session.execution.cancelled") {
    idle = true
  }
  // Legacy/compat signals kept for older runtimes.
  if (type === "session.idle") idle = true
  if (type === "session.status") {
    const status = data.status as { type?: unknown } | undefined
    idle = status?.type === "idle"
  }
  if (type === "session.next.step.failed") idle = true
  return { type, sessionID, idle }
}

function messageTexts(message: unknown): string[] {
  const m = (message ?? {}) as { content?: unknown; parts?: unknown; text?: unknown }
  const texts: string[] = []
  for (const part of (Array.isArray(m.content) ? m.content : [...(Array.isArray(m.parts) ? m.parts : [])]) as Array<{
    text?: unknown
  }>) {
    if (typeof part?.text === "string") texts.push(part.text)
  }
  if (typeof m.text === "string") texts.push(m.text)
  return texts
}

function sessionMessages(res: unknown): unknown[] {
  if (Array.isArray(res)) return res
  if (res && typeof res === "object") {
    const o = res as { data?: unknown; messages?: unknown }
    if (Array.isArray(o.data)) return o.data
    if (Array.isArray(o.messages)) return o.messages
  }
  return []
}

function safeJson(value: unknown): string {
  try {
    const s = JSON.stringify(value)
    if (typeof s !== "string") return ""
    return s.length > 4000 ? s.slice(0, 4000) : s
  } catch {
    return ""
  }
}

function partBlobs(part: unknown): string[] {
  if (part == null) return []
  if (typeof part === "string") return [part]
  if (typeof part !== "object") return [String(part)]
  const p = part as Record<string, unknown>
  const blobs: string[] = []
  if (typeof p.type === "string") blobs.push(p.type)
  if (typeof p.text === "string") blobs.push(p.text)
  if (typeof p.error === "string") blobs.push(p.error)
  else if (p.error != null) blobs.push(safeJson(p.error))
  if (typeof p.message === "string") blobs.push(p.message)
  if (typeof p.data === "string") blobs.push(p.data)
  else if (p.data != null) blobs.push(safeJson(p.data))
  if (p.type === "error" || p.type === "data" || p.type === "retry") blobs.push(safeJson(p))
  const state = p.state
  if (state && typeof state === "object") {
    const st = state as Record<string, unknown>
    if (typeof st.error === "string") blobs.push(st.error)
    else if (st.error != null) blobs.push(safeJson(st.error))
    if (st.status === "error") blobs.push(safeJson(st))
  }
  return blobs
}

function messageRole(message: unknown): string {
  const m = (message ?? {}) as Record<string, unknown>
  if (typeof m.type === "string") return m.type
  if (typeof m.role === "string") return m.role
  const info = m.info as Record<string, unknown> | undefined
  if (info && typeof info.role === "string") return info.role
  if (info && typeof info.type === "string") return info.type
  return ""
}

function messageBlobs(message: unknown): string[] {
  const m = (message ?? {}) as Record<string, unknown>
  const blobs: string[] = [...messageTexts(message)]
  if (typeof m.error === "string") blobs.push(m.error)
  else if (m.error != null) blobs.push(safeJson(m.error))
  if (typeof m.message === "string") blobs.push(m.message)
  const info = m.info
  if (info && typeof info === "object") {
    const inf = info as Record<string, unknown>
    if (inf.error != null) {
      blobs.push(safeJson(inf.error))
      if (typeof inf.providerID === "string") blobs.push(inf.providerID)
    }
    if (typeof inf.message === "string") blobs.push(inf.message)
  }
  const parts = Array.isArray(m.content) ? m.content : Array.isArray(m.parts) ? m.parts : []
  for (const part of parts) {
    for (const extra of partBlobs(part)) {
      if (!blobs.includes(extra)) blobs.push(extra)
    }
  }
  return blobs
}

function eventBlob(event: unknown): string {
  if (event == null) return ""
  const evt = event as Record<string, unknown>
  const chunks: string[] = []
  if (typeof evt.type === "string") chunks.push(evt.type)
  const data = evt.data ?? evt.properties
  if (typeof data === "string") chunks.push(data)
  else if (data != null) chunks.push(safeJson(data))
  if (typeof evt.error === "string") chunks.push(evt.error)
  else if (evt.error != null) chunks.push(safeJson(evt.error))
  return chunks.join("\n")
}

/**
 * How long a spent lane stays out of rotation when telemetry reports no reset.
 * When it does report one, capResetAt() wins: a plan that says 7d must not be
 * retried in an hour, and one that resets in ten minutes must not wait a full
 * hour. This constant is only the blind case.
 */
const USAGE_LANE_BLOCK_MS = 60 * 60 * 1000

/**
 * Classification lives in usage-reached.ts so the proxies, the Claude Code
 * harness and this router all speak the same blanket vocabulary. Nothing here
 * ever renders a status code: exhaustion is always "Usage reached — <model>".
 */
function findProviderFailure(blob: string): ProviderFailure | undefined {
  return detectProviderFailure(blob)
}

/**
 * The line the parent session actually reads. Names the spent model and the
 * successor, and records the same line as a failover notice so the orchestrator
 * sees it on its next turn even when the child never rendered a result.
 */
function renderFailure(failure: ProviderFailure): string {
  const from = failure.providerID ?? spawnLane(failure.modelID ?? "")
  const fallback = failure.kind === "usage" ? nextHealthyFallback(from, usageCache()) : undefined
  const text = failureMessage(failure, fallback)
  if (failure.kind === "usage") {
    // Blocking the lane is what makes the next spawn land somewhere healthy;
    // without it execute.before would re-pick the model that just ran out.
    // The block always self-heals: it expires at the window reset the usage
    // cache reports, or an hour out when it reports none. A permanent
    // blacklist would quietly strand a lane after one bad hour.
    //
    // Only a lane we actually identified is blocked. spawnLane() answers
    // "other" for anything it cannot place, and blocking that is both
    // meaningless and a way to poison shared capacity state from a blob we
    // never understood.
    if (from && from !== "other") {
      const resetAt = capResetAt(from, usageCache()) ?? new Date(Date.now() + USAGE_LANE_BLOCK_MS).toISOString()
      blockLane(from, text, resetAt, failure.detail)
    }
    rememberFailoverNotice(text)
  }
  return text
}

type ChildScan = { summary: string; providerError?: string; failure?: ProviderFailure }

async function parentAlreadyHasResult(sessionApi: { context?: Function }, parentID: string, childID: string) {
  try {
    const res = await sessionApi.context?.({ sessionID: parentID })
    const messages = sessionMessages(res)
    for (const message of messages) {
      // Only a TERMINAL rendering counts: the spawn call itself embeds
      // `state="running"` for the same child id, so a bare `task id=` match
      // would make us skip every completion. Also match the core's own
      // `<subagent ...>` completion format so a working core inject
      // suppresses the watchdog's copy.
      if (
        messageTexts(message).some(
          (text) =>
            text.includes(`<task id="${childID}" state="completed">`) ||
            text.includes(`<task id="${childID}" state="error">`) ||
            text.includes(`<subagent sessionID="${childID}" state="completed"`) ||
            text.includes(`<subagent sessionID="${childID}" state="error"`),
        )
      ) {
        return true
      }
    }
  } catch {}
  return false
}

async function parentAlreadyHasProviderError(sessionApi: { context?: Function }, parentID: string, childID: string) {
  try {
    const res = await sessionApi.context?.({ sessionID: parentID })
    for (const message of sessionMessages(res)) {
      if (
        messageTexts(message).some(
          (text) =>
            (text.includes(USAGE_REACHED) || text.includes("Provider unavailable —")) &&
            (text.includes(`<task id="${childID}"`) || text.includes(`sessionID="${childID}"`)),
        )
      ) {
        return true
      }
    }
  } catch {}
  return false
}

async function childSummary(sessionApi: { context?: Function }, childID: string, event?: unknown): Promise<ChildScan> {
  let lastAssistant = ""
  let failure: ProviderFailure | undefined
  try {
    const res = await sessionApi.context?.({ sessionID: childID })
    const messages = sessionMessages(res)
    for (let i = messages.length - 1; i >= 0; i--) {
      const message = messages[i]
      const hit = findProviderFailure(messageBlobs(message).join("\n"))
      if (hit && !failure) failure = hit
      if (!lastAssistant && messageRole(message) === "assistant") {
        const text = messageTexts(message).join("").trim()
        if (text) lastAssistant = text.slice(0, 1000)
      }
      if (failure && lastAssistant) break
    }
  } catch {}
  failure = failure ?? findProviderFailure(eventBlob(event))
  // `providerError` stays the internal detail (logs, papercuts, cap collection);
  // `summary` is the blanket user-facing line.
  if (failure) return { summary: renderFailure(failure), providerError: failure.detail, failure }
  return { summary: lastAssistant || "Background task completed" }
}

async function emitSessionText(
  sessionApi: { synthetic?: Function } | undefined,
  sessionID: string | undefined,
  text: string,
  label: string,
) {
  if (typeof sessionApi?.synthetic !== "function" || !sessionID) {
    console.warn(`[orchestration] ${label}: ctx.session.synthetic unavailable`)
    return
  }
  await sessionApi.synthetic({ sessionID, text })
}

async function handleSubagentEvent(event: unknown, sessionApi: any) {
  const { type, sessionID, idle } = eventSessionID(event)
  const eventSnippet = findProviderFailure(eventBlob(event))?.detail
  if (!type || !sessionID || !idle) {
    if (eventSnippet) forceUsageCollectOnCap(eventSnippet)
    return
  }
  const scanned = await childSummary(sessionApi, sessionID, event)
  if (scanned.providerError) forceUsageCollectOnCap(scanned.providerError)
  const child = await sessionApi.get({ sessionID }).catch((err: unknown) => {
    console.error(`[orchestration] watchdog: session.get(${sessionID}) failed:`, err)
    return undefined
  })
  const info = child?.data ?? child
  const parentID = info?.parentID
  if (!parentID) return
  const key = `${parentID}:${sessionID}`
  const notified = watchdogState().notified
  const failed =
    type === "session.next.step.failed" ||
    type === "session.execution.failed" ||
    type === "session.execution.cancelled" ||
    Boolean(scanned.providerError)
  recordTerminal(parentID, sessionID, failed ? (type === "session.execution.cancelled" ? "cancelled" : "failed") : "completed")
  if (await parentAlreadyHasProviderError(sessionApi, parentID, sessionID)) {
    notified.add(key)
    return
  }
  if (!scanned.providerError && (await parentAlreadyHasResult(sessionApi, parentID, sessionID))) {
    notified.add(key)
    return
  }
  let summary = scanned.summary
  if (failed && !scanned.providerError && summary === "Background task completed") {
    summary = "Background task failed"
  }
  const terminalState = failed ? (type === "session.execution.cancelled" ? "cancelled" : "failed") : "completed"
  const callID = readLedger().findLast((event) => event.parentID === parentID && event.childID === sessionID)?.callID ?? sessionID
  recordNativeSessionLineage(parentID, callID, sessionID)
  if (notified.has(key) || watchdogState().notifying.has(key)) return
  watchdogState().notifying.add(key)
  try {
    const completionSummary = [summary, scanned.providerError ? quotaLaneNotice() : undefined].filter(Boolean).join("\n")
    recordNotification(parentID, callID, sessionID, terminalState, completionSummary)
    const completion = pendingCompletionEvidence(parentID).find((entry) => entry.callID === callID)
    if (!completion || await deliverPendingCompletion(sessionApi, completion)) notified.add(key)
  } finally {
    watchdogState().notifying.delete(key)
  }
}

export async function watchSubagentCompletions(ctx: any) {
  const state = watchdogState()
  // A reloaded module instance must not create a second subscription: the
  // original one stays live for the server process (its controller is never
  // aborted), and it keeps shared state via globalThis.
  if (state.installed) return
  state.installed = true
  const eventApi = ctx?.event
  const sessionApi = ctx?.session
  if (!eventApi || typeof eventApi.subscribe !== "function") {
    console.error("[orchestration] watchdog: ctx.event.subscribe unavailable")
    return
  }
  if (!sessionApi || typeof sessionApi.get !== "function") {
    console.error("[orchestration] watchdog: ctx.session.get unavailable")
    return
  }
  state.sessionApi = sessionApi
  if (typeof sessionApi.synthetic === "function") void (async () => {
    const latest = new Map<string, ReturnType<typeof readLedger>[number]>()
    for (const event of readLedger()) {
      if (event.kind === "terminal" && event.childID) latest.set(`${event.parentID}:${event.childID}`, event)
    }
    for (const event of latest.values()) {
      if (!event.childID) continue
      const summary = event.state === "missing-result" ? `Worker result unavailable; resume the same session ID: ${event.childID}` : `Background task ${event.state ?? "finished"}: ${event.childID}`
      try {
        const state = event.state === "completed" ? "completed" : event.state === "failed" ? "failed" : event.state === "cancelled" ? "cancelled" : "missing-result"
        recordNotification(event.parentID, event.callID, event.childID, state, summary)
      } catch {}
    }
    for (const completion of pendingCompletionEvidence()) {
      try { await deliverPendingCompletion(sessionApi, completion) } catch {}
    }
  })()
  state.leaseTimer = setInterval(async () => {
    for (const event of expireExecutionLeases()) {
      if (!event.childID) continue
      const key = `${event.parentID}:${event.childID}`
      if (state.notified.has(key) || state.notifying.has(key)) continue
      state.notifying.add(key)
      try {
        const summary = `Worker lease expired; result unavailable. Resume the same session ID: ${event.childID}`
        recordNotification(event.parentID, event.callID, event.childID, "missing-result", summary)
        const completion = pendingCompletionEvidence(event.parentID).find((entry) => entry.callID === event.callID)
        if (!completion || await deliverPendingCompletion(sessionApi, completion)) state.notified.add(key)
      } finally { state.notifying.delete(key) }
    }
  }, 10_000)
  try {
    const stream = await eventApi.subscribe({ signal: state.controller.signal })
    if (stream && typeof stream[Symbol.asyncIterator] === "function") {
      for await (const event of stream) {
        await handleSubagentEvent(event, sessionApi)
      }
    } else if (stream && typeof stream.next === "function") {
      for (;;) {
        const res = await stream.next()
        if (res.done) break
        await handleSubagentEvent(res.value, sessionApi)
      }
    } else {
      console.error("[orchestration] watchdog: unsupported event stream shape")
    }
  } catch (err) {
    console.error("[orchestration] watchdog: event subscription failed:", err)
  }
}

const TASKS_STATUS = join(dirname(HERE), "tasks-status.ts")

function runTasksStatus(timeoutMs = 10_000): Promise<{ ok: boolean; stdout: string; error?: string }> {
  return new Promise((resolve) => {
    let settled = false
    const done = (result: { ok: boolean; stdout: string; error?: string }) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      resolve(result)
    }
    const attempt = (cmd: string): boolean => {
      try {
        const child = spawn(cmd, [TASKS_STATUS, "--json"], { windowsHide: true, stdio: ["ignore", "pipe", "pipe"] })
        let stdout = ""
        let stderr = ""
        child.stdout?.on("data", (chunk) => (stdout += String(chunk)))
        child.stderr?.on("data", (chunk) => (stderr += String(chunk)))
        child.once("exit", () => done({ ok: true, stdout }))
        child.once("error", () => {
          if (cmd === "bun" && !settled) {
            const alt = join(homedir(), ".bun", "bin", "bun.exe")
            if (existsSync(alt)) attempt(alt)
            else done({ ok: false, stdout: "", error: stderr || "bun spawn error" })
          } else {
            done({ ok: false, stdout: "", error: stderr || "bun spawn error" })
          }
        })
        return true
      } catch (error) {
        done({ ok: false, stdout: "", error: String(error) })
        return false
      }
    }
    const timer = setTimeout(() => done({ ok: false, stdout: "", error: `timeout after ${timeoutMs}ms` }), timeoutMs)
    attempt("bun")
  })
}

type TaskRec = {
  id: string
  parentID?: string | null
  agent?: string | null
  model?: string | null
  title?: string | null
  state?: string | null
  minutesAgo?: number | null
  subagent?: boolean
  warn?: boolean
}
type TasksPayload = { updated?: string; running?: TaskRec[]; recent?: TaskRec[]; requestedBy?: string; tracked?: Array<{ parentID: string; children: Array<{ childID?: string; callID: string; state: string; agent?: string }> }> }

function fmtMinutesAgo(min: number | null | undefined): string {
  if (min == null || !Number.isFinite(min)) return "?"
  if (min <= 1) return "just now"
  return `${Math.round(min)}m ago`
}

/** Concise text table of running (+ optional recent) sessions. */
function formatRunningTasks(payload: TasksPayload, includeRecent: boolean): string {
  const lines: string[] = []
  const running = payload.running ?? []
  lines.push(`Live subagent view (${payload.updated ?? new Date().toISOString()}):`)
  lines.push("")
  if (!running.length) {
    lines.push("RUNNING: none (no live executions)")
  } else {
    lines.push(`RUNNING (${running.length}):`)
    for (const t of running) {
      const parts = [
        formatAgentLabel(t.agent, t.title),
        t.model ?? "?",
        formatTaskDescription(t.title).slice(0, 60),
        ...(t.subagent && t.parentID ? [`parent ${t.parentID}`] : []),
        `lastActivity ${fmtMinutesAgo(t.minutesAgo)}`,
      ]
      if (t.warn) parts.push("WARN (>30m old - stalled?)")
      lines.push(`  ${t.id} · ${parts.join(" · ")}`)
    }
  }
  if (includeRecent && payload.recent?.length) {
    lines.push("")
    lines.push("RECENT (last 10 sessions, any state):")
    for (const t of payload.recent) {
      lines.push(`  ${t.id} · ${t.agent ?? "?"} · state ${t.state ?? "?"} · ${fmtMinutesAgo(t.minutesAgo)} · ${(t.title ?? "?").slice(0, 50)}`)
    }
  }
  const trackedGroups = payload.tracked ?? (payload.requestedBy ? [{ parentID: payload.requestedBy, children: trackedChildren(payload.requestedBy, payload) }] : [])
  for (const group of trackedGroups) {
    if (group.children.length) {
      lines.push("", `TRACKED CHILDREN (durable ledger; parent ${group.parentID}):`)
      for (const child of group.children) {
        const id = child.childID ?? `unbound call ${child.callID}`
        const action = child.state === "running" ? "Not yet — still active" : child.state === "failed" || child.state === "missing-result" ? "INCOMPLETE — resume SAME sessionID" : child.state === "cancelled" ? "CANCELLED — resume SAME sessionID only when explicitly requested" : child.state === "completed" ? "COMPLETED" : child.state === "stopped" ? "STOPPED — not active" : "Not yet — awaiting session binding"
        lines.push(`  ${id} · ${action}${child.agent ? ` · ${formatAgentLabel(child.agent)}` : ""}`)
      }
    }
  }
  lines.push("")
  lines.push("Ground truth = OpenCode DB + ledger (running = live execution / no terminal result, not a 10-min recency window). All sessions agree on this view.")
  return lines.join("\n")
}// The ids remain the actual Task targets; these helpers only change presentation.
const AGENT_DISPLAY_NAMES: Record<string, string> = {
  "claude-code-harness": "Claude Code (Harness)",
  "claude-code": "Claude Code (Harness)",
  build: "Build",
  explore: "Explore",
  orchestrator: "Orchestrator",
  reviewer: "Reviewer",
  "model-openai-gpt-5-6-luna-fast": "OpenAI/GPT-5.6 Luna Fast",
  "model-grok-sub-grok-4-6": "Grok 4.6",
  "model-opencode-go-muse-spark-1-2-contributor": "Muse Spark 1.2",
  "model-x-preview-f-free": "Preview",
}

/** Friendly name for a Task target, without changing the internal identifier. */
export function formatAgentLabel(agent: unknown, _description?: unknown): string {
  const id = String(agent ?? "").trim()
  if (AGENT_DISPLAY_NAMES[id]) return AGENT_DISPLAY_NAMES[id]
  if (!id) return "Unknown agent"
  if (id.startsWith("model-")) {
    return id.slice("model-".length).replace(/[-_]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
  }
  return id
}

/** Convert internal check/integration task ids into concise user-facing progress. */
export function formatTaskDescription(description: unknown, prompt?: unknown): string {
  const raw = String(description ?? "").trim()
  const explicit = typeof prompt === "object" && prompt !== null
    ? String((prompt as Record<string, unknown>).humanLabel ?? "").trim()
    : ""
  if (explicit) return explicit
  if (/^integrate-lean-/i.test(raw)) return "Updating orchestrator scaling (3 → N)"
  if (/^commit-review-skill/i.test(raw)) return "Reviewing integrated changes"
  if (/^verify-shell/i.test(raw)) return "Verifying shell guards"
  if (/^verify-/i.test(raw)) return "Verifying changes"
  if (/^check-/i.test(raw)) return "Checking implementation"
  return raw || "Working on assigned task"
}

/** Presentation-only normalization for a host Task.execute.before payload. */
export function normalizeTaskDisplay(input: unknown): void {
  if (!input || typeof input !== "object") return
  const args = input as Record<string, unknown>
  const description = args.description ?? args.title
  const label = formatTaskDescription(description, args)
  if (String(args.agent ?? "").trim().toLowerCase() === "claude-code") {
    args.description = `Claude Code (Harness) — ${label}`
    if ("title" in args) args.title = `Claude Code (Harness) — ${label}`
    return
  }
  if ("description" in args) args.description = label
  if ("title" in args) args.title = label
}
