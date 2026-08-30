// Shared ground-truth view of live OpenCode sessions (running subagents etc.).
//
// Backed ONLY by the local OpenCode DB (read-only) so every session reports
// the same answer instead of each TUI knowing only its own spawned tasks.
//
//   CLI: bun tasks-status.ts [--json]
//
// DATA SOURCES (documented):
//   - session_v2 (primary, 277 rows) + session (legacy) merged, deduped by id.
//     Neither table has an explicit state/status column; session_v2 carries
//     time_archived / time_suspended / time_idle / idle_outcome as the closest
//     lifecycle signals, and session (legacy) has none of them.
//   - session_inbox / session_pending are currently EMPTY in this DB (no queue
//     to consult), so they are not used.
//
// STATE INFERENCE (documented):
//   - state label: archived (time_archived set) -> "archived";
//     idle_outcome set -> "idle:<outcome>"; time_suspended set (no outcome) ->
//     "suspended"; otherwise "active".
//     NOTE: verified against real data - time_idle/time_suspended are NOT
//     cleared on resume, and stale ones survive across turns (e.g. a session
//     that finished its previous turn can be actively running again), so they
//     cannot alone decide running-ness. Do not treat time_suspended as
//     "dead, resume me".
//   - RUNNING = not archived AND (live ledger execution with no terminal
//     result). A grok/tool turn can be silent for >10 minutes and is still
//     running. Recency is NOT the running definition for workers.
//     lastActivity = max(session.time_updated, latest message time_created)
//     remains a display field. includeRecent still lists finished sessions.
//   - WARN marker: running session older than 30 minutes since time_created
//     (stalled / long-running candidate).

import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { readLedger, trackedChildren } from "./orchestration-ledger"

const HERE = dirname(fileURLToPath(import.meta.url))
const DB_PATH = join(homedir(), ".local", "share", "opencode", "opencode.db")

const INTERACTIVE_WINDOW_MS = 10 * 60_000
const WARN_AGE_MS = 30 * 60_000

export type ExecutionEvent = { childID?: string; kind?: string; state?: string }

export type Session = {
  id: string
  parentID: string | null
  agent: string | null
  model: string | null
  title: string | null
  state: string
  timeCreated: number
  timeUpdated: number
  lastActivity: number
  minutesAgo: number | null
  ageMin: number | null
  subagent: boolean
  warn: boolean
  live: boolean
}

/** Child IDs with an unclosed executing/heartbeat claim and no terminal result. */
export function executingChildIDs(events: ExecutionEvent[]): Set<string> {
  const executing = new Set<string>()
  for (const event of events) {
    if (!event.childID) continue
    if (event.state === "executing") executing.add(event.childID)
    if (
      event.kind === "terminal" ||
      event.kind === "notification" ||
      event.state === "completed" ||
      event.state === "failed" ||
      event.state === "cancelled" ||
      event.state === "stopped" ||
      event.state === "missing-result"
    ) {
      executing.delete(event.childID)
    }
  }
  return executing
}

/**
 * Live execution wins over recency and over stale time_suspended labels.
 * Subagents without a live ledger execution are not running, even if they
 * messaged recently (that is observation, not execution).
 */
export function isLiveRunning(
  s: { id: string; state: string; lastActivity: number; subagent: boolean },
  now: number,
  executing: Set<string>,
): boolean {
  if (s.state === "archived") return false
  if (executing.has(s.id)) return true
  if (s.subagent) return false
  return Boolean(s.lastActivity) && now - s.lastActivity <= INTERACTIVE_WINDOW_MS
}

async function openDb(path: string): Promise<any | undefined> {
  try {
    const { Database } = await import("bun:sqlite")
    const db = new Database(path, { readonly: true, strict: false })
    db.query("SELECT count(*) AS n FROM sqlite_master").get()
    return db
  } catch (error) {
    console.error(`tasks-status: bun:sqlite open failed: ${String(error)}`)
    return undefined
  }
}

function modelLabel(raw: unknown): string | null {
  if (typeof raw !== "string") return null
  try {
    const j = JSON.parse(raw)
    if (j && typeof j === "object") {
      const p = typeof j.providerID === "string" ? j.providerID : null
      const m = j.id ?? j.modelID
      if (typeof m === "string") return p ? `${p}/${m}` : m
    }
  } catch {}
  return raw.trim() ? raw : null
}

async function collect(db: any): Promise<Session[]> {
  const now = Date.now()
  const sessions = new Map<string, Session>()

  // last message activity per session (both message tables, merged)
  const lastMsg = new Map<string, number>()
  for (const table of ["message", "session_message"]) {
    try {
      const rows = db.query(`SELECT session_id, MAX(time_created) last FROM ${table} GROUP BY session_id`).all() as {
        session_id: string
        last: number
      }[]
      for (const r of rows) {
        const cur = lastMsg.get(r.session_id)
        if (cur == null || r.last > cur) lastMsg.set(r.session_id, r.last)
      }
    } catch (error) {
      console.error(`tasks-status: ${table} scan failed: ${String(error)}`)
    }
  }

  const add = (r: any, v2: boolean) => {
    let state = "active"
    if (r.time_archived != null) state = "archived"
    else if (v2 && r.idle_outcome != null) state = `idle:${r.idle_outcome}`
    else if (v2 && r.time_suspended != null) state = "suspended"
    if (sessions.has(r.id)) return // v2 wins (added first)
    const lastActivity = Math.max(Number(r.time_updated) || 0, lastMsg.get(r.id) ?? 0)
    sessions.set(r.id, {
      id: r.id,
      parentID: r.parent_id ?? null,
      agent: r.agent ?? null,
      model: modelLabel(r.model),
      title: r.title ?? null,
      state,
      timeCreated: Number(r.time_created) || 0,
      timeUpdated: Number(r.time_updated) || 0,
      lastActivity,
      minutesAgo: lastActivity ? Math.round((now - lastActivity) / 60_000) : null,
      ageMin: r.time_created ? Math.round((now - Number(r.time_created)) / 60_000) : null,
      subagent: r.parent_id != null,
      warn: false,
      live: false,
    })
  }

  try {
    const v2 = db
      .query(
        `SELECT id, parent_id, agent, title, model, time_created, time_updated, time_archived, time_suspended, time_idle, idle_outcome FROM session_v2`,
      )
      .all() as any[]
    for (const r of v2) add(r, true)
  } catch (error) {
    console.error(`tasks-status: session_v2 scan failed: ${String(error)}`)
  }
  try {
    // legacy session table has no suspend/idle columns - select only its own fields
    const legacy = db
      .query(
        `SELECT id, parent_id, agent, title, model, time_created, time_updated, time_archived FROM session`,
      )
      .all() as any[]
    for (const r of legacy) add(r, false)
  } catch (error) {
    console.error(`tasks-status: session scan failed: ${String(error)}`)
  }

  const out = [...sessions.values()]
  // A recent DB write is observation evidence, not execution evidence. For
  // subagents, only an unclosed machine-owned `executing` lifecycle event can
  // authorize the live projection. time_suspended is NOT cleared on resume —
  // a live execution is still running even when that marker is set.
  const executing = executingChildIDs(readLedger())
  for (const s of out) {
    if (executing.has(s.id) && s.state !== "archived") s.state = "active"
    s.live = isLiveRunning(s, now, executing)
    s.warn = s.subagent && s.ageMin != null && s.ageMin > WARN_AGE_MS / 60_000
  }

  const running = out.filter((s) => s.live).sort((a, b) => b.lastActivity - a.lastActivity)
  const recent = [...out]
    .sort((a, b) => b.lastActivity - a.lastActivity)
    .slice(0, 10)

  return running.length ? [...running, ...recent.filter((r) => !running.some((x) => x.id === r.id))] : recent
}

function iso(ms: number | null): string | null {
  return ms ? new Date(ms).toISOString() : null
}

// JSON output shape is { running, recent } (running first, deduped).
export function toJson(sessions: Session[]) {
  const running = sessions.filter((s) => s.live)
  const recent = [...sessions].filter((s) => !s.live).slice(0, 10)
  const entry = (s: Session) => ({
    id: s.id,
    parentID: s.parentID,
    agent: s.agent,
    model: s.model,
    title: s.title,
    state: s.state,
    lastActivity: iso(s.lastActivity),
    minutesAgo: s.minutesAgo,
    ageMin: s.ageMin,
    subagent: s.subagent,
    warn: s.warn,
    live: s.live,
  })
  const payload: any = {
    updated: new Date().toISOString(),
    method:
      "state inferred: running = live ledger execution with no terminal result (not a 10-min recency window; time_suspended is not cleared on resume and does not mean dead)",
    running: running.map(entry),
    recent: recent.map(entry),
  }
  // Include the machine-owned lineage independently of the ten-row recent
  // display. Consumers can therefore fail closed for a child that fell out of
  // the presentation window instead of mistaking omission for completion.
  const parents = new Set(readLedger().map((event) => event.parentID))
  const history = [...parents].map((parentID) => ({ parentID, children: trackedChildren(parentID, payload) }))
  // Keep the full journal projection auditable, but do not make decades of
  // stale incomplete calls look like current work in status/TUI output.
  payload.trackedHistory = history
  payload.tracked = history.map((group) => ({
    parentID: group.parentID,
    // User-facing activity must be strictly live. Terminal, stale, unresolved,
    // and resumable children remain available in trackedHistory for audit.
    children: group.children.filter((child) => child.state === "running"),
  })).filter((group) => group.children.length)
  return payload
}

export function humanTable(sessions: Session[]) {
  const running = sessions.filter((s) => s.live)
  const other = sessions.filter((s) => !s.live)
  const lines: string[] = []
  lines.push(`live session view: ${new Date().toISOString()}`)
  lines.push(`running: ${running.length} | recent: ${other.length} shown`)
  lines.push("")
  if (running.length) {
    lines.push("RUNNING:")
    for (const s of running) {
      const age = s.minutesAgo != null ? `${s.minutesAgo}m ago` : "?"
      lines.push(
        `  ${s.id} · ${s.agent ?? "?"} · ${s.model ?? "?"} · ${s.title ?? "?"} · last ${age}${s.subagent ? ` · parent ${s.parentID}` : ""}${s.warn ? " · WARN (>30m old)" : ""}`,
      )
    }
    lines.push("")
  } else {
    lines.push("RUNNING: none (no live executions)")
    lines.push("")
  }
  lines.push("RECENT (any state, top 10):")
  for (const s of other.slice(0, 10)) {
    const age = s.minutesAgo != null ? `${s.minutesAgo}m ago` : "never"
    lines.push(`${s.id} · ${s.agent ?? "?"} · ${s.model ?? "?"} · state ${s.state} · last ${age} · ${(s.title ?? "?").slice(0, 60)}`)
  }
  lines.push("")
  lines.push("running = live ledger execution / no terminal result (not a 10-min recency window; time_suspended is not dead)")
  return lines.join("\n")
}

async function main() {
  const db = await openDb(DB_PATH)
  if (!db) {
    console.error("tasks-status: could not open the OpenCode DB - no data")
    process.exit(1)
  }
  const sessions = await collect(db)
  try {
    db.close()
  } catch {}
  if (process.argv.includes("--json")) {
    console.log(JSON.stringify(toJson(sessions), null, 2))
  } else {
    console.log(humanTable(sessions))
  }
}

if (import.meta.main) {
  main().catch((error) => {
    console.error("tasks-status crashed:", error)
    process.exit(1)
  })
}
