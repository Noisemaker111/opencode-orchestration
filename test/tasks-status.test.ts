/**
 * Running is live ledger execution, not a 10-minute recency window.
 * time_suspended is not cleared on resume and must not mean "dead".
 */
import { expect, test } from "bun:test"
import {
  executingChildIDs,
  humanTable,
  isLiveRunning,
  toJson,
  type Session,
} from "../orchestration/tasks-status.ts"

const NOW = Date.parse("2026-08-28T12:00:00.000Z")
const MIN = 60_000

function session(partial: Partial<Session> & Pick<Session, "id">): Session {
  const lastActivity = partial.lastActivity ?? NOW
  return {
    parentID: partial.parentID ?? "ses_parent",
    agent: partial.agent ?? "model-grok-sub-grok-4-6",
    model: partial.model ?? "grok-sub/grok-4.6",
    title: partial.title ?? "long grok turn",
    state: partial.state ?? "active",
    timeCreated: partial.timeCreated ?? NOW - 20 * MIN,
    timeUpdated: partial.timeUpdated ?? lastActivity,
    lastActivity,
    minutesAgo: partial.minutesAgo ?? Math.round((NOW - lastActivity) / MIN),
    ageMin: partial.ageMin ?? 20,
    subagent: partial.subagent ?? true,
    warn: partial.warn ?? false,
    live: partial.live ?? false,
    ...partial,
  }
}

test("executing child with 15-min silence is still live", () => {
  const executing = executingChildIDs([
    { childID: "ses_grok", kind: "lifecycle", state: "executing" },
  ])
  const grok = session({ id: "ses_grok", lastActivity: NOW - 15 * MIN, minutesAgo: 15, state: "suspended" })
  expect(executing.has("ses_grok")).toBe(true)
  expect(isLiveRunning(grok, NOW, executing)).toBe(true)
})

test("time_suspended / suspended label does not veto a live execution", () => {
  const executing = new Set(["ses_grok"])
  expect(isLiveRunning(session({ id: "ses_grok", state: "suspended", lastActivity: NOW - 40 * MIN, minutesAgo: 40 }), NOW, executing)).toBe(true)
})

test("terminal result is not running even if last message is 1 min ago", () => {
  const executing = executingChildIDs([
    { childID: "ses_done", kind: "lifecycle", state: "executing" },
    { childID: "ses_done", kind: "terminal", state: "completed" },
  ])
  expect(executing.has("ses_done")).toBe(false)
  expect(isLiveRunning(session({ id: "ses_done", lastActivity: NOW - MIN, minutesAgo: 1 }), NOW, executing)).toBe(false)
})

test("notification / missing-result closes the live claim", () => {
  const executing = executingChildIDs([
    { childID: "ses_a", state: "executing" },
    { childID: "ses_a", kind: "notification", state: "missing-result" },
    { childID: "ses_b", state: "executing" },
    { childID: "ses_b", kind: "terminal", state: "failed" },
  ])
  expect([...executing]).toEqual([])
})

test("subagent recency alone is not running", () => {
  expect(isLiveRunning(session({ id: "ses_quiet", lastActivity: NOW - MIN, minutesAgo: 1 }), NOW, new Set())).toBe(false)
})

test("archived is never running", () => {
  expect(isLiveRunning(session({ id: "ses_grok", state: "archived" }), NOW, new Set(["ses_grok"]))).toBe(false)
})

test("toJson puts a silent live worker in running, finished in recent", () => {
  const live = session({ id: "ses_grok", live: true, lastActivity: NOW - 15 * MIN, minutesAgo: 15, state: "active" })
  const done = session({
    id: "ses_done",
    live: false,
    lastActivity: NOW - 2 * MIN,
    minutesAgo: 2,
    state: "idle:completed",
    title: "finished worker",
  })
  const payload = toJson([live, done])
  expect(payload.running.map((s: { id: string }) => s.id)).toEqual(["ses_grok"])
  expect(payload.recent.map((s: { id: string }) => s.id)).toEqual(["ses_done"])
  expect(payload.method).not.toMatch(/10\s*min/i)
  expect(humanTable([live, done])).toContain("ses_grok")
  expect(humanTable([live, done])).toContain("RECENT")
  expect(humanTable([live, done])).not.toMatch(/none within the last 10 minutes/)
})

test("interactive parent recency still shows as running without a ledger claim", () => {
  const parent = session({
    id: "ses_parent",
    parentID: null,
    subagent: false,
    lastActivity: NOW - 3 * MIN,
    minutesAgo: 3,
  })
  expect(isLiveRunning(parent, NOW, new Set())).toBe(true)
  expect(isLiveRunning({ ...parent, lastActivity: NOW - 20 * MIN }, NOW, new Set())).toBe(false)
})
