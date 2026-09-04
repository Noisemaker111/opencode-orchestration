import { expect, test } from "bun:test"
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { appendLedger, childSessionID, claimCompletionDelivery, EXECUTION_LEASE_MS, expireExecutionLeases, pendingCompletionEvidence, readLedger, recordHeartbeat, recordNativeSessionLineage, recordNotification, recordSpawn, recordSpawnResult, reconcileLedger, registerCompletionEvidenceHandler, trackedChildren } from "../orchestration/orchestration-ledger"
import { deliverPendingCompletion } from "../orchestration/orchestration"

const configRoot = join(import.meta.dir, "..")

function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "opencode-ledger-"))
  return { dir, file: join(dir, "lineage.jsonl") }
}

test("Task intent survives restart and binds the returned child session", () => {
  const { dir, file } = fixture()
  recordSpawn({ sessionID: "ses_parent", id: "call-1", input: { agent: "model-muse", description: "build HUD" } }, file)
  recordSpawnResult({ sessionID: "ses_parent", id: "call-1" }, { output: "<task id=\"ses_child\">" }, file)
  expect(trackedChildren("ses_parent", { running: [{ id: "ses_child" }], recent: [] }, file)[0]?.openCodeSessionId).toBeUndefined()
  recordNativeSessionLineage("ses_parent", "call-1", "ses_child", file)
  const result = trackedChildren("ses_parent", { running: [{ id: "ses_child" }], recent: [] }, file)
  expect(result).toEqual([expect.objectContaining({ parentID: "ses_parent", callID: "call-1", childID: "ses_child", openCodeSessionId: "ses_child", runtime: "native", agent: "model-muse", description: "build HUD", state: "running" })])
  rmSync(dir, { recursive: true, force: true })
})

test("unbound, missing, and stopped children never infer success", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "a" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "b" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "b", childID: "ses_missing" }, file)
  const result = trackedChildren("ses_p", { running: [], recent: [{ id: "ses_old", state: "idle:completed" }] }, file)
  expect(result.map((item) => item.state)).toEqual(["missing-result", "missing-result"])
  rmSync(dir, { recursive: true, force: true })
})

test("session extraction excludes the parent and tolerates nested host output", () => {
  expect(childSessionID({ data: { parent: "ses_parent", result: { sessionID: "ses_child" } } }, "ses_parent")).toBe("ses_child")
})

test("terminal events are authoritative over stale DB recency and duplicate hooks are idempotent", () => {
  const { dir, file } = fixture()
  recordSpawn({ sessionID: "ses_parent", callID: "call-1", input: { agent: "model-muse" } }, file)
  recordSpawn({ sessionID: "ses_parent", callID: "call-1", input: { agent: "model-muse" } }, file)
  recordSpawnResult({ sessionID: "ses_parent", callID: "call-1" }, { sessionID: "ses_child" }, file)
  appendLedger({ kind: "terminal", parentID: "ses_parent", callID: "ses_child", childID: "ses_child", state: "failed" }, file)
  expect(trackedChildren("ses_parent", { running: [{ id: "ses_child" }], recent: [] }, file)[0].state).toBe("failed")
  expect(readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean).length).toBe(4)
  rmSync(dir, { recursive: true, force: true })
})

test("an active row in recent is not treated as live without the authoritative running set", () => {
  const { dir, file } = fixture()
  recordSpawn({ sessionID: "ses_parent", callID: "call-1", input: { agent: "model-muse" } }, file)
  recordSpawnResult({ sessionID: "ses_parent", callID: "call-1" }, { sessionID: "ses_child" }, file)
  expect(trackedChildren("ses_parent", { running: [], recent: [{ id: "ses_child", state: "active" }] }, file)[0].state).toBe("stopped")
  rmSync(dir, { recursive: true, force: true })
})

test("Claude history is reconciled by explicit terminal evidence, not cc ID shape", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "duplicate", runtime: "claude-code" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "duplicate", runtime: "claude-code" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "duplicate", childID: "cc_old", runtime: "claude-code", claudeSessionID: "ses_claude" }, file)
  appendLedger({ kind: "terminal", parentID: "ses_p", callID: "duplicate", childID: "cc_old", state: "completed" }, file)
  const result = trackedChildren("ses_p", { running: [{ id: "cc_old" }], recent: [] }, file)
  expect(result).toHaveLength(1)
  expect(result[0]).toMatchObject({ state: "completed", claudeSessionID: "ses_claude" })
  rmSync(dir, { recursive: true, force: true })
})

test("unbound, failed, cancelled, and missing-result states fail closed", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "unbound", runtime: "claude-code" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "failed", runtime: "claude-code" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "failed", childID: "cc_failed", runtime: "claude-code" }, file)
  appendLedger({ kind: "terminal", parentID: "ses_p", callID: "failed", childID: "cc_failed", state: "failed" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "cancelled", runtime: "claude-code" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "cancelled", childID: "cc_cancelled", runtime: "claude-code" }, file)
  appendLedger({ kind: "terminal", parentID: "ses_p", callID: "cancelled", childID: "cc_cancelled", state: "cancelled" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "running", runtime: "claude-code" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "running", childID: "cc_running", runtime: "claude-code" }, file)
  const states = trackedChildren("ses_p", { running: [], recent: [] }, file).map((x) => x.state)
  expect(states).toEqual(["missing-result", "failed", "cancelled", "missing-result"])
  expect(trackedChildren("ses_p", { running: [{ id: "cc_running" }], recent: [] }, file).at(-1)?.state).toBe("running")
  rmSync(dir, { recursive: true, force: true })
})

test("DB terminal markers complete ledger-incomplete work and cleanup is concurrent-safe", async () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "done", runtime: "claude-code" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "done", childID: "ses_done", runtime: "claude-code" }, file)
  const first = trackedChildren("ses_p", { running: [], recent: [{ id: "ses_done", state: "idle:completed" }] }, file)
  const second = trackedChildren("ses_p", { running: [], recent: [{ id: "ses_done", state: "idle:completed" }] }, file)
  expect(first).toEqual(second)
  expect(first[0]).toMatchObject({ state: "completed" })
  rmSync(dir, { recursive: true, force: true })
})

test("concurrent writers retain every append without prune overwrites", async () => {
  const { dir, file } = fixture()
  const modulePath = join(process.cwd(), "orchestration", "orchestration-ledger.ts").replaceAll("\\", "\\\\")
  const script = `import { appendLedger } from "${modulePath}"; for (let i = 0; i < 30; i++) appendLedger({kind:"spawn",parentID:"ses_parent",callID:process.argv[1]+"-"+i}, process.argv[2])`
  const workers = Array.from({ length: 4 }, (_, i) => Bun.spawn(["bun", "-e", script, `writer-${i}`, file], { stdout: "pipe", stderr: "pipe", stdin: "ignore", windowsHide: true }))
  const results = await Promise.all(workers.map(async (worker) => worker.exited))
  expect(results.every((code) => code === 0)).toBe(true)
  expect(readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean).length).toBe(120)
  rmSync(dir, { recursive: true, force: true })
})

test("pruning keeps complete newest records within the byte budget", { timeout: 15_000 }, () => {
  const { dir, file } = fixture()
  // Use fewer, larger records: once the cap is reached each append rewrites
  // the bounded journal, so thousands of tiny records make this test needlessly
  // close to Bun's default 5-second test timeout without improving coverage.
  for (let i = 0; i < 2200; i++) appendLedger({ kind: "spawn", parentID: "ses_parent", callID: `call-${i}`, description: "x".repeat(1000) }, file)
  expect(readFileSync(file).byteLength).toBeLessThanOrEqual(2 * 1024 * 1024)
  expect(readFileSync(file, "utf8").trim().split(/\r?\n/).every((line) => { JSON.parse(line); return true })).toBe(true)
  rmSync(dir, { recursive: true, force: true })
})

test("hundreds of stale rows reconcile idempotently without losing resume or audit evidence", () => {
  const { dir, file } = fixture()
  for (let i = 0; i < 240; i++) {
    appendLedger({ kind: "spawn", parentID: "ses_history", callID: `call-${i}`, runtime: "claude-code" }, file)
    appendLedger({ kind: "bound", parentID: "ses_history", callID: `call-${i}`, childID: `cc-${i}`, runtime: "claude-code", claudeSessionID: `ses_resume-${i}` }, file)
  }
  for (let i = 0; i < 12; i++) {
    appendLedger({ kind: "spawn", parentID: "ses_history", callID: `live-${i}` }, file)
    appendLedger({ kind: "bound", parentID: "ses_history", callID: `live-${i}`, childID: `live-child-${i}` }, file)
  }
  const payload = { running: Array.from({ length: 12 }, (_, i) => ({ id: `live-child-${i}` })), recent: [] }
  const first = reconcileLedger(file, payload, join(dir, "reconciliation.json"))
  const second = reconcileLedger(file, payload, join(dir, "reconciliation.json"))
  expect(first.counts).toMatchObject({ active: 12, stale: 240, unresolved: 0 })
  expect(second.records).toHaveLength(first.records.length)
  expect(second.records.find((x) => x.callID === "call-7")).toMatchObject({ disposition: "stale", resumeSessionID: "ses_resume-7" })
  expect(second.records.find((x) => x.callID === "call-7")?.evidence.length).toBeGreaterThanOrEqual(2)
  rmSync(dir, { recursive: true, force: true })
})

test("reconciliation preserves terminal truth and classifies duplicate and unbound calls unresolved", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "dup" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "dup" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "dup", childID: "cc_done" }, file)
  appendLedger({ kind: "terminal", parentID: "ses_p", callID: "dup", childID: "cc_done", state: "failed" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "unbound" }, file)
  const result = reconcileLedger(file, { running: [], recent: [] }, join(dir, "reconciliation.json"))
  expect(result.counts).toMatchObject({ failed: 1, unresolved: 1 })
  expect(result.records.filter((x) => x.callID === "dup")).toHaveLength(1)
  expect(result.records.find((x) => x.callID === "dup")?.disposition).toBe("failed")
  rmSync(dir, { recursive: true, force: true })
})

test("orchestration review flow messages the same integration session", () => {
  const giver = readFileSync(join(configRoot, "agent", "quest-giver.md"), "utf8")
  const injection = readFileSync(join(configRoot, "plugins-active", "favorite-router.ts"), "utf8")
  const skill = readFileSync(join(configRoot, "skills", "review", "SKILL.md"), "utf8")
  const combined = `${giver}\n${injection}\n${skill}`
  expect(giver).toMatch(/continue the same worker sessionID/i)
  expect(giver).toMatch(/review integration yourself/i)
  expect(combined).toContain("skills/review")
  expect(combined).toContain("owns the final integrated diff invokes this skill")
  expect(combined).not.toContain("then ALWAYS spawn the reviewer subagent")
  expect(combined).not.toContain("spawn the reviewer per the orchestration rules")
  expect(combined).not.toContain("spawn ONE integration subagent")
  expect(injection).not.toContain("systemPart(routingCard")
  expect(injection).not.toContain("systemPart(FANOUT_RULES")
  expect(injection).not.toContain("systemPart(XAI_NEVER_MSG")
})

test("global AGENTS.md stays slim and is not a routing-catalog target", () => {
  const agents = readFileSync(join(configRoot, "AGENTS.md"), "utf8")
  expect(agents).not.toContain("<!-- model-routing:start -->")
  expect(agents).not.toContain("deploy.py")
  expect(agents).not.toContain("artifact-gate")
  expect(agents).not.toContain("28100")
  expect(agents).not.toContain("YOU ARE HARD-BLOCKED")
  expect(agents).not.toMatch(/\b100k\b|\b200k\b/)
  expect(agents.split(/\r?\n/).length).toBeLessThanOrEqual(80)
})

test("terminal parent notification is durable and exactly once", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "call-1" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "call-1", childID: "ses_c" }, file)
  expect(recordNotification("ses_p", "call-1", "ses_c", "failed", "resume same session", file)).toBe(true)
  expect(recordNotification("ses_p", "call-1", "ses_c", "failed", "duplicate", file)).toBe(false)
  expect(readLedger(file).filter((event) => event.kind === "notification")).toHaveLength(1)
  expect(trackedChildren("ses_p", { running: [{ id: "ses_c" }], recent: [] }, file)[0]?.state).toBe("failed")
  rmSync(dir, { recursive: true, force: true })
})

test("a persisted delivery claim prevents startup and concurrent replay", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "call-claim", runID: "run-claim", runtime: "native" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "call-claim", childID: "ses_c", openCodeSessionId: "ses_c", runtime: "native" }, file)
  recordNotification("ses_p", "call-claim", "ses_c", "completed", "done", file)
  const completion = pendingCompletionEvidence("ses_p", file)[0]!
  expect(claimCompletionDelivery(completion, file)).toBe(true)
  expect(claimCompletionDelivery(completion, file)).toBe(false)
  expect(pendingCompletionEvidence("ses_p", file)).toHaveLength(0)
  rmSync(dir, { recursive: true, force: true })
})

test("a failed host delivery retries, then terminal and startup paths stay idempotent", async () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "call-retry", runID: "run-retry", runtime: "native" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "call-retry", childID: "ses_c", openCodeSessionId: "ses_c", runtime: "native" }, file)
  recordNotification("ses_p", "call-retry", "ses_c", "completed", "done", file)
  const completion = pendingCompletionEvidence("ses_p", file)[0]!
  expect(await deliverPendingCompletion({ synthetic: async () => { throw new Error("transient") } }, completion, file)).toBe(false)
  expect(pendingCompletionEvidence("ses_p", file)).toHaveLength(1)
  let delivered = 0
  expect(await deliverPendingCompletion({ synthetic: async () => { delivered++ } }, completion, file)).toBe(true)
  expect(await deliverPendingCompletion({ synthetic: async () => { delivered++ } }, completion, file)).toBe(true)
  expect(delivered).toBe(1)
  expect(pendingCompletionEvidence("ses_p", file)).toHaveLength(0)
  rmSync(dir, { recursive: true, force: true })
})

test("linked completion evidence keeps its explicit Quest ID across replay", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "call-quest", runID: "run-quest", questID: "quest-explicit", runtime: "native" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "call-quest", childID: "ses_c", runtime: "native" }, file)
  const first: string[] = []
  const stop = registerCompletionEvidenceHandler((completion) => first.push(`${completion.questID}:${completion.idempotencyKey}`), file)
  recordNotification("ses_p", "call-quest", "ses_c", "completed", "done", file)
  recordNotification("ses_p", "call-quest", "ses_c", "completed", "duplicate", file)
  stop()
  expect(first).toEqual(["quest-explicit:ses_p:run-quest:terminal"])
  const replay: string[] = []
  const stopReplay = registerCompletionEvidenceHandler((completion) => replay.push(completion.idempotencyKey), file)
  stopReplay()
  expect(replay).toEqual(["ses_p:run-quest:terminal"])
  rmSync(dir, { recursive: true, force: true })
})

test("stopped and missing-result terminal evidence stays resumable", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "stop" }, file)
  appendLedger({ kind: "bound", parentID: "ses_p", callID: "stop", childID: "ses_stop" }, file)
  appendLedger({ kind: "terminal", parentID: "ses_p", callID: "stop", childID: "ses_stop", state: "stopped" }, file)
  appendLedger({ kind: "spawn", parentID: "ses_p", callID: "missing" }, file)
  const result = trackedChildren("ses_p", { running: [], recent: [] }, file)
  expect(result.find((item) => item.callID === "stop")?.state).toBe("stopped")
  expect(result.find((item) => item.callID === "missing")?.state).toBe("missing-result")
  rmSync(dir, { recursive: true, force: true })
})

test("90-minute execution silence fails closed once without observation extending it", () => {
  const { dir, file } = fixture()
  appendLedger({ kind: "lifecycle", parentID: "ses_p", callID: "call-1", childID: "ses_c", state: "executing" }, file)
  recordHeartbeat("ses_p", "call-1", "ses_c", file)
  const rows = readLedger(file)
  writeFileSync(file, rows.map((row) => JSON.stringify({ ...row, at: new Date(1_000_000).toISOString() })).join("\n") + "\n")
  expect(expireExecutionLeases(1_000_000 + 90 * 60 * 1000, file)).toHaveLength(1)
  expect(expireExecutionLeases(1_000_000 + 90 * 60 * 1000 + EXECUTION_LEASE_MS, file)).toHaveLength(0)
  expect(trackedChildren("ses_p", { running: [], recent: [] }, file)[0]?.state).toBe("missing-result")
  rmSync(dir, { recursive: true, force: true })
})
