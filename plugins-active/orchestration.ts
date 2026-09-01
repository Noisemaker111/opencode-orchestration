/**
 * The orchestration plugin: what did we start, is it alive, did its result
 * ever reach the parent.
 *
 * Policy lives in orchestration.ts at the repo root. This file is the wiring.
 *
 * What it owns:
 *  - the ledger of mcp_agent spawns, session bindings and terminal states
 *  - the watchdog that re-injects a completion an idle parent never received
 *  - presentation of spawned tasks (agent labels, task descriptions)
 *
 * Quest binding used to ride these same hooks. It now lives in the quests
 * plugin: both care about a worker starting and finishing, but they answer
 * different questions, and orchestration must not import quest/.
 */
import { define } from "@opencode-ai/plugin/v2/promise"
import {
  watchSubagentCompletions,
} from "../orchestration/orchestration"
import { recordSpawn, recordSpawnResult } from "../orchestration/orchestration-ledger"
import { canonicalizeDispatch } from "../orchestration/dispatch"

/** Attach a tool hook without letting one bad registration disable the rest. */
async function safeToolHook(hook: Function, name: string, fn: Function, essential = false) {
  try {
    await hook(name, async (...args: unknown[]) => {
      try { return await fn(...args) } catch (error) {
        if (essential) throw error
        console.error(`[orchestration] ${name} hook error:`, error)
      }
    })
  } catch (error) {
    console.error(`[orchestration] could not register ${name}:`, error)
    if (essential) throw error
  }
}

const isSpawn = (event: unknown) => {
  const ev = (event ?? {}) as Record<string, unknown>
  return /^mcp_agent$/i.test(String(ev.tool ?? ev.name ?? ""))
}

const isDispatchAttempt = (event: unknown) => {
  const ev = (event ?? {}) as Record<string, unknown>
  return /^(task|subagent|mcp_agent)$/i.test(String(ev.tool ?? ev.name ?? ""))
}

/** Record mcp_agent intent before execution and its result afterwards. */
export async function installLedger(ctx: { tool?: { hook?: Function } }) {
  const hook = ctx?.tool?.hook
  if (typeof hook !== "function") return
  await safeToolHook(hook, "execute.before", (event: unknown) => {
    if (isSpawn(event)) recordSpawn((event ?? {}) as Record<string, unknown>)
  }, true)
  await safeToolHook(hook, "execute.after", (event: unknown, output?: unknown) => {
    const ev = (event ?? {}) as Record<string, unknown>
    if (isSpawn(event)) recordSpawnResult(ev, output ?? ev.output)
  })
}

/** One fail-closed boundary: direct Task is denied and MCP identity is canonical. */
export async function installCanonicalDispatch(ctx: { tool?: { hook?: Function } }) {
  const hook = ctx?.tool?.hook
  if (typeof hook !== "function") return
  await safeToolHook(hook, "execute.before", (event: unknown) => {
    if (isDispatchAttempt(event)) canonicalizeDispatch(event)
  }, true)
}

/**
 * Re-inject a subagent completion the parent never received.
 *
 * Registered once per server process — the watchdog keeps its state on
 * globalThis so a plugin hot-reload reuses the running watch instead of
 * stacking a second subscription and double-injecting every result.
 *
 * NOT awaited, and it must never become awaited: watchSubagentCompletions
 * ends in `for await (const event of stream)` over the host event stream, so
 * it only returns when the server is going down. Awaiting it means setup()
 * never resolves and the host hangs before it finishes booting — no error,
 * no output, every model "timing out" in the promotion gate.
 */
export function installWatchdog(ctx: unknown) {
  queueMicrotask(() => { void watchSubagentCompletions(ctx) })
}

export default define({
  id: "orchestration",
  async setup(ctx) {
    for (const [name, install] of [
      ["canonical-dispatch", () => installCanonicalDispatch(ctx)],
      ["ledger", () => installLedger(ctx)],
      ["watchdog", () => installWatchdog(ctx)],
    ] as const) {
      try { await install() } catch (error) {
        console.error(`[orchestration] ${name} disabled:`, error)
      }
    }
  },
})
