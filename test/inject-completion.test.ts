import { expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { injectCompletion, type CompletionMetadata } from "../orchestration/watchdog-inject"

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..")
const completion: CompletionMetadata = {
  idempotencyKey: "ses_parent:call-1:ses_child:terminal",
  parentID: "ses_parent",
  callID: "call-1",
  runID: "call-1",
  openCodeSessionId: "ses_child",
  state: "completed",
  summary: "worker done",
  providerID: "openai",
  modelID: "gpt-5.6-luna-fast",
  agentRole: "build",
  runtime: "native",
}

test("delivers structured completion metadata as one synthetic record", async () => {
  const calls: unknown[] = []
  expect(await injectCompletion({ synthetic: async (args: unknown) => { calls.push(args) } }, completion)).toBe(true)
  expect(calls).toEqual([{
    sessionID: "ses_parent",
    text: "worker done",
    metadata: { kind: "subagent.completion", ...completion, navigation: { type: "session", sessionID: "ses_child", parentID: "ses_parent" } },
  }])
})

test("Claude runtime IDs never become OpenCode navigation metadata", async () => {
  const calls: any[] = []
  await injectCompletion({ synthetic: async (args: unknown) => { calls.push(args) } }, {
    ...completion,
    runtime: "claude-code",
    openCodeSessionId: undefined,
    runtimeSessionId: "123e4567-e89b-12d3-a456-426614174000",
  })
  expect(calls[0].metadata.navigation).toBeUndefined()
  expect(calls[0].metadata.runtimeSessionId).toBe("123e4567-e89b-12d3-a456-426614174000")
})

test("never creates an internal user prompt or XML task card", async () => {
  let prompted = false
  await injectCompletion({
    synthetic: async () => {},
    prompt: async () => { prompted = true },
  } as any, completion)
  expect(prompted).toBe(false)
  const helper = readFileSync(join(ROOT, "orchestration", "watchdog-inject.ts"), "utf8")
  expect(helper).not.toMatch(/sessionApi\.prompt|delivery:\s*["']queue|<task>|<task_result>/)
})

test("fails closed when the host has no synthetic metadata API", async () => {
  expect(await injectCompletion({}, completion)).toBe(false)
})
