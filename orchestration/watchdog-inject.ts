import type { CompletionEvidence as CompletionMetadata } from "./orchestration-ledger"

export type { CompletionMetadata }

/** Deliver an internal synthetic record, never an XML/user prompt card. */
export async function injectCompletion(
  sessionApi: { synthetic?: Function },
  completion: CompletionMetadata,
): Promise<boolean> {
  if (typeof sessionApi.synthetic !== "function") {
    console.error("[orchestration] watchdog: ctx.session.synthetic unavailable")
    return false
  }
  try {
    await sessionApi.synthetic({
      sessionID: completion.parentID,
      text: completion.summary,
      metadata: { kind: "subagent.completion", ...completion },
    })
    return true
  } catch (error) {
    console.error(`[orchestration] watchdog: synthetic completion failed for ${completion.idempotencyKey}:`, error)
    return false
  }
}
