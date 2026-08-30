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
    const navigation = completion.runtime === "native" && completion.openCodeSessionId && /^ses_[A-Za-z0-9_-]+$/.test(completion.openCodeSessionId)
      ? { type: "session", sessionID: completion.openCodeSessionId, parentID: completion.parentID }
      : undefined
    await sessionApi.synthetic({
      sessionID: completion.parentID,
      text: completion.summary,
      metadata: { kind: "subagent.completion", ...completion, ...(navigation ? { navigation } : {}) },
    })
    return true
  } catch (error) {
    console.error(`[orchestration] watchdog: synthetic completion failed for ${completion.idempotencyKey}:`, error)
    return false
  }
}
