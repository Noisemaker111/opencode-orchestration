# opencode-orchestration

Orchestration for OpenCode 2 — a ledger of what was spawned, and a watchdog that re-injects a subagent result an idle parent never received instead of leaving it hung.

## What it does

Answers three questions about work that was handed off: what did we start, is
it still alive, and did its result ever get back.

- **A ledger of every Task spawn**, its session binding and its terminal state.
- **A completion watchdog.** When a subagent finishes but the parent is idle
  and never received the result, the result is re-injected instead of the
  parent sitting there looking suspended. If the child died on a spent plan,
  the injected message says so and names the failover.
- **Readable task chrome** — agent labels and descriptions, so a board of
  running work is legible.

The watchdog registers once per server process and keeps its state on
`globalThis`, so a plugin hot-reload reuses the running watch rather than
stacking a second subscription and double-injecting every result.

## Install

```jsonc
// opencode.jsonc
{ "plugin": ["./plugins-active/orchestration.ts"] }
```

## Requires

- `opencode-models` — imported, not vendored. Install it alongside.

## License

MIT
