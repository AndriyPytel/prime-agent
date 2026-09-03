# ACP Mode

ACP mode makes Prime Agent an [Agent Client Protocol](https://agentclientprotocol.com) agent, speaking JSON-RPC 2.0 over newline-delimited JSON on stdin/stdout. Any ACP client — an editor like Zed or VS Code, or an evaluation harness — can drive it without knowing anything Prime Agent-specific.

```bash
prime-agent --mode acp
```

Use ACP mode when something external needs to *drive* a session interactively: prompt, watch tool calls stream, cancel a turn. For batch runs where you want every event dumped and an exit code, [JSON event stream mode](json.md) is a better fit. [RPC mode](rpc.md) remains available and exposes Prime Agent's own richer command surface.

## Transport

- One JSON-RPC message per line on stdout, requests read from stdin.
- stdin stays open for the life of the connection; the agent exits when it closes.
- Diagnostics go to stderr. Never write anything else to stdout, which belongs to the protocol.

## Supported methods

| Method | Notes |
|---|---|
| `initialize` | Returns protocol version, capabilities, and agent info. |
| `session/new` | Creates the session. One session per connection. |
| `session/prompt` | Runs one turn and resolves with a stop reason. |
| `session/set_config_option` | Switches the session's model. |
| `session/cancel` | Notification; aborts the addressed session's turn. |
| `session/close` | Releases the session and frees the connection for a new one. |

One session per connection is a deliberate limit: Prime Agent's underlying session is fixed at process startup, so a second concurrent session would silently share its conversation, working directory, and model. A second `session/new` is refused rather than pretending to isolate. Start another process for a second session.

Likewise `session/prompt` refuses a concurrent turn while one is running, and the working directory cannot be changed after startup — a client-supplied `cwd` that differs from the agent's real one is reported back in `_meta` rather than silently ignored.

<<<<<<< HEAD
## MCP servers

Prime Agent accepts standard stdio and HTTP servers in `session/new.mcpServers`. The servers are
available through the pre-imported `mcp` Python program for that ACP session:

```python
tools = await mcp.list_tools("task-tools")
result = await mcp.call_tool("task-tools", "lookup", {"query": "example"})
```

HTTP requests use only the URL and headers supplied by the ACP client. They do not read
`auth.json`, start or refresh Prime Agent OAuth, or modify persistent MCP settings. Stdio servers
run with the agent's actual session cwd, the supplied command and arguments, a scrubbed base
environment, and the exact environment values supplied by the ACP client.

The configuration is removed when the ACP session closes or the client disconnects. A same-named
persistent MCP server can therefore be shadowed for the ACP session without sending its stored
OAuth credential to the client-supplied HTTP endpoint. Daemon-backed configuration is bound to the
ACP connection that installed it, so another attached client cannot replace or clear it.

ACP stdio is a trusted-code boundary, not a sandbox. The requested command runs as the Prime Agent
user and can access any files that user can access, including credential stores. Only accept stdio
servers from trusted ACP clients or run Prime Agent inside an appropriate sandbox.
=======
## Model selection

`session/new` returns a `model` [session config option](https://agentclientprotocol.com/protocol/session-config-options) — an ACP `select` in the `model` category — so a client can render a model picker:

```json
{
  "id": "model",
  "name": "Model",
  "category": "model",
  "type": "select",
  "currentValue": "anthropic/claude-sonnet-4-5",
  "options": [{ "value": "anthropic/claude-sonnet-4-5", "name": "claude-sonnet-4-5", "description": "anthropic" }]
}
```

Values are the canonical `provider/id` reference, since the same model id is served by more than one provider. `session/set_config_option` switches the model and answers with the complete configuration state.

Only models with configured credentials are offered. The TUI lists the rest and starts a sign-in when one is picked; ACP has no sign-in flow, so an unauthenticated model would be a choice that only fails at the next prompt.
>>>>>>> 87ce76424 (feat(acp): add model picker to ACP mode)

## Streamed updates

Session activity arrives as `session/update` notifications:

| Prime Agent activity | ACP update |
|---|---|
| assistant text | `agent_message_chunk` |
| reasoning | `agent_thought_chunk` |
| tool starts | `tool_call` (`in_progress`) |
| tool finishes | `tool_call_update` (`completed` / `failed`) |
| shell output | `tool_call` plus incremental `tool_call_update` |
| context fills up | `usage_update` (tokens in context, context window) |

Context usage is reported after every assistant message and after compaction, so a client can show how full the window is. It is omitted while the number is unknown — right after compaction, until the next response is costed — rather than reported as zero.

After `session/new`, one `available_commands_update` lists the commands a prompt turn executes: the session commands (`compact`, `refine`, `goal`, `autonomous`), skills as `skill:<name>`, prompt templates, and extension commands. Commands that open a TUI selector, such as `/model` and `/settings`, are not advertised — outside the TUI they are ordinary prompt text.

Each name appears once, resolved the way a submitted command is: a session builtin first, then an extension command, then a skill, then a prompt template. The list is an initial snapshot; ACP has no method to reload resources, and a reload from elsewhere raises no session event, so nothing can invalidate it for the life of the connection.

The Python REPL is Prime Agent's model-facing tool, so a cell is a `tool_call` of kind `other` whose `rawInput` carries the cell source. The kind is deliberately not `execute`: a client that renders that kind as a terminal block drops the source once the call completes and falls back to a truncated `rawInput.command`, which a cell never carries. Output is streamed into the same call: every chunk republishes the cell and everything printed so far as fenced content blocks, so a long-running cell is readable while it runs and stays readable after it ends. The call is titled by the cell's first meaningful line, and the source also travels as a fenced content block for clients that render content rather than `rawInput`. Both treat the cell as untrusted text: the fence outruns the longest backtick run in the source, and control characters and bidi overrides are stripped from the title.
>>>>>>> c136b7f8a (fix(acp): advertise one command per name, in submission order)

## Prime Agent extensions

Prime Agent has capabilities ACP has no field for: subagents, autonomous quality gates, goals, heartbeats, continual-harness refinement, compaction, and rich kernel output. These travel in a reverse-domain `_meta` envelope:

```json
{
  "sessionUpdate": "session_info_update",
  "_meta": {
    "ai.primeintellect.prime-agent": {
      "subagents": [{ "id": "sub-1", "sessionName": "reviewer", "status": "running" }]
    }
  }
}
```

A standard ACP client ignores `_meta` entirely and still works. A Prime Agent-aware client, or a harness that cares about subagent trees and gate attempts, reads it. Nothing non-standard is ever added to an ACP object root, which the protocol reserves for future fields.

## Stop reasons

`session/prompt` resolves with one of ACP's stop reasons:

- `end_turn` — the turn finished normally.
- `cancelled` — `session/cancel` aborted it.
- `max_tokens` — an autonomous token budget was exhausted.
- `max_turn_requests` — an autonomous turn, continuation, or wall-clock limit stopped the run.

Autonomous quality gates run *inside* a single prompt turn. A failing gate is a continuation, not a stop reason, so the turn resolves only once the gate loop settles. Gate attempts are visible in `_meta` while that happens.
