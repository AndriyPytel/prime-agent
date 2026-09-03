import type { AssistantMessageEvent } from "@earendil-works/pi-ai";
import type { AgentConnectionSessionEvent } from "../agent-connection/types.js";
import type { PrimeAgentIpythonMeta, PrimeAgentSessionMeta } from "./acp-meta.js";
import { primeAgentMeta } from "./acp-meta.js";

/**
 * Translate prime-agent session events into ACP `session/update` payloads.
 *
 * Kept as a pure function so the mapping is testable without a live ACP client
 * or a running agent. Returning an array lets one prime-agent event fan out to
 * several ACP updates (or none, for events ACP has no place for).
 */

export type AcpToolKind = "read" | "edit" | "delete" | "move" | "search" | "execute" | "think" | "fetch" | "other";
export type AcpToolStatus = "pending" | "in_progress" | "completed" | "failed";

export interface AcpSessionUpdate {
	sessionUpdate: string;
	[key: string]: unknown;
}

/** prime-agent's model-facing tool is the Python REPL; bash is the secondary escape hatch. */
export const IPYTHON_TOOL_NAME = "ipython";

export function acpToolKind(toolName: string): AcpToolKind {
	switch (toolName) {
		// Not `execute`: the JetBrains client routes that kind to a terminal
		// block whose details are nulled once the call completes, leaving a
		// truncated fallback of `rawInput.command` — a key a cell has no reason
		// to carry. The generic block renders the cell source unconditionally.
		case IPYTHON_TOOL_NAME:
			return "other";
		case "bash":
			return "execute";
		case "read":
			return "read";
		case "edit":
		case "write":
			return "edit";
		default:
			return "other";
	}
}

/** Decoded byte length of a base64 payload, without materializing it. */
function base64ByteLength(data: string): number {
	const padding = data.endsWith("==") ? 2 : data.endsWith("=") ? 1 : 0;
	return Math.max(0, Math.floor((data.length * 3) / 4) - padding);
}

function textContent(text: string): { type: "text"; text: string } {
	return { type: "text", text };
}

/**
 * Whether an update must wait rather than land between two message chunks.
 *
 * The JetBrains client groups chunks by nothing but the update type next to
 * them: `ChunkBuffer.shouldAppendToBuffer` compares `lastElement.getClass()`,
 * and `Acp2ToAUIConverter` flushes the buffer on any other type, so an update
 * arriving mid-message ends the Markdown block there and a table spanning the
 * gap never parses. It reads no `messageId` at all — the field is absent from
 * the constant pool of both classes, though the deserialized model exposes it —
 * and the spec made it opt-in for exactly this reason (RFD #244, PR #536).
 *
 * These two carry no ordering against the text and are the ones that arrive in
 * bulk. Measured on `~/Library/Logs/JetBrains/WebStorm2026.2/acp/acp.log`,
 * holding them takes torn chunk pairs from 5 of 85 to 2 of 85; what remains is
 * a tool call or a thought, which is a real boundary and not noise.
 */
export function acpDefersWhileStreaming(update: AcpSessionUpdate): boolean {
	return update.sessionUpdate === "session_info_update" || update.sessionUpdate === "usage_update";
}

/**
 * Map one streaming assistant event to an ACP chunk.
 *
 * The delta discriminator lives on the event itself (`text_delta` /
 * `thinking_delta`) and carries a plain string, so reasoning and visible answer
 * text are distinct ACP update kinds a client can render or hide separately.
 *
 * Reasoning streams under its own id because ACP renders thought and answer as
 * different messages.
 */
function assistantDeltaUpdates(event: AssistantMessageEvent, messageId: string): AcpSessionUpdate[] {
	if (event.type === "thinking_delta" && event.delta.length > 0) {
		return [
			{ sessionUpdate: "agent_thought_chunk", messageId: `${messageId}-thought`, content: textContent(event.delta) },
		];
	}
	if (event.type === "text_delta" && event.delta.length > 0) {
		return [{ sessionUpdate: "agent_message_chunk", messageId, content: textContent(event.delta) }];
	}
	return [];
}

const CELL_TITLE_MAX_LENGTH = 120;

/**
 * Cell source is untrusted text, and a title is rendered as a single UI line.
 *
 * Control characters and bidi overrides survive the line split, so without this
 * a cell can reorder or blank out the row that claims to describe it.
 */
const TITLE_UNSAFE = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200e\u200f\u202a-\u202e\u2066-\u2069]/g;

/**
 * One-line label for a Python cell.
 *
 * Every kernel call otherwise carries the same constant title, which leaves an
 * ACP client rendering a column of identical rows with no way to tell one cell
 * from the next.
 */
function ipythonCellTitle(code: string): string {
	const lines = code
		.replace(TITLE_UNSAFE, "")
		.split("\n")
		.filter((line) => line.trim().length > 0);
	let title = lines[0]?.trim() ?? "";
	if (!title) return "Python cell";
	if (title.length > CELL_TITLE_MAX_LENGTH) title = `${title.slice(0, CELL_TITLE_MAX_LENGTH - 1)}\u2026`;
	const remaining = lines.length - 1;
	return remaining > 0 ? `${title} \u00b7 +${remaining} lines` : title;
}

/**
 * A content block that renders verbatim, for clients that render no `rawInput`.
 *
 * Cell source and its output may legally contain a backtick fence of their own,
 * so the fence has to outrun the longest run in the text: a fixed three would
 * let the rest of it escape the code block and render as arbitrary Markdown.
 */
function fencedContent(text: string, language: string): { type: "content"; content: { type: "text"; text: string } } {
	const longestRun = Math.max(0, ...[...text.matchAll(/`+/g)].map((match) => match[0].length));
	const fence = "`".repeat(Math.max(3, longestRun + 1));
	return { type: "content", content: textContent([`${fence}${language}`, text, fence].join("\n")) };
}

function ipythonCellContent(code: string): { type: "content"; content: { type: "text"; text: string } } {
	return fencedContent(code, "python");
}

/**
 * The cell and whatever it has printed, as one content array.
 *
 * A client replaces a tool call's content on update rather than appending to
 * it, so every update has to carry the whole picture: drop the cell here and
 * the source vanishes the moment the first line of output arrives.
 */
function ipythonCallContent(
	cell: string | undefined,
	output: string | undefined,
): { type: "content"; content: { type: "text"; text: string } }[] {
	return [
		...(cell !== undefined ? [ipythonCellContent(cell)] : []),
		...(output ? [fencedContent(output, "text")] : []),
	];
}

/** Extract the Python cell source so a client can show what is executing. */
function ipythonCellSource(args: unknown): string | undefined {
	if (!args || typeof args !== "object") return undefined;
	const code = (args as { code?: unknown }).code;
	return typeof code === "string" ? code : undefined;
}

function toolResultText(result: unknown): string | undefined {
	if (typeof result === "string") return result;
	if (!result || typeof result !== "object") return undefined;
	const output = (result as { output?: unknown }).output;
	if (typeof output === "string") return output;
	const content = (result as { content?: unknown }).content;
	if (Array.isArray(content)) {
		const parts = content
			.map((block) =>
				block && typeof block === "object" && (block as { type?: string }).type === "text"
					? ((block as { text?: string }).text ?? "")
					: "",
			)
			.filter(Boolean);
		if (parts.length > 0) return parts.join("\n");
	}
	return undefined;
}

/**
 * Rich kernel output that ACP has no content type for.
 *
 * The ipython tool reports media and diffs under `details` (images additionally
 * ride along as ACP image content blocks); mirror those exact fields rather than
 * inventing a MIME bundle the tool never produces.
 */
function ipythonRichOutput(result: unknown): PrimeAgentIpythonMeta | undefined {
	if (!result || typeof result !== "object") return undefined;
	const details = (result as { details?: unknown }).details;
	if (!details || typeof details !== "object") return undefined;
	const { attachments, diffs } = details as { attachments?: unknown; diffs?: unknown };
	const meta: PrimeAgentIpythonMeta = {};
	if (Array.isArray(attachments) && attachments.length > 0) {
		meta.attachments = attachments.map((attachment) => {
			// KernelAttachment exposes mimeType, base64 `data`, and an optional path.
			// Report the decoded size rather than a `bytes` field the kernel never
			// sends, and never inline the payload: ACP already carries images as
			// content blocks, so duplicating them here would bloat every update.
			const typed = (attachment ?? {}) as { mimeType?: unknown; path?: unknown; data?: unknown };
			return {
				...(typeof typed.mimeType === "string" ? { mimeType: typed.mimeType } : {}),
				...(typeof typed.path === "string" ? { path: typed.path } : {}),
				...(typeof typed.data === "string" ? { bytes: base64ByteLength(typed.data) } : {}),
			};
		});
	}
	if (Array.isArray(diffs) && diffs.length > 0) meta.diffCount = diffs.length;
	return meta.attachments || meta.diffCount !== undefined ? meta : undefined;
}

/** Correlates streamed bash output and assistant chunks with their owning run or message. */
export interface AcpEventMappingState {
	activeBashRunId?: string;
	activeAssistantMessageId?: string;
	nextAssistantMessageSequence?: number;
	/** Cell source per in-flight IPython call, keyed by tool call id. */
	ipythonCells?: Map<string, string>;
	/** Output streamed so far per in-flight IPython call, keyed by tool call id. */
	ipythonOutput?: Map<string, string>;
	/** Whether an assistant message is between its first chunk and its end. */
	streaming?: boolean;
	/** Last telemetry published per subagent, keyed by child id. */
	lastChildInfo?: Map<string, string>;
}

function startAssistantMessage(state: AcpEventMappingState): string {
	const sequence = (state.nextAssistantMessageSequence ?? 0) + 1;
	state.nextAssistantMessageSequence = sequence;
	state.activeAssistantMessageId = `prime-agent-assistant-${sequence}`;
	return state.activeAssistantMessageId;
}

export function acpUpdatesForSessionEvent(
	event: AgentConnectionSessionEvent,
	state: AcpEventMappingState = {},
): AcpSessionUpdate[] {
	switch (event.type) {
		case "message_start":
			if (event.message.role === "assistant") startAssistantMessage(state);
			return [];

		case "message_update": {
			if (event.message.role !== "assistant") return [];
			const updates = assistantDeltaUpdates(
				event.assistantMessageEvent,
				state.activeAssistantMessageId ?? startAssistantMessage(state),
			);
			if (updates.length > 0) state.streaming = true;
			return updates;
		}

		case "message_end":
			if (event.message.role !== "assistant") return [];
			state.activeAssistantMessageId = undefined;
			state.streaming = false;
			return [];

		// A cancelled or interrupted call never reports an end, so the run ending
		// is the point at which a still-held cell is known to be unreachable, and
		// the point a message left open by the cancellation is known to be over.
		case "agent_end":
			state.ipythonCells?.clear();
			state.ipythonOutput?.clear();
			state.streaming = false;
			return [];

		case "tool_execution_start": {
			const cell = event.toolName === IPYTHON_TOOL_NAME ? ipythonCellSource(event.args) : undefined;
			if (cell !== undefined) {
				state.ipythonCells ??= new Map();
				state.ipythonCells.set(event.toolCallId, cell);
			}
			return [
				{
					sessionUpdate: "tool_call",
					toolCallId: event.toolCallId,
					title: cell !== undefined ? ipythonCellTitle(cell) : event.toolName,
					kind: acpToolKind(event.toolName),
					status: "in_progress" satisfies AcpToolStatus,
					...(cell !== undefined ? { content: ipythonCallContent(cell, undefined) } : {}),
					rawInput: cell !== undefined ? { code: cell } : event.args,
				},
			];
		}

		// Output arrives in chunks while the cell runs, and a client that renders
		// a tool call's content shows nothing of a long loop until it ends. The
		// accumulated output is republished with the cell on every chunk, which
		// is what makes a running cell readable rather than a spinner.
		case "tool_execution_update": {
			if (event.toolName !== IPYTHON_TOOL_NAME) return [];
			const chunk = toolResultText(event.partialResult);
			if (!chunk) return [];
			state.ipythonOutput ??= new Map();
			const output = (state.ipythonOutput.get(event.toolCallId) ?? "") + chunk;
			state.ipythonOutput.set(event.toolCallId, output);
			return [
				{
					sessionUpdate: "tool_call_update",
					toolCallId: event.toolCallId,
					status: "in_progress" satisfies AcpToolStatus,
					content: ipythonCallContent(state.ipythonCells?.get(event.toolCallId), output),
				},
			];
		}

		case "tool_execution_end": {
			const text = toolResultText(event.result);
			const rich = event.toolName === IPYTHON_TOOL_NAME ? ipythonRichOutput(event.result) : undefined;
			// A client replaces a tool call's content on update rather than
			// appending to it, so the cell has to be repeated here or it vanishes
			// the moment the call completes.
			const cell = state.ipythonCells?.get(event.toolCallId);
			state.ipythonCells?.delete(event.toolCallId);
			state.ipythonOutput?.delete(event.toolCallId);
			const content = ipythonCallContent(cell, text);
			return [
				{
					sessionUpdate: "tool_call_update",
					toolCallId: event.toolCallId,
					status: (event.isError ? "failed" : "completed") satisfies AcpToolStatus,
					...(content.length > 0 ? { content } : {}),
					...(rich ? { _meta: primeAgentMeta({ ipython: rich }) } : {}),
				},
			];
		}

		// Bash runs outside the tool-call lifecycle, so it gets a synthetic tool
		// call keyed by run id to keep incremental output addressable.
		case "bash_start":
			state.activeBashRunId = event.runId;
			return [
				{
					sessionUpdate: "tool_call",
					toolCallId: bashToolCallId(event.runId),
					title: event.command,
					kind: "execute" satisfies AcpToolKind,
					status: "in_progress" satisfies AcpToolStatus,
					rawInput: { command: event.command },
				},
			];

		case "bash_output":
			return [
				{
					sessionUpdate: "tool_call_update",
					toolCallId: bashToolCallId(state.activeBashRunId),
					status: "in_progress" satisfies AcpToolStatus,
					content: [{ type: "content", content: textContent(event.chunk) }],
				},
			];

		case "bash_end":
			if (state.activeBashRunId === event.runId) state.activeBashRunId = undefined;
			return [
				{
					sessionUpdate: "tool_call_update",
					toolCallId: bashToolCallId(event.runId),
					status: (event.exitCode === 0 && !event.cancelled ? "completed" : "failed") satisfies AcpToolStatus,
				},
			];

		// Compaction, subagents, goals and recaps have no ACP equivalent: surface
		// them as namespaced metadata rather than distorting a standard update.
		case "compaction_end":
			return [
				{
					sessionUpdate: "session_info_update",
					_meta: primeAgentMeta({
						compaction: {
							tokensBefore: event.result?.tokensBefore,
							summary: event.result?.summary,
						},
					}),
				},
			];

		// A running child republishes its whole record on every tick, most of them
		// byte-identical: one measured session sent 13,693 of these updates
		// carrying 670 distinct states. Each one lands between two text chunks and
		// costs the client a re-render, so only a changed record is worth sending.
		case "rlm_child_update": {
			const child = {
				id: event.child.id,
				sessionName: event.child.sessionName,
				status: event.child.status,
				model: event.child.model,
				tokenCount: event.child.tokenCount,
				error: event.child.error,
			};
			const published = JSON.stringify(child);
			state.lastChildInfo ??= new Map();
			if (state.lastChildInfo.get(child.id) === published) return [];
			state.lastChildInfo.set(child.id, published);
			return [
				{
					sessionUpdate: "session_info_update",
					_meta: primeAgentMeta({ subagents: [child] }),
				},
			];
		}

		// Goals, continual-harness refinement, and agent-to-agent messaging are
		// prime-agent concepts with no ACP counterpart. They are still part of a
		// turn's observable behavior, so they surface as namespaced metadata
		// instead of being dropped.
		case "goal_update":
			return [
				{
					sessionUpdate: "session_info_update",
					_meta: primeAgentMeta({
						goal: {
							status: event.goal.status,
							objective: event.goal.objective,
							tokenBudget: event.goal.tokenBudget,
							tokensUsed: event.goal.tokensUsed,
						},
					}),
				},
			];

		case "refine_complete":
			return [
				{
					sessionUpdate: "session_info_update",
					_meta: primeAgentMeta({
						refinement: {
							status: "complete",
							summary: event.result.summary,
							changes: event.result.appliedEdits
								?.filter((edit) => edit.applied)
								.map((edit) => `${edit.action} ${edit.kind}:${edit.id}`),
						},
					}),
				},
			];

		case "refine_failed":
			return [
				{
					sessionUpdate: "session_info_update",
					_meta: primeAgentMeta({ refinement: { status: "failed", error: event.error } }),
				},
			];

		case "ipython_sent_agent_message":
			return [
				{
					sessionUpdate: "session_info_update",
					_meta: primeAgentMeta({
						agentMessage: {
							toolCallId: event.toolCallId,
							target: event.message.target.sessionName ?? event.message.target.sessionId,
							deliveryStatus: event.message.deliveryStatus,
						},
					}),
				},
			];

		default:
			return [];
	}
}

const BASH_TOOL_CALL_PREFIX = "prime-agent-bash";

export function bashToolCallId(runId: string | undefined): string {
	return runId ? `${BASH_TOOL_CALL_PREFIX}-${runId}` : BASH_TOOL_CALL_PREFIX;
}

export type { PrimeAgentSessionMeta };
