import { randomUUID } from "node:crypto";
import { realpathSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { Readable } from "node:stream";
import * as acp from "@agentclientprotocol/sdk";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { ImageContent } from "@earendil-works/pi-ai";
import { VERSION } from "../../config.js";
import type { AgentSessionRuntime } from "../../core/agent-session-runtime.js";
import type { AgentAutonomousStatus } from "../../core/autonomous.js";
import { takeOverStdout, writeRawStdout } from "../../core/output-guard.js";
import { BUILTIN_SLASH_COMMANDS } from "../../core/slash-commands.js";
import { InProcessAgentConnection } from "../agent-connection/in-process-agent-connection.js";
import type {
	AgentConnection,
	AgentConnectionModel,
	AgentConnectionRlmChildAgentSnapshot,
	AgentConnectionSessionEvent,
	AgentConnectionSessionInputPause,
	AgentConnectionSlashCommand,
} from "../agent-connection/types.js";
import { latestAutonomousGateAttempt } from "../headless-completion.js";
import {
	type AcpEventMappingState,
	type AcpSessionUpdate,
	acpDefersWhileStreaming,
	acpUpdatesForSessionEvent,
} from "./acp-events.js";
import { resolveAcpMcpServers } from "./acp-mcp.js";
import { PRIME_AGENT_META_NAMESPACE, type PrimeAgentAutonomousMeta, primeAgentMeta } from "./acp-meta.js";
import { type AcpStopReason, acpStopReason } from "./acp-stop-reason.js";

/**
 * ACP frames must reach real stdout.
 *
 * Startup calls `takeOverStdout()` for every non-interactive mode, which
 * redirects `process.stdout.write` to stderr so stray logging cannot corrupt a
 * machine-readable stream. Handing `process.stdout` to the SDK would therefore
 * publish the whole protocol on stderr; write through the raw escape hatch the
 * guard exposes, exactly as RPC mode does.
 */
function rawStdoutSink(): WritableStream<Uint8Array> {
	const decoder = new TextDecoder();
	return new WritableStream<Uint8Array>({
		write(chunk) {
			writeRawStdout(typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true }));
		},
	});
}

function normalizeWindowsDriveLetter(path: string): string {
	if (process.platform !== "win32" || !/^[A-Z]:/i.test(path)) return path;
	return path.slice(0, 1).toLowerCase() + path.slice(1);
}

function canonicalCwd(path: string): string {
	const resolved = resolve(path);
	let canonical: string;
	try {
		canonical = realpathSync(resolved);
	} catch {
		// Preserve the previous lexical comparison when a path is missing or inaccessible.
		canonical = resolved;
	}
	return normalizeWindowsDriveLetter(canonical);
}

function isJsonRpcResponse(message: unknown, requestId: unknown): boolean {
	if (typeof message !== "object" || message === null) return false;
	const record = message as Record<string, unknown>;
	return (
		record.jsonrpc === "2.0" &&
		record.id === requestId &&
		!Object.hasOwn(record, "method") &&
		Object.hasOwn(record, "result") !== Object.hasOwn(record, "error")
	);
}

function sameCwd(left: string, right: string): boolean {
	const canonicalLeft = canonicalCwd(left);
	const canonicalRight = canonicalCwd(right);
	if (canonicalLeft === canonicalRight) return true;

	try {
		const leftStat = statSync(canonicalLeft, { bigint: true });
		const rightStat = statSync(canonicalRight, { bigint: true });
		// Either half being zero makes the pair untrustworthy: Windows path-based
		// stat can report dev 0 with a real ino, and comparing ino alone would match
		// distinct directories on different volumes, since file IDs are volume-local.
		const leftIdentityMissing = leftStat.dev === 0n || leftStat.ino === 0n;
		const rightIdentityMissing = rightStat.dev === 0n || rightStat.ino === 0n;
		if (leftIdentityMissing || rightIdentityMissing) return false;
		return leftStat.dev === rightStat.dev && leftStat.ino === rightStat.ino;
	} catch {
		return false;
	}
}

export interface AcpModeOptions {
	/** Bind headless extensions once the connection is live (in-process mode). */
	bindHeadlessExtensions?: () => Promise<void>;
	/**
	 * Transport override. Defaults to NDJSON over stdio; tests supply an
	 * in-memory stream pair so the protocol runs without a subprocess.
	 */
	stream?: ReturnType<typeof acp.ndJsonStream>;
	/** Skip claiming stdout when the caller supplies its own transport. */
	ownStdout?: boolean;
}

interface AcpPendingTerminal {
	promptTurnId: number;
	boundary: TurnBoundary;
	outcome: "result" | "error";
	abort: AbortController;
	status?: AgentAutonomousStatus;
	turnFailure?: string;
	failure?: string;
	task?: Promise<void>;
}

interface AcpInputPauseRelease {
	promise: Promise<void>;
	resolve(): void;
	reject(error: unknown): void;
}

interface AcpSessionEntry {
	id: string;
	abort: AbortController | undefined;
	cancelling: boolean;
	cancelTask: Promise<void> | undefined;
	stopFailure: string | undefined;
	inputPause: AgentConnectionSessionInputPause | undefined;
	inputPauseKey: string | undefined;
	inputPauseRelease: AcpInputPauseRelease | undefined;
	pendingTerminal: AcpPendingTerminal | undefined;
	promptTask: Promise<void> | undefined;
	resolvePromptTask: (() => void) | undefined;
	unsubscribe: (() => void) | undefined;
	commandsTimer: ReturnType<typeof setTimeout> | undefined;
	producer: AcpUpdateProducer;
}

/**
 * The sole producer of ACP session updates for one ACP session.
 *
 * ACP notifications are asynchronous, so assigning an id at each call site is
 * insufficient: detached calls can be observed out of order. This producer
 * serializes publication and stamps the *delivered* order. Its phase/outcome
 * fields are application metadata, deliberately independent of ACP stop
 * reasons such as `end_turn`.
 */
class AcpUpdateProducer {
	private eventSequence = 0;
	private nextPromptTurnId = 0;
	private activePromptTurnId = 0;
	private tail: Promise<void> = Promise.resolve();
	private readonly childOriginTurnIds = new Map<string, number>();
	private readonly terminalChildOriginTurns = new Set<number>();
	private readonly responseCommittedTurns = new Set<number>();
	private readonly terminalLifecycleTurns = new Set<number>();
	private readonly finishedPromptTurns = new Set<number>();
	private readonly admissionReady: Promise<void>;
	private releaseAdmission!: () => void;
	private admissionOpen = false;
	private admissionClosed = false;

	constructor(
		private readonly sessionId: string,
		private readonly client: { notify(method: unknown, params: unknown): Promise<unknown> },
	) {
		// Subscribe before the initial snapshot, but do not let that subscription
		// publish a session-bound update before session/new has replied.
		this.admissionReady = new Promise<void>((resolve) => {
			this.releaseAdmission = resolve;
		});
	}

	commitSessionNewResponse(): void {
		if (this.admissionClosed) return;
		this.admissionOpen = true;
		this.releaseAdmission();
	}

	failSessionNewAdmission(): void {
		if (this.admissionOpen || this.admissionClosed) return;
		this.admissionClosed = true;
		this.releaseAdmission();
	}

	beginPrompt(): number {
		this.activePromptTurnId = ++this.nextPromptTurnId;
		return this.activePromptTurnId;
	}

	private cleanupTurn(turnId: number): void {
		this.responseCommittedTurns.delete(turnId);
		if (![...this.childOriginTurnIds.values()].some((originTurnId) => originTurnId === turnId)) {
			this.terminalChildOriginTurns.delete(turnId);
		}
	}

	beginTerminalLifecycle(turnId: number): void {
		this.terminalLifecycleTurns.add(turnId);
	}

	finishPrompt(turnId: number): void {
		if (this.activePromptTurnId === turnId) this.activePromptTurnId = 0;
		if (this.terminalLifecycleTurns.has(turnId)) {
			this.finishedPromptTurns.add(turnId);
			return;
		}
		this.cleanupTurn(turnId);
	}

	finishTerminalLifecycle(turnId: number): void {
		this.terminalLifecycleTurns.delete(turnId);
		if (this.finishedPromptTurns.delete(turnId)) this.cleanupTurn(turnId);
	}

	/**
	 * Cut a scoreable terminal boundary before it is queued. A subscription
	 * callback after this point is connection-scoped, never appended to a turn
	 * that an evaluator may treat as terminal.
	 */
	commitResponse(turnId: number): void {
		this.responseCommittedTurns.add(turnId);
	}

	isResponseCommitted(turnId: number): boolean {
		return this.responseCommittedTurns.has(turnId);
	}

	sealTerminal(turnId: number): void {
		this.commitResponse(turnId);
		if ([...this.childOriginTurnIds.values()].some((originTurnId) => originTurnId === turnId)) {
			this.terminalChildOriginTurns.add(turnId);
		}
		if (this.activePromptTurnId === turnId) this.activePromptTurnId = 0;
	}

	turnForEvent(event: AgentConnectionSessionEvent): number {
		if (event.type === "rlm_child_update") {
			const known = this.childOriginTurnIds.get(event.child.id);
			const originTurnId = known ?? this.activePromptTurnId;
			const turnId = this.terminalChildOriginTurns.has(originTurnId) ? 0 : originTurnId;
			const childFinished = ["done", "error", "cancelled"].includes(event.child.status);
			if (childFinished) {
				this.childOriginTurnIds.delete(event.child.id);
				if (![...this.childOriginTurnIds.values()].some((origin) => origin === originTurnId)) {
					this.terminalChildOriginTurns.delete(originTurnId);
				}
			} else if (known === undefined) {
				// Remember its initial origin, including connection scope, so a later
				// child update cannot be relabelled by a subsequent prompt.
				this.childOriginTurnIds.set(event.child.id, originTurnId);
			}
			return turnId;
		}
		return this.activePromptTurnId;
	}

	async publish(
		update: Record<string, unknown>,
		turnId: number,
		phase: "event" | "responseBoundary" | "terminalQuiescence",
		outcome?: "result" | "error",
	): Promise<boolean> {
		// Admission is synchronous through the tail assignment below: close either
		// rejects this call here or drains the update after it joins the queue.
		if (this.admissionClosed) return false;
		const eventSequence = ++this.eventSequence;
		const priorMeta = (update._meta && typeof update._meta === "object" ? update._meta : {}) as Record<
			string,
			unknown
		>;
		const priorPrimeMeta =
			priorMeta[PRIME_AGENT_META_NAMESPACE] && typeof priorMeta[PRIME_AGENT_META_NAMESPACE] === "object"
				? (priorMeta[PRIME_AGENT_META_NAMESPACE] as Record<string, unknown>)
				: {};
		const correlatedUpdate = {
			...update,
			_meta: {
				...priorMeta,
				[PRIME_AGENT_META_NAMESPACE]: {
					...priorPrimeMeta,
					promptTurnId: turnId,
					eventSequence,
					phase,
					...(outcome ? { outcome } : {}),
				},
			},
		};
		// Keep the chain alive after a failed notification, while preserving
		// the order of every later notification and allowing callers to await its drain.
		let published = false;
		this.tail = this.tail.then(async () => {
			try {
				await this.admissionReady;
				if (!this.admissionOpen) return;
				await this.client.notify(acp.methods.client.session.update, {
					sessionId: this.sessionId,
					update: correlatedUpdate,
				});
				published = true;
			} catch {
				// Drop only this update; a rejected queue tail would strand later updates.
			}
		});
		await this.tail;
		return published;
	}

	drain(): Promise<void> {
		return this.tail;
	}

	async close(): Promise<void> {
		this.admissionClosed = true;
		this.releaseAdmission();
		await this.tail;
		this.admissionOpen = false;
	}
}

/**
 * Split ACP prompt blocks into the text and images prime-agent accepts.
 *
 * Image and embedded-resource blocks are advertised in `initialize`, so they must
 * actually reach the model: dropping them silently would let a client believe a
 * pasted screenshot was accepted.
 */
function promptContent(blocks: readonly unknown[]): { text: string; images: ImageContent[] } {
	const texts: string[] = [];
	const images: ImageContent[] = [];
	for (const block of blocks) {
		if (!block || typeof block !== "object") continue;
		const typed = block as {
			type?: string;
			text?: string;
			data?: string;
			mimeType?: string;
			uri?: string;
			resource?: { text?: string; uri?: string };
		};
		if (typed.type === "text" && typeof typed.text === "string") {
			texts.push(typed.text);
		} else if (typed.type === "image" && typeof typed.data === "string" && typeof typed.mimeType === "string") {
			images.push({ type: "image", data: typed.data, mimeType: typed.mimeType });
		} else if (typed.type === "resource" && typeof typed.resource?.text === "string") {
			// Embedded text resources become context the model can read.
			const uri = typed.resource.uri ? `${typed.resource.uri}\n` : "";
			texts.push(`${uri}${typed.resource.text}`);
		} else if (typed.type === "resource_link" && typeof typed.uri === "string") {
			texts.push(typed.uri);
		}
	}
	return { text: texts.join("\n"), images };
}

/** ACP config option id for the model selector. */
const MODEL_CONFIG_ID = "model";

/**
 * The canonical `provider/id` reference for a model.
 *
 * The same model id is served by more than one provider, so an id alone does
 * not identify a model. This is the key `findExactModelReferenceMatch` and the
 * TUI picker already use, so a value a client sends back resolves to the model
 * that was advertised.
 */
function modelValueId(model: AgentConnectionModel): string {
	return `${model.provider}/${model.id}`;
}

/**
 * The session's model as an ACP config option, so a client can render a model
 * picker instead of being stuck with whatever model the agent started on.
 *
 * Only models with configured credentials are offered. The TUI lists the rest
 * and starts a sign-in when one is picked; ACP has no sign-in flow, so an
 * unauthenticated model would be a choice that only fails at the next prompt.
 *
 * Ordered the way the TUI picker orders equally-ranked models: by provider,
 * flagship models first, then by id.
 */
async function acpConfigOptions(connection: AgentConnection): Promise<acp.SessionConfigOption[]> {
	const [state, models] = await Promise.all([connection.getState(), connection.getAvailableModels()]);
	if (!state.model) return [];
	const currentValue = modelValueId(state.model);
	// A select whose current value is not selectable is worse than no selector:
	// the session's model lost its credentials and cannot be offered back.
	if (!models.some((model) => modelValueId(model) === currentValue)) return [];
	return [
		{
			id: MODEL_CONFIG_ID,
			name: "Model",
			category: "model",
			type: "select",
			currentValue,
			options: [...models]
				.sort(
					(left, right) =>
						left.provider.localeCompare(right.provider) ||
						Number(right.featured === true) - Number(left.featured === true) ||
						left.id.localeCompare(right.id, undefined, { numeric: true }),
				)
				.map((model) => ({ value: modelValueId(model), name: model.id, description: model.provider })),
		},
	];
}

function autonomousMeta(status: AgentAutonomousStatus | undefined): PrimeAgentAutonomousMeta | undefined {
	if (!status?.enabled) return undefined;
	return {
		enabled: status.enabled,
		continuationsUsed: status.continuationsUsed,
		turnsUsed: status.turnsUsed,
		tokensUsed: status.tokensUsed,
		gateAttempt: latestAutonomousGateAttempt(status) || undefined,
		gateFailure: status.lastGateFailure?.exitText,
	};
}

function outstandingSubagentCount(children: readonly AgentConnectionRlmChildAgentSnapshot[] | undefined): number {
	return (children ?? []).filter((child) => child.status === "queued" || child.status === "running").length;
}

function quiescenceMeta(
	status: AgentAutonomousStatus,
	children: readonly AgentConnectionRlmChildAgentSnapshot[] | undefined,
): { outstandingSubagents: number; remainingAutonomousContinuations: number } {
	return {
		outstandingSubagents: outstandingSubagentCount(children),
		remainingAutonomousContinuations: status.enabled
			? Math.max(0, status.limits.maxContinuations - status.continuationsUsed)
			: 0,
	};
}

/**
 * The transcript as it stood before a turn started, recorded so the turn's own
 * messages can be told apart from everything older.
 *
 * A pre-turn message *count* cannot do that job: auto-compaction can fire during
 * a turn and rebuild `state.messages` (it filters, slices, and re-materializes
 * persisted entries), so this turn's failure can end up at a lower index than
 * the count taken before prompting. Membership is tracked by things a rebuild
 * preserves instead — the message objects themselves, plus a content key for
 * transports that hand back fresh copies (daemon RPC re-parses JSON, so identity
 * does not survive it) and for compaction paths that re-materialize a kept
 * message from its persisted entry with its original timestamp.
 */
interface TurnBoundary {
	identities: WeakSet<object>;
	keys: Set<string>;
}

/** Key for a kept message: compaction drops messages, it does not rewrite them. */
function messageKey(message: unknown): string | undefined {
	if (typeof message !== "object" || message === null) return undefined;
	const record = message as { role?: unknown; timestamp?: unknown; stopReason?: unknown; errorMessage?: unknown };
	if (typeof record.timestamp !== "number") return undefined;
	return JSON.stringify([
		record.role ?? null,
		record.timestamp,
		record.stopReason ?? null,
		record.errorMessage ?? null,
	]);
}

function turnBoundary(messages: readonly AgentMessage[]): TurnBoundary {
	const identities = new WeakSet<object>();
	const keys = new Set<string>();
	for (const message of messages) {
		if (typeof message !== "object" || message === null) continue;
		identities.add(message);
		const key = messageKey(message);
		if (key) keys.add(key);
	}
	return { identities, keys };
}

function isPreTurn(message: unknown, boundary: TurnBoundary): boolean {
	if (typeof message !== "object" || message === null) return false;
	if (boundary.identities.has(message)) return true;
	const key = messageKey(message);
	return key !== undefined && boundary.keys.has(key);
}

/**
 * Error text from an assistant message this turn produced, when it failed.
 *
 * `promptAndWait` resolves for a failed turn just as it does for a successful
 * one, so the outcome has to be read off the transcript. Only messages that were
 * not in the transcript before the turn are considered: scanning the whole
 * transcript would let an earlier failed turn reject a later turn that never
 * called the model (a handled slash command, say), reporting a stale error.
 *
 * A transcript read that fails is not treated as success — that would restore
 * the silent-success behavior this exists to prevent.
 */
async function turnFailure(connection: AgentConnection, boundary: TurnBoundary): Promise<string | undefined> {
	const messages = await connection.getMessages();
	for (let index = messages.length - 1; index >= 0; index--) {
		const message = messages[index];
		if (message?.role !== "assistant") continue;
		// The newest assistant message predates the turn, so the turn appended none.
		if (isPreTurn(message, boundary)) return undefined;
		const assistant = message as { stopReason?: string; errorMessage?: string };
		if (assistant.stopReason !== "error") return undefined;
		return assistant.errorMessage || "the model request failed";
	}
	return undefined;
}

/**
 * A fire-and-forget `rlm()` child keeps the session's unfinished-action count
 * above zero for as long as it runs, and `waitForHeadlessCompletion` only
 * resolves at zero. Bound the wait so a long-lived child cannot hold the
 * `session/prompt` response open; the full reconciliation still runs to
 * completion out of band in `finalizePendingTerminal`.
 */
const PROMPT_HEADLESS_COMPLETION_GRACE_MS = 3_000;

/** Longest a `session_info_update`/`usage_update` may sit behind a streaming message. */
const DEFERRED_TELEMETRY_FLUSH_MS = 500;

/**
 * Race a promise against a timeout, discarding the promise's result (not the
 * promise itself) when the timeout wins.
 *
 * `AgentConnection.waitForHeadlessCompletion` takes no signal and cannot be
 * cancelled, so a timeout here does not stop the underlying wait — it only
 * stops waiting on it. The original promise keeps running: for
 * `waitForHeadlessCompletion` specifically, that means one abandoned
 * `session.waitForHeadlessIdle()` poll loop per call that hits the timeout,
 * which self-resolves the moment the session actually reaches idle (the same
 * idleness a long-lived child can delay indefinitely, which is why this
 * exists) and is otherwise inert: its result is read by nobody, and the
 * `.catch()` below keeps it from surfacing as an unhandled rejection. This is
 * a real orphaned promise per superseded wait, not a no-op — accepted because
 * the connection interface has no cancellable variant to call instead.
 */
async function withGracePeriod<T>(promise: Promise<T>, ms: number): Promise<T | undefined> {
	let timer: ReturnType<typeof setTimeout>;
	const timeout = new Promise<undefined>((resolve) => {
		timer = setTimeout(() => resolve(undefined), ms);
		timer.unref?.();
	});
	try {
		return await Promise.race([promise, timeout]);
	} finally {
		clearTimeout(timer!);
		promise.catch(() => undefined);
	}
}

/**
 * The order a submitted command is resolved in, mirroring `_normalizeSubmission`:
 * a session builtin first, then an extension command, then a skill, then a
 * prompt template. Extension names are already disambiguated upstream, so only
 * collisions across these sources have to be resolved here.
 */
const CONNECTION_COMMAND_PRECEDENCE: Record<AgentConnectionSlashCommand["source"], number> = {
	extension: 0,
	skill: 1,
	prompt: 2,
};

/**
 * Commands an ACP client can offer for completion.
 *
 * Only commands a prompt actually executes are advertised. The rest of
 * `BUILTIN_SLASH_COMMANDS` opens a TUI selector (`/model`, `/settings`) and has
 * no headless behavior, so listing it would complete to plain prompt text.
 *
 * One entry per name, resolved the way a submission is: a client cannot know
 * which route wins, so advertising both sides of a collision is worse than
 * advertising the loser not at all.
 */
async function acpAvailableCommands(connection: AgentConnection): Promise<acp.AvailableCommand[]> {
	const sessionCommands = BUILTIN_SLASH_COMMANDS.filter((command) => command.execution === "session").map(
		(command) => ({
			name: command.name,
			description: command.description,
			...(command.argumentHint ? { input: { hint: command.argumentHint } } : {}),
		}),
	);
	// Skills, prompt templates, and extension commands. A failure here costs
	// completion, not the session, so it must not reject session/new.
	const connectionCommands = await Promise.resolve(connection.getCommands?.() ?? []).catch(() => []);
	const advertised = [
		...sessionCommands,
		...[...connectionCommands]
			.sort(
				(left, right) => CONNECTION_COMMAND_PRECEDENCE[left.source] - CONNECTION_COMMAND_PRECEDENCE[right.source],
			)
			.map((command) => ({
				name: command.name,
				description: command.description ?? "",
				...(command.argumentHint ? { input: { hint: command.argumentHint } } : {}),
			})),
	];
	// A name that resolves to one route must be advertised once, or a client
	// completes to an entry whose description belongs to a route that will not run.
	const byName = new Map<string, acp.AvailableCommand>();
	for (const command of advertised) {
		if (!byName.has(command.name)) byName.set(command.name, command);
	}
	return [...byName.values()];
}

/**
 * Context-window usage for the session, as ACP's `usage_update`.
 *
 * Usage is unknown until a model response has been costed — notably right after
 * compaction — and a client cannot render a fraction without both numbers, so an
 * unknown reading is skipped rather than reported as zero.
 */
async function acpUsageUpdate(connection: AgentConnection): Promise<AcpSessionUpdate | undefined> {
	const usage = await connection
		.getState()
		.then((state) => state.contextUsage)
		.catch(() => undefined);
	if (!usage || usage.tokens === null || usage.contextWindow <= 0) return undefined;
	return { sessionUpdate: "usage_update", used: usage.tokens, size: usage.contextWindow };
}

/**
 * Events after which the context can hold a different number of tokens.
 *
 * Only a completed assistant message carries the usage the reading is computed
 * from, and compaction replaces the transcript, so polling on anything else
 * would cost a round trip per streamed delta to report an unchanged number.
 */
function changesContextUsage(event: AgentConnectionSessionEvent): boolean {
	if (event.type === "compaction_end") return true;
	return event.type === "message_end" && event.message.role === "assistant";
}

export async function runAcpMode(runtimeHost: AgentSessionRuntime): Promise<never> {
	const connection = new InProcessAgentConnection(runtimeHost);
	return runAcpModeWithConnection(connection, {
		bindHeadlessExtensions: () => connection.bindHeadlessExtensions({}),
	});
}

export async function runAcpModeWithConnection(
	connection: AgentConnection,
	options: AcpModeOptions = {},
): Promise<never> {
	// ACP owns stdout: any stray write corrupts the JSON-RPC stream.
	if (options.ownStdout !== false && !options.stream) {
		takeOverStdout();
	}
	const supportsMcpServers =
		connection.supportsAcpMcpServers?.() === true &&
		connection.replaceAcpMcpServers !== undefined &&
		connection.releaseAcpMcpServers !== undefined;
	const acpMcpOwnerId = randomUUID();
	let acpMcpServerNames: string[] = [];
	const clearAcpMcpServers = async (serverNames = acpMcpServerNames): Promise<void> => {
		if (!supportsMcpServers || !connection.releaseAcpMcpServers) return;
		await connection.releaseAcpMcpServers(acpMcpOwnerId, serverNames);
		acpMcpServerNames = [];
	};
	const replaceAcpMcpServers = async (servers: readonly acp.McpServer[], cwd: string): Promise<void> => {
		if (acpMcpServerNames.length > 0) {
			// Retry a prior best-effort close before admitting another session,
			// including one that does not declare replacement MCP servers.
			await clearAcpMcpServers();
		}
		if (servers.length === 0 && acpMcpServerNames.length === 0) return;
		if (!supportsMcpServers || !connection.replaceAcpMcpServers) {
			throw acp.RequestError.invalidParams({ reason: "MCP servers are unavailable in this ACP host" });
		}
		const resolved = resolveAcpMcpServers(servers, cwd);
		const serverNames = resolved.map((server) => server.name);
		try {
			await connection.replaceAcpMcpServers(resolved, acpMcpOwnerId);
		} catch (error) {
			// The daemon may have applied the configuration before its acknowledgement
			// was lost. Always attempt owner-scoped cleanup before rejecting admission.
			await clearAcpMcpServers(serverNames).catch(() => undefined);
			throw error;
		}
		acpMcpServerNames = serverNames;
	};

	// One ACP connection drives one AgentConnection, whose newSession() replaces
	// the live session rather than creating a parallel one. Tracking a single
	// session keeps every event unambiguously attributable; a second session/new
	// is refused rather than silently sharing conversation state, cwd, and queues.
	let session: AcpSessionEntry | undefined;
	let closedInputPause: AgentConnectionSessionInputPause | undefined;
	let closedInputPauseKey: string | undefined;
	let sessionNewInFlight = false;
	let sessionCloseInFlight = false;
	let sessionCloseTask: Promise<void> | undefined;
	let bound = false;

	const baseStream =
		options.stream ?? acp.ndJsonStream(rawStdoutSink(), Readable.toWeb(process.stdin) as ReadableStream<Uint8Array>);
	// ACP's public request handler only returns a response; it has no response
	// commit callback. Observe the outgoing response at the supplied stream
	// boundary instead. The SDK serializes every write, so opening the producer
	// after this write resolves puts buffered notifications strictly behind it.
	let pendingSessionNewResponse:
		| {
				requestId: unknown;
				producer: AcpUpdateProducer;
				entry: AcpSessionEntry;
				inputPause: AgentConnectionSessionInputPause | undefined;
		  }
		| undefined;
	const failPendingSessionNewResponse = (): void => {
		const admission = pendingSessionNewResponse;
		pendingSessionNewResponse = undefined;
		admission?.producer.failSessionNewAdmission();
		admission?.entry.inputPauseRelease?.reject(new Error("ACP session/new response was not delivered"));
	};
	type AcpStreamMessage = typeof baseStream.writable extends WritableStream<infer TMessage> ? TMessage : never;
	const stream: typeof baseStream = {
		readable: baseStream.readable,
		writable: new WritableStream<AcpStreamMessage>({
			async write(message) {
				let writer: WritableStreamDefaultWriter<AcpStreamMessage> | undefined;
				try {
					writer = baseStream.writable.getWriter();
					await writer.write(message);
				} catch (error) {
					failPendingSessionNewResponse();
					throw error;
				} finally {
					writer?.releaseLock();
				}
				if (pendingSessionNewResponse && isJsonRpcResponse(message, pendingSessionNewResponse.requestId)) {
					const admission = pendingSessionNewResponse;
					pendingSessionNewResponse = undefined;
					if (admission.inputPause) {
						try {
							await admission.inputPause.release();
							if (admission.entry.inputPause === admission.inputPause) {
								admission.entry.inputPause = undefined;
								admission.entry.inputPauseKey = undefined;
							}
							if (closedInputPause === admission.inputPause) {
								closedInputPause = undefined;
								closedInputPauseKey = undefined;
							}
							admission.entry.inputPauseRelease?.resolve();
							admission.entry.inputPauseRelease = undefined;
						} catch (error) {
							admission.entry.stopFailure = error instanceof Error ? error.message : String(error);
							admission.entry.inputPauseRelease?.reject(error);
						}
					}
					admission.producer.commitSessionNewResponse();
				}
			},
			async close() {
				let writer: WritableStreamDefaultWriter<AcpStreamMessage> | undefined;
				try {
					writer = baseStream.writable.getWriter();
					await writer.close();
				} catch (error) {
					failPendingSessionNewResponse();
					throw error;
				} finally {
					writer?.releaseLock();
				}
				failPendingSessionNewResponse();
			},
			async abort(reason) {
				let writer: WritableStreamDefaultWriter<AcpStreamMessage> | undefined;
				try {
					writer = baseStream.writable.getWriter();
					await writer.abort(reason);
				} catch (error) {
					failPendingSessionNewResponse();
					throw error;
				} finally {
					writer?.releaseLock();
				}
				failPendingSessionNewResponse();
			},
		}),
	};

	const cancelOutstandingRlmChildren = async (): Promise<void> => {
		const children = await connection.getRlmChildSnapshots();
		const cancellations = await Promise.allSettled(children.map((child) => connection.cancelRlmChild(child.id)));
		const failed = cancellations.find((result) => result.status === "rejected");
		if (failed?.status === "rejected") throw failed.reason;
	};
	const abortConnectionWork = async (): Promise<void> => {
		await connection.abortAndClearQueue();
	};
	const acquireStopInputPause = async (entry: AcpSessionEntry): Promise<AgentConnectionSessionInputPause> => {
		if (entry.inputPauseRelease) {
			await entry.inputPauseRelease.promise.catch(() => undefined);
			entry.inputPauseRelease = undefined;
		}
		const leaseKey = entry.inputPauseKey ?? randomUUID();
		entry.inputPauseKey = leaseKey;
		const pause = await connection.acquireSessionInputPause(leaseKey);
		entry.inputPause = pause;
		return pause;
	};

	const stopSessionWork = async (pending?: AcpPendingTerminal, promptTask?: Promise<void>): Promise<void> => {
		await abortConnectionWork();
		await connection.waitForIdle();
		await cancelOutstandingRlmChildren();
		await pending?.task;
		await promptTask;
	};

	const finalizePendingTerminal = (entry: AcpSessionEntry, pending: AcpPendingTerminal): void => {
		pending.task = (async () => {
			while (true) {
				const status = await connection.waitForHeadlessCompletion({ waitForRlmQuiescence: true });
				if (pending.abort.signal.aborted || session !== entry || entry.pendingTerminal !== pending) return;
				const finalFailure = await turnFailure(connection, pending.boundary);
				if (pending.abort.signal.aborted || session !== entry || entry.pendingTerminal !== pending) return;
				const liveChildren = await connection.getRlmChildSnapshots();
				if (pending.abort.signal.aborted || session !== entry || entry.pendingTerminal !== pending) return;
				const terminalQuiescence = quiescenceMeta(status, liveChildren);
				if (terminalQuiescence.outstandingSubagents !== 0) continue;
				pending.status = status;
				pending.turnFailure = finalFailure;

				entry.producer.sealTerminal(pending.promptTurnId);
				const autonomous = autonomousMeta(status);
				const publication = entry.producer.publish(
					{
						sessionUpdate: "session_info_update",
						_meta: primeAgentMeta({ ...(autonomous ? { autonomous } : {}), quiescence: terminalQuiescence }),
					},
					pending.promptTurnId,
					"terminalQuiescence",
					finalFailure ? "error" : pending.outcome,
				);
				// Keep terminal ownership until this settlement task has fully drained.
				// A follow-up prompt awaits that task; clearing ownership at publication
				// admission would let it overlap the first prompt handler.
				if (!(await publication)) return;
				await entry.producer.drain();
				return;
			}
		})()
			.catch((error: unknown) => {
				if (pending.abort.signal.aborted || entry.pendingTerminal !== pending) return;
				pending.failure = error instanceof Error ? error.message : String(error);
			})
			.finally(() => {
				entry.producer.finishTerminalLifecycle(pending.promptTurnId);
				if (entry.pendingTerminal === pending) entry.pendingTerminal = undefined;
				if (entry.abort === pending.abort) entry.abort = undefined;
			});
	};

	const handle = acp
		.agent({ name: "prime-agent" })
		.onRequest("initialize", async () => ({
			protocolVersion: acp.PROTOCOL_VERSION,
			agentCapabilities: {
				loadSession: false,
				promptCapabilities: { image: true, embeddedContext: true },
				...(supportsMcpServers ? { mcpCapabilities: { http: true } } : {}),
				// Advertise close so a client knows it can release the session (and
				// the single-session slot) instead of dropping the connection.
				sessionCapabilities: { close: {} },
			},
			agentInfo: { name: "prime-agent", title: "Prime Agent", version: VERSION },
			// Advertise prime-agent extras under a namespaced key: ACP reserves
			// every object root for future protocol fields.
			_meta: primeAgentMeta({}),
		}))
		.onRequest("session/new", async (ctx: any) => {
			// Reserve the single-session slot before the first await. Otherwise two
			// concurrent requests can both pass the empty-slot check while cwd or
			// snapshot reads are in flight, then overwrite each other's session.
			if (session || sessionNewInFlight || sessionCloseInFlight) {
				throw new Error(
					"prime-agent ACP mode hosts one session per connection; " +
						"start another prime-agent process for a second session",
				);
			}
			sessionNewInFlight = true;
			try {
				const params = ctx.params as acp.NewSessionRequest;
				const mcpServers = params.mcpServers ?? [];
				if (mcpServers.length > 0 && !supportsMcpServers) {
					throw acp.RequestError.invalidParams({ reason: "MCP servers are unavailable in this ACP host" });
				}
				if (!bound) {
					// Only latch after a successful bind: a rejected bind must not leave
					// extensions permanently unavailable for the rest of the process.
					await options.bindHeadlessExtensions?.();
					bound = true;
				}
				// prime-agent's cwd is fixed at startup by the session it was launched
				// with, so a client-supplied cwd cannot be adopted after the fact.
				// Report the real cwd back in `_meta` rather than failing the request or
				// letting the client assume a directory the agent is not using.
				const requestedCwd = params.cwd;
				const actualCwd = await connection
					.getState()
					.then((state) => state.cwd)
					.catch(() => undefined);
				if (!actualCwd && mcpServers.some((server) => "command" in server)) {
					throw acp.RequestError.invalidParams({ reason: "Could not resolve the ACP session cwd for stdio MCP" });
				}
				await replaceAcpMcpServers(mcpServers, actualCwd ?? "");
				let cwdMismatch: { requested: string; actual: string } | undefined;
				if (
					typeof requestedCwd === "string" &&
					requestedCwd.length > 0 &&
					actualCwd &&
					!sameCwd(requestedCwd, actualCwd)
				) {
					cwdMismatch = { requested: requestedCwd, actual: actualCwd };
				}
				// A failed read costs the client its model picker, not the session.
				const configOptions = await acpConfigOptions(connection).catch(() => []);
				const sessionId = randomUUID();
				// Install the listener before fetching the snapshot. Child updates can arrive
				// while the snapshot request is in flight; the connection remains the
				// authoritative source used when quiescence is emitted below.
				const producer = new AcpUpdateProducer(sessionId, ctx.client);
				let inputPauseRelease: AcpInputPauseRelease | undefined;
				if (closedInputPause) {
					let resolve!: () => void;
					let reject!: (error: unknown) => void;
					const promise = new Promise<void>((resolvePromise, rejectPromise) => {
						resolve = resolvePromise;
						reject = rejectPromise;
					});
					void promise.catch(() => undefined);
					inputPauseRelease = { promise, resolve, reject };
				}
				const entry: AcpSessionEntry = {
					id: sessionId,
					abort: undefined,
					commandsTimer: undefined,
					cancelling: false,
					cancelTask: undefined,
					stopFailure: undefined,
					inputPause: closedInputPause,
					inputPauseKey: closedInputPauseKey,
					inputPauseRelease,
					pendingTerminal: undefined,
					promptTask: undefined,
					resolvePromptTask: undefined,
					unsubscribe: undefined,
					producer,
				};
				// Subscribe for the session lifetime, not per prompt turn: prime-agent
				// subagents are fire-and-forget and keep reporting after the spawning turn
				// ends, so a turn-scoped subscription would drop their updates. One
				// mapping state per session keeps streaming bash output correlated with
				// the run that produced it.
				const mappingState: AcpEventMappingState = {};
				const observedChildren = new Map<string, unknown>();
				// Telemetry held back for the length of a message, in arrival order.
				// A client that groups chunks by what sits next to them tears the
				// message on anything else, so the only thing that keeps an answer
				// whole is that nothing else goes out while it streams
				// (`acp-events.ts :: acpDefersWhileStreaming`). The text itself is
				// never held: it streams token by token as it always has, and a
				// reading of the context arrives a message late rather than in the
				// middle of one.
				const deferred: Array<{ update: AcpSessionUpdate; turnId: number }> = [];
				// A long assistant message can hold telemetry for its whole duration,
				// which is fine for a chunky answer but starves a client's context-usage
				// and subagent tiles for the length of a slow autonomous run. Cap how
				// long anything sits in `deferred` so it flushes even without a
				// `message_end`/`agent_end` to release it.
				let deferredFlushTimer: ReturnType<typeof setTimeout> | undefined;
				const clearDeferredFlushTimer = () => {
					if (deferredFlushTimer === undefined) return;
					clearTimeout(deferredFlushTimer);
					deferredFlushTimer = undefined;
				};
				const flushDeferred = () => {
					if (mappingState.streaming) return;
					clearDeferredFlushTimer();
					for (const held of deferred.splice(0)) void producer.publish(held.update, held.turnId, "event");
				};
				const publishUpdate = (update: AcpSessionUpdate, turnId: number) => {
					if (mappingState.streaming && acpDefersWhileStreaming(update)) {
						deferred.push({ update, turnId });
						if (deferredFlushTimer === undefined) {
							deferredFlushTimer = setTimeout(() => {
								deferredFlushTimer = undefined;
								for (const held of deferred.splice(0)) void producer.publish(held.update, held.turnId, "event");
							}, DEFERRED_TELEMETRY_FLUSH_MS);
							deferredFlushTimer.unref?.();
						}
						return;
					}
					void producer.publish(update, turnId, "event");
				};
				const rawUnsubscribe = connection.subscribe((event) => {
					// Heartbeats are connection-scoped, including if one races a prompt.
					// They therefore intentionally use origin turn 0.
					if (event.type === "heartbeats_changed") {
						publishUpdate(
							{ sessionUpdate: "session_info_update", _meta: primeAgentMeta({ heartbeatsChanged: true }) },
							0,
						);
						return;
					}
					if (event.type !== "session_event") return;
					if (event.event.type === "rlm_child_update") {
						observedChildren.set(event.event.child.id, event.event.child);
					}
					const turnId = producer.turnForEvent(event.event);
					if (changesContextUsage(event.event)) {
						void acpUsageUpdate(connection).then((update) => {
							if (update) {
								publishUpdate(update, turnId);
								flushDeferred();
							}
						});
					}
					for (const update of acpUpdatesForSessionEvent(event.event, mappingState)) {
						publishUpdate(update, turnId);
					}
					// The mapper clears `streaming` on the end of a message and on a
					// run ending, so what was held is released the moment there is no
					// message left to tear.
					flushDeferred();
				});
				const unsubscribe = () => {
					rawUnsubscribe();
					clearDeferredFlushTimer();
				};
				try {
					// Reconcile after subscribing so updates cannot be lost while the snapshot
					// request is in flight. Do not turn a failed read into an empty roster.
					const initialSnapshot = await connection.getInitialSnapshot();
					for (const child of initialSnapshot.children ?? []) {
						if (observedChildren.has(child.id)) continue;
						observedChildren.set(child.id, child);
						const event = { type: "rlm_child_update", child } as const;
						const turnId = producer.turnForEvent(event);
						for (const update of acpUpdatesForSessionEvent(event, mappingState)) {
							void producer.publish(update, turnId, "event");
						}
					}
				} catch (error) {
					producer.failSessionNewAdmission();
					unsubscribe();
					await clearAcpMcpServers().catch(() => undefined);
					throw error;
				}
				// Claim the single-session slot only once the subscription and snapshot are
				// ready, so a failed setup cannot leave it occupied and unusable.
				entry.unsubscribe = unsubscribe;
				session = entry;
				const response = {
					sessionId,
					...(configOptions.length > 0 ? { configOptions } : {}),
					...(cwdMismatch ? { _meta: primeAgentMeta({ cwd: cwdMismatch }) } : {}),
				};
				// The stream wrapper commits this gate after this exact response has
				// written. Buffered subscription updates retain producer order.
				pendingSessionNewResponse = {
					requestId: ctx.requestId,
					producer: entry.producer,
					entry,
					inputPause: closedInputPause,
				};
				// Sent after this handler returns: a client cannot route a session
				// update for a session id it has not been told about yet.
				entry.commandsTimer = setTimeout(() => {
					void acpAvailableCommands(connection).then((availableCommands) =>
						ctx.client
							.notify(acp.methods.client.session.update, {
								sessionId,
								update: { sessionUpdate: "available_commands_update", availableCommands },
							})
							.catch(() => undefined),
					);
				}, 0);
				return response;
			} finally {
				sessionNewInFlight = false;
			}
		})
		.onRequest("session/prompt", async (ctx: any) => {
			const params = ctx.params as { sessionId: string; prompt: readonly unknown[] };
			const entry = session?.id === params.sessionId ? session : undefined;
			if (!entry) throw new Error(`Unknown ACP session: ${params.sessionId}`);
			if (sessionCloseInFlight) throw new Error(`ACP session is closing: ${params.sessionId}`);
			if (entry.cancelling) throw new Error(`ACP session is cancelling: ${params.sessionId}`);
			await entry.inputPauseRelease?.promise;
			// A pending terminal's owner may still be in flight (its own `await
			// pending.task` is grace-bounded, same as this one, so it returns on
			// schedule even when a fire-and-forget child never settles) or it may
			// already have returned, leaving `pending.task` running orphaned in the
			// background. Only the orphaned case should be detached: an in-flight
			// owner's turn must still queue behind it, exactly as `await
			// entry.pendingTerminal?.task` did, minus the unbounded wait.
			if (entry.pendingTerminal) {
				const superseded = entry.pendingTerminal;
				if (entry.promptTask) {
					await withGracePeriod(entry.promptTask, PROMPT_HEADLESS_COMPLETION_GRACE_MS);
				}
				if (entry.pendingTerminal === superseded && !entry.promptTask) {
					superseded.abort.abort();
					if (entry.pendingTerminal === superseded) entry.pendingTerminal = undefined;
					if (entry.abort === superseded.abort) entry.abort = undefined;
				}
			}
			if (session !== entry) throw new Error(`Unknown ACP session: ${params.sessionId}`);
			if (sessionCloseInFlight) throw new Error(`ACP session is closing: ${params.sessionId}`);
			// This prompt was admitted before the cancellation started; it is dropped
			// by the cancel rather than malformed, so report the protocol stop reason
			// instead of a request error.
			if (entry.cancelling) return { stopReason: "cancelled" satisfies AcpStopReason };
			if (entry.stopFailure) throw new Error(`ACP session stop failed: ${entry.stopFailure}`);
			if (entry.pendingTerminal?.failure) {
				throw new Error(`ACP lifecycle reconciliation failed: ${entry.pendingTerminal.failure}`);
			}
			if (entry.abort) throw new Error("A prompt turn is already running for this ACP session");

			const abort = new AbortController();
			entry.abort = abort;
			let resolvePromptTask!: () => void;
			const promptTask = new Promise<void>((resolve) => {
				resolvePromptTask = resolve;
			});
			entry.promptTask = promptTask;
			entry.resolvePromptTask = resolvePromptTask;
			// Allocate the causal turn before the first await, not when an update is
			// delivered. This prevents late producer events becoming the next turn.
			const promptTurnId = entry.producer.beginPrompt();
			let responseBoundaryEmitted = false;
			let terminalSettlementCancelled = false;
			try {
				const { text, images } = promptContent(params.prompt);
				const priorMessages = turnBoundary(await connection.getMessages());
				if (abort.signal.aborted) {
					await entry.producer.drain();
					return { stopReason: "cancelled" satisfies AcpStopReason };
				}
				// A follow-up prompt can arrive while injected work (subagent replies,
				// heartbeats) keeps the resident session busy. ACP has no native queue
				// field, so queue the host turn behind that work with follow-up
				// semantics instead of rejecting it as "Agent is already processing".
				await connection.promptAndWait(text, {
					...(images.length > 0 ? { images } : {}),
					streamingBehavior: "followUp",
					queueIfBusy: true,
					signal: abort.signal,
				});
				if (abort.signal.aborted) {
					await entry.producer.drain();
					return { stopReason: "cancelled" satisfies AcpStopReason };
				}
				// See `withGracePeriod` for what an expired grace period leaves running.
				const status = await withGracePeriod(
					connection.waitForHeadlessCompletion(),
					PROMPT_HEADLESS_COMPLETION_GRACE_MS,
				);
				if (abort.signal.aborted) {
					await entry.producer.drain();
					return { stopReason: "cancelled" satisfies AcpStopReason };
				}
				const failure = await turnFailure(connection, priorMessages);
				if (abort.signal.aborted) {
					await entry.producer.drain();
					return { stopReason: "cancelled" satisfies AcpStopReason };
				}
				const autonomous = autonomousMeta(status);
				const liveChildren = await connection.getRlmChildSnapshots();
				if (abort.signal.aborted) {
					await entry.producer.drain();
					return { stopReason: "cancelled" satisfies AcpStopReason };
				}
				const outcome = failure ? "error" : "result";
				let terminalStatus = status;
				// `status` is unknown when the grace period above expired: leave the
				// quiescence field out rather than invent a status. The already-sent
				// `terminalQuiescenceExpected: true` on the response boundary below is
				// the client's existing signal that the authoritative figures are
				// still coming from `finalizePendingTerminal`.
				const observedQuiescence = status ? quiescenceMeta(status, liveChildren) : undefined;
				// The roster is telemetry at the response cut, not proof of terminality:
				// a child can publish a terminal status before its result reaches the parent.
				// Every turn therefore finalizes through the strong settlement barrier.
				entry.producer.commitResponse(promptTurnId);
				responseBoundaryEmitted = await entry.producer.publish(
					{
						sessionUpdate: "session_info_update",
						_meta: primeAgentMeta({ terminalQuiescenceExpected: true }),
					},
					promptTurnId,
					"responseBoundary",
					outcome,
				);
				if (!responseBoundaryEmitted) throw new Error("Failed to publish ACP response boundary");
				const completionUpdateEmitted = await entry.producer.publish(
					{
						sessionUpdate: "session_info_update",
						_meta: primeAgentMeta({
							...(autonomous ? { autonomous } : {}),
							...(observedQuiescence ? { quiescence: observedQuiescence } : {}),
						}),
					},
					promptTurnId,
					"event",
				);
				if (!completionUpdateEmitted) throw new Error("Failed to publish ACP completion update");
				await entry.producer.drain();
				if (!abort.signal.aborted) {
					entry.producer.beginTerminalLifecycle(promptTurnId);
					const pending: AcpPendingTerminal = { promptTurnId, boundary: priorMessages, outcome, abort };
					entry.pendingTerminal = pending;
					finalizePendingTerminal(entry, pending);
					// See `withGracePeriod`: a fire-and-forget child can hold this exact
					// barrier open too, since it also resolves through
					// `waitForHeadlessCompletion({ waitForRlmQuiescence: true })`. Bounding
					// it here keeps a terminal quiescence reading on the fast path (the
					// case this await exists for) while still returning the turn on
					// schedule when nothing settles in time; `pending` stays owned by
					// `entry.pendingTerminal` and keeps running in the background, where a
					// later prompt's detach (above) is the only thing that stops it.
					await withGracePeriod(pending.task ?? Promise.resolve(), PROMPT_HEADLESS_COMPLETION_GRACE_MS);
					terminalSettlementCancelled = abort.signal.aborted;
					if (pending.failure) {
						throw new Error(`ACP lifecycle reconciliation failed: ${pending.failure}`);
					}
					if (pending.turnFailure) {
						throw new Error(`prime-agent turn failed: ${pending.turnFailure}`);
					}
					terminalStatus = pending.status ?? status;
				}
				if (failure) throw new Error(`prime-agent turn failed: ${failure}`);
				return {
					stopReason: acpStopReason({
						cancelled: terminalSettlementCancelled,
						autonomous: terminalStatus,
					}),
				};
			} catch (error) {
				if (abort.signal.aborted && !entry.producer.isResponseCommitted(promptTurnId)) {
					await entry.producer.drain();
					return { stopReason: "cancelled" satisfies AcpStopReason };
				}
				// Failed prompt/snapshot admission gets one correlated error boundary;
				// it never gets an invented terminal-quiescence update.
				if (!responseBoundaryEmitted) {
					await entry.producer.publish(
						{
							sessionUpdate: "session_info_update",
							_meta: primeAgentMeta({ terminalQuiescenceExpected: false }),
						},
						promptTurnId,
						"responseBoundary",
						"error",
					);
				}
				await entry.producer.drain();
				throw error;
			} finally {
				entry.producer.finishPrompt(promptTurnId);
				if (entry.promptTask === promptTask) {
					entry.promptTask = undefined;
					entry.resolvePromptTask = undefined;
					resolvePromptTask();
				}
				if (entry.abort === abort && entry.pendingTerminal?.abort !== abort) entry.abort = undefined;
			}
		})
		.onRequest("session/set_config_option", async (ctx: any) => {
			const params = ctx.params as { sessionId: string; configId: string; value?: unknown };
			if (session?.id !== params.sessionId) {
				throw new Error(`Unknown ACP session: ${params.sessionId}`);
			}
			if (params.configId !== MODEL_CONFIG_ID) {
				throw new Error(`Unknown ACP config option: ${params.configId}`);
			}
			// Resolve against the advertised values rather than parsing the id: a
			// model id can itself contain a slash, so splitting one off the provider
			// would pick the wrong model.
			const models = await connection.getAvailableModels();
			const model = models.find((candidate) => modelValueId(candidate) === params.value);
			if (!model) throw new Error(`Unknown model: ${String(params.value)}`);
			await connection.setModel(model.provider, model.id);
			// The response carries the complete configuration state, not the delta.
			return { configOptions: await acpConfigOptions(connection) };
		})
		.onRequest("session/close", async (ctx: any) => {
			const params = ctx.params as { sessionId: string };
			if (session?.id !== params.sessionId) {
				throw new Error(`Unknown ACP session: ${params.sessionId}`);
			}
			// Stop real work, not just local bookkeeping: aborting only the local
			// controller leaves the agent running with nobody listening, so closing
			// must abort the connection the same way session/cancel does.
			if (sessionCloseInFlight) throw new Error(`ACP session is already closing: ${params.sessionId}`);
			sessionCloseInFlight = true;
			let finishClose!: () => void;
			sessionCloseTask = new Promise<void>((resolve) => {
				finishClose = resolve;
			});
			const closing = session;
			try {
				if (closing.commandsTimer) clearTimeout(closing.commandsTimer);
				await closing.cancelTask?.catch(() => undefined);
				closing.cancelling = true;
				closing.abort?.abort();
				const pending = closing.pendingTerminal;
				const promptTask = closing.promptTask;
				try {
					const inputPause = await acquireStopInputPause(closing);
					const inputPauseKey = closing.inputPauseKey;
					if (!inputPauseKey) throw new Error("Missing ACP close input-pause key");
					await stopSessionWork(pending, promptTask);
					closing.unsubscribe?.();
					// Keep the backing session fenced until a replacement ACP session is admitted.
					await closing.producer.close();
					// Host credentials are already gone before kernel release runs. Do not
					// retain the ACP session slot if best-effort transport reaping fails.
					await clearAcpMcpServers().catch(() => undefined);
					closedInputPause = inputPause;
					closedInputPauseKey = inputPauseKey;
					if (closing.inputPause === inputPause) {
						closing.inputPause = undefined;
						closing.inputPauseKey = undefined;
					}
					closing.stopFailure = undefined;
				} catch (error) {
					closing.stopFailure = error instanceof Error ? error.message : String(error);
					throw error;
				}
				if (session === closing) session = undefined;
				return {};
			} finally {
				if (session === closing) closing.cancelling = false;
				finishClose();
				sessionCloseTask = undefined;
				sessionCloseInFlight = false;
			}
		})
		.onNotification("session/cancel", async (ctx: any) => {
			const params = ctx.params as { sessionId: string };
			while (sessionCloseInFlight) await sessionCloseTask;
			// Only cancel the addressed session: aborting unconditionally would kill
			// whichever turn happens to be running, and leave the real turn's
			// AbortController unmarked so it reports a wrong stop reason.
			if (session?.id !== params.sessionId) return;
			const cancelling = session;
			if (cancelling.cancelling) {
				await cancelling.cancelTask;
				return;
			}
			const abort = cancelling.abort;
			if (!abort && !cancelling.stopFailure && !cancelling.inputPauseRelease) return;
			const pending = abort && cancelling.pendingTerminal?.abort === abort ? cancelling.pendingTerminal : undefined;
			const promptTask = cancelling.promptTask;
			cancelling.cancelling = true;
			const cancelTask = (async () => {
				abort?.abort();
				try {
					const inputPause = await acquireStopInputPause(cancelling);
					await stopSessionWork(pending, promptTask);
					await inputPause.release();
					if (cancelling.inputPause === inputPause) {
						cancelling.inputPause = undefined;
						cancelling.inputPauseKey = undefined;
					}
					if (closedInputPause === inputPause) {
						closedInputPause = undefined;
						closedInputPauseKey = undefined;
					}
					cancelling.inputPauseRelease = undefined;
					cancelling.stopFailure = undefined;
					if (pending && cancelling.pendingTerminal === pending) cancelling.pendingTerminal = undefined;
					if (abort && cancelling.abort === abort) cancelling.abort = undefined;
				} catch (error) {
					cancelling.stopFailure = error instanceof Error ? error.message : String(error);
					throw error;
				}
			})();
			cancelling.cancelTask = cancelTask;
			try {
				await cancelTask;
			} finally {
				if (cancelling.cancelTask === cancelTask) cancelling.cancelTask = undefined;
				cancelling.cancelling = false;
			}
		})
		.connect(stream);

	// Exit when the client disconnects (stdin EOF or a closed transport). Blocking
	// forever would leave an orphaned agent per run, which matters most for a
	// harness that spawns many short-lived sessions.
	await handle.closed.catch(() => undefined);
	session?.abort?.abort();
	if (session?.commandsTimer) clearTimeout(session.commandsTimer);
	session?.unsubscribe?.();
	await session?.inputPause?.release().catch(() => undefined);
	session = undefined;
	await closedInputPause?.release().catch(() => undefined);
	closedInputPause = undefined;
	closedInputPauseKey = undefined;
	await clearAcpMcpServers().catch(() => undefined);
	await connection.dispose().catch(() => undefined);
	// Only the real stdio entrypoint owns the process; a caller-supplied transport
	// (tests, embedding) must never have its host exited from under it.
	if (options.stream) return undefined as never;
	return process.exit(0) as never;
}
