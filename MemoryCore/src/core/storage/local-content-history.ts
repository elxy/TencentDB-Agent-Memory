/**
 * local-content-history.ts — formal L2/L3 version history for the local backend.
 *
 * This is the local (filesystem) implementation of the parent-owned
 * `HistoryReader` + `commitChanges` contract (core/content/history-contract.ts).
 * It stores the formal version chain *inside the same immutable content bundle*
 * that already publishes scene_blocks / persona atomically:
 *
 *   - identity, relations, bodies, the change record, the current identity
 *     snapshot and the source sidecar all ship in ONE bundle one pointer switch;
 *   - the manifest carries `recorded_at` / `sequence` and a `prev_bundle_id`
 *     link; only the CURRENT-reachable chain counts as formal history (a bundle
 *     staged but never pointed at — crash, CAS loss, hook throw — is ignored);
 *   - old bodies are reused from the bundle that created them (never copied
 *     into later bundles), and `sequence`/`recorded_at` are allocated inside the
 *     publish lock, so no late/backfilled history is possible.
 *
 * Boundaries (explicit, never silently unsafe):
 *   - L2/L3 only. `locator` is a scaled content path (`scene_blocks/<n>.md` /
 *     `persona.md`), never a filesystem path or the stable identity.
 *   - `content_id` (identity) and `version_id` (version) are program-assigned and
 *     independent of the path; a `version_id` is never used as a directory, and
 *     all bundle/path access goes through `resolveSafeLocalPath`.
 *   - scope is per OBJECT, not just a root window: every read candidate must
 *     match the query scope exactly (canonicalised); a root scope only bounds
 *     which scopes may be committed. A wider root scope never turns into an
 *     object access grant.
 *   - a legacy root (content published before history existed) can be imported as
 *     an explicitly incomplete baseline (`history_complete=false`) — the old
 *     publications are never reconstructed from mtimes/file names/diagnostics.
 */

import path from "node:path";
import { createHash, randomUUID } from "node:crypto";

import { isL3DocumentLocator, StoragePaths } from "./types.js";
import {
  commitContentBundle,
  collectPublishedContent,
  readBundleFile,
  readCurrentPointer,
  readBundleHistory,
  type BundleFile,
  type BundleHistoryManifest,
  type ContentHistoryCommitContext,
  type ContentHistoryCommitOutcome,
  type ContentHistoryNoop,
  type ContentIndexRecord,
  type GenerationHistoryPlan,
  type LocalPublishHooks,
  type OperationRecord,
  type StoredVersion,
  type VersionBodyRef,
} from "./local-published-storage.js";
import {
  CONTENT_SOURCES_KEY,
  applySourceSidecarMutations,
  parseContentSourceFile,
  planContentSourceSidecar,
  type PlannedContentSource,
  type SourceSidecarMutation,
} from "../content/sources.js";
import type { ContentSourceEntry, ContentSourceLayer } from "../content/types.js";
import type {
  ChangeCommit,
  ChangeCommitResult,
  ChangeKind,
  ContentVersion,
  HistoryLayer,
  HistoryPage,
  HistoryPageQuery,
  HistoryRead,
  HistoryReader,
  HistoryScope,
  VersionMutation,
  VersionRef,
} from "../content/history-contract.js";
import type { CommittedOperationRecord } from "../store/types.js";

const TAG = "[storage][content-history]";

/** Hard cap on how far the prev chain is walked in one read/commit. */
const MAX_HISTORY_CHAIN = 10_000;

const MAX_PAGE_LIMIT = 1000;
const SCOPE_KEYS = new Set(["team_id", "user_id", "agent_id", "task_id", "session_id"]);
const CHANGE_KINDS = new Set<ChangeKind>([
  "baseline", "create", "revise", "retire", "restore",
]);
/** Change kinds that may introduce a brand-new identity (expected_version_id=null). */
const NEW_IDENTITY_KINDS = new Set<ChangeKind>(["create", "baseline"]);

/**
 * Program metadata key carrying the normalized client-request fingerprint of a
 * commit. Written by the gateway as program data (never user_metadata), bound
 * into the payload hash, stored on the operation record for replay verification,
 * and removed before a version is materialized. Keep in sync with the SQLite
 * history (`core/store/sqlite/content-history.ts`).
 */
const PROGRAM_REQUEST_METADATA_KEY = "__program_request";

/** Read the program request fingerprint a commit carries (first mutation wins). */
function readProgramRequestFingerprint(commit: ChangeCommit): string | null {
  for (const mutation of commit.mutations ?? []) {
    const metadata = mutation?.draft?.metadata as Record<string, unknown> | undefined;
    if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) continue;
    const value = metadata[PROGRAM_REQUEST_METADATA_KEY];
    if (value === undefined) continue;
    if (typeof value !== "string" || value.length === 0) {
      invalid(`${PROGRAM_REQUEST_METADATA_KEY} must be a non-empty string when present`);
    }
    return value;
  }
  return null;
}

/** Remove the program request fingerprint so it never becomes version metadata. */
function stripProgramRequestFingerprint(commit: ChangeCommit): void {
  for (const mutation of commit.mutations ?? []) {
    const metadata = mutation?.draft?.metadata as Record<string, unknown> | undefined;
    if (metadata && typeof metadata === "object" && !Array.isArray(metadata)
      && PROGRAM_REQUEST_METADATA_KEY in metadata) {
      delete metadata[PROGRAM_REQUEST_METADATA_KEY];
    }
  }
}

export type LocalContentHistoryErrorCode =
  | "history_not_enabled"
  | "operation_conflict"
  | "stale_expected_version"
  | "locator_in_use"
  | "unknown_content_id"
  | "content_id_exists"
  | "invalid_scope"
  | "scope_mismatch"
  | "invalid_mutation"
  | "invalid_locator"
  | "invalid_read"
  | "invalid_page"
  | "unknown_predecessor"
  | "predecessor_not_current"
  | "missing_version_body"
  | "body_checksum_mismatch"
  | "clock_regressed"
  | "history_manifest_missing"
  | "history_cycle"
  | "history_too_deep"
  | "history_sequence_not_monotonic"
  | "version_not_found"
  | "staged_content_missing"
  | "staged_content_mismatch"
  | "changed_output_not_covered"
  | "source_plan_mismatch";

/** Explicit, machine-readable failure for the local content history. */
export class LocalContentHistoryError extends Error {
  readonly code: LocalContentHistoryErrorCode;
  constructor(code: LocalContentHistoryErrorCode, message: string) {
    super(`${TAG} ${code}: ${message}`);
    this.name = "LocalContentHistoryError";
    this.code = code;
  }
}

export interface CommitChangesOptions {
  /** Test/injection publish hooks (e.g. fail just before the pointer swap). */
  hooks?: LocalPublishHooks;
}

/** Program-callable configuration for the workspace history planner. */
export interface LocalHistoryPlanConfig {
  /** The change commit to record for this publish. */
  commit: ChangeCommit;
  /**
   * Program-provided source relations for the outputs this commit (re)writes.
   * Ids come from the caller's own registered/resolved inputs — the planner
   * never fabricates or widens one. Omit to leave the source sidecar untouched.
   */
  sources?: PlannedContentSource[];
}

/** Internal options for {@link LocalContentHistory.buildCommit}. */
interface BuildCommitOptions {
  /**
   * The workspace's FINAL content file set. When present it is authoritative:
   * bodies come from these bytes, a retired/renamed locator must be absent, and
   * every changed version-bearing file must be covered by a mutation.
   */
  staged?: Map<string, Buffer>;
  /** Program-provided source relations (planner mode). */
  sources?: PlannedContentSource[];
}

export interface EnableHistoryOptions {
  /** Scope recorded as this root's history scope (defaults to unscoped `{}`). */
  scope?: HistoryScope;
  hooks?: LocalPublishHooks;
}

export interface EnableHistoryResult {
  sequence: number;
  history_complete: boolean;
  /** Number of current items imported as baseline versions (0 when re-enabling). */
  imported: number;
}

// ── small helpers ────────────────────────────────────────────────

function invalid(message: string): never {
  throw new LocalContentHistoryError("invalid_mutation", message);
}

function invalidRead(message: string): never {
  throw new LocalContentHistoryError("invalid_read", message);
}

function assertValidScope(scope: unknown, where: string): asserts scope is HistoryScope {
  if (!scope || typeof scope !== "object" || Array.isArray(scope)) {
    throw new LocalContentHistoryError("invalid_scope", `${where} must be an object`);
  }
  for (const [key, value] of Object.entries(scope as Record<string, unknown>)) {
    if (!SCOPE_KEYS.has(key)) {
      throw new LocalContentHistoryError("invalid_scope", `${where} has unsupported key "${key}"`);
    }
    if (value !== undefined && value !== null && (typeof value !== "string" || value.length === 0)) {
      throw new LocalContentHistoryError("invalid_scope", `${where}.${key} must be a non-empty string`);
    }
  }
}

/**
 * Canonical key for a scope: only defined, non-empty dimensions, sorted. Two
 * scopes are the same object scope iff their keys match — `{}` never equals
 * `{team_id:"t"}`, so a root-scope check can never be used as an object grant.
 */
function canonicalScopeKey(scope: HistoryScope): string {
  const parts: string[] = [];
  for (const key of Array.from(SCOPE_KEYS).sort()) {
    const value = (scope as Record<string, unknown>)[key];
    if (typeof value === "string" && value.length > 0) parts.push(`${key}=${value}`);
  }
  return parts.join("\u0000");
}

/** Exact object-scope equality (canonical; missing-unknown keys rejected earlier). */
function scopeEquals(a: HistoryScope, b: HistoryScope): boolean {
  return canonicalScopeKey(a) === canonicalScopeKey(b);
}

/** Every defined key/value of `superScope` is present and equal in `subScope`. */
function scopeContains(superScope: HistoryScope, subScope: HistoryScope): boolean {
  for (const key of Array.from(SCOPE_KEYS)) {
    const value = (superScope as Record<string, unknown>)[key];
    if (typeof value !== "string" || value.length === 0) continue;
    if ((subScope as Record<string, unknown>)[key] !== value) return false;
  }
  return true;
}

/** Whether `locator` is a valid L2/L3 content path for the given layer. */
function locatorForLayer(layer: HistoryLayer, locator: unknown): boolean {
  if (typeof locator !== "string" || locator.length === 0) return false;
  if (locator.includes("\0") || locator.startsWith("/") || locator.startsWith("\\")) return false;
  if (locator.includes("\\") || locator.split("/").includes("..")) return false;
  if (layer === "l2") return locator.startsWith(StoragePaths.sceneBlocksDir) && locator.endsWith(".md");
  // 2026-10-12：L3 = 用户理解 / 信念 / 当前重点三份文档；persona.md 不再是版本内容 locator。
  if (layer === "l3") return isL3DocumentLocator(locator);
  return false;
}

/** The scene-relative path the source sidecar uses for a content `locator`. */
function sourcePathForLayer(layer: HistoryLayer, locator: string): string {
  if (layer === "l2" && locator.startsWith(StoragePaths.sceneBlocksDir)) {
    return locator.slice(StoragePaths.sceneBlocksDir.length);
  }
  return locator;
}

/** Canonical sidecar key for one `(layer, path)` relation. */
function sourceKey(layer: HistoryLayer, path: string): string {
  return `${layer}\u0000${path}`;
}

/**
 * With an authoritative staged set, a retired / renamed / consumed locator must
 * already be absent (the generator removed it from the live view). Its presence
 * means the staged content disagrees with the commit, so refuse.
 */
function assertAbsentFromStaged(staged: Map<string, Buffer>, locator: string, what: string): void {
  if (staged.has(locator)) {
    throw new LocalContentHistoryError(
      "staged_content_mismatch",
      `${what} locator ${locator} is still present in the staged content`,
    );
  }
}

/** Whether a bundle content key is a version-bearing content path (body file). */
function isVersionLocator(key: string): boolean {
  return isL3DocumentLocator(key) || (key.startsWith(StoragePaths.sceneBlocksDir) && key.endsWith(".md"));
}

function layerOfLocator(key: string): HistoryLayer {
  return isL3DocumentLocator(key) ? "l3" : "l2";
}

function newContentId(): string {
  return `c_${randomUUID().replace(/-/g, "").slice(0, 20)}`;
}

function newVersionId(sequence: number): string {
  return `v_${sequence}_${randomUUID().replace(/-/g, "").slice(0, 12)}`;
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.keys(value as Record<string, unknown>).sort();
  return `{${entries
    .map((key) => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`)
    .join(",")}}`;
}

function deepEqualJson(a: unknown, b: unknown): boolean {
  return canonicalJson(a) === canonicalJson(b);
}

/** Deep, detached copy so callers can never mutate cached internal objects. */
function cloneJson<T>(value: T): T {
  if (value === undefined) return value;
  return JSON.parse(JSON.stringify(value)) as T;
}

function bufferSha256(buffer: Buffer): string {
  return createHash("sha256").update(buffer).digest("hex");
}

function hashPlannedPayload(
  commit: ChangeCommit,
  sources: PlannedContentSource[] | undefined,
  staged: Map<string, Buffer> | undefined,
): string {
  return createHash("sha256")
    .update(
      canonicalJson({
        actor: commit.actor,
        mutations: commit.mutations.map((mutation) => ({
          draft: { ...mutation.draft, body: effectiveMutationBody(mutation.draft, staged) },
          expected_version_id: mutation.expected_version_id,
          predecessors: mutation.predecessors,
          change_kind: mutation.change_kind,
        })),
        sources: sources ?? null,
      }),
      "utf-8",
    )
    .digest("hex");
}

/**
 * The body a mutation actually publishes. With an authoritative staged set the
 * staged bytes are the truth (and, by contract, `draft.body` must equal them
 * verbatim); otherwise the caller-declared body is. The idempotency fingerprint
 * binds this exact final body, so two requests that differ only in the bytes
 * they publish can never be treated as the same request.
 */
function effectiveMutationBody(
  draft: VersionMutation["draft"],
  staged: Map<string, Buffer> | undefined,
): string {
  if (staged && draft.state === "active" && typeof draft.locator === "string") {
    const buffer = staged.get(draft.locator);
    if (!buffer) {
      throw new LocalContentHistoryError("staged_content_missing", `missing staged content for ${draft.locator}`);
    }
    const body = buffer.toString("utf-8");
    if (draft.body !== body) {
      throw new LocalContentHistoryError("staged_content_mismatch", `declared body differs from staged content for ${draft.locator}`);
    }
    return body;
  }
  return draft.body;
}

/**
 * The `updated_at_ms` a re-recorded, semantically identical source relation must
 * keep: the prior version's own view first (guaranteeing a genuinely unchanged
 * output stays a no-op), then the prior sidecar entry, then the clock.
 */
function stableSourceUpdatedAt(
  prevRecord: ContentIndexRecord | null,
  priorEntry: { updated_at_ms?: unknown } | undefined,
  fallback: number,
): number {
  const priorView = prevRecord && prevRecord.metadata && typeof prevRecord.metadata === "object"
    ? (prevRecord.metadata as Record<string, unknown>).content_sources
    : undefined;
  if (priorView && typeof priorView === "object" && !Array.isArray(priorView)
    && typeof (priorView as Record<string, unknown>).updated_at_ms === "number") {
    return (priorView as Record<string, unknown>).updated_at_ms as number;
  }
  if (priorEntry && typeof priorEntry.updated_at_ms === "number") return priorEntry.updated_at_ms;
  return fallback;
}

function isValidDateTime(value: string): boolean {
  if (typeof value !== "string" || value.length === 0) return false;
  return Number.isFinite(Date.parse(value));
}

function assertOptionalDateTime(value: unknown, where: string): void {
  if (value === undefined) return;
  if (typeof value !== "string" || !isValidDateTime(value)) {
    invalidRead(`${where} must be a valid date-time string`);
  }
}

function assertOptionalUpperSequence(value: unknown, where: string): void {
  if (value === undefined) return;
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    invalidRead(`${where} must be a non-negative integer`);
  }
}

function assertValidAfterCursor(value: unknown): void {
  if (value === undefined) return;
  const cursor = value as Record<string, unknown> | null;
  if (!cursor || typeof cursor !== "object" || Array.isArray(cursor)) {
    invalidRead("after must be {sequence, version_id}");
  }
  if (!Number.isSafeInteger(cursor.sequence) || (cursor.sequence as number) < 0) {
    invalidRead("after.sequence must be a non-negative integer");
  }
  if (typeof cursor.version_id !== "string" || cursor.version_id.length === 0) {
    invalidRead("after.version_id must be a non-empty string");
  }
}

function refKey(ref: VersionRef): string {
  return `${ref.layer}\u0000${ref.content_id}\u0000${ref.version_id}`;
}

function materializeContentVersion(stored: StoredVersion, body: string): ContentVersion {
  return {
    layer: stored.layer,
    content_id: stored.content_id,
    locator: stored.locator,
    scope: cloneJson(stored.scope),
    body,
    metadata: cloneJson(stored.metadata),
    state: stored.state,
    ref: { ...stored.ref },
    recorded_at: stored.recorded_at,
    sequence: stored.sequence,
    predecessors: cloneJson(stored.predecessors),
    change_kind: stored.change_kind,
    history_complete: stored.history_complete,
  };
}

function assertValidMutation(mutation: VersionMutation, index: number): void {
  const where = `mutations[${index}]`;
  if (!mutation || typeof mutation !== "object") invalid(`${where} must be an object`);
  const draft = mutation.draft as unknown as Record<string, unknown> | undefined;
  if (!draft || typeof draft !== "object") invalid(`${where}.draft must be an object`);
  if (draft.layer !== "l2" && draft.layer !== "l3") invalid(`${where}.draft.layer must be "l2" or "l3"`);
  if (typeof draft.body !== "string") invalid(`${where}.draft.body must be a string`);
  if (draft.state !== "active" && draft.state !== "retired") invalid(`${where}.draft.state must be "active" or "retired"`);
  assertValidScope(draft.scope, `${where}.draft.scope`);
  if (!draft.metadata || typeof draft.metadata !== "object" || Array.isArray(draft.metadata)) {
    invalid(`${where}.draft.metadata must be an object`);
  }
  if (mutation.expected_version_id !== null && typeof mutation.expected_version_id !== "string") {
    invalid(`${where}.expected_version_id must be null or a string`);
  }
  if (!Array.isArray(mutation.predecessors)) invalid(`${where}.predecessors must be an array`);
  for (const [j, pred] of (mutation.predecessors as unknown[]).entries()) {
    const p = pred as Record<string, unknown> | null;
    if (!p || typeof p !== "object" || Array.isArray(p)) invalid(`${where}.predecessors[${j}] must be an object`);
    if (p.layer !== "l2" && p.layer !== "l3") invalid(`${where}.predecessors[${j}].layer is invalid`);
    if (typeof p.content_id !== "string" || p.content_id.length === 0) invalid(`${where}.predecessors[${j}].content_id is invalid`);
    if (typeof p.version_id !== "string" || p.version_id.length === 0) invalid(`${where}.predecessors[${j}].version_id is invalid`);
  }
  if (!CHANGE_KINDS.has(mutation.change_kind)) invalid(`${where}.change_kind is invalid`);
}

function assertValidCommit(commit: ChangeCommit): void {
  if (!commit || typeof commit !== "object") invalid("commit must be an object");
  if (typeof commit.operation_id !== "string" || commit.operation_id.length === 0) {
    invalid("operation_id is required");
  }
  if (commit.actor !== "user" && commit.actor !== "model" && commit.actor !== "program") {
    invalid("actor must be user|model|program");
  }
  if (!Array.isArray(commit.mutations)) invalid("mutations must be an array");
  for (let i = 0; i < commit.mutations.length; i++) assertValidMutation(commit.mutations[i], i);
}

/**
 * Local formal content history (L2/L3).
 *
 * Construct with the storage root (the same directory the LocalStorageBackend /
 * generation workspace publish under). The optional `clock` overrides the
 * program's `recorded_at` source (tests only); production uses the wall clock.
 */
export class LocalContentHistory implements HistoryReader {
  readonly rootDir: string;
  private readonly clock: () => Date;
  /**
   * Read-through cache of immutable bundle history manifests. It is a derived
   * optimisation only: every write decision is re-checked against disk inside
   * the publish lock, so the cache can never become a second authority. Values
   * returned to callers are always detached copies.
   */
  private readonly cache = new Map<string, BundleHistoryManifest | null>();

  constructor(rootDir: string, clock?: () => Date) {
    if (typeof rootDir !== "string" || rootDir.length === 0) {
      throw new LocalContentHistoryError("invalid_mutation", "rootDir is required");
    }
    this.rootDir = path.resolve(rootDir);
    this.clock = clock ?? (() => new Date());
  }

  /** Drop the in-memory derived cache (never touches the published chain). */
  clearCache(): void {
    this.cache.clear();
  }

  /** Whether this root already has a formal history chain enabled. */
  async isEnabled(): Promise<boolean> {
    const pointer = await readCurrentPointer(this.rootDir);
    return pointer?.history !== undefined;
  }

  /**
   * Enable history on this root, importing whatever content is currently
   * published as an explicit baseline.
   *
   * A root that already has content but no verifiable prev chain cannot have its
   * past reconstructed, so the imported baseline is marked `history_complete =
   * false`. An empty root starts a clean (complete) chain. Re-enabling is a
   * no-op that reports the current state.
   */
  async enableHistory(opts: EnableHistoryOptions = {}): Promise<EnableHistoryResult> {
    const scope = opts.scope ?? {};
    assertValidScope(scope, "enableHistory.scope");

    const pointer = await readCurrentPointer(this.rootDir);
    if (pointer?.history) {
      const chain = await this.walkChainFrom(pointer.bundleId);
      return {
        sequence: chain[0]?.sequence ?? 0,
        history_complete: chain[chain.length - 1]?.history_complete ?? false,
        imported: 0,
      };
    }

    const outcome = await commitContentBundle(this.rootDir, [], {
      hooks: opts.hooks,
      expectedRevision: pointer?.revision ?? 0,
      history: {
        enable: true,
        clock: this.clock,
        build: (ctx) => this.buildBaseline(ctx, scope),
      },
    });
    return outcome.historyResult as EnableHistoryResult;
  }

  /**
   * Commit one or more content mutations as a single formal revision.
   *
   * All mutations either all succeed (one bundle, one pointer switch) or none
   * do. `sequence`/`recorded_at` are allocated inside the publish lock; the
   * `expected_version_id` of every mutation is CAS-checked against the current
   * chain before anything is written. `operation_id` makes the commit idempotent
   * (same payload → same result; different payload → `operation_conflict`).
   *
   * A commit whose content/metadata/relations/state are all unchanged is a
   * no-op: it returns `changed=false` and the current version(s), and only
   * registers the operation record (no fabricated cognition version).
   */
  async commitChanges(commit: ChangeCommit, opts: CommitChangesOptions = {}): Promise<ChangeCommitResult> {
    assertValidCommit(commit);
    const pointer = await readCurrentPointer(this.rootDir);
    if (!pointer?.history) {
      throw new LocalContentHistoryError(
        "history_not_enabled",
        "call enableHistory() before committing formal content changes",
      );
    }
    if (commit.mutations.length === 0) {
      const chain = await this.walkChainFrom(pointer.bundleId);
      return { changed: false, versions: [], sequence: chain[0]?.sequence ?? 0 };
    }

    const outcome = await commitContentBundle(this.rootDir, [], {
      hooks: opts.hooks,
      expectedRevision: pointer.revision,
      history: {
        clock: this.clock,
        build: (ctx) => this.buildCommit(ctx, commit),
      },
    });
    return outcome.historyResult as ChangeCommitResult;
  }

  /**
   * Build a program-callable planner for ONE content commit, for use as a
   * `GenerationWorkspace.historyContext`.
   *
   * Unlike {@link commitChanges} the planner does not publish by itself: the
   * workspace hands it the FINAL staged content file set and it derives the
   * version chain, the source sidecar and each version's `metadata.content_sources`
   * from those exact bytes. The generator therefore never hand-assembles a
   * manifest, and the same code path backs both the direct commit and the
   * workspace publish (one planner, no second truth).
   */
  planCommit(config: LocalHistoryPlanConfig): GenerationHistoryPlan {
    assertValidCommit(config.commit);
    const commit = cloneJson(config.commit);
    const sources = cloneJson(config.sources ?? []);
    return {
      plan: (ctx, files) =>
        this.buildCommit(ctx, commit, {
          staged: new Map(files.map((file) => [file.key, file.content] as const)),
          sources,
        }),
    };
  }

  // ── HistoryReader ──────────────────────────────────────────────

  /**
   * Read the current version, one exact version, or the version in force at an
   * inclusive `as_of` Core-recorded time. Returns null when the content/version
   * is not visible in the requested object scope — never another content's
   * current body as a stand-in for a missing historical version.
   */
  async readVersion(query: HistoryRead): Promise<ContentVersion | null> {
    if (!query || typeof query !== "object") invalidRead("readVersion query is required");
    assertValidScope(query.scope, "HistoryRead.scope");
    if (query.version_id !== undefined && query.as_of !== undefined) {
      invalidRead("version_id and as_of are mutually exclusive");
    }
    if (query.version_id !== undefined && typeof query.version_id !== "string") {
      invalidRead("version_id must be a string");
    }
    assertOptionalDateTime(query.as_of, "HistoryRead.as_of");
    assertOptionalUpperSequence(query.upper_sequence, "HistoryRead.upper_sequence");
    if (query.layer !== "l2" && query.layer !== "l3") return null;
    if (typeof query.content_id !== "string" || query.content_id.length === 0) return null;

    const { chain, pointer } = await this.loadChain();
    if (!pointer?.history || chain.length === 0) return null;
    const upper = query.upper_sequence ?? chain[0].sequence;
    const stored = this.findVersion(chain, query, upper);
    if (!stored) return null;
    return this.materialize(stored);
  }

  /**
   * List change versions (ascending by `(sequence, version_id)`) with bounded,
   * keyset pagination. `upper_sequence` pins the commit boundary so a version
   * committed while a caller is paging never mixes into its pages.
   */
  async listChanges(query: HistoryPageQuery): Promise<HistoryPage> {
    if (!query || typeof query !== "object") invalidRead("listChanges query is required");
    assertValidScope(query.scope, "HistoryPageQuery.scope");
    if (!Number.isSafeInteger(query.limit) || query.limit < 1) {
      throw new LocalContentHistoryError("invalid_page", "limit must be a positive integer");
    }
    const limit = Math.min(query.limit, MAX_PAGE_LIMIT);
    assertOptionalDateTime(query.from_recorded_at, "HistoryPageQuery.from_recorded_at");
    assertOptionalDateTime(query.to_recorded_at, "HistoryPageQuery.to_recorded_at");
    assertOptionalUpperSequence(query.upper_sequence, "HistoryPageQuery.upper_sequence");
    assertValidAfterCursor(query.after);
    if (
      query.from_recorded_at !== undefined &&
      query.to_recorded_at !== undefined &&
      query.from_recorded_at > query.to_recorded_at
    ) {
      invalidRead("from_recorded_at must not be after to_recorded_at");
    }

    const { chain, pointer } = await this.loadChain();
    if (!pointer?.history || chain.length === 0) {
      return { items: [], upper_sequence: 0, next: null, history_complete: false };
    }
    const upper = query.upper_sequence ?? chain[0].sequence;
    const historyComplete = chain[chain.length - 1].history_complete;

    const collected: StoredVersion[] = [];
    for (const manifest of chain) {
      for (const version of manifest.changes) {
        if (version.sequence > upper) continue;
        if (query.layer && version.layer !== query.layer) continue;
        if (query.content_id && version.content_id !== query.content_id) continue;
        if (!scopeEquals(version.scope, query.scope)) continue;
        if (query.from_recorded_at && version.recorded_at < query.from_recorded_at) continue;
        if (query.to_recorded_at && version.recorded_at > query.to_recorded_at) continue;
        if (query.after && !afterKeyGreater(version, query.after)) continue;
        collected.push(version);
      }
    }
    collected.sort(compareVersions);

    const page = collected.slice(0, limit);
    const next = collected.length > page.length && page.length > 0
      ? { sequence: page[page.length - 1].sequence, version_id: page[page.length - 1].ref.version_id }
      : null;
    const items = await Promise.all(page.map((stored) => this.materialize(stored)));
    return { items, upper_sequence: upper, next, history_complete: historyComplete };
  }

  /**
   * Resolve an old locator (a profile path) to the exact version that used it —
   * NOT its current successor. A locator that was renamed away still resolves to
   * the version at the old path. Only versions in the requested object scope are
   * visible.
   */
  async resolveLocator(layer: HistoryLayer, locator: string, scope: HistoryScope): Promise<VersionRef | null> {
    assertValidScope(scope, "resolveLocator.scope");
    if (layer !== "l2" && layer !== "l3") return null;
    if (typeof locator !== "string" || locator.length === 0) return null;
    const { chain, pointer } = await this.loadChain();
    if (!pointer?.history || chain.length === 0) return null;

    let best: StoredVersion | null = null;
    for (const manifest of chain) {
      for (const version of manifest.changes) {
        if (version.layer !== layer || version.locator !== locator) continue;
        if (!scopeEquals(version.scope, scope)) continue;
        if (!best || version.sequence > best.sequence
          || (version.sequence === best.sequence && version.ref.version_id > best.ref.version_id)) {
          best = version;
        }
      }
    }
    return best ? { ...best.ref } : null;
  }

  // ── commit builders (run inside the publish lock) ──────────────

  /**
   * Read one committed operation from this root's OWN formal chain (the
   * CURRENT-reachable bundle history) and replay the exact original result.
   *
   * Read-only: no second idempotency store, no process map, no log scan. `null`
   * covers "absent", "history not enabled" and "out of this object scope".
   */
  async getCommittedOperation(scope: HistoryScope, operationId: string): Promise<CommittedOperationRecord | null> {
    assertValidScope(scope, "getCommittedOperation.scope");
    if (typeof operationId !== "string" || operationId.length === 0) return null;
    const pointer = await readCurrentPointer(this.rootDir);
    if (!pointer?.history) return null;
    const found = await this.findOperation(pointer.bundleId, operationId, canonicalScopeKey(scope));
    if (!found) return null;
    const result = await this.replayOperation(found.chain, found.op);
    const raw = found.op.request_fingerprint;
    return {
      operation_id: operationId,
      scope,
      request_fingerprint: typeof raw === "string" && raw.length > 0 ? raw : null,
      result,
    };
  }

  private buildBaseline(
    ctx: ContentHistoryCommitContext,
    scope: HistoryScope,
  ): Promise<ContentHistoryCommitOutcome> {
    return (async () => {
      const content = await collectPublishedContent(this.rootDir);
      const contentIsEmpty = content.size === 0;
      const historyComplete = contentIsEmpty;
      const sequence = 1;
      const recordedAt = ctx.now.toISOString();

      const sourcesRaw = content.get(CONTENT_SOURCES_KEY)?.toString("utf-8") ?? null;
      const sourceEntries = sourcesRaw ? parseContentSourceFile(sourcesRaw) : [];

      const changes: StoredVersion[] = [];
      const contents: Record<string, ContentIndexRecord> = {};
      for (const key of content.keys()) {
        if (!isVersionLocator(key)) continue;
        const layer = layerOfLocator(key);
        const contentId = newContentId();
        const versionId = newVersionId(sequence);
        const buffer = content.get(key) ?? Buffer.alloc(0);
        const sourcePath = sourcePathForLayer(layer, key);
        const entry = sourceEntries.find((e) => e.layer === layer && e.path === sourcePath) ?? null;
        const metadata = { content_sources: baselineSourceMetadata(entry?.content_sha256 === bufferSha256(buffer) ? entry : null) };
        changes.push({
          ref: { layer, content_id: contentId, version_id: versionId },
          layer,
          content_id: contentId,
          locator: key,
          scope,
          metadata,
          state: "active",
          recorded_at: recordedAt,
          sequence,
          predecessors: [],
          change_kind: "baseline",
          history_complete: historyComplete,
          body_ref: { bundle_id: ctx.bundleId, locator: key, size: buffer.length, sha256: bufferSha256(buffer) },
        });
        contents[contentId] = {
          content_id: contentId,
          layer,
          locator: key,
          scope,
          state: "active",
          current_version_id: versionId,
          metadata,
          updated_at: recordedAt,
          updated_sequence: sequence,
          history_complete: historyComplete,
        };
      }

      const files: BundleFile[] = Array.from(content.entries()).map(([key, buffer]) => ({ key, content: buffer }));
      const history: BundleHistoryManifest = {
        schema_version: 1,
        sequence,
        recorded_at: recordedAt,
        prev_bundle_id: null,
        history_complete: historyComplete,
        scope,
        changes,
        contents,
        operations: [],
      };
      const result: EnableHistoryResult = {
        sequence,
        history_complete: historyComplete,
        imported: changes.length,
      };
      return { files, history, result };
    })();
  }

  private async buildCommit(
    ctx: ContentHistoryCommitContext,
    commit: ChangeCommit,
    opts?: BuildCommitOptions,
  ): Promise<ContentHistoryCommitOutcome | ContentHistoryNoop> {
    const prev = ctx.prevHistory;
    if (!prev) {
      throw new LocalContentHistoryError(
        "history_manifest_missing",
        "current bundle has no history manifest; refusing to commit",
      );
    }

    // Fingerprint the FINAL semantic request: the staged bytes (the published
    // body) plus the separately-supplied source relations, so an operation id
    // reused for a different body OR different sources is a conflict, while a
    // byte-identical replay still returns the original result.
    // A planner may be reused, and direct callers may replay the same object.
    commit = cloneJson(commit);
    const requestFingerprint = readProgramRequestFingerprint(commit);
    const payloadHash = hashPlannedPayload(commit, opts?.sources, opts?.staged);
    // The program request tag is bound into the hash above but never becomes a
    // version's metadata.
    stripProgramRequestFingerprint(commit);

    // One commit is exactly one object scope (no mixed-scope batches).
    const commitScope = commit.mutations[0].draft.scope;
    const commitScopeKey = canonicalScopeKey(commitScope);
    for (let i = 1; i < commit.mutations.length; i++) {
      if (canonicalScopeKey(commit.mutations[i].draft.scope) !== commitScopeKey) {
        throw new LocalContentHistoryError(
          "scope_mismatch",
          `mutations[${i}] mixes object scopes; one commit must target a single scope`,
        );
      }
    }

    // Idempotency: the same operation_id + scope + payload returns the same
    // result without producing a new revision; a different payload conflicts.
    if (ctx.currentBundleId) {
      const found = await this.findOperation(ctx.currentBundleId, commit.operation_id, commitScopeKey);
      if (found) {
        if (found.op.payload_hash !== payloadHash) {
          throw new LocalContentHistoryError(
            "operation_conflict",
            `operation_id ${commit.operation_id} was already used with a different payload`,
          );
        }
        return { noop: true, result: await this.replayOperation(found.chain, found.op) };
      }
    }

    const rootScope = prev.scope ?? {};
    const sequence = prev.sequence + 1;
    const recordedAt = ctx.now.toISOString();
    if (typeof prev.recorded_at === "string" && recordedAt < prev.recorded_at) {
      throw new LocalContentHistoryError(
        "clock_regressed",
        `clock moved backwards: ${recordedAt} is before the last recorded_at ${prev.recorded_at}`,
      );
    }

    const chain = ctx.currentBundleId ? await this.walkChainFrom(ctx.currentBundleId) : [prev];
    const contents: Record<string, ContentIndexRecord> = {};
    for (const [id, record] of Object.entries(prev.contents)) contents[id] = cloneJson(record);

    // With `opts.staged` the workspace's FINAL content file set is authoritative:
    // bodies come from it (never from a caller-declared string) and every changed
    // version-bearing file must be covered by a mutation. Without it the commit
    // derives the file set from the published content + the mutation bodies.
    const staged = opts?.staged;
    const publishedFiles = staged ? await collectPublishedContent(this.rootDir) : null;
    const changedVersionKeys = new Set<string>();
    if (staged && publishedFiles) {
      for (const key of new Set([...publishedFiles.keys(), ...staged.keys()])) {
        if (!isVersionLocator(key)) continue;
        const before = publishedFiles.get(key);
        const after = staged.get(key);
        if (!before || !after || !before.equals(after)) changedVersionKeys.add(key);
      }
    }
    const coveredVersionKeys = new Set<string>();

    const currentFiles = staged ? new Map(staged) : await collectPublishedContent(this.rootDir);
    const sourcesRaw = currentFiles.get(CONTENT_SOURCES_KEY)?.toString("utf-8") ?? null;
    const priorSourceByKey = new Map<string, ContentSourceEntry>();
    if (staged && sourcesRaw) {
      for (const entry of parseContentSourceFile(sourcesRaw)) {
        priorSourceByKey.set(sourceKey(entry.layer, entry.path), entry);
      }
    }
    const plannedSourceByKey = new Map<string, PlannedContentSource>();
    for (const source of opts?.sources ?? []) {
      plannedSourceByKey.set(sourceKey(source.layer, source.path), source);
    }
    const sourceMutations: SourceSidecarMutation[] = [];

    const changes: StoredVersion[] = [];
    const versions: ContentVersion[] = [];
    const touched = new Set<string>();
    let anyChanged = false;

    for (let i = 0; i < commit.mutations.length; i++) {
      const mutation = commit.mutations[i];
      const draft = cloneJson(mutation.draft);
      if ((mutation.change_kind === "retire" && draft.state !== "retired") ||
          (mutation.change_kind !== "retire" && draft.state !== "active")) {
        invalid(`mutations[${i}] change kind and lifecycle state disagree`);
      }
      if (!scopeContains(rootScope, draft.scope)) {
        throw new LocalContentHistoryError(
          "scope_mismatch",
          `mutations[${i}] scope is outside this root's history scope`,
        );
      }
      if (!locatorForLayer(draft.layer, draft.locator)) {
        throw new LocalContentHistoryError(
          "invalid_locator",
          `mutations[${i}] locator ${JSON.stringify(draft.locator)} is not a valid ${draft.layer} content path`,
        );
      }

      let contentId: string;
      let prevRecord: ContentIndexRecord | null = null;
      let prevStored: StoredVersion | null = null;
      let predecessorRef: VersionRef | null = null;

      if (mutation.expected_version_id === null) {
        if (!NEW_IDENTITY_KINDS.has(mutation.change_kind)) {
          invalid(`mutations[${i}] change_kind "${mutation.change_kind}" requires an existing identity`);
        }
        contentId = draft.content_id ?? newContentId();
        if (contents[contentId] || touched.has(contentId)) {
          throw new LocalContentHistoryError(
            "content_id_exists",
            `mutations[${i}] content_id ${contentId} already exists`,
          );
        }
      } else {
        if (!draft.content_id) {
          throw new LocalContentHistoryError(
            "unknown_content_id",
            `mutations[${i}] content_id is required when expected_version_id is set`,
          );
        }
        contentId = draft.content_id;
        if (touched.has(contentId)) {
          invalid(`mutations[${i}] identity ${contentId} is modified more than once in one commit`);
        }
        prevRecord = contents[contentId] ?? null;
        if (!prevRecord) {
          throw new LocalContentHistoryError(
            "unknown_content_id",
            `mutations[${i}] content_id ${contentId} is unknown`,
          );
        }
        if (prevRecord.layer !== draft.layer) {
          invalid(`mutations[${i}] cannot change layer of ${contentId}`);
        }
        if (!scopeEquals(prevRecord.scope, draft.scope)) {
          throw new LocalContentHistoryError(
            "scope_mismatch",
            `mutations[${i}] cannot change object scope of ${contentId}`,
          );
        }
        if (prevRecord.current_version_id !== mutation.expected_version_id) {
          throw new LocalContentHistoryError(
            "stale_expected_version",
            `mutations[${i}] expected ${mutation.expected_version_id} but current is ${prevRecord.current_version_id}`,
          );
        }
        predecessorRef = {
          layer: prevRecord.layer,
          content_id: contentId,
          version_id: prevRecord.current_version_id,
        };
        prevStored = findStoredVersion(chain, predecessorRef);
        if (!prevStored) {
          invalid(`mutations[${i}] current version of ${contentId} is not on the history chain`);
        }
        if (NEW_IDENTITY_KINDS.has(mutation.change_kind)) {
          invalid(`mutations[${i}] ${mutation.change_kind} must create a new identity`);
        }
        // 收窄后 locator 即身份的地址：任何身份保持的修改（revise/retire/restore）
        // 都不能移动 locator；要换地址就是新身份 + 旧身份退役。
        if (prevRecord.locator !== draft.locator) {
          invalid(`mutations[${i}] identity-preserving change cannot move the locator`);
        }
        if (prevRecord.state === "retired") {
          if (mutation.change_kind !== "restore") {
            invalid(`mutations[${i}] retired identity ${contentId} can only be changed by restore`);
          }
        } else if (mutation.change_kind === "restore") {
          invalid(`mutations[${i}] identity ${contentId} is already active`);
        }
      }
      touched.add(contentId);

      // Validate every predecessor exactly: exists on the chain, same layer and
      // object scope, and still the current baseline of its identity.
      const rawPreds = mutation.predecessors.length > 0
        ? mutation.predecessors
        : (predecessorRef ? [predecessorRef] : []);
      const preds: VersionRef[] = [];
      const predSeen = new Set<string>();
      for (const pred of rawPreds) {
        const key = refKey(pred);
        if (predSeen.has(key)) invalid(`mutations[${i}] duplicate predecessor ${pred.version_id}`);
        predSeen.add(key);
        const pStored = findStoredVersion(chain, pred);
        if (!pStored) {
          throw new LocalContentHistoryError(
            "unknown_predecessor",
            `mutations[${i}] predecessor ${pred.version_id} does not exist on the history chain`,
          );
        }
        if (pStored.layer !== draft.layer) {
          invalid(`mutations[${i}] predecessor ${pred.version_id} has a different layer`);
        }
        const pRec = contents[pred.content_id];
        if (!pRec || pRec.current_version_id !== pred.version_id) {
          throw new LocalContentHistoryError(
            "predecessor_not_current",
            `mutations[${i}] predecessor ${pred.version_id} is not the current version of ${pred.content_id}`,
          );
        }
        if (!scopeEquals(pStored.scope, draft.scope)) {
          throw new LocalContentHistoryError(
            "scope_mismatch",
            `mutations[${i}] predecessor ${pred.version_id} is outside this commit's object scope`,
          );
        }
        if (pStored.content_id !== contentId) {
          invalid(`mutations[${i}] identity-preserving change has a predecessor of another identity`);
        }
        preds.push({ layer: pStored.layer, content_id: pStored.content_id, version_id: pStored.ref.version_id });
        if (staged) coveredVersionKeys.add(pStored.locator);
      }
      if (staged) {
        coveredVersionKeys.add(draft.locator);
        if (prevRecord) coveredVersionKeys.add(prevRecord.locator);
      }

      for (const [otherId, record] of Object.entries(contents)) {
        if (otherId === contentId) continue;
        if (record.state === "active" && record.locator === draft.locator) {
          throw new LocalContentHistoryError(
            "locator_in_use",
            `mutations[${i}] locator ${draft.locator} is held by active content ${otherId}`,
          );
        }
      }

      const prevBodyString = prevStored ? await this.readBodyString(prevStored) : null;
      if (prevStored && (mutation.change_kind === "retire" || mutation.change_kind === "restore")) {
        Object.assign(draft, { body: prevBodyString, metadata: cloneJson(prevStored.metadata) });
      }

      // With a staged set the published bytes are the ONLY truth for the body:
      // `draft.body` must equal them verbatim (an empty string addresses empty
      // content — it is NOT a wildcard placeholder), and a renamed locator must
      // already be gone. The final body is frozen here so the source view and
      // the no-op decision below all describe the same bytes.
      if (staged && draft.state !== "retired") {
        const stagedBody = staged.get(draft.locator);
        if (!stagedBody) {
          throw new LocalContentHistoryError(
            "staged_content_missing",
            `mutations[${i}] staged content is missing for locator ${draft.locator}`,
          );
        }
        const stagedBodyString = stagedBody.toString("utf-8");
        if (draft.body !== stagedBodyString) {
          throw new LocalContentHistoryError(
            "staged_content_mismatch",
            `mutations[${i}] declared body differs from the staged bytes for ${draft.locator}`,
          );
        }
        draft.body = stagedBodyString;
        if (prevRecord && prevRecord.locator !== draft.locator) {
          assertAbsentFromStaged(staged, prevRecord.locator, "renamed");
        }
      }

      // A formal version's `metadata.content_sources` must be EXACTLY the final
      // sidecar view for its output: a program-provided relation (re)supplied
      // this run is always written into the version — even when its input ids did
      // not change — so a metadata-only (or otherwise new) version never silently
      // downgrades a live `known` relation to `unknown`. A caller-forged
      // `metadata.content_sources` is never trusted: it is always overwritten from
      // the program planner (or cleared to `unknown`). When the relation is
      // unchanged the prior timestamp is preserved so a truly unchanged output
      // stays a no-op instead of being re-stamped by the clock.
      if (opts?.sources !== undefined) {
        draft.metadata = {
          ...draft.metadata,
          content_sources: this.resolveVersionSourceView({
            draft,
            prevRecord,
            prevBodyString,
            plannedSourceByKey,
            priorSourceByKey,
            nowMs: ctx.now.getTime(),
          }),
        };
      }

      // No change → no new cognition version (the operation is still recorded).
      if (prevStored && prevRecord && prevBodyString !== null) {
        const unchanged =
          prevRecord.locator === draft.locator
          && prevRecord.state === draft.state
          && scopeEquals(prevRecord.scope, draft.scope)
          && prevBodyString === draft.body
          && deepEqualJson(prevRecord.metadata, draft.metadata);
        if (unchanged) {
          versions.push(await this.materialize(prevStored));
          continue;
        }
      }

      anyChanged = true;
      const versionId = newVersionId(sequence);
      let body: string;
      let bodyRef: VersionBodyRef;
      if (draft.state === "retired") {
        if (!prevStored || !prevStored.body_ref) {
          invalid(`mutations[${i}] retire requires a predecessor body for ${contentId}`);
        }
        body = prevBodyString ?? await this.readBodyString(prevStored!);
        bodyRef = { ...prevStored!.body_ref! };
        if (staged) {
          // A retired cognition must not remain in the published (staged) set.
          assertAbsentFromStaged(staged, draft.locator, "retired");
          if (prevRecord && prevRecord.locator !== draft.locator) {
            assertAbsentFromStaged(staged, prevRecord.locator, "retired");
          }
        }
        currentFiles.delete(draft.locator);
        if (prevRecord && prevRecord.locator !== draft.locator) currentFiles.delete(prevRecord.locator);
      } else {
        if (!staged && mutation.change_kind === "restore" && draft.body === "" && prevStored?.body_ref) {
          body = prevBodyString ?? await this.readBodyString(prevStored);
        } else {
          body = draft.body;
        }
        if (!staged && prevRecord && prevRecord.locator !== draft.locator) currentFiles.delete(prevRecord.locator);
        const bodyBuf = Buffer.from(body, "utf-8");
        if (!staged) currentFiles.set(draft.locator, bodyBuf);
        bodyRef = {
          bundle_id: ctx.bundleId,
          locator: draft.locator,
          size: bodyBuf.length,
          sha256: bufferSha256(bodyBuf),
        };
      }

      const historyComplete: boolean = prevStored ? prevStored.history_complete : true;

      const stored: StoredVersion = {
        ref: { layer: draft.layer, content_id: contentId, version_id: versionId },
        layer: draft.layer,
        content_id: contentId,
        locator: draft.locator,
        scope: draft.scope,
        metadata: cloneJson(draft.metadata),
        state: draft.state,
        recorded_at: recordedAt,
        sequence,
        predecessors: preds,
        change_kind: mutation.change_kind,
        history_complete: historyComplete,
        body_ref: bodyRef,
      };
      changes.push(stored);
      contents[contentId] = {
        content_id: contentId,
        layer: draft.layer,
        locator: draft.locator,
        scope: draft.scope,
        state: draft.state,
        current_version_id: versionId,
        metadata: draft.metadata,
        updated_at: recordedAt,
        updated_sequence: sequence,
        history_complete: historyComplete,
      };
      versions.push(materializeContentVersion(stored, body));

      // Source sidecar stays in step with the same bundle. The API path is
      // scene-relative (`a.md`) while the content id is the full key; keep the
      // old relation only when the bytes are unchanged and the identity
      // continues, otherwise clear it (never rebind old inputs to new bytes).
      if (prevRecord) {
        const srcLayer = draft.layer as ContentSourceLayer;
        const oldPath = sourcePathForLayer(prevRecord.layer, prevRecord.locator);
        const newPath = sourcePathForLayer(draft.layer, draft.locator);
        if (draft.state === "retired") {
          sourceMutations.push({ layer: srcLayer, from_path: oldPath });
          if (newPath !== oldPath) sourceMutations.push({ layer: srcLayer, from_path: newPath });
        } else if (prevRecord.locator !== draft.locator) {
          if (prevBodyString === body) {
            sourceMutations.push({
              layer: srcLayer,
              from_path: oldPath,
              to_path: newPath,
              content_id: draft.locator,
              content_sha256: bufferSha256(Buffer.from(body, "utf-8")),
            });
          } else {
            sourceMutations.push({ layer: srcLayer, from_path: oldPath });
          }
        } else if (prevBodyString !== body) {
          sourceMutations.push({ layer: srcLayer, from_path: newPath });
        }
      }
    }

    // Coverage: with an authoritative staged set, a changed version-bearing file
    // that no mutation covers is a silently lost update — refuse instead of
    // publishing a partial cognition set.
    if (staged) {
      for (const key of changedVersionKeys) {
        if (!coveredVersionKeys.has(key)) {
          throw new LocalContentHistoryError(
            "changed_output_not_covered",
            `staged content ${key} changed but no mutation covers it; refusing to publish an unversioned change`,
          );
        }
      }
    }

    const plannedEntries = this.buildPlannedSourceEntries(staged, opts?.sources ?? [], commit, ctx.now.getTime());
    for (const source of opts?.sources ?? []) {
      if (source.input_ids.length === 0) sourceMutations.push({ layer: source.layer, from_path: source.path });
    }
    if (opts?.sources !== undefined || sourceMutations.length > 0) {
      const nextSources = opts?.sources !== undefined
        ? planContentSourceSidecar(sourcesRaw, sourceMutations, plannedEntries, ctx.now.getTime())
        : applySourceSidecarMutations(sourcesRaw, sourceMutations, ctx.now.getTime());
      if (nextSources === null) currentFiles.delete(CONTENT_SOURCES_KEY);
      else currentFiles.set(CONTENT_SOURCES_KEY, Buffer.from(nextSources, "utf-8"));
    }

    const files: BundleFile[] = Array.from(currentFiles.entries()).map(([key, content]) => ({ key, content }));
    // The operation record carries the normalized request fingerprint so a
    // later replay can verify that the SAME request is being repeated. The
    // field is validated program data on the existing operation record, never
    // a second idempotency store.
    const operation: OperationRecord = {
      operation_id: commit.operation_id,
      payload_hash: payloadHash,
      scope: commitScope,
      sequence,
      changed: anyChanged,
      version_refs: versions.map((version) => ({ ...version.ref })),
      request_fingerprint: requestFingerprint,
    };
    const history: BundleHistoryManifest = {
      schema_version: 1,
      sequence,
      recorded_at: recordedAt,
      prev_bundle_id: ctx.currentBundleId,
      history_complete: prev.history_complete,
      scope: rootScope,
      changes: anyChanged ? changes : [],
      contents: anyChanged ? contents : cloneJson(prev.contents),
      operations: [operation],
    };
    const result: ChangeCommitResult = { changed: anyChanged, versions, sequence };
    return { files, history, result };
  }

  /**
   * The final `metadata.content_sources` view for one output, derived from the
   * SAME data that produces the source sidecar: the program-provided relation
   * (when present), otherwise the relation that genuinely survives (unchanged
   * body plus same/renamed locator), otherwise `unknown`. It never adopts a
   * caller-supplied `content_sources` value, and it returns `unknown` for a
   * retired cognition so a version leaving the read path cannot keep
   * advertising sources.
   */
  private resolveVersionSourceView(args: {
    draft: VersionMutation["draft"];
    prevRecord: ContentIndexRecord | null;
    prevBodyString: string | null;
    plannedSourceByKey: Map<string, PlannedContentSource>;
    priorSourceByKey: Map<string, ContentSourceEntry>;
    nowMs: number;
  }): Record<string, unknown> {
    const { draft, prevRecord, prevBodyString, plannedSourceByKey, priorSourceByKey, nowMs } = args;
    // Retiring removes the live sidecar, not the historical evidence belonging
    // to the cognition being retired. Ordinary reads already hide this body.
    if (draft.state === "retired") {
      return cloneJson(prevRecord?.metadata?.content_sources as Record<string, unknown> | undefined) ?? { status: "unknown" };
    }

    const newPath = sourcePathForLayer(draft.layer, draft.locator);
    const key = sourceKey(draft.layer, newPath);
    const planned = plannedSourceByKey.get(key);
    const oldPath = prevRecord ? sourcePathForLayer(prevRecord.layer, prevRecord.locator) : null;
    const priorAtOld = oldPath ? priorSourceByKey.get(sourceKey(draft.layer, oldPath)) : undefined;
    const priorAtNew = priorSourceByKey.get(key);

    const knownView = (
      entry: { input_layer: "l1" | "l2"; input_ids: string[]; retained_input_ids?: string[] },
      updatedAtMs: number,
    ): Record<string, unknown> => ({
      status: "known",
      kind: "pipeline_input",
      input_layer: entry.input_layer,
      input_ids: [...entry.input_ids],
      ...(entry.retained_input_ids && entry.retained_input_ids.length > 0
        ? { retained_input_ids: [...entry.retained_input_ids] }
        : {}),
      updated_at_ms: updatedAtMs,
    });

    if (planned) {
      if (planned.input_ids.length === 0) return { status: "unknown" };
      // A relation for a renamed output lives at the OLD path in the prior
      // sidecar; look there first so an unchanged relation keeps its timestamp.
      const prior = priorAtOld ?? priorAtNew;
      const relationChanged = !prior
        || prior.path !== planned.path
        || prior.content_id !== planned.content_id
        || prior.content_sha256 !== bufferSha256(Buffer.from(draft.body, "utf-8"))
        || JSON.stringify(prior.scope ?? {}) !== JSON.stringify(planned.scope ?? {})
        || prior.input_layer !== planned.input_layer
        || !deepEqualJson(prior.input_ids, planned.input_ids)
        || !deepEqualJson(prior.retained_input_ids ?? [], planned.retained_input_ids ?? []);
      return knownView(planned, relationChanged ? nowMs : stableSourceUpdatedAt(prevRecord, prior, nowMs));
    }

    // No program-provided relation for this output this run: retain only a
    // relation that genuinely survives (same body and a same/renamed locator),
    // otherwise clear it so no stale source is attached to changed bytes.
    const survived = prevRecord
      ? (prevRecord.locator !== draft.locator ? priorAtOld : priorAtNew)
      : undefined;
    if (prevRecord !== null && prevBodyString === draft.body && survived) {
      return knownView(survived, prevRecord.locator !== draft.locator ? nowMs : stableSourceUpdatedAt(prevRecord, survived, nowMs));
    }
    return { status: "unknown" };
  }

  /**
   * Turn the caller's program-provided source relations into validated sidecar
   * entries for exactly the outputs this commit (re)wrote.
   *
   * A relation whose `(layer, path)` does not match an ACTIVE mutation output is
   * refused: the planner never attaches an input to content this commit did not
   * touch. The declared `content_sha256` must equal the staged bytes' hash, so a
   * stale declaration can never be bound to new content.
   */
  private buildPlannedSourceEntries(
    staged: Map<string, Buffer> | undefined,
    sources: PlannedContentSource[],
    commit: ChangeCommit,
    nowMs: number,
  ): ContentSourceEntry[] {
    if (sources.length === 0) return [];
    if (!staged) {
      throw new LocalContentHistoryError(
        "invalid_mutation",
        "planned source relations require an authoritative staged content set",
      );
    }
    const locatorByKey = new Map<string, string>();
    for (const mutation of commit.mutations) {
      const draft = mutation.draft;
      if (draft.state !== "active") continue;
      locatorByKey.set(sourceKey(draft.layer, sourcePathForLayer(draft.layer, draft.locator)), draft.locator);
    }
    const entries: ContentSourceEntry[] = [];
    const seenKeys = new Set<string>();
    for (let i = 0; i < sources.length; i++) {
      const source = sources[i];
      if (source.layer !== "l2" && source.layer !== "l3") {
        throw new LocalContentHistoryError("invalid_mutation", `planned source ${i}: layer must be "l2" or "l3"`);
      }
      if (typeof source.path !== "string" || source.path.length === 0) {
        throw new LocalContentHistoryError("invalid_mutation", `planned source ${i}: path must be a non-empty string`);
      }
      const key = sourceKey(source.layer, source.path);
      if (seenKeys.has(key)) {
        throw new LocalContentHistoryError(
          "invalid_mutation",
          `planned source ${i}: duplicate relation for (${source.layer}, ${source.path})`,
        );
      }
      seenKeys.add(key);
      if (typeof source.content_id !== "string" || source.content_id.length === 0) {
        throw new LocalContentHistoryError("invalid_mutation", `planned source ${i}: content_id must be a non-empty string`);
      }
      if (
        !Array.isArray(source.input_ids)
        || source.input_ids.some((id) => typeof id !== "string" || id.length === 0)
      ) {
        throw new LocalContentHistoryError(
          "invalid_mutation",
          `planned source ${i}: input_ids must be an array of non-empty strings`,
        );
      }
      if (source.scope !== undefined && (typeof source.scope !== "object" || source.scope === null || Array.isArray(source.scope))) {
        throw new LocalContentHistoryError("invalid_mutation", `planned source ${i}: scope must be an object when present`);
      }
      if (source.content_sha256 !== undefined && !/^[0-9a-f]{64}$/.test(source.content_sha256)) {
        throw new LocalContentHistoryError(
          "invalid_mutation",
          `planned source ${i}: content_sha256 must be a 64-char lowercase hex string`,
        );
      }
      const locator = locatorByKey.get(key);
      if (!locator) {
        throw new LocalContentHistoryError(
          "source_plan_mismatch",
          `planned source ${i} (${source.layer}, ${source.path}) does not match any output in this commit`,
        );
      }
      const bytes = staged.get(locator);
      if (!bytes) {
        throw new LocalContentHistoryError(
          "staged_content_missing",
          `planned source ${i} has no staged content at ${locator}`,
        );
      }
      const sha = bufferSha256(bytes);
      if (source.content_sha256 && source.content_sha256 !== sha) {
        throw new LocalContentHistoryError(
          "source_plan_mismatch",
          `planned source ${i} declares content_sha256 ${source.content_sha256} but the staged bytes hash to ${sha}`,
        );
      }
      // An explicitly empty list clears the relation, rather than leaving a
      // known sidecar with no evidence. Validation above still applies.
      if (source.input_ids.length === 0) continue;
      entries.push({
        schema_version: 1,
        layer: source.layer,
        path: source.path,
        content_id: source.content_id,
        source_kind: "pipeline_input",
        input_layer: source.input_layer,
        input_ids: [...source.input_ids],
        ...(source.retained_input_ids && source.retained_input_ids.length > 0
          ? { retained_input_ids: [...source.retained_input_ids] }
          : {}),
        scope: source.scope ?? {},
        updated_at_ms: nowMs,
        content_sha256: sha,
      });
    }
    return entries;
  }

  // ── reads ──────────────────────────────────────────────────────

  private async loadChain(): Promise<{ chain: BundleHistoryManifest[]; pointer: Awaited<ReturnType<typeof readCurrentPointer>> }> {
    const pointer = await readCurrentPointer(this.rootDir);
    if (!pointer?.history) return { chain: [], pointer: null };
    return { chain: await this.walkChainFrom(pointer.bundleId), pointer };
  }

  private async walkChainFrom(bundleId: string): Promise<BundleHistoryManifest[]> {
    const chain: BundleHistoryManifest[] = [];
    const seen = new Set<string>();
    let id: string | null = bundleId;
    while (id) {
      if (seen.has(id)) {
        throw new LocalContentHistoryError("history_cycle", `history chain revisits bundle ${id}`);
      }
      seen.add(id);
      if (seen.size > MAX_HISTORY_CHAIN) {
        throw new LocalContentHistoryError("history_too_deep", `history chain exceeds ${MAX_HISTORY_CHAIN} bundles`);
      }
      const manifest = await this.cacheGet(id);
      if (!manifest) {
        throw new LocalContentHistoryError("history_manifest_missing", `bundle ${id} has no valid history manifest`);
      }
      chain.push(manifest);
      id = manifest.prev_bundle_id ?? null;
    }
    for (let i = 1; i < chain.length; i++) {
      if (!(chain[i].sequence < chain[i - 1].sequence)) {
        throw new LocalContentHistoryError(
          "history_sequence_not_monotonic",
          `bundle ${chain[i - 1].prev_bundle_id} does not precede ${chain[i - 1].recorded_at}`,
        );
      }
    }
    return chain;
  }

  private async cacheGet(bundleId: string): Promise<BundleHistoryManifest | null> {
    if (this.cache.has(bundleId)) return this.cache.get(bundleId) ?? null;
    const manifest = await readBundleHistory(this.rootDir, bundleId);
    this.cache.set(bundleId, manifest);
    return manifest;
  }

  private findVersion(
    chain: BundleHistoryManifest[],
    query: HistoryRead,
    upper: number,
  ): StoredVersion | null {
    const visible = (version: StoredVersion): boolean =>
      version.layer === query.layer
      && version.content_id === query.content_id
      && version.sequence <= upper
      && scopeEquals(version.scope, query.scope);

    if (query.version_id) {
      for (const manifest of chain) {
        for (const version of manifest.changes) {
          if (visible(version) && version.ref.version_id === query.version_id) return version;
        }
      }
      return null;
    }

    let best: StoredVersion | null = null;
    for (const manifest of chain) {
      for (const version of manifest.changes) {
        if (!visible(version)) continue;
        if (query.as_of && version.recorded_at > query.as_of) continue;
        if (!best || compareVersions(version, best) > 0) best = version;
      }
    }
    return best;
  }

  private async readBodyString(stored: StoredVersion): Promise<string> {
    const bodyRef = stored.body_ref;
    if (!bodyRef) {
      throw new LocalContentHistoryError(
        "missing_version_body",
        `version ${stored.ref.version_id} has no body reference`,
      );
    }
    const object = await readBundleFile(this.rootDir, bodyRef.bundle_id, bodyRef.locator);
    if (!object) {
      throw new LocalContentHistoryError(
        "missing_version_body",
        `version ${stored.ref.version_id} body missing in bundle ${bodyRef.bundle_id} at ${bodyRef.locator}`,
      );
    }
    if (object.content.length !== bodyRef.size) {
      throw new LocalContentHistoryError(
        "missing_version_body",
        `version ${stored.ref.version_id} body in bundle ${bodyRef.bundle_id} has size ${object.content.length}, expected ${bodyRef.size}`,
      );
    }
    if (bufferSha256(object.content) !== bodyRef.sha256) {
      throw new LocalContentHistoryError(
        "body_checksum_mismatch",
        `version ${stored.ref.version_id} body in bundle ${bodyRef.bundle_id} failed SHA-256 verification`,
      );
    }
    return object.content.toString("utf-8");
  }

  private async materialize(stored: StoredVersion): Promise<ContentVersion> {
    const body = await this.readBodyString(stored);
    return materializeContentVersion(stored, body);
  }

  private async findOperation(
    bundleId: string,
    operationId: string,
    scopeKey: string,
  ): Promise<{ op: OperationRecord; chain: BundleHistoryManifest[] } | null> {
    const chain = await this.walkChainFrom(bundleId);
    for (const manifest of chain) {
      for (const op of manifest.operations) {
        if (op.operation_id === operationId && canonicalScopeKey(op.scope ?? {}) === scopeKey) {
          return { op, chain };
        }
      }
    }
    return null;
  }

  private async replayOperation(chain: BundleHistoryManifest[], op: OperationRecord): Promise<ChangeCommitResult> {
    const versions: ContentVersion[] = [];
    for (const ref of op.version_refs) {
      const stored = findStoredVersion(chain, ref);
      if (!stored) {
        throw new LocalContentHistoryError(
          "version_not_found",
          `operation ${op.operation_id} references missing version ${ref.version_id}`,
        );
      }
      versions.push(await this.materialize(stored));
    }
    return { changed: op.changed, versions, sequence: op.sequence };
  }
}

/** Program metadata captured for one baseline item's current source relation. */
function baselineSourceMetadata(entry: {
  source_kind: string;
  input_layer: string;
  input_ids: string[];
  retained_input_ids?: string[];
  updated_at_ms: number;
} | null): Record<string, unknown> {
  if (!entry) return { status: "unknown" };
  return {
    status: "known",
    kind: entry.source_kind,
    input_layer: entry.input_layer,
    input_ids: [...entry.input_ids],
    ...(entry.retained_input_ids && entry.retained_input_ids.length > 0
      ? { retained_input_ids: [...entry.retained_input_ids] }
      : {}),
    updated_at_ms: entry.updated_at_ms,
  };
}

function findStoredVersion(chain: BundleHistoryManifest[], ref: VersionRef): StoredVersion | null {
  for (const manifest of chain) {
    for (const version of manifest.changes) {
      if (
        version.ref.version_id === ref.version_id
        && version.ref.content_id === ref.content_id
        && version.layer === ref.layer
      ) {
        return version;
      }
    }
  }
  return null;
}

/** Ascending (sequence, version_id) order — version_id breaks same-sequence ties. */
function compareVersions(a: StoredVersion, b: StoredVersion): number {
  if (a.sequence !== b.sequence) return a.sequence - b.sequence;
  return a.ref.version_id < b.ref.version_id ? -1 : a.ref.version_id > b.ref.version_id ? 1 : 0;
}

function afterKeyGreater(version: StoredVersion, after: { sequence: number; version_id: string }): boolean {
  if (version.sequence !== after.sequence) return version.sequence > after.sequence;
  return version.ref.version_id > after.version_id;
}
