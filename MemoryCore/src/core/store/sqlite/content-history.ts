/**
 * SqliteContentHistory — immutable content versions, content-identity / locator
 * mapping and persisted operation de-duplication on the existing SQLite DB.
 *
 * This module does NOT open a new database and never rebuilds history from the
 * JSONL mirror or from generation logs. It extends the SAME `DatabaseSync`
 * connection that backs `VectorStore` (see `getRawDB()`), so the current row,
 * the version row, the replacement relation and the FTS/vector index rows can
 * all commit in a single transaction.
 *
 * Responsibilities (first batch of the 4.8 vertical slice):
 *   - Store every formal content publication as an immutable `ContentVersion`.
 *     Content identity (`content_id`) and version identity (`version_id`) are
 *     independent; locators (old record IDs / profile paths) are only an
 *     addressing shim mapped to the exact version they addressed.
 *   - Persist `operation_id` results so a repeated request is idempotent and a
 *     reused id with a different payload is rejected.
 *   - Serve the read contract (`HistoryReader`): `readVersion`, `listChanges`
 *     and `resolveLocator`, isolated by scope.
 *
 * One-current-truth rule
 * ----------------------
 * History never becomes a second source of truth for the *current* content of
 * the bound store. When an instance is bound to a store (see
 * {@link ContentHistoryStoreBinding}) every commit that changes state is applied
 * to the current row / FTS / vector index **inside the same transaction** via
 * that binding. Change kinds the binding cannot faithfully apply (create /
 * revise / baseline) are explicitly rejected instead of silently diverging —
 * the caller must use the store's own write entry points (`upsertL1` /
 * `upsertL0`).
 *
 * 2026-10-12 收窄（docs/memory-design-proposal.md §4.8/§5）：认识块、类型化
 * 关系、事件/适用时间、变化理由分类与改名/合并/拆分轨迹已撤掉；版本只记录
 * 正文、metadata、生命周期状态、recorded_at 与发布序号。
 *
 * See `docs/memory-design-proposal.md` §4.8 and `core/content/history-contract.ts`.
 */

import { createHash, randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
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
} from "../../content/history-contract.js";
import {
  scopeMatchesAuth,
  type AuthorizedHistoryPageQuery,
  type ContentHistoryAuthFilter,
} from "../../content/history-reader.js";
import { STORE_APPLICABLE_CHANGE_KINDS } from "../../content/history-write.js";
import type { CommittedOperationRecord } from "../types.js";

// ============================
// Errors
// ============================

/** Base error for the content-history surface. Carries a stable `code`. */
export class ContentHistoryError extends Error {
  constructor(public readonly code: string, message: string) {
    super(message);
    this.name = "ContentHistoryError";
  }
}

/**
 * Thrown when a destructive content delete is requested while content history
 * is enabled. The current `delete` semantics are not defined under history
 * (deleting would erase the only current pointer without preserving a version),
 * so instead of silently re-interpreting it as a retire/forget we reject it.
 */
export class HistoryDeleteUndefinedError extends Error {
  readonly code = "history_delete_undefined";
  constructor(public readonly operation: string) {
    super(
      `history_delete_undefined: destructive delete "${operation}" is not defined while content history is enabled; ` +
        `use an explicit retire/history mutation instead`,
    );
    this.name = "HistoryDeleteUndefinedError";
  }
}

/**
 * Thrown when content history is enabled but cannot be initialised. The store
 * must NOT fall back to a silent degraded/legacy-success mode: an enabled
 * history that cannot serve is a hard failure that the caller has to see.
 */
export class ContentHistoryInitError extends Error {
  readonly code = "history_init_failed";
  constructor(message: string) {
    super(message);
    this.name = "ContentHistoryInitError";
  }
}

// ============================
// Scope canonicalisation
// ============================
//
// A scope is an owned identity, not a query wildcard. Two scopes match only
// when they carry exactly the same dimensions. An omitted dimension and an
// empty string are both "absent" (never "any"); this prevents an empty
// dimension or a missing dimension from silently covering another owner's
// records.
//
// The scope key is a JSON array (null for an absent dimension), NOT a
// control-character join: a value containing the separator could otherwise
// collide with a different split of dimensions.

const SCOPE_DIMENSIONS = ["team_id", "user_id", "agent_id", "task_id", "session_id"] as const;

/**
 * Program metadata key carrying the NORMALIZED client-request fingerprint of a
 * commit. It is written by the gateway as program data (never user_metadata) and
 * is consumed — then removed — by the commit entry so it never becomes part of a
 * version's metadata. It travels WITH each output inside the same commit, and is
 * persisted on the operation record so a replay can be verified.
 *
 * The same literal is used by the local planner (`local-content-history.ts`).
 */
export const PROGRAM_REQUEST_METADATA_KEY = "__program_request";

/**
 * Read the program request fingerprint a commit carries (the FIRST mutation's
 * program metadata). A non-string value is an explicit input error, never
 * silently dropped.
 */
function readProgramRequestFingerprint(commit: ChangeCommit): string | null {
  for (const mutation of commit.mutations ?? []) {
    const metadata = mutation?.draft?.metadata;
    if (!isPlainObject(metadata)) continue;
    const value = metadata[PROGRAM_REQUEST_METADATA_KEY];
    if (value === undefined) continue;
    if (typeof value !== "string" || value.trim() === "") {
      throw new ContentHistoryError(
        "history_invalid_input",
        `${PROGRAM_REQUEST_METADATA_KEY} must be a non-empty string when present`,
      );
    }
    return value;
  }
  return null;
}

/**
 * Remove the program request fingerprint from every mutation draft so it is
 * never materialized into a version's metadata (a version only records content
 * semantics, not the transport request that produced it).
 */
function stripProgramRequestFingerprint(commit: ChangeCommit): void {
  for (const mutation of commit.mutations ?? []) {
    const metadata = mutation?.draft?.metadata;
    if (isPlainObject(metadata) && PROGRAM_REQUEST_METADATA_KEY in metadata) {
      delete metadata[PROGRAM_REQUEST_METADATA_KEY];
    }
  }
}

interface NormalizedScope {
  scopeKey: string;
  scopeJson: string;
  scope: HistoryScope;
}

export function canonicalScope(scope?: HistoryScope): NormalizedScope {
  if (scope !== undefined && scope !== null && !isPlainObject(scope)) {
    throw new ContentHistoryError("history_invalid_input", "scope must be a plain object");
  }
  const clean: Record<string, string> = {};
  for (const [key, value] of Object.entries(scope ?? {})) {
    if (!(SCOPE_DIMENSIONS as readonly string[]).includes(key)) {
      throw new ContentHistoryError("history_invalid_input", `unknown scope dimension "${key}"`);
    }
    if (value === undefined || value === null) continue;
    if (typeof value !== "string") {
      throw new ContentHistoryError("history_invalid_input", `scope dimension "${key}" must be a string`);
    }
    const trimmed = value.trim();
    if (trimmed) clean[key] = trimmed;
  }
  const scopeKey = JSON.stringify(SCOPE_DIMENSIONS.map((key) => clean[key] ?? null));
  return { scopeKey, scopeJson: JSON.stringify(clean), scope: clean as HistoryScope };
}

/**
 * Scope for an L1 content identity. Session is intentionally excluded: L1
 * cognition belongs to (team, user, agent, task) and cross-session de-dup
 * produces versions of the SAME identity, so a session-scoped key would split
 * one identity's history across scopes.
 */
export function l1HistoryScope(record: {
  teamId?: string; userId?: string; agentId?: string; taskId?: string;
}): HistoryScope {
  const scope: HistoryScope = {};
  if (record.teamId) scope.team_id = record.teamId;
  if (record.userId) scope.user_id = record.userId;
  if (record.agentId) scope.agent_id = record.agentId;
  if (record.taskId) scope.task_id = record.taskId;
  return scope;
}

/** Scope for an L0 raw message: a message belongs to a session as well. */
export function l0HistoryScope(record: {
  teamId?: string; userId?: string; agentId?: string; taskId?: string; sessionId?: string;
}): HistoryScope {
  const scope = l1HistoryScope(record);
  if (record.sessionId) scope.session_id = record.sessionId;
  return scope;
}

// ============================
// Small helpers
// ============================

const CHANGE_KINDS: ReadonlySet<ChangeKind> = new Set<ChangeKind>([
  "baseline", "create", "revise", "retire", "restore",
]);
const NEW_IDENTITY_KINDS: ReadonlySet<ChangeKind> = new Set<ChangeKind>([
  "baseline", "create",
]);
const LAYERS: ReadonlySet<HistoryLayer> = new Set<HistoryLayer>(["l0", "l1", "l2", "l3"]);
/**
 * Kinds a store-bound instance may apply to the current row in-transaction via
 * the narrow {@link ContentHistoryStoreBinding}. The store's formal commit entry
 * ({@link commitChanges} with `applyStoreChange`) opens the door to
 * {@link STORE_APPLICABLE_CHANGE_KINDS} instead, because there the store itself
 * applies the effect inside its own transaction.
 */
const BOUND_APPLICABLE_KINDS: ReadonlySet<ChangeKind> = new Set<ChangeKind>(["retire", "restore"]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Deterministic, key-sorted JSON used for payload hashing and no-op compare. */
export function stableStringify(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortValue);
  if (isPlainObject(value)) {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      if (value[key] !== undefined) out[key] = sortValue(value[key]);
    }
    return out;
  }
  return value;
}

function sha256(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

function isValidInstant(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "" && !Number.isNaN(Date.parse(value));
}

function newVersionId(): string {
  return `version_${randomUUID().replace(/-/g, "")}`;
}

function newContentId(): string {
  return `content_${randomUUID().replace(/-/g, "")}`;
}

// ============================
// Complete record metadata (partitioned from user metadata)
// ============================
//
// A version must record the ORIGINAL record semantics (type / priority / scene
// / timestamps / session / sources) in addition to any user metadata, and the
// baseline import and a fresh write must produce the SAME shape so that
// re-writing an imported row does not fabricate a revision just because the
// field names changed.
//
// The canonical record fields are written AFTER the user metadata so they can
// never be shadowed by a user key that happens to share a name (type, priority,
// scene_name, ...). `_legacy_import` is an annotation about the import, not part
// of the semantic identity, so it is ignored by the no-op comparison.

interface L1RecordSemantics {
  type: string;
  priority: number;
  sceneName: string;
  timestamps: string[];
  sessionKey: string;
  sessionId: string;
  taskId: string;
  sourceMessageIds: string[];
}

export function buildL1VersionMetadata(
  userMetadata: Record<string, unknown> | null | undefined,
  semantics: L1RecordSemantics,
  legacyImport?: Record<string, unknown>,
): Record<string, unknown> {
  const meta: Record<string, unknown> = isPlainObject(userMetadata) ? { ...userMetadata } : {};
  // Preserve every user key verbatim in a separate namespace, including keys
  // named type/version/user_metadata. Canonical fields never overwrite this copy.
  meta.user_metadata = isPlainObject(userMetadata) ? JSON.parse(JSON.stringify(userMetadata)) : {};
  meta.type = semantics.type;
  meta.priority = semantics.priority;
  meta.scene_name = semantics.sceneName;
  meta.timestamps = semantics.timestamps;
  meta.session_key = semantics.sessionKey;
  meta.session_id = semantics.sessionId;
  meta.task_id = semantics.taskId;
  meta.source_message_ids = semantics.sourceMessageIds;
  if (legacyImport) meta._legacy_import = legacyImport;
  return meta;
}

interface L0RecordSemantics {
  role: string;
  /** 消息来源：conversation（默认）或 framework_event（框架事件）。 */
  source: string;
  sessionKey: string;
  sessionId: string;
  taskId: string;
  messageRecordedAt: string;
  originalTimestampMs: number;
}

export function buildL0VersionMetadata(
  userMetadata: Record<string, unknown> | null | undefined,
  semantics: L0RecordSemantics,
  legacyImport?: Record<string, unknown>,
): Record<string, unknown> {
  const meta: Record<string, unknown> = isPlainObject(userMetadata) ? { ...userMetadata } : {};
  meta.user_metadata = isPlainObject(userMetadata) ? JSON.parse(JSON.stringify(userMetadata)) : {};
  meta.role = semantics.role;
  meta.source = semantics.source;
  meta.session_key = semantics.sessionKey;
  meta.session_id = semantics.sessionId;
  meta.task_id = semantics.taskId;
  meta.message_recorded_at = semantics.messageRecordedAt;
  meta.original_timestamp_ms = semantics.originalTimestampMs;
  if (legacyImport) meta._legacy_import = legacyImport;
  return meta;
}

/** System/volatile keys that must not participate in the no-op comparison. */
const VOLATILE_METADATA_KEYS: ReadonlySet<string> = new Set([
  "_legacy_import", "updatedAt", "updated_at", "record_version", "version",
]);

function comparableMetadata(meta: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(meta)) {
    if (VOLATILE_METADATA_KEYS.has(key)) continue;
    out[key] = meta[key];
  }
  return out;
}

// ============================
// Row shapes
// ============================

interface VersionRow {
  version_id: string;
  content_id: string;
  layer: string;
  scope_key: string;
  scope_json: string;
  sequence: number;
  recorded_at: string;
  locator: string;
  body: string;
  metadata_json: string;
  state: string;
  predecessors_json: string;
  change_kind: string;
  history_complete: number;
  created_at_ms: number;
}

interface CurrentRow {
  content_id: string;
  scope_key: string;
  layer: string;
  current_version_id: string;
  sequence: number;
  state: string;
  history_complete: number;
}

interface LocatorRow {
  content_id: string;
  version_id: string;
  sequence: number;
}

interface OperationRow {
  payload_hash: string;
  result_json: string;
  sequence: number;
  request_fingerprint?: string | null;
}

// ============================
// Store binding
// ============================

/**
 * A store-side effect of a journaled version. `retire` removes the locator from
 * the current row + recall indexes (the version stays in `content_versions`);
 * `restore` rebuilds the current row + indexes from the version.
 */
export type ContentHistoryStoreEffect =
  | { kind: "retire"; version: ContentVersion }
  | { kind: "restore"; version: ContentVersion };

/** One exact predecessor version, with the locator + body at commit time. */
export interface AppliedHistoryPredecessor {
  ref: VersionRef;
  locator: string;
  body: string;
}

/**
 * A freshly persisted history version plus the store-side effect it implies.
 *
 * Handed to the store's `applyStoreChange` callback INSIDE the same SQLite
 * transaction, so the current row / FTS / vector index and the immutable
 * version commit together (or roll back together). `predecessors` carries the
 * exact locator + body of every version this change consumes/supersedes so the
 * store can verify its current row matches before deleting/overwriting it.
 */
export interface AppliedHistoryChange {
  change_kind: ChangeKind;
  version: ContentVersion;
  predecessors: AppliedHistoryPredecessor[];
}

/**
 * Registered by a store co-tenant so a bound `commitChanges` can apply its
 * effect to the SAME current row / FTS / vector rows inside the SAME open
 * transaction. Implementations MUST throw (never half-write) when they cannot
 * faithfully apply the effect.
 */
export interface ContentHistoryStoreBinding {
  applyHistoryEffect(effect: ContentHistoryStoreEffect): void;
}

// ============================
// Options
// ============================

export interface ContentHistoryOptions {
  /**
   * Program clock. `recorded_at` is always the program's commit time; callers
   * (HTTP / model) can never provide it. Returns an ISO string / Date / epoch ms.
   */
  clock?: () => string | Date | number;
  /**
   * Optional store co-tenant. When present the instance is "bound": direct
   * commits that cannot be applied to the store's current row are rejected.
   */
  storeBinding?: ContentHistoryStoreBinding;
}

// ============================
// Store-write inputs (used by VectorStore inside its own transaction)
// ============================

export interface StoreContentWriteInput {
  layer: "l0" | "l1";
  locator: string;
  scope: HistoryScope;
  body: string;
  metadata: Record<string, unknown>;
  state?: "active" | "retired";
}

// ============================
// Plan result
// ============================

type MutationPlan =
  | { kind: "noop"; version: ContentVersion }
  | {
      kind: "new";
      contentId: string;
      draft: VersionMutation["draft"];
      scopeKey: string;
      scopeJson: string;
      predecessors: VersionRef[];
      historyComplete: boolean;
      changeKind: ChangeKind;
    };

// ============================
// SqliteContentHistory
// ============================

export class SqliteContentHistory implements HistoryReader {
  private readonly db: DatabaseSync;
  private readonly options: ContentHistoryOptions;
  /** Cached capability probe: whether this SQLite build can `json_extract`. */
  private jsonExtractAvailable: boolean | undefined;

  constructor(
    db: DatabaseSync,
    clockOrOptions?: (() => string | Date | number) | ContentHistoryOptions,
    maybeOptions?: ContentHistoryOptions,
  ) {
    this.db = db;
    if (typeof clockOrOptions === "function") {
      this.options = { ...(maybeOptions ?? {}), clock: clockOrOptions };
    } else {
      this.options = clockOrOptions ?? {};
    }
    this.ensureSchema();
  }

  // ── Schema ────────────────────────────────────────────────

  ensureSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS content_history_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS content_versions (
        version_id        TEXT PRIMARY KEY,
        content_id        TEXT NOT NULL,
        layer             TEXT NOT NULL,
        scope_key         TEXT NOT NULL,
        scope_json        TEXT NOT NULL,
        sequence          INTEGER NOT NULL,
        recorded_at       TEXT NOT NULL,
        locator           TEXT NOT NULL,
        body              TEXT NOT NULL,
        metadata_json     TEXT NOT NULL DEFAULT '{}',
        state             TEXT NOT NULL DEFAULT 'active',
        predecessors_json TEXT NOT NULL DEFAULT '[]',
        change_kind       TEXT NOT NULL,
        history_complete  INTEGER NOT NULL DEFAULT 1,
        created_at_ms     INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_content_versions_identity
        ON content_versions(content_id, sequence, version_id);
      CREATE INDEX IF NOT EXISTS idx_content_versions_scope
        ON content_versions(scope_key, layer, sequence, version_id);
      CREATE INDEX IF NOT EXISTS idx_content_versions_locator
        ON content_versions(scope_key, layer, locator);
      CREATE INDEX IF NOT EXISTS idx_content_versions_recorded
        ON content_versions(scope_key, recorded_at);

      CREATE TABLE IF NOT EXISTS content_current (
        content_id         TEXT PRIMARY KEY,
        scope_key          TEXT NOT NULL,
        layer              TEXT NOT NULL,
        current_version_id TEXT NOT NULL,
        sequence           INTEGER NOT NULL,
        state              TEXT NOT NULL,
        history_complete   INTEGER NOT NULL DEFAULT 1
      );
      CREATE INDEX IF NOT EXISTS idx_content_current_scope
        ON content_current(scope_key, layer);

      CREATE TABLE IF NOT EXISTS content_locators (
        scope_key  TEXT NOT NULL,
        layer      TEXT NOT NULL,
        locator    TEXT NOT NULL,
        content_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        sequence   INTEGER NOT NULL,
        PRIMARY KEY (scope_key, layer, locator)
      );
      CREATE INDEX IF NOT EXISTS idx_content_locators_content
        ON content_locators(content_id);

      CREATE TABLE IF NOT EXISTS content_history_operations (
        scope_key     TEXT NOT NULL,
        operation_id  TEXT NOT NULL,
        payload_hash  TEXT NOT NULL,
        result_json   TEXT NOT NULL,
        sequence      INTEGER NOT NULL,
        request_fingerprint TEXT,
        created_at_ms INTEGER NOT NULL,
        PRIMARY KEY (scope_key, operation_id)
      );
    `);

    // Online migration for databases created before the request fingerprint was
    // recorded. The plain ADD COLUMN is idempotent-safe behind try/catch.
    try {
      this.db.exec("ALTER TABLE content_history_operations ADD COLUMN request_fingerprint TEXT");
    } catch {
      /* column already exists */
    }
  }

  /** Persist that history is enabled so a restart cannot silently drop it. */
  markEnabled(): void {
    this.setMeta("history_enabled", "1");
  }

  // ── Clock / sequence ──────────────────────────────────────

  private now(): string {
    const raw = this.options.clock ? this.options.clock() : new Date().toISOString();
    if (typeof raw === "string") return raw;
    if (raw instanceof Date) return raw.toISOString();
    if (typeof raw === "number") return new Date(raw).toISOString();
    return new Date().toISOString();
  }

  private nowMs(): number {
    return Date.now();
  }

  private getSequence(): number {
    const row = this.db
      .prepare("SELECT value FROM content_history_meta WHERE key = 'sequence'")
      .get() as { value: string } | undefined;
    return row ? Number(row.value) || 0 : 0;
  }

  private nextSequence(): number {
    const next = this.getSequence() + 1;
    this.setMeta("sequence", String(next));
    return next;
  }

  private setMeta(key: string, value: string): void {
    this.db
      .prepare(
        "INSERT INTO content_history_meta (key, value) VALUES (?, ?) " +
          "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      )
      .run(key, value);
  }

  private getMeta(key: string): string | null {
    const row = this.db
      .prepare("SELECT value FROM content_history_meta WHERE key = ?")
      .get(key) as { value: string } | undefined;
    return row?.value ?? null;
  }

  /**
   * Track whether the program clock ever went backwards. We do NOT falsify a
   * rolled-back `recorded_at` (sequence keeps the true commit order), but any
   * later `as_of` read is explicitly refused because the wall-clock cut would
   * be ambiguous.
   */
  private noteRecordedAt(recordedAt: string): void {
    const last = this.getMeta("last_recorded_at");
    if (last && recordedAt < last) {
      this.setMeta("clock_regressed", "1");
      return;
    }
    if (!last || recordedAt > last) this.setMeta("last_recorded_at", recordedAt);
  }

  // ── Row helpers ───────────────────────────────────────────

  private getVersionRow(versionId: string): VersionRow | undefined {
    return this.db
      .prepare("SELECT * FROM content_versions WHERE version_id = ?")
      .get(versionId) as VersionRow | undefined;
  }

  private getCurrentRow(contentId: string): CurrentRow | undefined {
    return this.db
      .prepare("SELECT * FROM content_current WHERE content_id = ?")
      .get(contentId) as CurrentRow | undefined;
  }

  private getLocatorRow(layer: HistoryLayer, locator: string, scopeKey: string): LocatorRow | undefined {
    return this.db
      .prepare(
        "SELECT content_id, version_id, sequence FROM content_locators WHERE scope_key = ? AND layer = ? AND locator = ?",
      )
      .get(scopeKey, layer, locator) as LocatorRow | undefined;
  }

  /** Strict decode: corrupt JSON / unknown state / non-integer sequence is an error. */
  private rowToVersion(row: VersionRow): ContentVersion {
    try {
      if (!LAYERS.has(row.layer as HistoryLayer)) throw new Error(`invalid layer "${row.layer}"`);
      if (row.state !== "active" && row.state !== "retired") throw new Error(`invalid state "${row.state}"`);
      if (row.history_complete !== 0 && row.history_complete !== 1) {
        throw new Error(`invalid history_complete "${row.history_complete}"`);
      }
      if (!Number.isInteger(row.sequence) || row.sequence < 0) throw new Error(`invalid sequence "${row.sequence}"`);
      if (!CHANGE_KINDS.has(row.change_kind as ChangeKind)) throw new Error(`invalid change_kind "${row.change_kind}"`);

      const scope = JSON.parse(row.scope_json) as unknown;
      if (!isPlainObject(scope)) throw new Error("scope_json is not an object");
      const metadata = JSON.parse(row.metadata_json) as unknown;
      if (!isPlainObject(metadata)) throw new Error("metadata_json is not an object");
      const predecessors = JSON.parse(row.predecessors_json) as unknown;
      if (!Array.isArray(predecessors)) throw new Error("predecessors_json is not an array");

      return {
        layer: row.layer as HistoryLayer,
        content_id: row.content_id,
        locator: row.locator,
        scope: scope as HistoryScope,
        body: row.body,
        metadata: metadata as Record<string, unknown>,
        state: row.state,
        ref: { layer: row.layer as HistoryLayer, content_id: row.content_id, version_id: row.version_id },
        recorded_at: row.recorded_at,
        sequence: row.sequence,
        predecessors: predecessors as VersionRef[],
        change_kind: row.change_kind as ChangeKind,
        history_complete: row.history_complete === 1,
      };
    } catch (err) {
      throw new ContentHistoryError(
        "history_corrupt",
        `failed to decode content version ${row.version_id}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  private insertVersionRow(version: ContentVersion, scopeKey: string, scopeJson: string): void {
    this.db
      .prepare(`
        INSERT INTO content_versions (
          version_id, content_id, layer, scope_key, scope_json, sequence, recorded_at,
          locator, body, metadata_json, state, predecessors_json,
          change_kind, history_complete, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
      .run(
        version.ref.version_id,
        version.content_id,
        version.layer,
        scopeKey,
        scopeJson,
        version.sequence,
        version.recorded_at,
        version.locator,
        version.body,
        JSON.stringify(version.metadata ?? {}),
        version.state,
        JSON.stringify(version.predecessors ?? []),
        version.change_kind,
        version.history_complete ? 1 : 0,
        this.nowMs(),
      );
  }

  private upsertLocator(layer: HistoryLayer, locator: string, scopeKey: string, contentId: string, versionId: string, sequence: number): void {
    this.db
      .prepare(`
        INSERT INTO content_locators (scope_key, layer, locator, content_id, version_id, sequence)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope_key, layer, locator) DO UPDATE SET
          content_id = excluded.content_id,
          version_id = excluded.version_id,
          sequence   = excluded.sequence
      `)
      .run(scopeKey, layer, locator, contentId, versionId, sequence);
  }

  private updateCurrent(
    contentId: string,
    scopeKey: string,
    layer: HistoryLayer,
    versionId: string,
    sequence: number,
    state: "active" | "retired",
    historyComplete: boolean,
  ): void {
    this.db
      .prepare(`
        INSERT INTO content_current (content_id, scope_key, layer, current_version_id, sequence, state, history_complete)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_id) DO UPDATE SET
          scope_key = excluded.scope_key,
          layer = excluded.layer,
          current_version_id = excluded.current_version_id,
          sequence = excluded.sequence,
          state = excluded.state,
          history_complete = excluded.history_complete
      `)
      .run(contentId, scopeKey, layer, versionId, sequence, state, historyComplete ? 1 : 0);
  }

  private isRetired(contentId: string): boolean {
    const current = this.getCurrentRow(contentId);
    if (!current) return false;
    return current.state === "retired";
  }

  /** Remove recall index rows for a locator (the immutable version is kept). */
  private evictRecall(layer: HistoryLayer, locator: string): void {
    if (layer === "l1") {
      if (this.tableExists("l1_fts")) this.db.prepare("DELETE FROM l1_fts WHERE record_id = ?").run(locator);
      if (this.tableExists("l1_vec")) this.db.prepare("DELETE FROM l1_vec WHERE record_id = ?").run(locator);
    } else if (layer === "l0") {
      if (this.tableExists("l0_fts")) this.db.prepare("DELETE FROM l0_fts WHERE record_id = ?").run(locator);
      if (this.tableExists("l0_vec")) this.db.prepare("DELETE FROM l0_vec WHERE record_id = ?").run(locator);
    }
  }

  private tableExists(name: string): boolean {
    const row = this.db
      .prepare("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ?")
      .get(name) as { ok: number } | undefined;
    return !!row;
  }

  // ── commitChanges ─────────────────────────────────────────

  /**
   * Apply a commit of one or more mutations in a single scope.
   *
   * `options.inTransaction` is for co-tenants that already hold a SQLite
   * transaction — no nested BEGIN/COMMIT is issued.
   *
   * When bound to a store, only change kinds the binding can apply to the
   * current row/index in-transaction (retire / restore) are accepted; every
   * other kind is refused so history can never diverge from the current truth.
   */
  commitChanges(
    commit: ChangeCommit,
    options?: {
      inTransaction?: boolean;
      /**
       * Store-facing commit: apply each new version's current-row / FTS / vector
       * effect through this callback inside the SAME open transaction. When set,
       * every {@link STORE_APPLICABLE_CHANGE_KINDS} kind is accepted (the store
       * is the authority that applies it); the legacy narrow
       * {@link ContentHistoryStoreBinding} is then bypassed. When absent, the
       * historic behavior is preserved: an unbound instance accepts every kind
       * but applies nothing, a bound instance only accepts retire/restore.
       */
      applyStoreChange?: (change: AppliedHistoryChange) => void;
    },
  ): ChangeCommitResult {
    if (!isPlainObject(commit) || typeof commit.operation_id !== "string" || commit.operation_id.trim() === "") {
      throw new ContentHistoryError("history_invalid_input", "commit.operation_id must be a non-empty string");
    }
    if (!Array.isArray(commit.mutations) || commit.mutations.length === 0) {
      throw new ContentHistoryError("history_invalid_input", "commit.mutations must be a non-empty array");
    }

    // Stripping program-only fields must not mutate a caller-owned replay.
    commit = JSON.parse(JSON.stringify(commit)) as ChangeCommit;
    const scopeKeys = new Set(commit.mutations.map((m) => canonicalScope(m?.draft?.scope).scopeKey));
    if (scopeKeys.size !== 1) {
      throw new ContentHistoryError("history_scope_mismatch", "a single commit may only touch one scope");
    }
    const scopeKey = [...scopeKeys][0];
    this.assertNoBatchCollisions(commit.mutations);

    const payloadHash = sha256(
      stableStringify({
        actor: commit.actor,
        mutations: commit.mutations.map((m) => ({
          draft: m.draft,
          expected_version_id: m.expected_version_id,
          predecessors: m.predecessors,
          change_kind: m.change_kind,
        })),
      }),
    );

    // The program request fingerprint travels in the commit's program metadata
    // (never user_metadata): it is bound into the payload hash (so a different
    // request can never masquerade as the same one) and stored on the operation
    // record (so a later replay can be verified), then stripped so it never
    // becomes a version's metadata.
    const requestFingerprint = readProgramRequestFingerprint(commit);
    stripProgramRequestFingerprint(commit);

    const applyStoreChange = options?.applyStoreChange;
    const allowedKinds: ReadonlySet<ChangeKind> | null = applyStoreChange
      ? STORE_APPLICABLE_CHANGE_KINDS
      : this.options.storeBinding
        ? BOUND_APPLICABLE_KINDS
        : null;

    const run = (): ChangeCommitResult => {
      // Operation de-duplication is validated INSIDE the transaction: a
      // concurrent/duplicate insert must never overwrite an idempotent record.
      const existingOp = this.getOperation(scopeKey, commit.operation_id);
      if (existingOp) {
        if (existingOp.payload_hash !== payloadHash) {
          throw new ContentHistoryError(
            "history_operation_conflict",
            `operation_id "${commit.operation_id}" was already used with a different payload`,
          );
        }
        return JSON.parse(existingOp.result_json) as ChangeCommitResult;
      }

      const plans = commit.mutations.map((mutation) => this.planMutation(mutation, allowedKinds));
      const hasNew = plans.some((plan) => plan.kind === "new");
      const sequence = hasNew ? this.nextSequence() : this.getSequence();
      const recordedAt = this.now();
      if (hasNew) this.noteRecordedAt(recordedAt);

      const versions: ContentVersion[] = [];
      for (const plan of plans) {
        if (plan.kind === "noop") {
          versions.push(plan.version);
          continue;
        }
        const version = this.materializeVersion({
          contentId: plan.contentId,
          layer: plan.draft.layer,
          scope: plan.draft.scope,
          scopeKey: plan.scopeKey,
          scopeJson: plan.scopeJson,
          sequence,
          recordedAt,
          draft: plan.draft,
          predecessors: plan.predecessors,
          changeKind: plan.changeKind,
          historyComplete: plan.historyComplete,
        });
        this.persistVersion(version, plan.scopeKey, plan.scopeJson);
        // Store-facing commit: hand the exact effect (new version + the exact
        // predecessors it consumes) to the store so it applies the current row /
        // FTS / vector change in the SAME open transaction.
        if (applyStoreChange) {
          applyStoreChange({
            change_kind: plan.changeKind,
            version,
            predecessors: plan.predecessors.map((ref) => {
              const row = this.getVersionRow(ref.version_id);
              return { ref: { ...ref }, locator: row?.locator ?? "", body: row?.body ?? "" };
            }),
          });
        } else if (this.options.storeBinding) {
          // Legacy narrow bound path: only retire/restore reach here.
          this.options.storeBinding.applyHistoryEffect(
            plan.changeKind === "retire" ? { kind: "retire", version } : { kind: "restore", version },
          );
        }
        versions.push(version);
      }

      const result: ChangeCommitResult = { changed: hasNew, versions, sequence };
      this.insertOperation(scopeKey, commit.operation_id, payloadHash, result, requestFingerprint);
      return result;
    };

    if (options?.inTransaction) return run();

    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = run();
      this.db.exec("COMMIT");
      return result;
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        /* ignore rollback error */
      }
      throw err;
    }
  }

  /** A batch may not target the same locator or the same identity twice. */
  private assertNoBatchCollisions(mutations: VersionMutation[]): void {
    const seenLocators = new Set<string>();
    const seenContentIds = new Set<string>();
    for (const mutation of mutations) {
      const draft = mutation?.draft;
      if (!isPlainObject(draft)) continue;
      const layer = typeof draft.layer === "string" ? draft.layer : "";
      const locator = typeof draft.locator === "string" ? draft.locator.trim() : "";
      if (layer && locator) {
        const key = `${layer}\u0000${locator}`;
        if (seenLocators.has(key)) {
          throw new ContentHistoryError(
            "history_invalid_input",
            `commit touches locator "${locator}" of layer ${layer} more than once`,
          );
        }
        seenLocators.add(key);
      }
      const candidateId = (typeof draft.content_id === "string" && draft.content_id)
        ? draft.content_id
        : (Array.isArray(mutation?.predecessors) ? mutation.predecessors[0]?.content_id : undefined);
      if (typeof candidateId === "string" && candidateId) {
        if (seenContentIds.has(candidateId)) {
          throw new ContentHistoryError(
            "history_invalid_input",
            `commit touches content identity ${candidateId} more than once against the same baseline`,
          );
        }
        seenContentIds.add(candidateId);
      }
    }
  }

  private getOperation(scopeKey: string, operationId: string): OperationRow | undefined {
    return this.db
      .prepare(
        "SELECT payload_hash, result_json, sequence, request_fingerprint " +
          "FROM content_history_operations WHERE scope_key = ? AND operation_id = ?",
      )
      .get(scopeKey, operationId) as OperationRow | undefined;
  }

  /**
   * Read one committed operation from THIS store's own formal chain.
   *
   * Returns the exact `ChangeCommitResult` the first commit produced plus the
   * normalized request fingerprint it was assembled for. A read-only lookup: it
   * opens no second idempotency store, keeps no process map and never scans a
   * log. `null` covers both "absent" and "not in this scope".
   */
  getCommittedOperation(scope: HistoryScope, operationId: string): CommittedOperationRecord | null {
    if (typeof operationId !== "string" || operationId.trim() === "") {
      throw new ContentHistoryError("history_invalid_input", "operation_id must be a non-empty string");
    }
    const { scopeKey, scope: canonical } = canonicalScope(scope);
    try {
      const row = this.getOperation(scopeKey, operationId);
      if (!row) return null;
      return {
        operation_id: operationId,
        scope: canonical,
        request_fingerprint: typeof row.request_fingerprint === "string" ? row.request_fingerprint : null,
        result: JSON.parse(row.result_json) as ChangeCommitResult,
      };
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `getCommittedOperation failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Insert an idempotency record. A plain INSERT (no ON CONFLICT update) so a
   * duplicate can never overwrite an already-recorded result.
   */
  private insertOperation(
    scopeKey: string,
    operationId: string,
    payloadHash: string,
    result: ChangeCommitResult,
    requestFingerprint: string | null,
  ): void {
    this.db
      .prepare(`
        INSERT INTO content_history_operations (scope_key, operation_id, payload_hash, result_json, sequence, request_fingerprint, created_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `)
      .run(scopeKey, operationId, payloadHash, JSON.stringify(result), result.sequence, requestFingerprint, this.nowMs());
  }

  /** Resolve and validate an exact predecessor (triple + scope + current version). */
  private resolvePredecessor(ref: VersionRef, layer: HistoryLayer, scopeKey: string): VersionRow {
    if (!isPlainObject(ref) || typeof ref.version_id !== "string" || ref.version_id === "") {
      throw new ContentHistoryError("history_invalid_input", "predecessor must carry a version_id");
    }
    const row = this.getVersionRow(ref.version_id);
    if (!row) {
      throw new ContentHistoryError("history_invalid_input", `predecessor version not found: ${ref.version_id}`);
    }
    if (row.layer !== layer) {
      throw new ContentHistoryError("history_invalid_input", "predecessor layer does not match the draft layer");
    }
    if (row.content_id !== ref.content_id) {
      throw new ContentHistoryError(
        "history_invalid_input",
        `predecessor content_id mismatch: ${ref.content_id} does not own ${ref.version_id}`,
      );
    }
    if (row.scope_key !== scopeKey) {
      throw new ContentHistoryError("history_scope_mismatch", "predecessor belongs to a different scope");
    }
    const current = this.getCurrentRow(row.content_id);
    if (!current || current.current_version_id !== row.version_id) {
      throw new ContentHistoryError(
        "history_predecessor_not_current",
        `predecessor ${ref.version_id} is not the current version of ${row.content_id}`,
      );
    }
    if (current.layer !== layer) {
      throw new ContentHistoryError("history_invalid_input", "predecessor identity layer does not match the draft layer");
    }
    return row;
  }

  private planMutation(mutation: VersionMutation, allowedKinds?: ReadonlySet<ChangeKind> | null): MutationPlan {
    if (!isPlainObject(mutation) || !isPlainObject(mutation.draft)) {
      throw new ContentHistoryError("history_invalid_input", "mutation.draft is required");
    }
    const draft = JSON.parse(JSON.stringify(mutation.draft)) as VersionMutation["draft"];
    this.validateDraft(draft);
    const kind = mutation.change_kind;
    if (!CHANGE_KINDS.has(kind)) {
      throw new ContentHistoryError("history_invalid_input", `unknown change_kind "${String(kind)}"`);
    }
    if (allowedKinds && !allowedKinds.has(kind)) {
      throw new ContentHistoryError(
        "history_bound_commit_unsupported",
        `commit of change_kind "${kind}" is not supported by this writer; the current row and its ` +
          `FTS/vector index cannot be updated from history alone — use the store write entry points ` +
          `(commitContentChanges / upsertL1 / upsertL0)`,
      );
    }

    const { scopeKey, scopeJson } = canonicalScope(draft.scope);
    const predecessors = mutation.predecessors ?? [];
    if (!Array.isArray(predecessors)) {
      throw new ContentHistoryError("history_invalid_input", "mutation.predecessors must be an array");
    }

    if ((kind === "retire" && draft.state !== "retired") || (kind !== "retire" && draft.state !== "active")) {
      throw new ContentHistoryError("history_invalid_input", "change kind and lifecycle state disagree");
    }
    if (NEW_IDENTITY_KINDS.has(kind)) {
      if (mutation.expected_version_id !== null && mutation.expected_version_id !== undefined) {
        throw new ContentHistoryError(
          "history_invalid_input",
          "expected_version_id must be null when creating a new content identity",
        );
      }
      for (const ref of predecessors) this.resolvePredecessor(ref, draft.layer, scopeKey);
      // A new identity must not collide with an existing locator of the scope.
      if (this.getLocatorRow(draft.layer, draft.locator.trim(), scopeKey)) {
        throw new ContentHistoryError(
          "history_locator_conflict",
          `locator "${draft.locator}" already maps to a content identity in this scope`,
        );
      }
      let contentId: string;
      if (draft.content_id) {
        if (this.getCurrentRow(draft.content_id) || this.getVersionRow(draft.content_id)) {
          throw new ContentHistoryError(
            "history_invalid_input",
            `content_id "${draft.content_id}" already exists; new identity requires a fresh id`,
          );
        }
        contentId = draft.content_id;
      } else {
        contentId = newContentId();
      }
      const historyComplete = kind === "baseline"
        ? false
        : predecessors.every((ref) => this.versionComplete(ref.version_id));
      return {
        kind: "new",
        contentId,
        draft,
        scopeKey,
        scopeJson,
        predecessors: [...predecessors],
        historyComplete,
        changeKind: kind,
      };
    }

    // Identity-preserving kinds: revise / retire / restore
    const contentId = draft.content_id ?? predecessors[0]?.content_id;
    if (!contentId) {
      throw new ContentHistoryError("history_invalid_input", "content_id is required to revise an existing identity");
    }
    if (!mutation.expected_version_id) {
      throw new ContentHistoryError(
        "history_invalid_input",
        "expected_version_id is required when revising an existing identity",
      );
    }
    const current = this.getCurrentRow(contentId);
    if (!current) {
      throw new ContentHistoryError("history_invalid_input", `content identity not found: ${contentId}`);
    }
    if (current.scope_key !== scopeKey) {
      throw new ContentHistoryError("history_scope_mismatch", "content identity belongs to a different scope");
    }
    if (current.layer !== draft.layer) {
      throw new ContentHistoryError(
        "history_invalid_input",
        "an identity-preserving mutation may not change the layer of the identity",
      );
    }
    if (current.current_version_id !== mutation.expected_version_id) {
      throw new ContentHistoryError(
        "history_version_conflict",
        `stale write: current version of ${contentId} is ${current.current_version_id}, expected ${mutation.expected_version_id}`,
      );
    }
    if (this.isRetired(contentId) && kind !== "restore" && (draft.state ?? "active") !== "retired") {
      throw new ContentHistoryError(
        "history_retired_conflict",
        `content identity ${contentId} is retired; an explicit restore is required`,
      );
    }
    for (const ref of predecessors) {
      const row = this.resolvePredecessor(ref, draft.layer, scopeKey);
      if (row.content_id !== contentId) {
        throw new ContentHistoryError(
          "history_invalid_input",
          "identity-preserving mutation may only reference predecessors of the same content identity",
        );
      }
    }
    // A revise must not overwrite a locator that belongs to ANOTHER identity.
    const locatorOwner = this.getLocatorRow(draft.layer, draft.locator.trim(), scopeKey);
    if (locatorOwner && locatorOwner.content_id !== contentId) {
      throw new ContentHistoryError(
        "history_locator_conflict",
        `locator "${draft.locator}" already belongs to another content identity`,
      );
    }

    const currentVersionRow = this.getVersionRow(current.current_version_id);
    if (!currentVersionRow) {
      throw new ContentHistoryError("history_corrupt", `current version ${current.current_version_id} is missing`);
    }
    if (kind === "retire" || kind === "restore") {
      if (draft.locator !== currentVersionRow.locator ||
          (kind === "retire" && currentVersionRow.state !== "active") ||
          (kind === "restore" && currentVersionRow.state !== "retired")) {
        throw new ContentHistoryError("history_invalid_input", "retire/restore must keep the exact locator and perform a valid state transition");
      }
      // A lifecycle operation changes adoption, not the stored content.
      const prior = this.rowToVersion(currentVersionRow);
      Object.assign(draft, { body: prior.body, metadata: prior.metadata });
    }
    if (this.sameContent(draft, currentVersionRow)) {
      return { kind: "noop", version: this.rowToVersion(currentVersionRow) };
    }

    return {
      kind: "new",
      contentId,
      draft,
      scopeKey,
      scopeJson,
      predecessors: predecessors.length ? [...predecessors] : [
        { layer: currentVersionRow.layer as HistoryLayer, content_id: contentId, version_id: currentVersionRow.version_id },
      ],
      historyComplete: current.history_complete === 1,
      changeKind: kind,
    };
  }

  private validateDraft(draft: VersionMutation["draft"]): void {
    if (!LAYERS.has(draft.layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(draft.layer)}"`);
    }
    if (!isPlainObject(draft.scope)) {
      throw new ContentHistoryError("history_invalid_input", "draft.scope is required");
    }
    if (typeof draft.locator !== "string" || draft.locator.trim() === "") {
      throw new ContentHistoryError("history_invalid_input", "draft.locator must be a non-empty string");
    }
    if (typeof draft.body !== "string") {
      throw new ContentHistoryError("history_invalid_input", "draft.body must be a string");
    }
    if (draft.metadata !== undefined && !isPlainObject(draft.metadata)) {
      throw new ContentHistoryError("history_invalid_input", "draft.metadata must be a plain object");
    }
    if (draft.state !== undefined && draft.state !== "active" && draft.state !== "retired") {
      throw new ContentHistoryError("history_invalid_input", "draft.state must be active or retired");
    }
  }

  /** True when a new draft is a no-op relative to an existing version. */
  private sameContent(draft: VersionMutation["draft"], row: VersionRow): boolean {
    const candidate = {
      layer: draft.layer,
      locator: draft.locator.trim(),
      body: draft.body,
      metadata: comparableMetadata(isPlainObject(draft.metadata) ? draft.metadata : {}),
      state: draft.state ?? "active",
    };
    const current = {
      layer: row.layer,
      locator: row.locator,
      body: row.body,
      metadata: comparableMetadata(JSON.parse(row.metadata_json) as Record<string, unknown>),
      state: row.state,
    };
    return stableStringify(candidate) === stableStringify(current);
  }

  private materializeVersion(args: {
    contentId: string;
    layer: HistoryLayer;
    scope: HistoryScope;
    scopeKey: string;
    scopeJson: string;
    sequence: number;
    recordedAt: string;
    draft: VersionMutation["draft"];
    predecessors: VersionRef[];
    changeKind: ChangeKind;
    historyComplete: boolean;
  }): ContentVersion {
    return {
      layer: args.layer,
      content_id: args.contentId,
      locator: args.draft.locator.trim(),
      scope: canonicalScope(args.draft.scope).scope,
      body: args.draft.body,
      metadata: args.draft.metadata ?? {},
      state: args.draft.state ?? "active",
      ref: { layer: args.layer, content_id: args.contentId, version_id: newVersionId() },
      recorded_at: args.recordedAt,
      sequence: args.sequence,
      predecessors: args.predecessors,
      change_kind: args.changeKind,
      history_complete: args.historyComplete,
    };
  }

  private persistVersion(version: ContentVersion, scopeKey: string, scopeJson: string): void {
    this.insertVersionRow(version, scopeKey, scopeJson);
    this.upsertLocator(version.layer, version.locator, scopeKey, version.content_id, version.ref.version_id, version.sequence);
    this.updateCurrent(
      version.content_id,
      scopeKey,
      version.layer,
      version.ref.version_id,
      version.sequence,
      version.state,
      version.history_complete,
    );
    if (version.state === "retired") {
      this.evictRecall(version.layer, version.locator);
    }
  }

  // ── Store integration (called inside the store's own transaction) ──

  /**
   * Record an L0/L1 upsert (new record id → create; same record id → revise).
   * MUST be called inside an already-open SQLite transaction.
   */
  recordStoreWrite(input: StoreContentWriteInput): ContentVersion {
    const { scopeKey, scopeJson } = canonicalScope(input.scope);
    const state = input.state ?? "active";
    const existing = this.getLocatorRow(input.layer, input.locator, scopeKey);
    const recordedAt = this.now();

    if (existing) {
      const current = this.getVersionRow(existing.version_id);
      if (!current) {
        throw new ContentHistoryError("history_corrupt", `locator ${input.locator} points at a missing version`);
      }
      const contentCurrent = this.getCurrentRow(existing.content_id);
      if (!contentCurrent || contentCurrent.current_version_id !== existing.version_id) {
        // The locator addresses an OLD version (it was superseded by a
        // replacement). Rewriting it would either hijack the current identity
        // or overwrite the historical locator mapping, so refuse.
        throw new ContentHistoryError(
          "history_locator_superseded",
          `locator ${input.locator} no longer addresses the current version of ${existing.content_id}`,
        );
      }
      if (current.state === "retired") {
        throw new ContentHistoryError(
          "history_retired_conflict",
          `content ${existing.content_id} is retired; an explicit restore is required`,
        );
      }
      const candidate: VersionMutation["draft"] = {
        layer: input.layer,
        content_id: existing.content_id,
        locator: input.locator,
        scope: input.scope,
        body: input.body,
        metadata: input.metadata,
        state,
      };
      if (this.sameContent(candidate, current)) {
        return this.rowToVersion(current);
      }
      this.noteRecordedAt(recordedAt);
      const sequence = this.nextSequence();
      const version = this.materializeVersion({
        contentId: existing.content_id,
        layer: input.layer,
        scope: input.scope,
        scopeKey,
        scopeJson,
        sequence,
        recordedAt,
        draft: candidate,
        predecessors: [
          { layer: current.layer as HistoryLayer, content_id: current.content_id, version_id: current.version_id },
        ],
        changeKind: "revise",
        historyComplete: current.history_complete === 1,
      });
      this.persistVersion(version, scopeKey, scopeJson);
      return version;
    }

    const contentId = newContentId();
    this.noteRecordedAt(recordedAt);
    const sequence = this.nextSequence();
    const draft: VersionMutation["draft"] = {
      layer: input.layer,
      content_id: contentId,
      locator: input.locator,
      scope: input.scope,
      body: input.body,
      metadata: input.metadata,
      state,
    };
    const version = this.materializeVersion({
      contentId,
      layer: input.layer,
      scope: input.scope,
      scopeKey,
      scopeJson,
      sequence,
      recordedAt,
      draft,
      predecessors: [],
      changeKind: "create",
      historyComplete: true,
    });
    this.persistVersion(version, scopeKey, scopeJson);
    return version;
  }

  private versionComplete(versionId: string): boolean {
    const row = this.getVersionRow(versionId);
    return row ? row.history_complete === 1 : true;
  }

  /**
   * Import the current state of a pre-existing (non-empty) database as an
   * explicitly incomplete baseline. Runs once per database; the import time is
   * the baseline `recorded_at` and legacy times are kept in metadata only.
   */
  importBaseline(): { imported: boolean; l1Count: number; l0Count: number } {
    if (this.getMeta("baseline_imported") === "1") {
      return { imported: false, l1Count: 0, l0Count: 0 };
    }
    const l1Rows = this.db
      .prepare(`
        SELECT record_id, content, type, priority, scene_name, metadata_json, source_message_ids_json,
               team_id, user_id, agent_id, task_id, session_key, session_id,
               timestamp_str, timestamp_start, timestamp_end, created_time, updated_time, version
        FROM l1_records
      `)
      .all() as Array<Record<string, unknown>>;
    const l0Rows = this.db
      .prepare(`
        SELECT record_id, message_text, session_key, session_id, team_id, task_id,
               user_id, agent_id, role, recorded_at, timestamp, source
        FROM l0_conversations
      `)
      .all() as Array<Record<string, unknown>>;

    if (l1Rows.length === 0 && l0Rows.length === 0) {
      this.setMeta("baseline_imported", "1");
      return { imported: false, l1Count: 0, l0Count: 0 };
    }

    this.db.exec("BEGIN IMMEDIATE");
    try {
      const sequence = this.nextSequence();
      for (const row of l1Rows) {
        const scope = l1HistoryScope({
          teamId: str(row.team_id),
          userId: str(row.user_id),
          agentId: str(row.agent_id),
          taskId: str(row.task_id),
        });
        const { scopeKey } = canonicalScope(scope);
        if (this.getLocatorRow("l1", str(row.record_id), scopeKey)) continue;
        this.createBaseline(row, "l1", scopeKey, sequence);
      }
      for (const row of l0Rows) {
        const scope = l0HistoryScope({
          teamId: str(row.team_id),
          userId: str(row.user_id),
          agentId: str(row.agent_id),
          taskId: str(row.task_id),
          sessionId: str(row.session_id),
        });
        const { scopeKey } = canonicalScope(scope);
        if (this.getLocatorRow("l0", str(row.record_id), scopeKey)) continue;
        this.createBaseline(row, "l0", scopeKey, sequence);
      }
      this.setMeta("baseline_imported", "1");
      this.db.exec("COMMIT");
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        /* ignore */
      }
      throw err;
    }
    return { imported: true, l1Count: l1Rows.length, l0Count: l0Rows.length };
  }

  private createBaseline(
    row: Record<string, unknown>,
    layer: "l1" | "l0",
    scopeKey: string,
    reuseSequence?: number,
  ): LocatorRow {
    const sequence = reuseSequence ?? this.nextSequence();
    const recordedAt = this.now();
    this.noteRecordedAt(recordedAt);
    const locator = str(row.record_id);
    const scope =
      layer === "l1"
        ? l1HistoryScope({
            teamId: str(row.team_id), userId: str(row.user_id),
            agentId: str(row.agent_id), taskId: str(row.task_id),
          })
        : l0HistoryScope({
            teamId: str(row.team_id), userId: str(row.user_id),
            agentId: str(row.agent_id), taskId: str(row.task_id), sessionId: str(row.session_id),
          });
    const { scopeJson } = canonicalScope(scope);

    let metadata: Record<string, unknown>;
    let body: string;
    if (layer === "l1") {
      const timestamps = dedupeStrings([
        str(row.timestamp_str), str(row.timestamp_start), str(row.timestamp_end),
      ]);
      metadata = buildL1VersionMetadata(
        parseJsonObjectStrict(row.metadata_json, `l1_records(${locator}).metadata_json`),
        {
          type: str(row.type),
          priority: num(row.priority),
          sceneName: str(row.scene_name),
          timestamps,
          sessionKey: str(row.session_key),
          sessionId: str(row.session_id),
          taskId: str(row.task_id),
          sourceMessageIds: parseStringArrayStrict(
            row.source_message_ids_json,
            `l1_records(${locator}).source_message_ids_json`,
          ),
        },
        {
          created_time: str(row.created_time),
          updated_time: str(row.updated_time),
          record_version: num(row.version),
          imported_at: recordedAt,
        },
      );
      body = str(row.content);
    } else {
      metadata = buildL0VersionMetadata(
        null,
        {
          role: str(row.role),
          source: str(row.source) || "conversation",
          sessionKey: str(row.session_key),
          sessionId: str(row.session_id),
          taskId: str(row.task_id),
          messageRecordedAt: str(row.recorded_at),
          originalTimestampMs: num(row.timestamp),
        },
        { recorded_at: str(row.recorded_at), imported_at: recordedAt },
      );
      body = str(row.message_text);
    }

    const contentId = newContentId();
    const draft: VersionMutation["draft"] = {
      layer,
      content_id: contentId,
      locator,
      scope,
      body,
      metadata,
      state: "active",
    };
    const version = this.materializeVersion({
      contentId,
      layer,
      scope,
      scopeKey,
      scopeJson,
      sequence,
      recordedAt,
      draft,
      predecessors: [],
      changeKind: "baseline",
      historyComplete: false,
    });
    this.persistVersion(version, scopeKey, scopeJson);
    return { content_id: contentId, version_id: version.ref.version_id, sequence };
  }

  // ── HistoryReader ─────────────────────────────────────────

  readVersion(query: HistoryRead): ContentVersion | null {
    if (!isPlainObject(query)) {
      throw new ContentHistoryError("history_invalid_input", "readVersion query is required");
    }
    if (!LAYERS.has(query.layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(query.layer)}"`);
    }
    if (typeof query.content_id !== "string" || query.content_id.trim() === "") {
      throw new ContentHistoryError("history_invalid_input", "readVersion requires content_id");
    }
    if (query.version_id !== undefined && query.as_of !== undefined) {
      throw new ContentHistoryError(
        "history_invalid_input",
        "readVersion: version_id and as_of are mutually exclusive",
      );
    }
    if (query.upper_sequence !== undefined && (!Number.isInteger(query.upper_sequence) || query.upper_sequence < 0)) {
      throw new ContentHistoryError("history_invalid_input", "upper_sequence must be a non-negative integer");
    }
    if (query.as_of !== undefined && !isValidInstant(query.as_of)) {
      throw new ContentHistoryError("history_invalid_input", "as_of must be a valid ISO-8601 instant");
    }
    if (query.as_of !== undefined && this.getMeta("clock_regressed") === "1") {
      throw new ContentHistoryError(
        "history_as_of_ambiguous",
        "as_of cannot be resolved because the program clock regressed; use version_id or upper_sequence",
      );
    }
    const { scopeKey } = canonicalScope(query.scope);
    const upper = query.upper_sequence;

    try {
      if (query.version_id) {
        const row = this.getVersionRow(query.version_id);
        if (!row) return null;
        if (row.layer !== query.layer || row.content_id !== query.content_id) return null;
        if (row.scope_key !== scopeKey) return null;
        if (upper !== undefined && row.sequence > upper) return null;
        return this.rowToVersion(row);
      }

      const conditions = ["content_id = ?", "layer = ?", "scope_key = ?"];
      const params: Array<string | number> = [query.content_id, query.layer, scopeKey];
      if (upper !== undefined) {
        conditions.push("sequence <= ?");
        params.push(upper);
      }
      if (query.as_of !== undefined) {
        conditions.push("recorded_at <= ?");
        params.push(query.as_of);
      }
      const row = this.db
        .prepare(
          `SELECT * FROM content_versions WHERE ${conditions.join(" AND ")} ` +
            "ORDER BY sequence DESC, version_id DESC LIMIT 1",
        )
        .get(...params) as VersionRow | undefined;
      return row ? this.rowToVersion(row) : null;
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `readVersion failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  listChanges(query: HistoryPageQuery): HistoryPage {
    if (!isPlainObject(query)) {
      throw new ContentHistoryError("history_invalid_input", "listChanges query is required");
    }
    if (!Number.isInteger(query.limit) || query.limit <= 0 || query.limit > 1000) {
      throw new ContentHistoryError("history_invalid_input", "listChanges limit must be an integer in 1..1000");
    }
    if (query.layer !== undefined && !LAYERS.has(query.layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(query.layer)}"`);
    }
    if (query.content_id !== undefined && (typeof query.content_id !== "string" || query.content_id.trim() === "")) {
      throw new ContentHistoryError("history_invalid_input", "content_id must be a non-empty string");
    }
    if (query.upper_sequence !== undefined && (!Number.isInteger(query.upper_sequence) || query.upper_sequence < 0)) {
      throw new ContentHistoryError("history_invalid_input", "upper_sequence must be a non-negative integer");
    }
    if (query.from_recorded_at !== undefined && !isValidInstant(query.from_recorded_at)) {
      throw new ContentHistoryError("history_invalid_input", "from_recorded_at must be a valid ISO-8601 instant");
    }
    if (query.to_recorded_at !== undefined && !isValidInstant(query.to_recorded_at)) {
      throw new ContentHistoryError("history_invalid_input", "to_recorded_at must be a valid ISO-8601 instant");
    }
    if (query.from_recorded_at !== undefined && query.to_recorded_at !== undefined
        && query.from_recorded_at > query.to_recorded_at) {
      throw new ContentHistoryError("history_invalid_input", "from_recorded_at must not be after to_recorded_at");
    }
    if (query.after !== undefined) {
      const after = query.after;
      if (!isPlainObject(after)
          || !Number.isInteger(after.sequence) || after.sequence < 0
          || typeof after.version_id !== "string" || after.version_id === "") {
        throw new ContentHistoryError(
          "history_invalid_input",
          "after cursor must be { sequence: non-negative integer, version_id: non-empty string }",
        );
      }
    }
    const { scopeKey } = canonicalScope(query.scope);
    const storeSequence = this.getSequence();
    const upper = query.upper_sequence !== undefined
      ? Math.min(query.upper_sequence, storeSequence)
      : storeSequence;

    try {
      const conditions = ["scope_key = ?", "sequence <= ?"];
      const params: Array<string | number> = [scopeKey, upper];
      if (query.layer !== undefined) { conditions.push("layer = ?"); params.push(query.layer); }
      if (query.content_id !== undefined) { conditions.push("content_id = ?"); params.push(query.content_id); }
      if (query.from_recorded_at !== undefined) { conditions.push("recorded_at >= ?"); params.push(query.from_recorded_at); }
      if (query.to_recorded_at !== undefined) { conditions.push("recorded_at <= ?"); params.push(query.to_recorded_at); }
      if (query.after !== undefined) {
        conditions.push("(sequence > ? OR (sequence = ? AND version_id > ?))");
        params.push(query.after.sequence, query.after.sequence, query.after.version_id);
      }

      const rows = this.db
        .prepare(
          `SELECT * FROM content_versions WHERE ${conditions.join(" AND ")} ` +
            "ORDER BY sequence ASC, version_id ASC LIMIT ?",
        )
        .all(...params, query.limit + 1) as unknown as VersionRow[];

      const hasMore = rows.length > query.limit;
      const page = hasMore ? rows.slice(0, query.limit) : rows;
      const items = page.map((row) => this.rowToVersion(row));
      const last = items[items.length - 1];
      const next = hasMore && last ? { sequence: last.sequence, version_id: last.ref.version_id } : null;

      // Coverage, not the filtered page: a window that starts before an imported
      // (history_complete=false) baseline's earliest recorded version is
      // unknowable, even when the window itself returns no rows. An empty old
      // range must never be reported as a complete/covered history.
      const historyComplete = !this.coverageIsIncomplete({
        layer: query.layer,
        scopeSql: "scope_key = ?",
        scopeParams: [scopeKey],
        contentId: query.content_id,
        fromRecordedAt: query.from_recorded_at,
      });

      return {
        items,
        upper_sequence: upper,
        next,
        history_complete: historyComplete,
      };
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `listChanges failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Whether this layer/scope/content contains an identity whose recorded
   * coverage begins after the queried window's lower bound — i.e. the query
   * reaches into time the store never recorded.
   *
   * An identity is "incompletely covered" when its earliest stored version is an
   * imported baseline (`history_complete=false`): everything before that
   * baseline's `recorded_at` is unknown. A query whose window starts at or after
   * that instant is covered by the recorded rows; a query whose window starts
   * before it (or is unbounded) is not — regardless of whether the filtered page
   * happens to contain a `history_complete=0` row.
   *
   * This is deliberately independent of the filtered page/filter set (which can
   * be empty) so an empty old range is still honestly marked incomplete. It
   * never reads a log or reconstructs the past.
   */
  private coverageIsIncomplete(args: {
    layer?: HistoryLayer;
    scopeSql: string;
    scopeParams: Array<string | number>;
    contentId?: string;
    fromRecordedAt?: string;
  }): boolean {
    const conditions: string[] = [args.scopeSql, "history_complete = 0"];
    const params: Array<string | number> = [...args.scopeParams];
    if (args.layer !== undefined) { conditions.push("layer = ?"); params.push(args.layer); }
    if (args.contentId !== undefined) { conditions.push("content_id = ?"); params.push(args.contentId); }
    // Restrict to each identity's earliest stored version (its coverage start).
    conditions.push(
      "sequence = (SELECT MIN(sequence) FROM content_versions v2 " +
        "WHERE v2.content_id = content_versions.content_id AND v2.layer = content_versions.layer)",
    );
    if (args.fromRecordedAt !== undefined) {
      conditions.push("recorded_at > ?");
      params.push(args.fromRecordedAt);
    }
    const row = this.db
      .prepare(`SELECT 1 AS ok FROM content_versions WHERE ${conditions.join(" AND ")} LIMIT 1`)
      .get(...params) as { ok: number } | undefined;
    return !!row;
  }

  /**
   * Narrow read-only authorization lookup: the exact stored object scope of a
   * content identity, but ONLY when it lies inside the caller's team/user/agent
   * (+ optional task) filter.
   *
   * This exists so the gateway never has to build the scope of an arbitrary
   * content id from request dimensions (which would be wrong for L0, whose
   * stored scope carries a session the caller does not know) and never has to
   * read the raw DB and assemble SQL outside the store. `null` covers both
   * "absent" and "outside the caller's scope" so existence never leaks.
   */
  resolveIdentityScope(
    layer: HistoryLayer,
    contentId: string,
    auth: ContentHistoryAuthFilter,
  ): HistoryScope | null {
    if (!LAYERS.has(layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(layer)}"`);
    }
    if (typeof contentId !== "string" || contentId.trim() === "") {
      throw new ContentHistoryError("history_invalid_input", "content_id must be a non-empty string");
    }
    this.assertAuthFilter(auth);
    try {
      const current = this.getCurrentRow(contentId);
      if (!current) return null;
      if (current.layer !== layer) return null;
      const version = this.getVersionRow(current.current_version_id);
      if (!version) throw new ContentHistoryError("history_corrupt", "current history version is missing");
      const scope = JSON.parse(version.scope_json) as HistoryScope;
      if (!isPlainObject(scope)) {
        throw new ContentHistoryError("history_corrupt", `scope_json for ${contentId} is not an object`);
      }
      if (!scopeMatchesAuth(scope, auth)) return null;
      return scope;
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `resolveIdentityScope failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Browse every version inside an authorization filter (optionally narrowed to
   * one identity / recorded-time window), with the same ascending keyset
   * pagination as {@link listChanges}.
   *
   * The authorization dimensions are pushed into SQL (`json_extract` on the
   * stored `scope_json`) so an out-of-scope identity is never loaded, let alone
   * returned. `team_id`/`user_id`/`agent_id` are always constrained, so a
   * missing request dimension cannot degenerate into "any owner".
   */
  listAuthorizedChanges(query: AuthorizedHistoryPageQuery): HistoryPage {
    if (!isPlainObject(query)) {
      throw new ContentHistoryError("history_invalid_input", "listAuthorizedChanges query is required");
    }
    if (!Number.isInteger(query.limit) || query.limit <= 0 || query.limit > 1000) {
      throw new ContentHistoryError("history_invalid_input", "limit must be an integer in 1..1000");
    }
    if (!LAYERS.has(query.layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(query.layer)}"`);
    }
    this.assertAuthFilter(query.auth);
    if (query.content_id !== undefined && (typeof query.content_id !== "string" || query.content_id.trim() === "")) {
      throw new ContentHistoryError("history_invalid_input", "content_id must be a non-empty string");
    }
    if (query.upper_sequence !== undefined && (!Number.isInteger(query.upper_sequence) || query.upper_sequence < 0)) {
      throw new ContentHistoryError("history_invalid_input", "upper_sequence must be a non-negative integer");
    }
    if (query.from_recorded_at !== undefined && !isValidInstant(query.from_recorded_at)) {
      throw new ContentHistoryError("history_invalid_input", "from_recorded_at must be a valid ISO-8601 instant");
    }
    if (query.to_recorded_at !== undefined && !isValidInstant(query.to_recorded_at)) {
      throw new ContentHistoryError("history_invalid_input", "to_recorded_at must be a valid ISO-8601 instant");
    }
    if (query.from_recorded_at !== undefined && query.to_recorded_at !== undefined
        && query.from_recorded_at > query.to_recorded_at) {
      throw new ContentHistoryError("history_invalid_input", "from_recorded_at must not be after to_recorded_at");
    }
    if (query.after !== undefined) {
      const after = query.after;
      if (!isPlainObject(after)
          || !Number.isInteger(after.sequence) || after.sequence < 0
          || typeof after.version_id !== "string" || after.version_id === "") {
        throw new ContentHistoryError(
          "history_invalid_input",
          "after cursor must be { sequence: non-negative integer, version_id: non-empty string }",
        );
      }
    }
    this.assertJsonExtractAvailable();

    const storeSequence = this.getSequence();
    const upper = query.upper_sequence !== undefined
      ? Math.min(query.upper_sequence, storeSequence)
      : storeSequence;

    try {
      const conditions = ["sequence <= ?"];
      const params: Array<string | number> = [upper];
      conditions.push("layer = ?");
      params.push(query.layer);
      if (query.content_id !== undefined) { conditions.push("content_id = ?"); params.push(query.content_id); }
      if (query.from_recorded_at !== undefined) { conditions.push("recorded_at >= ?"); params.push(query.from_recorded_at); }
      if (query.to_recorded_at !== undefined) { conditions.push("recorded_at <= ?"); params.push(query.to_recorded_at); }
      if (query.after !== undefined) {
        conditions.push("(sequence > ? OR (sequence = ? AND version_id > ?))");
        params.push(query.after.sequence, query.after.sequence, query.after.version_id);
      }
      const authConditions: string[] = [];
      const authParams: Array<string | number> = [];
      for (const dimension of ["team_id", "user_id", "agent_id", "task_id"] as const) {
        const wanted = query.auth[dimension];
        if (wanted === undefined) continue;
        authConditions.push(`json_extract(scope_json, '$.${dimension}') = ?`);
        authParams.push(wanted);
      }
      conditions.push(...authConditions);
      params.push(...authParams);

      const rows = this.db
        .prepare(
          `SELECT * FROM content_versions WHERE ${conditions.join(" AND ")} ` +
            "ORDER BY sequence ASC, version_id ASC LIMIT ?",
        )
        .all(...params, query.limit + 1) as unknown as VersionRow[];

      const hasMore = rows.length > query.limit;
      const page = hasMore ? rows.slice(0, query.limit) : rows;
      const items = page.map((row) => this.rowToVersion(row));
      const last = items[items.length - 1];
      const next = hasMore && last ? { sequence: last.sequence, version_id: last.ref.version_id } : null;

      // Coverage is a property of the identities in scope, not of the filtered
      // page: an empty old time range is still incomplete while an imported
      // baseline's coverage start lies after the window's lower bound.
      const historyComplete = !this.coverageIsIncomplete({
        layer: query.layer,
        scopeSql: authConditions.length > 0 ? authConditions.join(" AND ") : "1 = 1",
        scopeParams: authParams,
        contentId: query.content_id,
        fromRecordedAt: query.from_recorded_at,
      });

      return { items, upper_sequence: upper, next, history_complete: historyComplete };
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `listAuthorizedChanges failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  private assertAuthFilter(auth: ContentHistoryAuthFilter | undefined): void {
    if (!isPlainObject(auth)) {
      throw new ContentHistoryError("history_invalid_input", "authorization filter is required");
    }
    for (const dimension of ["team_id", "user_id", "agent_id"] as const) {
      const value = auth[dimension];
      if (typeof value !== "string" || value.trim() === "") {
        throw new ContentHistoryError(
          "history_invalid_input",
          `authorization filter requires a non-empty ${dimension}`,
        );
      }
    }
    if (auth.task_id !== undefined && (typeof auth.task_id !== "string" || auth.task_id.trim() === "")) {
      throw new ContentHistoryError("history_invalid_input", "task_id must be a non-empty string when present");
    }
  }

  /**
   * Fail loudly (never silently) if this SQLite build cannot evaluate the
   * `json_extract` authorization predicate. Modern SQLite has JSON1 built in;
   * a build without it would otherwise return wrong rows.
   */
  private assertJsonExtractAvailable(): void {
    if (this.jsonExtractAvailable !== undefined) {
      if (!this.jsonExtractAvailable) {
        throw new ContentHistoryError(
          "history_scope_query_unsupported",
          "this SQLite build has no json_extract; authorized content-history queries are unavailable",
        );
      }
      return;
    }
    try {
      const row = this.db.prepare("SELECT json_extract('{\"a\":1}', '$.a') AS value").get() as { value?: number } | undefined;
      this.jsonExtractAvailable = row?.value === 1;
    } catch {
      this.jsonExtractAvailable = false;
    }
    if (!this.jsonExtractAvailable) {
      throw new ContentHistoryError(
        "history_scope_query_unsupported",
        "this SQLite build has no json_extract; authorized content-history queries are unavailable",
      );
    }
  }

  resolveLocator(layer: HistoryLayer, locator: string, scope: HistoryScope): VersionRef | null {
    if (!LAYERS.has(layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(layer)}"`);
    }
    if (typeof locator !== "string" || locator.trim() === "") {
      // An empty locator is not a wildcard — it addresses nothing.
      return null;
    }
    const { scopeKey } = canonicalScope(scope);
    try {
      const row = this.getLocatorRow(layer, locator.trim(), scopeKey);
      if (!row) return null;
      return { layer, content_id: row.content_id, version_id: row.version_id };
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `resolveLocator failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Resolve an old locator (record id / profile path) to the version it
   * addressed, behind an authorization filter.
   *
   * The caller (gateway) never has to know or reconstruct the identity's stored
   * object scope: the locator row already carries the identity and version, and
   * the version's stored `scope_json` is the authority for ownership. Every
   * `team_id`/`user_id`/`agent_id` (+ optional `task_id`) dimension of the
   * trusted filter must match exactly, so an absent dimension is never a
   * wildcard. Reading the locator mapping (not the current row) means a retired
   * identity stays resolvable. `null` covers absent and out-of-scope alike.
   */
  resolveLocatorAuthorized(
    layer: HistoryLayer,
    locator: string,
    auth: ContentHistoryAuthFilter,
  ): { scope: HistoryScope; ref: VersionRef } | null {
    if (!LAYERS.has(layer)) {
      throw new ContentHistoryError("history_invalid_input", `unknown layer "${String(layer)}"`);
    }
    this.assertAuthFilter(auth);
    if (typeof locator !== "string" || locator.trim() === "") {
      // An empty locator is not a wildcard — it addresses nothing.
      return null;
    }
    try {
      const rows = this.db
        .prepare(
          "SELECT l.content_id AS content_id, l.version_id AS version_id, v.scope_json AS scope_json " +
            "FROM content_locators l JOIN content_versions v ON v.version_id = l.version_id " +
            "WHERE l.layer = ? AND l.locator = ? ORDER BY l.sequence DESC",
        )
        .all(layer, locator.trim()) as Array<{ content_id: string; version_id: string; scope_json: string }>;
      for (const row of rows) {
        const scope = JSON.parse(row.scope_json) as unknown;
        if (!isPlainObject(scope)) {
          throw new ContentHistoryError("history_corrupt", `scope_json for locator ${locator} is not an object`);
        }
        if (!scopeMatchesAuth(scope as HistoryScope, auth)) continue;
        return {
          scope: scope as HistoryScope,
          ref: { layer, content_id: row.content_id, version_id: row.version_id },
        };
      }
      return null;
    } catch (err) {
      if (err instanceof ContentHistoryError) throw err;
      throw new ContentHistoryError(
        "history_read_failed",
        `resolveLocatorAuthorized failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
}

// ============================
// Module helpers
// ============================

function str(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function num(value: unknown): number {
  return typeof value === "number" ? value : Number(value) || 0;
}

function dedupeStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const value of values) {
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

/** Strict JSON-object decode: bad schema is an error, not an empty default. */
function parseJsonObjectStrict(raw: unknown, context: string): Record<string, unknown> {
  if (raw === null || raw === undefined || raw === "") return {};
  if (isPlainObject(raw)) return raw;
  if (typeof raw !== "string") {
    throw new ContentHistoryError("history_corrupt", `${context}: expected a JSON object`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new ContentHistoryError(
      "history_corrupt",
      `${context}: invalid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (parsed === null) return {};
  if (!isPlainObject(parsed)) {
    throw new ContentHistoryError("history_corrupt", `${context}: expected a JSON object`);
  }
  return parsed;
}

/** Strict JSON string-array decode: bad schema is an error, never a silent []. */
function parseStringArrayStrict(raw: unknown, context: string): string[] {
  if (raw === null || raw === undefined || raw === "") return [];
  if (Array.isArray(raw)) return raw.filter((v): v is string => typeof v === "string");
  if (typeof raw !== "string") {
    throw new ContentHistoryError("history_corrupt", `${context}: expected a JSON array`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new ContentHistoryError(
      "history_corrupt",
      `${context}: invalid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!Array.isArray(parsed)) {
    throw new ContentHistoryError("history_corrupt", `${context}: expected a JSON array`);
  }
  return parsed.filter((v): v is string => typeof v === "string");
}
