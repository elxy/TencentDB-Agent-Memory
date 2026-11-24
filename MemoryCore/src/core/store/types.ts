/**
 * Memory Store Abstraction Layer — Core Types & Interfaces.
 *
 * This module defines the storage contracts that all backend implementations
 * (SQLite local, Tencent Cloud VectorDB, etc.) must satisfy.
 *
 * Design principles:
 * 1. **Backend-agnostic**: Upper-layer modules (hooks, tools, pipeline, record)
 *    depend only on these interfaces — never on concrete implementations.
 * 2. **Capability-based**: Features like vector search, FTS, and hybrid search
 *    are expressed as capability flags so callers can gracefully degrade.
 * 3. **Fault-tolerant**: All methods return empty results or `false` on
 *    failure rather than throwing, unless explicitly documented otherwise.
 * 4. **Sync-first**: Matches current SQLite DatabaseSync usage. TCVDB backend
 *    adapts internally without changing these signatures.
 */

import type { MemoryRecord } from "../record/l1-writer.js";
import type { EmbeddingProviderInfo } from "./embedding.js";
import type { Logger } from "../types.js";
import type { IsolationFilter } from "./isolation.js";
import type { MemoryPromptStore } from "../memory-prompt/types.js";
import type { MemoryGenerationRefStore } from "../memory-generation-log/types.js";
import type { ChangeCommit, ChangeCommitResult, HistoryScope } from "../content/history-contract.js";
import type { CommitContentChangesOptions } from "../content/history-write.js";

// Re-export so consumers can import everything from types.ts
export type { MemoryRecord, EmbeddingProviderInfo };

// Re-export isolation primitives so all store consumers import from here.
export type {
  IsolationContext,
  IsolationFilter,
  IsolationConfig,
} from "./isolation.js";
export {
  DEFAULT_ISOLATION_ID,
  LEGACY_ISOLATION_PLACEHOLDER,
  DEFAULT_ISOLATION_CONFIG,
  assertIsolation,
  buildIsolationWhere,
  rowMatchesIsolation,
  IsolationError,
} from "./isolation.js";

// ============================
// Common Types
// ============================

/** Minimal logger interface accepted by store implementations. */
export type StoreLogger = Logger;

// ============================
// L1 Types (Structured Memories)
// ============================

/** Result from an L1 vector similarity search. */
export interface L1SearchResult {
  record_id: string;
  content: string;
  type: string;
  priority: number;
  scene_name: string;
  /** Similarity score (0–1, higher is better). */
  score: number;
  timestamp_str: string;
  timestamp_start: string;
  timestamp_end: string;
  version: number;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  metadata_json: string;
}

/** Result from an L1 FTS keyword search. */
export interface L1FtsResult {
  record_id: string;
  content: string;
  type: string;
  priority: number;
  scene_name: string;
  /** BM25-derived score (0–1, higher is better). */
  score: number;
  timestamp_str: string;
  timestamp_start: string;
  timestamp_end: string;
  version: number;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  metadata_json: string;
}

/** Filter options for querying L1 records. */
export interface L1QueryFilter {
  /** Query by document primary keys (maps to VDB `documentIds`, max 20). */
  recordIds?: string[];
  sessionKey?: string;
  sessionId?: string;
  taskId?: string;
  /** Isolation dimensions (any subset). */
  teamId?: string;
  userId?: string;
  agentId?: string;
  /** Only return records with updated_time strictly after this ISO 8601 UTC timestamp. */
  updatedAfter?: string;
}

/**
 * One old→new L1 supersession with the old row's full content captured at
 * replacement time. `record_json` is the serialized old `MemoryRecord` shape
 * (id/content/type/metadata/source ids/timestamps/identity), so callers can
 * audit or restore without depending on the JSONL mirror or logs.
 */
export interface L1SupersessionRow {
  old_id: string;
  new_id: string;
  action: string;
  /** Serialized old row (JSON). */
  record_json: string;
  superseded_at_ms: number;
}

/** Row shape returned by L1 query methods. */
export interface L1RecordRow {
  record_id: string;
  content: string;
  type: string;
  priority: number;
  scene_name: string;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  version: number;
  timestamp_str: string;
  timestamp_start: string;
  timestamp_end: string;
  created_time: string;
  updated_time: string;
  metadata_json: string;
  /**
   * Model-declared source message ids, JSON-encoded.
   *
   * Optional so pre-existing rows / statement shapes stay valid; new writes
   * always populate it (possibly `"[]"`). Callers parse it rather than
   * trusting an arbitrary metadata field with permission-like meaning.
   */
  source_message_ids_json?: string;
}

/**
 * A superseded (replaced) L1 record retained for read-by-old-id.
 *
 * When an update/merge replaces an existing record, the old row is archived
 * here before deletion so `atomic/get` can still return the old content and
 * point at the record that replaced it. `new_id` may be empty when the
 * replacement link could not be written — the API then reports the old row as
 * not-current with an unknown target instead of fabricating one.
 */
/**
 * Result of an atomic L1 replacement.
 *
 * `replaced` is `false` only when the store could not perform the replacement
 * (e.g. degraded). Callers must not treat a non-replaced result as success.
 */
export interface L1ReplaceResult {
  replaced: boolean;
  /** Old ids that were archived + deleted (subset of the requested ids). */
  superseded: string[];
}

export interface L1SupersededRow {
  old_id: string;
  new_id: string;
  content: string;
  type: string;
  priority: number;
  scene_name: string;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  source_message_ids: string[];
  metadata_json: string;
  version: number;
  created_time: string;
  updated_time: string;
  superseded_at_ms: number;
}

// ============================
// L0 Types (Raw Conversations)
// ============================

/**
 * Where an L0 message came from (2026-10-12 架构, docs/memory-design-proposal.md §5.2):
 *   - `conversation`: 用户消息或模型回复（默认）；
 *   - `framework_event`: 框架追加进会话的消息（钩子通知、分身问答、背景追加），
 *     不当作用户原话。
 */
export type L0MessageSource = "conversation" | "framework_event";

/** An L0 conversation message record for vector indexing. */
export interface L0Record {
  id: string;
  sessionKey: string;
  sessionId: string;
  /** Message provenance; absent means `conversation` for legacy callers. */
  source?: L0MessageSource;
  /**
   * Three-dim isolation (new in this branch).
   *
   * Mandatory for new writes once gateway-level enforcement is on, but kept
   * optional on the type during the rollout window. SQLite upsert defaults to
   * '' when missing; the migration script backfills existing rows to
   * `__legacy__`.
   */
  teamId?: string;
  userId?: string;
  agentId?: string;
  taskId?: string;
  role: string;
  messageText: string;
  recordedAt: string;
  /** Original message timestamp (epoch ms). */
  timestamp: number;
}

/** Result from an L0 vector similarity search. */
export interface L0SearchResult {
  record_id: string;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  role: string;
  message_text: string;
  /** Similarity score (0–1, higher is better). */
  score: number;
  recorded_at: string;
  timestamp: number;
}

/** Result from an L0 FTS keyword search. */
export interface L0FtsResult {
  record_id: string;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  role: string;
  message_text: string;
  /** BM25-derived score (0–1, higher is better). */
  score: number;
  recorded_at: string;
  timestamp: number;
}

/** Raw L0 row returned by query methods (used by L1 runner). */
export interface L0QueryRow {
  record_id: string;
  session_key: string;
  session_id: string;
  team_id: string;
  task_id: string;
  user_id: string;
  agent_id: string;
  role: string;
  message_text: string;
  recorded_at: string;
  timestamp: number;
}

/** L0 messages grouped by session ID (for L1 runner). */
export interface L0SessionGroup {
  sessionId: string;
  teamId?: string;
  userId: string;
  agentId: string;
  taskId?: string;
  messages: Array<{
    id: string;
    role: string;
    content: string;
    timestamp: number;
    /** Epoch ms when this message was recorded into L0 (used by L1 cursor). */
    recordedAtMs: number;
  }>;
}

// ============================
// Store Init Result
// ============================

/** Result of store initialization. */
export interface StoreInitResult {
  /** Whether embeddings need to be regenerated (provider/model change). */
  needsReindex: boolean;
  /** Human-readable reason (for logging). */
  reason?: string;
}

// ============================
// Capability Flags
// ============================

/**
 * Describes what search capabilities a store backend supports.
 * Callers use this to select search strategies and degrade gracefully.
 */
export interface StoreCapabilities {
  /** Whether vector (embedding) search is available. */
  vectorSearch: boolean;
  /** Whether FTS (full-text keyword) search is available. */
  ftsSearch: boolean;
  /** Whether native hybrid search is supported (e.g., TCVDB hybridSearch). */
  nativeHybridSearch: boolean;
  /** FTS accepts raw text and returns normalized BM25 scores without client fusion. */
  nativeBm25Search?: boolean;
  /** Whether the store supports sparse vectors (BM25 encoding). */
  sparseVectors: boolean;
  /**
   * Whether the store serves L2/L3 profile rows, i.e. satisfies
   * {@link IProfileRowStore}. Required to back a row-view filesystem (`rowfs`).
   * SQLite reports `false`.
   */
  profileRows: boolean;
}

// ============================
// L2/L3 Profile Sync Types
// ============================

/** Canonical L2/L3 profile row shared between local cache and remote store. */
export interface ProfileRecord {
  /** Stable ID: `profile:v1:${sha256(scope + "\0" + type + "\0" + filename)}`. */
  id: string;
  type: "l2" | "l3";
  filename: string;
  content: string;
  contentMd5: string;
  /** L2/L3 profile identity is team+agent scoped. userId/sessionId are optional
   *  for backwards compatibility with old in-memory shapes; new profile writes leave them empty. */
  teamId?: string;
  agentId?: string;
  userId?: string;
  sessionId?: string;
  version: number;
  createdAtMs: number;
  updatedAtMs: number;
}

/** Profile upsert payload with optimistic-lock baseline from the last pull. */
export interface ProfileSyncRecord extends ProfileRecord {
  baselineVersion?: number;
}

/**
 * Filter for profile row queries — shared by `countProfiles` and `queryProfiles`.
 *
 * `pathPrefix` matches `filename` by string prefix (Mongo `$regex ^prefix`,
 * TCVDB `startsWith`), which is what `IStorageBackend.listObjects` needs.
 */
export interface ProfileFilter {
  type?: ProfileRecord["type"];
  teamId?: string;
  userId?: string;
  agentId?: string;
  pathPrefix?: string;
}

/** @deprecated Use {@link ProfileFilter}. Kept so existing call sites keep compiling. */
export type ProfileCountFilter = ProfileFilter;

/**
 * The profile-row surface required to back a row-view filesystem
 * (`ProfileRowStorageBackend`).
 *
 * On {@link IMemoryStore} these methods are all optional, because SQLite does not
 * implement them. This interface restates them as **required**, so that anything
 * needing a row-view filesystem can demand `IMemoryStore & IProfileRowStore` and
 * let `tsc` reject a store that cannot serve it.
 *
 * Use {@link isProfileRowStore} (store/profile-row-store.ts) to narrow at runtime.
 */
export interface IProfileRowStore {
  pullProfiles(): Promise<ProfileRecord[]>;
  queryProfilesByIds(ids: string[]): Promise<ProfileRecord[]>;
  /** Query rows by scope/type/path-prefix. Returns rows rather than a count. */
  queryProfiles(filter?: ProfileFilter): Promise<ProfileRecord[]>;
  countProfiles(filter?: ProfileFilter): Promise<number>;
  syncProfiles(records: ProfileSyncRecord[]): Promise<void>;
  deleteProfiles(recordIds: string[]): Promise<void>;
}

/** An {@link IMemoryStore} that is statically known to serve profile rows. */
export type ProfileRowCapableStore = IMemoryStore & IProfileRowStore;

// ============================
// Committed operation read (idempotent replay)
// ============================
//
// A caller may want to know whether an `operation_id` was already committed in
// a scope and, if so, replay the EXACT original `ChangeCommitResult` instead of
// rebuilding the request from today's current state (which could drift). This is
// a read-only lookup against the store's OWN formal chain: it opens no second
// idempotency database, keeps no volatile process map and never scans logs.
//
// `request_fingerprint` is the normalized client-request semantics the commit
// was first assembled for — a program-written field, never a caller's
// `user_metadata`. A null fingerprint means the operation predates fingerprint
// recording and therefore cannot be verified: the caller must NOT treat it as a
// match.

export interface CommittedOperationRecord {
  operation_id: string;
  scope: HistoryScope;
  request_fingerprint: string | null;
  result: ChangeCommitResult;
}

/** Read-only committed-operation lookup capability. */
export interface CommittedOperationReader {
  getCommittedOperation(
    scope: HistoryScope,
    operationId: string,
  ): MaybePromise<CommittedOperationRecord | null>;
}

// ============================
// v2 API Paginated Query Types
// ============================

/** Filter for v2 L0 paginated query (`/conversation/query`). */
export interface L0CountFilter {
  /** Filter by session. */
  sessionId?: string;
  /** Isolation dimensions (any subset). */
  teamId?: string;
  userId?: string;
  agentId?: string;
  taskId?: string;
  /** Timestamp >= (epoch ms, inclusive). */
  timeStartMs?: number;
  /** Timestamp <= (epoch ms, inclusive). */
  timeEndMs?: number;
}

export interface L0PaginatedFilter extends L0CountFilter {
  /** Page size. */
  limit: number;
  /** Page offset. */
  offset: number;
}

/** Result of v2 L0 paginated query. */
export interface L0PaginatedResult {
  rows: L0QueryRow[];
  /** Total count matching filters (for pagination). */
  total: number;
}

/** Filter for v2 L1 paginated query (`/atomic/query`). */
export interface L1CountFilter {
  /** Filter by memory type (episodic/persona/instruction). */
  type?: string;
  /** Filter by session. */
  sessionId?: string;
  /** Isolation dimensions (any subset). */
  teamId?: string;
  userId?: string;
  agentId?: string;
  taskId?: string;
  /** Filter by updated_time >= (ISO 8601). */
  timeStart?: string;
  /** Filter by updated_time <= (ISO 8601). */
  timeEnd?: string;
}

export interface L1PaginatedFilter extends L1CountFilter {
  /** Page size. */
  limit: number;
  /** Page offset. */
  offset: number;
}

/** Result of v2 L1 paginated query. */
export interface L1PaginatedResult {
  rows: L1RecordRow[];
  /** Total count matching filters (for pagination). */
  total: number;
}

// ============================
// Entity Metadata Types (Team / User / Agent / Task)
// ============================

export type TeamStatus = "active" | "archived";
export type UserStatus = "active" | "inactive";
export type AgentStatus = "active" | "inactive";
export type AgentVisibility = "team" | "restricted";
/** 大类，不含具体来源（来源读 metadata_json.external.provider）。 */
export type TaskSourceType = "manual" | "external" | "other";

export interface TeamEntity {
  team_id: string;
  name: string;
  description?: string;
  owner_user_id: string;
  status: TeamStatus;
  user_ids?: string[];
  agent_ids?: string[];
  task_ids?: string[];
  created_at: string;
  updated_at: string;
}

export interface UserEntity {
  user_id: string;
  name: string;
  job_description?: string;
  team_ids: string[];
  task_ids: string[];
  owned_agent_ids: string[];
  task_agent_ids?: string[];
  status: UserStatus;
  created_at: string;
  updated_at: string;
}

export interface AgentEntity {
  agent_id: string;
  team_id: string;
  name: string;
  description?: string;
  prompt?: string;
  owner_user_id?: string;
  visibility: AgentVisibility;
  status: AgentStatus;
  task_ids?: string[];
  created_at: string;
  updated_at: string;
}

export interface TaskEntity {
  task_id: string;
  team_id: string;
  creator_user_id: string;
  title?: string;
  description?: string;
  source_type: TaskSourceType;
  source_url?: string;
  agent_ids: string[];
  user_ids: string[];
  created_at: string;
  updated_at: string;
}

export interface BatchDeleteResult {
  deleted_ids: string[];
  failed: Array<{ id: string; reason: string }>;
}

/**
 * 按隔离维度清空某个 memory 下的全部内容（不删除资产本身）。
 *
 * 语义约定（见 `/v3/chat-memory/clear`）：
 *   - 至少要给 teamId + agentId，否则实现必须直接拒绝（避免误删全库）；
 *   - 不带 sessionId：清空该 (team, agent) 下所有 session 的数据；
 *   - 只删内容行（L0/L1 + 向量 / FTS 附属行），不动 meta_* 资产表。
 */
export interface MemoryContentClearFilter {
  teamId: string;
  agentId: string;
  /** 可选：进一步收窄到单个 user。缺省表示该 agent 下所有 user。 */
  userId?: string;
}

/** 清空结果：各层实际删除行数。 */
export interface MemoryContentClearResult {
  l0Deleted: number;
  l1Deleted: number;
  /** L2/L3 profile 行数（VDB / sqlite profiles 表）。 */
  profilesDeleted: number;
}

export type KnowledgeType = "wiki" | "code-graph";

export interface KnowledgeEntity {
  knowledge_id: string;
  type: KnowledgeType;
  service_url: string;
  name: string;
  summary: string | null;
  team_id: string;
  /** 预留：agent 绑定维度（当前写 ""，绑定权威在 meta_assets）。 */
  agent_id?: string;
  user_id: string | null;
  repo_url?: string;
  branch?: string;
  created_at: string;
  updated_at: string;
}

export interface KnowledgeListResult {
  items: KnowledgeEntity[];
  total: number;
}

// ============================
// IMemoryStore — The Core Abstraction
// ============================

/**
 * Unified memory store interface.
 *
 * Implementations:
 * - `SqliteMemoryStore` (sqlite.ts) — local SQLite + sqlite-vec + FTS5
 * - `TcvdbMemoryStore` (tcvdb.ts) — Tencent Cloud VectorDB (future)
 *
 * All methods are fault-tolerant: they return empty results or `false` on
 * failure rather than throwing, unless explicitly documented otherwise.
 */
/**
 * Helper type: a value that may be sync or async.
 * Callers should always `await` the result — it's safe for both sync and async values.
 */
export type MaybePromise<T> = T | Promise<T>;

// ============================
// Memory Audit (修改审计)
// ============================

/**
 * 一次记忆修改事件。原始 L0/L1/L2/L3 表完全不动；这里只追加一行审计。
 *
 * 设计要点（per user 决策）：
 *   - 不存历史 content / 旧值，只记"什么时间、由谁、改了哪条"
 *   - team/agent/user/task 来自外部请求 IdFields（不是 record 原值）
 *   - version = 修改后该记录的新版本号（与原表 version 字段一致）
 *   - L0 不进 audit（不可变流水）；L1/L2/L3 update + delete 各记一条
 */
export interface AuditEntry {
  /** 自动生成主键，建议 audit-{uuid}。 */
  audit_id: string;
  /**
   * 被修改的记录主键：
   *   - L1 → MemoryRecord.id (msg-xxx / mem-xxx)
   *   - L2 → 文件路径 (scene_blocks/xxx.md)
   *   - L3 → "core"（全实例只一份）或 path
   */
  record_id: string;
  layer: "L1" | "L2" | "L3";
  action: "update" | "delete";
  /** 外部请求 IdFields 副本，来源是调用方传入的 body / header（resolveIsolation 后）。 */
  team_id?: string;
  agent_id?: string;
  user_id?: string;
  task_id?: string;
  /** 修改后该记录的新版本号。delete 用 0 或上一版本号 + 1。 */
  version: number;
  /** 修改时间（毫秒）。 */
  updated_at_ms: number;
  /** Gateway request_id，便于 trace。 */
  request_id?: string;
}

/** queryAudit 过滤条件，全部可选。 */
export interface AuditQueryFilter {
  record_id?: string;
  layer?: "L1" | "L2" | "L3";
  action?: "update" | "delete";
  team_id?: string;
  agent_id?: string;
  user_id?: string;
  task_id?: string;
  /** 只返 updated_at_ms ≥ since_ms 的事件。 */
  since_ms?: number;
  /** 只返 updated_at_ms ≤ until_ms 的事件。 */
  until_ms?: number;
  limit?: number;   // 默认 100，上限 1000
  offset?: number;
}

export interface IMemoryStore extends MemoryPromptStore, MemoryGenerationRefStore {
  // ── Capabilities ───────────────────────────────────────────

  /**
   * Whether this store supports deferred (background) embedding updates.
   *
   * When `true`, auto-capture writes metadata-only via `upsertL0(record, undefined)`
   * and later calls `updateL0Embedding()` in a fire-and-forget background task.
   * When `false` or absent, embedding is computed inline and passed to `upsertL0()`.
   */
  readonly supportsDeferredEmbedding?: boolean;

  // ── Lifecycle (always sync) ──────────────────────────────

  init(providerInfo?: EmbeddingProviderInfo): MaybePromise<StoreInitResult>;
  isDegraded(): boolean;
  getCapabilities(): StoreCapabilities;
  close(): void;

  // ── L1 Write ─────────────────────────────────────────────

  upsertL1(record: MemoryRecord, embedding?: Float32Array): MaybePromise<boolean>;
  deleteL1(recordId: string, filter?: IsolationFilter): MaybePromise<boolean>;
  deleteL1Batch(recordIds: string[], filter?: IsolationFilter): MaybePromise<boolean>;
  deleteL1Expired(cutoffIso: string): MaybePromise<number>;
  /**
   * Old record ids that were replaced by `newRecordId`. Backed by the store's
   * own replacement table so history does not depend on JSONL or logs.
   * Optional; unsupported stores return an empty list or undefined.
   */
  queryL1ReplacementTargets?(newRecordId: string): MaybePromise<string[]>;
  /**
   * Supersession history with the full old row preserved as JSON. Required for
   * old-content queries during a correction; unsupported stores omit it.
   */
  queryL1Supersessions?(newRecordId: string): MaybePromise<L1SupersessionRow[]>;

  // ── L1 Read ──────────────────────────────────────────────

  countL1(filter?: L1CountFilter): MaybePromise<number>;
  queryL1Records(filter?: L1QueryFilter): MaybePromise<L1RecordRow[]>;
  getAllL1Texts(): MaybePromise<Array<{ record_id: string; content: string; updated_time: string }>>;

  /**
   * Read one L1 record by primary key, honouring the isolation filter.
   * Returns `null` when absent OR outside the caller's scope — callers must
   * not distinguish the two (existence must not leak across scopes).
   *
   * Optional capability: a store that omits it makes `atomic/get` return an
   * explicit `unsupported` error instead of a fabricated miss.
   */
  getL1RecordById?(recordId: string, filter?: IsolationFilter): MaybePromise<L1RecordRow | null>;

  /**
   * Read an archived (superseded) L1 record by its old id, honouring the
   * isolation filter. Optional; see {@link getL1RecordById}.
   */
  getL1SupersededRecord?(oldId: string, filter?: IsolationFilter): MaybePromise<L1SupersededRow | null>;

  /**
   * @deprecated Do NOT call from the writer: archive-then-delete is not
   * atomic. 收窄后不再存在替代式写入路径（去重只新增或跳过完全重复），本
   * 方法仅为既有后端/测试保留，不应再被调用。
   */
  archiveL1Supersessions?(oldIds: string[], newId: string, filter?: IsolationFilter): MaybePromise<number>;

  // ── L1 Search ────────────────────────────────────────────

  searchL1Vector(queryEmbedding: Float32Array, topK?: number, queryText?: string, filter?: IsolationFilter): MaybePromise<L1SearchResult[]>;
  searchL1Fts(ftsQuery: string, limit?: number, filter?: IsolationFilter): MaybePromise<L1FtsResult[]>;
  searchL1Hybrid?(params: {
    query?: string;
    queryEmbedding?: Float32Array;
    sparseVector?: Array<[number, number]>;
    topK?: number;
    filter?: IsolationFilter;
  }): MaybePromise<L1SearchResult[]>;

  // ── L0 Write ─────────────────────────────────────────────

  upsertL0(record: L0Record, embedding?: Float32Array): MaybePromise<boolean>;
  /** Update only the vector embedding for an existing L0 record (sqlite background path). */
  updateL0Embedding?(recordId: string, embedding: Float32Array): MaybePromise<boolean>;
  /**
   * Insert a whole `/conversation/add` group in one batch (optional capability).
   *
   * Callers prefer this over the per-record `upsertL0` loop when present AND no
   * per-message embedding is required. Records carry freshly-generated ids, so
   * an `insertMany`-style implementation is correct; a duplicate id must surface
   * as an error rather than silently overwrite. Returns the number inserted.
   * See docs/design/mongodb/phases/phase-1-db/2026-08-24-l0-write-batch-and-id.md.
   */
  insertL0Batch?(records: L0Record[]): MaybePromise<number>;
  deleteL0(recordId: string, filter?: IsolationFilter): MaybePromise<boolean>;
  deleteL0Expired(cutoffIso: string): MaybePromise<number>;

  // ── L0 Read ──────────────────────────────────────────────

  countL0(filter?: L0CountFilter): MaybePromise<number>;
  /**
   * Read one L0 record by primary key, honouring the isolation filter.
   * Returns `null` when absent OR outside the caller's scope. Optional;
   * `conversation/get` returns an explicit `unsupported` error when missing.
   */
  getL0RecordById?(recordId: string, filter?: IsolationFilter): MaybePromise<L0QueryRow | null>;
  queryL0ForL1(sessionKey: string, afterRecordedAtMs?: number, limit?: number): MaybePromise<L0QueryRow[]>;
  queryL0GroupedBySessionId(sessionKey: string, afterRecordedAtMs?: number, limit?: number): MaybePromise<L0SessionGroup[]>;
  getAllL0Texts(): MaybePromise<Array<{ record_id: string; message_text: string; recorded_at: string }>>;

  // ── L0 Search ────────────────────────────────────────────

  searchL0Vector(queryEmbedding: Float32Array, topK?: number, queryText?: string, filter?: IsolationFilter): MaybePromise<L0SearchResult[]>;
  searchL0Fts(ftsQuery: string, limit?: number, filter?: IsolationFilter): MaybePromise<L0FtsResult[]>;
  searchL0Hybrid?(params: {
    query?: string;
    queryEmbedding?: Float32Array;
    sparseVector?: Array<[number, number]>;
    topK?: number;
    filter?: IsolationFilter;
  }): MaybePromise<L0SearchResult[]>;

  pullProfiles?(): Promise<ProfileRecord[]>;
  /**
   * 按 profile stable id 批量查询（轻量，仅查指定 id）。
   * 当 store 支持时，调用方应优先使用此接口而非 pullProfiles() 全量拉取。
   * 不支持的 store 返回 undefined → 调用方 fallback 到 pullProfiles()。
   */
  queryProfilesByIds?(ids: string[]): Promise<ProfileRecord[]>;
  /**
   * 按 scope / type / filename 前缀查询 profile 行（返回行，而非 count）。
   * 支持行视图文件系统的 `listObjects`，避免退化成全量 `pullProfiles()`。
   */
  queryProfiles?(filter?: ProfileFilter): Promise<ProfileRecord[]>;
  countProfiles?(filter?: ProfileFilter): Promise<number>;
  syncProfiles?(records: ProfileSyncRecord[]): Promise<void>;
  deleteProfiles?(recordIds: string[]): Promise<void>;

  // ── Content history (read-only capability surface, §4.8) ────
  //
  // Optional so a backend that cannot host an immutable version chain (TCVDB,
  // Mongo, COS-only file stores) simply omits them; the gateway then reports an
  // explicit `history_unavailable` instead of fabricating a version read.

  /** Whether content-history versioning is active for this store. */
  isContentHistoryEnabled?(): boolean;
  /**
   * The content-history reader for this store, when history is enabled. Its
   * authorization-filtered queries keep scope resolution inside the store — the
   * gateway never reads the raw DB or builds scope SQL itself.
   */
  getContentHistory?(): import("../content/history-reader.js").ScopedContentHistoryReader | undefined;

  /**
   * Apply one `ChangeCommit` of L0/L1 mutations in a SINGLE SQLite transaction:
   * the current row, its FTS / vector index and the immutable content-history
   * version commit together or not at all.
   *
   * This is the formal write entry — a caller must NOT pre-check a version and
   * then call the legacy `upsertL1` to fake a compare-and-
   * swap. `operation_id` de-duplication, `expected_version_id` / predecessor
   * CAS, the original-record-id uniqueness and owner verification are all
   * re-checked inside the transaction. Optional so a backend without a content
   * history chain simply omits it (the gateway then reports an explicit
   * `unsupported` instead of accepting an unversioned write).
   *
   * Change kinds the store cannot faithfully apply (`baseline`, `rename`, L2/L3)
   * are refused explicitly rather than partially written.
   */
  commitContentChanges?(
    commit: ChangeCommit,
    filter: IsolationFilter,
    options?: CommitContentChangesOptions,
  ): MaybePromise<ChangeCommitResult>;

  // ── Re-index ─────────────────────────────────────────────

  reindexAll(
    embedFn: (text: string) => Promise<Float32Array>,
    onProgress?: (done: number, total: number, layer: "L1" | "L0") => void,
  ): Promise<{ l1Count: number; l0Count: number }>;

  // ── FTS (always sync — cached flag) ──────────────────────

  isFtsAvailable(): boolean;

  // ── v2 API: Paginated queries (optional — added for Gateway v2) ──

  /**
   * L0 paginated query for v2 API `/conversation/query`.
   * Returns rows matching the filter, paginated by limit/offset,
   * plus the total count of matching rows.
   */
  queryL0Paginated?(filter: L0PaginatedFilter): MaybePromise<L0PaginatedResult>;

  /**
   * L1 paginated query for v2 API `/atomic/query`.
   * Returns rows matching the filter, paginated by limit/offset,
   * plus the total count of matching rows.
   */
  queryL1Paginated?(filter: L1PaginatedFilter): MaybePromise<L1PaginatedResult>;

  /**
   * Delete all L0 messages belonging to a session.
   * Returns the actual number of rows deleted.
   * Used by v2 API `/conversation/delete` (session mode).
   */
  deleteL0BySession?(sessionId: string, filter?: IsolationFilter): MaybePromise<number>;

  /**
   * 清空某个 (team, agent) 下的全部记忆内容：L0 + L1 + L2/L3 profile 行，
   * 连同它们的向量 / FTS 附属数据。**不触碰** meta_* 资产表 —— 资产 ID、
   * 归属、绑定、ACL、可见性、名称全部保留。
   *
   * 用于 `/v3/chat-memory/clear`。幂等：已清空的 memory 再次调用返回全 0。
   * 实现必须校验 filter.teamId / filter.agentId 非空，否则抛错拒绝执行。
   */
  clearMemoryContent?(filter: MemoryContentClearFilter): MaybePromise<MemoryContentClearResult>;

  // ── Entity metadata (Team / User / Agent / Task) ───────────
  createTeam?(input: Omit<TeamEntity, "created_at" | "updated_at" | "status" | "user_ids" | "agent_ids" | "task_ids"> & { team_id?: string; status?: TeamStatus }): MaybePromise<TeamEntity>;
  getTeam?(teamId: string): MaybePromise<TeamEntity | null>;
  updateTeam?(teamId: string, patch: Partial<Pick<TeamEntity, "name" | "description" | "owner_user_id" | "user_ids" | "agent_ids" | "status">>): MaybePromise<TeamEntity | null>;
  deleteTeams?(teamIds: string[]): MaybePromise<BatchDeleteResult>;

  createUser?(input: Pick<UserEntity, "name"> & Partial<Pick<UserEntity, "job_description" | "status">> & { user_id?: string }): MaybePromise<UserEntity>;
  getUser?(userId: string): MaybePromise<UserEntity | null>;
  updateUser?(userId: string, patch: Partial<Pick<UserEntity, "name" | "job_description" | "status">>): MaybePromise<UserEntity | null>;
  deleteUsers?(userIds: string[]): MaybePromise<BatchDeleteResult>;

  createAgent?(input: Omit<AgentEntity, "created_at" | "updated_at" | "status" | "visibility"> & { agent_id?: string; status?: AgentStatus; visibility?: AgentVisibility }): MaybePromise<AgentEntity>;
  getAgent?(agentId: string): MaybePromise<AgentEntity | null>;
  updateAgent?(agentId: string, patch: Partial<Pick<AgentEntity, "name" | "description" | "prompt" | "owner_user_id" | "visibility" | "status">>): MaybePromise<AgentEntity | null>;
  deleteAgents?(agentIds: string[]): MaybePromise<BatchDeleteResult>;

  createTask?(input: Omit<TaskEntity, "created_at" | "updated_at" | "source_type" | "agent_ids" | "user_ids"> & { task_id?: string; source_type?: TaskSourceType; agent_ids?: string[]; user_ids?: string[] }): MaybePromise<TaskEntity>;
  getTask?(taskId: string): MaybePromise<TaskEntity | null>;
  updateTask?(taskId: string, patch: Partial<Pick<TaskEntity, "title" | "description" | "source_type" | "source_url" | "agent_ids" | "user_ids">>): MaybePromise<TaskEntity | null>;
  deleteTasks?(taskIds: string[]): MaybePromise<BatchDeleteResult>;

  // ── Knowledge entity (wiki / code-graph metadata) ───────────
  createKnowledge?(input: Omit<KnowledgeEntity, "created_at" | "updated_at">): MaybePromise<KnowledgeEntity>;
  getKnowledge?(knowledgeId: string): MaybePromise<KnowledgeEntity | null>;
  updateKnowledge?(knowledgeId: string, patch: Partial<Pick<KnowledgeEntity, "name" | "summary" | "service_url" | "repo_url" | "branch">>): MaybePromise<KnowledgeEntity | null>;
  deleteKnowledge?(knowledgeIds: string[], teamId?: string): MaybePromise<BatchDeleteResult>;
  listKnowledge?(input: { team_id: string; type?: KnowledgeType; knowledge_ids?: string[]; limit?: number; offset?: number }): MaybePromise<KnowledgeListResult>;

  // ── Memory Audit（修改审计；optional 让 store 可以选择不实现）──
  appendAudit?(entry: AuditEntry): MaybePromise<void>;
  queryAudit?(filter: AuditQueryFilter): MaybePromise<AuditEntry[]>;
}

// ============================
// IEmbeddingService — re-exported from embedding.ts for convenience
// ============================

/**
 * Re-export EmbeddingService as IEmbeddingService for backward compatibility.
 * The canonical definition lives in `./embedding.ts`. All concrete implementations
 * (LocalEmbeddingService, OpenAIEmbeddingService, NoopEmbeddingService) implement
 * the EmbeddingService interface from embedding.ts.
 */
export type { EmbeddingService as IEmbeddingService } from "./embedding.js";
