/**
 * TDAI Memory Gateway — v2 API Schemas.
 *
 * Generated baseline: `generated/schemas.ts` (Kubb, do not edit).
 * This file re-exports everything from the generated baseline, then adds
 * hand-written overrides that OpenAPI cannot express:
 *   - safePath refine (path traversal prevention)
 *   - conversationDelete / atomicDelete 批量删除上限 + 去重 + 至少一项校验
 *   - Generic ApiResponseEnvelope<T> interface
 *   - formatZodError utility
 */

import { z } from "zod";
import { DEFAULT_ISOLATION_ID } from "../core/store/types.js";

// ============================
// Re-export all generated schemas as-is
// ============================

export {
  apiResponseEnvelopeSchema,
  conversationRoleSchema,
  paginationSchema,
  conversationItemSchema,
  conversationAddDataSchema,
  conversationQueryRequestSchema,
  conversationQueryDataSchema,
  conversationDeleteDataSchema,
  atomicDetailSchema,
  atomicUpdateRequestSchema,
  atomicUpdateDataSchema,
  atomicDeleteDataSchema,
  atomicQueryRequestSchema,
  atomicQueryDataSchema,
  scenarioListRequestSchema,
  scenarioEntrySchema,
  scenarioListDataSchema,
  scenarioFileSchema,
  scenarioWriteDataSchema,
  coreFileSchema,
  coreWriteRequestSchema,
  coreWriteDataSchema,
  conversationSearchRequestSchema,
  conversationSearchHitSchema,
  conversationSearchDataSchema,
  atomicSearchRequestSchema,
  atomicSearchHitSchema,
  atomicSearchDataSchema,
} from "./generated/schemas.js";

// Re-export generated types
export type {
  ConversationRole,
  Pagination,
  ConversationAddRequest,
  ConversationQueryRequest,
  ConversationQueryData,
  ConversationDeleteData,
  AtomicUpdateRequest,
  AtomicDeleteData,
  AtomicQueryRequest,
  ScenarioListRequest,
  CoreReadRequest,
  CoreWriteRequest,
  ConversationSearchRequest,
  ConversationSearchData,
  AtomicSearchRequest,
} from "./generated/types.js";

// Import schemas we need to override
import type {
  AtomicDetail as GeneratedAtomicDetail,
  AtomicUpdateData as GeneratedAtomicUpdateData,
  ConversationAddData as GeneratedConversationAddData,
  ConversationItem as GeneratedConversationItem,
  ScenarioEntry as GeneratedScenarioEntry,
  ScenarioFile as GeneratedScenarioFile,
  ScenarioWriteData as GeneratedScenarioWriteData,
  CoreFile as GeneratedCoreFile,
  CoreWriteData as GeneratedCoreWriteData,
} from "./generated/types.js";

export interface ConversationItem extends GeneratedConversationItem {
  /** L0 session isolation dimension returned by query/search responses. */
  session_id?: string;
  /** Team ownership dimension. */
  team_id?: string;
  /** L0 user isolation dimension returned by query/search responses. */
  user_id?: string;
  /** L0 agent isolation dimension returned by query/search responses. */
  agent_id?: string;
  /** Optional task ownership dimension for L0/L1 filtering. */
  task_id?: string;
}

export type ConversationSearchHit = ConversationItem & { score: number };
import {
  conversationItemSchema as _conversationItemSchema,
  scenarioReadRequestSchema as _scenarioReadRequestSchema,
  scenarioWriteRequestSchema as _scenarioWriteRequestSchema,
  scenarioRmRequestSchema as _scenarioRmRequestSchema,
} from "./generated/schemas.js";

// ============================
// Override: conversation add default session
// ============================

/**
 * One appended L0 message.
 *
 * `id` is the caller's STABLE message identity. When content history is enabled
 * the id becomes the message locator, so a retried append resolves to the same
 * version instead of creating a random duplicate. `operation_id` is an optional
 * idempotency key for the same purpose. Both are ignored in legacy mode (the
 * generator assigns a random id, as before).
 */
export const conversationAddMessageSchema = _conversationItemSchema.and(z.object({
  id: z.string().min(1).max(200).optional(),
  operation_id: z.string().min(1).max(200).optional(),
  /** 消息来源（§5.2）：conversation（默认）或 framework_event（框架追加的消息）。 */
  source: z.enum(["conversation", "framework_event"]).optional(),
}));
export type ConversationAddMessage = z.infer<typeof conversationAddMessageSchema>;

/** conversationAdd with session_id defaulting to compatibility bucket. */
export const conversationAddRequestSchema = z.object({
  session_id: z.string().min(1).default(DEFAULT_ISOLATION_ID),
  messages: z.array(conversationAddMessageSchema).min(1).max(100),
});
export type ConversationAddRequest = z.infer<typeof conversationAddRequestSchema>;

/**
 * `conversation/add` result. `duplicates` counts messages that were already
 * persisted (same stable id + identical body) and were therefore NOT written
 * again nor re-dispatched to the pipeline.
 */
export interface ConversationAddData extends GeneratedConversationAddData {
  duplicates?: number;
  /** Number of messages actually appended by this request. */
  appended?: number;
}

// ============================
// Count endpoints (sdk-v3.yaml)
// ============================

export interface CountData {
  total: number;
}

export const conversationCountRequestSchema = z.object({
  session_id: z.string().min(1).optional(),
  time_start: z.string().optional(),
  time_end: z.string().optional(),
});
export type ConversationCountRequest = z.infer<typeof conversationCountRequestSchema>;

export const atomicCountRequestSchema = z.object({
  type: z.string().optional(),
  time_start: z.string().optional(),
  time_end: z.string().optional(),
});
export type AtomicCountRequest = z.infer<typeof atomicCountRequestSchema>;

export const scenarioCountRequestSchema = z.object({
  path_prefix: z.string().optional(),
});
export type ScenarioCountRequest = z.infer<typeof scenarioCountRequestSchema>;

export const coreCountRequestSchema = z.object({});
export type CoreCountRequest = z.infer<typeof coreCountRequestSchema>;

// ============================
// Override: atomic response version exposure
// ============================

export interface AtomicDetail extends GeneratedAtomicDetail {
  /** Monotonic L1 memory version, starts from 0 and increments on update/merge. */
  version: number;
  team_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
}

export interface AtomicQueryData {
  items: AtomicDetail[];
  total: number;
}

/**
 * `/atomic/update` result. `source` reports the source state AFTER the update:
 * `declared` when the content was unchanged and the previous source set was
 * preserved, `unknown` when the content actually changed (the old declaration
 * never describes the new bytes).
 */
export type AtomicUpdateData = GeneratedAtomicUpdateData & {
  source?: AtomicSourceView;
};

export type AtomicSearchHit = AtomicDetail & {
  score: number;
  /** L1 来源标注（extraction 提炼 / agent 主动写入 / arbitration 主 Agent 裁决），来自版本元数据。 */
  l1_source?: string;
  /** 本条明确撤回的既有 L1 locator；撤回以新条目表达，旧条目不删。 */
  withdraws?: string;
};

export interface AtomicSearchData {
  items: AtomicSearchHit[];
}

export interface ScenarioEntry extends Omit<GeneratedScenarioEntry, "version"> {
  version?: number;
  summary?: string;
  team_id?: string;
  agent_id?: string;
}

export interface ScenarioListData {
  entries: ScenarioEntry[];
  total: number;
}

export interface ScenarioFile extends Omit<GeneratedScenarioFile, "version"> {
  version?: number;
  team_id?: string;
  agent_id?: string;
  /** Program-provided input scope for this L2 file (optional, backward compatible). */
  sources?: ContentSourceView;
  /** Unified content/version reference (present when history is enabled). */
  ref?: ContentVersionRef;
}

export interface ScenarioWriteData extends Omit<GeneratedScenarioWriteData, "version"> {
  version?: number;
  team_id?: string;
  agent_id?: string;
}

export interface CoreFile extends Omit<GeneratedCoreFile, "version"> {
  version?: number;
  team_id?: string;
  agent_id?: string;
  /** Program-provided input scope for this L3 persona (optional, backward compatible). */
  sources?: ContentSourceView;
  /** Unified content/version reference (present when history is enabled). */
  ref?: ContentVersionRef;
}

export interface CoreWriteData extends Omit<GeneratedCoreWriteData, "version"> {
  version?: number;
  team_id?: string;
  agent_id?: string;
}

// ============================
// Entity metadata schemas (Team / User / Agent / Task)
// ============================

const stringArray = z.array(z.string()).max(100).default([]);
const teamStatusSchema = z.enum(["active", "archived"]);
const userStatusSchema = z.enum(["active", "inactive"]);
const agentStatusSchema = z.enum(["active", "inactive"]);
const agentVisibilitySchema = z.enum(["team", "restricted"]);
const taskSourceTypeSchema = z.enum(["manual", "external", "other"]);

export interface BatchDeleteResult { deleted_ids: string[]; failed: Array<{ id: string; reason: string }> }

export interface TeamData {
  team_id: string; name: string; description?: string; owner_user_id: string; status: "active" | "archived";
  user_ids?: string[]; agent_ids?: string[]; task_ids?: string[]; created_at: string; updated_at: string;
}
export const teamCreateRequestSchema = z.object({ name: z.string().min(1), description: z.string().optional(), owner_user_id: z.string().min(1) });
export const teamGetRequestSchema = z.object({ team_id: z.string().min(1) });
export const teamUpdateRequestSchema = z.object({ team_id: z.string().min(1), name: z.string().min(1).optional(), description: z.string().optional(), owner_user_id: z.string().min(1).optional(), user_ids: z.array(z.string()).max(200).optional(), agent_ids: z.array(z.string()).max(200).optional(), status: teamStatusSchema.optional() });
export const teamBatchDeleteRequestSchema = z.object({ team_ids: z.array(z.string().min(1)).min(1).max(100) });

export interface UserData {
  user_id: string; name: string; job_description?: string; team_ids: string[]; task_ids: string[]; owned_agent_ids: string[]; task_agent_ids?: string[]; status: "active" | "inactive"; created_at: string; updated_at?: string;
}
export const userCreateRequestSchema = z.object({ name: z.string().min(1), job_description: z.string().optional() });
export const userGetRequestSchema = z.object({ user_id: z.string().min(1) });
export const userUpdateRequestSchema = z.object({ user_id: z.string().min(1), name: z.string().min(1).optional(), job_description: z.string().optional(), status: userStatusSchema.optional() });
export const userBatchDeleteRequestSchema = z.object({ user_ids: z.array(z.string().min(1)).min(1).max(100) });

export interface AgentData {
  agent_id: string; team_id: string; name: string; description?: string; prompt?: string; owner_user_id?: string; visibility: "team" | "restricted"; status: "active" | "inactive"; task_ids?: string[]; created_at: string; updated_at: string;
}
export const agentCreateRequestSchema = z.object({ team_id: z.string().min(1), name: z.string().min(1), description: z.string().optional(), prompt: z.string().optional(), owner_user_id: z.string().optional(), visibility: agentVisibilitySchema.optional() });
export const agentGetRequestSchema = z.object({ agent_id: z.string().min(1), team_id: z.string().min(1).optional() });
export const agentUpdateRequestSchema = z.object({ agent_id: z.string().min(1), team_id: z.string().min(1).optional(), name: z.string().min(1).optional(), description: z.string().optional(), prompt: z.string().optional(), owner_user_id: z.string().optional(), visibility: agentVisibilitySchema.optional(), status: agentStatusSchema.optional() });
export const agentBatchDeleteRequestSchema = z.object({ agent_ids: z.array(z.string().min(1)).min(1).max(100) });

export interface TaskData {
  task_id: string; team_id: string; creator_user_id: string; title?: string; description?: string; source_type: "manual" | "external" | "other"; source_url?: string; agent_ids: string[]; user_ids: string[]; created_at: string; updated_at: string;
}
export const taskCreateRequestSchema = z.object({ team_id: z.string().min(1), creator_user_id: z.string().min(1), title: z.string().optional(), description: z.string().optional(), source_type: taskSourceTypeSchema.optional(), source_url: z.string().optional(), agent_ids: stringArray.optional(), user_ids: stringArray.optional() });
export const taskGetRequestSchema = z.object({ task_id: z.string().min(1) });
export const taskUpdateRequestSchema = z.object({ task_id: z.string().min(1), title: z.string().optional(), description: z.string().optional(), source_type: taskSourceTypeSchema.optional(), source_url: z.string().optional(), agent_ids: stringArray.optional(), user_ids: stringArray.optional() });
export const taskBatchDeleteRequestSchema = z.object({ task_ids: z.array(z.string().min(1)).min(1).max(100) });

// ============================
// Override: safe path (prevent path traversal)
// ============================

const safePath = z.string().min(1).refine(
  (p) => !p.includes("\0")
    && !p.includes("\\")
    && !p.startsWith("/")
    && !p.split("/").some((part) => part === ".."),
  { message: "Path must be relative (no '..', no leading '/', no backslash/NUL)" },
);

/** scenarioRead with path traversal prevention. */
export const scenarioReadRequestSchema = z.object({
  path: safePath,
  /**
   * Historical L2 read. Not implemented this round: callers may omit it (old
   * behaviour) but a supplied version is rejected explicitly, never ignored.
   */
  version: z.int().positive().optional(),
  /**
   * New, string-addressed version identity (e.g. `v_3_ab12...`). Only meaningful
   * once content history is enabled; the numeric `version` above must never be
   * confused with it.
   */
  version_id: z.string().min(1).optional(),
});
export type ScenarioReadRequest = z.infer<typeof scenarioReadRequestSchema>;

/** scenarioWrite with path traversal prevention + summary. */
export const scenarioWriteRequestSchema = z.object({
  path: safePath,
  content: z.string(),
  summary: z.string().optional(),
});
export type ScenarioWriteRequest = z.infer<typeof scenarioWriteRequestSchema>;

/** scenarioRm with path traversal prevention. */
export const scenarioRmRequestSchema = z.object({ path: safePath });
export type ScenarioRmRequest = z.infer<typeof scenarioRmRequestSchema>;

/**
 * coreRead request. `version` mirrors scenarioRead: omitted keeps the legacy
 * read, provided is rejected as unsupported (not silently ignored).
 */
export const coreReadRequestSchema = z.object({
  version: z.int().positive().optional(),
  /** String-addressed version identity; see {@link scenarioReadRequestSchema}. */
  version_id: z.string().min(1).optional(),
});
export type CoreReadRequest = z.infer<typeof coreReadRequestSchema>;

// ============================
// Override: content single-read by id (L0/L1)
// ============================

/** L0 single-read by message id. */
export const conversationGetRequestSchema = z.object({
  id: z.string().min(1),
  /** Reserved for historical reads; supplied values are rejected as unsupported. */
  version: z.int().positive().optional(),
  /**
   * Exact content-history version identity. Only meaningful when content
   * history is enabled; an unknown version is an explicit miss, never a silent
   * fall-back to the current row.
   */
  version_id: z.string().min(1).optional(),
});
export type ConversationGetRequest = z.infer<typeof conversationGetRequestSchema>;

/** L1 single-read by memory id (also resolves superseded ids). */
export const atomicGetRequestSchema = z.object({
  id: z.string().min(1),
  /** Reserved for historical reads; supplied values are rejected as unsupported. */
  version: z.int().positive().optional(),
  /** String-addressed version identity; see {@link conversationGetRequestSchema}. */
  version_id: z.string().min(1).optional(),
});
export type AtomicGetRequest = z.infer<typeof atomicGetRequestSchema>;

export interface ConversationGetData {
  message: ConversationItem;
  /** Unified content/version reference (present when history is enabled). */
  ref?: ContentVersionRef;
  /** Full historical version when the read addressed an explicit `version_id`. */
  content_version?: ContentVersionView;
}

/**
 * L1 source view: the model-declared citation set. `declared` only means at
 * least one cited id belonged to this extraction's *addressable input set* —
 * it is not a program verification that the message contributed. `unknown`
 * means none survived; it must not be read as "no evidence".
 */
export interface AtomicSourceView {
  status: "declared" | "unknown";
  kind: "model_declared";
  message_ids: string[];
}

/** Program-provided L2/L3 input view attached to scenario/core reads. */
export type ContentSourceView =
  | {
      status: "known";
      kind: "pipeline_input";
      input_layer: "l1" | "l2";
      input_ids: string[];
      /** Previously retained inputs whose contribution survives this update. */
      retained_input_ids?: string[];
      updated_at_ms: number;
    }
  | { status: "unknown"; input_ids: [] }
  | { status: "unavailable"; input_ids: []; reason: string };

export interface AtomicGetData {
  item: {
    id: string;
    type: string;
    content: string;
    background?: string;
    version: number;
    team_id?: string;
    user_id?: string;
    agent_id?: string;
    task_id?: string;
    created_at: string;
    updated_at: string;
    /** False when this id was replaced; content is the retained old version. */
    current: boolean;
    /**
     * Direct successor of this id (first hop only). Kept for backward
     * compatibility; use `current_id` to find the live record.
     */
    superseded_by?: string;
    /** Alias of `superseded_by` (direct next id, first hop only). */
    successor?: string;
    /**
     * The live id this record ultimately resolves to (following the whole
     * replacement chain). Only present when a live record was found; callers
     * must use this — not `superseded_by` — to address the current record.
     */
    current_id?: string;
    source: AtomicSourceView;
    /** Unified content/version reference (present when history is enabled). */
    ref?: ContentVersionRef;
  };
  /** Full historical version when the read addressed an explicit `version_id`. */
  content_version?: ContentVersionView;
}

// ============================
// Content history read API (§4.8.6)
// ============================
//
// `content/read`, `content/history`, `content/changes` and `content/diff` are
// normal (read-only) content interfaces. They address content by its stable
// `content_id` / version identity — callers never build internal record ids,
// file paths or log-derived ids themselves. An old record id / path is mapped
// to a reference by the existing single-read endpoints (their `ref` field).

export const contentLayerSchema = z.enum(["l0", "l1", "l2", "l3"]);
export type ContentLayer = z.infer<typeof contentLayerSchema>;

/** A stable content/version reference. `version_id` is never a numeric version. */
export interface ContentVersionRef {
  layer: ContentLayer;
  content_id: string;
  version_id: string;
}

/** A synchronous slice of a stored version's lifecycle state. */
export interface ContentVersionStateView {
  version_id: string;
  state: "active" | "retired";
  recorded_at: string;
  sequence: number;
}

/**
 * Full content version as returned by the content read API. Mirrors
 * `ContentVersion` from the shared history contract, restated as a plain
 * JSON-safe shape so the gateway does not leak Node-only types.
 * 2026-10-12 收窄：认识块、关系、事件/适用时间与变化理由字段已撤掉。
 */
export interface ContentVersionView {
  layer: ContentLayer;
  content_id: string;
  locator: string;
  scope: Record<string, string>;
  body: string;
  metadata: Record<string, unknown>;
  state: "active" | "retired";
  ref: ContentVersionRef;
  recorded_at: string;
  sequence: number;
  predecessors: ContentVersionRef[];
  change_kind: string;
  /** False when this identity's version chain is known to be incomplete. */
  history_complete: boolean;
}

/**
 * `content/read` result: the requested version plus its identity's current
 * pointer. 比较两个版本、推导关系由读取方的模型现场完成，不再由接口预建。
 */
export interface ContentReadData {
  version: ContentVersionView;
  /** True when the requested version is the identity's current version. */
  is_current: boolean;
  /** Present only when the identity has a different current version. */
  current?: ContentVersionStateView;
  /** True when the identity has any newer version than the requested one. */
  has_successor: boolean;
  /**
   * The newest version of the SAME identity, when it differs from the requested
   * one.
   */
  latest_version?: ContentVersionStateView;
  /**
   * Present only for a historical (`as_of`) read: the boundary the returned
   * version was in force at.
   */
  as_of?: string;
  /**
   * Present only for a historical (`as_of`) read: `is_current`/`current` above
   * describe NOW, not the `as_of` boundary — so today's state is never read as
   * "current then".
   */
  current_is_now?: boolean;
}

export interface ContentHistoryData {
  items: ContentVersionView[];
  upper_sequence: number;
  /** Opaque cursor for the next page, or null when this is the last page. */
  next_cursor: string | null;
  history_complete: boolean;
}

/**
 * Per-layer coverage of a cross-layer `content/changes` page. A layer that is
 * unavailable or has history disabled is reported explicitly — never silently
 * omitted so the caller thinks the page covered it.
 */
export interface ContentChangesLayerView {
  status: "ok" | "unavailable" | "disabled" | "error" | "scan_capped";
  message?: string;
  /** The pinned per-layer commit boundary this page scanned up to. */
  upper_sequence?: number;
  history_complete?: boolean;
}

/**
 * `content/changes` result. A cross-layer page is ordered by
 * `(recorded_at, layer, sequence, version_id)` — a *display* order, not a global
 * transaction order (same-millisecond commits across stores are not a tx log).
 */
export interface ContentChangesData {
  items: ContentVersionView[];
  next_cursor: string | null;
  history_complete: boolean;
  /** Present for a single-layer request (backward compatible). */
  upper_sequence?: number;
  /** Present only for a cross-layer request. */
  partial?: boolean;
  /** Present only for a cross-layer request. */
  layers?: Partial<Record<ContentLayer, ContentChangesLayerView>>;
  /** Present only for a cross-layer request. */
  ordering?: { by: ["recorded_at", "layer", "sequence", "version_id"]; global_transaction_order: false };
}

const contentVersionRefSchema = z.object({
  layer: contentLayerSchema,
  content_id: z.string().min(1),
  version_id: z.string().min(1),
});

export const contentReadRequestSchema = z.object({
  layer: contentLayerSchema,
  content_id: z.string().min(1).optional(),
  /** 稳定 locator（L0 消息 id / L1 记忆编号 / L2 专题路径 / L3 文档名），与 content_id 二选一。 */
  locator: z.string().min(1).max(1024).optional(),
  version_id: z.string().min(1).optional(),
  as_of: z.string().min(1).optional(),
}).refine((data) => (data.content_id === undefined) !== (data.locator === undefined), {
  message: "exactly one of content_id or locator is required",
}).refine((data) => !(data.version_id !== undefined && data.as_of !== undefined), {
  message: "version_id and as_of are mutually exclusive",
});
export type ContentReadRequest = z.infer<typeof contentReadRequestSchema>;

const recordedTimeFilter = {
  from_recorded_at: z.string().min(1).optional(),
  to_recorded_at: z.string().min(1).optional(),
  limit: z.int().positive().max(500).optional(),
  cursor: z.string().min(1).optional(),
} as const;

export const contentHistoryRequestSchema = z.object({
  layer: contentLayerSchema,
  content_id: z.string().min(1),
  ...recordedTimeFilter,
});
export type ContentHistoryRequest = z.infer<typeof contentHistoryRequestSchema>;

/**
 * `content/changes` browses recorded-time changes. When `layer` is omitted the
 * request is a cross-layer (L0..L3) time-line; a single `layer` keeps the
 * existing single-layer behaviour exactly. A cross-layer page pins each
 * reader's `upper_sequence` and reports uncovered layers explicitly instead of
 * silently answering with one layer.
 */
export const contentChangesRequestSchema = z.object({
  layer: contentLayerSchema.optional(),
  content_id: z.string().min(1).optional(),
  ...recordedTimeFilter,
});
export type ContentChangesRequest = z.infer<typeof contentChangesRequestSchema>;

// ============================
// content/diff 与 content/relations 已撤掉（2026-10-12）
// ============================
//
// 两版差异与关系查询（正向/反查/多跳）不再由接口提供：比较两个版本、推导
// 关系由读取方的模型现场完成。

// ============================
// content/write — the controlled normal write entry（2026-10-12 收窄版）
// ============================
//
// 收窄后的写规则（docs/memory-design-proposal.md §5、implementation-next.md §2）：
//   - L0：只允许 create（追加对话文字；同 id 不同内容为 409）。
//   - L1：允许 create（原子认识，必标来源 extraction/agent/arbitration）、retire
//     （按 locator 撤回一条既有认识：不写正文、当前行移除、历史保留）与 retag
//     （只改归属元数据 scene_name/type/priority：正文由当前版本提供、locator 与
//     历史保留；纠正错误正文不在这条路径上）；
//     纠正（新认识取代旧认识）仍用 create + `withdraws`。
//   - L2：拒绝外部直接写入（只由内部整理流程发布）。
//   - L3：只接受三份文档（user-understanding.md / beliefs.md / current-focus.md）
//     的 create/revise，且各设篇幅上限（只由主 Agent 写入，框架按角色配工具）。
//
// The caller may only address ONE layer and ONE mutation per request; the scope
// is never taken from the body but derived from the trusted request isolation.
// recorded_at 是程序时钟；canonical metadata 由程序组装。

export const contentWriteChangeKindSchema = z.enum([
  "create",
  "revise",
  "retire",
  "restore",
  /** L1 only: change the same identity's scene_name/type/priority, never its body. */
  "retag",
]);
export type ContentWriteChangeKind = z.infer<typeof contentWriteChangeKindSchema>;

/** L0 role whitelist: a message reference is never promoted into an instruction. */
export const contentWriteL0RoleSchema = z.enum(["user", "assistant", "system", "tool"]);

/** L0 消息来源（§5.2）：对话文字或框架事件。 */
export const contentWriteL0SourceSchema = z.enum(["conversation", "framework_event"]);

/** L1 来源标记（§5.3）：提炼 / 主动写入 / 主 Agent 裁决。 */
export const l1SourceSchema = z.enum(["extraction", "agent", "arbitration"]);
export type L1Source = z.infer<typeof l1SourceSchema>;

export const contentWriteRequestSchema = z.object({
  layer: contentLayerSchema,
  /** Caller-supplied idempotency key; the same id + payload replays, a different payload conflicts. */
  operation_id: z.string().min(1).max(200),
  change: contentWriteChangeKindSchema,
  /** Stable content identity. Required for revise/retire/restore; forbidden for new identities. */
  content_id: z.string().min(1).max(200).optional(),
  /** CAS baseline. Required for identity-preserving changes; null/absent for new identities. */
  expected_version_id: z.string().min(1).max(200).nullable().optional(),
  /** L0/L1: record id. L3: 三份文档之一（L2 不接受外部写入）。 */
  locator: z.string().min(1).max(1024).optional(),
  body: z.string().optional(),
  /** L0 raw message fields (session is mandatory for L0 writes). */
  role: contentWriteL0RoleSchema.optional(),
  session_id: z.string().min(1).max(200).optional(),
  message_timestamp: z.string().min(1).optional(),
  /** L0 消息来源；缺省 conversation。 */
  source: contentWriteL0SourceSchema.optional(),
  /** L1 record semantics (canonical fields are assembled by Core, never trusted verbatim). */
  type: z.string().max(100).optional(),
  priority: z.int().min(-1).max(100).optional(),
  scene_name: z.string().max(500).optional(),
  source_message_ids: z.array(z.string().min(1)).max(200).optional(),
  timestamps: z.array(z.string().min(1)).max(200).optional(),
  /** L1 来源标记；提炼管线写入恒为 extraction，会话 Agent 主动写入为 agent，主 Agent 裁决为 arbitration。 */
  l1_source: l1SourceSchema.optional(),
  /** L1 撤回：本条 L1 明确撤回哪一条既有 L1（其 locator）。旧条目保留，读取方据此调和。 */
  withdraws: z.string().min(1).max(1024).optional(),
  /** The caller's OWN metadata (kept in a separate partition; reserved keys rejected). */
  user_metadata: z.record(z.string(), z.unknown()).optional(),
});
export type ContentWriteRequest = z.infer<typeof contentWriteRequestSchema>;

/** `content/write` result: the exact committed version(s) (body + source + ref agree). */
export interface ContentWriteData {
  /**
   * True only when THIS request produced at least one new version. False on an
   * idempotent replay (the operation was already applied) and on a byte-identical
   * no-op — even when the original operation did change something.
   */
  changed: boolean;
  /** The first committed version's ref. Kept for single-mutation callers. */
  ref: ContentVersionRef;
  /** The first committed version. Kept for single-mutation callers. */
  version: ContentVersionView;
  /** Every committed version ref, in request order. */
  refs: ContentVersionRef[];
  /** Every committed version, in request order. */
  versions: ContentVersionView[];
}

// ============================
// Override: L0 / L1 batch delete
// ============================

/** L0 单次批量删除上限：message_ids 5000 条、session_ids 100 条。 */
export const L0_DELETE_MESSAGE_IDS_MAX = 5000;
export const L0_DELETE_SESSION_IDS_MAX = 100;
/** L1 单次批量删除上限。 */
export const L1_DELETE_IDS_MAX = 5000;

/** 去重 + 丢弃空串，保持原顺序。 */
const dedupeIdList = (ids: string[]): string[] => {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of ids) {
    const id = raw.trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(id);
  }
  return out;
};

/**
 * conversationDelete：message_ids / session_ids 至少填一个，可同时填。
 *
 * 兼容旧调用方：保留单数 `session_id`，解析后归一到 `session_ids`，
 * 所以handler 只需处理 `session_ids` 一条路径。
 */
export const conversationDeleteRequestSchema = z.object({
  message_ids: z.array(z.string()).min(1).max(L0_DELETE_MESSAGE_IDS_MAX).optional(),
  session_ids: z.array(z.string()).min(1).max(L0_DELETE_SESSION_IDS_MAX).optional(),
  /** @deprecated 改用 `session_ids`；仍支持以兼容现网调用方。*/
  session_id: z.string().optional(),
}).transform((data) => {
  const messageIds = dedupeIdList(data.message_ids ?? []);
  const sessionIds = dedupeIdList([
    ...(data.session_ids ?? []),
    ...(data.session_id !== undefined ? [data.session_id] : []),
  ]);
  return { message_ids: messageIds, session_ids: sessionIds };
}).refine(
  (data) => data.message_ids.length > 0 || data.session_ids.length > 0,
  { message: "At least one of message_ids or session_ids must be provided" },
).refine(
  (data) => data.session_ids.length <= L0_DELETE_SESSION_IDS_MAX,
  { message: `session_ids must contain at most ${L0_DELETE_SESSION_IDS_MAX} items` },
);
export type ConversationDeleteRequest = z.infer<typeof conversationDeleteRequestSchema>;

/** atomicDelete：ids 必填，单次至多 5000 条，自动去重。 */
export const atomicDeleteRequestSchema = z.object({
  ids: z.array(z.string()).min(1).max(L1_DELETE_IDS_MAX),
}).transform((data) => ({ ids: dedupeIdList(data.ids) })).refine(
  (data) => data.ids.length > 0,
  { message: "ids must contain at least one non-empty id" },
);
export type AtomicDeleteRequest = z.infer<typeof atomicDeleteRequestSchema>;

// ============================
// Tenancy isolation extension
// ============================
//
// L0/L1 carry (user_id, agent_id, session_id); L2/L3 Profile is agent-level
// and uses (user_id, agent_id). The generated request schemas only validate `session_id`; user_id / agent_id arrive
// either in the request body or in HTTP headers. This block keeps the
// generated schemas untouched and adds a separate schema for the new fields,
// so the router can validate body and headers independently.

/** Headers / body fields used to carry the three-dim isolation context. */
export const isolationFieldsSchema = z.object({
  user_id: z.string().min(1).default(DEFAULT_ISOLATION_ID),
  agent_id: z.string().min(1).default(DEFAULT_ISOLATION_ID),
  session_id: z.string().min(1).default(DEFAULT_ISOLATION_ID),
}).passthrough();
export type IsolationFields = z.infer<typeof isolationFieldsSchema>;

/**
 * Pull the three-dim isolation triple from the parsed body or from request
 * headers. Body wins (consistent with how `session_id` is currently sourced).
 *
 * Missing fields are filled with the default isolation bucket. This keeps the
 * API backward-compatible while still making ownership explicit in storage.
 */
export function resolveIsolation(
  body: Record<string, unknown> | undefined,
  headers: Record<string, string | string[] | undefined>,
  opts: { legacyCompatMode?: boolean; legacyPlaceholder?: string } = {},
): { ok: true; ctx: { userId: string; agentId: string; sessionId: string; taskId?: string } } {
  const headerStr = (k: string): string | undefined => {
    const raw = headers[k] ?? headers[k.toLowerCase()];
    if (Array.isArray(raw)) return raw[0];
    return typeof raw === "string" ? raw : undefined;
  };
  const teamId = (body?.team_id as string | undefined) ?? headerStr("x-tdai-team-id") ?? "";
  const userId = (body?.user_id as string | undefined) ?? headerStr("x-tdai-user-id") ?? "";
  const agentId = (body?.agent_id as string | undefined) ?? headerStr("x-tdai-agent-id") ?? "";
  const sessionId =
    (body?.session_id as string | undefined)
    ?? headerStr("x-tdai-session-id")
    ?? "";
  const taskId =
    (body?.task_id as string | undefined)
    ?? headerStr("x-tdai-task-id")
    ?? undefined;
  const missing: string[] = [];
  if (!userId) missing.push("user_id");
  if (!agentId) missing.push("agent_id");
  if (!sessionId) missing.push("session_id");

  const ph = opts.legacyCompatMode ? (opts.legacyPlaceholder ?? DEFAULT_ISOLATION_ID) : DEFAULT_ISOLATION_ID;
  const ctx = { ...(teamId ? { teamId } : {}), userId: userId || ph, agentId: agentId || ph, sessionId: sessionId || ph, ...(taskId ? { taskId } : {}) };
  return { ok: true, ctx };
}

// ============================
// Generic API Response Envelope
// ============================

export interface ApiResponseEnvelope<T = unknown> {
  code: number;
  message: string;
  request_id: string;
  data?: T;
}

// ============================
// Zod error formatter
// ============================

export function formatZodError(error: z.ZodError): string {
  return error.issues
    .map((issue) => {
      const path = issue.path.length > 0 ? issue.path.join(".") : "(root)";
      return `${path}: ${issue.message}`;
    })
    .join("; ");
}

// ============================
// Auth context
// ============================

export const v2AuthContextSchema = z.object({
  apiKey: z.string().min(1),
  serviceId: z.string().min(1),
});
export type V2AuthContext = z.infer<typeof v2AuthContextSchema>;
