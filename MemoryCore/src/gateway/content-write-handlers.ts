/**
 * `content/write` — the ONE normal (manual) content write entry（2026-10-12 收窄版）.
 *
 * 收窄后的写规则（docs/memory-design-proposal.md §5、implementation-next.md §2）
 * ----------------------------------------------------------------------
 *   - **L0：只追加**（change=create）。locator 是稳定消息 id，session_id 必填；
 *     同 id 不同内容为 409；消息来源可标为框架事件（framework_event）。
 *     不再支持 revise：L0 正常只追加，不改原话。
 *   - **L1：新增、撤回或改归属**（change=create / retire / retag）。create 是原子认识，
 *     必须标来源 l1_source（extraction 提炼 / agent 主动写入 / arbitration 主 Agent
 *     裁决）；纠正（新认识取代旧认识）用 create + `withdraws`，旧条目原样保留，
 *     读取方按时间与来源调和。retire 撤回一条既有认识：只按 locator 定位（给
 *     expected_version_id 则做 CAS 校验），不写正文、不带来源，当前行连同
 *     FTS/向量在一个事务里移除，版本历史保留。撤回不是新认识，不该再占一条记忆。
 *     retag 只改归属元数据（scene_name / type / priority）：正文取自当前版本、
 *     locator 与历史保留，其余元数据（l1_source、withdraws、user_metadata.note 等）
 *     逐字沿用；它不能改正文，因此不是 revise 的别名。
 *   - **L2：拒绝外部直接写入**（只由内部整理流程发布）。
 *   - **L3：只接受三份文档**（user-understanding.md / beliefs.md /
 *     current-focus.md）的 create/revise，各设篇幅上限；只由主 Agent 写入
 *     （写入者身份由框架的工具配置保证，不依赖 Core 识别角色）。
 *
 * 提交路径不变：L0/L1 走 SQLite `commitContentChanges`（当前行 + FTS/向量 +
 * 不可变版本一个事务），L3 走本地 GenerationWorkspace + history planner
 * （staged 文档 + 版本链一个 bundle 一次指针切换）。operation_id 幂等与
 * expected_version_id CAS 语义与收窄前一致。
 */

import { createHash } from "node:crypto";
import type { EmbeddingService } from "../core/store/embedding.js";
import type { CommittedOperationRecord, CommittedOperationReader, IMemoryStore, IsolationFilter } from "../core/store/types.js";
import { DEFAULT_ISOLATION_ID } from "../core/store/types.js";
import type {
  ChangeCommit,
  ContentVersion,
  HistoryLayer,
  HistoryScope,
  VersionMutation,
  VersionRef,
} from "../core/content/history-contract.js";
import {
  buildContentHistoryAuthFilter,
  profileHistoryScope,
  readVersionAsync,
} from "../core/content/history-reader.js";
import { buildL1VersionMetadata, PROGRAM_REQUEST_METADATA_KEY } from "../core/store/sqlite/content-history.js";
import type { LocalContentHistory } from "../core/storage/local-content-history.js";
import { GenerationWorkspace } from "../core/storage/generation-workspace.js";
import {
  getLocalPublishRootOf,
  type ContentHistoryCommitOutcome,
  type ContentHistoryNoop,
} from "../core/storage/local-published-storage.js";
import { scopeProfileStorageView, type StorageAdapter } from "../core/storage/adapter.js";
import { isL3DocumentLocator, L3_DOCUMENT_DEFAULT_LIMITS, type L3DocumentName } from "../core/storage/types.js";
import { buildProfileIsolationScope } from "../core/profile/profile-scope.js";
import type { Logger } from "../core/types.js";
import { contentVersionView } from "./content-history-handlers.js";
import {
  contentWriteRequestSchema,
  formatZodError,
  type ApiResponseEnvelope,
  type ContentWriteData,
  type ContentWriteRequest,
} from "./v2-schemas.js";

const TAG = "[tdai-gateway][content-write]";

// ============================
// Dependencies (structural subset of V2RouterDeps)
// ============================

export interface ContentWriteDeps {
  getStore: () => IMemoryStore | undefined;
  getStorage: () => StorageAdapter | undefined;
  /** Optional embedding service; a missing one writes metadata + FTS only. */
  getEmbedding?: () => EmbeddingService | undefined;
  logger?: Logger;
  requestIsolation?: { teamId?: string; userId: string; agentId: string; sessionId: string; taskId?: string };
  /** L3 三份文档的篇幅上限（字符数）覆盖项；缺省用内置默认值。 */
  l3DocumentLimits?: Partial<Record<L3DocumentName, number>>;
}

// ============================
// Envelope helpers
// ============================

function ok<T>(data: T, requestId: string): ApiResponseEnvelope {
  return { code: 0, message: "ok", request_id: requestId, data };
}

function fail(status: number, message: string, requestId: string): ApiResponseEnvelope {
  return { code: status, message, request_id: requestId };
}

/** A caller-facing write failure with an explicit transport status. */
class ContentWriteRequestError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
    this.name = "ContentWriteRequestError";
  }
}

/**
 * Metadata keys the program owns. A caller that sets any of them would be
 * impersonating system metadata (timestamp/source/identity), so the request is
 * rejected instead of silently overwriting the canonical value.
 */
const RESERVED_METADATA_KEYS: ReadonlySet<string> = new Set([
  "user_metadata",
  "type",
  "priority",
  "scene_name",
  "timestamps",
  "session_key",
  "session_id",
  "task_id",
  "source_message_ids",
  "source_refs",
  "message_recorded_at",
  "original_timestamp_ms",
  "role",
  "source",
  "withdraws",
  "_legacy_import",
  "content_sources",
]);

// ============================
// Entry
// ============================

export async function handleContentWrite(
  body: unknown,
  _auth: unknown,
  requestId: string,
  deps: ContentWriteDeps,
): Promise<ApiResponseEnvelope> {
  const parsed = contentWriteRequestSchema.safeParse(body);
  if (!parsed.success) return fail(400, formatZodError(parsed.error), requestId);
  try {
    return await runContentWrite(parsed.data, deps, requestId);
  } catch (err) {
    const envelope = failureEnvelope(err, requestId);
    if (envelope.code >= 500) {
      deps.logger?.error?.(
        `${TAG} write failed [${errorCode(err)}]: ${err instanceof Error ? err.message : String(err)}`,
      );
    } else {
      deps.logger?.warn?.(`${TAG} write rejected (${envelope.code}): ${envelope.message}`);
    }
    return envelope;
  }
}

/** Stable, non-secret code for logs/observability (never a message body). */
function errorCode(err: unknown): string {
  const candidate = (err as { code?: unknown } | null)?.code;
  if (typeof candidate === "string" && candidate) return candidate;
  if (err instanceof Error && err.name) return err.name;
  return "unknown";
}

function failureEnvelope(err: unknown, requestId: string): ApiResponseEnvelope {
  if (err instanceof ContentWriteRequestError) return fail(err.status, err.message, requestId);
  const code = (err as { code?: unknown } | null)?.code;
  if (typeof code === "string") {
    const status = mapErrorCode(code);
    if (status !== null) return fail(status, errorMessage(err), requestId);
  }
  if ((err as { name?: string } | null)?.name === "HistoryContextRequiredError") {
    return fail(409, "history_context_required: a content commit must carry an explicit history context", requestId);
  }
  if ((err as { name?: string } | null)?.name === "HistoryDeleteUndefinedError") {
    return fail(409, "history_delete_undefined: destructive delete is not defined while content history is enabled", requestId);
  }
  return fail(500, "content_write_failed: the write did not complete", requestId);
}

function errorMessage(err: unknown): string {
  if (err instanceof Error && err.message) return err.message;
  return "content_write_failed";
}

/** Stable error code → transport status. Unknown codes return null. */
function mapErrorCode(code: string): number | null {
  switch (code) {
    case "history_invalid_input":
    case "invalid_read":
    case "invalid_page":
    case "invalid_scope":
    case "invalid_mutation":
    case "invalid_locator":
    case "changed_output_not_covered":
    case "source_plan_mismatch":
    case "unknown_predecessor":
    case "history_write_degraded":
      return 400;
    case "history_scope_mismatch":
    case "scope_mismatch":
      return 403;
    case "unknown_content_id":
    case "version_not_found":
      return 404;
    case "history_operation_conflict":
    case "operation_conflict":
    case "history_version_conflict":
    case "stale_expected_version":
    case "history_locator_conflict":
    case "locator_in_use":
    case "content_id_exists":
    case "history_predecessor_not_current":
    case "predecessor_not_current":
    case "history_retired_conflict":
    case "history_current_diverged":
    case "history_current_conflict":
    case "history_locator_superseded":
    case "history_not_enabled":
    case "history_context_required":
    case "history_delete_undefined":
    case "history_as_of_ambiguous":
    case "clock_regressed":
    case "staged_content_missing":
    case "staged_content_mismatch":
      return 409;
    case "history_layer_unsupported":
    case "history_bound_commit_unsupported":
    case "history_restore_unsupported":
    case "history_unavailable":
    case "history_scope_query_unsupported":
      return 501;
    case "history_corrupt":
    case "history_read_failed":
    case "history_init_failed":
    case "history_manifest_missing":
    case "history_cycle":
    case "history_too_deep":
    case "history_sequence_not_monotonic":
    case "missing_version_body":
    case "body_checksum_mismatch":
    case "history_capability_error":
      return 500;
    default:
      return null;
  }
}

// ============================
// Request normalisation
// ============================

/**
 * Canonical JSON projection used for the request fingerprint. Object keys are
 * sorted, `undefined` values dropped and arrays kept in order, so two
 * structurally identical requests always hash the same regardless of key order.
 */
function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      const entry = (value as Record<string, unknown>)[key];
      if (entry === undefined) continue;
      out[key] = canonicalize(entry);
    }
    return out;
  }
  return value;
}

/** The normalized client-request fingerprint (everything except operation_id). */
function fingerprintRequest(req: ContentWriteRequest): string {
  const semantic: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(req)) {
    if (key === "operation_id") continue;
    semantic[key] = value;
  }
  return createHash("sha256").update(JSON.stringify(canonicalize(semantic))).digest("hex");
}

/**
 * Look up an already-committed operation in this request's trusted scope. For
 * L0/L1 this reads the SQLite history's own operation table; for L3 it reads
 * the local planner's current-reachable bundle chain.
 */
async function lookupCommittedOperation(
  deps: ContentWriteDeps,
  layer: HistoryLayer,
  scope: HistoryScope,
  operationId: string,
): Promise<CommittedOperationRecord | null> {
  if (layer === "l0" || layer === "l1") {
    let reader: unknown;
    try {
      reader = deps.getStore()?.getContentHistory?.();
    } catch (err) {
      throw new ContentWriteRequestError(
        500,
        `history_capability_error: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
    const getter = (reader as Partial<CommittedOperationReader> | undefined)?.getCommittedOperation;
    if (typeof getter !== "function") return null;
    return await getter.call(reader as CommittedOperationReader, scope, operationId);
  }
  const history = getLocalHistory(deps);
  return await history.getCommittedOperation(scope, operationId);
}

// ============================
// Core flow
// ============================

async function runContentWrite(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  requestId: string,
): Promise<ApiResponseEnvelope> {
  const layer = req.layer;

  // 1. 收窄的写规则（见文件头）。
  if (layer === "l2") {
    return fail(
      400,
      "l2_external_write_rejected: L2 专题只由内部整理流程发布，不接受外部直接写入 " +
        "(docs/memory-design-proposal.md §5.4)",
      requestId,
    );
  }
  if (layer === "l0" && req.change !== "create") {
    return fail(
      400,
      `append_only_layer: ${layer} 只支持 create（追加）；L0 不改原话`,
      requestId,
    );
  }
  // L1 多 retire 与 retag：前者撤回一条既有原子认识（当前行移除，历史保留），
  // 后者只改归属元数据（正文不可改）。纠正（新认识取代旧认识）仍用 create + withdraws。
  if (layer === "l1" && req.change !== "create" && req.change !== "retire"
      && req.change !== "retag") {
    return fail(
      400,
      `append_only_layer: ${layer} 只支持 create（新增认识）、retire（撤回既有认识）` +
        "与 retag（只改归属元数据）；纠正请再追加一条带 l1_source/withdraws 的 L1",
      requestId,
    );
  }
  if (layer === "l3" && req.change !== "create" && req.change !== "revise") {
    return fail(400, `l3_change_unsupported: L3 只接受三份文档的 create/revise`, requestId);
  }

  // 2. Reserved metadata keys may never be supplied by the caller.
  const reserved = Object.keys(req.user_metadata ?? {}).filter((key) => RESERVED_METADATA_KEYS.has(key));
  if (reserved.length > 0) {
    return fail(
      400,
      `reserved_metadata_key: user_metadata may not set program-owned key(s): ${reserved.join(", ")}`,
      requestId,
    );
  }

  // 3. Structural validation per layer.
  validateStructure(req);

  // 4. Trusted isolation → object scope. The body can never widen it.
  const iso = deps.requestIsolation ?? {
    userId: DEFAULT_ISOLATION_ID,
    agentId: DEFAULT_ISOLATION_ID,
    sessionId: DEFAULT_ISOLATION_ID,
  };
  const scope = resolveWriteScope(req, iso);

  // 5. Capability pre-check.
  const capability = layer === "l0" || layer === "l1"
    ? storeCapabilityCheck(deps, requestId)
    : await localCapabilityCheck(deps, requestId);
  if (capability) return capability;

  // 6. Idempotent replay BEFORE any current-state read.
  const fingerprint = fingerprintRequest(req);
  const alreadyCommitted = await lookupCommittedOperation(deps, layer, scope, req.operation_id);
  if (alreadyCommitted && alreadyCommitted.request_fingerprint !== null) {
    if (alreadyCommitted.request_fingerprint !== fingerprint) {
      return fail(
        409,
        `operation_conflict: operation_id "${req.operation_id}" was already used with a different request payload`,
        requestId,
      );
    }
    return ok<ContentWriteData>(buildWriteData(false, alreadyCommitted.result.versions), requestId);
  }

  // 7. Reconstruct the mutation from its EXACT baseline (never from "today's current").
  //    L1 retire / retag 不需要调用方给基线：两者都按 locator 解析当前版本
  //    （见 buildL1RetireMutation / buildL1RetagMutation）。
  const baseline = req.change === "create"
    || (layer === "l1" && (req.change === "retire" || req.change === "retag"))
    ? null
    : await resolveBaseline(deps, layer, req.content_id!, req.expected_version_id!, scope);
  const mutations = [await buildMutation(req, deps, scope, baseline)];
  mutations[0].draft.metadata = {
    ...(mutations[0].draft.metadata ?? {}),
    [PROGRAM_REQUEST_METADATA_KEY]: fingerprint,
  };

  // 8. Dispatch to the store (L0/L1) or the local planner (L3).
  const commit: ChangeCommit = { operation_id: req.operation_id, actor: "user", mutations };
  try {
    if (layer === "l0" || layer === "l1") {
      return await runStoreWrite(req, deps, requestId, iso, commit, scope);
    }
    return await runLocalWrite(req, deps, requestId, scope, commit);
  } catch (err) {
    // An exact operation conflict (a concurrent writer committed between our
    // replay lookup and this commit) is resolved by re-reading the SAME
    // operation: only an identical request is treated as the replay.
    const code = (err as { code?: unknown } | null)?.code;
    if (code === "operation_conflict" || code === "history_operation_conflict") {
      const committed = await lookupCommittedOperation(deps, layer, scope, req.operation_id);
      if (committed && committed.request_fingerprint === fingerprint) {
        return ok<ContentWriteData>(buildWriteData(false, committed.result.versions), requestId);
      }
      return fail(409, `operation_conflict: ${errorMessage(err)}`, requestId);
    }
    throw err;
  }
}

/** Read the EXACT baseline version an identity-preserving change names. */
async function resolveBaseline(
  deps: ContentWriteDeps,
  layer: HistoryLayer,
  contentId: string,
  expectedVersionId: string,
  scope: HistoryScope,
): Promise<ContentVersion> {
  const version = await readExactVersion(deps, layer, contentId, expectedVersionId, scope);
  if (!version) {
    throw new ContentWriteRequestError(
      404,
      "unknown_content_id: the addressed identity or expected version is unknown or outside this scope",
    );
  }
  return version;
}

function validateStructure(req: ContentWriteRequest): void {
  const l1Retire = req.layer === "l1" && req.change === "retire";
  const l1Retag = req.layer === "l1" && req.change === "retag";
  if (req.change === "create") {
    if (req.expected_version_id !== undefined && req.expected_version_id !== null) {
      throw new ContentWriteRequestError(400, "expected_version_id must be null/absent for a new identity");
    }
    if (req.content_id !== undefined) {
      throw new ContentWriteRequestError(400, "content_id must be absent for a new identity (the program allocates it)");
    }
    if (typeof req.body !== "string" && req.layer !== "l0") {
      // L0 body may default to "" only when content is genuinely empty; L1/L3 require a body.
      throw new ContentWriteRequestError(400, "body is required for create");
    }
  } else if (l1Retire) {
    // 撤回不写正文：正文由被撤回的那一版提供（Core 在 store 层取 prior）。
    if (typeof req.body === "string" && req.body.length > 0) {
      throw new ContentWriteRequestError(400, "body must be absent for an L1 retire（撤回不写新认识）");
    }
  } else if (l1Retag) {
    // retag 只改归属：正文由当前版本提供，调用方给出正文就是想把这条路当 revise。
    if (req.body !== undefined) {
      throw new ContentWriteRequestError(
        400,
        "body must be absent for an L1 retag（retag 不改正文；要点正文请写新认识并用 withdraws 关联旧条目）",
      );
    }
    if (req.content_id !== undefined) {
      throw new ContentWriteRequestError(400, "content_id must be absent for an L1 retag（按 locator 定位当前版本）");
    }
    if (req.scene_name === undefined && req.type === undefined && req.priority === undefined) {
      throw new ContentWriteRequestError(
        400,
        "retag needs at least one of scene_name / type / priority（否则没有要改的归属字段）",
      );
    }
  } else {
    if (!req.content_id) throw new ContentWriteRequestError(400, "content_id is required to change an existing identity");
    if (!req.expected_version_id) {
      throw new ContentWriteRequestError(400, "expected_version_id is required for an identity-preserving change");
    }
    if (req.change === "revise" && typeof req.body !== "string") {
      throw new ContentWriteRequestError(400, "body is required for revise");
    }
  }

  if (req.layer === "l0") {
    if (!req.session_id) {
      throw new ContentWriteRequestError(400, "session_id is required for an L0 write (an L0 message belongs to a session)");
    }
    if (req.locator === undefined) {
      throw new ContentWriteRequestError(400, "locator (stable message id) is required for an L0 write");
    }
    if (req.l1_source !== undefined || req.withdraws !== undefined || req.type !== undefined || req.scene_name !== undefined) {
      throw new ContentWriteRequestError(400, "l1_source/withdraws/type/scene_name do not apply to an L0 write");
    }
  }

  if (req.layer === "l1") {
    if (req.locator === undefined) {
      throw new ContentWriteRequestError(400, "locator is required for an L1 write (the program-supplied record id)");
    }
    if (l1Retag) {
      // 归属之外的东西一律不接受：来源、撤回关系、来源消息、时间与用户元数据
      // 都从当前版本逐字沿用，调用方覆盖它们就不是“改归属”了。
      if (req.l1_source !== undefined || req.withdraws !== undefined
          || req.source_message_ids !== undefined || req.timestamps !== undefined
          || req.user_metadata !== undefined) {
        throw new ContentWriteRequestError(
          400,
          "l1_source/withdraws/source_message_ids/timestamps/user_metadata do not apply to an L1 retag" +
            "（只改归属字段，其余元数据原样保留）",
        );
      }
    } else if (l1Retire) {
      // 撤回只按 locator 定位：来源、正文属性、撤回关系都不属于撤回本身。
      if (req.l1_source !== undefined || req.withdraws !== undefined || req.type !== undefined
          || req.priority !== undefined || req.scene_name !== undefined
          || req.source_message_ids !== undefined || req.timestamps !== undefined
          || req.user_metadata !== undefined) {
        throw new ContentWriteRequestError(
          400,
          "l1_source/withdraws/type/priority/scene_name/source_message_ids/timestamps/user_metadata " +
            "do not apply to an L1 retire（撤回不产生新认识，也不带新元数据）",
        );
      }
    } else if (!req.l1_source) {
      throw new ContentWriteRequestError(
        400,
        "l1_source is required: 每条 L1 必须标明来源（extraction 提炼 / agent 主动写入 / arbitration 主 Agent 裁决）",
      );
    }
    if (req.source !== undefined || req.role !== undefined || req.message_timestamp !== undefined) {
      throw new ContentWriteRequestError(400, "source/role/message_timestamp do not apply to an L1 write");
    }
  }

  if (req.layer === "l3") {
    if (req.l1_source !== undefined || req.withdraws !== undefined || req.type !== undefined
        || req.source_message_ids !== undefined || req.timestamps !== undefined || req.priority !== undefined
        || req.scene_name !== undefined || req.source !== undefined || req.role !== undefined
        || req.message_timestamp !== undefined) {
      throw new ContentWriteRequestError(400, "L0/L1-only fields do not apply to an L3 write");
    }
  }
}

type Iso = { teamId?: string; userId: string; agentId: string; sessionId: string; taskId?: string };

function resolveWriteScope(req: ContentWriteRequest, iso: Iso): HistoryScope {
  if (req.layer === "l0") {
    const scope: HistoryScope = {
      team_id: iso.teamId || DEFAULT_ISOLATION_ID,
      user_id: iso.userId || DEFAULT_ISOLATION_ID,
      agent_id: iso.agentId || DEFAULT_ISOLATION_ID,
      session_id: req.session_id ?? DEFAULT_ISOLATION_ID,
    };
    if (iso.taskId) scope.task_id = iso.taskId;
    return scope;
  }
  if (req.layer === "l1") {
    const scope: HistoryScope = {
      team_id: iso.teamId || DEFAULT_ISOLATION_ID,
      user_id: iso.userId || DEFAULT_ISOLATION_ID,
      agent_id: iso.agentId || DEFAULT_ISOLATION_ID,
    };
    if (iso.taskId) scope.task_id = iso.taskId;
    return scope;
  }
  // L3 cognition is team+agent scoped; the storage view already carries the
  // (team-or-user, agent) collapse, and the version scope must match it exactly.
  return profileHistoryScope(iso);
}

// ============================
// Capability pre-checks
// ============================

function storeCapabilityCheck(deps: ContentWriteDeps, requestId: string): ApiResponseEnvelope | null {
  const store = deps.getStore();
  if (!store) return fail(503, "Store not available", requestId);
  if (typeof store.commitContentChanges !== "function") {
    return fail(501, "history_unavailable: this store has no formal content commit entry", requestId);
  }
  if (typeof store.isContentHistoryEnabled === "function" && !store.isContentHistoryEnabled()) {
    return fail(409, "history_not_enabled: content history is not enabled for this store", requestId);
  }
  return null;
}

/** The scoped storage view for the trusted request. */
function scopedStorageFor(deps: ContentWriteDeps): StorageAdapter | null {
  const base = deps.getStorage();
  if (!base) return null;
  const iso = deps.requestIsolation;
  if (!iso) return base;
  return scopeProfileStorageView(
    base,
    `profiles/${encodeURIComponent(buildProfileIsolationScope(iso))}/`,
    { teamId: iso.teamId, userId: iso.userId, agentId: iso.agentId },
  );
}

async function localCapabilityCheck(deps: ContentWriteDeps, requestId: string): Promise<ApiResponseEnvelope | null> {
  if (!deps.getStorage()) return fail(503, "Storage not available", requestId);
  let history: LocalContentHistory | null;
  try {
    history = scopedStorageFor(deps)?.getContentHistory() ?? null;
  } catch (err) {
    deps.logger?.error?.(`${TAG} content-history capability probe failed: ${err instanceof Error ? err.message : String(err)}`);
    return fail(500, "history_capability_error: the storage backend failed while probing its content-history capability", requestId);
  }
  if (!history) {
    return fail(501, "history_unavailable: this storage backend cannot host an L3 content-history chain", requestId);
  }
  let enabled: boolean;
  try {
    enabled = await history.isEnabled();
  } catch (err) {
    deps.logger?.error?.(`${TAG} content-history enable probe failed: ${err instanceof Error ? err.message : String(err)}`);
    return fail(500, "history_capability_error: the storage backend failed while reading the history enable marker", requestId);
  }
  if (!enabled) {
    return fail(409, "history_not_enabled: content history is not enabled for this storage root", requestId);
  }
  return null;
}

/** The scoped local history, or a thrown `history_capability_error` (never silent null). */
function getLocalHistory(deps: ContentWriteDeps): LocalContentHistory {
  let storage: StorageAdapter | null;
  try {
    storage = scopedStorageFor(deps);
  } catch (err) {
    throw new ContentWriteRequestError(
      500,
      `history_capability_error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!storage) throw new ContentWriteRequestError(503, "Storage not available");
  let history: LocalContentHistory | null;
  try {
    history = storage.getContentHistory();
  } catch (err) {
    throw new ContentWriteRequestError(
      500,
      `history_capability_error: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (!history) {
    throw new ContentWriteRequestError(501, "history_unavailable: this storage backend cannot host an L3 content-history chain");
  }
  return history;
}

// ============================
// Reader helpers
// ============================

/** Read one exact version behind the trusted scope. */
async function readExactVersion(
  deps: ContentWriteDeps,
  layer: HistoryLayer,
  contentId: string,
  versionId: string,
  scope: HistoryScope,
): Promise<ContentVersion | null> {
  if (layer === "l0" || layer === "l1") {
    const store = deps.getStore();
    const reader = store?.getContentHistory?.();
    if (!reader) return null;
    const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
    const stored = reader.resolveIdentityScope(layer, contentId, auth);
    if (!stored) return null;
    return await readVersionAsync(reader, { layer, content_id: contentId, version_id: versionId, scope: stored });
  }
  const history = getLocalHistory(deps);
  return await history.readVersion({ layer, content_id: contentId, version_id: versionId, scope });
}

// ============================
// Draft assembly
// ============================

/**
 * Build exactly ONE mutation from the request plus its EXACT baseline. Program
 * fields (metadata, locator, source refs) are reconstructed by the program —
 * never from today's current — so a repeated request is byte-identical.
 */
/**
 * L1 retire：撤回一条既有原子认识（不是新认识）。
 *
 * 按 locator 解析出它的**当前版本**，用 change_kind=retire 提交：store 在一个事务里
 * 把当前行连同 FTS/向量一起移除，版本历史保留（旧记录仍可回看）。
 * 调用方给了 expected_version_id 就做 CAS 校验；没给就是“撤回这个 locator 上的当前内容”。
 *
 * Core 原先只允许 L1 追加（撤回 = 再写一条带 withdraws 的 L1），但那样会让
 * “清理噪声”本身变成新噪声；撤回不产生认识，所以要能单独表达。
 */
async function buildL1RetireMutation(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  scope: HistoryScope,
): Promise<VersionMutation> {
  const reader = deps.getStore()?.getContentHistory?.();
  if (!reader) {
    throw new ContentWriteRequestError(501, "history_unavailable: this store cannot resolve an L1 locator");
  }
  const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
  const resolved = reader.resolveLocatorAuthorized("l1", req.locator!, auth);
  if (!resolved) {
    throw new ContentWriteRequestError(
      404,
      `locator_not_found: ${req.locator} is not an in-scope L1 locator`,
    );
  }
  if (req.expected_version_id !== undefined && req.expected_version_id !== null
      && req.expected_version_id !== resolved.ref.version_id) {
    throw new ContentWriteRequestError(
      409,
      `history_version_conflict: current version of ${req.locator} is ${resolved.ref.version_id}, ` +
        `expected ${req.expected_version_id}`,
    );
  }
  return {
    draft: {
      layer: "l1",
      content_id: resolved.ref.content_id,
      locator: req.locator!,
      scope,
      body: "",
      metadata: {},
      state: "retired",
    },
    expected_version_id: resolved.ref.version_id,
    predecessors: [],
    change_kind: "retire",
  };
}

/**
 * L1 retag：只改一条既有认识的归属元数据（scene_name / type / priority）。
 *
 * 正文、locator、content_id、版本链与其余元数据（l1_source、withdraws、
 * user_metadata.note、source_refs、时间戳、session/task）都从**当前版本**取，
 * 所以它不可能借这条路改认识内容——想改正文必须写新认识并 withdraws 关联旧条目。
 * 版本层面它是一次 `revise`：CAS 用当前版本号，历史保留；归属没变时 store 判 no-op
 * （`changed=false`），不会平白长出一个版本。
 */
async function buildL1RetagMutation(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  scope: HistoryScope,
): Promise<VersionMutation> {
  const store = deps.getStore();
  const reader = store?.getContentHistory?.();
  if (!reader) {
    throw new ContentWriteRequestError(501, "history_unavailable: this store cannot resolve an L1 locator");
  }
  const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
  const resolved = reader.resolveLocatorAuthorized("l1", req.locator!, auth);
  if (!resolved) {
    throw new ContentWriteRequestError(
      404,
      `locator_not_found: ${req.locator} is not an in-scope L1 locator`,
    );
  }
  if (req.expected_version_id !== undefined && req.expected_version_id !== null
      && req.expected_version_id !== resolved.ref.version_id) {
    throw new ContentWriteRequestError(
      409,
      `history_version_conflict: current version of ${req.locator} is ${resolved.ref.version_id}, ` +
        `expected ${req.expected_version_id}`,
    );
  }
  const current = await readVersionAsync(reader, {
    layer: "l1",
    content_id: resolved.ref.content_id,
    version_id: resolved.ref.version_id,
    scope: resolved.scope,
  });
  if (!current) {
    throw new ContentWriteRequestError(
      404,
      `locator_not_found: ${req.locator} has no readable current version`,
    );
  }
  const metadata: Record<string, unknown> = { ...(current.metadata ?? {}) };
  if (req.scene_name !== undefined) metadata.scene_name = req.scene_name;
  if (req.type !== undefined) metadata.type = req.type;
  if (req.priority !== undefined) metadata.priority = req.priority;
  return {
    draft: {
      layer: "l1",
      content_id: resolved.ref.content_id,
      locator: req.locator!,
      scope,
      body: current.body,
      metadata,
      state: "active",
    },
    expected_version_id: resolved.ref.version_id,
    predecessors: [{ ...resolved.ref }],
    change_kind: "revise",
  };
}

async function buildMutation(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  scope: HistoryScope,
  baseline: ContentVersion | null,
): Promise<VersionMutation> {
  const state: "active" = "active";
  const newIdentity = req.change === "create";

  if (req.layer === "l0") {
    const metadata = buildL0Metadata(req, baseline);
    return {
      draft: {
        layer: "l0",
        locator: req.locator!,
        scope,
        body: typeof req.body === "string" ? req.body : "",
        metadata,
        state,
      },
      expected_version_id: null,
      predecessors: [],
      change_kind: "create",
    };
  }

  if (req.layer === "l1") {
    if (req.change === "retire") return await buildL1RetireMutation(req, deps, scope);
    if (req.change === "retag") return await buildL1RetagMutation(req, deps, scope);
    const metadata = await buildL1Metadata(req, deps, scope, baseline);
    return {
      draft: {
        layer: "l1",
        locator: req.locator!,
        scope,
        body: typeof req.body === "string" ? req.body : "",
        metadata,
        state,
      },
      expected_version_id: null,
      predecessors: [],
      change_kind: "create",
    };
  }

  // L3: 三份文档之一；长度受篇幅上限约束。
  const locator = req.locator!;
  if (!isL3DocumentLocator(locator)) {
    throw new ContentWriteRequestError(
      400,
      `invalid_locator: L3 locator 必须是三份文档之一（user-understanding.md / beliefs.md / current-focus.md），got ${JSON.stringify(locator)}`,
    );
  }
  const body = typeof req.body === "string" ? req.body : "";
  const limit = deps.l3DocumentLimits?.[locator] ?? L3_DOCUMENT_DEFAULT_LIMITS[locator];
  if (body.length > limit) {
    throw new ContentWriteRequestError(
      400,
      `l3_document_too_long: ${locator} 篇幅 ${body.length} 超过上限 ${limit} 字符（可配置；默认见 L3_DOCUMENT_DEFAULT_LIMITS）`,
    );
  }
  return {
    draft: {
      layer: "l3",
      ...(newIdentity ? {} : { content_id: req.content_id }),
      locator,
      scope,
      body,
      metadata: { user_metadata: req.user_metadata ?? {} },
      state,
    },
    expected_version_id: newIdentity ? null : req.expected_version_id ?? null,
    predecessors: [],
    change_kind: req.change,
  };
}

/**
 * L0 metadata. `message_recorded_at` is only set from an explicit message
 * timestamp; otherwise the program's commit clock stands in. `source` 是消息
 * 来源（conversation / framework_event，§5.2）。
 */
function buildL0Metadata(req: ContentWriteRequest, baseline: ContentVersion | null): Record<string, unknown> {
  const prev = baseline?.metadata ?? null;
  const curUser = isPlainObject(prev?.user_metadata) ? (prev?.user_metadata as Record<string, unknown>) : {};
  const role = req.role
    ?? (typeof prev?.role === "string" && prev.role ? prev.role : "user");
  const source = req.source
    ?? (typeof prev?.source === "string" && prev.source ? prev.source : "conversation");
  let originalTimestampMs = 0;
  let messageRecordedAt: string | undefined;
  if (req.message_timestamp !== undefined) {
    const ms = Date.parse(req.message_timestamp);
    if (!Number.isFinite(ms)) {
      throw new ContentWriteRequestError(400, "message_timestamp must be a valid ISO-8601 instant");
    }
    originalTimestampMs = ms;
    messageRecordedAt = new Date(ms).toISOString();
  } else if (typeof prev?.original_timestamp_ms === "number") {
    originalTimestampMs = prev.original_timestamp_ms;
    if (typeof prev?.message_recorded_at === "string" && prev.message_recorded_at) {
      messageRecordedAt = prev.message_recorded_at;
    }
  }
  // An unmodified old user key must survive (never silently dropped).
  const userMeta = { ...curUser, ...(req.user_metadata ?? {}) };
  const meta: Record<string, unknown> = {
    ...userMeta,
    user_metadata: userMeta,
    role,
    source,
    session_key: req.session_id ?? DEFAULT_ISOLATION_ID,
    session_id: req.session_id ?? DEFAULT_ISOLATION_ID,
    task_id: typeof prev?.task_id === "string" ? prev.task_id : "",
    original_timestamp_ms: originalTimestampMs,
  };
  if (messageRecordedAt !== undefined) meta.message_recorded_at = messageRecordedAt;
  return meta;
}

/**
 * L1 metadata: canonical semantics assembled by the program, user keys kept in
 * a separate partition. Declared sources are resolved to EXACT in-scope L0
 * version refs and persisted as `source_refs`. `source` 是 L1 来源标记
 * （extraction/agent/arbitration），`withdraws` 记录本条明确撤回的既有 L1。
 */
async function buildL1Metadata(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  scope: HistoryScope,
  baseline: ContentVersion | null,
): Promise<Record<string, unknown>> {
  const prev = baseline?.metadata ?? null;
  const prevUser = isPlainObject(prev?.user_metadata) ? (prev?.user_metadata as Record<string, unknown>) : {};
  const userMeta = { ...prevUser, ...(req.user_metadata ?? {}) };

  // Sources are NOT inherited: an undeclared source is `unknown`. Any declared
  // id must resolve to an exact, in-scope L0 locator AND its exact version ref
  // is persisted.
  const sourceIds = req.source_message_ids ?? [];
  const sourceRefs: VersionRef[] = [];
  if (sourceIds.length > 0) {
    const store = deps.getStore();
    const reader = store?.getContentHistory?.();
    if (!reader) {
      throw new ContentWriteRequestError(501, "history_unavailable: this store cannot verify source_message_ids");
    }
    const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
    for (const id of sourceIds) {
      const resolved = reader.resolveLocatorAuthorized("l0", id, auth);
      if (!resolved) {
        throw new ContentWriteRequestError(
          400,
          `source_ref_not_found: ${id} is not an in-scope L0 message locator`,
        );
      }
      sourceRefs.push({ layer: "l0", content_id: resolved.ref.content_id, version_id: resolved.ref.version_id });
    }
  }

  // 撤回对象必须是一条可解析的、本 scope 内的既有 L1（按 locator 解析到其当前版本）。
  let withdrawsRef: VersionRef | null = null;
  if (req.withdraws !== undefined) {
    const store = deps.getStore();
    const reader = store?.getContentHistory?.();
    if (!reader) {
      throw new ContentWriteRequestError(501, "history_unavailable: this store cannot verify withdraws");
    }
    const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
    const resolved = reader.resolveLocatorAuthorized("l1", req.withdraws, auth);
    if (!resolved) {
      throw new ContentWriteRequestError(400, `withdraws_not_found: ${req.withdraws} is not an in-scope L1 locator`);
    }
    withdrawsRef = { layer: "l1", content_id: resolved.ref.content_id, version_id: resolved.ref.version_id };
  }

  const sessionId = deps.requestIsolation?.sessionId || DEFAULT_ISOLATION_ID;
  const meta = buildL1VersionMetadata(userMeta, {
    type: req.type ?? (typeof prev?.type === "string" ? prev.type : ""),
    priority: req.priority ?? (typeof prev?.priority === "number" ? prev.priority : 50),
    sceneName: req.scene_name ?? (typeof prev?.scene_name === "string" ? prev.scene_name : ""),
    timestamps: req.timestamps ?? (Array.isArray(prev?.timestamps) ? prev.timestamps.filter((t): t is string => typeof t === "string") : []),
    sessionKey: sessionId,
    sessionId,
    taskId: scope.task_id ?? "",
    sourceMessageIds: sourceIds,
  });
  meta.source_refs = sourceRefs;
  meta.source = req.l1_source;
  if (req.withdraws !== undefined) {
    meta.withdraws = req.withdraws;
    meta.withdraws_ref = withdrawsRef;
  }
  return meta;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// ============================
// L0/L1: SQLite store commit
// ============================

/**
 * The identity's current sequence BEFORE the commit (its newest version), used
 * to decide whether the returned version is genuinely new. `-1` means the
 * identity did not exist yet.
 */
async function resolveLocatorCurrentSequence(
  deps: ContentWriteDeps,
  layer: HistoryLayer,
  locator: string,
): Promise<number> {
  const store = deps.getStore();
  const reader = store?.getContentHistory?.();
  if (!reader) return -1;
  const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
  const resolved = reader.resolveLocatorAuthorized(layer, locator, auth);
  if (!resolved) return -1;
  const version = await readVersionAsync(reader, {
    layer,
    content_id: resolved.ref.content_id,
    version_id: resolved.ref.version_id,
    scope: resolved.scope,
  });
  return version?.sequence ?? -1;
}

async function runStoreWrite(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  requestId: string,
  iso: Iso,
  commit: ChangeCommit,
  scope: HistoryScope,
): Promise<ApiResponseEnvelope> {
  const store = deps.getStore();
  if (!store) return fail(503, "Store not available", requestId);
  if (typeof store.commitContentChanges !== "function") {
    return fail(501, "history_unavailable: this store has no formal content commit entry", requestId);
  }
  if (typeof store.isContentHistoryEnabled === "function" && !store.isContentHistoryEnabled()) {
    return fail(409, "history_not_enabled: content history is not enabled for this store", requestId);
  }

  const filter: IsolationFilter = {
    teamId: iso.teamId,
    userId: iso.userId,
    agentId: iso.agentId,
    taskId: iso.taskId,
  };
  if (req.layer === "l0") filter.sessionId = req.session_id;

  const beforeSeq = await resolveLocatorCurrentSequence(deps, req.layer, req.locator!);

  // Embeddings are an optimisation, never fabricated: an unavailable service
  // writes metadata + FTS only (the store never invents a vector).
  const embeddings = await embedForCommit(deps, commit.mutations);

  const result = await store.commitContentChanges(commit, filter, embeddings ? { embeddings } : undefined);
  const versions = result.versions;
  if (!versions || versions.length === 0) {
    return fail(500, "content_write_failed: the commit returned no version", requestId);
  }
  const changed = versions.some((version) => version.sequence > beforeSeq);
  return ok<ContentWriteData>(buildWriteData(changed, versions), requestId);
}

async function embedForCommit(
  deps: ContentWriteDeps,
  mutations: VersionMutation[],
): Promise<Map<string, Float32Array> | undefined> {
  const embedding = deps.getEmbedding?.();
  if (!embedding) return undefined;
  const vectors = new Map<string, Float32Array>();
  let any = false;
  for (const mutation of mutations) {
    if (mutation.draft.state === "retired") continue;
    try {
      vectors.set(mutation.draft.locator, await embedding.embed(mutation.draft.body));
      any = true;
    } catch (err) {
      deps.logger?.warn?.(
        `${TAG} embedding failed for ${mutation.draft.locator}; writing metadata + FTS only: ` +
          `${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return any ? vectors : undefined;
}

// ============================
// L3: local workspace + history planner
// ============================

/** Outcome captured from the planner through a closure wrapper. */
interface CapturedPlan {
  noop: boolean;
  changed: boolean;
  versions: ContentVersion[];
}

async function runLocalWrite(
  req: ContentWriteRequest,
  deps: ContentWriteDeps,
  requestId: string,
  scope: HistoryScope,
  commit: ChangeCommit,
): Promise<ApiResponseEnvelope> {
  const storage = scopedStorageFor(deps);
  if (!storage) return fail(503, "Storage not available", requestId);

  const history = getLocalHistory(deps);
  try {
    if (!(await history.isEnabled())) {
      return fail(409, "history_not_enabled: content history is not enabled for this storage root", requestId);
    }
  } catch (err) {
    if (err instanceof ContentWriteRequestError) throw err;
    deps.logger?.error?.(`${TAG} history enable probe failed: ${err instanceof Error ? err.message : String(err)}`);
    return fail(500, "history_capability_error: the storage backend failed while reading the history enable marker", requestId);
  }

  const publishRoot = getLocalPublishRootOf(storage.getBackend());
  if (!publishRoot) {
    return fail(501, "history_unavailable: this storage backend cannot publish a local content bundle", requestId);
  }

  // `sources: []` states that this write declares NO program-provided input
  // relation (主 Agent 写入 L3 是判断，不是整理，不登记整理输入清单).
  const inner = history.planCommit({ commit, sources: [] });

  // Capture the planner's own outcome through a closure: the response must
  // describe THIS commit, never a re-read of `current` that a concurrent writer
  // may already have advanced.
  let captured: CapturedPlan | null = null;
  const wrapped = {
    plan: async (ctx: Parameters<typeof inner.plan>[0], files: Parameters<typeof inner.plan>[1]) => {
      const outcome = await inner.plan(ctx, files);
      if (outcome && (outcome as ContentHistoryNoop).noop === true) {
        const noop = outcome as ContentHistoryNoop;
        const result = noop.result as { versions?: ContentVersion[] } | undefined;
        captured = { noop: true, changed: false, versions: result?.versions ?? [] };
      } else {
        const built = outcome as ContentHistoryCommitOutcome;
        const result = built.result as { changed?: boolean; versions?: ContentVersion[] } | undefined;
        captured = { noop: false, changed: result?.changed === true, versions: result?.versions ?? [] };
      }
      return outcome;
    },
  };

  const workspace = await GenerationWorkspace.begin({
    storage,
    dataDir: publishRoot,
    kind: "l3",
    logger: deps.logger,
    historyContext: wrapped,
  });

  try {
    const view = workspace.view;
    if (!view) throw new ContentWriteRequestError(500, `${TAG} local workspace has no storage view`);

    for (const mutation of commit.mutations) {
      await view.writeFile(mutation.draft.locator, mutation.draft.body);
    }

    await workspace.commit();
  } catch (err) {
    await workspace.discard();
    throw err;
  }

  if (!captured) {
    return fail(500, "content_write_unconfirmed: the planner did not report a commit outcome", requestId);
  }
  const plan: CapturedPlan = captured;
  if (plan.versions.length === 0) {
    return fail(500, "content_write_unconfirmed: the commit produced no version", requestId);
  }
  // A noop is an idempotent replay: no new version and no side effect. A full
  // outcome reports the planner's own `changed`.
  const changed = !plan.noop && plan.changed;
  return ok<ContentWriteData>(buildWriteData(changed, plan.versions), requestId);
}

function buildWriteData(changed: boolean, versions: ContentVersion[]): ContentWriteData {
  const views = versions.map((version) => contentVersionView(version));
  const refs = versions.map((version) => ({ ...version.ref }));
  return { changed, ref: refs[0], version: views[0], refs, versions: views };
}
