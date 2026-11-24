/**
 * TDAI Memory Gateway — v2 REST Router.
 *
 * Implements POST routes defined in `01-api-spec.yaml`:
 *
 *   L0 Conversation: add / query / search / delete
 *   L1 Atomic:       update / query / search / delete
 *   L2 Scenario:     ls / read / write / rm
 *   L3 Core:         read / write
 *
 * All routes are prefixed with `/v2/`.
 * Authentication: Authorization Bearer + x-tdai-service-id.
 * Request validation: Zod v4 safeParse → 400 on failure.
 * Response envelope: { code, message, request_id, data }.
 */

import { createHash, randomUUID } from "node:crypto";
import type http from "node:http";
import { classifyError } from "./error-handler.js";
import type { IMemoryStore, L0Record, L1RecordRow, ProfileSyncRecord, CommittedOperationRecord } from "../core/store/types.js";
import { DEFAULT_ISOLATION_ID } from "../core/store/types.js";
import { buildL0VersionMetadata, PROGRAM_REQUEST_METADATA_KEY } from "../core/store/sqlite/content-history.js";
import type { EmbeddingService } from "../core/store/embedding.js";
import { createScopedStorageAdapter, scopeProfileStorageView, type StorageAdapter } from "../core/storage/adapter.js";
import { openPublishedSnapshotOf } from "../core/storage/local-published-storage.js";
import { StoragePaths } from "../core/storage/types.js";
import type { Logger } from "../core/types.js";
import type { IStateBackend } from "../core/state/types.js";
import type { PipelineWorker } from "../services/pipeline-worker.js";
import { executeMemorySearch } from "../core/tools/memory-search.js";
import { executeConversationSearch } from "../core/tools/conversation-search.js";
import type { MemoryRecord } from "../core/record/l1-writer.js";
import { reportRecallMetrics } from "../core/report/metric-tracking-recall.js";

// ── Zod schemas (validated types + defaults) ──
import {
  conversationAddRequestSchema,
  conversationQueryRequestSchema,
  conversationSearchRequestSchema,
  conversationDeleteRequestSchema,
  conversationCountRequestSchema,
  conversationGetRequestSchema,
  atomicUpdateRequestSchema,
  atomicQueryRequestSchema,
  atomicSearchRequestSchema,
  atomicDeleteRequestSchema,
  atomicCountRequestSchema,
  atomicGetRequestSchema,
  scenarioListRequestSchema,
  scenarioReadRequestSchema,
  scenarioWriteRequestSchema,
  scenarioRmRequestSchema,
  scenarioCountRequestSchema,
  coreReadRequestSchema,
  coreWriteRequestSchema,
  coreCountRequestSchema,
  teamCreateRequestSchema,
  teamGetRequestSchema,
  teamUpdateRequestSchema,
  teamBatchDeleteRequestSchema,
  userCreateRequestSchema,
  userGetRequestSchema,
  userUpdateRequestSchema,
  userBatchDeleteRequestSchema,
  agentCreateRequestSchema,
  agentGetRequestSchema,
  agentUpdateRequestSchema,
  agentBatchDeleteRequestSchema,
  taskCreateRequestSchema,
  taskGetRequestSchema,
  taskUpdateRequestSchema,
  taskBatchDeleteRequestSchema,
  formatZodError,
  resolveIsolation,
  type ApiResponseEnvelope,
  type V2AuthContext,
  type ConversationItem,
  type ConversationSearchHit,
  type ConversationAddData,
  type ConversationQueryData,
  type ConversationSearchData,
  type ConversationDeleteData,
  type ConversationGetData,
  type CountData,
  type AtomicDetail,
  type AtomicUpdateData,
  type AtomicQueryData,
  type AtomicSearchData,
  type AtomicSearchHit,
  type AtomicDeleteData,
  type AtomicGetData,
  type AtomicSourceView,
  type ScenarioEntry,
  type ScenarioFile,
  type ScenarioWriteData,
  type CoreFile,
  type CoreWriteData,
  type BatchDeleteResult,
  type TeamData,
  type UserData,
  type AgentData,
  type TaskData,
} from "./v2-schemas.js";
import { stripSceneNavigation } from "../core/scene/scene-navigation.js";
import { buildProfileIsolationScope, buildProfileStableId, DEFAULT_PROFILE_SCOPE } from "../core/profile/profile-scope.js";
import { readContentSourceViewForContent, removeContentSources } from "../core/content/sources.js";
import { unknownContentSource } from "../core/content/types.js";
import type { ContentVersion, HistoryScope, VersionMutation } from "../core/content/history-contract.js";
import {
  contentSourceViewFromVersion,
  contentVersionView,
  handleContentChanges,
  handleContentHistory,
  handleContentRead,
  resolveLocalLocatorHistory,
  resolveStoreRecordHistory,
  resolveStoreRecordHistoryByLocator,
} from "./content-history-handlers.js";
import { handleContentWrite } from "./content-write-handlers.js";
import { buildContentHistoryAuthFilter, readVersionAsync } from "../core/content/history-reader.js";

const TAG = "[tdai-gateway][v2]";
const V2_PREFIX = "/v2";

/**
 * /v3 是 L0–L3 数据面接口的"严格 isolation 版本"：
 *
 *   - 必填 team_id + agent_id + user_id（缺一即 422）
 *   - session_id 可选：传入则按 session 收敛；不传则按 (team, agent, user) 维度聚合
 *     —— 满足"agent 跨 session 聚合视图"（如治理面板的 L0/L1 总数）和
 *     "L2/L3 团队级聚合"（profile scope 公式忽略 session）两类场景
 *   - 不接受 legacy_compat_mode 退化
 *   - 与 /v2 共享 handler 实现，仅在 dispatch 层换一套 isolation 校验
 *
 * 同名 /v2 路径保持现有行为（team_id 可选、user_id fallback、可走 legacyCompatMode），
 * 这样调用方按需选 v2/v3，互不影响。
 *
 * L0–L3 数据面 endpoint（含 count）上 v3；team/user/agent/task/pipeline 等管理面接口保留 v2 唯一入口。
 */
const V3_PREFIX = "/v3";

/**
 * /v3 严格 isolation 必填字段。从 request body 或 x-tdai-* header 任一来源取到即可。
 *
 * v3 全部 L0–L3 接口仅强制 team + agent + user 三元组。
 * session_id 一律可选：
 *   - L0 conversation/* 与 L1 atomic/*：传 session 走 session 内收敛；不传按
 *     (team, agent, user) 跨 session 聚合（便于"agent 维度全量视图"，如
 *     team-memory-control 治理面的 layer-counts 展示总量）
 *   - L2 scenario/* 与 L3 core/*：原本就是 team+agent 级 profile 聚合，session 忽略
 *
 * 返回缺失字段列表（空数组表示全齐）。
 */
function collectV3Missing(
  _subpath: string,
  body: Record<string, unknown> | undefined,
  headers: Record<string, string | string[] | undefined>,
): string[] {
  const headerStr = (k: string): string | undefined => {
    const raw = headers[k] ?? headers[k.toLowerCase()];
    if (Array.isArray(raw)) return raw[0];
    return typeof raw === "string" ? raw : undefined;
  };
  const get = (bodyKey: string, headerKey: string): string => {
    const v = (body?.[bodyKey] as string | undefined) ?? headerStr(headerKey) ?? "";
    return typeof v === "string" ? v.trim() : "";
  };
  const missing: string[] = [];
  if (!get("team_id", "x-tdai-team-id")) missing.push("team_id");
  if (!get("agent_id", "x-tdai-agent-id")) missing.push("agent_id");
  if (!get("user_id", "x-tdai-user-id")) missing.push("user_id");
  // session_id 不再强制：底层 handler 在缺 session 时按 (team,agent,user) 聚合查询。
  return missing;
}

/** /v3 强 isolation 覆盖的 L0–L3 子路径（去掉前缀后的 path）。 */
const V3_ALLOWED_SUBPATHS = new Set<string>([
  "/conversation/add",
  "/conversation/query",
  "/conversation/get",
  "/conversation/search",
  "/conversation/delete",
  "/conversation/count",
  "/atomic/update",
  "/atomic/query",
  "/atomic/get",
  "/atomic/search",
  "/atomic/delete",
  "/atomic/count",
  "/scenario/ls",
  "/scenario/read",
  "/scenario/write",
  "/scenario/rm",
  "/scenario/count",
  "/core/read",
  "/core/write",
  "/core/count",
  "/content/read",
  "/content/history",
  "/content/changes",
  "/content/write",
]);

/**
 * 写一条审计事件到 store.appendAudit。失败不阻塞主请求（容忍 audit 丢失）。
 *
 * 调用约定（per user 决策）：
 *   - 原始 L0/L1/L2/L3 表完全不动，本函数只追加事件
 *   - team/agent/user/task 来自外部请求 IdFields（resolveIsolation 后的 ctx）
 *   - L0 不参与（不可变流水）
 *   - 5 个 mutation handler 各调一次：
 *     atomic/update + atomic/delete + scenario/write + scenario/rm + core/write
 */
async function recordAudit(
  store: IMemoryStore | undefined,
  args: {
    record_id: string;
    layer: "L1" | "L2" | "L3";
    action: "update" | "delete";
    iso?: { teamId?: string; userId?: string; agentId?: string; sessionId?: string; taskId?: string };
    version: number;
    requestId: string;
    logger?: { warn?: (msg: string) => void };
  },
): Promise<void> {
  if (!store?.appendAudit) return; // store 不支持 audit → 跳过
  try {
    await store.appendAudit({
      audit_id: `audit-${randomUUID().replace(/-/g, "").slice(0, 16)}`,
      record_id: args.record_id,
      layer: args.layer,
      action: args.action,
      team_id: args.iso?.teamId,
      agent_id: args.iso?.agentId,
      user_id: args.iso?.userId,
      task_id: args.iso?.taskId,
      version: args.version,
      updated_at_ms: Date.now(),
      request_id: args.requestId,
    });
  } catch (err) {
    args.logger?.warn?.(
      `${TAG} audit append failed (${args.layer}/${args.action} record=${args.record_id}): ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

// ============================
// Dependencies injected at mount time
// ============================

export interface V2RouterDeps {
  /** Get the default IMemoryStore (standalone fallback). */
  getStore: () => IMemoryStore | undefined;
  /** Get the default EmbeddingService (standalone fallback). */
  getEmbedding: () => EmbeddingService | undefined;
  /** Get the default StorageAdapter (standalone fallback). */
  getStorage: () => StorageAdapter | undefined;
  logger: Logger;

  /**
   * Deploy mode of the gateway. Controls behaviors that diverge between
   * single-node open-source ("standalone") and cloud multi-tenant ("service"):
   *   - standalone: mirror v2 conversation/add L0 to <dataDir>/conversations/<date>.jsonl
   *                 (parity with v1 capture path; useful for human inspection / seed verify)
   *   - service:    skip the JSONL mirror — service stores authoritative L0 in TCVDB +
   *                 COS via its own pathway; mirroring to local FS would write to
   *                 ephemeral pod disk and is operationally meaningless.
   */
  deployMode: "standalone" | "service";

  // ── Service-mode per-instance resolvers (optional) ──
  // When provided, v2 handlers resolve store/storage per-request using
  // auth.serviceId as the instanceId key, falling back to the static getters above.

  /** Resolve IMemoryStore + EmbeddingService for a given instanceId (service mode). */
  resolveStore?: (instanceId: string) => Promise<{ store: IMemoryStore; embedding: EmbeddingService | undefined }>;
  /** Resolve per-instance StorageAdapter for a given instanceId (service mode). */
  resolveStorage?: (instanceId: string) => Promise<StorageAdapter | undefined>;

  /**
   * Notify pipeline that new L0 messages were added for a session.
   * Triggers async L1 extraction via state-backend Buffer → Scanner → Worker.
   *
   * Wired in both modes:
   *   - service mode: remote state backend
   *   - standalone: LocalStateBackend (single-process, default)
   * When absent (misconfiguration), v2 add writes L0 only — pipeline is not triggered.
   */
  notifyPipeline?: (instanceId: string, sessionId: string, messageCount: number, teamId?: string, agentId?: string) => Promise<void>;

  /** Quota manager for memory/credit limit checks and usage reporting (service mode). */
  quotaManager?: import("../core/quota/quota-manager.js").QuotaManager;

  /**
   * 拿到 (per instance) 的 MetadataService。仅当 `/v2/conversation/add` 首次
   * 写入某 (team, agent) 时用来自动登记 chat_memory 资产并绑定到 agent。
   * 未注入时该功能优雅降级：conversation 依旧正常写入，只是资产不会被自动
   * 创建 —— 老部署完全兼容。
   */
  getMetadataService?: (instanceId: string) => Promise<import("../metadata/service/metadata-service.js").MetadataService>;

  /**
   * State backend handle, used by /v2/pipeline/status to call listQueuedTasks().
   * Wired in standalone and service modes, but the status endpoint itself is
   * standalone-only. The handler returns 404 in service mode before touching
   * this field, so remote backends do not need to implement listQueuedTasks().
   */
  stateBackend?: IStateBackend;

  /**
   * Pipeline worker handle, used by /v2/pipeline/status to call getRunningTasks()
   * for per-L-type in-flight stats. Service mode never invokes this getter
   * (status endpoint returns 404 in service mode).
   */
  pipelineWorker?: PipelineWorker;

  // ── Tenancy isolation (three-dim) ──
  //
  // `isolationConfig` is set once at gateway start.  `requestIsolation` and
  // `requestIsolationMissing` are filled per-request by dispatchV2Request so
  // each handler can persist (user_id, agent_id, session_id) on writes
  // without changing handler signatures.

  /** Static config for isolation enforcement (set at gateway start). */
  isolationConfig?: {
    enforce: boolean;
    legacyCompatMode: boolean;
    legacyPlaceholder: string;
  };
  /**
   * Whether `/v3` L0–L3 enforces the team+agent+user triple.
   * Undefined defaults to strict to preserve direct router unit-test semantics;
   * server.ts injects the env-backed value, whose runtime default is OFF.
   * `/v3/skill/*` is always exempt.
   */
  v3StrictIsolation?: boolean;
  /** Resolved isolation context for the current request (set by dispatch). */
  requestIsolation?: { teamId?: string; userId: string; agentId: string; sessionId: string; taskId?: string };
  /** When isolation could not be resolved AND legacy_compat_mode is off, the missing fields. */
  requestIsolationMissing?: string[];
}

// ============================
// Envelope helpers
// ============================

export function makeRequestId(): string {
  return `req-${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

/** 优先使用上游 x-request-id / x-qcloud-transaction-id，否则本地生成。 */
export function resolveRequestId(
  headers: Record<string, string | string[] | undefined>,
): string {
  const raw = headers["x-qcloud-transaction-id"] ?? headers["x-request-id"];
  const id = Array.isArray(raw) ? raw[0] : raw;
  if (typeof id === "string" && id.trim()) return id.trim();
  return makeRequestId();
}

export function successEnvelope<T>(data: T, requestId: string): ApiResponseEnvelope<T> {
  return { code: 0, message: "ok", request_id: requestId, data };
}

export function errorEnvelope(code: number, message: string, requestId: string, extra?: Record<string, unknown>): ApiResponseEnvelope {
  return { code, message, request_id: requestId, ...(extra ? { data: extra } : {}) };
}

// ============================
// Auth middleware
// ============================

export function parseV2Auth(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  requestId: string,
  sendJsonFn: (res: http.ServerResponse, status: number, body: unknown) => void,
): V2AuthContext | null {
  const authHeader = req.headers["authorization"] ?? "";
  const serviceId = (req.headers["x-tdai-service-id"] as string) ?? "";

  if (!authHeader.startsWith("Bearer ") || !authHeader.slice(7).trim()) {
    sendJsonFn(res, 401, errorEnvelope(401, "Missing or invalid Authorization header. Expected: Bearer {api_key}", requestId));
    return null;
  }
  if (!serviceId.trim()) {
    sendJsonFn(res, 401, errorEnvelope(401, "Missing x-tdai-service-id header", requestId));
    return null;
  }

  return Object.fromEntries([
    ["apiKey", authHeader.slice(7).trim()],
    ["serviceId", serviceId.trim()],
  ]) as V2AuthContext;
}

// ============================
// Per-request resolution helpers
// ============================

/** Resolve store + embedding for a v2 request. Service mode → per-instance; standalone → core singleton. */
async function resolveStoreForRequest(
  auth: V2AuthContext,
  deps: V2RouterDeps,
): Promise<{ store: IMemoryStore | undefined; embedding: EmbeddingService | undefined }> {
  if (deps.resolveStore) {
    // Service mode: per-instance VDB store is mandatory. Do NOT fallback to local SQLite.
    return await deps.resolveStore(auth.serviceId);
  }
  // Standalone mode: use core singleton store
  return { store: deps.getStore(), embedding: deps.getEmbedding() };
}

/** Resolve storage adapter for a v2 request. Service mode → per-instance COS; standalone → core local. */
async function resolveStorageForRequest(
  auth: V2AuthContext,
  deps: V2RouterDeps,
): Promise<StorageAdapter | undefined> {
  if (deps.resolveStorage) {
    // Service mode: per-instance COS storage is mandatory. Do NOT fallback to local filesystem.
    return await deps.resolveStorage(auth.serviceId);
  }
  // Standalone mode: use core local storage
  return deps.getStorage();
}

// ============================
// Route table
// ============================

type RouteHandler = (
  body: unknown,
  auth: V2AuthContext,
  requestId: string,
  deps: V2RouterDeps,
) => Promise<ApiResponseEnvelope>;

/**
 * L0–L3 数据面 handler 映射（子路径 → handler）。历史接口同时挂载 /v2/* 与 /v3/*；
 * count 接口按 sdk-v3.yaml 仅挂载 /v3/*。/v3 走严格 isolation 校验；/v2 沿用现有 enforce/legacyCompat 配置。
 */
const DATAPLANE_HANDLERS: Record<string, RouteHandler> = {
  "/conversation/add": handleConversationAdd,
  "/conversation/query": handleConversationQuery,
  "/conversation/get": handleConversationGet,
  "/conversation/search": handleConversationSearch,
  "/conversation/delete": handleConversationDelete,
  "/conversation/count": handleConversationCount,
  "/atomic/update": handleAtomicUpdate,
  "/atomic/query": handleAtomicQuery,
  "/atomic/get": handleAtomicGet,
  "/atomic/search": handleAtomicSearch,
  "/atomic/delete": handleAtomicDelete,
  "/atomic/count": handleAtomicCount,
  "/scenario/ls": handleScenarioLs,
  "/scenario/read": handleScenarioRead,
  "/scenario/write": handleScenarioWrite,
  "/scenario/rm": handleScenarioRm,
  "/scenario/count": handleScenarioCount,
  "/core/read": handleCoreRead,
  "/core/write": handleCoreWrite,
  "/core/count": handleCoreCount,
  "/content/read": handleContentRead,
  "/content/history": handleContentHistory,
  "/content/changes": handleContentChanges,
  "/content/write": handleContentWrite,
};

const routeTable: Record<string, RouteHandler> = {
  // L0–L3 数据面：历史读写接口保留 /v2 与 /v3 双入口；count 仅按 sdk-v3.yaml 暴露 /v3。
  ...Object.fromEntries(
    Object.entries(DATAPLANE_HANDLERS).flatMap(([sub, h]) => {
      const v3Route = [`${V3_PREFIX}${sub}`, h] as const;
      if (sub.endsWith("/count")) return [v3Route];
      return [[`${V2_PREFIX}${sub}`, h] as const, v3Route];
    }),
  ),
  // ──────────────────────────────────────────────────────────────────────────
  // @deprecated v2 entity 路由（team/user/agent/task，16 条）。仅 /v2。
  // 元数据已由 v3 `/v3/meta/*`（规范化 meta_* 表 + MetadataService）接管，control
  // 面板 remote 模式直接走 v3。以下路由仍走旧 `entity_*` 聚合数组模型，**仅为兼容现网
  // 保留、行为不变**，不再演进，新接入方一律改用 v3。计划在确认无外部调用方后整体删除。
  // 决策（2026-06-26，per user）：标记废弃、代码暂留，不适配 MetadataService。
  // 见 team-memory-control/docs/architecture/08-metadata-migration-and-permission-design.md §8.5。
  // ──────────────────────────────────────────────────────────────────────────
  [`${V2_PREFIX}/team/create`]: handleTeamCreate, // @deprecated 改用 /v3/meta/team/create
  [`${V2_PREFIX}/team/get`]: handleTeamGet, // @deprecated 改用 /v3/meta/team/get
  [`${V2_PREFIX}/team/update`]: handleTeamUpdate, // @deprecated 改用 /v3/meta/team/update
  [`${V2_PREFIX}/team/delete`]: handleTeamDelete, // @deprecated 改用 /v3/meta/team/delete
  [`${V2_PREFIX}/user/create`]: handleUserCreate, // @deprecated 改用 /v3/meta/user/create
  [`${V2_PREFIX}/user/get`]: handleUserGet, // @deprecated 改用 /v3/meta/user/get
  [`${V2_PREFIX}/user/update`]: handleUserUpdate, // @deprecated 改用 /v3/meta/user/update
  [`${V2_PREFIX}/user/delete`]: handleUserDelete, // @deprecated 改用 /v3/meta/user/delete
  [`${V2_PREFIX}/agent/create`]: handleAgentCreate, // @deprecated 改用 /v3/meta/agent/create
  [`${V2_PREFIX}/agent/get`]: handleAgentGet, // @deprecated 改用 /v3/meta/agent/get
  [`${V2_PREFIX}/agent/update`]: handleAgentUpdate, // @deprecated 改用 /v3/meta/agent/update
  [`${V2_PREFIX}/agent/delete`]: handleAgentDelete, // @deprecated 改用 /v3/meta/agent/delete
  [`${V2_PREFIX}/task/create`]: handleTaskCreate, // @deprecated 改用 /v3/meta/task/create
  [`${V2_PREFIX}/task/get`]: handleTaskGet, // @deprecated 改用 /v3/meta/task/get
  [`${V2_PREFIX}/task/update`]: handleTaskUpdate, // @deprecated 改用 /v3/meta/task/update
  [`${V2_PREFIX}/task/delete`]: handleTaskDelete, // @deprecated 改用 /v3/meta/task/delete
  // ── end @deprecated v2 entity 路由 ──
  [`${V2_PREFIX}/pipeline/status`]: handlePipelineStatus,
};

export async function handleV2Route(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  pathname: string,
  method: string,
  parseJsonBody: <T>(req: http.IncomingMessage) => Promise<T>,
  sendJson: (res: http.ServerResponse, status: number, body: unknown) => void,
  deps: V2RouterDeps,
  /**
   * Optional: extra routes contributed by other modules (e.g.
   * /v3/skill/* from `makeSkillRouteTable()`). Looked up only when the
   * built-in `routeTable` doesn't contain the pathname, so module-level
   * routes always win on collision. Supports `/v2/*`, `/v3/*` (L0–L3
   * data-plane + extraRouteTable for /v3/skill/*, /v3/knowledge/* and
   * /v3/chat-memory/*).
   *
   * The handler's `deps` parameter is intentionally typed `unknown` here:
   * extra modules (skill/*, future namespaces) declare their own deps
   * shape (e.g. `SkillRouterDeps`). The caller is responsible for passing
   * a `deps` object that satisfies the union of every handler set's
   * requirements; v2-router just forwards it verbatim.
   */
  extraRouteTable?: Record<
    string,
    (body: unknown, auth: V2AuthContext, requestId: string, deps: unknown) => Promise<ApiResponseEnvelope>
  >,
): Promise<boolean> {
  const isPromptRead = method === "GET" && (
    pathname === "/v3/memory-prompt/get" ||
    pathname === "/v3/memory-prompt/setting/list" ||
    pathname === "/v3/memory-prompt/log" ||
    pathname === "/v3/memory-generation-log/list" ||
    pathname === "/v3/memory-generation-log/get"
  );
  if (method !== "POST" && !isPromptRead) return false;
  const isV3 = pathname.startsWith(`${V3_PREFIX}/`);
  const isV2 = pathname.startsWith(`${V2_PREFIX}/`);
  // Management-plane modules are provided by extraRouteTable, not by the
  // built-in V3 data-plane list. They bypass strict L0-L3 isolation because
  // each module validates its own target semantics.
  const isV3Extra = !!extraRouteTable && (
    pathname.startsWith("/v3/skill/") ||
    pathname.startsWith("/v3/knowledge/") ||
    pathname.startsWith("/v3/chat-memory/") ||
    pathname.startsWith("/v3/memory-prompt/") ||
    pathname.startsWith("/v3/memory-generation-log/")
  );
  if (!isV2 && !isV3) return false;

  // /v3 暴露 L0–L3 数据面 15 条（V3_ALLOWED_SUBPATHS）+ /v3/skill/* + /v3/knowledge/*（extraRouteTable）；
  // 其他 /v3 子路径直接走 404
  if (isV3 && !isV3Extra) {
    const sub = pathname.slice(V3_PREFIX.length);
    if (!V3_ALLOWED_SUBPATHS.has(sub)) return false;
  }

  const handler = routeTable[pathname];
  const extra = extraRouteTable?.[pathname];
  if (!handler && !extra) return false;

  const requestId = makeRequestId();
  // [skill-perf 2026-07-21] 只对 /v3/skill/ 打分段耗时，避免污染其他链路 log。
  // T0 从 server.ts socket-level 埋点取；没有则用 dispatch 进入时刻兜底。
  const isSkillPerf = pathname.startsWith("/v3/skill/");
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const perfT0 = isSkillPerf ? ((req as any).__skillPerfT0 as number | undefined) ?? Date.now() : 0;
  // 把 request_id 挂到 res 上，让 server.ts 的 res.on('finish') 也能带上
  if (isSkillPerf) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (res as any).__skillReqId = requestId;
    deps.logger.info(
      `[skill-perf] dispatch.enter req_id=${requestId} path=${pathname} t=${Date.now()} t0_offset=${Date.now() - perfT0}ms`,
    );
  }
  const perfMark = (phase: string, extraFields?: string) => {
    if (!isSkillPerf) return;
    const now = Date.now();
    deps.logger.info(
      `[skill-perf] phase=${phase} req_id=${requestId} elapsed=${now - perfT0}ms${extraFields ? " " + extraFields : ""}`,
    );
  };

  const authStart = Date.now();
  const auth = parseV2Auth(req, res, requestId, sendJson);
  if (isSkillPerf) {
    perfMark("parseAuth", `dur=${Date.now() - authStart}ms ok=${auth ? "true" : "false"}`);
  }
  if (!auth) return true;

  try {
    // Pre-resolve per-request store/storage (service mode → per-instance, standalone → core singleton)
    const resolveStart = Date.now();
    const resolved = await resolveStoreForRequest(auth, deps);
    const resolvedStorage = await resolveStorageForRequest(auth, deps);
    perfMark("resolveStoreAndStorage", `dur=${Date.now() - resolveStart}ms serviceId=${auth.serviceId}`);

    // Wrap deps so handlers use the resolved per-instance resources
    const resolvedDeps: V2RouterDeps = {
      ...deps,
      getStore: () => resolved.store,
      getEmbedding: () => resolved.embedding,
      getStorage: () => resolvedStorage,
    };

    const bodyStart = Date.now();
    const body = method === "GET"
      ? Object.fromEntries(new URL(req.url ?? pathname, "http://localhost").searchParams.entries())
      : await parseJsonBody(req);
    perfMark("parseJsonBody", `dur=${Date.now() - bodyStart}ms len=${req.headers["content-length"] ?? "?"}`);

    // Tenancy isolation — pulled from body (preferred) or x-tdai-* headers
    // and attached to the per-request deps so handlers can persist
    // (user_id, agent_id, session_id) on every L0/L1 write without
    // changing every handler signature.
    //
    // We only attempt resolution; whether missing fields are fatal is up
    // to each handler (some endpoints don't need isolation at all, e.g.
    // /v2/pipeline/status). See resolveIsolation() in v2-schemas.
    //
    // /v3 走严格校验：必须同时提供 team_id + agent_id + user_id + session_id，
    // 缺任意一个直接 422，且不走 legacyCompatMode 退化。
    const headers = (req.headers ?? {}) as Record<string, string | string[] | undefined>;
    const isoLegacyCompat = isV3 ? false : (deps.isolationConfig?.legacyCompatMode ?? false);
    const isoResolved = resolveIsolation(body as Record<string, unknown> | undefined, headers, {
      legacyCompatMode: isoLegacyCompat,
      legacyPlaceholder: deps.isolationConfig?.legacyPlaceholder,
    });

    // /v3 strict isolation is for L0–L3 memory data-plane only.
    // Skill and knowledge endpoints are team-scoped management-plane
    // and must not be blocked by per-agent memory isolation.
    // Runtime default comes from server.ts/env and is OFF;
    // undefined keeps strict in direct router tests for backward compatibility.
    const v3StrictEnabled = deps.v3StrictIsolation ?? true;
    if (isV3 && !isV3Extra && v3StrictEnabled) {
      const v3Subpath = pathname.slice(V3_PREFIX.length);
      const v3Missing = collectV3Missing(v3Subpath, body as Record<string, unknown> | undefined, headers);
      if (v3Missing.length > 0) {
        sendJson(res, 422, errorEnvelope(
          422,
          `/v3 requires strict isolation: missing ${v3Missing.join(", ")}. ` +
          `Provide via request body or x-tdai-{team-id,agent-id,user-id,session-id} headers.`,
          requestId,
        ));
        return true;
      }
    }

    const depsWithIsolation: V2RouterDeps = {
      ...resolvedDeps,
      // /v3 路径强制覆盖 isolationConfig.enforce，确保 handler 内部一致地走严格分支
      isolationConfig: isV3
        ? { enforce: true, legacyCompatMode: false, legacyPlaceholder: resolvedDeps.isolationConfig?.legacyPlaceholder ?? "" }
        : resolvedDeps.isolationConfig,
      requestIsolation: isoResolved.ctx,
      // resolveIsolation always returns { ok: true } — missing fields are filled with defaults.
      // requestIsolationMissing is only set when the caller explicitly needs to reject incomplete
      // isolation (e.g. /v3 strict mode), which is handled separately above via collectV3Missing.
      requestIsolationMissing: undefined,
    };

    const handlerStart = Date.now();
    const envelope = handler
      ? await handler(body, auth, requestId, depsWithIsolation)
      : await extra!(body, auth, requestId, depsWithIsolation as unknown);
    perfMark("handler", `dur=${Date.now() - handlerStart}ms envelope_code=${envelope.code}`);
    const httpStatus = envelope.code === 0 ? 200 : envelope.code >= 400 && envelope.code < 600 ? envelope.code : 200;
    const sendStart = Date.now();
    sendJson(res, httpStatus, envelope);
    perfMark("sendJson", `dur=${Date.now() - sendStart}ms status=${httpStatus}`);
  } catch (err) {
    // H-13: use classifyError so 5xx leaves no err.message leak; PayloadTooLargeError
    // and RecallFailure already carry safe messages but go through the same path for uniformity.
    const classified = classifyError(err);
    if (classified.status >= 500) {
      deps.logger.error(`${TAG} [${pathname}] ${classified.logLine}`);
    } else {
      deps.logger.warn(`${TAG} [${pathname}] ${classified.logLine}`);
    }
    sendJson(res, classified.status, {
      ...errorEnvelope(classified.client.code, classified.client.message, requestId),
      trace_id: classified.client.trace_id,
      retryable: classified.client.retryable,
    });
  }

  return true;
}

// ============================
// L0 Conversation Handlers
// ============================

async function handleConversationAdd(body: unknown, auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = conversationAddRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { session_id, messages } = parsed.data;

  // Enforce three-dim isolation. user_id / agent_id come from request body
  // or x-tdai-* headers (resolved in dispatchV2Request).  When the gateway's
  // isolationConfig.enforce is on AND legacy_compat_mode is off, missing
  // fields are a 422.
  if (deps.isolationConfig?.enforce && deps.requestIsolationMissing && deps.requestIsolationMissing.length > 0) {
    return errorEnvelope(
      422,
      `Tenancy isolation required: missing ${deps.requestIsolationMissing.join(", ")}. ` +
      `Provide via request body or x-tdai-{user-id,agent-id,session-id} headers.`,
      requestId,
    );
  }
  const iso = deps.requestIsolation;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);

  // Quota check: memory limit
  if (deps.quotaManager) {
    const check = await deps.quotaManager.checkMemoryQuota(auth.serviceId, messages.length);
    if (!check.allowed) {
      return errorEnvelope(4291, `Memory limit exceeded (current=${check.current}, limit=${check.limit})`, requestId);
    }
  }

  // 自动登记 chat_memory 资产（team+agent 粒度）并绑定到 agent。首次触发
  // 会 create asset + append 绑定；后续同 (team, agent) 走进程内 LRU 短路。
  // 失败降级：只打 warn，不阻塞 conversation 写入 —— 记忆数据的可用性优先
  // 于资产登记的一致性（asset 登记失败时下次调用会自动重试）。
  if (deps.getMetadataService && iso?.teamId && iso?.agentId) {
    try {
      const metaSvc = await deps.getMetadataService(auth.serviceId);
      await metaSvc.ensureChatMemoryAsset({
        team_id: iso.teamId,
        agent_id: iso.agentId,
      });
    } catch (err) {
      deps.logger.warn(
        `${TAG} ensureChatMemoryAsset failed (team=${iso.teamId} agent=${iso.agentId}): ` +
        `${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  const embedding = deps.getEmbedding();
  const historyMode = storeContentHistoryEnabled(store);
  const acceptedIds: string[] = [];
  // Only NEWLY appended records — a duplicate replay never re-dispatches or
  // re-mirrors an already-processed message.
  const acceptedRecords: L0Record[] = [];
  const duplicateIds: string[] = [];

  if (historyMode) {
    // History mode: an append is a `create` L0 content commit, committed through
    // the store's OWN transaction (never a check-then-raw-upsert race). The
    // store's locator conflict / operation de-duplication decides the outcome,
    // and a stable operation id derived from the message id makes a repeat
    // idempotent. A retired message id is never revived.
    if (typeof store.commitContentChanges !== "function") {
      return errorEnvelope(501, "history_unavailable: this store has no formal content commit entry", requestId);
    }
    const seenIds = new Set<string>();
    for (const msg of messages) {
      if (!msg.id) {
        return errorEnvelope(
          400,
          "history_message_id_required: content history is enabled, so every appended message must carry a stable `id`" +
            " (a random id would create an uncontrolled duplicate version). Use `id` (and, optionally, `operation_id`).",
          requestId,
        );
      }
      const id = msg.id;
      if (seenIds.has(id)) {
        return errorEnvelope(
          409,
          `history_message_conflict: message ${id} is declared more than once in one request`,
          requestId,
        );
      }
      seenIds.add(id);
    }
    // Validate the complete request before its first durable append.
    for (const msg of messages) {
      const id = msg.id!;
      const scope = l0AppendScope(session_id, iso);
      const operationId = msg.operation_id ?? `conversation/add:${id}`;
      const fingerprint = fingerprintL0Append(id, msg, session_id, iso);
      const mutation = buildL0AppendMutation(id, msg, session_id, iso);
      mutation.draft.metadata = { ...(mutation.draft.metadata ?? {}), [PROGRAM_REQUEST_METADATA_KEY]: fingerprint };

      // A committed append with the SAME request semantics is a duplicate; a
      // different payload under the same stable id is a conflict.
      const committed = await lookupL0CommittedOperation(store, scope, operationId);
      if (committed) {
        if (committed.request_fingerprint !== fingerprint) {
          return errorEnvelope(
            409,
            `history_message_conflict: message ${id} already exists with different content; ` +
              `use content/write with operation_id + expected_version_id to revise it`,
            requestId,
          );
        }
        duplicateIds.push(id);
        acceptedIds.push(id);
        continue;
      }

      const filter = {
        teamId: iso?.teamId,
        userId: iso?.userId,
        agentId: iso?.agentId,
        taskId: iso?.taskId,
        sessionId: session_id,
      };
      let result;
      try {
        result = await store.commitContentChanges(
          { operation_id: operationId, actor: "user", mutations: [mutation] },
          filter,
        );
      } catch (err) {
        const code = (err as { code?: unknown } | null)?.code;
        if (
          code === "history_operation_conflict" || code === "history_locator_conflict" ||
          code === "content_id_exists" || code === "history_retired_conflict"
        ) {
          // A concurrent/losing writer may have committed the SAME request: only
          // an identical fingerprint is a duplicate; anything else stays 409.
          const retry = await lookupL0CommittedOperation(store, scope, operationId);
          if (retry && retry.request_fingerprint === fingerprint) {
            duplicateIds.push(id);
            acceptedIds.push(id);
            continue;
          }
          return errorEnvelope(
            409,
            `history_message_conflict: message ${id} already exists with different content; ` +
              `use content/write with operation_id + expected_version_id to revise it`,
            requestId,
          );
        }
        return errorEnvelope(
          historyWriteStatus(code),
          `history_message_write_failed: ${err instanceof Error ? err.message : String(err)}`,
          requestId,
        );
      }
      const version = result.versions[0];
      if (!version) {
        return errorEnvelope(503, `L0 write not confirmed for message ${id}`, requestId);
      }
      acceptedIds.push(id);
      acceptedRecords.push(l0AppendRecord(version, msg));
    }
  } else {
    // Legacy (history off): unchanged — random ids, optional batch insert. The
    // upsert return value is CHECKED: an unconfirmed write must not be reported
    // as accepted.
    const ingestBaseMs = Date.now();
    for (const [index, msg] of messages.entries()) {
      // Full UUID (hyphens stripped, 32 hex) — do NOT truncate. At ~1e8 messages
      // a 12-hex (48-bit) id collides ~18 times by the birthday bound.
      const id = msg.id ?? `msg-${randomUUID().replace(/-/g, "")}`;
      const ingestRecordedAtMs = ingestBaseMs + index;
      const recordedAtMs = msg.recorded_at
        ? new Date(msg.recorded_at).getTime()
        : ingestRecordedAtMs;
      const record: L0Record = {
        id,
        sessionKey: session_id,
        sessionId: session_id,
        taskId: iso?.taskId,
        // Tenancy isolation: resolveIsolation() defaults missing fields to the default bucket.
        teamId: iso?.teamId,
        userId: iso?.userId,
        agentId: iso?.agentId,
        role: msg.role,
        source: msg.source === "framework_event" ? "framework_event" : "conversation",
        messageText: msg.content,
        recordedAt: new Date(recordedAtMs).toISOString(),
        timestamp: msg.timestamp ? new Date(msg.timestamp).getTime() : recordedAtMs,
      };
      acceptedIds.push(id);
      acceptedRecords.push(record);
    }
    if (acceptedRecords.length > 0) {
      if (store.insertL0Batch && !embedding) {
        await store.insertL0Batch(acceptedRecords);
      } else {
        for (const record of acceptedRecords) {
          let emb: Float32Array | undefined;
          if (embedding) {
            try { emb = await embedding.embed(record.messageText); } catch (e) { console.warn(`[v2-router] L0 embedding failed:`, e); }
          }
          const written = await store.upsertL0(record, emb);
          if (written === false) {
            return errorEnvelope(503, `L0 write not confirmed for message ${record.id}`, requestId);
          }
        }
      }
    }
  }

  // Notify pipeline: trigger async L1 extraction (service mode).
  // Each role=user message counts as one conversation round for threshold/timer logic.
  // teamId/agentId 透传给 captureAtomic 决定 hash slot 与锁粒度。
  // Only NEWLY appended messages count — a duplicate replay of an already
  // persisted message must not re-dispatch an already-processed operation.
  if (deps.notifyPipeline && acceptedRecords.length > 0) {
    const rounds = acceptedRecords.filter((r) => r.role === "user").length;
    if (rounds > 0) {
      try {
        await deps.notifyPipeline(auth.serviceId, session_id, rounds, iso?.teamId, iso?.agentId);
      } catch (err) {
        // Non-fatal: L0 is already persisted, pipeline will catch up later
        deps.logger.warn(`${TAG} Pipeline notify failed for ${session_id}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  // Standalone-only: mirror L0 to <dataDir>/conversations/<date>.jsonl.
  // Parity with v1 /capture (l0-recorder) path — gives humans a grep-able audit
  // log alongside SQLite. Service mode skips: COS is the authoritative store,
  // and writing to local FS in a multi-replica pod would be ephemeral + useless.
  // Failure is non-fatal: SQLite is the source of truth.
  if (deps.deployMode === "standalone") {
    const storage = deps.getStorage();
    if (storage) {
      try {
        const linesByRecordKey = new Map<string, string[]>();
        for (const record of acceptedRecords) {
          const recordKey = StoragePaths.conversation(formatLocalDateForJsonl(new Date(record.recordedAt)));
          const lines = linesByRecordKey.get(recordKey) ?? [];
          lines.push(JSON.stringify({
            id: record.id,
            sessionKey: record.sessionKey,
            sessionId: record.sessionId,
            taskId: record.taskId,
            teamId: record.teamId,
            userId: record.userId,
            agentId: record.agentId,
            role: record.role,
            content: record.messageText,
            recordedAt: record.recordedAt,
            timestamp: record.timestamp,
          }));
          linesByRecordKey.set(recordKey, lines);
        }
        for (const [recordKey, lines] of linesByRecordKey) {
          await storage.appendFile(recordKey, `${lines.join("\n")}\n`);
        }
      } catch (err) {
        deps.logger.warn(`${TAG} JSONL mirror failed for ${session_id}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  // Report memory usage (non-fatal)
  if (deps.quotaManager && acceptedIds.length > 0) {
    deps.quotaManager.reportMemoryAdded(auth.serviceId, acceptedIds.length).catch(() => {});
  }

  return successEnvelope<ConversationAddData>(
    {
      accepted_ids: acceptedIds,
      accepted_versions: acceptedIds.map(() => "v1"),
      total_count: acceptedIds.length,
      appended: acceptedRecords.length,
      duplicates: duplicateIds.length,
    },
    requestId,
  );
}

async function handleConversationQuery(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = conversationQueryRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { session_id, time_start, time_end } = parsed.data;
  const limit = parsed.data.limit ?? 20;
  const offset = parsed.data.offset ?? 0;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);

  // Tenancy isolation — narrow the query when caller supplied user_id /
  // agent_id (via body or headers). session_id, if present, comes from the
  // request body and is already in `session_id`.
  const iso = deps.requestIsolation;

  // Use paginated query if available (AR-3), else fallback
  if (store.queryL0Paginated) {
    const result = await store.queryL0Paginated({
      sessionId: session_id,
      teamId: iso?.teamId,
      userId: iso?.userId,
      agentId: iso?.agentId,
      taskId: iso?.taskId,
      timeStartMs: time_start ? new Date(time_start).getTime() : undefined,
      timeEndMs: time_end ? new Date(time_end).getTime() : undefined,
      limit,
      offset,
    });

    const messages: ConversationItem[] = result.rows.map((r) => ({
      id: r.record_id,
      session_id: r.session_id,
      team_id: r.team_id,
      user_id: r.user_id,
      agent_id: r.agent_id,
      task_id: r.task_id,
      role: r.role as ConversationItem["role"],
      content: r.message_text,
      timestamp: r.recorded_at,
    }));

    return successEnvelope<ConversationQueryData>({ messages, total: result.total }, requestId);
  }

  // Fallback: legacy path (capped at 1000 for safety)
  const allRows = await store.queryL0ForL1(session_id ?? "", undefined, 1000);
  let filtered = session_id ? allRows.filter((r) => r.session_key === session_id || r.session_id === session_id) : allRows;
  // Tenancy isolation post-filter for the legacy path.
  if (iso?.teamId) filtered = filtered.filter((r) => r.team_id === iso.teamId);
  if (iso?.userId) filtered = filtered.filter((r) => r.user_id === iso.userId);
  if (iso?.agentId) filtered = filtered.filter((r) => r.agent_id === iso.agentId);
  if (iso?.taskId) filtered = filtered.filter((r) => r.task_id === iso.taskId);
  if (time_start) { const ms = new Date(time_start).getTime(); filtered = filtered.filter((r) => r.timestamp >= ms); }
  if (time_end) { const ms = new Date(time_end).getTime(); filtered = filtered.filter((r) => r.timestamp <= ms); }
  const total = filtered.length;
  const page = filtered.slice(offset, offset + limit);
  const messages: ConversationItem[] = page.map((r) => ({
    id: r.record_id,
    session_id: r.session_id,
    team_id: r.team_id,
    user_id: r.user_id,
    agent_id: r.agent_id,
    task_id: r.task_id,
    role: r.role as ConversationItem["role"],
    content: r.message_text,
    timestamp: r.recorded_at,
  }));

  return successEnvelope<ConversationQueryData>({ messages, total }, requestId);
}

/**
 * `/conversation/get` — read one L0 message by id.
 *
 * Identity is enforced at the store query; an absent id and an id owned by
 * another scope both return 404, so the endpoint never reveals cross-scope
 * existence. `version` (the legacy numeric field) is rejected explicitly
 * rather than silently ignored.
 *
 * When content history is enabled the message carries a unified `ref`, and an
 * explicit `version_id` reads that exact historical version. An unknown version
 * is an explicit miss — it never falls back to the current message.
 */
async function handleConversationGet(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = conversationGetRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  if (parsed.data.version !== undefined) {
    return errorEnvelope(400, "unsupported_version: historical L0 reads are not implemented; omit version", requestId);
  }

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);
  if (typeof store.getL0RecordById !== "function") {
    return errorEnvelope(501, "unsupported: this store does not support L0 read by id", requestId);
  }

  const filter = buildContentIsolationFilter(deps.requestIsolation);
  const row = await store.getL0RecordById(parsed.data.id, filter);

  // A retired L0 message has no live row, but its immutable version is still
  // reachable by its old record id — resolved behind the trusted authorization
  // filter, so a caller never supplies a scope and existence is not leaked.
  if (!row && parsed.data.version_id !== undefined) {
    const retired = await resolveStoreRecordHistoryByLocator(deps, "l0", parsed.data.id, parsed.data.version_id);
    if (!retired.ok) return errorEnvelope(retired.failure.status, retired.failure.message, requestId);
    if (retired.version) {
      const fallback = {
        id: retired.version.locator,
        role: "user",
        content: retired.version.body,
        timestamp: retired.version.recorded_at,
      } as unknown as ConversationItem;
      return successEnvelope<ConversationGetData>({
        message: conversationItemFromVersion(retired.version, fallback),
        ref: retired.version.ref,
        content_version: contentVersionView(retired.version),
      }, requestId);
    }
  }

  if (!row) return errorEnvelope(404, `Conversation message not found: ${parsed.data.id}`, requestId);

  const currentMessage: ConversationItem = {
    id: row.record_id,
    session_id: row.session_id,
    team_id: row.team_id,
    user_id: row.user_id,
    agent_id: row.agent_id,
    task_id: row.task_id,
    role: row.role as ConversationItem["role"],
    content: row.message_text,
    timestamp: row.recorded_at,
  };

  const history = await resolveStoreRecordHistory(deps, "l0", row.record_id, row, parsed.data.version_id);
  if (!history.ok) return errorEnvelope(history.failure.status, history.failure.message, requestId);

  const data: ConversationGetData = {
    message: history.version ? conversationItemFromVersion(history.version, currentMessage) : currentMessage,
  };
  if (history.ref) data.ref = history.ref;
  if (history.version) data.content_version = contentVersionView(history.version);
  return successEnvelope<ConversationGetData>(data, requestId);
}

async function handleConversationCount(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = conversationCountRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { session_id, time_start, time_end } = parsed.data;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);
  const iso = deps.requestIsolation;

  const countFilter = {
    sessionId: session_id,
    teamId: iso?.teamId,
    userId: iso?.userId,
    agentId: iso?.agentId,
    taskId: iso?.taskId,
    timeStartMs: time_start ? new Date(time_start).getTime() : undefined,
    timeEndMs: time_end ? new Date(time_end).getTime() : undefined,
  };
  const total = await store.countL0(countFilter);
  return successEnvelope<CountData>({ total }, requestId);

  const allRows = await store.queryL0ForL1(session_id ?? "", undefined, 10000);
  let filtered = session_id ? allRows.filter((r) => r.session_key === session_id || r.session_id === session_id) : allRows;
  if (iso?.teamId) filtered = filtered.filter((r) => r.team_id === iso.teamId);
  if (iso?.userId) filtered = filtered.filter((r) => r.user_id === iso.userId);
  if (iso?.agentId) filtered = filtered.filter((r) => r.agent_id === iso.agentId);
  if (iso?.taskId) filtered = filtered.filter((r) => r.task_id === iso.taskId);
  if (time_start) { const ms = new Date(time_start).getTime(); filtered = filtered.filter((r) => r.timestamp >= ms); }
  if (time_end) { const ms = new Date(time_end).getTime(); filtered = filtered.filter((r) => r.timestamp <= ms); }
  return successEnvelope<CountData>({ total: filtered.length }, requestId);
}

async function handleConversationSearch(body: unknown, auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = conversationSearchRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { query, session_id } = parsed.data;
  const limit = parsed.data.limit ?? 5;

  const tStart = performance.now();
  // 搜索场景：只使用显式传入的 isolation 维度作为 filter，避免默认 sessionId="default" 错误过滤真实 session 数据。
  // 当请求未传 session_id 时，搜索应跨 session；当传了 session_id 时，由 sessionKey 参数单独过滤。
  const iso = deps.requestIsolation;
  const searchFilter = iso ? {
    ...(iso.teamId ? { teamId: iso.teamId } : {}),
    ...(iso.userId ? { userId: iso.userId } : {}),
    ...(iso.agentId ? { agentId: iso.agentId } : {}),
    ...(iso.taskId ? { taskId: iso.taskId } : {}),
    // 不传 sessionId：全局搜索不应被默认 sessionId 限制
  } : undefined;
  const result = await executeConversationSearch({
    query,
    limit,
    sessionKey: session_id,
    filter: searchFilter,
    vectorStore: deps.getStore(),
    embeddingService: deps.getEmbedding(),
    logger: deps.logger,
  });
  const recallLatencyMs = performance.now() - tStart;

  // 非侵入式上报召回指标（service 模式，静默失败，绝不影响业务返回）
  // L0 conversation search 同样属于"召回"行为，strategy 映射逻辑与 L1 相同
  try {
    reportRecallMetrics({
      instanceId: auth.serviceId,
      recalledL1Memories: result.results.map((r) => ({ content: r.content, score: r.score, type: "conversation" })),
      recallStrategy: result.strategy === "fts" ? "keyword" : result.strategy === "none" ? "skipped" : result.strategy,
      recallLatencyMs,
      hasError: false,
    });
  } catch {
    // 静默失败
  }

  // 非侵入式在当前 Span 上记录 recall query 和 results
  try {
    const otelApi = await import("@opentelemetry/api");
    const activeSpan = otelApi.trace.getSpan(otelApi.context.active());
    if (activeSpan) {
      activeSpan.setAttribute("tdai.recall.query", query);
      activeSpan.setAttribute("tdai.recall.hitCount", result.results.length);
      activeSpan.setAttribute("tdai.recall.strategy", result.strategy || "unknown");
      activeSpan.setAttribute("tdai.recall.level", "l0");
      if (result.results.length > 0) {
        activeSpan.setAttribute("tdai.recall.topScore", Math.max(...result.results.map(r => r.score)));
        const truncatedResults = result.results.slice(0, 5).map(r => ({
          content: r.content.substring(0, 200),
          score: r.score,
        }));
        activeSpan.setAttribute("tdai.recall.results", JSON.stringify(truncatedResults));
      } else {
        activeSpan.setAttribute("tdai.recall.results", "[]");
      }
    }
  } catch {
    // 静默失败
  }

  const messages: ConversationSearchHit[] = result.results.map((r) => ({
    id: r.id, role: r.role as ConversationSearchHit["role"], content: r.content, timestamp: r.recorded_at, score: r.score,
  }));

  return successEnvelope<ConversationSearchData>({ messages }, requestId);
}

async function handleConversationDelete(body: unknown, auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = conversationDeleteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  // schema 已归一：message_ids / session_ids 都是去重后的数组（单数 session_id 已合并进来）。
  const { message_ids, session_ids } = parsed.data;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);

  // History mode refuses a destructive L0 delete; retire via content/write instead.
  const historyForbidden = historyModeDeleteForbidden(store, requestId, "conversation/delete");
  if (historyForbidden) return historyForbidden;

  // 删除的作用域是 (team, user, agent)，**不含 session**。
  // /v3 严格 isolation 要求请求头带 session_id，但删除语义是"删指定的
  // message / session"，若把 requestIsolation.sessionId 也塞进 filter，
  // 就只能删当前会话 —— 批量删多个 session 会被静默过滤成 0 条。
  const iso = deps.requestIsolation;
  const scope = iso
    ? { teamId: iso.teamId, userId: iso.userId, agentId: iso.agentId, taskId: iso.taskId }
    : undefined;

  // message_ids 与 session_ids 可同时给出；两条路径都跑，用 id 集合去重避免
  // 同一条消息被session 路径重复计数。
  const deletedRecordIds = new Set<string>();

  for (const id of message_ids) {
    const ok = await store.deleteL0(id, scope);
    if (ok) deletedRecordIds.add(id);
  }

  let sessionDeletedCount = 0;
  for (const sessionId of session_ids) {
    if (store.deleteL0BySession) {
      sessionDeletedCount += await store.deleteL0BySession(sessionId, scope);
      continue;
    }
    // Fallback：store 未实现按 session 删除时逐条删。
    const rows = await store.queryL0ForL1(sessionId, undefined, 10000);
    const sessionRows = rows.filter((r) => r.session_key === sessionId || r.session_id === sessionId);
    for (const row of sessionRows) {
      if (deletedRecordIds.has(row.record_id)) continue;
      const ok = await store.deleteL0(row.record_id, scope);
      if (ok) deletedRecordIds.add(row.record_id);
    }
  }

  // deleteL0BySession 只回行数（无法回id），所以两条路径的计数相加。
  // 同一批请求里message_ids 与 session_ids 重叠时可能轻微重复计数，
  // 这是 store 接口的既有限制，不影响实际删除的正确性。
  const deletedCount = deletedRecordIds.size + sessionDeletedCount;

  // Report memory deletion (non-fatal)
  if (deps.quotaManager && deletedCount > 0) {
    deps.quotaManager.reportMemoryDeleted(auth.serviceId, deletedCount).catch(() => {});
  }

  return successEnvelope<ConversationDeleteData>({ deleted_count: deletedCount }, requestId);
}

async function handleAtomicUpdate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = atomicUpdateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { id, content, background } = parsed.data;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);

  // History mode: an L1 modification without an expected_version_id / operation_id
  // cannot be CAS-checked, so it is refused instead of silently revising.
  const historyForbidden = historyModeStoreWriteForbidden(store, requestId, "atomic/update");
  if (historyForbidden) return historyForbidden;

  const iso = deps.requestIsolation;
  // Scope the read itself: an id owned by another team/user/agent/task is
  // indistinguishable from a missing one (existence never leaks). The model
  // cannot widen this by putting anything in metadata.
  //
  // Use the exact by-id read: `queryL1Records({ recordIds })` is NOT guaranteed
  // to filter by id (SQLite ignores recordIds), so relying on it could mutate
  // the wrong row. Fall back to an exact match only when the store lacks the
  // single-read capability.
  const scopeFilter = buildContentIsolationFilter(iso);
  let record: L1RecordRow | null;
  if (typeof store.getL1RecordById === "function") {
    record = await store.getL1RecordById(id, scopeFilter);
  } else {
    const rows = await store.queryL1Records({ ...(scopeFilter ?? {}) });
    record = rows.find((row) => row.record_id === id) ?? null;
  }
  if (!record) {
    return errorEnvelope(404, `Atomic note not found: ${id}`, requestId);
  }

  const now = new Date().toISOString();

  // Defense in depth: even if a store ignores the query filter, re-check every
  // supplied isolation dimension before mutating the row.
  if (iso?.teamId && record.team_id && record.team_id !== iso.teamId) {
    return errorEnvelope(403, `Atomic note ${id} belongs to a different team`, requestId);
  }
  if (iso?.userId && record.user_id && record.user_id !== iso.userId) {
    return errorEnvelope(403, `Atomic note ${id} belongs to a different user`, requestId);
  }
  if (iso?.agentId && record.agent_id && record.agent_id !== iso.agentId) {
    return errorEnvelope(403, `Atomic note ${id} belongs to a different agent`, requestId);
  }
  if (iso?.taskId && record.task_id && record.task_id !== iso.taskId) {
    return errorEnvelope(403, `Atomic note ${id} belongs to a different task`, requestId);
  }

  // Sources describe the content bytes they were extracted from. If the content
  // is unchanged, keep them; if it actually changed, the old declaration no
  // longer describes the new bytes and must become unknown (never reused).
  const contentUnchanged = record.content === content;
  const preservedSourceIds = contentUnchanged
    ? parseSourceMessageIdsJson(record.source_message_ids_json, record.record_id, deps.logger)
    : [];

  const updatedVersion = (record.version ?? 0) + 1;
  const updated: MemoryRecord = {
    id,
    content,
    type: record.type as any,
    priority: record.priority ?? 50,
    scene_name: background !== undefined ? background : (record.scene_name ?? ""),
    source_message_ids: preservedSourceIds,
    metadata: parseMetadataJson(record.metadata_json),
    timestamps: record.timestamp_str ? [record.timestamp_str] : [],
    createdAt: record.created_time,
    updatedAt: now,
    version: updatedVersion,
    sessionKey: record.session_key ?? "",
    sessionId: record.session_id ?? iso?.sessionId ?? "",
    taskId: record.task_id ?? iso?.taskId,
    teamId: record.team_id ?? iso?.teamId,
    userId: record.user_id ?? iso?.userId,
    agentId: record.agent_id ?? iso?.agentId,
  };

  const embedding = deps.getEmbedding();
  let emb: Float32Array | undefined;
  if (embedding) { try { emb = await embedding.embed(content); } catch (e) { console.warn(`[v2-router] L1 embedding failed:`, e); } }

  if (!await store.upsertL1(updated, emb)) return errorEnvelope(503, "Atomic content update failed; result not confirmed", requestId);

  // 审计：L1 update — 用外部请求的 IdFields 而非 record 原值（per user 决策）
  await recordAudit(store, {
    record_id: id,
    layer: "L1",
    action: "update",
    iso,
    version: updatedVersion,
    requestId,
    logger: deps.logger,
  });

  return successEnvelope<AtomicUpdateData>({
    id,
    version: `v${updatedVersion}`,
    updated_at: now,
    source: buildAtomicSourceView(preservedSourceIds),
  }, requestId);
}

async function handleAtomicQuery(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = atomicQueryRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { type, time_start, time_end } = parsed.data;
  const limit = parsed.data.limit ?? 20;
  const offset = parsed.data.offset ?? 0;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);

  // Tenancy isolation — narrow the query by user_id / agent_id when supplied.
  const iso = deps.requestIsolation;

  // Use paginated query if available
  if (store.queryL1Paginated) {
    const result = await store.queryL1Paginated({
      type, timeStart: time_start, timeEnd: time_end, limit, offset,
      teamId: iso?.teamId, userId: iso?.userId, agentId: iso?.agentId, taskId: iso?.taskId,
    });
    const items: AtomicDetail[] = result.rows.map((r) => ({
      id: r.record_id, type: r.type, content: r.content,
      background: r.scene_name || undefined,
      version: r.version ?? 0,
      team_id: r.team_id,
      user_id: r.user_id,
      agent_id: r.agent_id,
      task_id: r.task_id,
      created_at: r.created_time, updated_at: r.updated_time,
    }));
    return successEnvelope<AtomicQueryData>({ items, total: result.total }, requestId);
  }

  // Fallback: legacy
  const allRecords = await store.queryL1Records();
  let filtered = allRecords;
  if (type) filtered = filtered.filter((r) => r.type === type);
  if (iso?.teamId) filtered = filtered.filter((r) => r.team_id === iso.teamId);
  if (iso?.userId) filtered = filtered.filter((r) => r.user_id === iso.userId);
  if (iso?.agentId) filtered = filtered.filter((r) => r.agent_id === iso.agentId);
  if (iso?.taskId) filtered = filtered.filter((r) => r.task_id === iso.taskId);
  if (time_start) filtered = filtered.filter((r) => r.updated_time >= time_start);
  if (time_end) filtered = filtered.filter((r) => r.updated_time <= time_end);
  const total = filtered.length;
  const page = filtered.slice(offset, offset + limit);
  const items: AtomicDetail[] = page.map((r) => ({
    id: r.record_id, type: r.type, content: r.content,
    background: r.scene_name || undefined,
    version: r.version ?? 0,
    team_id: r.team_id,
    user_id: r.user_id,
    agent_id: r.agent_id,
    task_id: r.task_id,
    created_at: r.created_time, updated_at: r.updated_time,
  }));

  return successEnvelope<AtomicQueryData>({ items, total }, requestId);
}

/**
 * `/atomic/get` — read one L1 memory by id, including superseded versions.
 *
 * Live rows return `current: true`. Rows replaced by an update/merge are
 * retained in the store's supersession archive and returned with
 * `current: false` plus `superseded_by` when the replacement is known. Old
 * (superseded) records are never returned by search/recall.
 */
async function handleAtomicGet(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = atomicGetRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  if (parsed.data.version !== undefined) {
    return errorEnvelope(400, "unsupported_version: historical L1 reads are not implemented; omit version", requestId);
  }

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);
  if (typeof store.getL1RecordById !== "function") {
    return errorEnvelope(501, "unsupported: this store does not support L1 read by id", requestId);
  }

  const filter = buildContentIsolationFilter(deps.requestIsolation);
  const current = await store.getL1RecordById(parsed.data.id, filter);
  if (current) {
    return finishAtomicGet(deps, requestId, {
      item: {
        id: current.record_id,
        type: current.type,
        content: current.content,
        background: current.scene_name || undefined,
        version: current.version ?? 0,
        team_id: current.team_id,
        user_id: current.user_id,
        agent_id: current.agent_id,
        task_id: current.task_id,
        created_at: current.created_time,
        updated_at: current.updated_time,
        current: true,
        source: buildAtomicSourceView(parseSourceMessageIdsJson(current.source_message_ids_json, current.record_id, deps.logger)),
      },
    }, current, parsed.data.version_id);
  }

  // Superseded (replaced) rows are only reachable when the store keeps the
  // archive; a store without it returns an explicit 404, never a fabricated
  // "current" result.
  if (typeof store.getL1SupersededRecord === "function") {
    const old = await store.getL1SupersededRecord(parsed.data.id, filter);
    if (old) {
      // Follow the full replacement chain: a single hop's `new_id` may itself
      // have been superseded since. Callers must be able to reach the live id.
      const successor = old.new_id || undefined;
      let currentId: string | undefined;
      const visited = new Set<string>([old.old_id]);
      let cursor: typeof old | null = old;
      while (cursor && cursor.new_id && !visited.has(cursor.new_id)) {
        visited.add(cursor.new_id);
        const live = await store.getL1RecordById!(cursor.new_id, filter);
        if (live) {
          currentId = live.record_id;
          break;
        }
        cursor = await store.getL1SupersededRecord!(cursor.new_id, filter);
      }

      return finishAtomicGet(deps, requestId, {
        item: {
          id: old.old_id,
          type: old.type,
          content: old.content,
          background: old.scene_name || undefined,
          version: old.version ?? 0,
          team_id: old.team_id,
          user_id: old.user_id,
          agent_id: old.agent_id,
          task_id: old.task_id,
          created_at: old.created_time,
          updated_at: old.updated_time,
          current: false,
          ...(successor ? { superseded_by: successor, successor } : {}),
          ...(currentId ? { current_id: currentId } : {}),
          source: buildAtomicSourceView(old.source_message_ids),
        },
      }, old, parsed.data.version_id);
    }
  }

  // A retired identity has no live row and is not in the supersession archive,
  // but its immutable version is still reachable by its old record id (the
  // locator mapping is kept). Resolve it behind the trusted authorization filter
  // — the caller never supplies a scope, and existence is not leaked.
  if (parsed.data.version_id !== undefined) {
    const retired = await resolveStoreRecordHistoryByLocator(deps, "l1", parsed.data.id, parsed.data.version_id);
    if (!retired.ok) return errorEnvelope(retired.failure.status, retired.failure.message, requestId);
    if (retired.version) {
      return successEnvelope<AtomicGetData>({
        item: atomicItemFromVersion(retired.version, false),
        content_version: contentVersionView(retired.version),
      }, requestId);
    }
  }

  return errorEnvelope(404, `Atomic note not found: ${parsed.data.id}`, requestId);
}

/**
 * Attach the content-history reference to an L1 read (and, when an explicit
 * `version_id` was requested, return that exact historical version instead of
 * the current/archived row).
 */
async function finishAtomicGet(
  deps: V2RouterDeps,
  requestId: string,
  result: AtomicGetData,
  row: { team_id?: string; user_id?: string; agent_id?: string; task_id?: string },
  versionId: string | undefined,
): Promise<ApiResponseEnvelope> {
  const item = result.item;
  const history = await resolveStoreRecordHistory(deps, "l1", item.id, row, versionId);
  if (!history.ok) return errorEnvelope(history.failure.status, history.failure.message, requestId);
  let finalItem: AtomicGetData["item"] = item;
  if (history.version) {
    // One pinned version supplies body / type / background / source / ref, so a
    // race between reading the current row and resolving the ref can never mix
    // two commits into one response. An explicit `version_id` addresses a
    // historical version, so it is never reported as the current row.
    finalItem = {
      ...item,
      ...atomicItemFromVersion(history.version, versionId === undefined && item.current === true),
      // The numeric legacy version only describes the live row; a historical
      // read must not pass the current number off as the old version's number.
      version: versionId !== undefined ? 0 : item.version,
    };
  } else if (history.ref) {
    finalItem = { ...item, ref: history.ref };
  }
  const data: AtomicGetData = { item: finalItem };
  if (history.version) data.content_version = contentVersionView(history.version);
  return successEnvelope<AtomicGetData>(data, requestId);
}

async function handleAtomicCount(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = atomicCountRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { type, time_start, time_end } = parsed.data;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);
  const iso = deps.requestIsolation;

  const total = await store.countL1({
    type,
    timeStart: time_start,
    timeEnd: time_end,
    teamId: iso?.teamId,
    userId: iso?.userId,
    agentId: iso?.agentId,
    taskId: iso?.taskId,
  });
  return successEnvelope<CountData>({ total }, requestId);
}

async function handleAtomicSearch(body: unknown, auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = atomicSearchRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { query, type } = parsed.data;
  const limit = parsed.data.limit ?? 5;

  const tStart = performance.now();
  // L1 召回为 agent 维度（跨 session）：filter 只取 team/user/agent/task，
  // **不带 sessionId**，否则会把其它 session 写入的 L1 记忆过滤掉，导致
  // 新会话里召不回历史记忆（与 conversation/search 的处理保持一致）。
  const iso = deps.requestIsolation;
  const searchFilter = iso ? {
    ...(iso.teamId ? { teamId: iso.teamId } : {}),
    ...(iso.userId ? { userId: iso.userId } : {}),
    ...(iso.agentId ? { agentId: iso.agentId } : {}),
    ...(iso.taskId ? { taskId: iso.taskId } : {}),
    // 不传 sessionId：L1 召回应跨 session（agent 维度）
  } : undefined;
  const result = await executeMemorySearch({
    query, limit, type,
    filter: searchFilter,
    vectorStore: deps.getStore(),
    embeddingService: deps.getEmbedding(),
    logger: deps.logger,
  });
  const recallLatencyMs = performance.now() - tStart;

  // 非侵入式上报召回指标（service 模式，静默失败，绝不影响业务返回）
  try {
    reportRecallMetrics({
      instanceId: auth.serviceId,
      recalledL1Memories: result.results.map((r) => ({ content: r.content, score: r.score, type: r.type })),
      recallStrategy: result.strategy === "fts" ? "keyword" : result.strategy === "none" ? "skipped" : result.strategy,
      recallLatencyMs,
      hasError: false,
    });
  } catch {
    // 静默失败
  }

  // 非侵入式在当前 Span 上记录 recall query 和 results，供在线评测系统消费
  try {
    const { getObservabilityBackend } = await import("../core/report/factory.js");
    const ctx = getObservabilityBackend().tracePropagation.serializeTraceContext();
    if (ctx && (ctx as any)._traceId) {
      // 通过 OTel API 在当前 span 上添加属性
      try {
        const otelApi = await import("@opentelemetry/api");
        const activeSpan = otelApi.trace.getSpan(otelApi.context.active());
        if (activeSpan) {
          activeSpan.setAttribute("tdai.recall.query", query);
          activeSpan.setAttribute("tdai.recall.hitCount", result.results.length);
          activeSpan.setAttribute("tdai.recall.strategy", result.strategy || "unknown");
          if (result.results.length > 0) {
            activeSpan.setAttribute("tdai.recall.topScore", Math.max(...result.results.map(r => r.score)));
            // 限制 results 属性长度（OTel 属性不宜过长），最多前 5 条
            const truncatedResults = result.results.slice(0, 5).map(r => ({
              content: r.content.substring(0, 200),
              score: r.score,
              type: r.type,
            }));
            activeSpan.setAttribute("tdai.recall.results", JSON.stringify(truncatedResults));
          } else {
            activeSpan.setAttribute("tdai.recall.results", "[]");
          }
          activeSpan.setAttribute("tdai.recall.level", type === "l0" ? "l0" : "l1");
        }
      } catch {
        // OTel API 不可用时静默降级
      }
    }
  } catch {
    // 静默失败
  }

  const items: AtomicSearchHit[] = result.results.map((r) => ({
    id: r.id, type: r.type, content: r.content,
    background: r.scene_name || undefined,
    version: r.version ?? 0,
    team_id: r.team_id,
    user_id: r.user_id,
    agent_id: r.agent_id,
    task_id: r.task_id,
    created_at: r.created_at, updated_at: r.updated_at, score: r.score,
  }));

  // 来源标注与撤回指针是读取方判断认识有效性的必要信息（design §5.3/§5.4）。
  // 它们在版本元数据里（行内 metadata 按设计不含 canonical 键），按命中逐条
  // 从 content history 补充；解析失败不虚构，留空即未标注。
  const historyReader = deps.getStore()?.getContentHistory?.();
  if (historyReader) {
    const authFilter = buildContentHistoryAuthFilter(deps.requestIsolation);
    for (const item of items) {
      const resolved = historyReader.resolveLocatorAuthorized("l1", item.id, authFilter);
      if (!resolved) continue;
      const version = await readVersionAsync(historyReader, {
        layer: "l1", content_id: resolved.ref.content_id, scope: resolved.scope,
      });
      const meta = version?.metadata as Record<string, unknown> | undefined;
      if (typeof meta?.source === "string") item.l1_source = meta.source;
      if (typeof meta?.withdraws === "string") item.withdraws = meta.withdraws;
    }
  }

  return successEnvelope<AtomicSearchData>({ items }, requestId);
}

async function handleAtomicDelete(body: unknown, auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = atomicDeleteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { ids } = parsed.data;

  const store = deps.getStore();
  if (!store) return errorEnvelope(503, "Store not available", requestId);

  // History mode refuses a destructive L1 delete; retire via content/write instead.
  const historyForbidden = historyModeDeleteForbidden(store, requestId, "atomic/delete");
  if (historyForbidden) return historyForbidden;

  // deleteL1Batch returns bool, but we need actual count
  // Fall back to per-id deletion for accurate counting
  // 按 id 删除时，只使用显式传入的 isolation 维度作为 filter。
  // 避免默认 sessionId="default" 导致真实 session 下的 L1 记录无法删除。
  const iso = deps.requestIsolation;
  const deleteFilter = iso ? {
    ...(iso.teamId ? { teamId: iso.teamId } : {}),
    ...(iso.userId ? { userId: iso.userId } : {}),
    ...(iso.agentId ? { agentId: iso.agentId } : {}),
    ...(iso.taskId ? { taskId: iso.taskId } : {}),
    // 不传 sessionId：按 id 删除不应被默认 sessionId 限制
  } : undefined;
  let deletedCount = 0;
  const deletedIds: string[] = [];
  for (const id of ids) {
    const ok = await store.deleteL1(id, deleteFilter);
    if (ok) {
      deletedCount++;
      deletedIds.push(id);
    }
  }

  // 审计：L1 delete — 每条删除一行 audit
  for (const id of deletedIds) {
    await recordAudit(store, {
      record_id: id,
      layer: "L1",
      action: "delete",
      iso: deps.requestIsolation,
      version: 0, // 已删除，无新版本
      requestId,
      logger: deps.logger,
    });
  }

  // Report memory deletion (non-fatal)
  if (deps.quotaManager && deletedCount > 0) {
    deps.quotaManager.reportMemoryDeleted(auth.serviceId, deletedCount).catch(() => {});
  }

  return successEnvelope<AtomicDeleteData>({ deleted_count: deletedCount }, requestId);
}

// ============================
// Entity Metadata Handlers (Team / User / Agent / Task)
// ============================

type EntityStore = IMemoryStore & {
  createTeam?: (input: any) => TeamData | Promise<TeamData>;
  getTeam?: (id: string) => TeamData | null | Promise<TeamData | null>;
  updateTeam?: (id: string, patch: any) => TeamData | null | Promise<TeamData | null>;
  deleteTeams?: (ids: string[]) => BatchDeleteResult | Promise<BatchDeleteResult>;
  createUser?: (input: any) => UserData | Promise<UserData>;
  getUser?: (id: string) => UserData | null | Promise<UserData | null>;
  updateUser?: (id: string, patch: any) => UserData | null | Promise<UserData | null>;
  deleteUsers?: (ids: string[]) => BatchDeleteResult | Promise<BatchDeleteResult>;
  createAgent?: (input: any) => AgentData | Promise<AgentData>;
  getAgent?: (id: string) => AgentData | null | Promise<AgentData | null>;
  updateAgent?: (id: string, patch: any) => AgentData | null | Promise<AgentData | null>;
  deleteAgents?: (ids: string[]) => BatchDeleteResult | Promise<BatchDeleteResult>;
  createTask?: (input: any) => TaskData | Promise<TaskData>;
  getTask?: (id: string) => TaskData | null | Promise<TaskData | null>;
  updateTask?: (id: string, patch: any) => TaskData | null | Promise<TaskData | null>;
  deleteTasks?: (ids: string[]) => BatchDeleteResult | Promise<BatchDeleteResult>;
};

function getEntityStore(deps: V2RouterDeps): EntityStore | undefined {
  return deps.getStore() as EntityStore | undefined;
}

function missingEntityStore(requestId: string): ApiResponseEnvelope {
  return errorEnvelope(503, "Entity metadata store not available", requestId);
}

/* ============================================================================
 * @deprecated v2 entity handlers（team/user/agent/task，下方 16 个）。
 *
 * 这些 handler 走旧 `entity_*` 聚合数组模型（EntityStore 可选方法）。元数据已迁移到
 * v3 规范化模型（`/v3/meta/*` + MetadataService），control 面板 remote 模式直连 v3。
 * 保留这些 handler 仅为兼容现网既有调用方，**行为冻结、不再演进**；新接入方一律用 v3。
 * 计划：确认无外部调用方后，连同 routeTable 中对应条目与 `entity_*` 表一并删除。
 * 决策（2026-06-26，per user）：标记废弃 + 代码暂留，不适配 MetadataService。
 * ========================================================================== */

/** @deprecated 改用 `/v3/meta/team/create`（MetadataService.createTeam）。 */
async function handleTeamCreate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = teamCreateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.createTeam) return missingEntityStore(requestId);
  return successEnvelope<TeamData>(await store.createTeam(parsed.data), requestId);
}

/** @deprecated 改用 `/v3/meta/team/get`。 */
async function handleTeamGet(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = teamGetRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.getTeam) return missingEntityStore(requestId);
  const data = await store.getTeam(parsed.data.team_id);
  return data ? successEnvelope<TeamData>(data, requestId) : errorEnvelope(404, "Team not found", requestId);
}

/** @deprecated 改用 `/v3/meta/team/update`。 */
async function handleTeamUpdate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = teamUpdateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { team_id, ...patch } = parsed.data;
  const store = getEntityStore(deps);
  if (!store?.updateTeam) return missingEntityStore(requestId);
  const data = await store.updateTeam(team_id, patch);
  return data ? successEnvelope<TeamData>(data, requestId) : errorEnvelope(404, "Team not found", requestId);
}

/** @deprecated 改用 `/v3/meta/team/delete`。 */
async function handleTeamDelete(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = teamBatchDeleteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.deleteTeams) return missingEntityStore(requestId);
  return successEnvelope<BatchDeleteResult>(await store.deleteTeams(parsed.data.team_ids), requestId);
}

/** @deprecated 改用 `/v3/meta/user/create`。 */
async function handleUserCreate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = userCreateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.createUser) return missingEntityStore(requestId);
  return successEnvelope<UserData>(await store.createUser(parsed.data), requestId);
}

/** @deprecated 改用 `/v3/meta/user/get`。 */
async function handleUserGet(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = userGetRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.getUser) return missingEntityStore(requestId);
  const data = await store.getUser(parsed.data.user_id);
  return data ? successEnvelope<UserData>(data, requestId) : errorEnvelope(404, "User not found", requestId);
}

/** @deprecated 改用 `/v3/meta/user/update`。 */
async function handleUserUpdate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = userUpdateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { user_id, ...patch } = parsed.data;
  const store = getEntityStore(deps);
  if (!store?.updateUser) return missingEntityStore(requestId);
  const data = await store.updateUser(user_id, patch);
  return data ? successEnvelope<UserData>(data, requestId) : errorEnvelope(404, "User not found", requestId);
}

/** @deprecated 改用 `/v3/meta/user/delete`。 */
async function handleUserDelete(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = userBatchDeleteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.deleteUsers) return missingEntityStore(requestId);
  return successEnvelope<BatchDeleteResult>(await store.deleteUsers(parsed.data.user_ids), requestId);
}

/** @deprecated 改用 `/v3/meta/agent/create`。 */
async function handleAgentCreate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = agentCreateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.createAgent) return missingEntityStore(requestId);
  return successEnvelope<AgentData>(await store.createAgent(parsed.data), requestId);
}

/** @deprecated 改用 `/v3/meta/agent/get`。 */
async function handleAgentGet(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = agentGetRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.getAgent) return missingEntityStore(requestId);
  const data = await store.getAgent(parsed.data.agent_id);
  if (!data) return errorEnvelope(404, "Agent not found", requestId);
  if (parsed.data.team_id && data.team_id !== parsed.data.team_id) return errorEnvelope(403, "Agent team_id mismatch", requestId);
  return successEnvelope<AgentData>(data, requestId);
}

/** @deprecated 改用 `/v3/meta/agent/update`。 */
async function handleAgentUpdate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = agentUpdateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { agent_id, team_id, ...patch } = parsed.data;
  const store = getEntityStore(deps);
  if (!store?.updateAgent) return missingEntityStore(requestId);
  const current = store.getAgent ? await store.getAgent(agent_id) : null;
  if (!current) return errorEnvelope(404, "Agent not found", requestId);
  if (team_id && current.team_id !== team_id) return errorEnvelope(403, "Agent team_id mismatch", requestId);
  const data = await store.updateAgent(agent_id, patch);
  return data ? successEnvelope<AgentData>(data, requestId) : errorEnvelope(404, "Agent not found", requestId);
}

/** @deprecated 改用 `/v3/meta/agent/delete`。 */
async function handleAgentDelete(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = agentBatchDeleteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.deleteAgents) return missingEntityStore(requestId);
  return successEnvelope<BatchDeleteResult>(await store.deleteAgents(parsed.data.agent_ids), requestId);
}

/** @deprecated 改用 `/v3/meta/task/create`。 */
async function handleTaskCreate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = taskCreateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.createTask) return missingEntityStore(requestId);
  return successEnvelope<TaskData>(await store.createTask(parsed.data), requestId);
}

/** @deprecated 改用 `/v3/meta/task/get`。 */
async function handleTaskGet(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = taskGetRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.getTask) return missingEntityStore(requestId);
  const data = await store.getTask(parsed.data.task_id);
  return data ? successEnvelope<TaskData>(data, requestId) : errorEnvelope(404, "Task not found", requestId);
}

/** @deprecated 改用 `/v3/meta/task/update`。 */
async function handleTaskUpdate(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = taskUpdateRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { task_id, ...patch } = parsed.data;
  const store = getEntityStore(deps);
  if (!store?.updateTask) return missingEntityStore(requestId);
  const data = await store.updateTask(task_id, patch);
  return data ? successEnvelope<TaskData>(data, requestId) : errorEnvelope(404, "Task not found", requestId);
}

/** @deprecated 改用 `/v3/meta/task/delete`。 */
async function handleTaskDelete(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = taskBatchDeleteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = getEntityStore(deps);
  if (!store?.deleteTasks) return missingEntityStore(requestId);
  return successEnvelope<BatchDeleteResult>(await store.deleteTasks(parsed.data.task_ids), requestId);
}

// ============================
// L2/L3 Profile Sync Helpers (write-through to VDB)
// ============================

type RequestIsolation = { teamId?: string; userId: string; agentId: string; sessionId: string; taskId?: string };

function buildIsolationScope(isolation?: RequestIsolation): string {
  return isolation ? buildProfileIsolationScope(isolation) : DEFAULT_PROFILE_SCOPE;
}

function buildIsolationStoragePrefix(isolation: RequestIsolation): string {
  return `profiles/${encodeURIComponent(buildIsolationScope(isolation))}/`;
}

function scopedProfileStorage(storage: StorageAdapter, isolation?: RequestIsolation): StorageAdapter {
  // Direct unit callers may not go through handleV2Route and therefore do not
  // have requestIsolation attached. Keep that legacy path at root; real HTTP
  // requests always resolve to either explicit ids or the `default` bucket.
  if (!isolation) return storage;
  // rowfs 后端（type === "rowfs"）在 scopeProfileStorageView 内改走隔离重绑定，
  // 不再套 profiles/{scope}/ 键前缀（D12 ③：键里不含 scope，前缀会让行视图全部 404）。
  return scopeProfileStorageView(storage, buildIsolationStoragePrefix(isolation), {
    teamId: isolation.teamId,
    userId: isolation.userId,
    agentId: isolation.agentId,
  });
}

function md5Hex(text: string): string {
  return createHash("md5").update(text).digest("hex");
}

function parseMetadataJson(raw: string | undefined): MemoryRecord["metadata"] {
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as MemoryRecord["metadata"] : {};
  } catch {
    return {};
  }
}

/**
 * Isolation filter for content-by-id reads. Mirrors the query handlers: it
 * narrows by team/user/agent/task but never by session, because a content id is
 * globally addressed and L1 reads are agent-level. Absent/out-of-scope rows
 * resolve to `null`, so a wrong identity sees a 404, not another scope's data.
 */
function buildContentIsolationFilter(
  iso: RequestIsolation | undefined,
): { teamId?: string; userId?: string; agentId?: string; taskId?: string } | undefined {
  if (!iso) return undefined;
  const filter: { teamId?: string; userId?: string; agentId?: string; taskId?: string } = {};
  if (iso.teamId) filter.teamId = iso.teamId;
  if (iso.userId) filter.userId = iso.userId;
  if (iso.agentId) filter.agentId = iso.agentId;
  if (iso.taskId) filter.taskId = iso.taskId;
  return Object.keys(filter).length > 0 ? filter : undefined;
}

// ============================
// History-mode guards (bypass-path checks)
// ============================
//
// Once content history is enabled the ONLY normal write entry is
// `POST /v3/content/write`: it carries an explicit `operation_id` and
// `expected_version_id`, and the CAS re-check happens inside the store's own
// transaction. Every legacy mutation endpoint that would otherwise perform a
// non-CAS modification (`atomic/update`, `atomic/delete`, `conversation/delete`,
// `scenario/write`, `scenario/rm`, `core/write`) is therefore refused here
// instead of silently bypassing the version chain. `conversation/add` remains
// an append: it never overwrites an existing locator, so it is routed through
// the history-recording path rather than refused.

/** Whether the bound SQLite store has content history enabled (L0/L1). */
function storeContentHistoryEnabled(store: IMemoryStore | undefined): boolean {
  // A capability read failure is NOT "history disabled": letting it fall through
  // to `false` would silently re-open the legacy (non-CAS) write/delete path.
  // The store's own `isContentHistoryEnabled()` is a pure boolean read, so any
  // throw is a real defect and must surface (fail closed) rather than unlock
  // legacy.
  if (!store || typeof store.isContentHistoryEnabled !== "function") return false;
  return store.isContentHistoryEnabled() === true;
}

/**
 * Whether the request-scoped L2/L3 profile root has content history enabled.
 *
 * A backend that simply cannot host a version chain (getContentHistory absent /
 * throws) is a capability absence → `false`. A FAILURE while reading the enable
 * marker is NOT "history disabled": it is surfaced so the guard fails CLOSED
 * instead of silently letting a legacy write bypass the version chain.
 */
async function profileContentHistoryEnabled(deps: V2RouterDeps): Promise<boolean> {
  const base = deps.getStorage();
  if (!base) return false;
  let history: ReturnType<StorageAdapter["getContentHistory"]>;
  try {
    const storage = scopedProfileStorage(base, deps.requestIsolation);
    history = storage.getContentHistory();
  } catch {
    // Scoped backend without a local publish root → cannot host L2/L3 history.
    return false;
  }
  if (!history) return false;
  return await history.isEnabled();
}

/** Refuse an L0/L1 legacy mutation that cannot carry a CAS baseline. */
function historyModeStoreWriteForbidden(store: IMemoryStore | undefined, requestId: string, operation: string): ApiResponseEnvelope | null {
  if (!storeContentHistoryEnabled(store)) return null;
  return errorEnvelope(
    409,
    `history_write_forbidden: content history is enabled, so ${operation} cannot modify content without an explicit ` +
      `operation_id + expected_version_id; use POST /v3/content/write`,
    requestId,
  );
}

/** Refuse a destructive delete in history mode: it is not defined, never retired silently. */
function historyModeDeleteForbidden(store: IMemoryStore | undefined, requestId: string, operation: string): ApiResponseEnvelope | null {
  if (!storeContentHistoryEnabled(store)) return null;
  return errorEnvelope(
    409,
    `history_delete_undefined: destructive delete (${operation}) is not defined while content history is enabled; ` +
      `use an explicit retire via POST /v3/content/write, or wait for the confirmed permanent-forget flow`,
    requestId,
  );
}

/** Refuse an L2/L3 legacy content mutation that cannot carry a CAS baseline. */
async function historyModeLocalWriteForbidden(deps: V2RouterDeps, requestId: string, operation: string): Promise<ApiResponseEnvelope | null> {
  if (!(await profileContentHistoryEnabled(deps))) return null;
  return errorEnvelope(
    409,
    `history_write_forbidden: content history is enabled, so ${operation} cannot modify content without an explicit ` +
      `operation_id + expected_version_id; use POST /v3/content/write`,
    requestId,
  );
}

/** Refuse an L2/L3 destructive delete in history mode. */
async function historyModeLocalDeleteForbidden(deps: V2RouterDeps, requestId: string, operation: string): Promise<ApiResponseEnvelope | null> {
  if (!(await profileContentHistoryEnabled(deps))) return null;
  return errorEnvelope(
    409,
    `history_delete_undefined: destructive delete (${operation}) is not defined while content history is enabled; ` +
      `use an explicit retire via POST /v3/content/write, or wait for the confirmed permanent-forget flow`,
    requestId,
  );
}

// ============================
// L0 append via the formal history commit (conversation/add)
// ============================

/** Canonical JSON (sorted keys, `undefined` dropped) for the append fingerprint. */
function canonicalJson(value: unknown): string {
  if (value === null || value === undefined || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.keys(value as Record<string, unknown>).sort();
  return `{${entries
    .map((key) => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`)
    .join(",")}}`;
}

/** The L0 object scope of an appended message (includes the session). */
function l0AppendScope(sessionId: string, iso: RequestIsolation | undefined): HistoryScope {
  const scope: HistoryScope = {
    team_id: iso?.teamId || DEFAULT_ISOLATION_ID,
    user_id: iso?.userId || DEFAULT_ISOLATION_ID,
    agent_id: iso?.agentId || DEFAULT_ISOLATION_ID,
    session_id: sessionId || DEFAULT_ISOLATION_ID,
  };
  if (iso?.taskId) scope.task_id = iso.taskId;
  return scope;
}

/** Epoch ms of a caller-supplied message time (number or ISO/date string), else null. */
function messageTimeMs(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value) {
    const ms = Date.parse(value);
    return Number.isFinite(ms) ? ms : null;
  }
  return null;
}

/**
 * The normalized request fingerprint of one appended message. It captures the
 * message semantics (id / session / role / body / times / task) so a repeat is
 * idempotent while a different role/session/time under the same id conflicts.
 */
function fingerprintL0Append(
  id: string,
  msg: { role?: unknown; content?: unknown; recorded_at?: unknown; timestamp?: unknown; source?: unknown },
  sessionId: string,
  iso: RequestIsolation | undefined,
): string {
  return createHash("sha256")
    .update(canonicalJson({
      layer: "l0",
      id,
      session_id: sessionId,
      role: typeof msg.role === "string" ? msg.role : null,
      source: msg.source === "framework_event" ? "framework_event" : "conversation",
      content: typeof msg.content === "string" ? msg.content : null,
      recorded_at: typeof msg.recorded_at === "string" ? msg.recorded_at : null,
      timestamp: msg.timestamp === undefined ? null : msg.timestamp,
      task_id: iso?.taskId ?? null,
    }))
    .digest("hex");
}

/**
 * Build the `create` L0 mutation for one appended message. No per-request
 * wall-clock is embedded: when the caller supplies no message time, the
 * program's commit clock stands in for `message_recorded_at` (and the version's
 * `recorded_at`), so the payload stays replay-stable.
 */
function buildL0AppendMutation(
  id: string,
  msg: { role?: unknown; content?: unknown; recorded_at?: unknown; timestamp?: unknown; source?: unknown },
  sessionId: string,
  iso: RequestIsolation | undefined,
): VersionMutation {
  const ms = messageTimeMs(msg.timestamp) ?? messageTimeMs(msg.recorded_at);
  const metadata = buildL0VersionMetadata(null, {
    role: typeof msg.role === "string" ? msg.role : "user",
    source: msg.source === "framework_event" ? "framework_event" : "conversation",
    sessionKey: sessionId,
    sessionId,
    taskId: iso?.taskId ?? "",
    messageRecordedAt: ms !== null ? new Date(ms).toISOString() : "",
    originalTimestampMs: ms ?? 0,
  });
  if (!metadata.message_recorded_at) delete metadata.message_recorded_at;
  return {
    draft: {
      layer: "l0",
      locator: id,
      scope: l0AppendScope(sessionId, iso),
      body: typeof msg.content === "string" ? msg.content : "",
      metadata,
      state: "active",
    },
    expected_version_id: null,
    predecessors: [],
    change_kind: "create",
  };
}

/** The (mirror/notify) L0 record of a freshly committed append. */
function l0AppendRecord(
  version: ContentVersion,
  msg: { role?: unknown },
): L0Record {
  const meta = (version.metadata ?? {}) as Record<string, unknown>;
  const recordedAt = typeof meta.message_recorded_at === "string" && meta.message_recorded_at
    ? meta.message_recorded_at
    : version.recorded_at;
  return {
    id: version.locator,
    sessionKey: typeof meta.session_key === "string" ? meta.session_key : version.scope.session_id ?? "",
    sessionId: version.scope.session_id ?? "",
    taskId: typeof meta.task_id === "string" ? meta.task_id : undefined,
    teamId: version.scope.team_id,
    userId: version.scope.user_id,
    agentId: version.scope.agent_id,
    role: typeof meta.role === "string" ? meta.role : (typeof msg.role === "string" ? msg.role : "user"),
    source: meta.source === "framework_event" ? "framework_event" : "conversation",
    messageText: version.body,
    recordedAt,
    timestamp: typeof meta.original_timestamp_ms === "number" ? meta.original_timestamp_ms : 0,
  };
}

/** Read one committed operation from the store's own formal chain (read-only). */
async function lookupL0CommittedOperation(
  store: IMemoryStore | undefined,
  scope: HistoryScope,
  operationId: string,
): Promise<CommittedOperationRecord | null> {
  const reader = store?.getContentHistory?.();
  const getter = (reader as unknown as
    | { getCommittedOperation?: (scope: HistoryScope, operationId: string) => CommittedOperationRecord | null }
    | undefined)?.getCommittedOperation;
  if (typeof getter !== "function") return null;
  return getter.call(reader, scope, operationId);
}

/** Stable error code → transport status for an L0/L1 history commit failure. */
function historyWriteStatus(code: unknown): number {
  switch (code) {
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
    case "history_retired_conflict":
    case "history_predecessor_not_current":
    case "predecessor_not_current":
    case "history_current_diverged":
    case "history_current_conflict":
    case "history_not_enabled":
    case "clock_regressed":
      return 409;
    case "history_layer_unsupported":
    case "history_bound_commit_unsupported":
    case "history_unavailable":
      return 501;
    case "history_invalid_input":
    case "invalid_mutation":
    case "invalid_locator":
      return 400;
    default:
      return 500;
  }
}

function parseSourceMessageIdsJson(raw: string | undefined, recordId?: string, logger?: Logger): string[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      logger?.warn?.(`${TAG} source_message_ids_json for ${recordId ?? "?"} is not an array (treated as unknown)`);
      return [];
    }
    return parsed.filter((v): v is string => typeof v === "string" && v.length > 0);
  } catch {
    logger?.warn?.(`${TAG} source_message_ids_json for ${recordId ?? "?"} is malformed (treated as unknown)`);
    return [];
  }
}

/** Build the L1 source view; an empty citation set is explicitly `unknown`. */
function buildAtomicSourceView(ids: string[]): AtomicSourceView {
  const valid = ids.filter((id) => typeof id === "string" && id.length > 0);
  if (valid.length === 0) return { status: "unknown", kind: "model_declared", message_ids: [] };
  return { status: "declared", kind: "model_declared", message_ids: valid };
}

// ============================
// Content-history view builders
// ============================
//
// These turn an exact stored `ContentVersion` into the shape the existing
// single-read endpoints already return. They are only used when the caller
// asked for an explicit `version_id` (or when history is enabled and a ref is
// attached); the legacy response shape is untouched otherwise.

/** An L0 message view built from its historical version (metadata is canonical). */
function conversationItemFromVersion(version: ContentVersion, fallback: ConversationItem): ConversationItem {
  const meta = (version.metadata ?? {}) as Record<string, unknown>;
  const role = typeof meta.role === "string" ? meta.role : fallback.role;
  const recordedAt = typeof meta.message_recorded_at === "string" ? meta.message_recorded_at : version.recorded_at;
  const item: ConversationItem = {
    id: version.locator,
    session_id: version.scope.session_id ?? fallback.session_id,
    team_id: version.scope.team_id ?? fallback.team_id,
    user_id: version.scope.user_id ?? fallback.user_id,
    agent_id: version.scope.agent_id ?? fallback.agent_id,
    task_id: version.scope.task_id ?? fallback.task_id,
    role: role as ConversationItem["role"],
    content: version.body,
    timestamp: recordedAt,
  };
  return item;
}

/** The model-declared source view of a stored L1 version. */
function atomicSourceFromVersion(version: ContentVersion): AtomicSourceView {
  const meta = (version.metadata ?? {}) as Record<string, unknown>;
  const ids = Array.isArray(meta.source_message_ids)
    ? meta.source_message_ids.filter((v): v is string => typeof v === "string")
    : [];
  return buildAtomicSourceView(ids);
}

/** An L1 item view built from a stored historical version. */
function atomicItemFromVersion(version: ContentVersion, current: boolean): AtomicGetData["item"] {
  const meta = (version.metadata ?? {}) as Record<string, unknown>;
  return {
    id: version.locator,
    type: typeof meta.type === "string" ? meta.type : "",
    content: version.body,
    background: typeof meta.scene_name === "string" && meta.scene_name ? meta.scene_name : undefined,
    version: 0,
    team_id: version.scope.team_id,
    user_id: version.scope.user_id,
    agent_id: version.scope.agent_id,
    task_id: version.scope.task_id,
    created_at: version.recorded_at,
    updated_at: version.recorded_at,
    current,
    ref: { ...version.ref },
    source: atomicSourceFromVersion(version),
  };
}

async function getProfileVersion(
  store: IMemoryStore | undefined,
  type: "l2" | "l3",
  filename: string,
  isolation?: RequestIsolation,
): Promise<number> {
  if (!store) return 0;
  const scope = buildIsolationScope(isolation);
  const id = buildProfileStableId(scope, type, filename);
  try {
    // 优先使用轻量按 id 查询，避免全量 pullProfiles()
    if (typeof store.queryProfilesByIds === "function") {
      const results = await store.queryProfilesByIds([id]);
      return results[0]?.version ?? 0;
    }
    if (typeof store.pullProfiles === "function") {
      const existing = (await store.pullProfiles()).find((r) => r.id === id);
      return existing?.version ?? 0;
    }
  } catch {
    // fall through
  }
  return 0;
}

/**
 * 批量获取多个 profile 的 version，一次查询。
 * 优先使用 queryProfilesByIds（轻量），fallback 到 pullProfiles（全量）。
 */
async function getProfileVersionBatch(
  store: IMemoryStore | undefined,
  type: "l2" | "l3",
  filenames: string[],
  isolation?: RequestIsolation,
): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  if (!store || filenames.length === 0) return result;
  const scope = buildIsolationScope(isolation);
  const ids = filenames.map((fn) => buildProfileStableId(scope, type, fn));
  try {
    if (typeof store.queryProfilesByIds === "function") {
      const records = await store.queryProfilesByIds(ids);
      const versionMap = new Map(records.map((r) => [r.id, r.version]));
      for (let i = 0; i < filenames.length; i++) {
        result.set(filenames[i], versionMap.get(ids[i]) ?? 0);
      }
      return result;
    }
    if (typeof store.pullProfiles === "function") {
      const all = await store.pullProfiles();
      const versionMap = new Map(all.map((r) => [r.id, r.version]));
      for (let i = 0; i < filenames.length; i++) {
        result.set(filenames[i], versionMap.get(ids[i]) ?? 0);
      }
      return result;
    }
  } catch {
    // fall through
  }
  return result;
}

/** Best-effort write-through L2/L3 profile to VDB. Failure is logged but does not break the API. */
async function syncProfileToVdb(
  store: IMemoryStore | undefined,
  type: "l2" | "l3",
  filename: string,
  content: string,
  logger: Logger,
  createdAtOverride?: number,
  isolation?: RequestIsolation,
): Promise<number> {
  if (!store || typeof store.syncProfiles !== "function") return 0;
  try {
    const now = Date.now();

    // Try to extract created time from META in content
    let createdAtMs = createdAtOverride ?? 0;
    if (!createdAtMs) {
      const metaMatch = content.match(/^-----META-START-----\n([\s\S]*?)\n-----META-END-----/);
      if (metaMatch) {
        for (const line of metaMatch[1].split("\n")) {
          if (line.startsWith("created: ")) {
            const ts = Date.parse(line.slice(9));
            if (!isNaN(ts)) createdAtMs = ts;
            break;
          }
        }
      }
    }
    if (!createdAtMs) createdAtMs = now;

    const scope = buildIsolationScope(isolation);
    const id = buildProfileStableId(scope, type, filename);

    // Probe current VDB version to satisfy the optimistic-lock check in
    // TcvdbMemoryStore.syncProfiles (which compares baselineVersion against
    // the remote version). Without this, the second and subsequent writes
    // to the same profile would be silently skipped as a version conflict.
    // Best-effort: if queryProfilesByIds/pullProfiles is unavailable or fails,
    // fall back to undefined and let syncProfiles decide (insert if remote missing,
    // otherwise log + skip — which preserves the previous behaviour).
    let baselineVersion: number | undefined;
    let currentMd5: string | undefined;
    try {
      if (typeof store.queryProfilesByIds === "function") {
        const results = await store.queryProfilesByIds([id]);
        const existing = results[0];
        if (existing) {
          baselineVersion = existing.version;
          currentMd5 = existing.contentMd5;
        }
      } else if (typeof store.pullProfiles === "function") {
        const remote = await store.pullProfiles();
        const existing = remote.find((r) => r.id === id);
        if (existing) {
          baselineVersion = existing.version;
          currentMd5 = existing.contentMd5;
        }
      }
    } catch (err) {
      logger.warn(`${TAG} [profile-sync] probe failed for ${filename}: ${err instanceof Error ? err.message : String(err)}`);
    }

    const contentMd5 = md5Hex(content);
    const nextVersion = currentMd5 === contentMd5
      ? (baselineVersion ?? 0)
      : (baselineVersion === undefined ? 0 : baselineVersion + 1);

    const record: ProfileSyncRecord = {
      id,
      type,
      filename,
      content,
      contentMd5,
      version: nextVersion,
      createdAtMs,
      updatedAtMs: now,
      baselineVersion,
      teamId: isolation?.teamId,
      userId: isolation?.userId,
      agentId: isolation?.agentId,
      // L2/L3 profiles are team+agent scoped; session_id/task_id are intentionally not written.
      sessionId: undefined,
    };
    await store.syncProfiles([record]);
    logger.debug?.(`${TAG} [profile-sync] ${type} upserted to VDB: ${filename} (baselineVersion=${baselineVersion ?? "new"}, version=${nextVersion})`);
    return nextVersion;
  } catch (err) {
    logger.warn(`${TAG} [profile-sync] FAILED to sync ${type} profile ${filename} to VDB: ${err instanceof Error ? err.message : String(err)}`);
    return 0;
  }
}

/** Best-effort delete L2 profiles from VDB. */
async function deleteProfilesFromVdb(
  store: IMemoryStore | undefined,
  type: "l2" | "l3",
  filenames: string[],
  logger: Logger,
  isolation?: RequestIsolation,
): Promise<void> {
  if (!store || typeof store.deleteProfiles !== "function" || filenames.length === 0) return;
  try {
    const scope = buildIsolationScope(isolation);
    const ids = filenames.map((fn) => buildProfileStableId(scope, type, fn));
    await store.deleteProfiles(ids);
    logger.debug?.(`${TAG} [profile-sync] ${type} deleted from VDB: ${filenames.length} files`);
  } catch (err) {
    logger.warn(`${TAG} [profile-sync] FAILED to delete ${type} profiles from VDB: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/** Best-effort refresh scene_index.json so pipeline sees the user-written L2 files. */
async function refreshSceneIndex(storage: StorageAdapter, logger: Logger): Promise<void> {
  try {
    const { syncSceneIndex } = await import("../core/scene/scene-index.js");
    // Pass empty dataDir; we only use storage in service mode.
    await syncSceneIndex("", storage);
  } catch (err) {
    logger.warn(`${TAG} [scene-index] FAILED to refresh scene index: ${err instanceof Error ? err.message : String(err)}`);
  }
}

// ============================
// L2 Scenario Handlers
// ============================

async function handleScenarioLs(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = scenarioListRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { path_prefix } = parsed.data;

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);

  const prefix = path_prefix
    ? `${StoragePaths.sceneBlocksDir}${path_prefix}`
    : StoragePaths.sceneBlocksDir;

  deps.logger.debug?.(`${TAG} [scenario/ls] storage.type=${storage.type}, prefix="${prefix}"`);

  // One-shot full listing (no pagination; marker-based pagination planned for phase 2)
  // A single pinned snapshot keeps the listing and the index on the same
  // revision, so a concurrent publish cannot produce a mixed view.
  const snap = await openPublishedSnapshotOf(storage.getBackend());
  const result = snap
    ? await snap.list(prefix, { recursive: true })
    : await storage.getBackend().listObjects(prefix, { recursive: true });
  deps.logger.debug?.(`${TAG} [scenario/ls] listObjects returned ${result.entries.length} entries`);
  const allEntries = result.entries;

  // Read scene_index.json for summary + created/updated lookup (same snapshot)
  let sceneIndex: Array<{ filename: string; summary?: string; created?: string; updated?: string }>;
  if (snap) {
    const indexObj = await snap.read(StoragePaths.sceneIndex);
    sceneIndex = indexObj ? parseSceneIndexEntries(indexObj.content.toString("utf-8")) : [];
  } else {
    const { readSceneIndex } = await import("../core/scene/scene-index.js");
    sceneIndex = await readSceneIndex("", storage);
  }
  const indexMap = new Map(sceneIndex.map((e) => [e.filename, e]));

  // 批量获取所有 L2 文件的 profile version（一次查询，避免 N+1）
  const l2Filenames = allEntries.filter((e) => !e.isDirectory).map((e) => {
    return e.key.startsWith(StoragePaths.sceneBlocksDir)
      ? e.key.slice(StoragePaths.sceneBlocksDir.length)
      : e.key;
  });
  const versionMap = await getProfileVersionBatch(deps.getStore(), "l2", l2Filenames, deps.requestIsolation);

  const entries: ScenarioEntry[] = allEntries.map((e) => {
    const externalPath = e.key.startsWith(StoragePaths.sceneBlocksDir)
      ? e.key.slice(StoragePaths.sceneBlocksDir.length)
      : e.key;
    const displayPath = e.isDirectory && !externalPath.endsWith("/") ? `${externalPath}/` : externalPath;
    const indexEntry = indexMap.get(externalPath);
    const fallbackTime = e.lastModified.toISOString();
    return {
      path: displayPath,
      summary: indexEntry?.summary || undefined,
      version: e.isDirectory ? 0 : (versionMap.get(externalPath) ?? 0),
      team_id: deps.requestIsolation?.teamId,
      agent_id: deps.requestIsolation?.agentId,
      created_at: indexEntry?.created || fallbackTime,
      updated_at: indexEntry?.updated || fallbackTime,
    };
  });

  return successEnvelope({ entries, total: entries.length }, requestId);
}

async function handleScenarioCount(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = scenarioCountRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { path_prefix } = parsed.data;

  const store = deps.getStore();
  if (typeof store?.countProfiles === "function") {
    const total = await store.countProfiles({
      type: "l2",
      pathPrefix: path_prefix,
      teamId: deps.requestIsolation?.teamId,
      userId: deps.requestIsolation?.teamId ? undefined : deps.requestIsolation?.userId,
      agentId: deps.requestIsolation?.agentId,
    });
    return successEnvelope<CountData>({ total }, requestId);
  }

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);
  const prefix = path_prefix
    ? `${StoragePaths.sceneBlocksDir}${path_prefix}`
    : StoragePaths.sceneBlocksDir;
  const result = await storage.getBackend().listObjects(prefix, { recursive: true });
  const total = result.entries.filter((e) => !e.isDirectory).length;
  return successEnvelope<CountData>({ total }, requestId);
}

async function handleScenarioRead(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = scenarioReadRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { path } = parsed.data;

  // Historical L2 reads are not implemented this round. A supplied numeric
  // `version` is rejected explicitly so callers cannot mistake an ignored field
  // for a reliable versioned read. The string `version_id` is the history-aware
  // alternative and is only meaningful once content history is enabled.
  if (parsed.data.version !== undefined) {
    return errorEnvelope(400, "unsupported_version: historical L2 reads are not implemented; omit version", requestId);
  }

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);

  const key = `${StoragePaths.sceneBlocksDir}${path}`;

  // Content-history reference (and, when `version_id` is set, the exact
  // historical body). Legacy mode returns no ref and never silently reads the
  // current bytes for a requested version.
  const history = await resolveLocalLocatorHistory(deps, "l2", key, parsed.data.version_id);
  if (!history.ok) return errorEnvelope(history.failure.status, history.failure.message, requestId);
  if (history.version) {
    // The ref, body and sources all come from ONE pinned version, so a
    // concurrent commit cannot pair an old ref with a newer body. The numeric
    // legacy version only describes the live profile row; for an explicit
    // historical read it is omitted rather than passing the current number off
    // as the old version's number.
    const explicit = parsed.data.version_id !== undefined;
    if (!explicit && history.version.state === "retired") {
      return successEnvelope({ path, content: null, state: "retired", ref: history.ref, sources: unknownContentSource() }, requestId);
    }
    const historical: ScenarioFile = {
      path,
      content: history.version.body,
      team_id: history.version.scope.team_id ?? deps.requestIsolation?.teamId,
      agent_id: history.version.scope.agent_id ?? deps.requestIsolation?.agentId,
      created_at: history.version.recorded_at,
      updated_at: history.version.recorded_at,
      // The version's OWN recorded sources: never the current sidecar (which
      // describes other bytes), and never erased to `unknown` when the version
      // already recorded real input sources.
      sources: contentSourceViewFromVersion(history.version),
    };
    if (history.ref) historical.ref = history.ref;
    return successEnvelope({ ...historical, state: history.version.state, content_version: contentVersionView(history.version) }, requestId);
  }
  // Pin one revision so content + index + stat all come from the same bundle.
  const snap = await openPublishedSnapshotOf(storage.getBackend());
  const sourceStorage = snap ? { readFile: async (key: string) => (await snap.read(key))?.content.toString("utf-8") ?? null } as StorageAdapter : storage;
  const contentObj = snap ? await snap.read(key) : null;
  const content = snap
    ? (contentObj ? contentObj.content.toString("utf-8") : null)
    : await storage.readFile(key);

  // File not found → return 200 with null content (not 404)
  if (content === null) {
    return successEnvelope<ScenarioFile>({
      path,
      content: null as unknown as string,
      created_at: null as unknown as string,
      updated_at: null as unknown as string,
      sources: unknownContentSource(),
    }, requestId);
  }

  // Parse META for created/updated
  const now = new Date().toISOString();
  let createdAt = now;
  let updatedAt = now;

  const metaMatch = content.match(/^-----META-START-----\n([\s\S]*?)\n-----META-END-----/);
  if (metaMatch) {
    for (const line of metaMatch[1].split("\n")) {
      const idx = line.indexOf(": ");
      if (idx > 0) {
        const k = line.slice(0, idx);
        const v = line.slice(idx + 2);
        if (k === "created") createdAt = v;
        if (k === "updated") updatedAt = v;
      }
    }
  } else {
    // Fallback: try scene_index (same pinned snapshot)
    let entry: { created?: string; updated?: string } | undefined;
    if (snap) {
      const indexObj = await snap.read(StoragePaths.sceneIndex);
      entry = indexObj
        ? parseSceneIndexEntries(indexObj.content.toString("utf-8")).find((e) => e.filename === path)
        : undefined;
    } else {
      const { readSceneIndex } = await import("../core/scene/scene-index.js");
      const sceneIndex = await readSceneIndex("", storage);
      entry = sceneIndex.find((e) => e.filename === path);
    }
    if (entry) {
      createdAt = entry.created || now;
      updatedAt = entry.updated || now;
    } else {
      const lastModified = snap ? contentObj?.lastModified : (await storage.stat(key))?.lastModified;
      if (lastModified) {
        createdAt = new Date(lastModified).toISOString();
        updatedAt = createdAt;
      }
    }
  }

  const file: ScenarioFile = {
    path, content,
    version: await getProfileVersion(deps.getStore(), "l2", path, deps.requestIsolation),
    team_id: deps.requestIsolation?.teamId,
    agent_id: deps.requestIsolation?.agentId,
    created_at: createdAt,
    updated_at: updatedAt,
    // Program-provided input scope for this file; hash-bound to the exact bytes
    // just read, so a stale sidecar can never be attached to changed content.
    // `unknown` when none was persisted (never an empty list implying "no
    // evidence"); `unavailable` when the sidecar itself could not be read.
    sources: await readContentSourceViewForContent(sourceStorage, "l2", path, content),
  };
  if (history.ref) file.ref = history.ref;
  return successEnvelope<ScenarioFile>(file, requestId);
}

async function handleScenarioWrite(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = scenarioWriteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { path, content, summary } = parsed.data;

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);

  // History mode: an L2 modification without a CAS baseline is refused; use content/write.
  const historyForbidden = await historyModeLocalWriteForbidden(deps, requestId, "scenario/write");
  if (historyForbidden) return historyForbidden;

  const key = `${StoragePaths.sceneBlocksDir}${path}`;

  // Existence check: path must already exist (no upsert/create)
  const existing = await storage.readFile(key);
  if (existing === null) return errorEnvelope(404, `Scenario file not found: ${path}`, requestId);

  // Parse existing META to preserve created + update updated/summary
  const now = new Date().toISOString();
  let finalContent: string;

  const metaMatch = existing.match(/^-----META-START-----\n([\s\S]*?)\n-----META-END-----\n?/);
  if (metaMatch) {
    // Parse existing META fields
    const metaBlock = metaMatch[1];
    const metaFields: Record<string, string> = {};
    for (const line of metaBlock.split("\n")) {
      const idx = line.indexOf(": ");
      if (idx > 0) metaFields[line.slice(0, idx)] = line.slice(idx + 2);
    }

    // Update fields
    metaFields["updated"] = now;
    if (summary !== undefined) metaFields["summary"] = summary;

    const newMeta = Object.entries(metaFields).map(([k, v]) => `${k}: ${v}`).join("\n");
    finalContent = `-----META-START-----\n${newMeta}\n-----META-END-----\n\n${content}`;
  } else {
    // META missing or corrupted — rebuild
    const metaLines = [
      `created: ${now}`,
      `updated: ${now}`,
    ];
    if (summary !== undefined) metaLines.push(`summary: ${summary}`);
    finalContent = `-----META-START-----\n${metaLines.join("\n")}\n-----META-END-----\n\n${content}`;
  }

  await storage.writeFile(key, finalContent);

  // Direct API write changes the bytes under the sidecar: drop the old
  // pipeline relation so it can never be served for the new content. A removal
  // failure is surfaced (not swallowed) — a success response must not leave a
  // stale source relation behind.
  await removeContentSources(storage, [{ layer: "l2", path }]);

  // Sync L2 to VDB profiles + refresh scene index (best-effort)
  const store = deps.getStore();
  const version = await syncProfileToVdb(store, "l2", path, finalContent, deps.logger, undefined, deps.requestIsolation);
  await refreshSceneIndex(storage, deps.logger);

  // 审计：L2 update — record_id 用 path（L2 主键 = 文件路径）
  await recordAudit(store, {
    record_id: path,
    layer: "L2",
    action: "update",
    iso: deps.requestIsolation,
    version,
    requestId,
    logger: deps.logger,
  });

  return successEnvelope<ScenarioWriteData>({
    path,
    updated_at: now,
    version,
    team_id: deps.requestIsolation?.teamId,
    agent_id: deps.requestIsolation?.agentId,
  }, requestId);
}

async function handleScenarioRm(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = scenarioRmRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { path } = parsed.data;

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);

  // History mode refuses a destructive L2 delete; retire via content/write instead.
  const historyForbidden = await historyModeLocalDeleteForbidden(deps, requestId, "scenario/rm");
  if (historyForbidden) return historyForbidden;

  const key = `${StoragePaths.sceneBlocksDir}${path}`;
  // Collect filenames to delete from VDB (single file or all files under a directory)
  let removedFilenames: string[] = [];
  if (path.endsWith("/")) {
    try {
      const names = await storage.readdirNames(key, ".md");
      removedFilenames = names.map((name) => `${path}${name}`);
    } catch { /* ignore */ }
    await storage.rmdir(key);
  } else {
    removedFilenames = [path];
    await storage.unlink(key);
  }

  // Delete L2 profiles from VDB (best-effort)
  const store = deps.getStore();
  await deleteProfilesFromVdb(store, "l2", removedFilenames, deps.logger, deps.requestIsolation);
  await refreshSceneIndex(storage, deps.logger);

  // 审计：L2 delete — 每个被删的 path 一行
  for (const fname of removedFilenames) {
    await recordAudit(store, {
      record_id: fname,
      layer: "L2",
      action: "delete",
      iso: deps.requestIsolation,
      version: 0,
      requestId,
      logger: deps.logger,
    });
  }

  return successEnvelope(undefined, requestId);
}

// ============================
// L3 Core Handlers
// ============================

async function handleCoreRead(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = coreReadRequestSchema.safeParse(body ?? {});
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  // Historical L3 reads are not implemented this round; reject an explicit
  // numeric version instead of silently ignoring it. `version_id` is the
  // history-aware alternative (see the L2 handler).
  if (parsed.data.version !== undefined) {
    return errorEnvelope(400, "unsupported_version: historical L3 reads are not implemented; omit version", requestId);
  }

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);

  // Content-history reference (and the exact historical persona when a
  // `version_id` was requested). Legacy mode returns no ref.
  const history = await resolveLocalLocatorHistory(deps, "l3", StoragePaths.persona, parsed.data.version_id);
  if (!history.ok) return errorEnvelope(history.failure.status, history.failure.message, requestId);
  if (history.version) {
    // Same single-snapshot rule as scenario/read: ref + body + sources all come
    // from the one pinned version. The numeric legacy version is only kept for
    // the current read; an explicit historical read must not report the live
    // profile number as the old version's number.
    const explicit = parsed.data.version_id !== undefined;
    if (!explicit && history.version.state === "retired") {
      return successEnvelope({ content: null, state: "retired", ref: history.ref, sources: unknownContentSource() }, requestId);
    }
    const historical: CoreFile = {
      content: history.version.body,
      team_id: history.version.scope.team_id ?? deps.requestIsolation?.teamId,
      agent_id: history.version.scope.agent_id ?? deps.requestIsolation?.agentId,
      created_at: history.version.recorded_at,
      updated_at: history.version.recorded_at,
      sources: contentSourceViewFromVersion(history.version),
    };
    if (history.ref) historical.ref = history.ref;
    return successEnvelope({ ...historical, state: history.version.state, content_version: contentVersionView(history.version) }, requestId);
  }

  deps.logger.debug?.(`${TAG} [core/read] storage.type=${storage.type}, key="${StoragePaths.persona}"`);

  // Pin one revision: content, sources and stat must come from the same bundle.
  const snap = await openPublishedSnapshotOf(storage.getBackend());
  const sourceStorage = snap ? { readFile: async (key: string) => (await snap.read(key))?.content.toString("utf-8") ?? null } as StorageAdapter : storage;
  let content: string | null;
  let lastModifiedMs: number | undefined;
  if (snap) {
    const obj = await snap.read(StoragePaths.persona);
    content = obj ? obj.content.toString("utf-8") : null;
    lastModifiedMs = obj?.lastModified?.getTime();
  } else {
    content = await storage.readFile(StoragePaths.persona);
    if (content !== null) {
      lastModifiedMs = (await storage.stat(StoragePaths.persona))?.lastModified;
    }
  }
  deps.logger.debug?.(`${TAG} [core/read] readFile result: ${content === null ? "null (not found)" : `${content.length} chars`}`);

  // File not found → return 200 with null content (not 404)
  if (content === null) {
    return successEnvelope<CoreFile>({
      content: null as unknown as string,
      created_at: null as unknown as string,
      updated_at: null as unknown as string,
      sources: unknownContentSource(),
    }, requestId);
  }

  const now = new Date().toISOString();

  const file: CoreFile = {
    content,
    version: await getProfileVersion(deps.getStore(), "l3", StoragePaths.persona, deps.requestIsolation),
    team_id: deps.requestIsolation?.teamId,
    agent_id: deps.requestIsolation?.agentId,
    created_at: lastModifiedMs !== undefined ? new Date(lastModifiedMs).toISOString() : now,
    updated_at: lastModifiedMs !== undefined ? new Date(lastModifiedMs).toISOString() : now,
    sources: await readContentSourceViewForContent(sourceStorage, "l3", StoragePaths.persona, content),
  };
  if (history.ref) file.ref = history.ref;
  return successEnvelope<CoreFile>(file, requestId);
}

async function handleCoreCount(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = coreCountRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const store = deps.getStore();
  if (typeof store?.countProfiles === "function") {
    const total = await store.countProfiles({
      type: "l3",
      teamId: deps.requestIsolation?.teamId,
      userId: deps.requestIsolation?.teamId ? undefined : deps.requestIsolation?.userId,
      agentId: deps.requestIsolation?.agentId,
    });
    return successEnvelope<CountData>({ total }, requestId);
  }

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);
  const content = await storage.readFile(StoragePaths.persona);
  return successEnvelope<CountData>({ total: content ? 1 : 0 }, requestId);
}

async function handleCoreWrite(body: unknown, _auth: V2AuthContext, requestId: string, deps: V2RouterDeps): Promise<ApiResponseEnvelope> {
  const parsed = coreWriteRequestSchema.safeParse(body);
  if (!parsed.success) return errorEnvelope(400, formatZodError(parsed.error), requestId);
  const { content } = parsed.data;

  const baseStorage = deps.getStorage();
  if (!baseStorage) return errorEnvelope(503, "Storage not available", requestId);
  const storage = scopedProfileStorage(baseStorage, deps.requestIsolation);

  // History mode: an L3 modification without a CAS baseline is refused; use content/write.
  const historyForbidden = await historyModeLocalWriteForbidden(deps, requestId, "core/write");
  if (historyForbidden) return historyForbidden;

  // Normalize before persistence: persona body must NOT contain Scene Navigation
  // (a derived section rebuilt from scene_index.json) or stray surrounding
  // whitespace. Both COS and VDB get the *exact* same bytes so md5(content) is
  // a stable identity across stores. Without this, /v2/core/write callers that
  // post the raw round-tripped body (which includes the navigation footer and
  // a trailing newline appended by refreshPersonaNavigation) would write a
  // mismatched copy to each store, and pullProfilesToLocal would later treat
  // the persona as corrupted and delete the COS copy.
  const personaBody = stripSceneNavigation(content).trim();

  await storage.writeFile(StoragePaths.persona, personaBody);

  // Direct API write invalidates the old pipeline relation (see scenario/write).
  await removeContentSources(storage, [{ layer: "l3", path: StoragePaths.persona }]);

  // Sync L3 persona to VDB profiles (best-effort)
  const store = deps.getStore();
  const version = await syncProfileToVdb(store, "l3", StoragePaths.persona, personaBody, deps.logger, undefined, deps.requestIsolation);

  // 审计：L3 update — record_id 用 persona 的 storage path
  await recordAudit(store, {
    record_id: StoragePaths.persona,
    layer: "L3",
    action: "update",
    iso: deps.requestIsolation,
    version,
    requestId,
    logger: deps.logger,
  });

  return successEnvelope<CoreWriteData>({
    updated_at: new Date().toISOString(),
    version,
    team_id: deps.requestIsolation?.teamId,
    agent_id: deps.requestIsolation?.agentId,
  }, requestId);
}

// ─────────────────────────────────────────────────────────────────────────
// /v2/pipeline/status — standalone-only introspection.
// Returns per-L-type queue/in-flight stats by reading the in-memory task
// queue (LocalStateBackend.listQueuedTasks) and worker's running set
// (PipelineWorker.getRunningTasks). idle = queued===0 && running===0.
// Mirrors the old MemoryPipelineManager.getQueueSizes() {l1Idle,l2Idle,l3Idle}
// semantics so seed clients can wait specifically for L1 to drain (without
// being blocked by slow L2/L3 cascades).
// Service mode returns 404 (route not exposed).
// ─────────────────────────────────────────────────────────────────────────

interface LayerStatus {
  /** Tasks waiting to be consumed (in queue). */
  queued: number;
  /** Tasks consumed by worker but not yet completed/failed. */
  running: number;
  /** Distinct sessionIds of queued tasks (for diagnostics). */
  queued_sessions: string[];
  /** Distinct sessionIds of running tasks (for diagnostics). */
  running_sessions: string[];
  /** True iff queued===0 && running===0. */
  idle: boolean;
}

interface PipelineStatusData {
  l1: LayerStatus;
  l2: LayerStatus;
  l3: LayerStatus;
}

function emptyLayer(): LayerStatus {
  return { queued: 0, running: 0, queued_sessions: [], running_sessions: [], idle: true };
}

async function handlePipelineStatus(
  _body: unknown,
  _auth: V2AuthContext,
  requestId: string,
  deps: V2RouterDeps,
): Promise<ApiResponseEnvelope> {
  // Service mode does not expose this endpoint — pretend it's not routed.
  if (deps.deployMode !== "standalone") {
    return errorEnvelope(404, "Not found", requestId);
  }

  // Legacy standalone (no stateBackend / no worker) — pipeline isn't running.
  if (!deps.stateBackend || !deps.pipelineWorker) {
    return errorEnvelope(503, "Pipeline not running (legacy standalone mode)", requestId);
  }

  // listQueuedTasks is optional on IStateBackend; LocalStateBackend implements
  // it, remote backends may not. Service mode never reaches here anyway.
  if (!deps.stateBackend.listQueuedTasks) {
    return errorEnvelope(
      503,
      "stateBackend does not support listQueuedTasks (status endpoint requires LocalStateBackend)",
      requestId,
    );
  }

  const queued = await deps.stateBackend.listQueuedTasks();
  const running = deps.pipelineWorker.getRunningTasks();

  const layers: Record<"L1" | "L2" | "L3", LayerStatus> = {
    L1: emptyLayer(),
    L2: emptyLayer(),
    L3: emptyLayer(),
  };
  // Track sessionIds in a Set per layer/category for de-dup, then materialize.
  const queuedSessionSets: Record<"L1" | "L2" | "L3", Set<string>> = {
    L1: new Set(),
    L2: new Set(),
    L3: new Set(),
  };
  const runningSessionSets: Record<"L1" | "L2" | "L3", Set<string>> = {
    L1: new Set(),
    L2: new Set(),
    L3: new Set(),
  };

  for (const t of queued) {
    if (t.type === "L1" || t.type === "L2" || t.type === "L3") {
      layers[t.type].queued++;
      queuedSessionSets[t.type].add(t.sessionId);
    }
    // "flush" tasks behave like L1 work (see executor.executeFlush fallback);
    // tally them under L1 so the seed-v2 idle wait doesn't miss them.
    if (t.type === "flush") {
      layers.L1.queued++;
      queuedSessionSets.L1.add(t.sessionId);
    }
  }
  for (const t of running) {
    if (t.type === "L1" || t.type === "L2" || t.type === "L3") {
      layers[t.type].running++;
      runningSessionSets[t.type].add(t.sessionId);
    }
    if (t.type === "flush") {
      layers.L1.running++;
      runningSessionSets.L1.add(t.sessionId);
    }
  }
  for (const k of ["L1", "L2", "L3"] as const) {
    layers[k].queued_sessions = Array.from(queuedSessionSets[k]).sort();
    layers[k].running_sessions = Array.from(runningSessionSets[k]).sort();
    layers[k].idle = layers[k].queued === 0 && layers[k].running === 0;
  }

  const data: PipelineStatusData = {
    l1: layers.L1,
    l2: layers.L2,
    l3: layers.L3,
  };

  return successEnvelope<PipelineStatusData>(data, requestId);
}

// ============================
// Helpers
// ============================

/**
 * Format a Date as YYYY-MM-DD in local timezone, matching the convention used by
 * v1 l0-recorder and l1-writer for daily JSONL shard names. Local copy to keep
 * v2-router self-contained (avoids exporting a util just for one call site).
 */
function formatLocalDateForJsonl(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/** Parse a scene_index.json payload to filename-scoped entries (lenient). */
function parseSceneIndexEntries(
  raw: string,
): Array<{ filename: string; summary?: string; created?: string; updated?: string }> {
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (entry): entry is { filename: string; summary?: string; created?: string; updated?: string } =>
        !!entry && typeof entry === "object" && typeof entry.filename === "string",
    );
  } catch {
    return [];
  }
}

// ============================
// Exported for testing
// ============================

export {
  handleConversationAdd,
  handleConversationQuery,
  handleConversationGet,
  handleConversationSearch,
  handleConversationDelete,
  handleConversationCount,
  handleAtomicUpdate,
  handleAtomicQuery,
  handleAtomicGet,
  handleAtomicSearch,
  handleAtomicDelete,
  handleAtomicCount,
  handleScenarioLs,
  handleScenarioRead,
  handleScenarioWrite,
  handleScenarioRm,
  handleScenarioCount,
  handleCoreRead,
  handleCoreWrite,
  handleCoreCount,
  handleContentRead,
  handleContentHistory,
  handleContentChanges,
  handleContentWrite,
  handleTeamCreate,
  handleTeamGet,
  handleTeamUpdate,
  handleTeamDelete,
  handleUserCreate,
  handleUserGet,
  handleUserUpdate,
  handleUserDelete,
  handleAgentCreate,
  handleAgentGet,
  handleAgentUpdate,
  handleAgentDelete,
  handleTaskCreate,
  handleTaskGet,
  handleTaskUpdate,
  handleTaskDelete,
  handlePipelineStatus,
};
