/**
 * Core content-history read handlers (§4.8.6).
 *
 * Provides the normal (read-only) content interfaces that let a caller read
 * the current content, a specific historical version, or the time-line of
 * changes without scanning internal files, parsing diagnostics or building
 * internal record ids themselves:
 *
 *   POST content/read      — one version (current / exact `version_id` / `as_of`)
 *   POST content/history   — the version list of one content identity
 *   POST content/changes   — browse recorded-time changes of a layer
 *
 * 2026-10-12 收窄：content/diff 与 content/relations 已撤掉——两版差异、
 * 关系与影响范围交给读取方的模型现场推导（docs/memory-design-proposal.md
 * §4.8）。读取结果也不再预建后继/延续图，只给版本本身与身份当前指针。
 *
 * Security model
 * --------------
 * Authorization is derived from the *trusted request isolation* only
 * (team/user/agent + optional task); nothing in the body can widen it. A
 * request never supplies a scope. The store reader resolves an identity's
 * exact stored scope behind an authorization filter (so L0's session dimension
 * is never guessed by the caller), and the local reader is handed the profile
 * object scope derived from the same trusted request.
 *
 * Failure semantics (all explicit, none silent)
 * ---------------------------------------------
 *   - 501 `history_unavailable`  — the store/backend cannot host a version chain
 *   - 409 `history_not_enabled`  — capability exists but history is turned off
 *   - 404 `not_found`            — absent OR outside the caller's scope
 *   - 404 `version_not_found`    — the identity exists but that version does not;
 *                                  it NEVER falls back to the current body
 *   - 400 `invalid_cursor`       — the pagination cursor is malformed or does
 *                                  not match the trusted request
 *   - 500 with the reader's own reason for a corrupt/unreadable version
 */

import type {
  ContentVersion,
  HistoryLayer,
  HistoryPage,
  HistoryReader,
  HistoryScope,
  VersionRef,
} from "../core/content/history-contract.js";
import {
  buildContentHistoryAuthFilter,
  decodeHistoryCursor,
  encodeHistoryCursor,
  HistoryCursorError,
  l0RowHistoryScope,
  l1RowHistoryScope,
  profileHistoryScope,
  readVersionAsync,
  scopeKeyOf,
  type ContentHistoryAuthFilter,
  type ScopedContentHistoryReader,
} from "../core/content/history-reader.js";
import type { IMemoryStore } from "../core/store/types.js";
import { scopeProfileStorageView, type StorageAdapter } from "../core/storage/adapter.js";
import { getLocalPublishRootOf } from "../core/storage/local-published-storage.js";
import { buildProfileIsolationScope } from "../core/profile/profile-scope.js";
import type { LocalContentHistory } from "../core/storage/local-content-history.js";
import type { Logger } from "../core/types.js";
import {
  contentChangesRequestSchema,
  contentHistoryRequestSchema,
  contentReadRequestSchema,
  formatZodError,
  type ApiResponseEnvelope,
  type ContentChangesData,
  type ContentChangesLayerView,
  type ContentHistoryData,
  type ContentReadData,
  type ContentSourceView,
  type ContentVersionStateView,
  type ContentVersionView,
  type V2AuthContext,
} from "./v2-schemas.js";

// ============================
// Dependencies (a structural subset of V2RouterDeps)
// ============================

export interface ContentHistoryDeps {
  getStore: () => IMemoryStore | undefined;
  getStorage: () => StorageAdapter | undefined;
  logger?: Logger;
  requestIsolation?: { teamId?: string; userId: string; agentId: string; sessionId: string; taskId?: string };
  requestIsolationMissing?: string[];
}

export interface HandlerFailure {
  status: number;
  message: string;
}

type FailResult = { ok: false; failure: HandlerFailure };

// ============================
// Envelope helpers (kept local to avoid a router import cycle)
// ============================

function ok<T>(data: T, requestId: string): ApiResponseEnvelope {
  return { code: 0, message: "ok", request_id: requestId, data };
}

function fail(status: number, message: string, requestId: string): ApiResponseEnvelope {
  return { code: status, message, request_id: requestId };
}

function failResult(status: number, message: string): FailResult {
  return { ok: false, failure: { status, message } };
}

// ============================
// Reader resolution
// ============================

export function resolveStoreHistorySoft(deps: ContentHistoryDeps): ScopedContentHistoryReader | null {
  const store = deps.getStore();
  if (!store || typeof store.getContentHistory !== "function") return null;
  if (typeof store.isContentHistoryEnabled === "function" && !store.isContentHistoryEnabled()) return null;
  return store.getContentHistory() ?? null;
}

function resolveStoreHistory(deps: ContentHistoryDeps): { ok: true; reader: ScopedContentHistoryReader } | FailResult {
  const store = deps.getStore();
  if (!store) return failResult(503, "Store not available");
  if (typeof store.getContentHistory !== "function") {
    return failResult(501, "history_unavailable: this store backend does not support content history");
  }
  if (typeof store.isContentHistoryEnabled === "function" && !store.isContentHistoryEnabled()) {
    return failResult(409, "history_not_enabled: content history is not enabled for this store");
  }
  const reader = store.getContentHistory();
  if (!reader) return failResult(409, "history_not_enabled: content history is not enabled for this store");
  return { ok: true, reader };
}

function resolveProfileStorage(deps: ContentHistoryDeps): StorageAdapter | undefined {
  const storage = deps.getStorage();
  const iso = deps.requestIsolation;
  if (!storage || !iso) return storage;
  return scopeProfileStorageView(storage, `profiles/${encodeURIComponent(buildProfileIsolationScope(iso))}/`, {
    teamId: iso.teamId, userId: iso.userId, agentId: iso.agentId,
  });
}

export async function resolveLocalHistorySoft(
  deps: ContentHistoryDeps,
): Promise<{ reader: HistoryReader; scope: HistoryScope } | null> {
  const storage = resolveProfileStorage(deps);
  if (!storage) return null;
  // Capability probe only: a backend that cannot host a local publish root
  // (COS / rowfs / mongofs) simply has no history. A capability-present failure
  // (e.g. an unreadable pointer) is NOT swallowed here — it propagates so the
  // caller surfaces a real error instead of mis-labelling it `unsupported`.
  if (!hasLocalPublishRoot(storage)) return null;
  const history: LocalContentHistory | null = storage.getContentHistory() ?? null;
  if (!history) return null;
  if (!(await history.isEnabled())) return null;
  return { reader: history, scope: profileHistoryScope(deps.requestIsolation) };
}

/** Whether this storage view can host a local publish root (capability only). */
function hasLocalPublishRoot(storage: StorageAdapter): boolean {
  try {
    return getLocalPublishRootOf(storage.getBackend()) != null;
  } catch {
    return false;
  }
}

async function resolveLocalHistory(
  deps: ContentHistoryDeps,
): Promise<{ ok: true; reader: HistoryReader; scope: HistoryScope } | FailResult> {
  const storage = resolveProfileStorage(deps);
  if (!storage) return failResult(503, "Storage not available");
  // Capability absence (no local publish root) is the ONLY case mapped to 501.
  // Construction/read failures are left to the caller's catch → 500 with the
  // reader's own reason, never a misleading `history_unavailable`.
  if (!hasLocalPublishRoot(storage)) {
    return failResult(501, "history_unavailable: this storage backend does not support content history");
  }
  const history: LocalContentHistory | null = storage.getContentHistory() ?? null;
  if (!history) {
    return failResult(501, "history_unavailable: this storage backend does not support content history");
  }
  const enabled = await history.isEnabled();
  if (!enabled) return failResult(409, "history_not_enabled: content history is not enabled for this storage root");
  return { ok: true, reader: history, scope: profileHistoryScope(deps.requestIsolation) };
}

// ============================
// Error mapping
// ============================

export function failureFromError(err: unknown): HandlerFailure {
  if (err instanceof HistoryCursorError) return { status: 400, message: err.message };
  const message = err instanceof Error ? err.message : String(err);
  const code = (err as { code?: unknown } | null)?.code;
  switch (code) {
    case "history_invalid_input":
    case "invalid_read":
    case "invalid_page":
    case "invalid_scope":
    case "invalid_mutation":
    case "invalid_locator":
    case "unknown_content_id":
      return { status: 400, message };
    case "history_not_enabled":
    case "history_context_required":
    case "history_as_of_ambiguous":
    case "clock_regressed":
      return { status: 409, message };
    case "history_scope_query_unsupported":
    case "history_bound_commit_unsupported":
      return { status: 501, message };
    default:
      return { status: 500, message };
  }
}

function failureEnvelope(err: unknown, requestId: string): ApiResponseEnvelope {
  const failure = failureFromError(err);
  return fail(failure.status, failure.message, requestId);
}

// ============================
// View mapping
// ============================

function toView(version: ContentVersion): ContentVersionView {
  return {
    layer: version.layer,
    content_id: version.content_id,
    locator: version.locator,
    scope: { ...version.scope } as unknown as Record<string, string>,
    body: version.body,
    metadata: version.metadata,
    state: version.state,
    ref: { ...version.ref },
    recorded_at: version.recorded_at,
    sequence: version.sequence,
    predecessors: version.predecessors,
    change_kind: version.change_kind,
    history_complete: version.history_complete,
  };
}

/** Convert an internal `ContentVersion` into its JSON-safe API view. */
export function contentVersionView(version: ContentVersion): ContentVersionView {
  return toView(version);
}

// ============================
// content/read
// ============================

export async function handleContentRead(
  body: unknown,
  _auth: V2AuthContext,
  requestId: string,
  deps: ContentHistoryDeps,
): Promise<ApiResponseEnvelope> {
  const parsed = contentReadRequestSchema.safeParse(body);
  if (!parsed.success) return fail(400, formatZodError(parsed.error), requestId);
  const { layer, version_id, as_of } = parsed.data;
  let content_id = parsed.data.content_id;

  try {
    // 按稳定 locator 读取（L3 三份文档 / L2 专题 / L0 消息 / L1 编号）：先解析
    // 到 content_id 再走进有流程，解析不到即 404，不虚构空版本。
    if (content_id === undefined && parsed.data.locator !== undefined) {
      if (layer === "l0" || layer === "l1") {
        const resolved = resolveStoreHistory(deps);
        if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
        const found = resolved.reader.resolveLocatorAuthorized(
          layer, parsed.data.locator, buildContentHistoryAuthFilter(deps.requestIsolation));
        if (!found) {
          return fail(404, `not_found: no content at locator ${parsed.data.locator} in layer ${layer} for this scope`, requestId);
        }
        content_id = found.ref.content_id;
      } else {
        const resolved = await resolveLocalHistory(deps);
        if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
        const ref = await resolved.reader.resolveLocator(layer, parsed.data.locator, resolved.scope);
        if (!ref) {
          return fail(404, `not_found: no content at locator ${parsed.data.locator} in layer ${layer} for this scope`, requestId);
        }
        content_id = ref.content_id;
      }
    }
    if (content_id === undefined) {
      return fail(400, "content_id or locator is required", requestId);
    }

    if (layer === "l0" || layer === "l1") {
      const resolved = resolveStoreHistory(deps);
      if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
      const { reader } = resolved;
      const scope = reader.resolveIdentityScope(layer, content_id, buildContentHistoryAuthFilter(deps.requestIsolation));
      if (!scope) {
        return fail(404, `not_found: no content ${content_id} in layer ${layer} for this scope`, requestId);
      }
      const query = {
        layer,
        content_id,
        scope,
        ...(version_id !== undefined ? { version_id } : {}),
        ...(as_of !== undefined ? { as_of } : {}),
      };
      const version = await readVersionAsync(reader, query);
      if (!version) {
        if (as_of !== undefined) {
          const coverage = await describeAsOfMiss(reader, layer, content_id, scope, as_of);
          if (coverage) return fail(404, coverage, requestId);
        }
        return fail(404, "version_not_found: the requested version does not exist (never falls back to the current body)", requestId);
      }
      const current = await readVersionAsync(reader, { layer, content_id, scope });
      return ok(buildReadData(version, current, { asOf: as_of }), requestId);
    }

    const resolved = await resolveLocalHistory(deps);
    if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
    const { reader, scope } = resolved;
    if (!(await localIdentityExists(reader, layer, content_id, scope))) {
      return fail(404, `not_found: no content ${content_id} in layer ${layer} for this scope`, requestId);
    }
    const version = await readVersionAsync(reader, {
      layer,
      content_id,
      scope,
      ...(version_id !== undefined ? { version_id } : {}),
      ...(as_of !== undefined ? { as_of } : {}),
    });
    if (!version) {
      if (as_of !== undefined) {
        const coverage = await describeAsOfMiss(reader, layer, content_id, scope, as_of);
        if (coverage) return fail(404, coverage, requestId);
      }
      return fail(404, "version_not_found: the requested version does not exist (never falls back to the current body)", requestId);
    }
    const current = await readVersionAsync(reader, { layer, content_id, scope });
    return ok(buildReadData(version, current, { asOf: as_of }), requestId);
  } catch (err) {
    return failureEnvelope(err, requestId);
  }
}

/**
 * Distinguish "the identity's history was never recorded before this instant"
 * from "that version never existed".
 *
 * An `as_of` earlier than an imported baseline's coverage start is reported as
 * `history_not_recorded` with the coverage boundary — a plain
 * `version_not_found` would falsely claim the version never existed. A complete
 * identity (created under history) has no such gap and returns null so the
 * caller can answer `version_not_found`.
 */
async function describeAsOfMiss(
  reader: HistoryReader,
  layer: HistoryLayer,
  contentId: string,
  scope: HistoryScope,
  asOf: string,
): Promise<string | null> {
  const page = await reader.listChanges({ scope, layer, content_id: contentId, limit: 1 });
  const earliest = page.items[0];
  if (!earliest) return null;
  if (earliest.history_complete === false && asOf < earliest.recorded_at) {
    return (
      `history_not_recorded: this identity's recorded history starts at ${earliest.recorded_at}; ` +
      `history before that point was not recorded (coverage is incomplete)`
    );
  }
  return null;
}

// ============================
// Successor / continuation helpers
// ============================

const PAGE_FETCH_LIMIT = 500;

interface LayerVersionScan {
  status: "ok" | "unavailable" | "disabled";
  message?: string;
  versions: ContentVersion[];
  complete: boolean;
  upper_sequence: number | null;
}

interface StoreHistorySource {
  status: "ok" | "unavailable" | "disabled";
  message?: string;
  reader?: ScopedContentHistoryReader;
}

function storeHistorySource(deps: ContentHistoryDeps): StoreHistorySource {
  const store = deps.getStore();
  if (!store) return { status: "unavailable", message: "store_unavailable" };
  if (typeof store.getContentHistory !== "function") {
    return { status: "unavailable", message: "history_unavailable" };
  }
  if (typeof store.isContentHistoryEnabled === "function" && !store.isContentHistoryEnabled()) {
    return { status: "disabled", message: "history_not_enabled" };
  }
  const reader = store.getContentHistory();
  if (!reader) return { status: "disabled", message: "history_not_enabled" };
  return { status: "ok", reader };
}

interface LocalHistorySource {
  status: "ok" | "unavailable" | "disabled";
  message?: string;
  reader?: HistoryReader;
  scope?: HistoryScope;
}

async function localHistorySource(deps: ContentHistoryDeps): Promise<LocalHistorySource> {
  const storage = resolveProfileStorage(deps);
  if (!storage) return { status: "unavailable", message: "storage_unavailable" };
  if (!hasLocalPublishRoot(storage)) return { status: "unavailable", message: "history_unavailable" };
  const history: LocalContentHistory | null = storage.getContentHistory() ?? null;
  if (!history) return { status: "unavailable", message: "history_unavailable" };
  if (!(await history.isEnabled())) return { status: "disabled", message: "history_not_enabled" };
  return { status: "ok", reader: history, scope: profileHistoryScope(deps.requestIsolation) };
}

function probeStoreUpper(
  reader: ScopedContentHistoryReader,
  auth: ContentHistoryAuthFilter,
  layer: HistoryLayer,
): number {
  return reader.listAuthorizedChanges({ auth, layer, limit: 1 }).upper_sequence;
}

function scanStoreLayer(
  reader: ScopedContentHistoryReader,
  auth: ContentHistoryAuthFilter,
  layer: HistoryLayer,
  upper: number,
  cap: number,
): { versions: ContentVersion[]; complete: boolean } {
  const versions: ContentVersion[] = [];
  let after: { sequence: number; version_id: string } | undefined;
  for (;;) {
    const page = reader.listAuthorizedChanges({
      auth,
      layer,
      upper_sequence: upper,
      ...(after !== undefined ? { after } : {}),
      limit: Math.max(1, Math.min(PAGE_FETCH_LIMIT, cap - versions.length)),
    });
    versions.push(...page.items);
    if (!page.next || versions.length >= cap) return { versions, complete: !page.next };
    after = page.next;
  }
}

async function scanLocalLayer(
  reader: HistoryReader,
  scope: HistoryScope,
  layer: HistoryLayer,
  upper: number,
  cap: number,
): Promise<{ versions: ContentVersion[]; complete: boolean }> {
  const versions: ContentVersion[] = [];
  let after: { sequence: number; version_id: string } | undefined;
  for (;;) {
    const page = await reader.listChanges({
      scope,
      layer,
      upper_sequence: upper,
      ...(after !== undefined ? { after } : {}),
      limit: Math.max(1, Math.min(PAGE_FETCH_LIMIT, cap - versions.length)),
    });
    versions.push(...page.items);
    if (!page.next || versions.length >= cap) return { versions, complete: !page.next };
    after = page.next;
  }
}

/** 身份的当前指针摘要（"最新版本"语义，不含跨身份的延续图）。 */
function stateView(version: ContentVersion): ContentVersionStateView {
  return {
    version_id: version.ref.version_id,
    state: version.state,
    recorded_at: version.recorded_at,
    sequence: version.sequence,
  };
}

function buildReadData(
  version: ContentVersion,
  current: ContentVersion | null,
  options: { asOf?: string } = {},
): ContentReadData {
  const isCurrent = !!current && current.ref.version_id === version.ref.version_id;
  const data: ContentReadData = {
    version: toView(version),
    is_current: isCurrent,
    has_successor: !!current && current.sequence > version.sequence,
  };
  if (!isCurrent && current) {
    data.current = stateView(current);
    data.latest_version = stateView(current);
  }
  if (options.asOf !== undefined) {
    // `current`/`is_current` above describe NOW, not the `as_of` boundary; label
    // it so a caller can never read today's state as "current then".
    data.as_of = options.asOf;
    data.current_is_now = true;
  }
  return data;
}

async function localIdentityExists(
  reader: HistoryReader,
  layer: HistoryLayer,
  contentId: string,
  scope: HistoryScope,
): Promise<boolean> {
  const page = await reader.listChanges({ scope, layer, content_id: contentId, limit: 1 });
  return page.items.length > 0;
}

// ============================
// Pagination
// ============================

interface PageRequest {
  layer: HistoryLayer;
  contentId: string | null;
  /** Scope key the cursor must reproduce exactly (trusted-request derived). */
  cursorScope: HistoryScope;
  fromRecordedAt?: string;
  toRecordedAt?: string;
  limit: number;
  cursor?: string;
  list: (query: {
    scope: HistoryScope;
    layer: HistoryLayer;
    content_id?: string;
    from_recorded_at?: string;
    to_recorded_at?: string;
    upper_sequence?: number;
    after?: { sequence: number; version_id: string };
    limit: number;
  }) => HistoryPage | Promise<HistoryPage>;
}

async function runPage(request: PageRequest): Promise<{ data: ContentHistoryData; effectiveFrom: string | undefined; effectiveTo: string | undefined }> {
  const cursor = request.cursor !== undefined ? decodeHistoryCursor(request.cursor) : null;
  if (cursor) {
    if (cursor.layer !== request.layer) throw new HistoryCursorError("cursor layer does not match this request");
    if ((cursor.content_id ?? null) !== request.contentId) {
      throw new HistoryCursorError("cursor content_id does not match this request");
    }
    if (scopeKeyOf(cursor.scope) !== scopeKeyOf(request.cursorScope)) {
      throw new HistoryCursorError("cursor scope does not match the trusted request scope");
    }
    if (request.fromRecordedAt !== undefined && cursor.from_recorded_at !== request.fromRecordedAt) {
      throw new HistoryCursorError("cursor from_recorded_at does not match this request");
    }
    if (request.toRecordedAt !== undefined && cursor.to_recorded_at !== request.toRecordedAt) {
      throw new HistoryCursorError("cursor to_recorded_at does not match this request");
    }
  }

  const from = cursor?.from_recorded_at ?? request.fromRecordedAt;
  const to = cursor?.to_recorded_at ?? request.toRecordedAt;
  const page = await request.list({
    scope: request.cursorScope,
    layer: request.layer,
    ...(request.contentId !== null ? { content_id: request.contentId } : {}),
    ...(from !== undefined ? { from_recorded_at: from } : {}),
    ...(to !== undefined ? { to_recorded_at: to } : {}),
    ...(cursor ? { upper_sequence: cursor.upper_sequence } : {}),
    ...(cursor?.after ? { after: cursor.after } : {}),
    limit: request.limit,
  });

  const nextCursor = page.next
    ? encodeHistoryCursor({
        v: 1,
        upper_sequence: page.upper_sequence,
        layer: request.layer,
        content_id: request.contentId,
        from_recorded_at: from ?? null,
        to_recorded_at: to ?? null,
        scope: request.cursorScope,
        after: page.next,
      })
    : null;

  return {
    data: {
      items: page.items.map(toView),
      upper_sequence: page.upper_sequence,
      next_cursor: nextCursor,
      history_complete: page.history_complete,
    },
    effectiveFrom: from,
    effectiveTo: to,
  };
}

const DEFAULT_PAGE_LIMIT = 50;

// ============================
// content/history
// ============================

export async function handleContentHistory(
  body: unknown,
  _auth: V2AuthContext,
  requestId: string,
  deps: ContentHistoryDeps,
): Promise<ApiResponseEnvelope> {
  const parsed = contentHistoryRequestSchema.safeParse(body);
  if (!parsed.success) return fail(400, formatZodError(parsed.error), requestId);
  const { layer, content_id, from_recorded_at, to_recorded_at, cursor } = parsed.data;
  const limit = parsed.data.limit ?? DEFAULT_PAGE_LIMIT;

  try {
    if (layer === "l0" || layer === "l1") {
      const resolved = resolveStoreHistory(deps);
      if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
      const { reader } = resolved;
      const scope = reader.resolveIdentityScope(layer, content_id, buildContentHistoryAuthFilter(deps.requestIsolation));
      if (!scope) {
        return fail(404, `not_found: no content ${content_id} in layer ${layer} for this scope`, requestId);
      }
      const page = await runPage({
        layer,
        contentId: content_id,
        cursorScope: scope,
        fromRecordedAt: from_recorded_at,
        toRecordedAt: to_recorded_at,
        limit,
        cursor,
        list: (query) => reader.listChanges(query),
      });
      return ok(page.data, requestId);
    }

    const resolved = await resolveLocalHistory(deps);
    if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
    const { reader, scope } = resolved;
    if (!(await localIdentityExists(reader, layer, content_id, scope))) {
      return fail(404, `not_found: no content ${content_id} in layer ${layer} for this scope`, requestId);
    }
    const page = await runPage({
      layer,
      contentId: content_id,
      cursorScope: scope,
      fromRecordedAt: from_recorded_at,
      toRecordedAt: to_recorded_at,
      limit,
      cursor,
      list: (query) => reader.listChanges(query),
    });
    return ok(page.data, requestId);
  } catch (err) {
    return failureEnvelope(err, requestId);
  }
}

// ============================
// content/changes
// ============================

export async function handleContentChanges(
  body: unknown,
  _auth: V2AuthContext,
  requestId: string,
  deps: ContentHistoryDeps,
): Promise<ApiResponseEnvelope> {
  const parsed = contentChangesRequestSchema.safeParse(body);
  if (!parsed.success) return fail(400, formatZodError(parsed.error), requestId);
  const { layer, content_id, from_recorded_at, to_recorded_at, cursor } = parsed.data;
  const limit = parsed.data.limit ?? DEFAULT_PAGE_LIMIT;

  try {
    if (layer === "l0" || layer === "l1") {
      const resolved = resolveStoreHistory(deps);
      if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
      const { reader } = resolved;
      const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
      const page = await runPage({
        layer,
        contentId: content_id ?? null,
        cursorScope: authAsScope(auth),
        fromRecordedAt: from_recorded_at,
        toRecordedAt: to_recorded_at,
        limit,
        cursor,
        list: (query) =>
          reader.listAuthorizedChanges({
            auth,
            layer,
            ...(query.content_id !== undefined ? { content_id: query.content_id } : {}),
            ...(query.from_recorded_at !== undefined ? { from_recorded_at: query.from_recorded_at } : {}),
            ...(query.to_recorded_at !== undefined ? { to_recorded_at: query.to_recorded_at } : {}),
            ...(query.upper_sequence !== undefined ? { upper_sequence: query.upper_sequence } : {}),
            ...(query.after !== undefined ? { after: query.after } : {}),
            limit: query.limit,
          }),
      });
      return ok(page.data, requestId);
    }

    if (layer === "l2" || layer === "l3") {
      const resolved = await resolveLocalHistory(deps);
      if (!resolved.ok) return fail(resolved.failure.status, resolved.failure.message, requestId);
      const { reader, scope } = resolved;
      const page = await runPage({
        layer,
        contentId: content_id ?? null,
        cursorScope: scope,
        fromRecordedAt: from_recorded_at,
        toRecordedAt: to_recorded_at,
        limit,
        cursor,
        list: (query) => reader.listChanges(query),
      });
      return ok(page.data, requestId);
    }

    return await handleCrossLayerChanges(deps, { content_id, from_recorded_at, to_recorded_at, limit, cursor }, requestId);
  } catch (err) {
    return failureEnvelope(err, requestId);
  }
}

// ============================
// content/changes — cross-layer (L0..L3)
// ============================

/** Per-layer scan cap for a cross-layer changes page (bounded). */
const CROSS_LAYER_SCAN_CAP = 4000;

interface ChangesCursor {
  v: 1;
  content_id: string | null;
  from_recorded_at: string | null;
  to_recorded_at: string | null;
  store_scope_key: string;
  profile_scope_key: string;
  bounds: Partial<Record<HistoryLayer, number>>;
  /**
   * Layers that were NOT covered on the first page (unavailable / disabled /
   * error). Pinned so a layer that becomes available later cannot silently add
   * its (newer) content to an already-issued cursor's universe.
   */
  unavailable: Partial<Record<HistoryLayer, { status: "unavailable" | "disabled" | "error"; message?: string }>>;
  after: { recorded_at: string; layer: HistoryLayer; sequence: number; version_id: string } | null;
}

function encodeChangesCursor(cursor: ChangesCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf-8").toString("base64url");
}

function decodeChangesCursor(raw: string): ChangesCursor {
  if (typeof raw !== "string" || raw.trim() === "") throw new HistoryCursorError("changes cursor must be a non-empty string");
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(raw, "base64url").toString("utf-8"));
  } catch {
    throw new HistoryCursorError("changes cursor is not valid base64url JSON");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new HistoryCursorError("changes cursor root must be an object");
  const c = parsed as Record<string, unknown>;
  if (c.v !== 1) throw new HistoryCursorError(`unsupported changes cursor version ${String(c.v)}`);
  for (const key of ["content_id", "from_recorded_at", "to_recorded_at", "store_scope_key", "profile_scope_key"] as const) {
    const value = c[key];
    if (value !== null && (typeof value !== "string" || value === "")) {
      throw new HistoryCursorError(`changes cursor.${key} must be null or a non-empty string`);
    }
  }
  if (typeof c.store_scope_key !== "string" || c.store_scope_key === "") throw new HistoryCursorError("changes cursor.store_scope_key is required");
  if (typeof c.profile_scope_key !== "string" || c.profile_scope_key === "") throw new HistoryCursorError("changes cursor.profile_scope_key is required");
  const boundsRaw = c.bounds;
  if (!boundsRaw || typeof boundsRaw !== "object" || Array.isArray(boundsRaw)) throw new HistoryCursorError("changes cursor.bounds must be an object");
  const bounds: Partial<Record<HistoryLayer, number>> = {};
  for (const [key, value] of Object.entries(boundsRaw as Record<string, unknown>)) {
    if (!["l0", "l1", "l2", "l3"].includes(key)) throw new HistoryCursorError(`changes cursor.bounds has unsupported key "${key}"`);
    if (!Number.isInteger(value) || (value as number) < 0) throw new HistoryCursorError(`changes cursor.bounds.${key} must be a non-negative integer`);
    bounds[key as HistoryLayer] = value as number;
  }
  const unavailableRaw = c.unavailable;
  const unavailable: ChangesCursor["unavailable"] = {};
  if (unavailableRaw !== null && unavailableRaw !== undefined) {
    if (typeof unavailableRaw !== "object" || Array.isArray(unavailableRaw)) {
      throw new HistoryCursorError("changes cursor.unavailable must be an object");
    }
    for (const [key, value] of Object.entries(unavailableRaw as Record<string, unknown>)) {
      if (!["l0", "l1", "l2", "l3"].includes(key)) throw new HistoryCursorError(`changes cursor.unavailable has unsupported key "${key}"`);
      const entry = value as Record<string, unknown> | null;
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
        throw new HistoryCursorError(`changes cursor.unavailable.${key} must be { status, message? }`);
      }
      const status = entry.status;
      const message = entry.message;
      if ((status !== "unavailable" && status !== "disabled" && status !== "error")
        || (message !== undefined && typeof message !== "string")) {
        throw new HistoryCursorError(`changes cursor.unavailable.${key} must be { status, message? }`);
      }
      unavailable[key as HistoryLayer] = {
        status,
        ...(typeof message === "string" ? { message } : {}),
      };
    }
  }
  let after: ChangesCursor["after"] = null;
  if (c.after !== null && c.after !== undefined) {
    const a = c.after as Record<string, unknown>;
    if (typeof a !== "object" || Array.isArray(a)
      || typeof a.recorded_at !== "string" || a.recorded_at === ""
      || !["l0", "l1", "l2", "l3"].includes(String(a.layer))
      || !Number.isInteger(a.sequence) || (a.sequence as number) < 0
      || typeof a.version_id !== "string" || a.version_id === "") {
      throw new HistoryCursorError("changes cursor.after must be { recorded_at, layer, sequence, version_id }");
    }
    after = { recorded_at: a.recorded_at, layer: a.layer as HistoryLayer, sequence: a.sequence as number, version_id: a.version_id };
  }
  return {
    v: 1,
    content_id: (c.content_id as string | null) ?? null,
    from_recorded_at: (c.from_recorded_at as string | null) ?? null,
    to_recorded_at: (c.to_recorded_at as string | null) ?? null,
    store_scope_key: c.store_scope_key,
    profile_scope_key: c.profile_scope_key,
    bounds,
    unavailable,
    after,
  };
}

function compareGlobalOrder(a: ContentVersion, b: ContentVersion): number {
  if (a.recorded_at !== b.recorded_at) return a.recorded_at < b.recorded_at ? -1 : 1;
  if (a.layer !== b.layer) return a.layer < b.layer ? -1 : 1;
  if (a.sequence !== b.sequence) return a.sequence - b.sequence;
  return a.ref.version_id < b.ref.version_id ? -1 : a.ref.version_id > b.ref.version_id ? 1 : 0;
}

function afterGlobalKey(version: ContentVersion, after: NonNullable<ChangesCursor["after"]>): boolean {
  if (version.recorded_at !== after.recorded_at) return version.recorded_at > after.recorded_at;
  if (version.layer !== after.layer) return version.layer > after.layer;
  if (version.sequence !== after.sequence) return version.sequence > after.sequence;
  return version.ref.version_id > after.version_id;
}

function globalKeyOf(version: ContentVersion): NonNullable<ChangesCursor["after"]> {
  return { recorded_at: version.recorded_at, layer: version.layer, sequence: version.sequence, version_id: version.ref.version_id };
}

interface LayerCollect {
  status: "ok" | "unavailable" | "disabled" | "error";
  message?: string;
  upper_sequence?: number;
  items: ContentVersion[];
  history_complete: boolean;
  capped: boolean;
}

async function collectChangesLayer(
  deps: ContentHistoryDeps,
  layer: HistoryLayer,
  bounds: Partial<Record<HistoryLayer, number>>,
  window: { from?: string; to?: string },
  contentId: string | undefined,
  cap: number,
): Promise<LayerCollect> {
  if (layer === "l0" || layer === "l1") {
    const source = storeHistorySource(deps);
    if (source.status !== "ok" || !source.reader) {
      return { status: source.status === "ok" ? "unavailable" : source.status, message: source.message, items: [], history_complete: false, capped: false };
    }
    const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
    const upper = bounds[layer] ?? probeStoreUpper(source.reader, auth, layer);
    const items: ContentVersion[] = [];
    let after: { sequence: number; version_id: string } | undefined;
    let historyComplete = true;
    let capped = false;
    for (;;) {
      const page = source.reader.listAuthorizedChanges({
        auth,
        layer,
        upper_sequence: upper,
        ...(contentId !== undefined ? { content_id: contentId } : {}),
        ...(window.from !== undefined ? { from_recorded_at: window.from } : {}),
        ...(window.to !== undefined ? { to_recorded_at: window.to } : {}),
        ...(after !== undefined ? { after } : {}),
        limit: Math.max(1, Math.min(PAGE_FETCH_LIMIT, cap - items.length)),
      });
      items.push(...page.items);
      historyComplete = page.history_complete;
      if (!page.next) break;
      if (items.length >= cap) { capped = true; break; }
      after = page.next;
    }
    bounds[layer] = upper;
    return { status: "ok", upper_sequence: upper, items, history_complete: historyComplete, capped };
  }

  const source = await localHistorySource(deps);
  if (source.status !== "ok" || !source.reader || !source.scope) {
    return { status: source.status === "ok" ? "unavailable" : source.status, message: source.message, items: [], history_complete: false, capped: false };
  }
  const probeUpper = bounds.l2 ?? bounds.l3;
  const upper = probeUpper ?? (await source.reader.listChanges({ scope: source.scope, limit: 1 })).upper_sequence;
  const items: ContentVersion[] = [];
  let after: { sequence: number; version_id: string } | undefined;
  let historyComplete = true;
  let capped = false;
  for (;;) {
    const page = await source.reader.listChanges({
      scope: source.scope,
      layer,
      upper_sequence: upper,
      ...(contentId !== undefined ? { content_id: contentId } : {}),
      ...(window.from !== undefined ? { from_recorded_at: window.from } : {}),
      ...(window.to !== undefined ? { to_recorded_at: window.to } : {}),
      ...(after !== undefined ? { after } : {}),
      limit: Math.max(1, Math.min(PAGE_FETCH_LIMIT, cap - items.length)),
    });
    items.push(...page.items);
    historyComplete = page.history_complete;
    if (!page.next) break;
    if (items.length >= cap) { capped = true; break; }
    after = page.next;
  }
  bounds.l2 = upper;
  bounds.l3 = upper;
  return { status: "ok", upper_sequence: upper, items, history_complete: historyComplete, capped };
}

async function handleCrossLayerChanges(
  deps: ContentHistoryDeps,
  req: { content_id?: string; from_recorded_at?: string; to_recorded_at?: string; limit: number; cursor?: string },
  requestId: string,
): Promise<ApiResponseEnvelope> {
  const auth = buildContentHistoryAuthFilter(deps.requestIsolation);
  const profileScope = profileHistoryScope(deps.requestIsolation);
  const storeScopeKey = scopeKeyOf(authAsScope(auth));
  const profileScopeKey = scopeKeyOf(profileScope);

  const cursor = req.cursor !== undefined ? decodeChangesCursor(req.cursor) : null;
  if (cursor) {
    if (cursor.content_id !== (req.content_id ?? null)) throw new HistoryCursorError("changes cursor content_id does not match this request");
    if (cursor.from_recorded_at !== (req.from_recorded_at ?? null)) throw new HistoryCursorError("changes cursor from_recorded_at does not match this request");
    if (cursor.to_recorded_at !== (req.to_recorded_at ?? null)) throw new HistoryCursorError("changes cursor to_recorded_at does not match this request");
    if (cursor.store_scope_key !== storeScopeKey) throw new HistoryCursorError("changes cursor does not match the trusted request scope");
    if (cursor.profile_scope_key !== profileScopeKey) throw new HistoryCursorError("changes cursor does not match the trusted request profile scope");
  }

  const bounds: Partial<Record<HistoryLayer, number>> = { ...(cursor?.bounds ?? {}) };
  // Layers excluded on the first page stay excluded: a layer that becomes
  // available later must NOT add its (newer) content to this cursor's universe.
  const unavailable: ChangesCursor["unavailable"] = { ...(cursor?.unavailable ?? {}) };
  const window: { from?: string; to?: string } = {
    ...(req.from_recorded_at !== undefined ? { from: req.from_recorded_at } : {}),
    ...(req.to_recorded_at !== undefined ? { to: req.to_recorded_at } : {}),
  };

  const results: Partial<Record<HistoryLayer, LayerCollect>> = {};
  for (const layer of ["l0", "l1", "l2", "l3"] as const) {
    const pinned = unavailable[layer];
    if (pinned) {
      results[layer] = { status: pinned.status, message: pinned.message, items: [], history_complete: false, capped: false };
      continue;
    }
    try {
      results[layer] = await collectChangesLayer(deps, layer, bounds, window, req.content_id, CROSS_LAYER_SCAN_CAP);
    } catch (err) {
      results[layer] = {
        status: "error",
        message: err instanceof Error ? err.message : String(err),
        items: [],
        history_complete: false,
        capped: false,
      };
    }
    const collected = results[layer];
    if (collected && collected.status !== "ok") {
      unavailable[layer] = { status: collected.status, ...(collected.message !== undefined ? { message: collected.message } : {}) };
    }
  }

  const covered = (["l0", "l1", "l2", "l3"] as const).filter((layer) => results[layer]?.status === "ok");
  if (covered.length === 0) {
    const statuses = (["l0", "l1", "l2", "l3"] as const).map((layer) => results[layer]?.status);
    if (statuses.every((status) => status === "disabled")) {
      return fail(409, "history_not_enabled: no layer has content history enabled", requestId);
    }
    if (statuses.some((status) => status === "error")) {
      return fail(500, "history_read_failed: every layer failed to read content history", requestId);
    }
    return fail(501, "history_unavailable: no layer exposes content history", requestId);
  }

  const all: ContentVersion[] = [];
  for (const layer of covered) all.push(...(results[layer]?.items ?? []));
  all.sort(compareGlobalOrder);
  const after = cursor?.after ?? null;
  const ordered = after ? all.filter((version) => afterGlobalKey(version, after)) : all;

  const anyCapped = covered.some((layer) => results[layer]?.capped === true);
  const page = ordered.slice(0, req.limit);
  const hasMore = ordered.length > page.length;
  const last = page[page.length - 1];
  const nextCursor = !anyCapped && hasMore && last !== undefined
    ? encodeChangesCursor({
        v: 1,
        content_id: req.content_id ?? null,
        from_recorded_at: req.from_recorded_at ?? null,
        to_recorded_at: req.to_recorded_at ?? null,
        store_scope_key: storeScopeKey,
        profile_scope_key: profileScopeKey,
        bounds,
        unavailable,
        after: globalKeyOf(last),
      })
    : null;

  const layersView: Partial<Record<HistoryLayer, ContentChangesLayerView>> = {};
  for (const layer of ["l0", "l1", "l2", "l3"] as const) {
    const result = results[layer];
    if (!result) continue;
    layersView[layer] = {
      status: result.status === "ok" ? (result.capped ? "scan_capped" : "ok") : result.status,
      ...(result.message !== undefined ? { message: result.message } : {}),
      ...(result.upper_sequence !== undefined ? { upper_sequence: result.upper_sequence } : {}),
      ...(result.status === "ok" ? { history_complete: result.history_complete && !result.capped } : {}),
    };
  }

  const data: ContentChangesData = {
    items: page.map(toView),
    next_cursor: nextCursor,
    history_complete: !anyCapped && covered.length === 4 && covered.every((layer) => results[layer]?.history_complete === true),
    partial: covered.length < 4 || anyCapped,
    layers: layersView,
    ordering: { by: ["recorded_at", "layer", "sequence", "version_id"], global_transaction_order: false },
  };
  return ok(data, requestId);
}

/** The authorization filter expressed as a scope, for cursor pinning. */
function authAsScope(auth: ContentHistoryAuthFilter): HistoryScope {
  const scope: HistoryScope = { team_id: auth.team_id, user_id: auth.user_id, agent_id: auth.agent_id };
  if (auth.task_id) scope.task_id = auth.task_id;
  return scope;
}

// ============================
// Ref lookup for the existing single-read endpoints
// ============================

export interface RefLookup {
  ok: true;
  /** Present when history is enabled and the locator maps to a version. */
  ref?: VersionRef;
  /**
   * The locator's exact materialized version (or the requested one), read in
   * the SAME snapshot as `ref`. Present whenever a ref could be resolved, so a
   * caller never has to re-read the current row/file for the body.
   */
  version?: ContentVersion;
}

type RefLookupResult = RefLookup | FailResult;

export interface LocalizedRow {
  team_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
  session_id?: string;
}

/**
 * Map an existing L0/L1 record id to its content version reference, and (when
 * `versionId` is given) read that exact version.
 *
 * Legacy mode (history off) returns `{ ok: true }` with no ref, preserving the
 * existing response shape. An explicit `version_id` in legacy mode is an
 * explicit 501 — it is never silently ignored.
 */
export async function resolveStoreRecordHistory(
  deps: ContentHistoryDeps,
  layer: "l0" | "l1",
  locator: string,
  row: LocalizedRow,
  versionId?: string,
): Promise<RefLookupResult> {
  const reader = resolveStoreHistorySoft(deps);
  if (!reader) {
    if (versionId !== undefined) {
      return failResult(501, "history_unavailable: version_id reads require content history to be enabled");
    }
    return { ok: true };
  }
  const scope = layer === "l0" ? l0RowHistoryScope(row) : l1RowHistoryScope(row);
  const ref = await reader.resolveLocator(layer, locator, scope);
  if (!ref) {
    if (versionId !== undefined) {
      return failResult(404, "not_found: this record has no content-history version");
    }
    return { ok: true };
  }
  // Always materialize the exact version in ONE read. Even when the requested
  // `version_id` equals the locator's mapped version, the body/metadata must be
  // read from the immutable version (not re-read from the current row/file), so
  // the returned content, ref and source all describe the same snapshot — and a
  // rename/retire that moved the current pointer cannot put a newer body behind
  // an older ref.
  const targetVersionId = versionId ?? ref.version_id;
  const version = await readVersionAsync(reader, { layer, content_id: ref.content_id, scope, version_id: targetVersionId });
  if (!version) {
    if (versionId !== undefined) {
      return failResult(404, "version_not_found: the requested version does not exist (never falls back to the current record)");
    }
    return failResult(500, "history_corrupt: locator points to a missing version");
  }
  return { ok: true, ref: version.ref, version };
}

/**
 * Resolve an L0/L1 locator to its exact version **without** needing the current
 * row (which a retired identity no longer has), using the trusted request
 * authorization filter instead of a caller-supplied scope.
 *
 * This is the read path for `atomic/get` / `conversation/get` when the live row
 * is gone (retired): the store resolves the stored object scope behind the
 * team/user/agent (+ task) filter, so the gateway never guesses L0's session
 * dimension and never sees another owner's record.
 */
export async function resolveStoreRecordHistoryByLocator(
  deps: ContentHistoryDeps,
  layer: "l0" | "l1",
  locator: string,
  versionId?: string,
): Promise<RefLookupResult> {
  const reader = resolveStoreHistorySoft(deps);
  if (!reader) {
    if (versionId !== undefined) {
      return failResult(501, "history_unavailable: version_id reads require content history to be enabled");
    }
    return { ok: true };
  }
  if (typeof reader.resolveLocatorAuthorized !== "function") {
    if (versionId !== undefined) {
      return failResult(501, "history_unavailable: this store cannot resolve a locator behind an authorization filter");
    }
    return { ok: true };
  }
  const found = reader.resolveLocatorAuthorized(layer, locator, buildContentHistoryAuthFilter(deps.requestIsolation));
  if (!found) {
    // Absent and out-of-scope are indistinguishable (existence never leaks).
    if (versionId !== undefined) {
      return failResult(404, `not_found: no content ${locator} in layer ${layer} for this scope`);
    }
    return { ok: true };
  }
  const targetVersionId = versionId ?? found.ref.version_id;
  const version = await readVersionAsync(reader, {
    layer,
    content_id: found.ref.content_id,
    scope: found.scope,
    version_id: targetVersionId,
  });
  if (!version) {
    if (versionId !== undefined) {
      return failResult(404, "version_not_found: the requested version does not exist (never falls back to the current record)");
    }
    return failResult(500, "history_corrupt: locator points to a missing version");
  }
  return { ok: true, ref: version.ref, version };
}

/**
 * Map an L2/L3 locator (scene path / persona) to its content version reference,
 * and (when `versionId` is given) read that exact version.
 */
export async function resolveLocalLocatorHistory(
  deps: ContentHistoryDeps,
  layer: "l2" | "l3",
  locator: string,
  versionId?: string,
): Promise<RefLookupResult> {
  let soft: { reader: HistoryReader; scope: HistoryScope } | null;
  try {
    soft = await resolveLocalHistorySoft(deps);
  } catch (err) {
    return failResult(500, err instanceof Error ? err.message : String(err));
  }
  if (!soft) {
    if (versionId !== undefined) {
      return failResult(501, "history_unavailable: version_id reads require content history to be enabled");
    }
    return { ok: true };
  }
  const ref = await soft.reader.resolveLocator(layer, locator, soft.scope);
  if (!ref) {
    if (versionId !== undefined) {
      return failResult(404, "not_found: this content path has no content-history version");
    }
    return { ok: true };
  }
  // Pin the ref and its body together: read the locator's exact version (or the
  // requested one) in a single immutable read, so a concurrent rename/revise
  // cannot return an old ref with a newer body. The old path of a renamed file
  // still resolves to the version it addressed even though the current file no
  // longer exists.
  const targetVersionId = versionId ?? ref.version_id;
  const version = await readVersionAsync(soft.reader, {
    layer,
    content_id: ref.content_id,
    scope: soft.scope,
    version_id: targetVersionId,
  });
  if (!version) {
    if (versionId !== undefined) {
      return failResult(404, "version_not_found: the requested version does not exist (never falls back to the current content)");
    }
    return failResult(500, "history_corrupt: locator points to a missing version");
  }
  return { ok: true, ref: version.ref, version };
}

/**
 * The program-provided source view of a stored version, read straight from the
 * version's own metadata (`content_sources`). This is the *same-snapshot* source
 * for a historical body: it is never re-derived from the current sidecar (which
 * would describe different bytes) and never downgraded to `unknown` when the
 * version already recorded real sources.
 *
 * `known` is only returned for a well-formed known entry; a missing/unusable
 * `content_sources` is `unknown` (never an empty list implying "no evidence").
 */
export function contentSourceViewFromVersion(version: ContentVersion): ContentSourceView {
  const raw = (version.metadata ?? {})["content_sources"];
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    const value = raw as Record<string, unknown>;
    if (value.status === "unavailable" && typeof value.reason === "string") {
      return { status: "unavailable", input_ids: [], reason: value.reason };
    }
    if (
      value.status === "known"
      && (value.input_layer === "l1" || value.input_layer === "l2")
      && Array.isArray(value.input_ids)
      && typeof value.updated_at_ms === "number"
      && Number.isFinite(value.updated_at_ms)
    ) {
      const inputIds = value.input_ids.filter((id): id is string => typeof id === "string" && id.length > 0);
      const retained = Array.isArray(value.retained_input_ids)
        ? value.retained_input_ids.filter((id): id is string => typeof id === "string" && id.length > 0)
        : [];
      return {
        status: "known",
        kind: "pipeline_input",
        input_layer: value.input_layer,
        input_ids: inputIds,
        ...(retained.length > 0 ? { retained_input_ids: retained } : {}),
        updated_at_ms: value.updated_at_ms,
      };
    }
  }
  return { status: "unknown", input_ids: [] };
}
