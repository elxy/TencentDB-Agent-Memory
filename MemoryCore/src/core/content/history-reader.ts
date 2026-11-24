/**
 * Content-history read helpers shared by the gateway content API.
 *
 * This module holds the *pure* pieces of the read path so the HTTP handlers
 * stay thin and the security-relevant decisions (which scope a caller may
 * read, how a pagination cursor is validated, how two versions are compared)
 * are unit-testable without a router:
 *
 *   - the authorization filter derived from the trusted request isolation;
 *   - the exact object scope a stored version carries;
 *   - the keyset-pagination cursor (base64url JSON, re-validated against the
 *     trusted request on every page).
 *
 * 2026-10-12 收窄：两版差异接口（行 diff / 字段 diff）已撤掉；比较两个版本
 * 交给读取方的模型现场完成。
 *
 * It deliberately contains NO SQL and NO filesystem access: the store
 * (`SqliteContentHistory`) and the local (`LocalContentHistory`) readers own
 * their own storage. The gateway only narrows what those readers may return.
 *
 * Scope is an *owned identity*, never a wildcard: a missing dimension is not
 * "any owner". The store reader is handed a team/user/agent filter that is
 * always fully populated (defaults applied by the caller, matching how rows are
 * written), so an absent request dimension can only ever address the default
 * bucket — never another tenant's records.
 */

import type {
  ContentVersion,
  HistoryLayer,
  HistoryPage,
  HistoryReader,
  HistoryScope,
  VersionRef,
} from "./history-contract.js";

// ============================
// Trusted isolation → scope
// ============================

/** The isolation dimensions a gateway request exposes to the content API. */
export interface RequestIsolationView {
  teamId?: string;
  userId?: string;
  agentId?: string;
  sessionId?: string;
  taskId?: string;
}

/** Default bucket the SQLite store writes when a dimension is not supplied. */
const DEFAULT_ISOLATION_ID = "default";

/**
 * Authorization filter for the SQLite content-history reader. `team_id`,
 * `user_id` and `agent_id` are always present (the default bucket stands in for
 * an omitted request dimension) so a missing dimension can never turn into an
 * "any owner" query. `task_id` is an optional *narrowing* dimension.
 */
export interface ContentHistoryAuthFilter {
  team_id: string;
  user_id: string;
  agent_id: string;
  task_id?: string;
}

/** Build the store reader's authorization filter from the trusted request. */
export function buildContentHistoryAuthFilter(iso: RequestIsolationView | undefined): ContentHistoryAuthFilter {
  const filter: ContentHistoryAuthFilter = {
    team_id: iso?.teamId || DEFAULT_ISOLATION_ID,
    user_id: iso?.userId || DEFAULT_ISOLATION_ID,
    agent_id: iso?.agentId || DEFAULT_ISOLATION_ID,
  };
  if (iso?.taskId) filter.task_id = iso.taskId;
  return filter;
}

/**
 * Object scope of a content identity for the *local* L2/L3 reader.
 *
 * L2/L3 cognition is team+agent scoped and the storage view is already scoped
 * to `profiles/{profileScope}/`, but the version records carry their own object
 * scope. It must match the profile scope's (team-or-user, agent) collapse — the
 * reader requires exact object-scope equality, so this is the only scope the
 * gateway may pass. It is never widened to `{}`.
 */
export function profileHistoryScope(iso: RequestIsolationView | undefined): HistoryScope {
  if (!iso) return {};
  const teamId = iso.teamId || iso.userId || DEFAULT_ISOLATION_ID;
  const scope: HistoryScope = { team_id: teamId, agent_id: iso.agentId || DEFAULT_ISOLATION_ID };
  return scope;
}

/** Exact object scope of an L1 record (session is intentionally excluded). */
export function l1RowHistoryScope(row: {
  team_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
}): HistoryScope {
  const scope: HistoryScope = {};
  if (row.team_id) scope.team_id = row.team_id;
  if (row.user_id) scope.user_id = row.user_id;
  if (row.agent_id) scope.agent_id = row.agent_id;
  if (row.task_id) scope.task_id = row.task_id;
  return scope;
}

/** Exact object scope of an L0 message (includes the session). */
export function l0RowHistoryScope(row: {
  team_id?: string;
  user_id?: string;
  agent_id?: string;
  task_id?: string;
  session_id?: string;
}): HistoryScope {
  const scope = l1RowHistoryScope(row);
  if (row.session_id) scope.session_id = row.session_id;
  return scope;
}

// ============================
// Reader capability surface
// ============================

/** A {@link HistoryReader} that can additionally answer auth-filtered queries. */
export interface ScopedContentHistoryReader extends HistoryReader {
  /**
   * The exact stored object scope of a content identity, if (and only if) it
   * lies inside the caller's authorization filter. `null` means "absent or
   * outside the caller's scope" — the two are deliberately indistinguishable.
   */
  resolveIdentityScope(
    layer: HistoryLayer,
    contentId: string,
    auth: ContentHistoryAuthFilter,
  ): HistoryScope | null;

  /**
   * Resolve an old locator (record id / profile path) to the exact version it
   * addressed, but ONLY inside the caller's authorization filter.
   *
   * Unlike {@link HistoryReader.resolveLocator} this does not need the caller to
   * already know the identity's stored object scope (which for L0 carries a
   * session the caller cannot guess): the stored scope is resolved inside the
   * store and every authorization dimension is re-checked before anything is
   * returned. `null` covers both "absent" and "outside the caller's scope" so
   * existence never leaks. The locator mapping (not the current row) is the
   * source of truth, so a retired identity is still resolvable.
   */
  resolveLocatorAuthorized(
    layer: HistoryLayer,
    locator: string,
    auth: ContentHistoryAuthFilter,
  ): { scope: HistoryScope; ref: VersionRef } | null;

  /**
   * Browse every version inside the caller's authorization filter, optionally
   * narrowed to one identity or a recorded-time window. Same keyset pagination
   * contract as {@link HistoryReader.listChanges}.
   */
  listAuthorizedChanges(query: AuthorizedHistoryPageQuery): HistoryPage;
}

export interface AuthorizedHistoryPageQuery {
  auth: ContentHistoryAuthFilter;
  layer: HistoryLayer;
  content_id?: string;
  from_recorded_at?: string;
  to_recorded_at?: string;
  upper_sequence?: number;
  /** Ascending keyset cursor; `(sequence, version_id)` is the total order. */
  after?: { sequence: number; version_id: string };
  limit: number;
}

/** Await a reader that may be sync (SQLite) or async (local filesystem). */
export async function readVersionAsync(
  reader: HistoryReader,
  query: Parameters<HistoryReader["readVersion"]>[0],
): Promise<ContentVersion | null> {
  return await reader.readVersion(query);
}

/**
 * Whether a stored object scope satisfies an authorization filter. Absent
 * dimensions compare as the empty string, so an omitted stored dimension never
 * matches a non-empty filter value.
 */
export function scopeMatchesAuth(scope: HistoryScope, auth: ContentHistoryAuthFilter): boolean {
  if ((scope.team_id ?? "") !== auth.team_id) return false;
  if ((scope.user_id ?? "") !== auth.user_id) return false;
  if ((scope.agent_id ?? "") !== auth.agent_id) return false;
  if (auth.task_id !== undefined && (scope.task_id ?? "") !== auth.task_id) return false;
  return true;
}

// ============================
// Pagination cursor
// ============================

export const HISTORY_CURSOR_VERSION = 1 as const;

/**
 * A keyset cursor. It pins the storage `upper_sequence` (so a commit that lands
 * mid-pagination cannot mix into later pages) and repeats the scope/filters so
 * a decoded cursor is re-validated against the trusted request instead of being
 * trusted outright. It is opaque data, not a signed credential: a caller that
 * forges one only restates a scope it already had to be authorized for.
 */
export interface HistoryCursor {
  v: typeof HISTORY_CURSOR_VERSION;
  upper_sequence: number;
  layer: HistoryLayer;
  content_id: string | null;
  from_recorded_at: string | null;
  to_recorded_at: string | null;
  scope: HistoryScope;
  after: { sequence: number; version_id: string } | null;
}

/** Raised when a cursor is structurally invalid; callers map this to HTTP 400. */
export class HistoryCursorError extends Error {
  readonly code = "invalid_cursor";
  constructor(message: string) {
    super(`invalid_cursor: ${message}`);
    this.name = "HistoryCursorError";
  }
}

export function encodeHistoryCursor(cursor: HistoryCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf-8").toString("base64url");
}

export function decodeHistoryCursor(raw: string): HistoryCursor {
  if (typeof raw !== "string" || raw.trim() === "") throw new HistoryCursorError("cursor must be a non-empty string");
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(raw, "base64url").toString("utf-8"));
  } catch {
    throw new HistoryCursorError("cursor is not valid base64url JSON");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new HistoryCursorError("cursor root must be an object");
  const c = parsed as Record<string, unknown>;
  if (c.v !== HISTORY_CURSOR_VERSION) throw new HistoryCursorError(`unsupported cursor version ${String(c.v)}`);
  if (!Number.isInteger(c.upper_sequence) || (c.upper_sequence as number) < 0) {
    throw new HistoryCursorError("cursor.upper_sequence must be a non-negative integer");
  }
  if (c.layer !== "l0" && c.layer !== "l1" && c.layer !== "l2" && c.layer !== "l3") {
    throw new HistoryCursorError("cursor.layer is invalid");
  }
  if (c.content_id !== null && (typeof c.content_id !== "string" || c.content_id === "")) {
    throw new HistoryCursorError("cursor.content_id must be null or a non-empty string");
  }
  for (const key of ["from_recorded_at", "to_recorded_at"] as const) {
    const value = c[key];
    if (value !== null && (typeof value !== "string" || Number.isNaN(Date.parse(value)))) {
      throw new HistoryCursorError(`cursor.${key} must be null or a valid ISO-8601 instant`);
    }
  }
  const scopeRaw = c.scope;
  if (!scopeRaw || typeof scopeRaw !== "object" || Array.isArray(scopeRaw)) {
    throw new HistoryCursorError("cursor.scope must be an object");
  }
  const scope: HistoryScope = {};
  for (const [key, value] of Object.entries(scopeRaw as Record<string, unknown>)) {
    if (!["team_id", "user_id", "agent_id", "task_id", "session_id"].includes(key)) {
      throw new HistoryCursorError(`cursor.scope has unsupported key "${key}"`);
    }
    if (typeof value !== "string" || value === "") {
      throw new HistoryCursorError(`cursor.scope.${key} must be a non-empty string`);
    }
    (scope as Record<string, string>)[key] = value;
  }
  let after: { sequence: number; version_id: string } | null = null;
  if (c.after !== null && c.after !== undefined) {
    const a = c.after as Record<string, unknown>;
    if (typeof a !== "object" || Array.isArray(a)
      || !Number.isInteger(a.sequence) || (a.sequence as number) < 0
      || typeof a.version_id !== "string" || a.version_id === "") {
      throw new HistoryCursorError("cursor.after must be null or { sequence, version_id }");
    }
    after = { sequence: a.sequence as number, version_id: a.version_id };
  }
  return {
    v: HISTORY_CURSOR_VERSION,
    upper_sequence: c.upper_sequence as number,
    layer: c.layer,
    content_id: (c.content_id as string | null) ?? null,
    from_recorded_at: (c.from_recorded_at as string | null) ?? null,
    to_recorded_at: (c.to_recorded_at as string | null) ?? null,
    scope,
    after,
  };
}

/** Canonical, order-independent key for a scope (empty dimensions dropped). */
export function scopeKeyOf(scope: HistoryScope): string {
  return JSON.stringify(["team_id", "user_id", "agent_id", "task_id", "session_id"].map((k) => scope[k as keyof HistoryScope] ?? null));
}
