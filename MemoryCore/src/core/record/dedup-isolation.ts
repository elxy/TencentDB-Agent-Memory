/**
 * Pure L1 dedup candidate-isolation helper.
 *
 * L1 conflict detection (batchDedup) recalls candidate memories through an
 * `IsolationFilter`. By default that filter is session-scoped, so dedup never
 * compares a new memory against records from another session.
 *
 * `extraction.dedupAcrossSessions` is an explicit, default-off opt-in that
 * drops ONLY the `sessionId` dimension from that filter, letting candidate
 * recall span sessions while every other identity dimension
 * (`teamId` / `userId` / `agentId` / `taskId`) is retained byte-for-byte.
 *
 * The helper refuses to opt in when no tenant identity is available at all —
 * without `teamId`, `userId` or `agentId` there is no boundary left to contain
 * the widened recall, so widening silently would be unsafe.
 *
 * This module is intentionally free of runtime dependencies (type-only
 * import) so it can be unit-tested in isolation.
 */

import type { IsolationFilter } from "../store/types.js";

/** Identity dimensions that scope L1 candidate recall. */
export interface DedupIsolationIdentity {
  teamId?: string;
  userId?: string;
  agentId?: string;
  taskId?: string;
  sessionId?: string;
}

/**
 * Build the `IsolationFilter` for L1 dedup candidate recall.
 *
 * - `dedupAcrossSessions=false` (default): identical to the historical
 *   behaviour — retain all provided dimensions, including `sessionId`.
 *   Returns `undefined` when no dimension is provided (no narrowing).
 * - `dedupAcrossSessions=true`: retain `teamId` / `userId` / `agentId` /
 *   `taskId` exactly and drop `sessionId`. Throws when none of
 *   `teamId` / `userId` / `agentId` is provided, because there would be no
 *   tenant identity left to scope the wider candidate pool.
 */
export function buildDedupIsolationFilter(
  identity: DedupIsolationIdentity,
  dedupAcrossSessions: boolean,
): IsolationFilter | undefined {
  const { teamId, userId, agentId, taskId, sessionId } = identity;

  if (!dedupAcrossSessions) {
    const hasAny = Boolean(teamId || userId || agentId || sessionId || taskId);
    if (!hasAny) return undefined;
    return { teamId, userId, agentId, sessionId, taskId };
  }

  if (!teamId && !userId && !agentId) {
    throw new Error(
      "dedupAcrossSessions requires at least one of teamId/userId/agentId identity " +
        "to scope candidate recall; refusing to widen dedup without an identity boundary",
    );
  }

  // Cross-session opt-in: drop sessionId only, retain everything else exactly.
  return { teamId, userId, agentId, taskId };
}
