/**
 * history-write.ts — the small, program-only shared surface for the formal
 * content write entry points (2026-10-12 收窄版).
 *
 * This module exists so the SQLite store commit entry, the local L2/L3 planner
 * and their callers agree on ONE structural validation and ONE vocabulary of
 * "which change kinds a writer can faithfully apply" — instead of each writer
 * inventing its own. It intentionally has no storage dependency: it neither
 * opens a database nor touches the filesystem.
 *
 * What lives here
 * ---------------
 *   - {@link CONTENT_CHANGE_KINDS}/{@link STORE_APPLICABLE_CHANGE_KINDS}: the
 *     change-kind vocabulary. After the 2026-10-12 trim the only kinds are
 *     `baseline` / `create` / `revise` / `retire` / `restore`; rename/merge/
 *     split (content identity continuation) are no longer part of the design.
 *   - {@link assertChangeCommitShape}: structural validation of an external
 *     `ChangeCommit`. It proves the shape is well formed; it does NOT prove the
 *     semantics (CAS, scope, content truth) — those are re-checked inside the
 *     store transaction.
 *   - {@link CommitContentChangesOptions}: caller-supplied embeddings for the
 *     store commit entry. Embeddings are never fabricated here.
 *
 * Everything in this module is program-owned data, never model authority.
 */

import type {
  ChangeCommit,
  ChangeKind,
} from "./history-contract.js";

/** Every change kind the contract defines. */
export const CONTENT_CHANGE_KINDS: ReadonlySet<ChangeKind> = new Set<ChangeKind>([
  "baseline",
  "create",
  "revise",
  "retire",
  "restore",
]);

/**
 * Change kinds a store commit can faithfully apply to an L0/L1 current row
 * (metadata + FTS + vector index) inside the same transaction.
 *
 * `baseline` is excluded: a baseline is created by the import path, not by a
 * caller commit.
 */
export const STORE_APPLICABLE_CHANGE_KINDS: ReadonlySet<ChangeKind> = new Set<ChangeKind>([
  "create",
  "revise",
  "retire",
  "restore",
]);

/**
 * Structural failure of a caller-supplied write request. Distinct from a
 * storage-level conflict so the HTTP layer can map it to a 400 while CAS
 * conflicts map to a 409.
 */
export class HistoryWriteInputError extends Error {
  readonly code = "history_invalid_input";
  constructor(message: string) {
    super(message);
    this.name = "HistoryWriteInputError";
  }
}

function invalid(message: string): never {
  throw new HistoryWriteInputError(message);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
}

function assertVersionRef(value: unknown, where: string): void {
  if (!isPlainObject(value)) invalid(`${where} must be an object`);
  const ref = value as Record<string, unknown>;
  if (ref.layer !== "l0" && ref.layer !== "l1" && ref.layer !== "l2" && ref.layer !== "l3") {
    invalid(`${where}.layer is invalid`);
  }
  if (!isNonEmptyString(ref.content_id)) invalid(`${where}.content_id must be a non-empty string`);
  if (!isNonEmptyString(ref.version_id)) invalid(`${where}.version_id must be a non-empty string`);
}

function assertMutationShape(mutation: unknown, index: number): void {
  const where = `mutations[${index}]`;
  if (!isPlainObject(mutation)) invalid(`${where} must be an object`);
  const m = mutation as Record<string, unknown>;
  const draft = m.draft as Record<string, unknown> | undefined;
  if (!isPlainObject(draft)) invalid(`${where}.draft must be an object`);
  if (draft.layer !== "l0" && draft.layer !== "l1" && draft.layer !== "l2" && draft.layer !== "l3") {
    invalid(`${where}.draft.layer is invalid`);
  }
  if (typeof draft.locator !== "string" || draft.locator.trim() === "") {
    invalid(`${where}.draft.locator must be a non-empty string`);
  }
  if (!isPlainObject(draft.scope)) invalid(`${where}.draft.scope must be an object`);
  if (typeof draft.body !== "string") invalid(`${where}.draft.body must be a string`);
  if (draft.metadata !== undefined && !isPlainObject(draft.metadata)) {
    invalid(`${where}.draft.metadata must be an object`);
  }
  if (draft.state !== "active" && draft.state !== "retired") invalid(`${where}.draft.state is invalid`);
  if (m.expected_version_id !== null && typeof m.expected_version_id !== "string") {
    invalid(`${where}.expected_version_id must be null or a string`);
  }
  if (!Array.isArray(m.predecessors)) invalid(`${where}.predecessors must be an array`);
  for (const [i, ref] of (m.predecessors as unknown[]).entries()) {
    assertVersionRef(ref, `${where}.predecessors[${i}]`);
  }
  if (!CONTENT_CHANGE_KINDS.has(m.change_kind as ChangeKind)) invalid(`${where}.change_kind is invalid`);
}

/**
 * Prove a `ChangeCommit` is structurally well formed.
 *
 * This is the same shape the contract defines; it is deliberately conservative
 * (extra unknown keys are tolerated, missing/invalid required ones are
 * refused). It never inspects storage.
 */
export function assertChangeCommitShape(commit: ChangeCommit): void {
  if (!isPlainObject(commit)) invalid("commit must be an object");
  const c = commit as unknown as Record<string, unknown>;
  if (!isNonEmptyString(c.operation_id)) invalid("operation_id must be a non-empty string");
  if (c.actor !== "user" && c.actor !== "model" && c.actor !== "program") invalid("actor must be user|model|program");
  if (!Array.isArray(c.mutations) || c.mutations.length === 0) invalid("mutations must be a non-empty array");
  c.mutations.forEach((mutation, index) => assertMutationShape(mutation, index));
}

/**
 * Caller-supplied options for the store commit entry.
 *
 * `embeddings` is keyed by the mutation's `draft.locator`. A locator with no
 * embedding is written metadata+FTS only (exactly like `upsertL1` without an
 * embedding) — the store never fabricates a placeholder vector.
 */
export interface CommitContentChangesOptions {
  embeddings?: Map<string, Float32Array>;
}
