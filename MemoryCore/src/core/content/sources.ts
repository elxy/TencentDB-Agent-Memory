/**
 * Content-source persistence for L2/L3 (Core limited change A).
 *
 * A single small internal sidecar (`.metadata/content_sources.json`) holds the
 * program-provided input relationships for generated profile content. It is
 * written together with the content publication and read back by
 * `/scenario/read` and `/core/read`.
 *
 * Callers must never parse this file. It is internal Core data; the content
 * APIs expose a normalized {@link ContentSourceView} instead.
 *
 * Failure semantics (review返修A):
 *   - Missing sidecar            → no entries (the API reports `unknown`).
 *   - Read IO error / bad JSON   → throw / `unavailable` with a diagnostic.
 *     This is deliberately NOT collapsed into "empty" — an unreadable sidecar
 *     is not the same as a record that never had sources.
 *   - Write error                → throws. Callers must abort publication; a
 *     successful content write plus a stale sidecar is not allowed.
 *   - Schema violations          → throw on read (bad structure is an error).
 *   - `content_sha256` mismatch  → the API reports `unknown`, so an old sidecar
 *     can never be attached to new content bytes.
 */

import { createHash } from "node:crypto";

import type { StorageAdapter } from "../storage/adapter.js";
import { StoragePaths } from "../storage/types.js";
import type { Logger } from "../types.js";
import {
  unavailableContentSource,
  unknownContentSource,
  type BeforeCommitHook,
  type ContentSourceCommitResult,
  type ContentSourceEntry,
  type ContentSourceFile,
  type ContentSourceLayer,
  type ContentSourceScope,
  type ContentSourceView,
} from "./types.js";

/** Internal sidecar key. Callers must not read this directly. */
export const CONTENT_SOURCES_KEY = `${StoragePaths.metadataDir}content_sources.json`;

/**
 * Upper bound on the combined current + retained input id list. Keeps the
 * sidecar (and API payload) bounded; exceeding it is an explicit failure, not
 * a silent truncation, so consumers never lose sources without noticing.
 */
export const MAX_CONTENT_SOURCE_INPUT_IDS = 200;

/** Error raised for a structurally invalid or unreadable source sidecar. */
export class ContentSourceError extends Error {
  readonly code: "read_failed" | "invalid_schema" | "invalid_entry" | "write_failed";
  constructor(code: ContentSourceError["code"], message: string) {
    super(`[content-sources] ${code}: ${message}`);
    this.name = "ContentSourceError";
    this.code = code;
  }
}

function entryKey(layer: ContentSourceLayer, path: string): string {
  return `${layer}\u0000${path}`;
}

/** SHA-256 (lowercase hex) of the exact content bytes. */
export function contentSha256(content: string): string {
  return createHash("sha256").update(content, "utf-8").digest("hex");
}

const SHA256_RE = /^[0-9a-f]{64}$/;
const SCOPE_KEYS = new Set(["team_id", "agent_id", "user_id", "session_id"]);

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function validateScope(scope: unknown, where: string): void {
  if (!scope || typeof scope !== "object" || Array.isArray(scope)) {
    throw new ContentSourceError("invalid_entry", `${where}: scope must be an object`);
  }
  for (const [key, value] of Object.entries(scope as Record<string, unknown>)) {
    if (!SCOPE_KEYS.has(key)) {
      throw new ContentSourceError("invalid_entry", `${where}: unsupported scope key "${key}"`);
    }
    if (value !== undefined && value !== null && !isNonEmptyString(value)) {
      throw new ContentSourceError("invalid_entry", `${where}: scope.${key} must be a non-empty string`);
    }
  }
}

/**
 * Strictly validate one entry. Throws on any structural problem so a corrupt
 * sidecar is surfaced rather than silently filtered down to "no sources".
 */
export function assertValidContentSourceEntry(value: unknown, where = "entry"): asserts value is ContentSourceEntry {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ContentSourceError("invalid_entry", `${where}: not an object`);
  }
  const v = value as Record<string, unknown>;
  if (v.schema_version !== 1) {
    throw new ContentSourceError("invalid_entry", `${where}: schema_version must be 1`);
  }
  if (v.layer !== "l2" && v.layer !== "l3") {
    throw new ContentSourceError("invalid_entry", `${where}: layer must be "l2" or "l3"`);
  }
  if (!isNonEmptyString(v.path)) {
    throw new ContentSourceError("invalid_entry", `${where}: path must be a non-empty string`);
  }
  if (!isNonEmptyString(v.content_id)) {
    throw new ContentSourceError("invalid_entry", `${where}: content_id must be a non-empty string`);
  }
  if (v.source_kind !== "pipeline_input") {
    throw new ContentSourceError("invalid_entry", `${where}: source_kind must be "pipeline_input"`);
  }
  if (v.input_layer !== "l1" && v.input_layer !== "l2") {
    throw new ContentSourceError("invalid_entry", `${where}: input_layer must be "l1" or "l2"`);
  }
  if (!Array.isArray(v.input_ids)) {
    throw new ContentSourceError("invalid_entry", `${where}: input_ids must be an array`);
  }
  const seen = new Set<string>();
  for (const id of v.input_ids) {
    if (!isNonEmptyString(id)) {
      throw new ContentSourceError("invalid_entry", `${where}: input_ids must contain non-empty strings`);
    }
    if (seen.has(id)) {
      throw new ContentSourceError("invalid_entry", `${where}: duplicate input id "${id}"`);
    }
    seen.add(id);
  }
  const retainedSeen = new Set<string>();
  if (v.retained_input_ids !== undefined) {
    if (!Array.isArray(v.retained_input_ids)) {
      throw new ContentSourceError("invalid_entry", `${where}: retained_input_ids must be an array when present`);
    }
    for (const id of v.retained_input_ids) {
      if (!isNonEmptyString(id)) {
        throw new ContentSourceError("invalid_entry", `${where}: retained_input_ids must contain non-empty strings`);
      }
      if (seen.has(id)) {
        throw new ContentSourceError("invalid_entry", `${where}: retained input id "${id}" duplicates a current input id`);
      }
      if (retainedSeen.has(id)) {
        throw new ContentSourceError("invalid_entry", `${where}: duplicate retained input id "${id}"`);
      }
      retainedSeen.add(id);
    }
  }
  if (seen.size + retainedSeen.size > MAX_CONTENT_SOURCE_INPUT_IDS) {
    throw new ContentSourceError(
      "invalid_entry",
      `${where}: current+retained input ids exceed the ${MAX_CONTENT_SOURCE_INPUT_IDS} limit`,
    );
  }
  validateScope(v.scope, where);
  if (typeof v.updated_at_ms !== "number" || !Number.isFinite(v.updated_at_ms) || v.updated_at_ms < 0) {
    throw new ContentSourceError("invalid_entry", `${where}: updated_at_ms must be a finite, non-negative number`);
  }
  if (typeof v.content_sha256 !== "string" || !SHA256_RE.test(v.content_sha256)) {
    throw new ContentSourceError("invalid_entry", `${where}: content_sha256 must be a 64-char lowercase hex string`);
  }
}

/**
 * Parse and strictly validate the sidecar bytes.
 *
 * Callers that already distinguish "missing" (null) from "present" must do so
 * BEFORE calling this — this function assumes the content exists.
 */
export function parseContentSourceFile(raw: string): ContentSourceEntry[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new ContentSourceError("invalid_schema", `sidecar is not valid JSON (${err instanceof Error ? err.message : String(err)})`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new ContentSourceError("invalid_schema", "sidecar root must be an object");
  }
  const file = parsed as Partial<ContentSourceFile>;
  if (file.schema_version !== 1) {
    throw new ContentSourceError("invalid_schema", "sidecar schema_version must be 1");
  }
  if (!Array.isArray(file.entries)) {
    throw new ContentSourceError("invalid_schema", "sidecar entries must be an array");
  }
  const out: ContentSourceEntry[] = [];
  const keys = new Set<string>();
  for (let i = 0; i < file.entries.length; i++) {
    const entry = file.entries[i];
    assertValidContentSourceEntry(entry, `entries[${i}]`);
    const key = entryKey(entry.layer, entry.path);
    if (keys.has(key)) {
      throw new ContentSourceError("invalid_schema", `duplicate key (${entry.layer}, ${entry.path})`);
    }
    keys.add(key);
    out.push(entry);
  }
  return out;
}

/**
 * Read every persisted content-source entry.
 *
 * Missing sidecar → `[]`. Any IO/parse/schema failure throws
 * {@link ContentSourceError}; callers that only need a best-effort view should
 * use {@link readContentSourceView}, which maps that to `unavailable`.
 */
export async function readContentSourceEntries(storage: StorageAdapter): Promise<ContentSourceEntry[]> {
  let raw: string | null;
  try {
    raw = await storage.readFile(CONTENT_SOURCES_KEY);
  } catch (err) {
    throw new ContentSourceError("read_failed", err instanceof Error ? err.message : String(err));
  }
  if (raw === null || raw === "") return [];
  return parseContentSourceFile(raw);
}

/**
 * Merge `entries` into the sidecar keyed by `(layer, path)`.
 *
 * Throws {@link ContentSourceError} on any failure (invalid input, unreadable
 * existing sidecar, write IO error). A caller that observes a throw MUST NOT
 * report the content publication as complete with sources.
 */
export async function upsertContentSources(
  storage: StorageAdapter,
  entries: ContentSourceEntry[],
  logger?: Logger,
  opts?: { inheritRetained?: boolean },
): Promise<void> {
  if (entries.length === 0) return;
  const validated: ContentSourceEntry[] = [];
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    assertValidContentSourceEntry(entry, `entries[${i}]`);
    validated.push(entry);
  }

  // Reading the existing sidecar is intentionally part of the write: a corrupt
  // sidecar must abort the write rather than be silently overwritten, because
  // the caller would then publish content while discarding known relations.
  const existing = await readContentSourceEntries(storage);
  const byKey = new Map(existing.map((entry) => [entryKey(entry.layer, entry.path), entry]));
  for (const entry of validated) {
    const key = entryKey(entry.layer, entry.path);
    let merged = entry;
    if (opts?.inheritRetained) {
      // Do not let this batch's new inputs erase the previous bases: keep the
      // prior current+retained ids as `retained_input_ids` (distinct from the
      // new `input_ids`). Bounded and never fabricated.
      const prior = byKey.get(key);
      if (prior) {
        const newIds = new Set(entry.input_ids);
        const inherited = [...prior.input_ids, ...(prior.retained_input_ids ?? [])];
        const retained = Array.from(new Set(inherited.filter((id) => !newIds.has(id))));
        if (retained.length > 0) {
          if (newIds.size + retained.length > MAX_CONTENT_SOURCE_INPUT_IDS) {
            throw new ContentSourceError(
              "invalid_entry",
              `(${entry.layer}, ${entry.path}): current+retained input ids exceed the ${MAX_CONTENT_SOURCE_INPUT_IDS} limit`,
            );
          }
          merged = { ...entry, retained_input_ids: retained };
        }
      }
    }
    byKey.set(key, merged);
  }
  const file: ContentSourceFile = { schema_version: 1, entries: Array.from(byKey.values()) };
  try {
    await storage.writeFile(CONTENT_SOURCES_KEY, JSON.stringify(file, null, 2));
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    logger?.warn?.(`[content-sources] write_failed: ${message}`);
    throw new ContentSourceError("write_failed", message);
  }
}

/**
 * Remove source relations for the given `(layer, path)` keys.
 *
 * Throws on IO failure so a direct content write cannot silently leave a stale
 * relation behind. Missing sidecar is a no-op.
 */
export async function removeContentSources(
  storage: StorageAdapter,
  targets: ReadonlyArray<{ layer: ContentSourceLayer; path: string }>,
): Promise<void> {
  if (targets.length === 0) return;
  const existing = await readContentSourceEntries(storage);
  if (existing.length === 0) return;
  const drop = new Set(targets.map((t) => entryKey(t.layer, t.path)));
  const kept = existing.filter((entry) => !drop.has(entryKey(entry.layer, entry.path)));
  if (kept.length === existing.length) return;
  const file: ContentSourceFile = { schema_version: 1, entries: kept };
  try {
    await storage.writeFile(CONTENT_SOURCES_KEY, JSON.stringify(file, null, 2));
  } catch (err) {
    throw new ContentSourceError("write_failed", err instanceof Error ? err.message : String(err));
  }
}

/** How one content path's source relation changes in a content publication. */
export interface SourceSidecarMutation {
  layer: ContentSourceLayer;
  /** Path the relation currently points at. */
  from_path: string;
  /** New path for a rename; omit to retire (drop) the relation. */
  to_path?: string;
  /** New content_id (e.g. a rename keeps the identity, a rebuild gets a new one). */
  content_id?: string;
  /** SHA-256 of the bytes now published at the resulting path. */
  content_sha256?: string;
}

/**
 * Pure edit of a serialized content-sources sidecar for retire/rename/revise,
 * applied as part of the SAME content publication (no separate success/failure).
 *
 * Returns the new serialized sidecar, or `null` when no sidecar should exist
 * (nothing to edit, or every entry was removed). A present-but-corrupt sidecar
 * throws: it must fail loudly rather than be silently dropped while new content
 * is published.
 */
export function applySourceSidecarMutations(
  raw: string | null,
  mutations: SourceSidecarMutation[],
  nowMs: number,
): string | null {
  if (raw === null || raw === "") return null;
  const entries = parseContentSourceFile(raw);
  if (entries.length === 0) return null;

  const working = entries.slice();
  for (const mutation of mutations) {
    const index = working.findIndex(
      (entry) => entry.layer === mutation.layer && entry.path === mutation.from_path,
    );
    if (index < 0) continue;
    if (mutation.to_path === undefined) {
      working.splice(index, 1);
      continue;
    }
    const previous = working[index];
    working[index] = {
      ...previous,
      path: mutation.to_path,
      content_id: mutation.content_id ?? previous.content_id,
      content_sha256: mutation.content_sha256 ?? previous.content_sha256,
      updated_at_ms: nowMs,
    };
  }

  if (working.length === 0) return null;
  const file: ContentSourceFile = { schema_version: 1, entries: working };
  return JSON.stringify(file, null, 2);
}

/**
 * A program-provided source relation for one L2/L3 output, supplied to the
 * write planner. `input_ids` MUST be the ids this run actually read (or the
 * retained ids it inherits) — the planner never invents a relation and never
 * widens one to "the whole index".
 */
export interface PlannedContentSource {
  layer: ContentSourceLayer;
  /** Sidecar addressing form: `scene_blocks/a.md`-relative path or `persona.md`. */
  path: string;
  /** Store/profile stable id of the generated content. */
  content_id: string;
  input_layer: "l1" | "l2";
  /** Empty explicitly clears this output’s source relation; omission is not a clear. */
  input_ids: string[];
  retained_input_ids?: string[];
  scope?: ContentSourceScope;
  /**
   * Optional SHA-256 (lowercase hex) the caller believes the published bytes
   * have. When present it must equal the staged bytes' hash — the planner always
   * recomputes it and refuses a stale declaration rather than rebinding it.
   */
  content_sha256?: string;
}

/**
 * Whether two entries describe the SAME source relation (everything except the
 * volatile `updated_at_ms`). Used to keep the prior `updated_at_ms` when a
 * re-supplied relation did not actually change, so an unchanged output is not
 * re-stamped with a new time on every publish.
 */
function sameSourceRelation(a: ContentSourceEntry, b: ContentSourceEntry): boolean {
  return a.layer === b.layer
    && a.path === b.path
    && a.content_id === b.content_id
    && a.source_kind === b.source_kind
    && a.input_layer === b.input_layer
    && a.content_sha256 === b.content_sha256
    && JSON.stringify(a.input_ids) === JSON.stringify(b.input_ids)
    && JSON.stringify(a.retained_input_ids ?? []) === JSON.stringify(b.retained_input_ids ?? [])
    && JSON.stringify(a.scope ?? {}) === JSON.stringify(b.scope ?? {});
}

/**
 * Build the sidecar that belongs to ONE content commit: apply the retire/rename
 * mutations derived from the commit, then merge in the program-provided source
 * relations for the outputs this commit (re)wrote.
 *
 * Pure and deterministic. `entries` must already carry the exact published
 * bytes' `content_sha256` (the write planner computes it from the staged file).
 * Returns the serialized sidecar, or `null` when no relation should exist. A
 * present-but-corrupt sidecar throws (it must fail loudly, never be silently
 * dropped while new content is published).
 */
export function planContentSourceSidecar(
  raw: string | null,
  mutations: SourceSidecarMutation[],
  entries: ContentSourceEntry[],
  nowMs: number,
): string | null {
  const mutatedRaw = raw === null || raw === "" ? null : applySourceSidecarMutations(raw, mutations, nowMs);
  const byKey = new Map<string, ContentSourceEntry>();
  if (mutatedRaw !== null) {
    for (const entry of parseContentSourceFile(mutatedRaw)) {
      byKey.set(entryKey(entry.layer, entry.path), entry);
    }
  }
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    assertValidContentSourceEntry(entry, `planned[${i}]`);
    const key = entryKey(entry.layer, entry.path);
    const existing = byKey.get(key);
    // A re-supplied, semantically identical relation keeps its prior timestamp
    // (never fabricates a modification time out of a no-op publish).
    byKey.set(key, existing && sameSourceRelation(existing, entry)
      ? { ...entry, updated_at_ms: existing.updated_at_ms }
      : entry);
  }
  if (byKey.size === 0) return null;
  const file: ContentSourceFile = { schema_version: 1, entries: Array.from(byKey.values()) };
  return JSON.stringify(file, null, 2);
}

/**
 * Look up the program-provided source view for one content item.
 *
 * - no entry / empty ids / `content_sha256` mismatch → `unknown`
 * - sidecar read or validation failure              → `unavailable` (diagnostic)
 * - otherwise                                       → `known`
 *
 * `contentSha256` should be the SHA-256 of the exact bytes the API just read,
 * computed in the same scope as the storage view used here.
 */
export async function readContentSourceView(
  storage: StorageAdapter,
  layer: ContentSourceLayer,
  path: string,
  contentSha256Hex?: string,
): Promise<ContentSourceView> {
  let entries: ContentSourceEntry[];
  try {
    entries = await readContentSourceEntries(storage);
  } catch (err) {
    return unavailableContentSource(err instanceof Error ? err.message : String(err));
  }
  const found = entries.find((entry) => entry.layer === layer && entry.path === path);
  if (!found || found.input_ids.length === 0) return unknownContentSource();
  if (contentSha256Hex !== undefined && found.content_sha256 !== contentSha256Hex) {
    // Content changed under the sidecar — never claim the old relation.
    return unknownContentSource();
  }
  return {
    status: "known",
    kind: found.source_kind,
    input_layer: found.input_layer,
    input_ids: found.input_ids,
    ...(found.retained_input_ids && found.retained_input_ids.length > 0
      ? { retained_input_ids: found.retained_input_ids }
      : {}),
    updated_at_ms: found.updated_at_ms,
  };
}

/** Convenience: compute the hash from the content string and read the view. */
export async function readContentSourceViewForContent(
  storage: StorageAdapter,
  layer: ContentSourceLayer,
  path: string,
  content: string,
): Promise<ContentSourceView> {
  return readContentSourceView(storage, layer, path, contentSha256(content));
}

/**
 * Build the generator `beforeCommit(view, result)` hook that persists the
 * program-provided input scope for L2/L3 content BEFORE the generator commits.
 *
 * Lives here (not in pipeline-factory) so it stays a lightweight, dependency-
 * free contract that both the generator and tests can import without pulling
 * in store backends.
 *
 * Integration contract (父代理 / 可靠性生成器):
 *   - `result.changed_outputs` MUST contain only the outputs this generation
 *     actually changed (L2: only changed scene files; L3: the persona), each
 *     with the exact final bytes to be published.
 *   - `result.input_ids` MUST be only the ids actually read into this run's
 *     prompt (L2: this run's L1 ids; L3: the changed scene filenames — NOT the
 *     whole scene index).
 *   - The generator MUST run the hook inside the same working view it commits
 *     from, and MUST NOT publish if the hook throws.
 *
 * Throws on any write failure; never falls back to a best-effort post-publish
 * sidecar write. `inheritRetained` keeps prior bases as `retained_input_ids`.
 */
export function buildContentSourcesBeforeCommit(logger?: Logger) {
  return async ({ view, result }: { view?: StorageAdapter; result?: unknown }) => {
    const detail = result as ContentSourceCommitResult;
    if (!view || !detail || !Array.isArray(detail.changed_outputs)) throw new ContentSourceError("invalid_entry", "Missing before-commit content result");
    if (detail.changed_outputs.length === 0 || detail.input_ids.length === 0) return;
    const now = Date.now();
    const entries: ContentSourceEntry[] = detail.changed_outputs.map((output) => ({
      schema_version: 1,
      layer: detail.layer,
      path: output.path,
      content_id: output.content_id,
      source_kind: "pipeline_input",
      input_layer: detail.input_layer,
      input_ids: detail.input_ids,
      scope: detail.scope,
      updated_at_ms: now,
      content_sha256: contentSha256(output.content),
    }));
    await upsertContentSources(view, entries, logger, { inheritRetained: true });
  };
}

// Re-export the commit contract types for pipeline/generator consumers.
export type { ContentSourceCommitResult };
