/**
 * local-published-storage.ts — crash-safe local publishing via an immutable
 * content bundle plus a single atomic pointer.
 *
 * Why not "two renames": the previous implementation moved the live directory
 * aside and then moved the staged directory in. That leaves a window where the
 * live directory is absent, and a crash between the two renames plus a buggy
 * recovery could delete the only complete copy. It also published the scene
 * directory, the scene index and persona as three independent operations, so a
 * failure midway left a mixed old/new cognition set.
 *
 * Instead every successful generation builds one complete, immutable bundle
 * (scene_blocks/ + persona.md + .metadata/scene_index.json +
 * .metadata/content_sources.json) under
 * `<root>/.generation-publish/bundles/<bundleId>/`, validates it, and then
 * switches a single pointer file (`<root>/.generation-publish/CURRENT`) with an
 * atomic tmp+rename. Readers resolve content keys through the pointer, so they
 * always see either the complete old bundle or the complete new one:
 *
 *   - crash before the pointer rename → pointer still names the old bundle;
 *   - crash after  the pointer rename → pointer names the complete new bundle;
 *   - two writers → a single local lock plus a revision compare make the loser
 *     fail loudly instead of overwriting the winner's baseline.
 *
 * Boundaries (explicit, never silently unsafe):
 *   - local filesystem only. COS / rowfs / mongofs cannot publish an immutable
 *     bundle atomically and are rejected at generation start by the caller.
 *   - old bundles are never deleted here: a reader may still hold a pointer and
 *     we do not yet track reader lifetimes. Cleanup is out of scope this round.
 *   - this is not a general-purpose transactional filesystem. It provides the
 *     single atomic pointer switch above, nothing more.
 *
 * All control-plane paths (pointer, lock, markers, bundle files) go through the
 * shared `resolveSafeLocalPath` so a symlinked component is rejected rather than
 * followed. Unexpected IO errors propagate; only ENOENT means "absent".
 */

import fs from "node:fs/promises";
import type { Dirent } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { resolveSafeLocalPath } from "./safe-local-path.js";
import { pageEntries } from "./list-page.js";
import { L3_DOCUMENTS, StoragePaths } from "./types.js";
import type {
  IStorageBackend,
  ListEntry,
  ListObjectsOptions,
  ListResult,
  StorageObject,
} from "./types.js";
import type {
  ChangeKind,
  HistoryLayer,
  HistoryScope,
  VersionRef,
} from "../content/history-contract.js";

export interface LocalPublishLogger {
  debug?: (message: string) => void;
  info?: (message: string) => void;
  warn?: (message: string) => void;
  error?: (message: string) => void;
}

/** Test/injection hooks for the publish path. */
export interface LocalPublishHooks {
  /** After the complete new bundle is written + verified, before the lock/switch. */
  onStaged?: () => Promise<void> | void;
  /** Immediately before the atomic pointer rename (lock held). */
  onBeforeSwap?: () => Promise<void> | void;
}

/** One file in a published bundle. `key` is storage-root-relative. */
export interface BundleFile {
  key: string;
  content: Buffer;
}

export interface BundlePointer {
  bundleId: string;
  revision: number;
  /**
   * Present once this root has history enabled. Its only job is to mark that
   * content commits must carry an explicit history context; the formal version
   * chain itself lives in the bundle manifests (reachable from CURRENT).
   */
  history?: { schema_version: 1 };
}

/** On-disk schema version of the per-bundle history manifest section. */
export const HISTORY_SCHEMA_VERSION = 1 as const;

/**
 * Where one version's body bytes live. The body is deliberately NOT stored in
 * the change record: it is the bundle file at `locator` inside `bundle_id`
 * (bodies are reused from their original bundle, never copied into every later
 * bundle). A `retire` version reuses its predecessor's body reference rather
 * than duplicating the bytes, so its `locator` may differ from the version's
 * own current `locator`.
 */
export interface VersionBodyRef {
  /** Bundle that holds this version's body bytes. Immutable; never rewritten. */
  bundle_id: string;
  /** Content key of the body file inside `bundle_id`. */
  locator: string;
  /** Byte length of the body file. Re-checked against disk on read. */
  size: number;
  /** SHA-256 (lowercase hex) of the body bytes. Re-checked on read. */
  sha256: string;
}

/**
 * One immutable version record as stored inside the bundle that created it.
 *
 * The body itself is not stored here — only a validated `body_ref` pointing at
 * the immutable bundle that carries the bytes.
 */
export interface StoredVersion {
  ref: VersionRef;
  layer: HistoryLayer;
  content_id: string;
  locator: string;
  scope: HistoryScope;
  metadata: Record<string, unknown>;
  state: "active" | "retired";
  recorded_at: string;
  sequence: number;
  predecessors: VersionRef[];
  change_kind: ChangeKind;
  history_complete: boolean;
  /** Where this version's body bytes live (the bundle that created them). */
  body_ref: VersionBodyRef | null;
}

/** Current identity snapshot for one content item, snapshotted per bundle. */
export interface ContentIndexRecord {
  content_id: string;
  layer: HistoryLayer;
  locator: string;
  scope: HistoryScope;
  state: "active" | "retired";
  current_version_id: string;
  metadata: Record<string, unknown>;
  updated_at: string;
  updated_sequence: number;
  /** Whether this identity's formal history is complete (legacy = false). */
  history_complete: boolean;
}

/** Idempotency record for one committed operation (same payload → same result). */
export interface OperationRecord {
  operation_id: string;
  payload_hash: string;
  /** Optional client-request identity on the authoritative operation, not content metadata. */
  request_fingerprint?: string | null;
  /** Operation ids are unique inside their scope; the scope routes idempotency. */
  scope: HistoryScope;
  sequence: number;
  changed: boolean;
  version_refs: VersionRef[];
}

/**
 * The history section embedded in one bundle manifest.
 *
 * It carries ONLY: this commit's change records, the current content identity
 * snapshot after this commit, and this commit's operation records. It never
 * accumulates the whole history — the chain is walked through `prev_bundle_id`,
 * and only the CURRENT-reachable chain counts as formal history.
 */
export interface BundleHistoryManifest {
  schema_version: 1;
  sequence: number;
  recorded_at: string;
  prev_bundle_id: string | null;
  history_complete: boolean;
  scope: HistoryScope | null;
  changes: StoredVersion[];
  contents: Record<string, ContentIndexRecord>;
  operations: OperationRecord[];
}

/** Parsed bundle manifest (content files + optional history section). */
export interface BundleManifestRecord {
  bundleId: string;
  revision: number;
  createdAtMs: number;
  files: Array<{ key: string; size: number }>;
  history?: BundleHistoryManifest;
}

/** Context handed to a history commit hook, evaluated with the publish lock held. */
export interface ContentHistoryCommitContext {
  rootDir: string;
  /** Bundle id reserved for this commit (safe to reference in change records). */
  bundleId: string;
  currentRevision: number;
  currentBundleId: string | null;
  /** History manifest of the bundle CURRENT points at (null on first enable). */
  prevHistory: BundleHistoryManifest | null;
  /** Clock reading taken inside the lock (recorded_at is never caller-supplied). */
  now: Date;
  /** Read any reachable bundle's history manifest (validated, symlink-safe). */
  readBundleHistory: (bundleId: string) => Promise<BundleHistoryManifest | null>;
}

export interface ContentHistoryCommitOutcome {
  /** Full content file set to publish (content keys only). */
  files: BundleFile[];
  /** Additional program-owned bundle files (optional). */
  extraFiles?: BundleFile[];
  history: BundleHistoryManifest;
  result?: unknown;
}

export interface ContentHistoryNoop {
  noop: true;
  result?: unknown;
}

export type ContentHistoryCommitHook = (
  ctx: ContentHistoryCommitContext,
) => Promise<ContentHistoryCommitOutcome | ContentHistoryNoop> | ContentHistoryCommitOutcome | ContentHistoryNoop;

/**
 * A program-internal history planner for a generation workspace.
 *
 * Unlike the raw {@link ContentHistoryCommitHook} it is handed the workspace's
 * FINAL staged content file set, so the generator never has to hand-assemble a
 * history manifest: it supplies the commit (and, optionally, the program-provided
 * source relations) and the planner derives the version records, the source
 * sidecar and the `metadata.content_sources` from the actual bytes that are
 * about to be published. The workspace still verifies the planner's returned
 * file set against the staged bytes, so a planner can never ship a version that
 * does not describe the live body.
 */
export interface GenerationHistoryPlan {
  plan(
    ctx: ContentHistoryCommitContext,
    files: BundleFile[],
  ):
    | Promise<ContentHistoryCommitOutcome | ContentHistoryNoop>
    | ContentHistoryCommitOutcome
    | ContentHistoryNoop;
}

export interface CommitBundleHistoryOptions {
  build: ContentHistoryCommitHook;
  clock?: () => Date;
  /**
   * Explicitly enable history on this commit. Only LocalContentHistory's enable
   * path sets this; a normal content commit may never silently turn history on.
   */
  enable?: boolean;
}

export interface CommitBundleOptions {
  logger?: LocalPublishLogger;
  hooks?: LocalPublishHooks;
  /**
   * Pointer revision the caller built against. When provided the commit fails
   * (without touching the pointer) if the on-disk revision changed — i.e. a
   * concurrent writer committed in the meantime.
   */
  expectedRevision?: number;
  /**
   * History context. When the root already has history enabled this becomes
   * mandatory — a content commit without it is refused with
   * {@link HistoryContextRequiredError}, so no legacy entry point can bypass
   * the formal version chain.
   */
  history?: CommitBundleHistoryOptions;
}

export interface CommitBundleResult extends BundlePointer {
  /** Value returned by the history hook (e.g. a ChangeCommitResult). */
  historyResult?: unknown;
  /** True when the history hook declined the commit: nothing was published. */
  noop?: boolean;
}

/** Raised when a content commit would bypass an already-enabled history chain. */
export class HistoryContextRequiredError extends Error {
  readonly code = "history_context_required";
  constructor(message: string) {
    super(message);
    this.name = "HistoryContextRequiredError";
  }
}

const TAG = "[storage][local-publish]";
const PUBLISH_DIR = ".generation-publish";
const BUNDLES_DIR = "bundles";
const POINTER_FILE = "CURRENT";
const LOCK_DIR = "LOCK";
const LOCK_OWNER = "owner.json";
const MANIFEST_FILE = "bundle-manifest.json";
const STALE_LOCK_MS = 5 * 60 * 1000;

/** Internal metadata that ships inside a bundle (program-written, not model). */
export const CONTENT_SOURCES_KEY = ".metadata/content_sources.json";

/**
 * Keys whose authoritative bytes live in the content bundle. Everything else
 * (L0/L1 JSONL, checkpoint, backups, discovery bookkeeping) stays outside and
 * is read/written directly under the storage root.
 */
export function isBundleContentKey(key: string): boolean {
  if ((L3_DOCUMENTS as readonly string[]).includes(key)) return true;
  if (key === StoragePaths.sceneIndex) return true;
  if (key === CONTENT_SOURCES_KEY) return true;
  return key.startsWith(StoragePaths.sceneBlocksDir);
}

/**
 * Whether a list prefix reaches into (or partially spells) bundle content.
 * The empty prefix deliberately returns false: a universal listing must not
 * silently drop the non-bundle keys.
 */
export function isBundleContentPrefix(prefix: string): boolean {
  if (!prefix) return false;
  const dir = StoragePaths.sceneBlocksDir;
  if (prefix.startsWith(dir) || dir.startsWith(prefix)) return true;
  for (const doc of L3_DOCUMENTS) {
    if (doc.startsWith(prefix)) return true;
  }
  if (StoragePaths.sceneIndex.startsWith(prefix)) return true;
  if (CONTENT_SOURCES_KEY.startsWith(prefix)) return true;
  return false;
}

/**
 * Read the public local-publish root of a backend view, if it has one.
 * Capability check only — no private fields, no type guessing.
 */
export function getLocalPublishRootOf(backend: IStorageBackend): string | null {
  const fn = (backend as unknown as { getLocalPublishRoot?: () => string }).getLocalPublishRoot;
  if (typeof fn !== "function") return null;
  const root = fn.call(backend);
  return typeof root === "string" && root.length > 0 ? root : null;
}

function bundleDir(rootDir: string, bundleId: string): string {
  if (!bundleId || bundleId.includes("/") || bundleId.includes("\\") || bundleId.includes("..")) {
    throw new Error(`${TAG} invalid bundle id ${JSON.stringify(bundleId)}`);
  }
  return resolveSafeLocalPath(rootDir, `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}`);
}

function pointerPath(rootDir: string): string {
  return resolveSafeLocalPath(rootDir, `${PUBLISH_DIR}/${POINTER_FILE}`);
}

function lockPath(rootDir: string): string {
  return resolveSafeLocalPath(rootDir, `${PUBLISH_DIR}/${LOCK_DIR}`);
}

/** Read the current pointer. ENOENT → null; every other IO error propagates. */
export async function readCurrentPointer(rootDir: string): Promise<BundlePointer | null> {
  const abs = pointerPath(path.resolve(rootDir));
  let raw: string;
  try {
    raw = await fs.readFile(abs, "utf-8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw err;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`${TAG} corrupt publish pointer at ${abs}`);
  }
  const obj = parsed as { bundleId?: unknown; revision?: unknown; history?: unknown };
  if (typeof obj.bundleId !== "string" || !obj.bundleId) {
    throw new Error(`${TAG} corrupt publish pointer at ${abs}: missing bundleId`);
  }
  if (!/^[A-Za-z0-9_-]+$/.test(obj.bundleId) || !Number.isSafeInteger(obj.revision) || (obj.revision as number) < 1) throw new Error(`${TAG} invalid publish pointer at ${abs}`);
  let history: { schema_version: 1 } | undefined;
  if (obj.history !== undefined) {
    if (
      !obj.history ||
      typeof obj.history !== "object" ||
      Array.isArray(obj.history) ||
      (obj.history as { schema_version?: unknown }).schema_version !== HISTORY_SCHEMA_VERSION
    ) {
      throw new Error(`${TAG} invalid publish pointer at ${abs}: bad history marker`);
    }
    history = { schema_version: 1 };
  }
  return { bundleId: obj.bundleId, revision: obj.revision as number, ...(history ? { history } : {}) };
}

async function acquireLock(rootDir: string): Promise<() => Promise<void>> {
  const abs = lockPath(rootDir);
  await fs.mkdir(path.dirname(abs), { recursive: true });
  const ownerPath = resolveSafeLocalPath(rootDir, `${PUBLISH_DIR}/${LOCK_DIR}/${LOCK_OWNER}`);
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await fs.mkdir(abs, { recursive: false });
      await fs.writeFile(
        ownerPath,
        JSON.stringify({ pid: process.pid, startedAtMs: Date.now() }),
        "utf-8",
      );
      return async () => {
        await fs.rm(abs, { recursive: true, force: true });
      };
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
      let stale = false;
      try {
        const owner = JSON.parse(await fs.readFile(ownerPath, "utf-8")) as { pid?: unknown };
        if (!Number.isSafeInteger(owner.pid) || (owner.pid as number) < 1) throw new Error("Invalid publish lock owner");
        try { process.kill(owner.pid as number, 0); } catch (probe) { if ((probe as NodeJS.ErrnoException).code === "ESRCH") stale = true; else throw probe; }
      } catch (ownerError) {
        // Missing owner may be a writer between mkdir and write: never steal that lock.
        if ((ownerError as NodeJS.ErrnoException).code !== "ENOENT") throw ownerError;
      }
      if (stale && attempt === 0) {
        await fs.rm(abs, { recursive: true, force: true });
        continue;
      }
      throw new Error(`${TAG} concurrent local publish in progress (lock ${abs})`);
    }
  }
  throw new Error(`${TAG} could not acquire local publish lock at ${abs}`);
}

async function readRawFile(absPath: string, key: string): Promise<StorageObject | null> {
  let content: Buffer;
  let stat;
  try {
    [content, stat] = await Promise.all([fs.readFile(absPath), fs.stat(absPath)]);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw err;
  }
  return { key, content, lastModified: stat.mtime, size: stat.size };
}

/**
 * Resolve the base directory (relative to `rootDir`) that currently holds
 * content. Empty string = the legacy root (no pointer yet). A pointer that
 * references a missing/non-directory bundle throws (never reads as empty).
 */
async function currentContentBase(rootDir: string): Promise<string> {
  const pointer = await readCurrentPointer(rootDir);
  if (!pointer) return "";
  return bundleBaseRel(rootDir, pointer);
}

/** Validate that a pointer names a present bundle directory; return its rel path. */
async function bundleBaseRel(rootDir: string, pointer: BundlePointer): Promise<string> {
  const rel = `${PUBLISH_DIR}/${BUNDLES_DIR}/${pointer.bundleId}`;
  const abs = resolveSafeLocalPath(rootDir, rel);
  let stat;
  try {
    stat = await fs.stat(abs);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`${TAG} published bundle missing for pointer ${pointer.bundleId}: ${abs}`);
    }
    throw err;
  }
  if (!stat.isDirectory()) {
    throw new Error(`${TAG} published bundle is not a directory: ${abs}`);
  }
  return rel;
}

async function readAtBase(rootDir: string, baseRel: string, key: string): Promise<StorageObject | null> {
  const abs = resolveSafeLocalPath(rootDir, baseRel ? `${baseRel}/${key}` : key);
  return readRawFile(abs, key);
}

/** Read one content key through the current pointer. ENOENT → null. */
export async function readPublishedObject(rootDir: string, key: string): Promise<StorageObject | null> {
  if (!isBundleContentKey(key)) {
    throw new Error(`${TAG} readPublishedObject called for non-content key ${JSON.stringify(key)}`);
  }
  const baseRel = await currentContentBase(path.resolve(rootDir));
  return readAtBase(path.resolve(rootDir), baseRel, key);
}

/** Collect every file currently in the published content set. */
export async function collectPublishedContent(rootDir: string): Promise<Map<string, Buffer>> {
  const root = path.resolve(rootDir);
  const baseRel = await currentContentBase(root);
  return collectContentAtBase(root, baseRel);
}

/** Collect content from an explicit base (bundle rel dir or "" for legacy root). */
async function collectContentAtBase(root: string, baseRel: string): Promise<Map<string, Buffer>> {
  const relBase = baseRel ? `${baseRel}/` : "";
  const out = new Map<string, Buffer>();

  // scene_blocks/**/*.md (recursive)
  const blocksAbs = resolveSafeLocalPath(root, `${relBase}${StoragePaths.sceneBlocksDir}`);
  const blockFiles: Array<{ abs: string; key: string }> = [];
  await walkFiles(blocksAbs, (abs, rel) => {
    if (rel.endsWith(".md")) {
      blockFiles.push({ abs, key: `${StoragePaths.sceneBlocksDir}${rel}` });
    }
  });
  for (const file of blockFiles) {
    out.set(file.key, await fs.readFile(file.abs));
  }

  for (const key of [...L3_DOCUMENTS, StoragePaths.sceneIndex, CONTENT_SOURCES_KEY]) {
    const abs = resolveSafeLocalPath(root, `${relBase}${key}`);
    try {
      out.set(key, await fs.readFile(abs));
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    }
  }
  return out;
}

/**
 * A revision pinned at open time. Every read made through this handle resolves
 * against the same bundle, so a single API request cannot read content from one
 * revision and sources from another.
 */
export interface PublishedSnapshot {
  revision: number;
  read(key: string): Promise<StorageObject | null>;
  list(prefix: string, opts?: ListObjectsOptions): Promise<ListResult>;
  /** All bundle content at the pinned revision. */
  listAll(): Promise<Map<string, Buffer>>;
}

/** Pin the current pointer once; reads never re-resolve it. */
export async function openPublishedSnapshot(rootDir: string): Promise<PublishedSnapshot> {
  const root = path.resolve(rootDir);
  const pointer = await readCurrentPointer(root);
  const baseRel = pointer ? await bundleBaseRel(root, pointer) : "";
  return {
    revision: pointer?.revision ?? 0,
    read: (key: string) => readAtBase(root, baseRel, key),
    list: async (prefix: string, opts?: ListObjectsOptions) =>
      listFromContent(await collectContentAtBase(root, baseRel), prefix, opts),
    listAll: () => collectContentAtBase(root, baseRel),
  };
}

/** Open a pinned snapshot through a backend capability, or null if unsupported. */
export async function openPublishedSnapshotOf(
  backend: IStorageBackend,
): Promise<PublishedSnapshot | null> {
  const fn = (backend as unknown as { openPublishedSnapshot?: () => Promise<PublishedSnapshot> })
    .openPublishedSnapshot;
  if (typeof fn !== "function") return null;
  return fn.call(backend);
}

/** Walk regular files under `dir` (no symlinks), yielding dir-relative keys. */
async function walkFiles(
  dir: string,
  visit: (abs: string, relKey: string) => void,
  relKey = "",
): Promise<void> {
  let entries: Dirent[];
  try {
    entries = await fs.readdir(dir, { withFileTypes: true });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
    throw err;
  }
  for (const entry of entries) {
    const childRel = relKey ? `${relKey}/${entry.name}` : entry.name;
    const childAbs = resolveSafeLocalPath(dir, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`${TAG} symlink in published scene set`);
    if (entry.isDirectory()) {
      await walkFiles(childAbs, visit, childRel);
    } else if (entry.isFile()) {
      visit(childAbs, childRel);
    }
  }
}

/** Verify every manifest entry exists at the recorded size. */
async function verifyBundle(rootDir: string, bundleId: string, files: BundleFile[]): Promise<void> {
  for (const file of files) {
    const abs = resolveSafeLocalPath(
      rootDir,
      `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}/${file.key}`,
    );
    let stat;
    try {
      stat = await fs.stat(abs);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        throw new Error(`${TAG} bundle verification failed: missing ${file.key}`);
      }
      throw err;
    }
    if (stat.size !== file.content.length) {
      throw new Error(
        `${TAG} bundle verification failed: size mismatch for ${file.key} ` +
          `(on-disk=${stat.size}, expected=${file.content.length})`,
      );
    }
  }
}

/** Validate a content file set (keys must be content keys, unique, safe paths). */
function validateBundleFiles(root: string, files: BundleFile[]): void {
  const seen = new Set<string>();
  for (const file of files) {
    if (!file || typeof file.key !== "string") {
      throw new Error(`${TAG} invalid bundle file entry`);
    }
    if (!isBundleContentKey(file.key)) {
      throw new Error(`${TAG} refusing to publish non-content key ${JSON.stringify(file.key)}`);
    }
    if (seen.has(file.key)) {
      throw new Error(`${TAG} duplicate bundle key ${JSON.stringify(file.key)}`);
    }
    seen.add(file.key);
    // Reject unsafe keys eagerly (throws UnsafeLocalPathError).
    resolveSafeLocalPath(root, file.key);
  }
}

/** Write one bundle file via tmp+rename so a crash never leaves a partial file. */
async function writeBundleFile(root: string, bundleId: string, file: BundleFile): Promise<void> {
  const abs = resolveSafeLocalPath(root, `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}/${file.key}`);
  await fs.mkdir(path.dirname(abs), { recursive: true });
  const tmp = resolveSafeLocalPath(root, `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}/${file.key}.tmp`);
  await fs.writeFile(tmp, file.content);
  await fs.rename(tmp, abs);
}

async function writeBundleManifest(root: string, bundleId: string, manifest: BundleManifestRecord): Promise<void> {
  await fs.writeFile(
    resolveSafeLocalPath(root, `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}/${MANIFEST_FILE}`),
    JSON.stringify(manifest),
    "utf-8",
  );
}

/** Atomic pointer switch (tmp+rename). The only moment a revise becomes visible. */
async function switchPointer(root: string, nextPointer: BundlePointer): Promise<void> {
  const absPointer = pointerPath(root);
  const tmpPointer = resolveSafeLocalPath(root, `${PUBLISH_DIR}/${POINTER_FILE}.tmp-${randomUUID()}`);
  await fs.mkdir(path.dirname(absPointer), { recursive: true });
  await fs.writeFile(tmpPointer, JSON.stringify(nextPointer), "utf-8");
  await fs.rename(tmpPointer, absPointer);
}

const HISTORY_SCOPE_KEYS = new Set(["team_id", "user_id", "agent_id", "task_id", "session_id"]);
const HISTORY_CHANGE_KINDS = new Set<ChangeKind>([
  "baseline", "create", "revise", "retire", "restore",
]);
const SAFE_ID_RE = /^[A-Za-z0-9_-]+$/;
const SHA256_RE = /^[0-9a-f]{64}$/;

function isHistoryNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

/** A scope is either absent (null) or an object of known, non-empty string keys. */
function isValidHistoryScope(value: unknown): value is HistoryScope {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
    if (!HISTORY_SCOPE_KEYS.has(key)) return false;
    if (val !== undefined && val !== null && !(typeof val === "string" && val.length > 0)) return false;
  }
  return true;
}

function isValidVersionRef(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const ref = value as Record<string, unknown>;
  if (typeof ref.version_id !== "string" || !SAFE_ID_RE.test(ref.version_id)) return false;
  if (!isHistoryNonEmptyString(ref.content_id)) return false;
  if (ref.layer !== "l0" && ref.layer !== "l1" && ref.layer !== "l2" && ref.layer !== "l3") return false;
  return true;
}

function isValidBodyRef(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const body = value as Record<string, unknown>;
  if (typeof body.bundle_id !== "string" || !SAFE_ID_RE.test(body.bundle_id)) return false;
  if (!isHistoryNonEmptyString(body.locator)) return false;
  if (!Number.isSafeInteger(body.size) || (body.size as number) < 0) return false;
  if (typeof body.sha256 !== "string" || !SHA256_RE.test(body.sha256)) return false;
  return true;
}

/**
 * Validate a history manifest loaded from disk. A bad manifest, chain link,
 * scope, identity, state or checksum reference must fail loudly — never be
 * papered over with the current content.
 */
export function assertValidBundleHistory(value: unknown, where: string): asserts value is BundleHistoryManifest {
  const fail = (msg: string): never => {
    throw new Error(`${TAG} invalid history manifest (${where}): ${msg}`);
  };
  if (!value || typeof value !== "object" || Array.isArray(value)) fail("not an object");
  const h = value as Record<string, unknown>;
  if (h.schema_version !== HISTORY_SCHEMA_VERSION) fail("bad schema_version");
  if (!Number.isSafeInteger(h.sequence) || (h.sequence as number) < 0) fail("bad sequence");
  if (typeof h.recorded_at !== "string" || h.recorded_at.length === 0) fail("bad recorded_at");
  if (h.prev_bundle_id !== null && (typeof h.prev_bundle_id !== "string" || !SAFE_ID_RE.test(h.prev_bundle_id))) {
    fail("bad prev_bundle_id");
  }
  if (typeof h.history_complete !== "boolean") fail("bad history_complete");
  if (h.scope !== null && !isValidHistoryScope(h.scope)) fail("bad scope");
  if (!Array.isArray(h.changes)) fail("changes must be an array");
  if (!h.contents || typeof h.contents !== "object" || Array.isArray(h.contents)) fail("contents must be an object");
  if (!Array.isArray(h.operations)) fail("operations must be an array");

  for (const change of h.changes as unknown[]) {
    if (!change || typeof change !== "object" || Array.isArray(change)) fail("change must be an object");
    const c = change as Record<string, unknown>;
    const ref = c.ref as Record<string, unknown> | undefined;
    if (!ref || typeof ref !== "object") return fail("change.ref missing");
    if (typeof ref.version_id !== "string" || !SAFE_ID_RE.test(ref.version_id)) fail("bad version_id");
    if (!isHistoryNonEmptyString(ref.content_id)) fail("bad content_id");
    if (ref.layer !== "l0" && ref.layer !== "l1" && ref.layer !== "l2" && ref.layer !== "l3") fail("bad layer");
    if (c.layer !== ref.layer) fail("change layer mismatch");
    if (c.content_id !== ref.content_id) fail("change content_id mismatch");
    if (!isHistoryNonEmptyString(c.locator)) fail("bad locator");
    if (!isValidHistoryScope(c.scope)) fail("bad change scope");
    if (c.state !== "active" && c.state !== "retired") fail("bad change state");
    if (!Number.isSafeInteger(c.sequence) || (c.sequence as number) < 0) fail("bad change sequence");
    if (typeof c.recorded_at !== "string" || c.recorded_at.length === 0) fail("bad change recorded_at");
    if (!HISTORY_CHANGE_KINDS.has(c.change_kind as ChangeKind)) fail("bad change_kind");
    if (typeof c.history_complete !== "boolean") fail("bad change history_complete");
    if (!c.metadata || typeof c.metadata !== "object" || Array.isArray(c.metadata)) fail("bad change metadata");
    if (!Array.isArray(c.predecessors) || !(c.predecessors as unknown[]).every(isValidVersionRef)) {
      fail("bad change predecessors");
    }
    if (!c.body_ref || !isValidBodyRef(c.body_ref)) fail("bad body_ref");
  }

  for (const [id, record] of Object.entries(h.contents as Record<string, unknown>)) {
    if (!record || typeof record !== "object" || Array.isArray(record)) fail(`contents[${id}] must be an object`);
    const rec = record as Record<string, unknown>;
    if (rec.content_id !== id) fail(`contents[${id}].content_id must match its key`);
    if (rec.layer !== "l0" && rec.layer !== "l1" && rec.layer !== "l2" && rec.layer !== "l3") fail(`contents[${id}].layer`);
    if (!isHistoryNonEmptyString(rec.locator)) fail(`contents[${id}].locator`);
    if (!isValidHistoryScope(rec.scope)) fail(`contents[${id}].scope`);
    if (rec.state !== "active" && rec.state !== "retired") fail(`contents[${id}].state`);
    if (typeof rec.current_version_id !== "string" || !SAFE_ID_RE.test(rec.current_version_id)) fail(`contents[${id}].current_version_id`);
    if (!rec.metadata || typeof rec.metadata !== "object" || Array.isArray(rec.metadata)) fail(`contents[${id}].metadata`);
    if (typeof rec.updated_at !== "string" || !Number.isSafeInteger(rec.updated_sequence)) fail(`contents[${id}] timestamp/sequence`);
    if (typeof rec.history_complete !== "boolean") fail(`contents[${id}].history_complete`);
  }

  for (const op of h.operations as unknown[]) {
    if (!op || typeof op !== "object" || Array.isArray(op)) fail("operation must be an object");
    const record = op as Record<string, unknown>;
    if (!isHistoryNonEmptyString(record.operation_id)) fail("bad operation_id");
    if (typeof record.payload_hash !== "string" || record.payload_hash.length === 0) fail("bad payload_hash");
    if (record.request_fingerprint !== undefined && record.request_fingerprint !== null
      && (typeof record.request_fingerprint !== "string" || record.request_fingerprint.trim() === "")) {
      fail("bad operation request_fingerprint");
    }
    if (!isValidHistoryScope(record.scope)) fail("bad operation scope");
    if (!Number.isSafeInteger(record.sequence) || (record.sequence as number) < 0) fail("bad operation sequence");
    if (typeof record.changed !== "boolean") fail("bad operation changed");
    if (!Array.isArray(record.version_refs) || !(record.version_refs as unknown[]).every(isValidVersionRef)) {
      fail("bad operation version_refs");
    }
  }
}

/** Read + parse a bundle's manifest. ENOENT → null; anything else throws. */
export async function readBundleManifest(rootDir: string, bundleId: string): Promise<BundleManifestRecord | null> {
  const root = path.resolve(rootDir);
  // Validates the bundle id form (no traversal) before touching the path.
  bundleDir(root, bundleId);
  const abs = resolveSafeLocalPath(root, `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}/${MANIFEST_FILE}`);
  let raw: string;
  try {
    raw = await fs.readFile(abs, "utf-8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw err;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(`${TAG} corrupt bundle manifest at ${abs}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${TAG} corrupt bundle manifest at ${abs}`);
  }
  return parsed as BundleManifestRecord;
}

/** Read a bundle's history section (validated). Missing manifest/section → null. */
export async function readBundleHistory(rootDir: string, bundleId: string): Promise<BundleHistoryManifest | null> {
  const manifest = await readBundleManifest(rootDir, bundleId);
  if (!manifest || manifest.history === undefined) return null;
  assertValidBundleHistory(manifest.history, `bundle ${bundleId}`);
  return manifest.history;
}

/**
 * Read one file directly out of an immutable bundle. Used to resolve a
 * historical version's body from the bundle that created it (never by falling
 * back to a current path). ENOENT → null.
 */
export async function readBundleFile(rootDir: string, bundleId: string, key: string): Promise<StorageObject | null> {
  const root = path.resolve(rootDir);
  bundleDir(root, bundleId);
  const abs = resolveSafeLocalPath(root, `${PUBLISH_DIR}/${BUNDLES_DIR}/${bundleId}/${key}`);
  return readRawFile(abs, key);
}

/**
 * Build, verify and atomically switch to a complete content bundle.
 *
 * The old bundle is left in place; a reader that already resolved the previous
 * pointer can still finish. No content is deleted outside the transaction of a
 * single pointer rename.
 *
 * With `opts.history` the caller supplies the formal version chain: sequence,
 * `recorded_at` and the CAS checks are resolved inside the publish lock, before
 * a single byte of the new bundle is written. A bundle staged but never pointed
 * at (crash before the pointer rename, or a hook that throws) is therefore NOT
 * part of the reachable history chain.
 */
export async function commitContentBundle(
  rootDir: string,
  files: BundleFile[],
  opts: CommitBundleOptions = {},
): Promise<CommitBundleResult> {
  const root = path.resolve(rootDir);
  validateBundleFiles(root, files);

  const release = await acquireLock(root);
  let switched = false;
  try {
    const pointerBefore = await readCurrentPointer(root);
    const currentRevision = pointerBefore?.revision ?? 0;
    if (opts.expectedRevision !== undefined && currentRevision !== opts.expectedRevision) {
      throw new Error(
        `${TAG} concurrent publish detected: expected revision ${opts.expectedRevision}, ` +
          `found ${currentRevision} — refusing to overwrite another writer's baseline`,
      );
    }

    const historyOnDisk = pointerBefore?.history !== undefined;

    if (historyOnDisk && !opts.history) {
      throw new HistoryContextRequiredError(
        `${TAG} history is enabled for this root; a content commit without an explicit history context is refused`,
      );
    }
    if (opts.history && !historyOnDisk && opts.history.enable !== true) {
      throw new HistoryContextRequiredError(
        `${TAG} history is not enabled for this root; refusing to attach a history change to a plain content commit ` +
          `(call LocalContentHistory.enableHistory first)`,
      );
    }

    const bundleId = `b_${Date.now()}_${randomUUID().slice(0, 8)}`;

    if (opts.history) {
      const clock = opts.history.clock ?? (() => new Date());
      const now = clock();
      const prevHistory = pointerBefore ? await readBundleHistory(root, pointerBefore.bundleId) : null;
      if (pointerBefore && historyOnDisk && !prevHistory) {
        throw new Error(
          `${TAG} history is enabled but the current bundle ${pointerBefore.bundleId} has no valid history manifest`,
        );
      }
      const outcome = await opts.history.build({
        rootDir: root,
        bundleId,
        currentRevision,
        currentBundleId: pointerBefore?.bundleId ?? null,
        prevHistory,
        now,
        readBundleHistory: (id: string) => readBundleHistory(root, id),
      });
      if (outcome && (outcome as ContentHistoryNoop).noop === true) {
        return {
          bundleId: pointerBefore?.bundleId ?? "__noop__",
          revision: currentRevision,
          noop: true,
          historyResult: (outcome as ContentHistoryNoop).result,
        };
      }
      const built = outcome as ContentHistoryCommitOutcome;
      if (!built || !Array.isArray(built.files) || !built.history) {
        throw new Error(`${TAG} history commit hook returned an invalid outcome`);
      }
      assertValidBundleHistory(built.history, `history commit ${bundleId}`);
      validateBundleFiles(root, built.files);
      for (const extra of built.extraFiles ?? []) {
        if (!extra || typeof extra.key !== "string") throw new Error(`${TAG} invalid extra bundle file`);
        resolveSafeLocalPath(root, extra.key);
      }
      const finalFiles = [...built.files, ...(built.extraFiles ?? [])];
      const seenKeys = new Set<string>();
      for (const f of finalFiles) {
        if (seenKeys.has(f.key)) throw new Error(`${TAG} duplicate bundle key ${JSON.stringify(f.key)}`);
        seenKeys.add(f.key);
      }

      await fs.mkdir(bundleDir(root, bundleId), { recursive: true });
      for (const file of finalFiles) await writeBundleFile(root, bundleId, file);
      await writeBundleManifest(root, bundleId, {
        bundleId,
        revision: currentRevision + 1,
        createdAtMs: Date.now(),
        files: finalFiles.map((f) => ({ key: f.key, size: f.content.length })),
        history: built.history,
      });
      await verifyBundle(root, bundleId, finalFiles);
      await opts.hooks?.onStaged?.();
      await opts.hooks?.onBeforeSwap?.();

      const nextPointer: BundlePointer = {
        bundleId,
        revision: currentRevision + 1,
        history: { schema_version: 1 },
      };
      await switchPointer(root, nextPointer);
      switched = true;
      opts.logger?.info?.(
        `${TAG} published history bundle ${bundleId} revision=${nextPointer.revision} (${finalFiles.length} files)`,
      );
      return { bundleId, revision: nextPointer.revision, historyResult: built.result };
    }

    await fs.mkdir(bundleDir(root, bundleId), { recursive: true });
    for (const file of files) await writeBundleFile(root, bundleId, file);
    await writeBundleManifest(root, bundleId, {
      bundleId,
      revision: currentRevision + 1,
      createdAtMs: Date.now(),
      files: files.map((f) => ({ key: f.key, size: f.content.length })),
    });
    await verifyBundle(root, bundleId, files);
    await opts.hooks?.onStaged?.();
    await opts.hooks?.onBeforeSwap?.();

    const nextPointer: BundlePointer = { bundleId, revision: currentRevision + 1 };
    await switchPointer(root, nextPointer);
    switched = true;
    opts.logger?.info?.(
      `${TAG} published bundle ${bundleId} revision=${nextPointer.revision} (${files.length} files)`,
    );
    return { bundleId, revision: nextPointer.revision };
  } finally {
    try { await release(); } catch (releaseError) {
      if (!switched) throw releaseError;
      console.error(`${TAG} content already committed; publish lock cleanup failed`, releaseError);
    }
  }
}

/**
 * Apply a set of single-key mutations (used by `putObject`/`deleteObject`) by
 * rebuilding the complete content set and committing it as one bundle.
 */
export async function publishContentMutation(
  rootDir: string,
  mutations: { writes?: BundleFile[]; deletes?: string[] },
  opts: CommitBundleOptions = {},
): Promise<BundlePointer> {
  // The revision current when we read the content: fail if another writer
  // committed in between instead of silently clobbering its result.
  const pointer = await readCurrentPointer(path.resolve(rootDir));
  const content = await collectPublishedContent(rootDir);
  for (const write of mutations.writes ?? []) content.set(write.key, write.content);
  for (const key of mutations.deletes ?? []) content.delete(key);
  const files: BundleFile[] = Array.from(content.entries()).map(([key, value]) => ({
    key,
    content: value,
  }));
  return commitContentBundle(rootDir, files, {
    ...opts,
    expectedRevision: opts.expectedRevision ?? (pointer?.revision ?? 0),
  });
}

/** List published content matching `prefix` (view-relative keys). */
export async function listPublishedObjects(
  rootDir: string,
  prefix: string,
  opts?: ListObjectsOptions,
): Promise<ListResult> {
  const content = await collectPublishedContent(rootDir);
  return listFromContent(content, prefix, opts);
}

function listFromContent(
  content: Map<string, Buffer>,
  prefix: string,
  opts?: ListObjectsOptions,
): ListResult {
  const recursive = opts?.recursive ?? false;

  if (recursive) {
    const entries: ListEntry[] = [];
    for (const [key, value] of content) {
      if (!key.startsWith(prefix)) continue;
      entries.push({ key, size: value.length, lastModified: new Date(), isDirectory: false });
    }
    return pageEntries(entries, opts);
  }

  const lastSlash = prefix.lastIndexOf("/");
  const dirKey = lastSlash >= 0 ? prefix.slice(0, lastSlash + 1) : "";
  const namePrefix = lastSlash >= 0 ? prefix.slice(lastSlash + 1) : prefix;
  const entries: ListEntry[] = [];
  const seenDirs = new Set<string>();
  for (const [key, value] of content) {
    if (!key.startsWith(dirKey) || !key.slice(dirKey.length).startsWith(namePrefix)) continue;
    const rest = key.slice(dirKey.length);
    const slash = rest.indexOf("/");
    if (slash < 0) {
      entries.push({ key, size: value.length, lastModified: new Date(), isDirectory: false });
      continue;
    }
    const childDir = `${dirKey}${rest.slice(0, slash + 1)}`;
    if (seenDirs.has(childDir)) continue;
    seenDirs.add(childDir);
    entries.push({ key: childDir, size: 0, lastModified: new Date(0), isDirectory: true });
  }
  return pageEntries(entries, opts);
}

/**
 * Validate/repair the published state on startup.
 *
 * With a single-pointer commit there is no half-swapped directory to repair:
 * the pointer always names a complete bundle. We only fail loudly if the
 * pointer references a missing bundle (external deletion/corruption), since
 * silently serving a stale/partial view would be worse.
 */
export async function recoverLocalPublishes(
  rootDir: string,
  logger?: LocalPublishLogger,
): Promise<void> {
  const root = path.resolve(rootDir);
  const pointer = await readCurrentPointer(root);
  if (!pointer) return;
  const dir = bundleDir(root, pointer.bundleId);
  try {
    const stat = await fs.stat(dir);
    if (!stat.isDirectory()) throw new Error(`not a directory: ${dir}`);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(
        `${TAG} publish pointer references missing bundle ${pointer.bundleId} at ${dir}`,
      );
    }
    throw err;
  }
  logger?.debug?.(`${TAG} publish pointer valid: ${pointer.bundleId} (revision ${pointer.revision})`);
}

/** Legacy helper: atomically write one published content key via a bundle. */
export async function publishLocalSingleFile(
  rootDir: string,
  key: string,
  content: string | Buffer,
  logger?: LocalPublishLogger,
): Promise<void> {
  const buf = typeof content === "string" ? Buffer.from(content, "utf-8") : content;
  await publishContentMutation(rootDir, { writes: [{ key, content: buf }] }, { logger });
}

/** Legacy helper: delete one published content key via a bundle. */
export async function deleteLocalFile(
  rootDir: string,
  key: string,
  logger?: LocalPublishLogger,
): Promise<void> {
  await publishContentMutation(rootDir, { deletes: [key] }, { logger });
}
