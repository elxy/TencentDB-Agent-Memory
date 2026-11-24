/**
 * generation-workspace.ts — restricted working view for L2/L3 generation.
 *
 * Problem this solves (parent review probes): in service mode the L2/L3 LLM
 * tools received the *published* StorageAdapter, so a model that wrote a scene
 * block / persona.md and then failed left that partial output visible to every
 * concurrent reader. On the raw-FS path the runner was pointed straight at the
 * published `scene_blocks/` directory, with the same effect.
 *
 * Design:
 *   - `GenerationWorkspace.begin()` opens a private working view over the
 *     published store (or, in raw-FS mode, a private physical staging copy).
 *   - Model/tool writes go to that view; reads fall through to the published
 *     store so the model still sees the current scene set.
 *   - `commit()` publishes the staged changes only after the caller has
 *     validated them and finished post-processing. `discard()` throws the
 *     working view away, leaving published content untouched.
 *   - `writeInternal()` lets the *program* stage metadata (e.g. scene_index)
 *     that must ship with the content. It is never exposed to the model.
 *
 * Capability boundaries (explicit, never silently unsafe):
 *   - `local` underlying backend (including a scoped `profiles/<scope>/` view) →
 *     one immutable content bundle + a single atomic pointer switch.
 *   - `cos` / `rowfs` / `mongofs` → generation is refused at `begin()`; these
 *     backends cannot publish a multi-file bundle atomically and we do not
 *     pretend a per-key write is a transaction. No formal write happens.
 *
 * The workspace deliberately does not read or write the database / checkpoint:
 * those live on the caller's base store and are handled separately. The
 * checkpoint is never part of a bundle.
 */

import fs from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { resolveSafeLocalPath } from "./safe-local-path.js";
import { StorageAdapter } from "./adapter.js";
import { pageEntries } from "./list-page.js";
import {
  commitContentBundle,
  collectPublishedContent,
  openPublishedSnapshot,
  isBundleContentKey,
  isBundleContentPrefix,
  readCurrentPointer,
  recoverLocalPublishes,
  getLocalPublishRootOf,
  CONTENT_SOURCES_KEY,
  type BundleFile,
  type ContentHistoryCommitContext,
  type ContentHistoryCommitHook,
  type ContentHistoryCommitOutcome,
  type ContentHistoryNoop,
  type GenerationHistoryPlan,
  type LocalPublishHooks,
} from "./local-published-storage.js";
import { isL3DocumentLocator, L3_DOCUMENTS, StoragePaths } from "./types.js";
import type {
  IStorageBackend,
  ListEntry,
  ListObjectsOptions,
  ListResult,
  PutObjectOptions,
  StorageObject,
} from "./types.js";
import type { Logger } from "../types.js";

const TAG = "[memory-tdai][generation-workspace]";

export type GenerationWorkspaceKind = "l2" | "l3";
export type PublishAtomicity = "local-directory-snapshot" | "per-key";

/** Context handed to the optional `beforeCommit` hook. */
export interface BeforeCommitContext {
  kind: GenerationWorkspaceKind;
  /** Storage view (undefined in raw-FS mode). */
  view: StorageAdapter | undefined;
  /** Program-only staged write of internal metadata (never exposed to the model). */
  writeInternal: (key: string, content: string | Buffer) => Promise<void>;
  /** Whatever the caller passes to `commit(result)`. */
  result?: unknown;
}

/**
 * Optional generator hook: write program-owned metadata (e.g. content source
 * sidecars) into the working view immediately before the bundle is published.
 * A throw here fails the commit without publishing anything.
 */
export type BeforeCommitCallback = (ctx: BeforeCommitContext) => Promise<void> | void;

export interface GenerationWorkspaceOptions {
  /** Published store (service mode). Undefined = raw local-FS mode. */
  storage?: StorageAdapter;
  /** Plugin/scope data directory (raw-FS published root + staging parent). */
  dataDir: string;
  /** Which generation layer this workspace belongs to. */
  kind: GenerationWorkspaceKind;
  logger?: Logger;
  /** Profile-scope label, for diagnostics only. */
  scopeLabel?: string;
  /** Test-only publish injection hooks. */
  publishHooks?: LocalPublishHooks;
  /** Optional program metadata hook run just before the atomic publish. */
  beforeCommit?: BeforeCommitCallback;
  /**
   * Formal history context for this publish. Required once the publish root has
   * history enabled (the commit is refused otherwise).
   *
   * Two program-internal, validated forms are accepted:
   *   - {@link GenerationHistoryPlan} (the preferred one): the workspace hands
   *     the planner its FINAL staged content file set, so the generator supplies
   *     only the commit (+ optional source relations) and never hand-assembles a
   *     manifest. The committed file set MUST byte-for-byte equal the staged
   *     content except for the program-owned source sidecar.
   *   - a raw {@link ContentHistoryCommitHook}: still program-internal, but the
   *     returned file set must equal the staged content exactly.
   *
   * Neither is ever exposed to the model, and neither may fabricate an input
   * version on the pipeline's behalf.
   */
  historyContext?: ContentHistoryCommitHook | GenerationHistoryPlan;
  /** Clock used to stamp the history change (tests only). */
  historyClock?: () => Date;
}

interface OverlayWrite {
  content: Buffer;
  contentType?: string;
  metadata?: Record<string, string>;
}

/**
 * Read-through / write-back overlay over a published backend.
 *
 * Reads consult the overlay first, then the base (unless tombstoned). Lists
 * merge base entries with staged writes and drop tombstones. Nothing reaches
 * the base backend until `stagedOperations()` is applied by the workspace.
 */
export class GenerationOverlayBackend implements IStorageBackend {
  readonly type: IStorageBackend["type"];
  private readonly writes = new Map<string, OverlayWrite>();
  private readonly tombstones = new Set<string>();
  private readonly base: IStorageBackend;

  constructor(base: IStorageBackend) {
    this.base = base;
    this.type = base.type;
  }

  async putObject(key: string, content: string | Buffer, opts?: PutObjectOptions): Promise<void> {
    const buf = typeof content === "string" ? Buffer.from(content, "utf-8") : Buffer.from(content);
    this.writes.set(key, { content: buf, contentType: opts?.contentType, metadata: opts?.metadata });
    this.tombstones.delete(key);
  }

  async appendObject(key: string, content: string | Buffer): Promise<void> {
    const existing = await this.getObject(key);
    const addition = typeof content === "string" ? Buffer.from(content, "utf-8") : Buffer.from(content);
    const merged = existing ? Buffer.concat([existing.content, addition]) : addition;
    this.writes.set(key, {
      content: merged,
      contentType: existing?.contentType,
      metadata: existing?.metadata,
    });
    this.tombstones.delete(key);
  }

  async getObject(key: string): Promise<StorageObject | null> {
    const staged = this.writes.get(key);
    if (staged) {
      return {
        key,
        content: Buffer.from(staged.content),
        contentType: staged.contentType,
        metadata: staged.metadata,
        size: staged.content.length,
      };
    }
    if (this.tombstones.has(key)) return null;
    return this.base.getObject(key);
  }

  async exists(key: string): Promise<boolean> {
    if (this.writes.has(key)) return true;
    if (this.tombstones.has(key)) return false;
    return this.base.exists(key);
  }

  async listObjects(prefix: string, opts?: ListObjectsOptions): Promise<ListResult> {
    const baseResult = await this.base.listObjects(prefix, {
      ...opts,
      marker: undefined,
      maxKeys: 100_000,
    });
    const merged = new Map<string, ListEntry>();
    for (const entry of baseResult.entries) {
      if (this.tombstones.has(entry.key)) continue;
      merged.set(entry.key, entry);
    }
    for (const [key, write] of this.writes) {
      if (!key.startsWith(prefix)) continue;
      merged.set(key, {
        key,
        size: write.content.length,
        lastModified: new Date(),
        isDirectory: false,
      });
    }
    return pageEntries(Array.from(merged.values()), opts);
  }

  async deleteObject(key: string): Promise<void> {
    this.writes.delete(key);
    this.tombstones.add(key);
  }

  async deleteByPrefix(prefix: string): Promise<number> {
    for (const key of [...this.writes.keys()]) {
      if (key.startsWith(prefix)) this.writes.delete(key);
    }
    let count = this.tombstones.size;
    // No broad catch: a failed base listing must fail the generation rather
    // than silently publish a tree with un-removed entries.
    const baseResult = await this.base.listObjects(prefix, { recursive: true, maxKeys: 100_000 });
    for (const entry of baseResult.entries) {
      this.tombstones.add(entry.key);
      count += 1;
    }
    return count;
  }

  getBase(): IStorageBackend {
    return this.base;
  }

  hasChanges(): boolean {
    return this.writes.size > 0 || this.tombstones.size > 0;
  }

  stagedOperations(): {
    writes: Array<{ key: string; content: Buffer; contentType?: string; metadata?: Record<string, string> }>;
    deletes: string[];
  } {
    return {
      writes: Array.from(this.writes.entries()).map(([key, write]) => ({
        key,
        content: write.content,
        contentType: write.contentType,
        metadata: write.metadata,
      })),
      deletes: Array.from(this.tombstones).filter((key) => !this.writes.has(key)),
    };
  }
}

/**
 * Staging workspace handed to one L2/L3 generation.
 *
 * Lifecycle: `begin()` → run model against `view`/`stagingDir` → validate →
 * post-process → `commit()` (or `discard()` on any failure). Exactly one of
 * commit/discard should be called; `discard()` is safe to call after commit.
 */
export class GenerationWorkspace {
  /** Staging view for StorageAdapter-mode model tools. Undefined in raw-FS mode. */
  readonly view: StorageAdapter | undefined;
  /**
   * Physical staging dir. In raw-FS mode this is the working copy of the
   * published tree; in storage mode it is the run's private tool workspace.
   */
  readonly stagingDir: string;
  /** Value the caller should use as its `dataDir` for the duration of the run. */
  readonly activeDataDir: string;
  readonly publishAtomicity: PublishAtomicity;
  readonly kind: GenerationWorkspaceKind;

  private readonly overlay: GenerationOverlayBackend | undefined;
  private readonly logger: Logger | undefined;
  private readonly publishHooks: LocalPublishHooks | undefined;
  private readonly beforeCommit: BeforeCommitCallback | undefined;
  private readonly historyContext: ContentHistoryCommitHook | GenerationHistoryPlan | undefined;
  private readonly historyClock: (() => Date) | undefined;
  /** Absolute root under which the content bundle pointer lives. */
  private readonly publishRoot: string;
  /** Pointer revision captured at `begin`; commit fails if it changed. */
  private readonly revision: number;
  private baseline = new Map<string, Buffer>();
  private committed = false;
  private discarded = false;

  private constructor(args: {
    kind: GenerationWorkspaceKind;
    view: StorageAdapter | undefined;
    overlay: GenerationOverlayBackend | undefined;
    stagingDir: string;
    activeDataDir: string;
    publishAtomicity: PublishAtomicity;
    publishRoot: string;
    revision: number;
    logger?: Logger;
    publishHooks?: LocalPublishHooks;
    beforeCommit?: BeforeCommitCallback;
    historyContext?: ContentHistoryCommitHook | GenerationHistoryPlan;
    historyClock?: () => Date;
  }) {
    this.kind = args.kind;
    this.view = args.view;
    this.overlay = args.overlay;
    this.stagingDir = args.stagingDir;
    this.activeDataDir = args.activeDataDir;
    this.publishAtomicity = args.publishAtomicity;
    this.publishRoot = args.publishRoot;
    this.revision = args.revision;
    this.logger = args.logger;
    this.publishHooks = args.publishHooks;
    this.beforeCommit = args.beforeCommit;
    this.historyContext = args.historyContext;
    this.historyClock = args.historyClock;
  }

  static async begin(opts: GenerationWorkspaceOptions): Promise<GenerationWorkspace> {
    const stagingDir = resolveSafeLocalPath(path.resolve(opts.dataDir), `.generation-workspace/gen-${Date.now()}-${randomUUID().slice(0, 8)}`);
    await fs.mkdir(stagingDir, { recursive: true });

    if (opts.storage) {
      const baseBackend = opts.storage.getBackend();
      const publishRoot = getLocalPublishRootOf(baseBackend);
      if (!publishRoot) {
        throw new Error(
          `${TAG} backend type="${baseBackend.type}" cannot publish an immutable local content bundle; ` +
            `generation refused (COS/rowfs/mongofs multi-file atomicity is not supported)`,
        );
      }
      // Validate the current pointer before we read published content.
      await recoverLocalPublishes(publishRoot, opts.logger);
      const snapshot = await openPublishedSnapshot(publishRoot);
      const revision = snapshot.revision;
      const pinned: IStorageBackend = {
        type: baseBackend.type,
        getObject: key => isBundleContentKey(key) ? snapshot.read(key) : baseBackend.getObject(key),
        listObjects: (prefix, options) => isBundleContentPrefix(prefix) ? snapshot.list(prefix, options) : baseBackend.listObjects(prefix, options),
        exists: async key => isBundleContentKey(key) ? (await snapshot.read(key)) !== null : baseBackend.exists(key),
        putObject: (...args) => baseBackend.putObject(...args),
        appendObject: (...args) => baseBackend.appendObject(...args),
        deleteObject: key => baseBackend.deleteObject(key),
        deleteByPrefix: prefix => baseBackend.deleteByPrefix(prefix),
      };
      const overlay = new GenerationOverlayBackend(pinned);
      const view = new StorageAdapter(overlay);
      opts.logger?.debug?.(
        `${TAG} opened storage workspace kind=${opts.kind} atomicity=local-directory-snapshot ` +
          `revision=${revision} staging=${stagingDir}`,
      );
      const workspace = new GenerationWorkspace({
        kind: opts.kind,
        view,
        overlay,
        stagingDir,
        activeDataDir: opts.dataDir,
        publishAtomicity: "local-directory-snapshot",
        publishRoot,
        revision,
        logger: opts.logger,
        publishHooks: opts.publishHooks,
        beforeCommit: opts.beforeCommit,
        historyContext: opts.historyContext,
        historyClock: opts.historyClock,
      });
      workspace.baseline = await snapshot.listAll();
      return workspace;
    }

    // Raw-FS mode: seed a private working copy from the published read view
    // (bundle if present, else legacy root) — never by pointing the model at the
    // raw legacy directory.
    const publishRoot = path.resolve(opts.dataDir);
    await recoverLocalPublishes(publishRoot, opts.logger);
    const revision = (await readCurrentPointer(publishRoot))?.revision ?? 0;
    const workspace = new GenerationWorkspace({
      kind: opts.kind,
      view: undefined,
      overlay: undefined,
      stagingDir,
      activeDataDir: stagingDir,
      publishAtomicity: "local-directory-snapshot",
      publishRoot,
      revision,
      logger: opts.logger,
      publishHooks: opts.publishHooks,
      beforeCommit: opts.beforeCommit,
      historyContext: opts.historyContext,
      historyClock: opts.historyClock,
    });
    workspace.baseline = await collectPublishedContent(publishRoot);
    await workspace.seedRawFs();
    opts.logger?.debug?.(`${TAG} opened raw-fs workspace kind=${opts.kind} staging=${stagingDir}`);
    return workspace;
  }

  /** Whether the workspace has staged changes that `commit()` would publish. */
  hasStagedChanges(): boolean {
    return this.overlay?.hasChanges() ?? false;
  }

  /** Program-only staged write of internal metadata (never exposed to the model). */
  async writeInternal(key: string, content: string | Buffer): Promise<void> {
    if (!key || key.includes("\0") || key.startsWith("/") || key.startsWith("\\") || key.split("/").includes("..")) {
      throw new Error(`${TAG} writeInternal rejected key: ${JSON.stringify(key)}`);
    }
    if (this.overlay) {
      await this.overlay.putObject(key, content);
      return;
    }
    const abs = resolveSafeLocalPath(this.stagingDir, key);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, content);
  }

  /**
   * Publish staged changes as one immutable bundle.
   *
   * `result` is forwarded to the optional `beforeCommit` hook. Throws on
   * failure; the previous bundle stays authoritative and no partial bundle is
   * ever pointed at.
   */
  async commit(result?: unknown): Promise<void> {
    if (this.committed) return;
    if (this.discarded) throw new Error(`${TAG} cannot commit a discarded workspace`);
    try {
      if (this.beforeCommit) {
        const draft = this.view ? await this.collectViewContent(this.view) : await this.collectStagedContent();
        const info = (result ?? {}) as { input_ids?: string[]; scope?: Record<string, string | undefined> };
        const changed_outputs = draft.filter(file => (this.kind === "l2" ? file.key.startsWith(StoragePaths.sceneBlocksDir) : isL3DocumentLocator(file.key))
          && !this.baseline.get(file.key)?.equals(file.content)).map(file => ({
            path: this.kind === "l2" ? file.key.slice(StoragePaths.sceneBlocksDir.length) : file.key,
            content_id: file.key, content: file.content.toString("utf-8"),
          }));
        const view = this.view ?? {
          readFile: async (key: string) => { try { return await fs.readFile(resolveSafeLocalPath(this.stagingDir, key), "utf-8"); } catch (err) { if ((err as NodeJS.ErrnoException).code === "ENOENT") return null; throw err; } },
          writeFile: (key: string, content: string) => this.writeInternal(key, content),
        } as StorageAdapter;
        await this.beforeCommit({
          kind: this.kind, view,
          writeInternal: (key, content) => this.writeInternal(key, content),
          result: { layer: this.kind, input_layer: this.kind === "l2" ? "l1" : "l2", input_ids: info.input_ids ?? [], scope: info.scope ?? {}, changed_outputs },
        });
      }
      const files = this.view
        ? await this.collectViewContent(this.view)
        : await this.collectStagedContent();
      // The history context is program-internal and validated: whatever it
      // returns MUST publish exactly the content the workspace staged, so it can
      // never ship a version that does not describe the live body.
      const history = this.historyContext
        ? { build: makeHistoryBuild(this.historyContext, files), clock: this.historyClock }
        : undefined;
      await commitContentBundle(this.publishRoot, files, {
        logger: this.logger,
        hooks: this.publishHooks,
        expectedRevision: this.revision,
        // Once history is enabled the publish root refuses a context-free
        // commit, so a workspace that has a builder passes it through here.
        ...(history ? { history } : {}),
      });
      this.committed = true;
      this.logger?.debug?.(`${TAG} committed kind=${this.kind} (${files.length} files)`);
    } finally {
      await fs.rm(this.stagingDir, { recursive: true, force: true }).catch(() => {});
    }
  }

  /** Abandon the workspace. Published content is never touched. */
  async discard(): Promise<void> {
    if (this.committed) return;
    this.discarded = true;
    await fs.rm(this.stagingDir, { recursive: true, force: true }).catch(() => {});
    this.logger?.debug?.(`${TAG} discarded kind=${this.kind}`);
  }

  get isCommitted(): boolean {
    return this.committed;
  }

  // ── raw-FS helpers ───────────────────────────────────────────────

  /**
   * Seed the private working copy from the published read view. Content is
   * copied out of the current bundle (or the legacy root when no bundle exists
   * yet) and written into the staging tree, so the model never touches the
   * published root and never reads a half-swapped directory.
   */
  private async seedRawFs(): Promise<void> {
    const published = this.baseline;
    for (const [key, content] of published) {
      const abs = resolveSafeLocalPath(this.stagingDir, key);
      await fs.mkdir(path.dirname(abs), { recursive: true });
      await fs.writeFile(abs, content);
    }
  }

  private async collectStagedContent(): Promise<BundleFile[]> {
    const files: BundleFile[] = [];

    const stagedSceneDir = path.join(this.stagingDir, "scene_blocks");
    let names: string[] = [];
    try {
      names = await fs.readdir(stagedSceneDir);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    }
    for (const name of names) {
      if (!name.endsWith(".md")) continue;
      const abs = resolveSafeLocalPath(this.stagingDir, `${StoragePaths.sceneBlocksDir}${name}`);
      const stat = await fs.lstat(abs);
      if (!stat.isFile()) continue;
      files.push({ key: `${StoragePaths.sceneBlocksDir}${name}`, content: await fs.readFile(abs) });
    }

    for (const key of [...L3_DOCUMENTS, StoragePaths.sceneIndex, CONTENT_SOURCES_KEY]) {
      const abs = resolveSafeLocalPath(this.stagingDir, key);
      try {
        files.push({ key, content: await fs.readFile(abs) });
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
      }
    }
    return files;
  }

  // ── StorageAdapter helpers ───────────────────────────────────────

  /**
   * Materialise the complete new content set from the merged working view. The
   * read-through overlay already reflects base (published) content plus staged
   * writes/tombstones, so this captures additions *and* deletions without any
   * broad catch that could silently publish an empty tree.
   */
  private async collectViewContent(view: StorageAdapter): Promise<BundleFile[]> {
    const files: BundleFile[] = [];
    const names = await view.readdirNames(StoragePaths.sceneBlocksDir, ".md");
    for (const name of names) {
      const buffer = await view.readFileBuffer(`${StoragePaths.sceneBlocksDir}${name}`);
      if (buffer) files.push({ key: `${StoragePaths.sceneBlocksDir}${name}`, content: buffer });
    }
    for (const key of [...L3_DOCUMENTS, StoragePaths.sceneIndex, CONTENT_SOURCES_KEY]) {
      const buffer = await view.readFileBuffer(key);
      if (buffer) files.push({ key, content: buffer });
    }
    return files;
  }
}

/**
 * Normalise a workspace history context into the bundle commit hook, wrapping it
 * with the content-set guard. A plan is handed the staged file set (so the
 * generator never assembles a manifest) and may additionally set the
 * program-owned source sidecar; a raw hook keeps the strict exact-match guard.
 */
function makeHistoryBuild(
  context: ContentHistoryCommitHook | GenerationHistoryPlan,
  expected: BundleFile[],
): ContentHistoryCommitHook {
  if (typeof context === "function") return guardHistoryContext(context, expected, false);
  return guardHistoryContext(
    (ctx: ContentHistoryCommitContext) => context.plan(ctx, expected),
    expected,
    true,
  );
}

/** Wrap a workspace history context so its content set must equal the staged set. */
function guardHistoryContext(
  hook: ContentHistoryCommitHook,
  expected: BundleFile[],
  allowSidecarDelta: boolean,
): ContentHistoryCommitHook {
  return async (ctx) => {
    const outcome = await hook(ctx);
    if (outcome && (outcome as ContentHistoryNoop).noop === true) return outcome;
    const built = outcome as ContentHistoryCommitOutcome;
    if (!built || !Array.isArray(built.files) || !built.history) {
      throw new Error(`${TAG} history context returned an invalid outcome`);
    }
    if (built.extraFiles && built.extraFiles.length > 0) {
      throw new Error(`${TAG} history context may not add extra bundle files`);
    }
    if (!sameContentSet(expected, built.files, allowSidecarDelta)) {
      throw new Error(
        `${TAG} history context returned a content set that does not match the workspace's staged content; refusing to publish`,
      );
    }
    return built;
  };
}

/**
 * Key + byte equality between two content file sets (order-independent). When
 * `allowSidecarDelta` is set the program-owned source sidecar is excluded from
 * the comparison (the planner owns it); every other key must match exactly.
 */
function sameContentSet(a: BundleFile[], b: BundleFile[], allowSidecarDelta = false): boolean {
  const strip = (files: BundleFile[]): Map<string, Buffer> => {
    const map = new Map<string, Buffer>();
    for (const file of files) {
      if (allowSidecarDelta && file.key === CONTENT_SOURCES_KEY) continue;
      map.set(file.key, file.content);
    }
    return map;
  };
  const expected = strip(a);
  const actual = strip(b);
  if (expected.size !== actual.size) return false;
  for (const [key, content] of expected) {
    const other = actual.get(key);
    if (!other || !other.equals(content)) return false;
  }
  return true;
}

/**
 * Which keys a successful generation is allowed to publish.
 *
 * L2: scene blocks + the derived scene index + persona navigation.
 * L3: persona.md only.
 *
 * Everything else (e.g. checkpoint / recall state) stays owned by the caller's
 * base store and is never published from a generation workspace.
 */
export function isPublishableKey(kind: GenerationWorkspaceKind, key: string): boolean {
  if (key === StoragePaths.persona) return true;
  if (key === CONTENT_SOURCES_KEY) return true;
  if (kind === "l2") {
    return key.startsWith(StoragePaths.sceneBlocksDir) || key === StoragePaths.sceneIndex;
  }
  return false;
}
