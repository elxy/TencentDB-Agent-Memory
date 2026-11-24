/**
 * LocalStorageBackend — file-system based implementation of IStorageBackend.
 *
 * Used for local development and "free" mode where no COS is available.
 * Maps object keys to local file paths under a configurable root directory.
 */

import { readFile, writeFile, mkdir, readdir, lstat, stat, unlink, rm, appendFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { resolveSafeLocalPath } from "./safe-local-path.js";
import {
  readPublishedObject,
  listPublishedObjects,
  publishContentMutation,
  openPublishedSnapshot,
  isBundleContentKey,
  isBundleContentPrefix,
  type PublishedSnapshot,
} from "./local-published-storage.js";
import { LocalContentHistory } from "./local-content-history.js";
import type {
  IStorageBackend,
  StorageObject,
  PutObjectOptions,
  ListObjectsOptions,
  ListResult,
  ListEntry,
  StorageLogger,
} from "./types.js";
import { pageEntries } from "./list-page.js";

const TAG = "[storage][local]";

export interface LocalStorageBackendOptions {
  /** Root directory for all stored files. */
  rootDir: string;
  /** Logger instance. */
  logger?: StorageLogger;
}

export class LocalStorageBackend implements IStorageBackend {
  readonly type = "local" as const;
  private readonly rootDir: string;
  private readonly logger?: StorageLogger;

  constructor(opts: LocalStorageBackendOptions | string) {
    if (typeof opts === "string") {
      // Backwards-compatible: accept plain string rootDir
      this.rootDir = opts;
    } else {
      this.rootDir = opts.rootDir;
      this.logger = opts.logger;
    }
  }

  /**
   * Resolve a storage key to an absolute filesystem path under rootDir.
   *
   * CR-6 fix (2026-05-19): rejects path-traversal attempts. Without this guard
   * a key containing "../" or absolute paths could read/write arbitrary files
   * on disk (e.g. getObject("../../../etc/passwd") would resolve outside
   * rootDir). Affects standalone mode where user-controllable fields like
   * instanceId / sceneName / sessionKey end up in the key.
   *
   * Rejected:
   * - Empty key
   * - Keys containing NUL (\0) — POSIX/Linux path terminator, can confuse
   *   downstream tooling (sqlite, file managers).
   * - Keys with leading "/" or "\" (absolute paths).
   * - Keys whose resolved path falls outside rootDir (../ traversal).
   * - Symlinked components (any existing ancestor, including rootDir itself).
   *
   * CR-6b fix: path containment is now segment-aware and symlinks are rejected
   * via the shared `resolveSafeLocalPath` helper (safe-local-path.ts). That
   * helper documents the explicit TOCTOU / non-OS-sandbox limitations of these
   * checks — they defend against path-shaped attacks, not a hostile process
   * concurrently mutating the filesystem.
   */
  private resolvePath(key: string): string {
    if (!key || typeof key !== "string") {
      throw new Error(`Invalid storage key: ${JSON.stringify(key)}`);
    }
    return resolveSafeLocalPath(this.rootDir, key);
  }

  /**
   * Resolve the sidecar `.meta.json` path for a key through the same strict
   * boundary checks, so a symlinked/escaping sidecar is rejected too.
   */
  private resolveMetaPath(key: string): string {
    return resolveSafeLocalPath(this.rootDir, `${key}.meta.json`);
  }

  /**
   * Expose the configured storage root. Used by the generation workspace to
   * perform crash-safe local directory publishing (see
   * `local-published-storage.ts`). Read-only escape hatch; callers must not
   * mutate files under the root directly.
   */
  getRootDir(): string {
    return this.rootDir;
  }

  /**
   * Local publish capability: the absolute root under which this view's
   * content bundle pointer lives. Consumed by the generation workspace and
   * forwarded by scoped views (never parsed from private fields).
   */
  getLocalPublishRoot(): string {
    return this.rootDir;
  }

  /** Pin one revision for a whole read request (content + sources). */
  async openPublishedSnapshot(): Promise<PublishedSnapshot> {
    return openPublishedSnapshot(this.rootDir);
  }

  /**
   * Formal L2/L3 content history for this root. Construct via this capability
   * (not by string-building paths) so a scoped view resolves the same root the
   * publish pointer lives under.
   */
  getContentHistory(clock?: () => Date): LocalContentHistory {
    return new LocalContentHistory(this.rootDir, clock);
  }

  async putObject(key: string, content: string | Buffer, opts?: PutObjectOptions): Promise<void> {
    if (isBundleContentKey(key)) {
      // Content keys are published through an immutable bundle so readers never
      // observe a half-written scene set. A single-key write rebuilds the full
      // content set and switches the pointer once.
      const buf = typeof content === "string" ? Buffer.from(content, "utf-8") : Buffer.from(content);
      await publishContentMutation(this.rootDir, { writes: [{ key, content: buf }] }, { logger: this.logger });
      return;
    }
    const filePath = this.resolvePath(key);
    await mkdir(dirname(filePath), { recursive: true });

    const buf = typeof content === "string" ? Buffer.from(content, "utf-8") : content;
    await writeFile(filePath, buf);

    // Store metadata as a sidecar .meta.json file if metadata is provided
    if (opts?.contentType || (opts?.metadata && Object.keys(opts.metadata).length > 0)) {
      const metaPath = this.resolveMetaPath(key);
      await writeFile(metaPath, JSON.stringify({
        contentType: opts.contentType,
        metadata: opts.metadata,
      }));
    }

    this.logger?.debug?.(`${TAG} putObject: ${key} (${buf.length} bytes)`);
  }

  /**
   * Append content to the end of a file. CR-1 fix (2026-05-19): uses POSIX
   * fs.appendFile (with O_APPEND flag) which is atomic per call on local fs,
   * so concurrent appendObject calls to the same key do not race.
   */
  async appendObject(key: string, content: string | Buffer): Promise<void> {
    if (isBundleContentKey(key)) {
      throw new Error(
        `${TAG} appendObject is not supported for bundle content key ${JSON.stringify(key)} ` +
          `(content is published as a whole immutable bundle)`,
      );
    }
    const filePath = this.resolvePath(key);
    await mkdir(dirname(filePath), { recursive: true });

    const buf = typeof content === "string" ? Buffer.from(content, "utf-8") : content;
    // fs.appendFile uses O_APPEND which provides POSIX atomic-append guarantees
    // for writes up to PIPE_BUF (4096 bytes on Linux). Larger writes may be
    // interleaved but never overwritten — each chunk lands at the end.
    await appendFile(filePath, buf);

    this.logger?.debug?.(`${TAG} appendObject: ${key} (+${buf.length} bytes)`);
  }

  async getObject(key: string): Promise<StorageObject | null> {
    if (isBundleContentKey(key)) {
      return readPublishedObject(this.rootDir, key);
    }
    const filePath = this.resolvePath(key);
    const metaPath = this.resolveMetaPath(key);

    if (!existsSync(filePath)) {
      return null;
    }

    try {
      const [content, stats] = await Promise.all([
        readFile(filePath),
        stat(filePath),
      ]);

      // Try to read sidecar metadata
      let contentType: string | undefined;
      let metadata: Record<string, string> | undefined;
      if (existsSync(metaPath)) {
        try {
          const metaRaw = await readFile(metaPath, "utf-8");
          const meta = JSON.parse(metaRaw);
          contentType = meta.contentType;
          metadata = meta.metadata;
        } catch {
          // ignore corrupt meta file
        }
      }

      return {
        key,
        content,
        contentType,
        metadata,
        lastModified: stats.mtime,
        size: stats.size,
      };
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        return null;
      }
      throw err;
    }
  }

  async exists(key: string): Promise<boolean> {
    if (isBundleContentKey(key)) {
      return (await readPublishedObject(this.rootDir, key)) !== null;
    }
    const filePath = this.resolvePath(key);
    return existsSync(filePath);
  }

  /**
   * List objects under a key prefix, per the D9.2 contract
   * (see storage/__contract__/storage-backend.contract.ts).
   *
   * The prefix is a *string* prefix rather than a directory, so it is split
   * into the deepest enclosing directory plus a leading fragment of the entry
   * name: "scene_blocks/wo" lists "scene_blocks/" and keeps names starting
   * with "wo".
   */
  async listObjects(prefix: string, opts?: ListObjectsOptions): Promise<ListResult> {
    if (isBundleContentPrefix(prefix)) {
      return listPublishedObjects(this.rootDir, prefix, opts);
    }
    const recursive = opts?.recursive ?? false;

    const lastSlash = prefix.lastIndexOf("/");
    const dirKey = lastSlash >= 0 ? prefix.slice(0, lastSlash + 1) : "";
    const namePrefix = lastSlash >= 0 ? prefix.slice(lastSlash + 1) : prefix;
    const dirPath = this.resolveDir(dirKey);

    if (!existsSync(dirPath)) {
      return { entries: [], total: 0 };
    }

    const allEntries: ListEntry[] = [];
    if (recursive) {
      await this.collectFiles(dirPath, dirKey, prefix, allEntries, 10_000);
    } else {
      await this.collectFolded(dirPath, dirKey, namePrefix, allEntries);
    }

    return pageEntries(allEntries, opts);
  }

  async deleteObject(key: string): Promise<void> {
    if (isBundleContentKey(key)) {
      await publishContentMutation(this.rootDir, { deletes: [key] }, { logger: this.logger });
      return;
    }
    const filePath = this.resolvePath(key);
    const metaPath = this.resolveMetaPath(key);

    try {
      await unlink(filePath);
      // Also remove sidecar metadata if exists
      if (existsSync(metaPath)) {
        await unlink(metaPath);
      }
      this.logger?.debug?.(`${TAG} deleteObject: ${key}`);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
        throw err;
      }
      // Idempotent: ignore if file doesn't exist
    }
  }

  async deleteByPrefix(prefix: string): Promise<number> {
    if (isBundleContentPrefix(prefix)) {
      const listed = await listPublishedObjects(this.rootDir, prefix, { recursive: true, maxKeys: 100_000 });
      const keys = listed.entries.filter((entry) => !entry.isDirectory).map((entry) => entry.key);
      if (keys.length === 0) return 0;
      await publishContentMutation(this.rootDir, { deletes: keys }, { logger: this.logger });
      return keys.length;
    }
    const dirPath = this.resolvePath(prefix);

    if (!existsSync(dirPath)) {
      return 0;
    }

    // Count files before deleting
    const entries: ListEntry[] = [];
    const dirKey = prefix.endsWith("/") ? prefix : `${prefix}/`;
    await this.collectFiles(dirPath, dirKey, dirKey, entries, Number.MAX_SAFE_INTEGER);
    const fileCount = entries.length;

    await rm(dirPath, { recursive: true, force: true });
    this.logger?.debug?.(`${TAG} deleteByPrefix: ${prefix} (${fileCount} files)`);

    return fileCount;
  }

  // ── Private helpers ──────────────────────────────────────

  /** Resolve the directory part of a key prefix; "" means the backend root. */
  private resolveDir(dirKey: string): string {
    // allowRoot: the empty prefix names the backend root itself.
    return resolveSafeLocalPath(this.rootDir, dirKey, { allowRoot: true });
  }

  /**
   * Non-recursive listing (D9.2-2): direct children whose name starts with
   * `namePrefix`; subdirectories collapse into a single trailing-slash entry
   * without being descended into.
   */
  private async collectFolded(
    dirPath: string,
    dirKey: string,
    namePrefix: string,
    entries: ListEntry[],
  ): Promise<void> {
    let items: string[];
    try {
      items = await readdir(dirPath);
    } catch {
      return;
    }

    for (const item of items) {
      if (item.endsWith(".meta.json")) continue;
      if (!item.startsWith(namePrefix)) continue;

      try {
        // lstat (not stat) + explicit symlink skip: never follow a symlinked
        // entry out of the root while listing.
        const stats = await lstat(join(dirPath, item));
        if (stats.isSymbolicLink()) continue;
        entries.push(stats.isDirectory()
          ? { key: `${dirKey}${item}/`, size: 0, lastModified: stats.mtime, isDirectory: true }
          : { key: `${dirKey}${item}`, size: stats.size, lastModified: stats.mtime, isDirectory: false });
      } catch {
        // skip inaccessible items
      }
    }
  }

  /**
   * Recursive listing (D9.2-3): files only. Descends a subdirectory only when
   * it can still contain keys matching `filterPrefix`.
   */
  private async collectFiles(
    dirPath: string,
    dirKey: string,
    filterPrefix: string,
    entries: ListEntry[],
    limit: number,
  ): Promise<void> {
    if (entries.length >= limit) return;

    let items: string[];
    try {
      items = await readdir(dirPath);
    } catch {
      return;
    }

    for (const item of items) {
      if (entries.length >= limit) break;
      if (item.endsWith(".meta.json")) continue;

      const fullPath = join(dirPath, item);
      const itemKey = `${dirKey}${item}`;

      try {
        // lstat + symlink skip: recursive walk must not descend through a
        // symlinked directory that points outside the root.
        const stats = await lstat(fullPath);
        if (stats.isSymbolicLink()) continue;

        if (stats.isDirectory()) {
          const subKey = `${itemKey}/`;
          if (subKey.startsWith(filterPrefix) || filterPrefix.startsWith(subKey)) {
            await this.collectFiles(fullPath, subKey, filterPrefix, entries, limit);
          }
        } else if (itemKey.startsWith(filterPrefix)) {
          entries.push({
            key: itemKey,
            size: stats.size,
            lastModified: stats.mtime,
            isDirectory: false,
          });
        }
      } catch {
        // skip inaccessible items
      }
    }
  }
}
