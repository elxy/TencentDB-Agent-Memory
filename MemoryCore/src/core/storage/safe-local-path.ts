/**
 * safe-local-path.ts — shared strict path resolver for local filesystem access.
 *
 * Both the standalone LLM runner's sandboxed read/write/edit tools and the
 * LocalStorageBackend resolve caller-influenced relative paths under a configured
 * root directory. The original implementations used `path.resolve()` plus a
 * string `startsWith()` prefix check, which has two problems:
 *
 *   1. Prefix bypass: a sibling path that merely shares a text prefix with the
 *      root (root `/a/work`, target `/a/work2/secret`) passes `startsWith()`.
 *   2. Symlink traversal: a symlink *inside* the root that points outside the
 *      root is transparently followed by read/write/open calls, escaping the
 *      boundary even though the resolved string stayed under the root.
 *
 * `resolveSafeLocalPath` fixes both with:
 *   - strict relative-path validation (no absolute, no NUL, no backslash
 *     ambiguity, no ".." segments),
 *   - segment-aware containment via `path.relative()`,
 *   - an `lstat` walk over every existing component from the root down to the
 *     target, rejecting a symlink in ANY existing component (including the root
 *     itself).
 *
 * ── SECURITY MODEL / LIMITATIONS (READ THIS) ────────────────────────────────
 *
 *   - This is NOT an OS sandbox. It is an application-level best-effort guard.
 *   - It is NOT TOCTOU-safe against a hostile process that can concurrently
 *     mutate the filesystem: the `lstat` checks and the later `open`/`read`/
 *     `write`/`mkdir` calls are separate syscalls, so a component can be
 *     swapped for a symlink in between. Fully hardening that requires
 *     `openat`+`O_NOFOLLOW` per component, `O_PATH`/`RESOLVE_BENEATH`, or a real
 *     OS sandbox (chroot / mount namespace / Landlock / container).
 *   - Missing components are tolerated on purpose so writes (and reads of
 *     not-yet-created files) can create/observe them; there is nothing to
 *     `lstat` for a path that does not exist yet.
 *   - Symlinks are rejected wholesale (not resolved-and-rechecked) because a
 *     symlink to a location that happens to stay inside the root is still an
 *     ambiguous, attacker-influenceable indirection and callers here only ever
 *     need plain files/directories.
 *
 * No new dependencies; synchronous `lstat` so the existing synchronous
 * LocalStorageBackend resolver (`exists()`) can use it directly.
 */

import { lstatSync, type Stats } from "node:fs";
import { isAbsolute, relative, resolve, parse } from "node:path";

/** Error thrown when a path is rejected by the strict validation rules. */
export class UnsafeLocalPathError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsafeLocalPathError";
  }
}

export interface ResolveSafeLocalPathOptions {
  /**
   * Permit the resolved path to equal `rootDir` itself (used when a "" / "."
   * key means "the storage root", e.g. listing the root directory).
   * Defaults to false.
   */
  allowRoot?: boolean;
}

/**
 * `lstat` that treats "does not exist" (ENOENT) and "an ancestor is not a
 * directory" (ENOTDIR) as "missing", and rethrows every other error so real
 * filesystem problems (EACCES, ELOOP, EIO, …) fail loud instead of being
 * mistaken for a boundary rejection.
 */
function lstatOrNull(p: string): Stats | null {
  try {
    return lstatSync(p);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException | undefined)?.code;
    if (code === "ENOENT") return null;
    throw err;
  }
}

/**
 * Resolve `relPath` under `rootDir`, rejecting anything that is not a plain,
 * non-symlinked path inside the root.
 *
 * @param rootDir  Absolute or relative root directory (resolved internally).
 * @param relPath  Relative path using "/" separators. "" / "." only allowed
 *                 with `allowRoot`.
 * @returns The absolute resolved path (which may point at a not-yet-existing
 *          file/directory when the target and/or root is missing).
 * @throws {UnsafeLocalPathError} for boundary violations.
 * @throws {Error} for unexpected filesystem errors (permission, symlink loop…).
 */
export function resolveSafeLocalPath(
  rootDir: string,
  relPath: string,
  opts: ResolveSafeLocalPathOptions = {},
): string {
  if (typeof rootDir !== "string" || rootDir.length === 0) {
    throw new UnsafeLocalPathError(`Invalid root directory: ${JSON.stringify(rootDir)}`);
  }
  if (typeof relPath !== "string") {
    throw new UnsafeLocalPathError(`Invalid path: ${JSON.stringify(relPath)}`);
  }
  if (relPath.includes("\0")) {
    throw new UnsafeLocalPathError("Path must not contain NUL character");
  }
  // Reject absolute paths up front. `path.isAbsolute` covers POSIX "/…" and
  // Windows drive/UNC forms; the explicit leading "/" and "\" checks also catch
  // them on the other platform.
  if (isAbsolute(relPath) || relPath.startsWith("/") || relPath.startsWith("\\")) {
    throw new UnsafeLocalPathError(`Path must be relative, got absolute: ${relPath}`);
  }
  // Backslash is rejected on every platform: on Windows it is a separator (so
  // "..\\.." would evade a "/"-only split), and on POSIX it is a valid filename
  // character whose meaning would differ across platforms.
  if (relPath.includes("\\")) {
    throw new UnsafeLocalPathError(`Path must not contain backslash: ${relPath}`);
  }

  const segments: string[] = [];
  for (const segment of relPath.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") {
      throw new UnsafeLocalPathError(`Path traversal rejected: "${relPath}" contains ".." segment`);
    }
    segments.push(segment);
  }

  const absRoot = resolve(rootDir);

  if (segments.length === 0) {
    if (!opts.allowRoot) {
      throw new UnsafeLocalPathError(
        `Path must not resolve to the root directory: ${JSON.stringify(relPath)}`,
      );
    }
    assertNoSymlinkChain(absRoot, [], relPath);
    return absRoot;
  }

  const absTarget = resolve(absRoot, ...segments);

  // Segment-aware containment: `relative()` yields "../…" (or an absolute
  // path) exactly when absTarget is outside absRoot, so the "/a/work" vs
  // "/a/work2" sibling-prefix bypass is impossible.
  const relFromRoot = relative(absRoot, absTarget);
  if (relFromRoot === "" || (relFromRoot === ".." || relFromRoot.startsWith("../")) || isAbsolute(relFromRoot)) {
    throw new UnsafeLocalPathError(
      `Path traversal rejected: "${relPath}" escapes rootDir "${absRoot}"`,
    );
  }

  assertNoSymlinkChain(absRoot, segments, relPath);
  return absTarget;
}

/**
 * Walk every existing component from the root down to the target and reject a
 * symlink in any of them (root included). A missing component stops the walk:
 * it cannot be a symlink, and for a write the remaining components do not exist
 * yet. A non-final existing component must be a directory.
 */
function assertNoSymlinkChain(absRoot: string, segments: string[], relPath: string): void {
  // Inspect root ancestors too; a configured root below a symlink is unsafe.
  const fsRoot = parse(absRoot).root;
  let ancestor = fsRoot;
  const chain: string[] = [fsRoot];
  for (const component of relative(fsRoot, absRoot).split("/").filter(Boolean)) {
    ancestor = resolve(ancestor, component);
    chain.push(ancestor);
  }
  let current = absRoot;
  for (const segment of segments) {
    current = resolve(current, segment);
    chain.push(current);
  }

  for (let i = 0; i < chain.length; i++) {
    const component = chain[i];
    const isFinal = i === chain.length - 1;
    const stats = lstatOrNull(component);
    if (stats === null) return; // missing: nothing further can be checked
    if (stats.isSymbolicLink()) {
      throw new UnsafeLocalPathError(
        `Path traversal rejected: "${relPath}" traverses symlink "${component}"`,
      );
    }
    if (!isFinal && !stats.isDirectory()) {
      throw new UnsafeLocalPathError(
        `Path traversal rejected: "${relPath}" has non-directory component "${component}"`,
      );
    }
  }
}
