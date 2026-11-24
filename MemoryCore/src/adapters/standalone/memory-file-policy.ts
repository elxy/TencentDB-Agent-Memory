/**
 * memory-file-policy.ts — layer-specific, per-run file access policy for the
 * standalone LLM file tools (`read` / `write` / `edit`).
 *
 * The tool sandbox alone is not enough. A sandbox root only stops the model
 * from leaving its workspace; *inside* that root the model can still reach
 * files it has no business touching (checkpoints, scene_index, JSONL records,
 * subdirectories, dotfiles) or read back content the caller already supplied
 * in the prompt. This module narrows the sandbox per layer so a run can only
 * touch the exact files its layer owns.
 *
 * Layers:
 *   - L2 scene extraction: only **direct** `*.md` files directly under the
 *     scene-block tool root. Never checkpoints / index / db / JSONL / nested
 *     directories / hidden dotfiles.
 *   - L3 persona generation: no reads at all (the changed scene content is
 *     already embedded in the prompt); write/edit only `persona.md`.
 *
 * Design notes:
 *   - Pure, dependency-free, side-effect free. This module only *decides*;
 *     callers still perform the IO.
 *   - `undefined` policy means "no extra restriction", so existing callers
 *     keep their current sandbox-only behaviour.
 *   - Paths are normalized with the same rules used by the enforcement sites
 *     (backslash rejection, `.` / empty segment folding) and rejected for
 *     traversal, absolute paths and NUL bytes *before* any IO happens. The
 *     filesystem branch additionally runs the shared
 *     `resolveSafeLocalPath` traversal check.
 */

import type {
  FilePolicyDecision,
  FilePolicyOperation,
  FilePolicyRule,
  MemoryFilePolicy,
} from "../../core/types.js";

export type {
  FilePolicyDecision,
  FilePolicyOperation,
  FilePolicyRule,
  MemoryFilePolicy,
} from "../../core/types.js";

/**
 * Normalize a model-supplied relative path into the canonical form used by the
 * policy checks. Returns `null` when the raw path is structurally invalid:
 * not a string, empty, absolute, contains a NUL byte, contains a backslash
 * (Windows separator ambiguity) or a `..` segment.
 *
 * This mirrors the normalization performed by `resolveStorageKey` in
 * `storage-tools.ts` and is at least as strict as the shared
 * `resolveSafeLocalPath` filesystem resolver.
 */
export function normalizePolicyPath(rawPath: unknown): string | null {
  if (typeof rawPath !== "string") return null;
  if (rawPath.includes("\0")) return null;
  // Backslashes are rejected outright (rather than folded) to avoid the
  // Windows-vs-POSIX separator ambiguity that `resolveSafeLocalPath` also
  // guards against.
  if (rawPath.includes("\\")) return null;
  if (rawPath.startsWith("/")) return null;

  const segments: string[] = [];
  for (const segment of rawPath.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") return null;
    segments.push(segment);
  }
  if (segments.length === 0) return null;
  return segments.join("/");
}

/**
 * Match one normalized relative path against one allow rule.
 *
 * A rule matches when:
 *   - the operation is listed,
 *   - the path equals `path` (when given) or starts with `prefix/` (when given),
 *   - hidden dot-segments are absent unless `allowHidden`,
 *   - `directOnly` implies the matched remainder has no `/`,
 *   - `markdownOnly` implies the matched basename ends in `.md`.
 */
function matchesRule(rule: FilePolicyRule, operation: FilePolicyOperation, normalized: string): boolean {
  if (!rule.operations.includes(operation)) return false;

  const segments = normalized.split("/");
  if (!rule.allowHidden && segments.some((segment) => segment.startsWith("."))) return false;

  let remainder: string;
  if (rule.path !== undefined) {
    if (normalized !== rule.path) return false;
    remainder = normalized;
  } else if (rule.prefix !== undefined) {
    const prefix = rule.prefix.replace(/\/+$/, "");
    if (prefix.length === 0) {
      remainder = normalized;
    } else {
      if (!normalized.startsWith(`${prefix}/`)) return false;
      remainder = normalized.slice(prefix.length + 1);
    }
  } else {
    remainder = normalized;
  }

  if (remainder.length === 0) return false; // a rule must match an actual file
  if (rule.directOnly && remainder.includes("/")) return false;
  if (rule.markdownOnly && !remainder.endsWith(".md")) return false;
  return true;
}

/**
 * Evaluate one `(operation, path)` pair against a policy.
 *
 * `undefined` policy → always allowed (backwards compatibility). Every other
 * policy is deny-by-default: a path is allowed only when some rule grants the
 * requested operation.
 */
export function evaluateFilePolicy(
  policy: MemoryFilePolicy | undefined,
  operation: FilePolicyOperation,
  rawPath: unknown,
): FilePolicyDecision {
  const normalizedPath = normalizePolicyPath(rawPath);
  if (normalizedPath === null) {
    return {
      allowed: false,
      normalizedPath: "",
      reason: `invalid relative path: ${JSON.stringify(rawPath)}`,
    };
  }
  if (!policy) {
    return { allowed: true, normalizedPath };
  }

  for (const rule of policy.rules) {
    if (matchesRule(rule, operation, normalizedPath)) {
      return { allowed: true, normalizedPath };
    }
  }

  return {
    allowed: false,
    normalizedPath,
    reason: `operation "${operation}" on "${normalizedPath}" not permitted by policy "${policy.label}"`,
  };
}

/** Convenience wrapper: is `(operation, path)` permitted? */
export function isFilePolicyAllowed(
  policy: MemoryFilePolicy | undefined,
  operation: FilePolicyOperation,
  rawPath: unknown,
): boolean {
  return evaluateFilePolicy(policy, operation, rawPath).allowed;
}

/**
 * Build the JSON error payload returned to the model for a denied tool call.
 * Kept in this module so both the raw-FS and StorageAdapter branches produce
 * byte-identical errors.
 */
export function filePolicyDeniedResult(decision: FilePolicyDecision): string {
  return JSON.stringify({ error: `File policy denied: ${decision.reason ?? "not permitted"}` });
}

/**
 * L2 scene extraction policy. The runner's tool root is already
 * `scene_blocks/`, so this restricts the model to direct `*.md` children of
 * that root. Nested paths, hidden dotfiles, non-markdown files, checkpoints,
 * the scene index and JSONL records are all rejected.
 */
export const SCENE_BLOCK_FILE_POLICY: MemoryFilePolicy = {
  label: "l2-scene-blocks",
  rules: [
    {
      operations: ["read", "write", "edit"],
      directOnly: true,
      markdownOnly: true,
    },
  ],
};

/**
 * L3 persona generation policy. The prompt already contains the changed scene
 * content, so reads are unnecessary and denied outright. Writes/edits are
 * limited to `persona.md` at the tool root.
 */
export const PERSONA_FILE_POLICY: MemoryFilePolicy = {
  label: "l3-persona",
  rules: [
    {
      operations: ["write", "edit"],
      path: "persona.md",
    },
  ],
};
