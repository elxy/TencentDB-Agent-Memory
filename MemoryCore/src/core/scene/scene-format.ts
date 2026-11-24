/**
 * Scene Block file format: parse and format the META-delimited Markdown files.
 *
 * Two layers live here:
 *   - `parseSceneBlock` — the legacy, permissive reader. It tolerates missing
 *     META sections and malformed values so that existing on-disk data keeps
 *     loading exactly as before. Do not change its behaviour.
 *   - `validateSceneBlock` — a strict, pure, side-effect-free validator used
 *     on freshly generated model output (and at the profile-sync boundary).
 *     It rejects anything that is not an exact, ordered META block with a
 *     non-empty Markdown body.
 */

export interface SceneBlockMeta {
  created: string;
  updated: string;
  summary: string;
  heat: number;
}

export interface SceneBlock {
  filename: string;
  meta: SceneBlockMeta;
  content: string;
}

export const META_START = "-----META-START-----";
export const META_END = "-----META-END-----";

/**
 * Exact marker the LLM writes to soft-delete a scene block. It is an
 * orchestration-level signal, not a valid scene, so `validateSceneBlock`
 * rejects it while the extractor treats it as a deletion.
 */
export const SCENE_DELETED_MARKER = "[DELETED]";

/**
 * Parse a Scene Block file into structured data.
 *
 * Legacy, permissive: files without a META section are returned with empty
 * meta and the whole file as content, and unparseable fields fall back to
 * defaults. `validateSceneBlock` is the strict counterpart for new output.
 */
export function parseSceneBlock(raw: string, filename: string): SceneBlock {
  const startIdx = raw.indexOf(META_START);
  const endIdx = raw.indexOf(META_END);

  if (startIdx === -1 || endIdx === -1) {
    // No META section — treat entire file as content
    return {
      filename,
      meta: { created: "", updated: "", summary: "", heat: 0 },
      content: raw.trim(),
    };
  }

  const metaBlock = raw.slice(startIdx + META_START.length, endIdx).trim();
  const content = raw.slice(endIdx + META_END.length).trim();

  const meta: SceneBlockMeta = {
    created: extractMetaField(metaBlock, "created"),
    updated: extractMetaField(metaBlock, "updated"),
    summary: extractMetaField(metaBlock, "summary"),
    heat: parseInt(extractMetaField(metaBlock, "heat"), 10) || 0,
  };

  return { filename, meta, content };
}

/**
 * Format a Scene Block back into file content.
 */
export function formatSceneBlock(meta: SceneBlockMeta, content: string): string {
  return `${formatMeta(meta)}\n\n${content}`;
}

/**
 * Format the META section.
 */
export function formatMeta(meta: SceneBlockMeta): string {
  return [
    META_START,
    `created: ${meta.created}`,
    `updated: ${meta.updated}`,
    `summary: ${meta.summary}`,
    `heat: ${meta.heat}`,
    META_END,
  ].join("\n");
}

// ============================
// Strict validation (new output only)
// ============================

/** META fields, required exactly once each, in this exact order. */
const META_FIELDS = ["created", "updated", "summary", "heat"] as const;

/** `created:`/`updated:` must look like an ISO 8601 date or datetime. */
const ISO_DATETIME_RE =
  /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/;

export interface SceneValidationResult {
  /** True when `raw` is an exact, valid scene block. */
  valid: boolean;
  /** Alias of `valid`, for callers that prefer `result.ok`. */
  ok: boolean;
  /** All individual issues found (empty when valid). */
  errors: string[];
  /** Human-readable summary of `errors` (undefined when valid). */
  error?: string;
  /** Parsed block — only present when `valid` is true. */
  block?: SceneBlock;
}

/**
 * True when the raw file content is the exact soft-deletion marker.
 * Surrounding whitespace is tolerated (the LLM tool wraps content).
 */
export function isSoftDeletedScene(raw: string): boolean {
  return typeof raw === "string" && raw.trim() === SCENE_DELETED_MARKER;
}

/**
 * Strictly validate a Scene Block file.
 *
 * Rules (all mandatory):
 *   - The file starts with exactly `-----META-START-----` on its own line.
 *   - Exactly one `-----META-START-----` and one `-----META-END-----`.
 *   - The META section contains exactly one `created`, `updated`, `summary`
 *     and `heat` line, in that order; duplicates/unknown fields are rejected.
 *   - `created`/`updated` are parseable ISO 8601 dates/datetimes and
 *     `created <= updated`.
 *   - `summary` is non-empty.
 *   - `heat` is a nonnegative safe integer (`active`, negatives, fractional
 *     and exponent/hex forms are rejected).
 *   - The Markdown body after `-----META-END-----` is non-empty.
 *
 * There is deliberately **no** length limit, truncation or compression: the
 * validator only checks structure, never size.
 *
 * Pure and dependency-free — safe to call from any layer.
 */
export function validateSceneBlock(raw: string, filename: string): SceneValidationResult {
  const errors: string[] = [];
  const fail = (): SceneValidationResult => ({
    valid: false,
    ok: false,
    errors,
    error: errors.join("; "),
  });

  if (typeof raw !== "string") {
    errors.push("scene content must be a string");
    return fail();
  }
  if (raw.length === 0) {
    errors.push("scene file is empty");
    return fail();
  }

  // Line-based structural parse. `\r\n` is tolerated; the delimiter lines
  // themselves must match byte-for-byte.
  const lines = raw.split(/\r?\n/);

  if (lines[0] !== META_START) {
    errors.push(`missing leading exact "${META_START}" delimiter`);
  }

  const startCount = lines.reduce((n, l) => (l === META_START ? n + 1 : n), 0);
  const endCount = lines.reduce((n, l) => (l === META_END ? n + 1 : n), 0);
  if (startCount !== 1) {
    errors.push(`expected exactly one "${META_START}" delimiter (found ${startCount})`);
  }
  if (endCount !== 1) {
    errors.push(`expected exactly one "${META_END}" delimiter (found ${endCount})`);
  }

  const searchFrom = lines[0] === META_START ? 1 : 0;
  const endLine = lines.indexOf(META_END, searchFrom);
  if (endLine === -1) {
    errors.push(`missing "${META_END}" delimiter`);
    return fail();
  }
  if (lines[0] === META_START && endLine === 0) {
    errors.push("META section is empty");
    return fail();
  }
  if (errors.length > 0) {
    // Structural mismatch — field parsing would be meaningless.
    return fail();
  }

  const metaLines = lines.slice(1, endLine);
  if (metaLines.length !== META_FIELDS.length) {
    errors.push(
      `META section must contain exactly ${META_FIELDS.length} fields in order ` +
        `(${META_FIELDS.join(", ")}); found ${metaLines.length} line(s)`,
    );
    return fail();
  }

  const values: Partial<Record<(typeof META_FIELDS)[number], string>> = {};
  const seen = new Set<string>();
  for (let i = 0; i < META_FIELDS.length; i++) {
    const expected = META_FIELDS[i]!;
    const line = metaLines[i]!;
    const match = /^([A-Za-z_][A-Za-z0-9_]*):[ \t]*(.*)$/.exec(line);
    if (!match) {
      errors.push(`META line ${i + 1} is malformed: ${JSON.stringify(line)}`);
      continue;
    }
    const name = match[1]!;
    const value = match[2]!;
    if (seen.has(name)) {
      errors.push(`duplicate META field: "${name}"`);
      continue;
    }
    seen.add(name);
    if (name !== expected) {
      errors.push(`META field ${i + 1} must be "${expected}" but found "${name}"`);
      continue;
    }
    values[expected] = value;
  }
  if (errors.length > 0) return fail();

  const created = values.created!.trim();
  const updated = values.updated!.trim();
  const summary = values.summary!.trim();
  const heatText = values.heat!.trim();

  const createdOk = isParseableIsoDateTime(created);
  const updatedOk = isParseableIsoDateTime(updated);
  if (!createdOk) {
    errors.push(`created must be a parseable ISO datetime (found ${JSON.stringify(values.created)})`);
  }
  if (!updatedOk) {
    errors.push(`updated must be a parseable ISO datetime (found ${JSON.stringify(values.updated)})`);
  }
  if (summary.length === 0) {
    errors.push("summary must be non-empty");
  }

  let heat = Number.NaN;
  if (!/^\d+$/.test(heatText)) {
    errors.push(`heat must be a nonnegative safe integer (found ${JSON.stringify(values.heat)})`);
  } else {
    heat = Number(heatText);
    if (!Number.isSafeInteger(heat) || heat < 0) {
      errors.push(`heat must be a nonnegative safe integer (found ${JSON.stringify(values.heat)})`);
    }
  }

  // Ordered bounds: creation may not be after the last update.
  if (createdOk && updatedOk) {
    const createdMs = Date.parse(created);
    const updatedMs = Date.parse(updated);
    if (createdMs > updatedMs) {
      errors.push(`created (${created}) must not be after updated (${updated})`);
    }
  }

  const body = lines.slice(endLine + 1).join("\n");
  if (body.trim().length === 0) {
    errors.push("Markdown body must be non-empty");
  }

  if (errors.length > 0) return fail();

  return {
    valid: true,
    ok: true,
    errors: [],
    block: {
      filename,
      meta: { created, updated, summary, heat },
      content: body.trim(),
    },
  };
}

function isParseableIsoDateTime(value: string): boolean {
  if (!ISO_DATETIME_RE.test(value)) return false;
  return Number.isFinite(Date.parse(value));
}

function extractMetaField(metaBlock: string, field: string): string {
  const re = new RegExp(`^${field}:\\s*(.*)$`, "m");
  const m = metaBlock.match(re);
  return m ? m[1]!.trim() : "";
}
