/**
 * Explicit L2 system-template replacement.
 *
 * `persona.sceneSystemTemplate` (L2 scene extraction) is trusted
 * operator configuration. When set, the supplied template fully replaces the
 * built-in system prompt for that layer. A minimal mandatory
 * storage/provenance/file contract is then appended so an override can never
 * relax the layer's file-scope, META-header or evidence rules.
 *
 * Guarantees:
 *   - The template is NEVER derived from memory payloads or model output.
 *   - Undefined (the default) preserves the built-in system prompt
 *     byte-for-byte; selection therefore happens *before*
 *     `composeMemorySystemPrompt`, which keeps the additive memoryPrompt
 *     strategy behaviour intact.
 *   - The appended contract does not grant any permission. The runtime
 *     filePolicy remains the sole authority on what the LLM may read/write.
 *   - There is intentionally NO length limit, truncation, compression or
 *     forced-generality pass applied to an explicit override.
 */

import type { MemoryPromptLayer } from "./types.js";

/** Layers that support an explicit operator system-template override. */
export type SystemTemplateLayer = Extract<MemoryPromptLayer, "l2">;

/**
 * Mandatory L2 (scene extraction) storage/provenance/file contract.
 * Appended to every explicit L2 override.
 */
export const L2_SYSTEM_TEMPLATE_CONTRACT = `----- MANDATORY STORAGE / PROVENANCE / FILE CONTRACT (L2) -----
These rules are mandatory instructions. File access is enforced by runtime filePolicy; evidence/content fidelity requires output verification and is not guaranteed by filePolicy.
1. FILE SCOPE: operate only on direct \`.md\` files at the root of the scene-block workspace. Never create, read, edit or delete indexes, checkpoints, databases, JSONL records, nested directories or hidden/dot files.
2. REQUIRED META: every created or updated file must begin EXACTLY with these lines (substitute values, preserve delimiters):
-----META-START-----
created: <original ISO-8601 creation timestamp>
updated: <current ISO-8601 timestamp>
summary: <natural-language summary>
heat: 0
-----META-END-----

<Markdown body>
Heat must be a nonnegative integer, never active. Do not use plain META heading, JSON or a code fence. Preserve original created timestamp on update.
3. NO SYSTEM EDITS: never modify the scene index, checkpoints, navigation or any engineering-maintained system file.
4. EVIDENCE: derive scenes, preferences and implicit signals only from the supplied memories. Do not fabricate evidence or applicability conditions; omit a field when the input is insufficient.
The template above grants no permission beyond the runtime filePolicy, which remains authoritative.
----- END MANDATORY CONTRACT -----`;

/**
 * Return the mandatory contract for a layer.
 */
export function systemTemplateContract(layer: SystemTemplateLayer): string {
  return L2_SYSTEM_TEMPLATE_CONTRACT;
}

/**
 * Validate an operator-supplied system template.
 *
 * - `undefined` → `undefined` (use the built-in default).
 * - non-blank string → returned verbatim (no trimming, length limit or
 *   compression).
 * - anything else (null, non-string, blank/whitespace-only) → throw, so
 *   malformed configuration fails fast instead of silently falling back.
 *
 * @param fieldName Operator-facing config path used in the error message.
 */
export function validateSystemTemplate(
  value: unknown,
  fieldName = "systemTemplate",
): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(
      `invalid ${fieldName}: ${JSON.stringify(value)} (expected a non-blank string)`,
    );
  }
  return value;
}

/**
 * Select the effective system prompt for a layer.
 *
 * When `override` is undefined the built-in `defaultSystemPrompt` is returned
 * byte-for-byte. When present, the override replaces it and the layer's
 * mandatory contract is appended. Call this *before*
 * `composeMemorySystemPrompt()` so the additive memoryPrompt strategy
 * behaviour is preserved.
 */
export function resolveSystemTemplate(
  defaultSystemPrompt: string,
  override: string | undefined,
  layer: SystemTemplateLayer,
): string {
  if (override === undefined) return defaultSystemPrompt;
  return `${override}\n\n${systemTemplateContract(layer)}`;
}
