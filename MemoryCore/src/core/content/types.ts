/**
 * Content-source contracts (Core limited change A).
 *
 * These types describe *program-provided* content relationships that the
 * content APIs may return directly. They are intentionally separate from the
 * best-effort generation logs: a caller must never have to grep internal
 * diagnostic files to learn where a memory came from.
 *
 * Two distinct notions of "source" are kept apart on purpose:
 *
 *   - L1 `source_message_ids`: the **model-declared** citation. It is a claim,
 *     not proof that the program verified the message contributed.
 *   - L2/L3 `ContentSourceEntry`: the **program-provided input scope** — the
 *     exact L1/L2 records that this pipeline run fed to the model. This is
 *     evidence of participation, not a per-sentence attribution.
 *
 * Unknown is represented explicitly (`status: "unknown"`), never as an empty
 * list that could be misread as "there was no evidence". A sidecar that exists
 * but cannot be read/parsed is `unavailable` with a diagnostic — it is NOT the
 * same as "this record was never assigned sources".
 */

import type { StorageAdapter } from "../storage/adapter.js";

/** Which layer owns a persisted content-source relation. */
export type ContentSourceLayer = "l2" | "l3";

/**
 * How the input relationship was established.
 *
 * Only `pipeline_input` exists today: the ids were assembled by the pipeline
 * as this run's input set. The field is explicit so a future model-declared
 * citation kind cannot be confused with program-provided evidence.
 */
export type ContentSourceKind = "pipeline_input";

export interface ContentSourceScope {
  team_id?: string;
  agent_id?: string;
  user_id?: string;
  session_id?: string;
}

/**
 * One persisted, program-provided input relationship for an L2/L3 content item.
 *
 * Persisted together with the generated content (not derived from logs), keyed
 * by `(layer, path)` so a single internal sidecar can hold every relation.
 */
export interface ContentSourceEntry {
  schema_version: 1;
  layer: ContentSourceLayer;
  /** Stable content addressing: L2 scene path, or `persona.md` for L3. */
  path: string;
  /** Store/profile id of the generated content, when one exists. */
  content_id: string;
  source_kind: ContentSourceKind;
  /** Layer of the input ids (`l1` for L2 output, `l2` for L3 output). */
  input_layer: "l1" | "l2";
  /** Ids of the records that were this run's input. */
  input_ids: string[];
  /**
   * Ids inherited from the previous relation for this output because the
   * content they supported is still retained. Kept separate from `input_ids`
   * so a consumer can distinguish this batch's inputs from retained history.
   * Bounded (see MAX_CONTENT_SOURCE_INPUT_IDS); never auto-fabricated.
   */
  retained_input_ids?: string[];
  scope: ContentSourceScope;
  updated_at_ms: number;
  /**
   * SHA-256 (hex) of the exact published content bytes this relation belongs
   * to. A read that computes a different hash treats the relation as unknown:
   * the sidecar must never be attached to content it was not written for.
   */
  content_sha256: string;
}

/** The on-disk sidecar shape (`.metadata/content_sources.json`). */
export interface ContentSourceFile {
  schema_version: 1;
  entries: ContentSourceEntry[];
}

/** Normalized view returned by the content APIs. */
export type ContentSourceView =
  | {
      status: "known";
      kind: ContentSourceKind;
      input_layer: "l1" | "l2";
      input_ids: string[];
      /** Previously retained inputs whose contribution survives this update. */
      retained_input_ids?: string[];
      updated_at_ms: number;
    }
  | {
      status: "unknown";
      input_ids: [];
    }
  | {
      /** The sidecar exists but could not be read/validated. Never `known`. */
      status: "unavailable";
      input_ids: [];
      /** Machine-readable diagnostic for operators, safe to expose. */
      reason: string;
    };

/**
 * Normalized view of an L1 memory's model-declared source messages.
 *
 * `declared` only means the model cited at least one id that was part of this
 * extraction's *addressable input set*. It is NOT a program verification that
 * the message contributed to the memory — ids outside the input batch are
 * dropped (with a count-only warning), and a dropped id is not "proven".
 * `unknown` means no usable citation survived (empty after validation) — the
 * API must not claim "no source".
 */
export interface L1SourceView {
  status: "declared" | "unknown";
  kind: "model_declared";
  message_ids: string[];
}

/** Build the unknown L1 source view (well-typed empty literal). */
export function unknownL1Source(): L1SourceView {
  return { status: "unknown", kind: "model_declared", message_ids: [] };
}

/** Build the unknown content-source view. */
export function unknownContentSource(): ContentSourceView {
  return { status: "unknown", input_ids: [] };
}

/** Build the unavailable content-source view (sidecar read/validation failed). */
export function unavailableContentSource(reason: string): ContentSourceView {
  return { status: "unavailable", input_ids: [], reason };
}

// ============================
// L2/L3 before-commit contract (父代理/可靠性生成器整合点)
// ============================
//
// The generator owns the working view in which the profiles are written. To
// avoid the old "publish content, then best-effort append a sidecar" race, the
// generator may accept an optional `beforeCommit(view, result)` hook. The hook
// runs inside the same working view, writes `content_sources` for exactly the
// changed outputs using result.input_ids, and returns normally to let the
// generator publish. If the hook throws, the generator MUST NOT publish.

/** One L2/L3 output that this generation actually changed. */
export interface ContentSourceCommitOutput {
  /** Store/profile stable id of the generated content. */
  content_id: string;
  /** L2 scene filename, or `persona.md` for L3. */
  path: string;
  /** Exact final bytes that will be published for this output. */
  content: string;
}

/**
 * Everything the hook needs to persist `content_sources` for one generation.
 * `input_ids` are ONLY the ids this run actually read into the prompt — never
 * the whole scene index and never a fabricated citation.
 */
export interface ContentSourceCommitResult {
  layer: ContentSourceLayer;
  /** Only the outputs whose content changed in this generation. */
  changed_outputs: ContentSourceCommitOutput[];
  /** `l1` for L2 output, `l2` for L3 output. */
  input_layer: "l1" | "l2";
  input_ids: string[];
  scope: ContentSourceScope;
}

/**
 * Optional generator hook. Runs before the generator commits/publishes the
 * changed outputs. Throwing aborts the publication (fail-closed).
 */
export type BeforeCommitHook = (
  view: StorageAdapter,
  result: ContentSourceCommitResult,
) => Promise<void>;
