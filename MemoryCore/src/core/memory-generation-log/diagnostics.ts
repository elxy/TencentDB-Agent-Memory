/**
 * Diagnostics helpers for memory-generation logs.
 *
 * Two narrow, pure concerns shared by the L2/L3 runners:
 *
 * 1. Describe the *effective* system prompt actually handed to the model.
 *    The additive `memory_prompt` ref alone cannot express this: it stays
 *    `builtin` with an empty hash when no strategy is configured, even though
 *    the model received a real system prompt (builtin base or operator
 *    template, plus the mandatory contract).
 * 2. Diff local profiles before/after a generation so the log records what
 *    *this layer* produced, and never mislabels another layer's files.
 *
 * These helpers hold no state and perform no IO; they only turn values the
 * caller already has into log fields.
 */

import { createHash } from "node:crypto";

import type { ResolvedMemoryPrompt } from "../memory-prompt/types.js";
import type { ProfileRecord } from "../store/types.js";
import type { MemoryGenerationSystemPromptRef } from "./types.js";

/** Where the base system prompt (before any additive strategy) came from. */
export type SystemPromptBaseSource = "builtin" | "operator_template";

/**
 * The system prompt a generator actually built for one generation.
 * Captured by SceneExtractor right before the LLM run.
 */
export interface EffectiveSystemPromptInfo {
  /** Exact final system prompt string passed to the runner. */
  systemPrompt: string;
  /**
   * Base prompt before the additive strategy — the builtin default, or the
   * operator override with its appended mandatory contract.
   */
  baseSystemPrompt: string;
  /** Source of `baseSystemPrompt`. */
  baseSource: SystemPromptBaseSource;
}

export function sha256Hex(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function hasCustomStrategy(strategy?: ResolvedMemoryPrompt): strategy is ResolvedMemoryPrompt {
  return !!strategy && strategy.source !== "system" && strategy.prompt.trim().length > 0;
}

/**
 * Build the optional diagnostic ref describing the real system prompt.
 *
 * `effective_sha256` hashes the exact string the model received, including the
 * fixed file/provenance contract and any additive strategy overlay. The base
 * and strategy hashes are kept distinct so a reader can tell a builtin prompt
 * from an operator override, and see which strategy (if any) was overlaid.
 */
export function buildSystemPromptGenerationRef(
  effective: EffectiveSystemPromptInfo,
  strategy?: ResolvedMemoryPrompt,
): MemoryGenerationSystemPromptRef {
  const ref: MemoryGenerationSystemPromptRef = {
    effective_sha256: sha256Hex(effective.systemPrompt),
    base_source: effective.baseSource,
    base_sha256: sha256Hex(effective.baseSystemPrompt),
  };
  if (hasCustomStrategy(strategy)) {
    ref.strategy = {
      memory_prompt_id: strategy.memory_prompt_id,
      version: strategy.version,
      source: strategy.source,
      prompt_sha256: sha256Hex(strategy.prompt),
    };
  }
  return ref;
}

/**
 * Local content fingerprints captured before a generation starts.
 * Keyed by the profile's stable id.
 */
export interface LocalProfileBaseline {
  readonly contentMd5ById: ReadonlyMap<string, string>;
}

export function snapshotLocalProfiles(profiles: ProfileRecord[]): LocalProfileBaseline {
  return {
    contentMd5ById: new Map(profiles.map((profile) => [profile.id, profile.contentMd5])),
  };
}

/**
 * Select the profiles of `layer` whose content changed since the baseline.
 * Files absent from the baseline count as created. Files of other layers are
 * never returned, so an L2 run cannot label an L3 persona as its output (or
 * vice versa). This must be a pre-run local snapshot, not an empty store-pull
 * baseline; files absent from a genuine local snapshot are correctly new.
 */
export function selectChangedProfiles(
  baseline: LocalProfileBaseline,
  profiles: ProfileRecord[],
  layer: "l2" | "l3",
): ProfileRecord[] {
  return profiles.filter(
    (profile) =>
      profile.type === layer && baseline.contentMd5ById.get(profile.id) !== profile.contentMd5,
  );
}
