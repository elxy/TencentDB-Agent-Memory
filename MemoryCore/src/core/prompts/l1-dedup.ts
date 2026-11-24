/**
 * L1 Conflict Detection Prompt (Batch Mode) — 2026-10-12 收窄版
 *
 * 去重模型只做一件事：判断每条新记忆是否与候选池中某条已有记忆【完全重复】。
 *   - 完全重复 → skip（不摄入）；
 *   - 其余一律 store（新增）。
 *
 * 它不再执行 update/merge 这类替代操作（docs/memory-design-proposal.md §5.4）：
 * 相互矛盾的 L1 都保留，各自带时间，由读取时的模型调和，主 Agent 负责裁决；
 * 部分重叠但非完全重复的内容也必须 store（宁可保留冗余，不让旧认识被自动改写）。
 */

import type { MemoryPromptMode } from "../../config.js";
import type { MemoryRecord, ExtractedMemory } from "../record/l1-writer.js";

// ============================
// System Prompt
// ============================

export const CONFLICT_DETECTION_SYSTEM_PROMPT = `你是记忆重复检测器。批量比较多条【新记忆】与【统一候选记忆池】中的已有记忆，逐条判断：这条新记忆是否与某条已有记忆**完全重复**。

## 核心规则

- 你的选择只有两个：
  - "store"：新增这条记忆（默认）。
  - "skip"：它与候选池中某条已有记忆**语义完全重复**，没有任何新增信息（措辞可以不同，但表述的事实、条件、范围完全一致）。
- **保守原则**：只要有任何一点新信息、条件差异、范围差异或时间差异，都必须 store。矛盾**不是**重复——与旧记忆冲突的新记忆也要 store，由读取方调和。
- 你不被允许改写、合并、替代任何已有记忆。

## 输出格式

严格输出 JSON 数组，每个元素对应一条新记忆的决策。不输出任何其他内容：

[
  { "record_id": "新记忆的 record_id", "action": "store | skip" }
]`;

export const WORK_CONFLICT_DETECTION_SYSTEM_PROMPT = `你是团队工作记忆重复检测器。批量比较多条【新记忆】与【统一候选记忆池】中的已有工作记忆，逐条判断：这条新记忆是否与某条已有记忆**完全重复**。

## 核心规则

- 你的选择只有两个：
  - "store"：新增这条记忆（默认）。
  - "skip"：它与候选池中某条已有记忆**语义完全重复**，没有任何新增信息。
- **保守原则**：只要事实、任务、方法或资产有任何增量（进展、条件、范围、时间、责任人之差异），都必须 store。与旧记忆冲突也 store。
- 你不被允许改写、合并、替代任何已有记忆。

## 输出格式

严格输出 JSON 数组，每个元素对应一条新记忆的决策。不输出任何其他内容：

[
  { "record_id": "新记忆的 record_id", "action": "store | skip" }
]`;

export function getConflictDetectionSystemPrompt(mode: MemoryPromptMode = "chat"): string {
  return mode === "code" ? WORK_CONFLICT_DETECTION_SYSTEM_PROMPT : CONFLICT_DETECTION_SYSTEM_PROMPT;
}

// ============================
// Prompt Builder
// ============================

/**
 * Candidate search result for a single new memory.
 */
export interface CandidateMatch {
  newMemory: ExtractedMemory & { record_id: string };
  candidates: MemoryRecord[];
}

/**
 * Format the batch conflict detection prompt using a unified candidate pool.
 *
 * Format (aligned with prototype):
 * 1. Unified candidate pool: de-duplicated list of all existing candidates across all new memories
 * 2. Per new memory: content + list of related candidate IDs from the pool
 *
 * @param matches - Array of new memories with their candidate matches
 */
export function formatBatchConflictPrompt(matches: CandidateMatch[]): string {
  // Step 1: Build unified candidate pool (de-duplicate across all new memories)
  const unifiedPool = new Map<string, MemoryRecord>();
  const perMemoryCandidateIds = new Map<string, string[]>();

  for (const m of matches) {
    const candidateIds: string[] = [];
    for (const c of m.candidates) {
      if (!unifiedPool.has(c.id)) {
        unifiedPool.set(c.id, c);
      }
      candidateIds.push(c.id);
    }
    perMemoryCandidateIds.set(m.newMemory.record_id, candidateIds);
  }

  // Step 2: Format unified pool as JSON
  const poolList = Array.from(unifiedPool.values()).map((c) => ({
    record_id: c.id,
    content: c.content,
    type: c.type,
    priority: c.priority,
    scene_name: c.scene_name,
    timestamps: c.timestamps,
  }));

  let poolSection: string;
  if (poolList.length === 0) {
    poolSection = "## 统一候选记忆池\n\n（空，没有已有记忆，所有新记忆直接 store）";
  } else {
    const poolStr = JSON.stringify(poolList, null, 2);
    poolSection = `## 统一候选记忆池（共 ${poolList.length} 条已有记忆）\n\n${poolStr}`;
  }

  // Step 3: Format each new memory with its related candidate IDs
  const memoryParts = matches.map((m, idx) => {
    const relatedIds = perMemoryCandidateIds.get(m.newMemory.record_id) ?? [];
    const relatedNote =
      relatedIds.length > 0
        ? JSON.stringify(relatedIds)
        : "[]（无相似候选，直接 store）";

    const memStr = JSON.stringify(
      {
        record_id: m.newMemory.record_id,
        content: m.newMemory.content,
        type: m.newMemory.type,
        priority: m.newMemory.priority,
        scene_name: m.newMemory.scene_name,
      },
      null,
      2,
    );

    return `### 第 ${idx + 1} 条新记忆 (record_id: ${m.newMemory.record_id})\n${memStr}\n\n【关联候选 ID】${relatedNote}`;
  });

  const newMemoriesText = memoryParts.join(
    "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n",
  );

  // Step 4: Assemble final prompt
  return `${poolSection}

${"═".repeat(50)}

## 待判断的新记忆（共 ${matches.length} 条）

${newMemoriesText}

请逐条判断并输出决策 JSON 数组：与候选池中某条记忆**完全重复**的输出 action=skip，其余一律 action=store。当某条新记忆的候选列表为空时，该条直接输出 action=store。`;
}
