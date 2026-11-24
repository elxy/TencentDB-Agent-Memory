/**
 * L1 Memory Writer: writes extracted memories to JSONL files.
 *
 * File naming: records/YYYY-MM-DD.jsonl (daily shards, all sessions merged).
 * Each record includes sessionKey for traceability.
 *
 * Write strategy:
 * - JSONL is the append-only persistent store (source of truth for backup/recovery).
 * - VectorStore (SQLite) is the primary retrieval engine.
 * 2026-10-12 收窄：支持且仅支持 store（追加新认识）与 skip（跳过完全重复）。
 * 纠正与矛盾以新 L1 追加表达，不再原地替代旧记录。
 *
 * Supports only store (append) and skip operations.
 *
 * v3: Aligned with Kenty's prompt output format — 3 memory types (persona/episodic/instruction),
 * numeric priority, scene_name, source_message_ids, metadata, timestamps.
 */

import crypto from "node:crypto";
import assert from "node:assert";
import { DEFAULT_ISOLATION_ID, type IMemoryStore } from "../store/types.js";
import type { EmbeddingService } from "../store/embedding.js";
import type { StorageAdapter } from "../storage/adapter.js";
import { StoragePaths } from "../storage/types.js";
import type { Logger } from "../types.js";
import type { L1HistoryRun } from "../content/generation-history.js";

// ============================
// Types
// ============================

/** L1 memory types: chat-mode legacy types + code/work-mode team memory types. */
export type MemoryType =
  | "persona"
  | "episodic"
  | "instruction"
  | "work_fact"
  | "work_task"
  | "work_method"
  | "work_artifact";

/** Metadata for episodic memories (activity time range) */
export interface EpisodicMetadata {
  activity_start_time?: string; // ISO 8601
  activity_end_time?: string; // ISO 8601
}

/**
 * A persisted memory record in L1 JSONL files.
 *
 * v3 changes from v2:
 * - `importance: "high"|"medium"|"low"` → `priority: number` (0-100, -1 for strict global instructions)
 * - Added `scene_name`, `source_message_ids`, `metadata`, `timestamps`
 * - Removed `keywords` (will be rebuilt from content for search)
 * - MemoryType reduced from 4 to 3 (removed "preference", folded into "persona")
 */
export interface MemoryRecord {
  /** Unique ID for dedup updates */
  id: string;
  /** Memory content */
  content: string;
  /** Memory type: persona / episodic / instruction */
  type: MemoryType;
  /** Priority score: 0-100 (higher = more important), -1 = strict global instruction */
  priority: number;
  /** Scene name this memory belongs to */
  scene_name: string;
  /** Source message IDs that contributed to this memory */
  source_message_ids: string[];
  /** Type-specific metadata (e.g., activity_start_time for episodic) */
  metadata: EpisodicMetadata | Record<string, never>;
  /** Timestamp trail: all timestamps related to this memory (for merge history tracking) */
  timestamps: string[];
  /** Creation timestamp (ISO) */
  createdAt: string;
  /** Last update timestamp (ISO) */
  updatedAt: string;
  /** Monotonic version. New memories start at 1; update/merge increments by 1. */
  version?: number;
  /** Source session key (conversation channel identifier) */
  sessionKey: string;
  /** Source session ID (single conversation instance identifier) */
  sessionId: string;
  /** Optional task dimension for L0/L1 filtering. */
  taskId?: string;
  /**
   * Three-dim tenancy isolation (new in this branch).
   *
   * `userId` / `agentId` are mandatory for new writes once gateway-level
   * isolation enforcement is on, but kept optional on the type to avoid
   * breaking pre-isolation call sites and tests during rollout. The SQLite
   * upsert defaults them to '' if missing; the migration script backfills
   * existing rows with `__legacy__`.
   *
   * See `docs/l0l3-tenant-isolation-design.md`.
   */
  teamId?: string;
  userId?: string;
  agentId?: string;
}

/**
 * A memory as extracted by LLM (before dedup / persistence).
 * Matches the output format of Kenty's extraction prompt.
 */
export interface ExtractedMemory {
  content: string;
  type: MemoryType;
  priority: number;
  source_message_ids: string[];
  metadata: EpisodicMetadata | Record<string, never>;
  /** Scene name this memory was extracted in */
  scene_name: string;
}

/**
 * 2026-10-12 收窄（docs/memory-design-proposal.md §5.4）：自动提炼只新增
 * （store），或跳过完全重复的内容（skip），不再由去重模型执行 update/merge
 * 这类替代操作。相互矛盾的 L1 都保留，由读取时的模型判断，主 Agent 负责裁决。
 */
export type DedupAction = "store" | "skip";

/** Batch dedup decision — one per new memory: store it, or skip it as a duplicate. */
export interface DedupDecision {
  /** Which new memory this decision is about */
  record_id: string;
  action: DedupAction;
}

const TAG = "[memory-tdai][l1-writer]";

// ============================
// Core functions
// ============================

/**
 * Generate a unique memory ID.
 */
export function generateMemoryId(): string {
  return `m_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
}

/**
 * Write a memory record according to the dedup decision.
 *
 * - store: append new record
 * - skip: do nothing
 *
 * 收窄后没有 update/merge 写入路径（2026-10-12）。
 */
export async function writeMemory(params: {
  memory: ExtractedMemory;
  decision: DedupDecision;
  dedupAcrossSessions?: boolean;
  baseDir: string;
  sessionKey: string;
  sessionId?: string;
  taskId?: string;
  /** Tenancy isolation propagated into MemoryRecord and downstream store. */
  teamId?: string;
  userId?: string;
  agentId?: string;
  logger?: Logger;
  /** Optional vector store for dual-write (JSONL + vector DB) */
  vectorStore?: IMemoryStore;
  /** Optional embedding service (required when vectorStore is provided) */
  embeddingService?: EmbeddingService;
  /** StorageAdapter for file operations (COS/local). Falls back to fs when absent. */
  storage?: StorageAdapter;
  /**
   * History-mode commit context. When present the write is a FORMAL content
   * commit through `store.commitContentChanges` (current row + FTS/vector +
   * immutable version in one transaction); the legacy append/upsertL1 path is
   * bypassed entirely and any failure throws instead of degrading to
   * "store as new".
   */
  history?: {
    run: L1HistoryRun;
    /** Deterministic commit time derived from the exact input versions. */
    recordedAt: string;
  };
}): Promise<MemoryRecord | null> {
  const { memory, decision, baseDir, sessionKey, sessionId, taskId, teamId, userId, agentId, logger, vectorStore, embeddingService, storage, history } = params;

  if (decision.action === "skip") {
    logger?.debug?.(`${TAG} Skipping memory: ${memory.content.slice(0, 50)}...`);
    return null;
  }
  // 2026-10-12 收窄：去重只有新增或跳过完全重复两条路，替代式 update/merge 已
  // 撤掉。遗留调用方若仍传 update/merge，立即显式失败，绝不静默改成“新增一条”。
  assert.ok(
    decision.action === "store",
    `${TAG} dedup_decision_invalid: 写入只允许 store/skip决策，非法 action：${String((decision as { action: unknown }).action)}`,
  );

  const strict = params.dedupAcrossSessions === true;
  if (strict && !teamId && !userId && !agentId) throw new Error("Cross-session write requires identity");
  const now = history ? history.recordedAt : new Date().toISOString();

  // 收窄后 dedup 只回答 store/skip：写入的始终是提炼出的原文。
  const finalContent = memory.content;
  const finalType: MemoryType = memory.type;
  const finalPriority = memory.priority;
  const finalTimestamps = [now];

  const record: MemoryRecord = {
    id: decision.record_id || generateMemoryId(),
    content: finalContent,
    type: finalType,
    priority: finalPriority,
    scene_name: memory.scene_name,
    source_message_ids: memory.source_message_ids,
    metadata: memory.metadata,
    timestamps: finalTimestamps,
    createdAt: now,
    updatedAt: now,
    version: 0,
    sessionKey,
    sessionId: sessionId || DEFAULT_ISOLATION_ID,
    taskId,
    teamId,
    // Tenancy isolation — propagated end-to-end so SQLite / TCVDB upsert
    // can persist the row's owner. Empty strings preserve pre-isolation
    // behaviour for callers that haven't been updated yet.
    userId: userId || DEFAULT_ISOLATION_ID,
    agentId: agentId || DEFAULT_ISOLATION_ID,
  };

  const shardDate = formatLocalDate(new Date());
  const recordKey = StoragePaths.record(shardDate);

  // Helper: append a JSONL line
  // - standalone (no storage): write to local fs
  // - service (storage provided): write via StorageAdapter, no fs fallback
  //
  // Guard log (CR-2 fix, 2026-05-19): if storage is absent, emit a warn so any
  // missed wiring (e.g. caller forgot to pass storage in service mode) is
  // immediately visible instead of silently writing to ephemeral pod fs.
  // In standalone mode this warn is benign — the gateway auto-wires a
  // LocalStorageBackend at startup (server.ts:199-203), so storage should
  // normally be defined. Seeing this warn = caller forgot to pass it.
  const appendRecord = async (line: string) => {
    if (storage) {
      await storage.appendFile(recordKey, line);
    } else {
      logger?.warn?.(
        `${TAG} [CR-2 guard] writeMemory called without storage adapter; ` +
        `falling back to local fs at ${baseDir}/records/${shardDate}.jsonl. ` +
        `In service mode this means JSONL is written to ephemeral pod fs and ` +
        `will be lost on restart. Caller must pass 'storage' to writeMemory.`,
      );
      const fs = await import("node:fs/promises");
      const path = await import("node:path");
      const recordsDir = path.default.join(baseDir, "records");
      await fs.default.mkdir(recordsDir, { recursive: true });
      await fs.default.appendFile(path.default.join(recordsDir, `${shardDate}.jsonl`), line, "utf-8");
    }
  };

  if (history) {
    // Formal history write. The embedding is computed first (easy-to-fail), then
    // the record commits through the store entry. A failure here propagates: an
    // uncommitted record must NEVER be resurrected as a new row, and the
    // checkpoint must not advance on a partial success.
    let embedding: Float32Array | undefined;
    if (embeddingService) {
      try {
        embedding = await embeddingService.embed(record.content);
      } catch (embedErr) {
        logger?.warn?.(
          `${TAG} [history] embedding failed for id=${record.id}; committing metadata/FTS only: ${embedErr instanceof Error ? embedErr.message : String(embedErr)}`,
        );
      }
    }
    await history.run.commitRecord(record, embedding);
    // JSONL is a non-authoritative mirror and cannot join the store transaction;
    // write it only after the authoritative commit so it never represents an
    // uncommitted candidate. A mirror failure is non-fatal.
    try {
      await appendRecord(JSON.stringify(record) + "\n");
    } catch (err) {
      logger?.warn?.(`${TAG} JSONL append after history commit failed (non-fatal, store committed): ${err instanceof Error ? err.message : String(err)}`);
    }
    logger?.debug?.(`${TAG} history ${decision.action} memory: ${record.id}: ${finalContent.slice(0, 80)}...`);
    return record;
  }

  // store: append a new line
  try {
    await appendRecord(JSON.stringify(record) + "\n");
  } catch (err) {
    if (strict) throw err;
    logger?.warn?.(`${TAG} JSONL append failed (non-fatal, VDB write continues): ${err instanceof Error ? err.message : String(err)}`);
  }
  logger?.debug?.(`${TAG} Stored memory ${record.id}: ${finalContent.slice(0, 80)}...`);

  // === Vector Store dual-write ===
  if (vectorStore) {
    try {
      logger?.debug?.(
        `${TAG} [vec-dual-write] START id=${record.id}, contentLen=${record.content.length}, ` +
        `content="${record.content.slice(0, 80)}..."`,
      );

      let embedding: Float32Array | undefined;

      if (embeddingService) {
        try {
          embedding = await embeddingService.embed(record.content);
          logger?.debug?.(
            `${TAG} [vec-dual-write] Embedding OK: dims=${embedding.length}, ` +
            `norm=${Math.sqrt(Array.from(embedding).reduce((s, v) => s + v * v, 0)).toFixed(4)}`,
          );
        } catch (embedErr) {
          // Embedding failed — pass undefined to upsert() which writes
          // metadata + FTS only, skipping the vec0 table.
          logger?.warn(
            `${TAG} [vec-dual-write] Embedding FAILED for id=${record.id}, ` +
            `will write metadata only: ${embedErr instanceof Error ? embedErr.message : String(embedErr)}`,
          );
        }
      }

      const upsertOk = await vectorStore.upsertL1(record, embedding);
      if (strict && !upsertOk) throw new Error("Authoritative memory write rejected");
      logger?.debug?.(`${TAG} [vec-dual-write] upsert result=${upsertOk} id=${record.id}`);
    } catch (err) {
      if (strict) throw err;
      // Vector write failure should NOT block the main JSONL write
      logger?.warn?.(
        `${TAG} [vec-dual-write] FAILED (JSONL already written) id=${record.id}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  } else {
    logger?.debug?.(
      `${TAG} [vec-dual-write] SKIPPED id=${record.id}: vectorStore=${!!vectorStore}`,
    );
  }

  return record;
}

// ============================
// Helpers
// ============================

function formatLocalDate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}