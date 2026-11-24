/**
 * SceneExtractor: LLM-driven memory extraction into scene blocks.
 *
 * Replaces the keyword-based SceneManager.processNewMemories() with an
 * LLM agent that autonomously reads/writes scene block files using tools.
 *
 * Security: The LLM is sandboxed — workspaceDir is set to scene_blocks/
 * so it can ONLY operate on .md scene files. System files (checkpoint,
 * scene_index, persona.md) are physically invisible to the LLM.
 *
 * Flow (2026-10-12 收窄):
 *   1. Backup + load scene index + build summaries
 *   2. Assemble extraction prompt with memories + scene context
 *   3. Run via CleanContextRunner (tools enabled, sandboxed to scene_blocks/)
 *   4. Cleanup: remove soft-deletes, sync index, atomic publish
 *
 * 收窄：不再维护 persona.md 导航（L3 三份文档只由主 Agent 写入）；不再有
 * out-of-band persona 更新信号与 SCENE_OPERATIONS 声明通道；"改名/合并/拆分"
 * 专题就是写新文件 + 把旧文件显式写为 [DELETED]，按名称+时间读取。
 */

import { CleanContextRunner } from "../../utils/clean-context-runner.js";
import { CheckpointManager } from "../../utils/checkpoint.js";
import { BackupManager } from "../../utils/backup.js";
import { readSceneIndex, syncSceneIndex } from "../scene/scene-index.js";
import type { SceneIndexEntry } from "../scene/scene-index.js";
import { parseSceneBlock, validateSceneBlock, isSoftDeletedScene } from "../scene/scene-format.js";
import { normalizeSceneFilenames } from "./filename-normalizer.js";
import type { MemoryPromptMode } from "../../config.js";
import { buildSceneExtractionPrompt } from "../prompts/scene-extraction.js";
import { report } from "../report/reporter.js";
import { reportL2LatencyMetrics } from "../report/metric-tracking-l2-latency.js";
import type { LLMRunner, Logger, TraceContext } from "../types.js";
import { buildTraceParams } from "../types.js";
import type { StorageAdapter } from "../storage/adapter.js";
import { GenerationWorkspace } from "../storage/generation-workspace.js";
import type { BeforeCommitCallback } from "../storage/generation-workspace.js";
import { StoragePaths } from "../storage/types.js";
import type { ResolvedMemoryPrompt } from "../memory-prompt/types.js";
import { composeMemorySystemPrompt } from "../memory-prompt/composer.js";
import { resolveSystemTemplate, validateSystemTemplate } from "../memory-prompt/template.js";
import { SCENE_BLOCK_FILE_POLICY } from "../../adapters/standalone/memory-file-policy.js";
import type { EffectiveSystemPromptInfo } from "../memory-generation-log/diagnostics.js";
import type { ProfileHistoryRun } from "../content/generation-history.js";

const TAG = "[memory-tdai] [extractor]";

type ExtractorLogger = Logger;

export interface ExtractionResult {
  memoriesProcessed: number;
  success: boolean;
  error?: string;
  /** True if LLM ran but produced no file changes (scene_index unchanged). */
  emptyExtraction?: boolean;
  /** Scene files that failed strict format validation (filenames only). */
  invalidSceneFiles?: string[];
  /**
   * Set only when a post-validation rollback could not be completed. Kept
   * distinct from `error` so a failed rollback is never reported as success.
   */
  rollbackError?: string;
}

export interface SceneExtractorOptions {
  dataDir: string;
  config: unknown;
  model?: string;
  maxScenes?: number;
  /** Prompt family for L2 scene extraction (default: chat). */
  promptMode?: MemoryPromptMode;
  /** Resolved custom strategy. Undefined preserves the current system prompt exactly. */
  memoryPrompt?: ResolvedMemoryPrompt;
  /**
   * Explicit L2 system-template override (trusted operator config, typically
   * `persona.sceneSystemTemplate`). Undefined preserves the built-in system
   * prompt byte-for-byte; a provided template fully replaces it and gets the
   * mandatory L2 storage/provenance/file contract appended.
   */
  systemTemplate?: string;
  sceneBackupCount?: number;
  timeoutMs?: number;
  logger?: ExtractorLogger;
  /** Plugin instance ID for metric reporting (optional) */
  instanceId?: string;
  /**
   * Host-neutral LLM runner. When provided, used instead of creating
   * a CleanContextRunner (decouples from OpenClaw runtime).
   * Must be configured with `enableTools: true`.
   */
  llmRunner?: LLMRunner;
  /** StorageAdapter for file operations (COS/local). Falls back to fs when absent. */
  storage?: StorageAdapter;
  /**
   * Optional program hook run inside the workspace just before the atomic
   * publish. Used to write program-owned sidecars (e.g. content sources) that
   * must ship with the generated content. A throw aborts the publish.
   */
  beforeCommit?: BeforeCommitCallback;
  /** langfuse 上报身份四元组（team/user/agent/session）。传下来会填充 trace 顶级字段。 */
  traceContext?: TraceContext;
  /**
   * History-mode run. When present, the exact L1 inputs are registered before
   * the prompt, the prior scene/persona versions are captured before the model,
   * and the publish carries a program-only {@link GenerationHistoryPlan}.
   */
  history?: ProfileHistoryRun;
}

export class SceneExtractor {
  private dataDir: string;
  private runner: LLMRunner;
  private maxScenes: number;
  private sceneBackupCount: number;
  private promptMode: MemoryPromptMode;
  private memoryPrompt: ResolvedMemoryPrompt | undefined;
  private systemTemplate: string | undefined;
  private timeoutMs: number;
  private logger: ExtractorLogger | undefined;
  private instanceId: string | undefined;
  private storage: StorageAdapter | undefined;
  private beforeCommit: BeforeCommitCallback | undefined;
  private traceContext: TraceContext | undefined;
  private history: ProfileHistoryRun | undefined;
  /** Diagnostic capture of the last prompt built for the LLM (see getter). */
  private lastEffectiveSystemPrompt: EffectiveSystemPromptInfo | undefined;

  constructor(opts: SceneExtractorOptions) {
    this.dataDir = opts.dataDir;
    this.maxScenes = opts.maxScenes ?? 15;
    this.promptMode = opts.promptMode ?? "chat";
    this.memoryPrompt = opts.memoryPrompt;
    this.systemTemplate = validateSystemTemplate(opts.systemTemplate, "persona.sceneSystemTemplate");
    this.sceneBackupCount = opts.sceneBackupCount ?? 10;
    this.timeoutMs = opts.timeoutMs ?? 300_000; // 5 min — LLM may do multiple tool calls
    this.logger = opts.logger;
    this.instanceId = opts.instanceId;
    this.storage = opts.storage;
    this.beforeCommit = opts.beforeCommit;
    this.traceContext = opts.traceContext;
    this.history = opts.history;

    // Use injected LLMRunner if available, otherwise fall back to CleanContextRunner
    this.runner = opts.llmRunner ?? new CleanContextRunner({
      config: opts.config,
      modelRef: opts.model,
      enableTools: true,
      logger: opts.logger,
    });

    this.logger?.debug?.(`${TAG} Created: dataDir=${opts.dataDir}, model=${opts.model ?? "(default)"}, promptMode=${this.promptMode}, maxScenes=${this.maxScenes}, timeout=${this.timeoutMs}ms`);
  }

  /**
   * The system prompt handed to the runner in the last extract() call, plus
   * its base/override provenance. Undefined until the prompt has been built.
   * Diagnostic only — never fed back into generation.
   */
  getLastEffectiveSystemPrompt(): EffectiveSystemPromptInfo | undefined {
    return this.lastEffectiveSystemPrompt;
  }

  /**
   * Extract a batch of memories into scene blocks using the LLM agent.
   *
   * @param memories - Array of raw memory records from the API
   * @returns Extraction result with count and success flag
   */
  async extract(memories: Array<{ content: string; created_at: string; id?: string }>): Promise<ExtractionResult> {
    const extractStartMs = Date.now();
    this.logger?.info(`${TAG} extract() start: ${memories.length} memories`);

    if (memories.length === 0) {
      this.logger?.debug?.(`${TAG} extract() skipped: no memories`);
      return { memoriesProcessed: 0, success: true };
    }

    // ── Restricted generation workspace ─────────────────────────────────
    // From here on every content read/write goes through a private working
    // view. Published scene_blocks / persona / scene_index are only updated
    // when the run completes validation + post-processing and commit()
    // succeeds. Raw-FS mode seeds a physical staging copy; StorageAdapter
    // mode uses a read-through/write-back overlay that never touches the
    // published store until commit.
    const baseStorage = this.storage;
    const baseDataDir = this.dataDir;
    const workspace = await GenerationWorkspace.begin({
      storage: baseStorage,
      dataDir: baseDataDir,
      kind: "l2",
      logger: this.logger,
      beforeCommit: this.beforeCommit,
      ...(this.history ? { historyContext: this.history.createPlan() } : {}),
    });
    this.storage = workspace.view;
    this.dataDir = workspace.activeDataDir;
    try {

    const sceneBlocksDir = this.storage ? StoragePaths.sceneBlocksDir : await (async () => { const path = await import("node:path"); return path.default.join(this.dataDir, "scene_blocks"); })();
    const metadataDir = this.storage ? StoragePaths.metadataDir : await (async () => { const path = await import("node:path"); return path.default.join(this.dataDir, ".metadata"); })();

    // Ensure directories exist
    if (this.storage) {
      await this.storage.mkdir(StoragePaths.sceneBlocksDir);
      await this.storage.mkdir(StoragePaths.metadataDir);
    } else {
      const fs = await import("node:fs/promises");
      await fs.default.mkdir(sceneBlocksDir, { recursive: true });
      await fs.default.mkdir(metadataDir, { recursive: true });
    }

    // Phase 1: Backup
    const backupStartMs = Date.now();
    const cpManager = new CheckpointManager(baseDataDir, this.logger, baseStorage);
    const cp = await cpManager.read();
    // BackupManager is only meaningful for local-fs deployments. In service
    // mode (StorageAdapter / COS) snapshot/restore is the storage backend's
    // responsibility, so `bm` stays undefined and the catch-block restore
    // below short-circuits to a no-op.
    let bm: BackupManager | undefined;
    if (!this.storage) {
      const path = await import("node:path");
      bm = new BackupManager(path.default.join(this.dataDir, ".backup"));
      await bm.backupDirectory(sceneBlocksDir, "scene_blocks", `offset${cp.total_processed}`, this.sceneBackupCount);
    }
    this.logger?.debug?.(`${TAG} extract() backup phase: ${Date.now() - backupStartMs}ms`);

    // Phase 2: Load scene index
    const indexStartMs = Date.now();
    const index = await readSceneIndex(this.dataDir, this.storage);
    this.logger?.debug?.(`${TAG} extract() scene index loaded: ${index.length} entries (${Date.now() - indexStartMs}ms)`);

    // Build scene summaries for the prompt (relative filenames only)
    const { summaries: sceneSummaries, filenames: existingSceneFiles } =
      this.buildSceneSummaries(index);

    // History mode: register the EXACT versions of the material this run
    // supplies (L1 inputs) and capture the prior version of every output we may
    // rewrite — all BEFORE the model runs, so no baseline is taken "after the
    // fact" from a current id. The exact bodies of the existing scenes are ALSO
    // rendered into the prompt below so the model sees the same bytes that were
    // registered.
    const suppliedSceneBodies: Array<{ name: string; body: string }> = [];
    if (this.history) {
      const memoryIds = memories
        .map((memory) => memory.id)
        .filter((id): id is string => typeof id === "string" && id.length > 0);
      for (const memoryId of memoryIds) {
        await this.history.registerL1Input(memoryId, `L1 ${memoryId}`);
        // Supply the EXACT registered version body (never a recall/current copy
        // keyed by the same id), so the prompt and the registration are one read.
        const exact = this.history.bodyForL1Input(memoryId);
        if (exact !== undefined) {
          const memory = memories.find((candidate) => candidate.id === memoryId);
          if (memory && memory.content !== exact) memory.content = exact;
        }
      }
      this.history.setRunInputIds(memoryIds);
      // Capture the prior version of every existing scene file BEFORE the model
      // runs; their exact bodies are handed to the model in this prompt (below).
      for (const name of existingSceneFiles) {
        const key = `${StoragePaths.sceneBlocksDir}${name}`;
        const prior = await this.history.capturePriorVersion(key, "l2");
        if (prior) {
          suppliedSceneBodies.push({ name, body: prior.body });
        }
      }
    }

    // Build scene count warning (tiered system)
    let sceneCountWarning: string | undefined;
    const sceneCount = index.length;
    if (sceneCount >= this.maxScenes) {
      sceneCountWarning = `当前场景数量为 **${sceneCount} 个**，已达到或超过 ${this.maxScenes} 个上限！\n**你必须先执行 MERGE 操作**，将最相似的 2-4 个场景合并为 1 个，然后再处理新记忆。\n参考合并对象：热度最低或主题高度重叠的场景。`;
      this.logger?.warn(`${TAG} extract() scene count at limit: ${sceneCount}/${this.maxScenes}`);
    } else if (sceneCount === this.maxScenes - 1) {
      sceneCountWarning = `当前场景数量为 **${sceneCount} 个**，距离上限只差 1 个！\n本次处理**只能 UPDATE 现有场景，不能 CREATE 新场景**。`;
      this.logger?.warn(`${TAG} extract() scene count near limit (CREATE blocked): ${sceneCount}/${this.maxScenes}`);
    } else if (sceneCount >= this.maxScenes - 3) {
      sceneCountWarning = `当前场景数量为 **${sceneCount} 个**，建议优先考虑 UPDATE 或主动 MERGE 相似场景。`;
      this.logger?.debug?.(`${TAG} extract() scene count approaching limit: ${sceneCount}/${this.maxScenes}`);
    }

    // Snapshot scene index + content before LLM — used later to diff created/updated/deleted
    const preExtractIndex = new Map(index.map((e) => [e.filename, e.summary]));
    // Also snapshot scene content so we can detect content-only changes vs metadata-only changes
    const preExtractContent = new Map<string, string>();
    for (const e of index) {
      try {
        let raw: string | null;
        if (this.storage) {
          raw = await this.storage.readFile(`${StoragePaths.sceneBlocksDir}${e.filename}`);
        } else {
          const fs = await import("node:fs/promises");
          const path = await import("node:path");
          raw = await fs.default.readFile(path.default.join(this.dataDir, "scene_blocks", e.filename), "utf-8");
        }
        if (!raw) continue;
        const block = parseSceneBlock(raw, e.filename);
        preExtractContent.set(e.filename, block.content);
      } catch { /* non-fatal */ }
    }

    // Raw pre-run snapshot of every scene_blocks/*.md file (full raw content,
    // not just the parsed body). Used to roll the directory back if the model
    // produces output that fails strict format validation below.
    const preRunSnapshot = await this.snapshotSceneBlocks(sceneBlocksDir);

    // Phase 3: Build prompt
    const promptStartMs = Date.now();
    const memoriesJson = JSON.stringify(
      memories.map((m) => ({
        content: m.content,
        created_at: m.created_at,
        id: m.id ?? "",
      })),
      null,
      2,
    );

    const currentTimestamp = formatTimestamp(new Date());

    const { systemPrompt: baseSystemPrompt, userPrompt: sceneUserPrompt } = buildSceneExtractionPrompt({
      memoriesJson,
      sceneSummaries: sceneSummaries || "(无已有场景)",
      currentTimestamp,
      sceneCountWarning,
      existingSceneFiles,
      maxScenes: this.maxScenes,
      promptMode: this.promptMode,
    });
    const userPrompt = `${sceneUserPrompt}${suppliedSceneBodies.length > 0
      ? "\n\n【既有场景完整正文（程序在模型运行前登记的确切读取版本）】\n" +
        suppliedSceneBodies.map((entry) => `### ${entry.name}\n\n\`\`\`markdown\n${entry.body}\n\`\`\``).join("\n\n")
      : ""}`;
    const selectedSystemPrompt = resolveSystemTemplate(baseSystemPrompt, this.systemTemplate, "l2");
    const systemPrompt = composeMemorySystemPrompt(selectedSystemPrompt, this.memoryPrompt);
    this.lastEffectiveSystemPrompt = {
      systemPrompt,
      baseSystemPrompt: selectedSystemPrompt,
      baseSource: this.systemTemplate === undefined ? "builtin" : "operator_template",
    };
    this.logger?.debug?.(`${TAG} extract() prompt built: ${userPrompt.length} chars (${Date.now() - promptStartMs}ms)`);

    // Phase 4: Run LLM agent (sandboxed to scene_blocks/)
    let llmOutput = "";
    let llmDurationMs = 0;
    try {
      this.logger?.debug?.(`${TAG} extract() starting LLM runner (timeout=${this.timeoutMs}ms, maxTokens=model default)...`);
      const runnerStartMs = Date.now();
      // langfuse trace 语义：L2 场景抽取有独立 name / 顶级 user/session 列 / 可筛选 tags。
      const traceParams = buildTraceParams("memory.scene-extract", this.traceContext);
      llmOutput = await this.runner.run({
        systemPrompt,
        prompt: userPrompt,
        taskId: `scene-extract-${Date.now()}`,
        timeoutMs: this.timeoutMs,
        // maxTokens omitted → core uses the resolved model's maxTokens from catalog
        workspaceDir: this.storage ? workspace.stagingDir : sceneBlocksDir,
        // Service mode: LLM tools read/write via StorageAdapter (COS) instead of local FS
        storage: this.storage,
        storagePrefix: this.storage ? StoragePaths.sceneBlocksDir : undefined,
        // L2: restrict the LLM's file tools to direct *.md scene blocks under
        // this run's tool root — never checkpoints/index/JSONL/nested/hidden.
        filePolicy: SCENE_BLOCK_FILE_POLICY,
        ...traceParams,
      }) ?? "";
      llmDurationMs = Date.now() - runnerStartMs;
      this.logger?.debug?.(`${TAG} extract() LLM runner completed: ${llmDurationMs}ms`);
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      const totalMs = Date.now() - extractStartMs;
      this.logger?.error(`${TAG} extract() LLM runner failed after ${totalMs}ms: ${errMsg}`);

      // Restore scene_blocks/ from the Phase 1 backup so partial LLM writes
      // (or a wiped sandbox) don't leak into the next recall cycle.
      // Fail-soft: a restore failure must never mask the original LLM error.
      // In service mode (`this.storage` set) we never took a local backup,
      // so `bm` is undefined and we skip restore — the storage backend
      // owns its own snapshot/restore semantics.
      if (bm) {
        try {
          const result = await bm.restoreLatestDirectory("scene_blocks", sceneBlocksDir);
          if (result.restored) {
            this.logger?.warn(`${TAG} extract() restored scene_blocks/ from backup: ${result.from}`);
          } else {
            this.logger?.debug?.(`${TAG} extract() no scene_blocks backup to restore from (first run or empty)`);
          }
        } catch (restoreErr) {
          const rMsg = restoreErr instanceof Error ? restoreErr.message : String(restoreErr);
          this.logger?.warn(`${TAG} extract() restore failed (non-fatal, original LLM error preserved): ${rMsg}`);
        }
      }

      return { memoriesProcessed: 0, success: false, error: errMsg };
    }

    // Phase 4b: Strict output validation. The model writes files before we can
    // inspect them, so validate every resulting .md scene file (exact [DELETED]
    // markers excepted) BEFORE cleanup/index/navigation/checkpoint. Any
    // malformed output rolls scene_blocks/ back to the pre-run snapshot and
    // fails the extraction without advancing any progress state.
    const formatCheck = await this.validateSceneBlocksAfterRun(sceneBlocksDir);
    if (!formatCheck.valid) {
      const baseError =
        `Invalid scene block format in ${formatCheck.invalidFiles.length} file(s) ` +
        `[${formatCheck.invalidFiles.join(", ")}]: ${formatCheck.errors.join("; ")}`;
      this.logger?.error(`${TAG} extract() format validation failed: ${baseError}`);

      const rollback = await this.restoreSceneBlocksSnapshot(sceneBlocksDir, preRunSnapshot);
      if (rollback.ok) {
        this.logger?.warn(`${TAG} extract() rolled back scene_blocks/ to pre-run snapshot`);
        return {
          memoriesProcessed: 0,
          success: false,
          error: baseError,
          invalidSceneFiles: formatCheck.invalidFiles,
        };
      }

      const rollbackError = rollback.error ?? "unknown rollback failure";
      this.logger?.error(`${TAG} extract() rollback did NOT complete: ${rollbackError}`);
      return {
        memoriesProcessed: 0,
        success: false,
        error: `${baseError} | rollback failed: ${rollbackError}`,
        rollbackError,
        invalidSceneFiles: formatCheck.invalidFiles,
      };
    }

    // Phase 5: Subsequent processing — safe cleanup of soft-deleted files
    //
    // Security: The LLM has no `exec` tool and cannot run shell commands.
    // Instead, it "deletes" files by writing the marker `[DELETED]` to the file
    // (writing empty/whitespace-only content is rejected by core's write tool
    // parameter validation). Here we detect and remove those soft-deleted files
    // before syncing the index, so syncSceneIndex won't re-index stale entries.
    //
    // We also detect "META-only" files — files that contain only a META header
    // (e.g. [ARCHIVE] or [CONSOLIDATED] markers) but no actual scene content.
    // These are artifacts of LLM merges that didn't properly delete old files.
    const cleanupStartMs = Date.now();
    let cleanedCount = 0;
    try {
      let allFiles: string[];
      if (this.storage) {
        allFiles = await this.storage.readdirNames(StoragePaths.sceneBlocksDir, ".md");
      } else {
        const fs = await import("node:fs/promises");
        allFiles = (await fs.default.readdir(sceneBlocksDir)).filter((f: string) => f.endsWith(".md"));
      }
      for (const file of allFiles) {
        let raw: string | null;
        if (this.storage) {
          raw = await this.storage.readFile(`${StoragePaths.sceneBlocksDir}${file}`);
        } else {
          const fs = await import("node:fs/promises");
          const path = await import("node:path");
          raw = await fs.default.readFile(path.default.join(this.dataDir, "scene_blocks", file), "utf-8");
        }
        if (!raw || raw.trim().length === 0 || raw.trim() === "[DELETED]") {
          // Empty file or [DELETED] marker — soft-delete. In history mode this is
          // the model's EXPLICIT retire declaration for the locator.
          if (this.history) this.history.noteRetired(`${StoragePaths.sceneBlocksDir}${file}`);
          if (this.storage) {
            await this.storage.unlink(`${StoragePaths.sceneBlocksDir}${file}`);
          } else {
            const fs = await import("node:fs/promises");
            const path = await import("node:path");
            await fs.default.unlink(path.default.join(this.dataDir, "scene_blocks", file));
          }
          cleanedCount++;
          this.logger?.debug?.(`${TAG} extract() removed soft-deleted file: ${file}`);
        } else {
          // Check if file has only META header but no actual content
          const block = parseSceneBlock(raw, file);
          if (!block.content || block.content.trim().length === 0) {
            if (this.history) this.history.noteRetired(`${StoragePaths.sceneBlocksDir}${file}`);
            if (this.storage) {
              await this.storage.unlink(`${StoragePaths.sceneBlocksDir}${file}`);
            } else {
              const fs = await import("node:fs/promises");
              const path = await import("node:path");
              await fs.default.unlink(path.default.join(this.dataDir, "scene_blocks", file));
            }
            cleanedCount++;
            this.logger?.debug?.(`${TAG} extract() removed META-only file (no content): ${file}`);
          }
        }
      }
    } catch (cleanupErr) {
      // Non-fatal — log and continue to index sync
      this.logger?.warn(`${TAG} extract() soft-delete cleanup error: ${cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr)}`);
    }
    this.logger?.debug?.(`${TAG} extract() soft-delete cleanup: removed ${cleanedCount} empty files (${Date.now() - cleanupStartMs}ms)`);

    // Phase 5b: Normalize filenames (defensive — LLM occasionally produces names
    // with spaces / punctuation despite the prompt forbidding them, e.g.
    // "Daily Rhythm in Shanghai.md". Such names break downstream consumers
    // that parse Markdown navigation refs with `\S+\.md` style regexes
    // (health-checker), shell tools, and URL-encoded path consumers.
    //
    // Renaming here — *before* syncSceneIndex — means scene_index.json and
    // every downstream reader (recall, profile-sync, 主 Agent 读取) only
    // ever sees canonical filenames. Idempotent and safe to run repeatedly.
    const normStartMs = Date.now();
    try {
      const normResult = await normalizeSceneFilenames(sceneBlocksDir, this.logger);
      if (normResult.renamed > 0) {
        this.logger?.info(
          `${TAG} extract() filename normalization: renamed ${normResult.renamed}, skipped ${normResult.skipped} (${Date.now() - normStartMs}ms)`,
        );
      } else {
        this.logger?.debug?.(
          `${TAG} extract() filename normalization: skipped ${normResult.skipped} (${Date.now() - normStartMs}ms)`,
        );
      }
    } catch (normErr) {
      // Non-fatal — log and continue. Index sync below will simply pick up
      // whatever names are present on disk.
      this.logger?.warn(`${TAG} extract() filename normalization error: ${normErr instanceof Error ? normErr.message : String(normErr)}`);
    }

    // Phase 6: Sync scene index (rebuilds from remaining non-empty files)
    const syncStartMs = Date.now();
    await syncSceneIndex(this.dataDir, this.storage);
    this.logger?.debug?.(`${TAG} extract() scene index synced: ${Date.now() - syncStartMs}ms`);

    // Publish staged content before any control-plane write, so a failed
    // publish can never advance checkpoint state.
    try {
      await workspace.commit({ kind: "l2", memoriesProcessed: memories.length, input_ids: memories.flatMap(memory => memory.id ? [memory.id] : []), scope: this.traceContext ? { team_id: this.traceContext.teamId, agent_id: this.traceContext.agentId } : {} });
    } catch (publishErr) {
      const msg = publishErr instanceof Error ? publishErr.message : String(publishErr);
      this.logger?.error(`${TAG} extract() publish failed: ${msg}`);
      return { memoriesProcessed: 0, success: false, error: `publish failed: ${msg}`, rollbackError: msg };
    }

    const totalMs = Date.now() - extractStartMs;
    this.logger?.info(`${TAG} extract() completed: ${memories.length} memories processed in ${totalMs}ms`);

    // ── l2_extraction metric ──
    if (this.instanceId && this.logger) {
      // Read updated scene index to report final state + diff against pre-extract snapshot
      let resultScenes: Array<{ title: string; summary: string; content: string; status: "created" | "updated" }> = [];
      let scenesCreated = 0;
      let scenesUpdated = 0;
      let scenesDeleted = 0;
      try {
        const finalIndex = await readSceneIndex(this.dataDir, this.storage);
        const postFilenames = new Set<string>();
        for (const e of finalIndex) {
          postFilenames.add(e.filename);
          const oldSummary = preExtractIndex.get(e.filename);
          // Read scene block content from disk
          let content = "";
          try {
            let raw: string | null;
            if (this.storage) {
              raw = await this.storage.readFile(`${StoragePaths.sceneBlocksDir}${e.filename}`);
            } else {
              const fs = await import("node:fs/promises");
              const path = await import("node:path");
              raw = await fs.default.readFile(path.default.join(this.dataDir, "scene_blocks", e.filename), "utf-8");
            }
            if (raw) {
              const block = parseSceneBlock(raw, e.filename);
              content = block.content;
            }
          } catch { /* file read failure is non-fatal */ }

          if (oldSummary === undefined) {
            // New scene
            scenesCreated++;
            resultScenes.push({
              title: e.filename.replace(/\.md$/, ""),
              summary: e.summary,
              content,
              status: "created",
            });
          } else {
            // Existing scene — check if content actually changed (not just metadata)
            const oldContent = preExtractContent.get(e.filename) ?? "";
            if (content !== oldContent) {
              scenesUpdated++;
              resultScenes.push({
                title: e.filename.replace(/\.md$/, ""),
                summary: e.summary,
                content,
                status: "updated",
              });
            }
            // If only metadata (summary/heat) changed but content is the same, skip
          }
        }
        // Scenes in pre-extract but missing from post-extract = deleted
        for (const [filename] of preExtractIndex) {
          if (!postFilenames.has(filename)) {
            scenesDeleted++;
          }
        }
      } catch { /* non-fatal */ }

      report("l2_extraction", {
        inputMemoryCount: memories.length,
        resultSceneCount: resultScenes.length,
        resultScenes,
        scenesCreated,
        scenesUpdated,
        scenesDeleted,
        llmDurationMs,
        totalDurationMs: totalMs,
        success: true,
        error: null,
      });

      // ── 评测指标：L2 延迟 + 场景变化 ──
      try {
        const postSceneCount = (await readSceneIndex(this.dataDir, this.storage)).length;
        reportL2LatencyMetrics({
          instanceId: this.instanceId ?? "",
          extractionLatencyMs: totalMs,
          llmDurationMs: llmDurationMs > 0 ? llmDurationMs : null,
          sceneCountBefore: preExtractIndex.size,
          sceneCountAfter: postSceneCount,
          scenesCreated,
          scenesUpdated,
          scenesDeleted,
          hasError: false,
        });
      } catch {
        // 静默忽略
      }
    }

    // Detect empty extraction: pre and post scene_index both empty means LLM didn't write anything
    const postIndex = await readSceneIndex(this.dataDir, this.storage);
    const emptyExtraction = preExtractIndex.size === 0 && postIndex.length === 0;
    if (emptyExtraction) {
      this.logger?.warn(`${TAG} extract() empty extraction detected: LLM produced no file changes`);
    }

    return { memoriesProcessed: memories.length, success: true, emptyExtraction };
    } finally {
      this.storage = baseStorage;
      this.dataDir = baseDataDir;
      if (!workspace.isCommitted) {
        try {
          await workspace.discard();
        } catch {
          // discard is best-effort — a leaked staging dir must never affect reads
        }
      }
    }
  }

  /**
   * Build human-readable scene summaries for the prompt,
   * and collect the list of existing scene filenames (relative).
   *
   * Includes a capacity counter at the top (e.g. "当前场景总数：5 / 15")
   * so the LLM can immediately see how close it is to the limit.
   */
  private buildSceneSummaries(
    index: SceneIndexEntry[],
  ): { summaries: string; filenames: string[] } {
    if (index.length === 0) return { summaries: "", filenames: [] };

    const lines: string[] = [];
    const filenames: string[] = [];

    // Inject capacity counter at the top — LLM sees this first
    lines.push(`**当前场景总数：${index.length} / ${this.maxScenes}**`);
    lines.push("");

    for (const entry of index) {
      filenames.push(entry.filename);
      lines.push(`### ${entry.filename}`);
      lines.push(`**热度**: ${entry.heat} | **更新**: ${entry.updated}`);
      lines.push(`**summary**: ${entry.summary}`);
      lines.push("");
    }
    return { summaries: lines.join("\n"), filenames };
  }

  /** List the .md scene block filenames currently present (sorted). */
  private async listSceneBlockNames(sceneBlocksDir: string): Promise<string[]> {
    if (this.storage) {
      const names = await this.storage.readdirNames(StoragePaths.sceneBlocksDir, ".md");
      return names.filter((name) => name.endsWith(".md")).sort();
    }
    const fs = await import("node:fs/promises");
    try {
      const names = (await fs.default.readdir(sceneBlocksDir)).filter((f) => f.endsWith(".md"));
      return names.sort();
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
      throw err;
    }
  }

  /** Read one scene block's raw content (null when absent). */
  private async readSceneBlockRaw(sceneBlocksDir: string, name: string): Promise<string | null> {
    if (this.storage) {
      return await this.storage.readFile(`${StoragePaths.sceneBlocksDir}${name}`);
    }
    const fs = await import("node:fs/promises");
    const path = await import("node:path");
    return await fs.default.readFile(path.default.join(sceneBlocksDir, name), "utf-8");
  }

  /** Snapshot raw content of every .md scene block before the model runs. */
  private async snapshotSceneBlocks(sceneBlocksDir: string): Promise<Map<string, string>> {
    const snapshot = new Map<string, string>();
    for (const name of await this.listSceneBlockNames(sceneBlocksDir)) {
      const raw = await this.readSceneBlockRaw(sceneBlocksDir, name);
      if (raw !== null) snapshot.set(name, raw);
    }
    return snapshot;
  }

  /**
   * Strictly validate every .md scene file produced by the model run.
   * Exact `[DELETED]` markers are skipped (orchestration-level deletion),
   * everything else must pass `validateSceneBlock`.
   */
  private async validateSceneBlocksAfterRun(
    sceneBlocksDir: string,
  ): Promise<{ valid: boolean; invalidFiles: string[]; errors: string[] }> {
    const invalidFiles: string[] = [];
    const errors: string[] = [];
    for (const name of await this.listSceneBlockNames(sceneBlocksDir)) {
      let raw: string | null;
      try {
        raw = await this.readSceneBlockRaw(sceneBlocksDir, name);
      } catch (err) {
        invalidFiles.push(name);
        errors.push(`${name}: unreadable (${err instanceof Error ? err.message : String(err)})`);
        continue;
      }
      if (raw === null) continue;
      if (isSoftDeletedScene(raw)) continue;
      const result = validateSceneBlock(raw, name);
      if (!result.valid) {
        invalidFiles.push(name);
        errors.push(`${name}: ${result.error ?? "invalid scene format"}`);
      }
    }
    return { valid: invalidFiles.length === 0, invalidFiles, errors };
  }

  /**
   * Restore scene_blocks/ to the pre-run snapshot:
   *   - delete .md files the model newly added,
   *   - rewrite every pre-existing .md file with its original raw content.
   * Only .md scene files are touched. Returns `ok:false` with a distinct error
   * when any file could not be restored, so callers never claim a successful
   * rollback.
   */
  private async restoreSceneBlocksSnapshot(
    sceneBlocksDir: string,
    snapshot: Map<string, string>,
  ): Promise<{ ok: boolean; error?: string }> {
    const errors: string[] = [];

    let present: string[];
    try {
      present = await this.listSceneBlockNames(sceneBlocksDir);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { ok: false, error: `could not list scene_blocks for rollback: ${msg}` };
    }

    for (const name of present) {
      if (snapshot.has(name)) continue;
      try {
        if (this.storage) {
          await this.storage.unlink(`${StoragePaths.sceneBlocksDir}${name}`);
        } else {
          const fs = await import("node:fs/promises");
          const path = await import("node:path");
          await fs.default.unlink(path.default.join(sceneBlocksDir, name));
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`remove added ${name}: ${msg}`);
      }
    }

    for (const [name, raw] of snapshot) {
      try {
        if (this.storage) {
          await this.storage.writeFile(`${StoragePaths.sceneBlocksDir}${name}`, raw);
        } else {
          const fs = await import("node:fs/promises");
          const path = await import("node:path");
          await fs.default.mkdir(sceneBlocksDir, { recursive: true });
          await fs.default.writeFile(path.default.join(sceneBlocksDir, name), raw, "utf-8");
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`restore ${name}: ${msg}`);
      }
    }

    return errors.length === 0 ? { ok: true } : { ok: false, error: errors.join("; ") };
  }
}

function formatTimestamp(d: Date): string {
  return d.toISOString();
}
