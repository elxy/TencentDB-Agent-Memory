#!/usr/bin/env node
/**
 * trim-generation.test.mjs — 2026-10-12 收窄后自动整理管线的行为验收。
 *
 * 覆盖：
 *   - L0→L1 提炼（注入的固定模型输出）：版本带 source=extraction；重复摄入
 *     同一授权输入幂等重放，不产生新认识；模型失败不推进 L1 游标。
 *   - 去重只新增 / 跳过完全重复：skip 不写入；非重复一律 store。
 *   - L2 整理（注入的固定模型输出）：场景文件与整理输入清单同 bundle 发布；
 *     未改动的场景不会因运行而换来源；显式重声明输入可产生 source-only 版本；
 *     未登记的输入 id 被拒绝。只整理、不裁决的规则在提示词中。
 *   - L3 自动生成已撤掉：createL3Runner 不再导出；kind='l3' 的整理运行被拒。
 *
 * 所有模型输出都是注入的固定虚构内容；存储与发布链路全部是真实实现。
 */

import { test, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SRC = "../MemoryCore/src";

const { VectorStore } = await import(`${SRC}/core/store/sqlite/memory-store.ts`);
const { LocalStorageBackend } = await import(`${SRC}/core/storage/local-backend.ts`);
const { StorageAdapter, createScopedStorageAdapter } = await import(`${SRC}/core/storage/adapter.ts`);
const { handleV2Route } = await import(`${SRC}/gateway/v2-router.ts`);
const { createL1Runner, createL2Runner } = await import(`${SRC}/utils/pipeline-factory.ts`);
const pipelineFactory = await import(`${SRC}/utils/pipeline-factory.ts`);
const { L1HistoryRun, ProfileHistoryRun } = await import(`${SRC}/core/content/generation-history.ts`);
const { extractL1Memories } = await import(`${SRC}/core/record/l1-extractor.ts`);
const { batchDedup } = await import(`${SRC}/core/record/l1-dedup.ts`);
const { GenerationWorkspace } = await import(`${SRC}/core/storage/generation-workspace.ts`);
const { CheckpointManager } = await import(`${SRC}/utils/checkpoint.ts`);
const { buildSceneExtractionPrompt } = await import(`${SRC}/core/prompts/scene-extraction.ts`);

const AUTH_HEADERS = { authorization: "Bearer test-key", "x-tdai-service-id": "svc-trim-gen" };
const silentLogger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

const iso = { teamId: "T", userId: "U", agentId: "A", sessionId: "s" };
const scope = { team_id: "T", user_id: "U", agent_id: "A" };
const profileScope = { team_id: "T", agent_id: "A" };

const sceneBody = (body) =>
  `-----META-START-----\ncreated: 2026-01-01\nupdated: 2026-01-02\nsummary: Fixture\nheat: 0\n-----META-END-----\n\n${body}`;

// ── 每个测试一个独立临时环境 ──
const testDataRoot = path.join(HERE, "test-data");
const cleanups = [];
after(async () => {
  for (const fn of cleanups) await fn();
});

async function setup() {
  await mkdir(testDataRoot, { recursive: true });
  const realRoot = await realpath(testDataRoot);
  const dir = await realpath(await mkdtemp(path.join(realRoot, "trim-gen-")));
  const store = new VectorStore(path.join(dir, "db"), 0, silentLogger, { contentHistory: true });
  store.init();
  const storage = new StorageAdapter(new LocalStorageBackend(dir));
  const profile = createScopedStorageAdapter(storage, `profiles/${encodeURIComponent("team:T|agent:A")}/`);
  cleanups.push(async () => {
    try { store.close(); } catch { /* ignore */ }
    await rm(dir, { recursive: true, force: true }).catch(() => {});
  });

  const deps = { getStore: () => store, getStorage: () => storage, getEmbedding: () => undefined, logger: silentLogger, deployMode: "standalone" };
  const call = async (endpoint, body) => {
    let out;
    await handleV2Route(
      { headers: { ...AUTH_HEADERS } }, {},
      `/v3/${endpoint}`, "POST",
      async () => ({ team_id: "T", user_id: "U", agent_id: "A", ...body }),
      (_r, _s, v) => { out = v; },
      deps,
    );
    return out;
  };

  // 两条真实 L0（含确切版本），供 L1 提炼注册
  const time = "2026-10-12T09:00:00.000Z";
  store.upsertL0({ id: "m1", sessionKey: "s", ...iso, taskId: "", role: "user",
    messageText: "用户说：不是不喜欢 JavaScript，只是这个项目倾向 Python", recordedAt: time, timestamp: Date.parse(time) }, undefined);
  store.upsertL0({ id: "m2", sessionKey: "s", ...iso, taskId: "", role: "user",
    messageText: "用户说：本周优先解决线上问题，纹理研究暂缓", recordedAt: time, timestamp: Date.parse(time) + 1000 }, undefined);

  const history = store.getContentHistory();
  return { dir, store, storage, profile, call, history, time };
}

const extractionPrompt = (memories) =>
  JSON.stringify([{ scene_name: "项目约定", message_ids: ["m1"], memories }]);

// ============================
// L1：提炼提交与幂等
// ============================

test("L1 extraction commits versions marked source=extraction; a repeat input replays", async () => {
  const e = await setup();
  const run = () => new L1HistoryRun({ store: e.store, reader: e.history, ...iso, taskId: "", sessionKey: "s", logger: silentLogger });
  const llmRunner = { run: async () => extractionPrompt([{
    content: "本项目（agent-ernest）倾向 Python，不等于用户拒绝 JavaScript",
    type: "instruction", priority: 90, source_message_ids: ["m1"], metadata: {},
  }]) };

  const params = {
    messages: [{ id: "m1", role: "user", content: "ignored: the exact stored body is supplied", timestamp: Date.parse(e.time) }],
    sessionKey: "s", ...iso, baseDir: e.dir, config: {},
    options: { enableDedup: false, history: run(), vectorStore: e.store, llmRunner },
    logger: silentLogger,
  };
  const result = await extractL1Memories(params);
  assert.equal(result.success, true);
  assert.equal(result.records.length, 1);

  const out = await e.call("atomic/get", { id: result.records[0].id });
  assert.equal(out.code, 0, JSON.stringify(out));
  assert.equal(out.data.content_version.metadata.source, "extraction");
  // 提示词供给的是确切存储正文（不是按同 id 缓存的旧副本）
  assert.equal(out.data.content_version.body, "本项目（agent-ernest）倾向 Python，不等于用户拒绝 JavaScript");

  // 同一授权输入再跑一次：确定性 id + operation id 使第二次成为重放，
  // 不会产生第二个身份（同一来源重复摄入不提高可信度）。
  const before = e.history.listChanges({ scope, layer: "l1", limit: 100 }).items.length;
  const again = await extractL1Memories({ ...params, options: { ...params.options, history: run() } });
  assert.equal(again.success, true);
  const afterCount = e.history.listChanges({ scope, layer: "l1", limit: 100 }).items.length;
  assert.equal(afterCount, before, "a repeat of the same authorized input must not mint new evidence");
});

test("dedup only stores or skips: a fully-duplicate candidate is skipped", async () => {
  const e = await setup();
  store_with_l1(e);
  const memories = [
    { record_id: "new-dup", content: "本项目倾向 Python（与已有认识完全重复）", type: "instruction", priority: 90, source_message_ids: ["m1"], metadata: {}, scene_name: "项目约定" },
    { record_id: "new-fact", content: "用户决定本周优先解决线上问题，纹理研究暂缓", type: "episodic", priority: 90, source_message_ids: ["m2"], metadata: {}, scene_name: "本周重点" },
  ];
  const llmRunner = { run: async (p) => {
    assert.equal(p.taskId, "l1-conflict-detection");
    return JSON.stringify([
      { record_id: "new-dup", action: "skip" },
      { record_id: "new-fact", action: "store" },
    ]);
  } };
  const decisions = await batchDedup({
    memories, config: {}, logger: silentLogger, vectorStore: e.store, llmRunner,
  });
  assert.deepEqual(decisions.map((d) => `${d.record_id}:${d.action}`).sort(), ["new-dup:skip", "new-fact:store"]);

  // 收窄后的决策形状里没有替代旧认识的字段
  for (const d of decisions) {
    assert.equal("target_ids" in d, false);
  }
});

function store_with_l1(e) {
  e.store.upsertL1({
    id: "existing-dup", content: "本项目倾向 Python（与已有认识完全重复）",
    type: "instruction", priority: 90, scene_name: "项目约定",
    source_message_ids: ["m1"], metadata: {}, timestamps: [e.time],
    createdAt: e.time, updatedAt: e.time, version: 1, sessionKey: "s", ...iso, taskId: "",
  }, undefined);
}

test("dedup prompt is conservative: only full duplicates may be skipped", async () => {
  const { getConflictDetectionSystemPrompt } = await import(`${SRC}/core/prompts/l1-dedup.ts`);
  const chat = getConflictDetectionSystemPrompt("chat");
  assert.match(chat, /完全重复/);
  assert.match(chat, /矛盾.{0,20}不是.{0,10}重复|矛盾\*\*不是\*\*重复/s);
  assert.doesNotMatch(chat, /merged_content/);
  const work = getConflictDetectionSystemPrompt("code");
  assert.match(work, /完全重复/);
  assert.doesNotMatch(work, /merged_content/);
});

test("an L1 factory model failure throws and does not advance the checkpoint", async () => {
  const e = await setup();
  const cfg = {
    promptMode: "chat",
    extraction: { enableDedup: false, maxMemoriesPerSession: 10 },
    embedding: {},
    persona: { triggerEveryN: 1, maxScenes: 15, backupCount: 3, sceneBackupCount: 3, promptMode: "chat" },
  };
  const runner = createL1Runner({
    pluginDataDir: e.dir, cfg, openclawConfig: {},
    vectorStore: e.store, embeddingService: undefined, logger: silentLogger,
    llmRunner: { run: async () => { throw new Error("synthetic model failure"); } },
    storage: e.storage,
    contentHistory: { l1: true },
  });
  await assert.rejects(() => runner({ sessionKey: "s" }), /model failure|success=false/);
  const checkpoint = new CheckpointManager(e.dir, silentLogger, e.storage);
  const cp = await checkpoint.read();
  const state = checkpoint.getRunnerState(cp, "s");
  assert.equal(state.last_l1_cursor, 0, "a failed extraction must not advance the L1 cursor");
});

// ============================
// L2：整理发布与整理输入清单
// ============================

async function prepareL2(e, l1Id) {
  const history = e.profile.getContentHistory();
  await history.enableHistory({ scope: profileScope });
  const run = new ProfileHistoryRun({
    kind: "l2", history, reader: e.history, scope: profileScope,
    auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await run.capturePriorVersion("scene_blocks/topic.md", "l2");
  await run.registerL1Input(l1Id);
  run.setRunInputIds([l1Id]);
  const ws = await GenerationWorkspace.begin({
    storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run.createPlan(),
  });
  return { run, ws };
}

function upsertL1(e, id, content) {
  e.store.upsertL1({
    id, content, type: "episodic", priority: 50, scene_name: "x",
    source_message_ids: ["m1"], metadata: {}, timestamps: [e.time],
    createdAt: e.time, updatedAt: e.time, version: 1, sessionKey: "s", ...iso, taskId: "",
  }, undefined);
}

test("L2 plan publishes the scene with its 整理输入清单 sidecar in one bundle", async () => {
  const e = await setup();
  upsertL1(e, "l1-topic", "产品方向仅保留 PQ 和 Linear，不再研究 RLT");
  const first = await prepareL2(e, "l1-topic");
  await first.ws.view.writeFile("scene_blocks/topic.md", sceneBody("方向判断：只做 PQ/Linear"));
  await first.ws.commit();

  const read = await e.call("scenario/read", { path: "topic.md" });
  assert.equal(read.code, 0, JSON.stringify(read));
  assert.equal(read.data.sources.status, "known");
  assert.deepEqual(read.data.sources.input_ids, ["l1-topic"]);

  // 未登记就声明的输入 id 被拒绝，且不会带病发布
  const second = await prepareL2(e, "l1-topic");
  await assert.rejects(async () => {
    second.run.noteOutputInputs("scene_blocks/topic.md", ["never-supplied"]);
    await second.ws.view.writeFile("scene_blocks/topic.md", sceneBody("方向判断：只做 PQ/Linear"));
    await second.ws.commit();
  }, /supplied|input|register/i);
  const after = await e.call("scenario/read", { path: "topic.md" });
  assert.equal(after.data.sources.input_ids.length, 1, "the refused commit must not publish");
});

test("an untouched scene does not get re-sourced; an explicit re-declaration versions sources only", async () => {
  const e = await setup();
  upsertL1(e, "l1-topic", "产品方向仅保留 PQ 和 Linear");
  upsertL1(e, "l1-other", "另一个完全不同的依据");

  // 第一轮：topic（输入 l1-topic）+ other（输入 l1-other）
  const run1 = new ProfileHistoryRun({
    kind: "l2", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await e.profile.getContentHistory().enableHistory({ scope: profileScope });
  await run1.registerL1Input("l1-topic");
  await run1.registerL1Input("l1-other");
  run1.setRunInputIds(["l1-topic"]);
  run1.noteOutputInputs("scene_blocks/other.md", ["l1-other"]);
  const ws1 = await GenerationWorkspace.begin({ storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run1.createPlan() });
  await ws1.view.writeFile("scene_blocks/topic.md", sceneBody("TP"));
  await ws1.view.writeFile("scene_blocks/other.md", sceneBody("OB"));
  await ws1.commit();
  const topicV1 = await e.profile.getContentHistory().resolveLocator("l2", "scene_blocks/topic.md", profileScope);
  const otherV1 = await e.profile.getContentHistory().resolveLocator("l2", "scene_blocks/other.md", profileScope);

  // 第二轮：只改 topic，other 未重新评估——other 不得换来源也不得产生新版本
  const run2 = new ProfileHistoryRun({
    kind: "l2", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await run2.capturePriorVersion("scene_blocks/topic.md", "l2");
  await run2.capturePriorVersion("scene_blocks/other.md", "l2");
  await run2.registerL1Input("l1-topic");
  await run2.registerL1Input("l1-other");
  run2.setRunInputIds(["l1-topic"]);
  const ws2 = await GenerationWorkspace.begin({ storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run2.createPlan() });
  await ws2.view.writeFile("scene_blocks/topic.md", sceneBody("TP v2"));
  await ws2.view.writeFile("scene_blocks/other.md", sceneBody("OB"));
  await ws2.commit();
  const otherV2 = await e.profile.getContentHistory().resolveLocator("l2", "scene_blocks/other.md", profileScope);
  assert.equal(otherV2.version_id, otherV1.version_id, "an output this run did NOT re-evaluate must not be re-versioned");

  // 第三轮：正文未变，但显式重声明 other 的输入 → source-only 版本
  const run3 = new ProfileHistoryRun({
    kind: "l2", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await run3.capturePriorVersion("scene_blocks/other.md", "l2");
  await run3.registerL1Input("l1-topic");
  await run3.registerL1Input("l1-other");
  run3.noteOutputInputs("scene_blocks/other.md", ["l1-other", "l1-topic"]);
  const ws3 = await GenerationWorkspace.begin({ storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run3.createPlan() });
  await ws3.view.writeFile("scene_blocks/topic.md", sceneBody("TP v2"));
  await ws3.view.writeFile("scene_blocks/other.md", sceneBody("OB"));
  await ws3.commit();
  const otherV3 = await e.profile.getContentHistory().resolveLocator("l2", "scene_blocks/other.md", profileScope);
  assert.notEqual(otherV3.version_id, otherV2.version_id);
  const version = await e.profile.getContentHistory().readVersion({
    layer: "l2", content_id: otherV3.content_id, version_id: otherV3.version_id, scope: profileScope,
  });
  assert.deepEqual(version.metadata.content_sources.input_ids, ["l1-other", "l1-topic"]);
  assert.equal(version.body, sceneBody("OB"), "a source-only revision keeps the exact body bytes");

  // 显式清空输入清单：sidecar 变为 unknown
  const run4 = new ProfileHistoryRun({
    kind: "l2", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await run4.capturePriorVersion("scene_blocks/other.md", "l2");
  await run4.registerL1Input("l1-topic");
  run4.noteOutputInputs("scene_blocks/other.md", []);
  const ws4 = await GenerationWorkspace.begin({ storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run4.createPlan() });
  await ws4.view.writeFile("scene_blocks/other.md", sceneBody("OB"));
  await ws4.commit();
  const cleared = await e.profile.getContentHistory().readVersion({
    layer: "l2", content_id: otherV3.content_id, scope: profileScope,
  });
  assert.equal(cleared.metadata.content_sources.status, "unknown");
});

test("L2 prompt instructs 只整理、不裁决: contradictions are written up, never decided", async () => {
  const { systemPrompt } = buildSceneExtractionPrompt({
    memoriesJson: "[]", sceneSummaries: "(无)", currentTimestamp: "2026-10-12", maxScenes: 15, promptMode: "chat",
  });
  assert.match(systemPrompt, /只整理、不裁决/);
  assert.match(systemPrompt, /由主 Agent 裁决/);
  assert.doesNotMatch(systemPrompt, /PERSONA_UPDATE_REQUEST/);
  const work = buildSceneExtractionPrompt({
    memoriesJson: "[]", sceneSummaries: "(无)", currentTimestamp: "2026-10-12", maxScenes: 15, promptMode: "code",
  });
  assert.match(work.systemPrompt, /只整理、不裁决/);
  assert.doesNotMatch(work.systemPrompt, /PERSONA_UPDATE_REQUEST/);
});

// ============================
// L3 自动生成已撤掉
// ============================

test("Core no longer generates L3: createL3Runner is gone and kind=l3 is refused", async () => {
  assert.equal("createL3Runner" in pipelineFactory, false, "createL3Runner must not be exported");
  const e = await setup();
  await e.profile.getContentHistory().enableHistory({ scope: profileScope });
  assert.throws(() => new ProfileHistoryRun({
    kind: "l3", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  }), /history_l3_auto_removed/);
});

test("a disappearing scene without an explicit retire is refused, never silently retired", async () => {
  const e = await setup();
  upsertL1(e, "l1-topic", "产品方向仅保留 PQ 和 Linear");
  const first = await prepareL2(e, "l1-topic");
  await first.ws.view.writeFile("scene_blocks/topic.md", sceneBody("方向判断"));
  await first.ws.commit();

  // 第二轮：文件从发布集中消失却没有 [DELETED] 显式退役声明 → 拒绝发布
  const run2 = new ProfileHistoryRun({
    kind: "l2", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await run2.capturePriorVersion("scene_blocks/topic.md", "l2");
  await run2.registerL1Input("l1-topic");
  const ws2 = await GenerationWorkspace.begin({ storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run2.createPlan() });
  await ws2.view.unlink("scene_blocks/topic.md");
  await assert.rejects(() => ws2.commit(), /retire|disappeared|missing/);

  // 显式退役：文件从发布集移除并加以 [DELETED] 显式声明 → 退役版本发布
  const run3 = new ProfileHistoryRun({
    kind: "l2", history: e.profile.getContentHistory(), reader: e.history,
    scope: profileScope, auth: { team_id: "T", user_id: "U", agent_id: "A" }, logger: silentLogger,
  });
  await run3.capturePriorVersion("scene_blocks/topic.md", "l2");
  await run3.registerL1Input("l1-topic");
  run3.noteRetired("scene_blocks/topic.md");
  const ws3 = await GenerationWorkspace.begin({ storage: e.profile, dataDir: e.dir, kind: "l2", historyContext: run3.createPlan() });
  await ws3.view.unlink("scene_blocks/topic.md");
  await ws3.commit();
  const retiredRead = await e.profile.getContentHistory().readVersion({
    layer: "l2",
    content_id: (await e.profile.getContentHistory().resolveLocator("l2", "scene_blocks/topic.md", profileScope)).content_id,
    scope: profileScope,
  });
  assert.equal(retiredRead.state, "retired");
});
