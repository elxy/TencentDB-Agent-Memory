#!/usr/bin/env node
/**
 * retag.test.mjs — L1 `change: "retag"` 的验收（2026-11-24 新增）。
 *
 * 契约：
 *   - retag 只改归属元数据（scene_name / type / priority），正文由当前版本提供；
 *   - 正文、locator、content_id、l1_source、withdraws、user_metadata.note 等
 *     一律沿用当前版本，调用方给了就拒（400）；
 *   - CAS：给了 expected_version_id 就必须等于当前版本，否则 409；
 *   - 归属没变时是 no-op（changed=false），不长新版本；
 *   - 版本历史保留：retag 前后的两个版本都能按 content_id + version_id 读回；
 *   - 只对 L1 生效（L0/L3 的 retag 被拒）。
 *
 * 真实链路：真 handleV2Route、真 SQLite VectorStore（content history enabled）、
 * 真 LocalContentHistory。没有 writer 被 mock。
 */

import { test, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SRC = "../MemoryCore/src";

const { handleV2Route } = await import(`${SRC}/gateway/v2-router.ts`);
const { VectorStore } = await import(`${SRC}/core/store/sqlite/memory-store.ts`);
const { LocalStorageBackend } = await import(`${SRC}/core/storage/local-backend.ts`);
const { StorageAdapter, scopeProfileStorageView } = await import(`${SRC}/core/storage/adapter.ts`);
const { buildProfileIsolationScope } = await import(`${SRC}/core/profile/profile-scope.ts`);

const AUTH_HEADERS = { authorization: "Bearer test-key", "x-tdai-service-id": "svc-retag" };
const silentLogger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

const iso = { teamId: "team-retag", userId: "user-retag", agentId: "agent-retag", sessionId: "sess-retag-1" };

function makeDeps({ store, storage }) {
  return {
    getStore: () => store,
    getEmbedding: () => undefined,
    getStorage: () => storage,
    logger: silentLogger,
    deployMode: "standalone",
    isolationConfig: { enforce: true, legacyCompatMode: false, legacyPlaceholder: "default" },
    v3StrictIsolation: true,
  };
}

async function call(deps, subpath, body) {
  const captured = { status: null, body: null };
  const handled = await handleV2Route(
    { headers: { ...AUTH_HEADERS }, url: `/v3${subpath}`, method: "POST" },
    {}, `/v3${subpath}`, "POST",
    async () => body,
    (_r, status, payload) => { captured.status = status; captured.body = payload; },
    deps,
  );
  return { handled, ...captured };
}

const withIso = (extra) => ({
  team_id: iso.teamId, agent_id: iso.agentId, user_id: iso.userId, ...extra,
});
const code = (r) => (r.body && typeof r.body.code === "number" ? r.body.code : null);
const data = (r) => (r.body ? r.body.data : null);
const message = (r) => String(r.body?.message ?? "");

const testDataRoot = path.join(HERE, "test-data");
let ctxPromise;

async function createContext() {
  await mkdir(testDataRoot, { recursive: true });
  const realRoot = await realpath(testDataRoot);
  const tmp = await realpath(await mkdtemp(path.join(realRoot, "retag-")));
  const store = new VectorStore(path.join(tmp, "vectors.db"), 0, silentLogger, { contentHistory: true });
  store.init();
  const base = new StorageAdapter(new LocalStorageBackend({ rootDir: path.join(tmp, "store"), logger: silentLogger }));
  const profilePrefix = `profiles/${encodeURIComponent(buildProfileIsolationScope(iso))}/`;
  const scoped = scopeProfileStorageView(base, profilePrefix, {
    teamId: iso.teamId, userId: iso.userId, agentId: iso.agentId,
  });
  await scoped.getContentHistory().enableHistory();
  return { tmp, store, base, deps: makeDeps({ store, storage: base }) };
}

function ctx() {
  ctxPromise ??= createContext();
  return ctxPromise;
}

after(async () => {
  const c = await ctxPromise?.catch(() => null);
  if (c) {
    try { c.store.close(); } catch { /* ignore */ }
    await rm(c.tmp, { recursive: true, force: true }).catch(() => {});
  }
});

async function write(c, body) { return await call(c.deps, "/content/write", withIso(body)); }
async function read(c, body) { return await call(c.deps, "/content/read", withIso(body)); }
async function atomicGet(c, id) { return await call(c.deps, "/atomic/get", withIso({ id })); }

let seq = 0;
const op = (name) => `op-retag-${name}-${++seq}`;

async function createAtom(c, locator, { scene = "项目约定", note = "from:conv-1:entry:1:entry:9", withdraws } = {}) {
  const created = await write(c, {
    layer: "l1", operation_id: op(`create-${locator}`), change: "create",
    locator, body: "用户要求弹幕下载不要并发，避免被限流", type: "instruction", priority: 70,
    scene_name: scene, l1_source: "agent", user_metadata: { note },
    ...(withdraws === undefined ? {} : { withdraws }),
  });
  assert.equal(code(created), 0, JSON.stringify(created.body));
  return data(created).version;
}

test("retag 只改归属：正文、locator 与其余元数据原样保留，版本历史可回读", async () => {
  const c = await ctx();
  const before = await createAtom(c, "mem-retag-1");

  const retagged = await write(c, {
    layer: "l1", operation_id: op("retag-1"), change: "retag",
    locator: "mem-retag-1", scene_name: "暗场景降百万投比专项",
  });
  assert.equal(code(retagged), 0, JSON.stringify(retagged.body));
  assert.equal(data(retagged).changed, true, "改归属必须产生新版本");
  const after_ = data(retagged).version;
  assert.equal(after_.body, before.body, "retag 不能改正文");
  assert.equal(after_.metadata.scene_name, "暗场景降百万投比专项");
  assert.equal(after_.metadata.type, "instruction", "没点名的归属字段沿用");
  assert.equal(after_.metadata.priority, 70);
  assert.equal(after_.metadata.source, "agent", "l1_source 沿用，不能借 retag 改来源");
  assert.equal(after_.metadata.user_metadata.note, "from:conv-1:entry:1:entry:9", "原文指针必须保留");
  assert.notEqual(after_.ref.version_id, before.ref.version_id);
  assert.equal(after_.ref.content_id, before.ref.content_id, "content_id 不变（同一条认识）");

  // 旧版本仍可按 content_id + version_id 读回，且带旧归属
  const old = await read(c, { layer: "l1", content_id: before.ref.content_id,
    version_id: before.ref.version_id });
  assert.equal(code(old), 0, JSON.stringify(old.body));
  assert.equal(data(old).version.metadata.scene_name, "项目约定");

  // 当前行（召回路径读的那一行）也已经是新归属
  const current = await atomicGet(c, "mem-retag-1");
  assert.equal(code(current), 0, JSON.stringify(current.body));
  assert.equal(data(current).item.background, "暗场景降百万投比专项");
  assert.equal(data(current).item.content, before.body);
});

test("retag 也可改 type/priority，并保留 withdraws 关系", async () => {
  const c = await ctx();
  await createAtom(c, "mem-retag-target");
  const withdrawal = await createAtom(c, "mem-retag-withdraw", { withdraws: "mem-retag-target" });
  assert.equal(withdrawal.metadata.withdraws, "mem-retag-target");

  const retagged = await write(c, {
    layer: "l1", operation_id: op("retag-withdraw"), change: "retag",
    locator: "mem-retag-withdraw", scene_name: "工具/框架约定", type: "episodic", priority: 10,
  });
  assert.equal(code(retagged), 0, JSON.stringify(retagged.body));
  const version = data(retagged).version;
  assert.equal(version.metadata.scene_name, "工具/框架约定");
  assert.equal(version.metadata.type, "episodic");
  assert.equal(version.metadata.priority, 10);
  assert.equal(version.metadata.withdraws, "mem-retag-target", "撤回关系不能被 retag 抹掉");
  assert.ok(version.metadata.withdraws_ref, "withdraws_ref 一并保留");
  assert.equal(version.metadata.user_metadata.note, "from:conv-1:entry:1:entry:9");
});

test("归属没变时 retag 是 no-op：不长版本", async () => {
  const c = await ctx();
  await createAtom(c, "mem-retag-noop", { scene: "同一条线" });
  const again = await write(c, {
    layer: "l1", operation_id: op("retag-noop"), change: "retag",
    locator: "mem-retag-noop", scene_name: "同一条线",
  });
  assert.equal(code(again), 0, JSON.stringify(again.body));
  assert.equal(data(again).changed, false, "没有变化的 retag 不应写新版本");
});

test("retag 不接受正文：想改认识内容必须写新条目", async () => {
  const c = await ctx();
  await createAtom(c, "mem-retag-body");
  const bad = await write(c, {
    layer: "l1", operation_id: op("retag-body"), change: "retag",
    locator: "mem-retag-body", body: "顺手把正文也改了", scene_name: "另一条线",
  });
  assert.equal(code(bad), 400, JSON.stringify(bad.body));
  assert.match(message(bad), /body must be absent/);
  // 拒绝后原条目没有被改动
  const current = await atomicGet(c, "mem-retag-body");
  assert.equal(data(current).item.background, "项目约定");
});

test("retag 只改归属：来源/撤回/用户元数据等字段一律拒", async () => {
  const c = await ctx();
  await createAtom(c, "mem-retag-guard");
  for (const extra of [
    { l1_source: "arbitration" },
    { withdraws: "mem-retag-guard" },
    { user_metadata: { note: "from:fake" } },
    { source_message_ids: ["msg-x"] },
    { timestamps: ["2026-11-24T00:00:00.000Z"] },
    { content_id: "content-fake" },
  ]) {
    const bad = await write(c, {
      layer: "l1", operation_id: op("retag-guard"), change: "retag",
      locator: "mem-retag-guard", scene_name: "另一条线", ...extra,
    });
    assert.equal(code(bad), 400, `${JSON.stringify(extra)} -> ${JSON.stringify(bad.body)}`);
  }
  // 没有任何归属字段也是拒绝，而不是静默无事发生
  const empty = await write(c, {
    layer: "l1", operation_id: op("retag-empty"), change: "retag", locator: "mem-retag-guard",
  });
  assert.equal(code(empty), 400, JSON.stringify(empty.body));
  assert.match(message(empty), /scene_name/);
});

test("retag 的 CAS：未知 locator 404，过期版本 409", async () => {
  const c = await ctx();
  const before = await createAtom(c, "mem-retag-cas");

  const missing = await write(c, {
    layer: "l1", operation_id: op("retag-missing"), change: "retag",
    locator: "mem-not-there", scene_name: "某条线",
  });
  assert.equal(code(missing), 404, JSON.stringify(missing.body));

  const first = await write(c, {
    layer: "l1", operation_id: op("retag-cas-1"), change: "retag",
    locator: "mem-retag-cas", scene_name: "第一条线", expected_version_id: before.ref.version_id,
  });
  assert.equal(code(first), 0, JSON.stringify(first.body));

  const stale = await write(c, {
    layer: "l1", operation_id: op("retag-cas-2"), change: "retag",
    locator: "mem-retag-cas", scene_name: "第二条线", expected_version_id: before.ref.version_id,
  });
  assert.equal(code(stale), 409, JSON.stringify(stale.body));
  assert.match(message(stale), /history_version_conflict/);

  const current = await atomicGet(c, "mem-retag-cas");
  assert.equal(data(current).item.background, "第一条线", "冲突的写入不能生效");
});

test("retag 只对 L1 生效", async () => {
  const c = await ctx();
  const l3 = await write(c, {
    layer: "l3", operation_id: op("retag-l3"), change: "retag",
    locator: "beliefs.md", scene_name: "x", body: "信念正文",
  });
  assert.equal(code(l3), 400, JSON.stringify(l3.body));
  assert.match(message(l3), /l3_change_unsupported/);

  const l0 = await write(c, {
    layer: "l0", operation_id: op("retag-l0"), change: "retag",
    session_id: "sess-retag-1", locator: "msg-retag-l0", scene_name: "x",
  });
  assert.equal(code(l0), 400, JSON.stringify(l0.body));
  assert.match(message(l0), /append_only_layer/);
});

test("retag 不能越出可信 scope：别人的 locator 当作不存在", async () => {
  const c = await ctx();
  await createAtom(c, "mem-retag-scope");
  const other = {
    team_id: "team-other", agent_id: "agent-other", user_id: "user-other",
  };
  const crossed = await call(c.deps, "/content/write", {
    ...other, layer: "l1", operation_id: op("retag-cross"), change: "retag",
    locator: "mem-retag-scope", scene_name: "插队",
  });
  assert.equal(code(crossed), 404, JSON.stringify(crossed.body));
  const current = await atomicGet(c, "mem-retag-scope");
  assert.equal(data(current).item.background, "项目约定");
});

test("retag 幂等：同一 operation_id + 同一请求重放不写第二版", async () => {
  const c = await ctx();
  await createAtom(c, "mem-retag-replay");
  const body = {
    layer: "l1", operation_id: "op-retag-replay-fixed", change: "retag",
    locator: "mem-retag-replay", scene_name: "重放线",
  };
  const first = await write(c, body);
  assert.equal(code(first), 0, JSON.stringify(first.body));
  assert.equal(data(first).changed, true);
  const replay = await write(c, body);
  assert.equal(code(replay), 0, JSON.stringify(replay.body));
  assert.equal(data(replay).changed, false, "重放不是新版本");
  assert.equal(data(replay).version.ref.version_id, data(first).version.ref.version_id);
});
