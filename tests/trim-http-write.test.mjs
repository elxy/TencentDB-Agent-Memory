#!/usr/bin/env node
/**
 * trim-http-write.test.mjs — 2026-10-12 收窄后 content/write 的行为验收。
 *
 * 收窄规则（docs/memory-design-proposal.md §5 / implementation-next.md §2）：
 *   - L0 只追加（create；来源可标 framework_event），revise 一律 400；
 *   - L1 只新增（create；l1_source 必填），撤回以带 withdraws 的新条目表达；
 *   - L2 拒绝外部直接写入（只由内部整理流程发布）；
 *   - L3 只接受三份文档（user-understanding.md / beliefs.md / current-focus.md）
 *     的 create/revise 并逐份校验篇幅上限。
 *
 * 与旧 http.test.mjs 相同的真实链路：真实 handleV2Route、真实 SQLite
 * VectorStore（content history enabled）、真实本地 GenerationWorkspace +
 * LocalContentHistory planner。没有任何 writer 被 mock。
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

const AUTH_HEADERS = { authorization: "Bearer test-key", "x-tdai-service-id": "svc-trim-http" };
const silentLogger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

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
  const req = { headers: { ...AUTH_HEADERS }, url: `/v3${subpath}`, method: "POST" };
  const res = {};
  const captured = { status: null, body: null };
  const handled = await handleV2Route(
    req, res, `/v3${subpath}`, "POST",
    async () => body,
    (_r, status, payload) => { captured.status = status; captured.body = payload; },
    deps,
  );
  return { handled, ...captured };
}

function withIso(iso, body) {
  return {
    team_id: iso.teamId,
    agent_id: iso.agentId,
    user_id: iso.userId,
    ...(iso.sessionId ? { session_id: iso.sessionId } : {}),
    ...body,
  };
}

const code = (r) => (r.body && typeof r.body.code === "number" ? r.body.code : null);
const data = (r) => (r.body ? r.body.data : null);

const testDataRoot = path.join(HERE, "test-data");
const iso = { teamId: "team-trim", userId: "user-trim", agentId: "agent-trim", sessionId: "sess-trim-1" };

let ctxPromise;
function ctx() {
  if (!ctxPromise) ctxPromise = createContext();
  return ctxPromise;
}

async function createContext() {
  await mkdir(testDataRoot, { recursive: true });
  const realRoot = await realpath(testDataRoot);
  const tmp = await realpath(await mkdtemp(path.join(realRoot, "trim-http-")));

  const store = new VectorStore(path.join(tmp, "vectors.db"), 0, silentLogger, { contentHistory: true });
  store.init();

  const base = new StorageAdapter(new LocalStorageBackend({ rootDir: path.join(tmp, "store"), logger: silentLogger }));
  const profilePrefix = `profiles/${encodeURIComponent(buildProfileIsolationScope(iso))}/`;
  const scoped = scopeProfileStorageView(base, profilePrefix, {
    teamId: iso.teamId, userId: iso.userId, agentId: iso.agentId,
  });
  await scoped.getContentHistory().enableHistory();

  return { tmp, store, base, profilePrefix, deps: makeDeps({ store, storage: base }) };
}

after(async () => {
  const c = await ctxPromise.catch(() => null);
  if (c) {
    try { c.store.close(); } catch { /* ignore */ }
    await rm(c.tmp, { recursive: true, force: true }).catch(() => {});
  }
});

async function write(c, body) {
  return await call(c.deps, "/content/write", withIso(iso, body));
}
async function read(c, body) {
  return await call(c.deps, "/content/read", withIso(iso, body));
}

let seq = 0;
const op = (name) => `op-trim-${name}-${++seq}`;

// ============================
// L0：只追加 + 框架事件来源
// ============================

test("L0 append records framework_event source; revise is rejected", async () => {
  const c = await ctx();
  const appended = await write(c, {
    layer: "l0", operation_id: op("l0-src"), change: "create",
    session_id: "sess-trim-1", locator: "msg-fw-1", body: "钩子通知：后台任务已结束",
    role: "assistant", source: "framework_event",
    message_timestamp: "2026-10-12T08:00:00.000Z",
  });
  assert.equal(code(appended), 0, JSON.stringify(appended.body));
  const version = data(appended).version;
  assert.equal(version.metadata.source, "framework_event");

  const got = await read(c, {
    layer: "l0", content_id: version.content_id, version_id: version.ref.version_id,
  });
  assert.equal(code(got), 0, JSON.stringify(got.body));
  assert.equal(data(got).version.metadata.source, "framework_event");

  // 缺省来源是 conversation
  const plain = await write(c, {
    layer: "l0", operation_id: op("l0-plain"), change: "create",
    session_id: "sess-trim-1", locator: "msg-plain-1", body: "普通用户消息", role: "user",
  });
  assert.equal(code(plain), 0, JSON.stringify(plain.body));
  assert.equal(data(plain).version.metadata.source, "conversation");

  // L0 不改原话：revise 一律拒绝
  const revise = await write(c, {
    layer: "l0", operation_id: op("l0-revise"), change: "revise",
    content_id: version.content_id, expected_version_id: version.ref.version_id,
    session_id: "sess-trim-1", body: "改写",
  });
  assert.equal(code(revise), 400, JSON.stringify(revise.body));
  assert.match(String(revise.body.message), /append_only_layer/);
});

test("L0 create at an occupied locator conflicts", async () => {
  const c = await ctx();
  const other = await write(c, {
    layer: "l0", operation_id: op("l0-conflict"), change: "create",
    session_id: "sess-trim-1", locator: "msg-fw-1", body: "另一个正文", role: "user",
  });
  assert.equal(code(other), 409, JSON.stringify(other.body));
});

// ============================
// L1：只新增 + 来源标记 + 撤回
// ============================

test("L1 create requires l1_source and records it; source refs resolve", async () => {
  const c = await ctx();
  // 一条真实 L0 作为来源
  const l0 = await write(c, {
    layer: "l0", operation_id: op("l1-src-l0"), change: "create",
    session_id: "sess-trim-1", locator: "msg-l1-src", body: "用户说：不是不喜欢 JavaScript，只是这个项目倾向 Python",
    role: "user", message_timestamp: "2026-10-12T09:00:00.000Z",
  });
  assert.equal(code(l0), 0, JSON.stringify(l0.body));

  const missing = await write(c, {
    layer: "l1", operation_id: op("l1-nosrc"), change: "create",
    locator: "mem-no-source", body: "没有来源标记的认识",
  });
  assert.equal(code(missing), 400, JSON.stringify(missing.body));
  assert.match(String(missing.body.message), /l1_source/);

  const created = await write(c, {
    layer: "l1", operation_id: op("l1-create"), change: "create",
    locator: "mem-python-pref", body: "本项目（agent-ernest）倾向 Python，不等于用户拒绝 JavaScript",
    type: "instruction", priority: 90, scene_name: "项目约定",
    source_message_ids: ["msg-l1-src"],
    l1_source: "agent",
  });
  assert.equal(code(created), 0, JSON.stringify(created.body));
  const version = data(created).version;
  assert.equal(version.metadata.source, "agent");
  assert.equal(version.metadata.source_message_ids[0], "msg-l1-src");
  // 声明的来源被解析为确切版本引用并持久化
  assert.equal(version.metadata.source_refs.length, 1);
  assert.equal(version.metadata.source_refs[0].version_id, data(l0).version.ref.version_id);

  // 未知 L0 来源引用被拒绝
  const badRef = await write(c, {
    layer: "l1", operation_id: op("l1-badref"), change: "create",
    locator: "mem-badref", body: "引用不存在消息的认识",
    source_message_ids: ["msg-does-not-exist"], l1_source: "agent",
  });
  assert.equal(code(badRef), 400, JSON.stringify(badRef.body));

  // L1 只新增：revise/retire 拒绝
  const revise = await write(c, {
    layer: "l1", operation_id: op("l1-revise"), change: "revise",
    content_id: version.content_id, expected_version_id: version.ref.version_id,
    body: "改写", l1_source: "agent",
  });
  assert.equal(code(revise), 400, JSON.stringify(revise.body));
  assert.match(String(revise.body.message), /append_only_layer/);
  const retire = await write(c, {
    layer: "l1", operation_id: op("l1-retire"), change: "retire",
    content_id: version.content_id, expected_version_id: version.ref.version_id,
    l1_source: "agent",
  });
  assert.equal(code(retire), 400, JSON.stringify(retire.body));
});

test("L1 withdrawal is an appended record that resolves its target", async () => {
  const c = await ctx();
  const withdrawal = await write(c, {
    layer: "l1", operation_id: op("l1-withdraw"), change: "create",
    locator: "mem-withdraw-1",
    body: "撤回：此前将“倾向 Python”泛化为用户偏好的认识不再生效（见 mem-python-pref）",
    type: "instruction", priority: 95,
    l1_source: "arbitration",
    withdraws: "mem-python-pref",
  });
  assert.equal(code(withdrawal), 0, JSON.stringify(withdrawal.body));
  const version = data(withdrawal).version;
  assert.equal(version.metadata.source, "arbitration");
  assert.equal(version.metadata.withdraws, "mem-python-pref");
  // 被撤回条目的确切版本引用一并记录，便于回看
  assert.ok(version.metadata.withdraws_ref, "withdraws_ref must be recorded");

  // 旧条目原样保留（撤回是追加，不是修改）
  const original = await read(c, { layer: "l1", content_id: version.metadata.withdraws_ref.content_id });
  assert.equal(code(original), 0, JSON.stringify(original.body));
  assert.equal(data(original).version.body, "本项目（agent-ernest）倾向 Python，不等于用户拒绝 JavaScript");

  // 撤回不存在的 L1 被拒绝
  const badWithdraw = await write(c, {
    layer: "l1", operation_id: op("l1-withdraw-bad"), change: "create",
    locator: "mem-withdraw-bad", body: "撤回不存在条目的尝试",
    l1_source: "agent", withdraws: "mem-not-there",
  });
  assert.equal(code(badWithdraw), 400, JSON.stringify(badWithdraw.body));
});

test("L1 reserved metadata keys are rejected", async () => {
  const c = await ctx();
  const reserved = await write(c, {
    layer: "l1", operation_id: op("l1-reserved"), change: "create",
    locator: "mem-reserved", body: "试图伪造来源的认识",
    l1_source: "agent", user_metadata: { source: "extraction" },
  });
  assert.equal(code(reserved), 400, JSON.stringify(reserved.body));
  assert.match(String(reserved.body.message), /reserved_metadata_key/);
});

// ============================
// L2：拒绝外部直接写入
// ============================

test("L2 external direct writes are rejected", async () => {
  const c = await ctx();
  for (const change of ["create", "revise", "retire", "restore"]) {
    const out = await write(c, {
      layer: "l2", operation_id: op(`l2-${change}`), change,
      content_id: change === "create" ? undefined : "c_x",
      expected_version_id: change === "create" ? undefined : "v_x",
      locator: "topic.md", body: "试图直接写入专题", summary: "x",
    });
    assert.equal(code(out), 400, `${change}: ${JSON.stringify(out.body)}`);
    assert.match(String(out.body.message), /l2_external_write_rejected/);
  }
  // 已撤的 change kind
  const merge = await write(c, {
    layer: "l2", operation_id: op("l2-merge"), change: "merge", locator: "topic.md", body: "x",
  });
  assert.notEqual(code(merge), 0, "merge must not be accepted");
  const split = await write(c, {
    layer: "l2", operation_id: op("l2-split"), change: "split",
    outputs: [{ locator: "a.md", body: "a" }, { locator: "b.md", body: "b" }],
  });
  assert.notEqual(code(split), 0, "split must not be accepted");
});

// ============================
// L3：三份文档 + 篇幅上限 + CAS
// ============================

test("L3 writes only the three documents, with per-document length caps", async () => {
  const c = await ctx();
  const understanding = "用户是腾讯 IEG 的工程师，维护长上下文记忆系统；判断时先看证据再下结论。";
  const a = await write(c, {
    layer: "l3", operation_id: op("l3-u"), change: "create",
    locator: "user-understanding.md", body: understanding,
  });
  assert.equal(code(a), 0, JSON.stringify(a.body));
  assert.equal(data(a).version.locator, "user-understanding.md");

  const readBack = await read(c, { layer: "l3", content_id: data(a).version.content_id });
  assert.equal(code(readBack), 0, JSON.stringify(readBack.body));
  assert.equal(data(readBack).version.body, understanding);

  // persona.md 不再是合法的 L3 locator
  const persona = await write(c, {
    layer: "l3", operation_id: op("l3-persona"), change: "create",
    locator: "persona.md", body: "旧 persona",
  });
  assert.equal(code(persona), 400, JSON.stringify(persona.body));
  assert.match(String(persona.body.message), /invalid_locator/);

  // 篇幅上限：user-understanding/beliefs 4000，current-focus 2000
  const tooLongFocus = await write(c, {
    layer: "l3", operation_id: op("l3-long"), change: "create",
    locator: "current-focus.md", body: "x".repeat(2001),
  });
  assert.equal(code(tooLongFocus), 400, JSON.stringify(tooLongFocus.body));
  assert.match(String(tooLongFocus.body.message), /l3_document_too_long/);

  const longBeliefs = await write(c, {
    layer: "l3", operation_id: op("l3-longb"), change: "create",
    locator: "beliefs.md", body: "b".repeat(4001),
  });
  assert.equal(code(longBeliefs), 400, JSON.stringify(longBeliefs.body));

  const focus = await write(c, {
    layer: "l3", operation_id: op("l3-focus"), change: "create",
    locator: "current-focus.md", body: "本季度：记忆与会话架构 MVP（纹理研究暂缓）",
  });
  assert.equal(code(focus), 0, JSON.stringify(focus.body));
});

test("L3 revise honours CAS, replays idempotently, and conflicts on stale baselines", async () => {
  const c = await ctx();
  const list = await call(c.deps, "/content/changes", withIso(iso, { layer: "l3", limit: 50 }));
  assert.equal(code(list), 0, JSON.stringify(list.body));
  const item = data(list).items.find((i) => i.locator === "user-understanding.md");
  assert.ok(item, "user-understanding.md must exist from the previous test");
  const baseRead = await read(c, { layer: "l3", content_id: item.content_id });
  assert.equal(code(baseRead), 0, JSON.stringify(baseRead.body));
  const base = data(baseRead).version;

  const opRevise = op("l3-revise");
  const revised = await write(c, {
    layer: "l3", operation_id: opRevise, change: "revise",
    content_id: base.content_id, expected_version_id: base.ref.version_id,
    locator: "user-understanding.md",
    body: "用户是腾讯 IEG 的工程师；同时是 agent-ernest 的唯一用户。",
  });
  assert.equal(code(revised), 0, JSON.stringify(revised.body));
  assert.equal(data(revised).changed, true);

  // 同 operation_id + 同 payload 幂等重放
  const sameOp = await write(c, {
    layer: "l3", operation_id: opRevise, change: "revise",
    content_id: base.content_id, expected_version_id: base.ref.version_id,
    locator: "user-understanding.md",
    body: "用户是腾讯 IEG 的工程师；同时是 agent-ernest 的唯一用户。",
  });
  assert.equal(code(sameOp), 0, JSON.stringify(sameOp.body));
  assert.equal(data(sameOp).changed, false);

  // 旧基线已被消耗：stale expected → 409
  const stale = await write(c, {
    layer: "l3", operation_id: op("l3-stale"), change: "revise",
    content_id: base.content_id, expected_version_id: base.ref.version_id,
    locator: "user-understanding.md", body: "过期的改写",
  });
  assert.equal(code(stale), 409, JSON.stringify(stale.body));
});

test("L3 create with a duplicate operation id but different payload conflicts", async () => {
  const c = await ctx();
  const opId = op("l3-dup");
  const first = await write(c, {
    layer: "l3", operation_id: opId, change: "create",
    locator: "beliefs.md", body: "长期目标：让 agent-ernest 真正理解用户的目标与取舍。",
  });
  assert.equal(code(first), 0, JSON.stringify(first.body));
  const conflict = await write(c, {
    layer: "l3", operation_id: opId, change: "create",
    locator: "beliefs.md", body: "完全不同的正文",
  });
  assert.equal(code(conflict), 409, JSON.stringify(conflict.body));
  const replay = await write(c, {
    layer: "l3", operation_id: opId, change: "create",
    locator: "beliefs.md", body: "长期目标：让 agent-ernest 真正理解用户的目标与取舍。",
  });
  assert.equal(code(replay), 0, JSON.stringify(replay.body));
  assert.equal(data(replay).changed, false);
});

// ============================
// content/changes：三层的变更列表仍可用
// ============================

test("content/changes lists trimmed versions across layers", async () => {
  const c = await ctx();
  const changes = await call(c.deps, "/content/changes", withIso(iso, { limit: 500 }));
  assert.equal(code(changes), 0, JSON.stringify(changes.body));
  const items = data(changes).items;
  assert.ok(items.some((i) => i.layer === "l0" && i.locator === "msg-fw-1"));
  assert.ok(items.some((i) => i.layer === "l1" && i.locator === "mem-withdraw-1"));
  assert.ok(items.some((i) => i.layer === "l3" && i.locator === "user-understanding.md"));
});

test("atomic/search hits carry l1_source and withdraws from version metadata", async () => {
  const c = await ctx();
  const oldWrite = await write(c, { layer: "l1", operation_id: op("s-old"), change: "create",
    locator: "mem-old-js", body: "用户不喜欢 JavaScript", type: "instruction",
    l1_source: "agent" });
  assert.equal(code(oldWrite), 0, JSON.stringify(oldWrite.body));
  const correction = await write(c, { layer: "l1", operation_id: op("s-new"), change: "create",
    locator: "mem-new-python", body: "不是不喜欢 JavaScript，只是这个项目倾向 Python",
    type: "instruction", l1_source: "arbitration", withdraws: "mem-old-js" });
  assert.equal(code(correction), 0, JSON.stringify(correction.body));

  const hits = await call(c.deps, "/atomic/search", withIso(iso, { query: "JavaScript", limit: 5 }));
  assert.equal(code(hits), 0, JSON.stringify(hits.body));
  const byId = Object.fromEntries(data(hits).items.map((item) => [item.id, item]));
  assert.equal(byId["mem-old-js"].l1_source, "agent");
  assert.equal(byId["mem-old-js"].withdraws, undefined);
  assert.equal(byId["mem-new-python"].l1_source, "arbitration");
  assert.equal(byId["mem-new-python"].withdraws, "mem-old-js");
});

test("content/read resolves L1/L3 by stable locator", async () => {
  const c = await ctx();
  const l1 = await write(c, { layer: "l1", operation_id: op("loc-l1"), change: "create",
    locator: "mem-loc-test", body: "按编号读取的认识", type: "instruction", l1_source: "agent" });
  assert.equal(code(l1), 0, JSON.stringify(l1.body));
  const l1Read = await read(c, { layer: "l1", locator: "mem-loc-test" });
  assert.equal(code(l1Read), 0, JSON.stringify(l1Read.body));
  assert.equal(data(l1Read).version.body, "按编号读取的认识");
  assert.equal(data(l1Read).version.metadata.source, "agent");

  const missing = await read(c, { layer: "l1", locator: "no-such-memory" });
  assert.equal(code(missing), 404);

  // 三份文档由前面的 L3 测试在共享夹具里创建；按名称读取任一既有文档。
  const l3Read = await read(c, { layer: "l3", locator: "user-understanding.md" });
  assert.equal(code(l3Read), 0, JSON.stringify(l3Read.body));
  assert.equal(data(l3Read).version.locator, "user-understanding.md");
  assert.equal(typeof data(l3Read).version.body, "string");
  assert.ok(data(l3Read).version.body.length > 0);
  const l3Missing = await read(c, { layer: "l3", locator: "no-such-doc.md" });
  assert.equal(code(l3Missing), 404);
});
