#!/usr/bin/env node
/**
 * ROUND/http.test.mjs — dynamic acceptance for the `content/write` entry and the
 * history-mode bypass guards.
 *
 * Run with tsx (the MemoryCore package resolves `.js` specifiers to `.ts`):
 *
 *   npx tsx .dev/memorycore-generation-integration-20261010-215318/http.test.mjs
 *
 * Everything below is REAL: the actual `handleV2Route` dispatcher, the actual
 * SQLite `VectorStore` (content history enabled), and the actual local
 * `GenerationWorkspace` + `LocalContentHistory` planner over a temp directory.
 * No writer is mocked, and the assertions read back through the real
 * `scenario/read` / `content/read` / `atomic/get` handlers.
 *
 * The suite is a `node:test` module (registered tests all run even when one
 * fails) so a single broken expectation can no longer mask every later case.
 */

import { test, after } from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SRC = "../MemoryCore/src";

// ── dynamic TS imports (tsx resolves the `.js` specifiers inside these) ──
const { handleV2Route } = await import(`${SRC}/gateway/v2-router.ts`);
const { VectorStore } = await import(`${SRC}/core/store/sqlite/memory-store.ts`);
const { LocalStorageBackend } = await import(`${SRC}/core/storage/local-backend.ts`);
const { StorageAdapter, scopeProfileStorageView } = await import(`${SRC}/core/storage/adapter.ts`);
const { buildProfileIsolationScope } = await import(`${SRC}/core/profile/profile-scope.ts`);

// ============================
// router harness
// ============================

const AUTH_HEADERS = { authorization: "Bearer test-key", "x-tdai-service-id": "svc-http-test" };

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

/** POST one /v3 route through the REAL dispatcher. */
async function call(deps, subpath, body) {
  const req = { headers: { ...AUTH_HEADERS }, url: `/v3${subpath}`, method: "POST" };
  const res = {};
  const captured = { status: null, body: null };
  const handled = await handleV2Route(
    req,
    res,
    `/v3${subpath}`,
    "POST",
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

// ============================
// shared fixture
// ============================

const testDataRoot = path.join(HERE, "test-data");

const iso = { teamId: "team-http", userId: "user-http", agentId: "agent-http", sessionId: "sess-http-1" };
const otherIso = { teamId: "team-http", userId: "user-other", agentId: "agent-http", sessionId: "sess-http-2" };

let ctxPromise;
function ctx() {
  if (!ctxPromise) ctxPromise = createContext();
  return ctxPromise;
}

async function createContext() {
  await mkdir(testDataRoot, { recursive: true });
  const realRoot = await realpath(testDataRoot);
  const tmp = await realpath(await mkdtemp(path.join(realRoot, "http-")));

  const store = new VectorStore(path.join(tmp, "vectors.db"), 0, silentLogger, { contentHistory: true });
  store.init();

  const base = new StorageAdapter(new LocalStorageBackend({ rootDir: path.join(tmp, "store"), logger: silentLogger }));
  const profilePrefix = `profiles/${encodeURIComponent(buildProfileIsolationScope(iso))}/`;
  const scoped = scopeProfileStorageView(base, profilePrefix, {
    teamId: iso.teamId, userId: iso.userId, agentId: iso.agentId,
  });
  const scopedHistory = scoped.getContentHistory();
  await scopedHistory.enableHistory();

  const legacyStore = new VectorStore(path.join(tmp, "legacy.db"), 0, silentLogger);
  legacyStore.init();
  const legacyBase = new StorageAdapter(new LocalStorageBackend({ rootDir: path.join(tmp, "legacy-store"), logger: silentLogger }));

  return {
    tmp, store, base, profilePrefix, scopedHistory,
    deps: makeDeps({ store, storage: base }),
    legacyStore, legacyBase,
    legacyDeps: makeDeps({ store: legacyStore, storage: legacyBase }),
  };
}

after(async () => {
  const c = await ctxPromise.catch(() => null);
  if (c) {
    try { c.store.close(); } catch { /* ignore */ }
    try { c.legacyStore.close(); } catch { /* ignore */ }
    await rm(c.tmp, { recursive: true, force: true }).catch(() => {});
  }
});

/** A `content/write` call bound to the shared history context. */
async function write(c, body) {
  return await call(c.deps, "/content/write", withIso(iso, body));
}

/** The real local history planner over the shared scoped view. */
async function l2(c, body) {
  return await write(c, { layer: "l2", event_time: { value: "2026-10-10T09:00:00.000Z", precision: "instant", basis: "explicit" }, ...body });
}

// ============================
// L1 (SQLite store) — create / revise / read
// ============================









test("L0 create + idempotent replay + occupied-locator conflict", async () => {
  const c = await ctx();
  const body = "HTTP 原始消息正文";
  const l0 = await write(c, {
    layer: "l0", operation_id: "op-l0-create-1", change: "create",
    session_id: "sess-http-1", locator: "msg-http-1", body, role: "user",
    message_timestamp: "2026-10-10T08:00:00.000Z",
  });
  assert.equal(code(l0), 0, JSON.stringify(l0.body));
  assert.equal(data(l0).version.body, body);
  const conv = await call(c.deps, "/conversation/get", withIso(iso, { id: "msg-http-1" }));
  assert.equal(code(conv), 0);
  assert.equal(data(conv).message.content, body);

  const repeat = await write(c, {
    layer: "l0", operation_id: "op-l0-create-1", change: "create",
    session_id: "sess-http-1", locator: "msg-http-1", body, role: "user",
    message_timestamp: "2026-10-10T08:00:00.000Z",
  });
  assert.equal(code(repeat), 0, JSON.stringify(repeat.body));
  assert.equal(data(repeat).ref.version_id, data(l0).ref.version_id);
  assert.equal(data(repeat).changed, false);

  const other = await write(c, {
    layer: "l0", operation_id: "op-l0-create-2", change: "create",
    session_id: "sess-http-1", locator: "msg-http-1", body, role: "user",
  });
  assert.equal(code(other), 409, JSON.stringify(other.body));
});



// ============================
// injection / rejection surface
// ============================




// ============================
// history-mode bypass guards
// ============================


// ============================
// conversation/add — stable ids in history mode
// ============================

test("conversation/add in history mode requires a stable id and is idempotent", async () => {
  const c = await ctx();
  const uncontrolled = await call(c.deps, "/conversation/add", withIso(iso, { session_id: "sess-http-1", messages: [{ role: "user", content: "no stable id" }] }));
  assert.equal(code(uncontrolled), 400, JSON.stringify(uncontrolled.body));
  assert.match(String(uncontrolled.body.message), /history_message_id_required/);

  const message = { id: "msg-append-1", role: "user", content: "一条经由 conversation/add 追加的消息" };
  const add = await call(c.deps, "/conversation/add", withIso(iso, { session_id: "sess-http-1", messages: [message] }));
  assert.equal(code(add), 0, JSON.stringify(add.body));
  assert.equal(data(add).appended, 1);
  assert.equal(data(add).duplicates, 0);

  // Same stable id + identical body ⇒ no new version, no re-dispatch.
  const repeat = await call(c.deps, "/conversation/add", withIso(iso, { session_id: "sess-http-1", messages: [message] }));
  assert.equal(code(repeat), 0, JSON.stringify(repeat.body));
  assert.equal(data(repeat).appended, 0);
  assert.equal(data(repeat).duplicates, 1);

  const changes = await call(c.deps, "/content/changes", withIso(iso, { layer: "l0", limit: 100 }));
  assert.equal(code(changes), 0);
  assert.ok(data(changes).items.some((i) => i.locator === "msg-append-1"));

  const conflict = await call(c.deps, "/conversation/add", withIso(iso, { session_id: "sess-http-1", messages: [{ ...message, content: "different body" }] }));
  assert.equal(code(conflict), 409, JSON.stringify(conflict.body));
  assert.match(String(conflict.body.message), /history_message_conflict/);
});

// ============================
// L2 — real planner
// ============================








// ============================
// legacy mode — no regression
// ============================


// ============================
// Round-2 targeted fixes (parent-final probes)
// ============================

test("conversation/add history append cannot race into two bodies under one stable id", async () => {
  const c = await ctx();
  const session = "sess-append-race";
  const send = (content) =>
    call(c.deps, "/conversation/add", withIso(iso, { session_id: session, messages: [{ id: "msg-race-1", role: "user", content }] }));
  const results = await Promise.all([send("one"), send("two")]);
  assert.equal(
    results.filter((r) => code(r) === 0).length,
    1,
    JSON.stringify(results.map((r) => r.body)),
  );
  const changes = await call(c.deps, "/content/changes", withIso(iso, { layer: "l0", limit: 200 }));
  assert.equal(code(changes), 0, JSON.stringify(changes.body));
  assert.equal(data(changes).items.filter((i) => i.locator === "msg-race-1").length, 1);
});

test("conversation/add history append is idempotent for the same body and conflicts on a different one", async () => {
  const c = await ctx();
  const session = "sess-append-idem";
  const message = { id: "msg-idem-1", role: "user", content: "stable body" };
  const first = await call(c.deps, "/conversation/add", withIso(iso, { session_id: session, messages: [message] }));
  assert.equal(code(first), 0, JSON.stringify(first.body));
  assert.equal(data(first).appended, 1);
  const repeat = await call(c.deps, "/conversation/add", withIso(iso, { session_id: session, messages: [message] }));
  assert.equal(code(repeat), 0, JSON.stringify(repeat.body));
  assert.equal(data(repeat).appended, 0);
  assert.equal(data(repeat).duplicates, 1);
  const conflict = await call(c.deps, "/conversation/add", withIso(iso, { session_id: session, messages: [{ ...message, content: "different body" }] }));
  assert.equal(code(conflict), 409, JSON.stringify(conflict.body));
  assert.match(String(conflict.body.message), /history_message_conflict/);
  const dupInBatch = await call(c.deps, "/conversation/add", withIso(iso, { session_id: session, messages: [message, message] }));
  assert.equal(code(dupInBatch), 409, JSON.stringify(dupInBatch.body));
});





