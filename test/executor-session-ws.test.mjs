// test/executor-session-ws.test.mjs — the worker registry and a worker over a real local
// WebSocket: token handshake, exec through the supervisor seam, flow control, signals,
// release, idle reclaim, loss, and boot-time reaping. The worker runs in this process
// (backends/in-process.mjs); its children are real.
import { test, after, afterEach, before } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, chmodSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WebSocket } from 'ws';
import { useTempHome } from './helpers/temp-home.mjs';
import { createWorkerRegistry } from '../src/core/executor/registry.mjs';
import { createInProcessBackend } from '../src/core/executor/backends/in-process.mjs';
import { installExecutor } from '../src/core/executor/index.mjs';
import { withExecutorScope } from '../src/core/executor/scope.mjs';
import { superviseSpawn } from '../src/core/engines/spawn.mjs';
import { encodeFrame, PROTOCOL_VERSION } from '../src/core/executor/protocol.mjs';
import { startWorker } from '../src/worker/client.mjs';
import { worcaHome } from '../src/core/projects.mjs';

const home = useTempHome(after);
const POSIX = process.platform === 'win32' ? { skip: 'POSIX shell fixtures' } : {};
const dirs = [];
const tmp = () => { const d = mkdtempSync(join(tmpdir(), 'worca-ws-')); dirs.push(d); return d; };
after(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });
function script(body) { const f = join(tmp(), 'bin.sh'); writeFileSync(f, `#!/bin/sh\n${body}\n`); chmodSync(f, 0o755); return f; }
const base = (over = {}) => ({
  displayBin: 'fake', cwd: tmpdir(), env: { PATH: process.env.PATH }, stdin: null, signal: undefined, asAgent: false,
  stagedDir: null, cleanup: () => {}, onEvent: () => {}, onStdoutLine: () => {}, stdoutErrorDetail: () => '', classify: () => null,
  spawnError: (err, prefix) => Object.assign(new Error(`${prefix}: ${err.message}`), err.code ? { code: err.code } : {}),
  onDone: (code) => ({ code }), ...over,
});
const RUN = { kind: 'run', id: 'r-ws' };
const CHAT = { kind: 'chat', id: 't-ws' };
const CAPS = { engines: { claude: null }, platform: process.platform, node: process.version, user: null };

let registry; let backend; const logs = [];
before(async () => {
  backend = createInProcessBackend({ homeRoot: worcaHome(), capabilities: CAPS, log: (m) => logs.push(m) });
  registry = createWorkerRegistry({ backend, host: '127.0.0.1', port: 0, startTimeoutMs: 5000, chatIdleMs: 300, pingMs: 200, log: (m) => logs.push(m) });
  await registry.listen();
  installExecutor({ kind: 'docker', registry });
});
after(async () => { installExecutor(null); await registry.closeAll('test over'); });
afterEach(async () => { await registry.release(RUN, { reason: 'test' }); await registry.release(CHAT, { reason: 'test' }); });

const inRun = (fn) => withExecutorScope(RUN, fn);

test('acquire starts a worker that connects with its token; the same scope reuses it', async () => {
  const s1 = await registry.acquire(RUN);
  assert.equal(s1.scope.kind, 'run');
  assert.deepEqual(s1.capabilities, CAPS);
  assert.ok(s1.pid > 0);
  const s2 = await registry.acquire(RUN);
  assert.equal(s2, s1, 'one worker per scope');
  assert.equal(backend.started.length, 1);
  assert.deepEqual(registry.snapshot().map((e) => [e.scope.id, e.state, e.liveExecs]), [['r-ws', 'ready', 0]]);
  assert.match(registry.controllerUrl, /^ws:\/\/127\.0\.0\.1:\d+$/);
});

test('a spawn inside the scope runs on the worker: lines, stdin, exit code, HOME and the host-guard pid are the worker\'s', POSIX, async () => {
  const seen = [];
  const r = await inRun(() => superviseSpawn(base({ file: script('read x; echo "in:$x"; echo "home:$HOME"; echo "pid:$WORCA_HOST_PID"; echo "tok:${WORCA_WORKER_TOKEN:-none}"; echo e 1>&2; exit 0'), args: [], stdin: 'hi', env: { PATH: process.env.PATH, WORCA_WORKER_TOKEN: 'leak?' }, onStdoutLine: (l) => seen.push(l) })));
  assert.deepEqual(r, { code: 0 });
  const expectHome = join(worcaHome(), 'worker-homes', 'run', 'r-ws');
  assert.deepEqual(seen, ['in:hi', `home:${expectHome}`, `pid:${process.pid}`, 'tok:none']);
  assert.ok(seen[1].startsWith(`home:${home}`));
});

test('ten thousand lines arrive complete and in order through the send window', POSIX, async () => {
  const seen = [];
  const r = await inRun(() => superviseSpawn(base({ file: script('i=0; while [ $i -lt 10000 ]; do echo "line $i"; i=$((i+1)); done'), args: [], onStdoutLine: (l) => seen.push(l) })));
  assert.deepEqual(r, { code: 0 });
  assert.equal(seen.length, 10000);
  assert.equal(seen[0], 'line 0');
  assert.equal(seen[9999], 'line 9999');
  assert.ok(seen.every((l, i) => l === `line ${i}`), 'in order');
});

test('two execs on one worker run concurrently', POSIX, async () => {
  const t0 = Date.now();
  const [a, b] = await inRun(() => Promise.all([
    superviseSpawn(base({ file: script('sleep 0.4; echo a'), args: [] })),
    superviseSpawn(base({ file: script('sleep 0.4; echo b'), args: [] })),
  ]));
  assert.deepEqual([a, b], [{ code: 0 }, { code: 0 }]);
  assert.ok(Date.now() - t0 < 1500, 'not serialized');
});

test('a fatal stdout line escalates SIGTERM -> SIGKILL on the worker; the exit carries the signal', POSIX, async () => {
  const fatal = new Error('stop');
  const prev = process.env.WORCA_SIGKILL_GRACE_MS;
  process.env.WORCA_SIGKILL_GRACE_MS = '100';
  try {
    const t0 = Date.now();
    const err = await inRun(() => superviseSpawn(base({ file: script("trap '' TERM; echo go; exec sleep 30"), args: [], onStdoutLine: (l) => (l === 'go' ? fatal : undefined) }))).then(() => null, (e) => e);
    assert.equal(err, fatal);
    assert.ok(Date.now() - t0 < 5000);
  } finally { if (prev === undefined) delete process.env.WORCA_SIGKILL_GRACE_MS; else process.env.WORCA_SIGKILL_GRACE_MS = prev; }
});

test('release sends shutdown: the worker ends its children, says bye and exits 0; the backend removes it', POSIX, async () => {
  const s = await registry.acquire(RUN);
  const p = inRun(() => superviseSpawn(base({ file: script('echo up; sleep 30'), args: [] }))).then(() => null, (e) => e);
  await new Promise((r) => setTimeout(r, 150));
  assert.equal(s.liveExecCount, 1);
  await registry.release(RUN, { reason: 'done', graceMs: 300 });
  const handle = backend.started.at(-1);
  assert.equal(await handle.done, 0, 'the worker chose to leave');
  assert.equal(s.closed, true);
  assert.equal(s.closeReason, 'bye');
  const err = await p;
  assert.ok(err, 'the in-flight spawn failed');
  assert.deepEqual(registry.snapshot(), []);
  assert.deepEqual(await backend.listWorkers(), []);
});

test('a chat worker is reclaimed after the idle timeout; the next turn gets a fresh one', async () => {
  const s = await registry.acquire(CHAT);
  await withExecutorScope(CHAT, () => superviseSpawn(base({ file: 'true', args: [] })));
  await new Promise((r) => setTimeout(r, 900));
  assert.equal(s.closed, true, `idle worker left (${s.closeReason})`);
  assert.deepEqual(registry.snapshot(), []);
  const s2 = await registry.acquire(CHAT);
  assert.notEqual(s2, s);
});

test('when the socket drops, the worker kills its children and exits 1; the controller side reports a lost worker', POSIX, async () => {
  const s = await registry.acquire(RUN);
  const handle = backend.started.at(-1);
  const p = inRun(() => superviseSpawn(base({ file: script('echo up; sleep 30'), args: [] }))).then(() => null, (e) => e);
  await new Promise((r) => setTimeout(r, 150));
  s.terminate('cable pulled');
  const err = await p;
  assert.equal(err.code, 'EWORKERLOST');
  assert.equal(err.errorClass, 'network');
  assert.match(err.message, /lost: cable pulled/);
  assert.equal(await handle.done, 1);
  for (let i = 0; i < 60 && handle.worker.liveCount > 0; i += 1) await new Promise((r) => setTimeout(r, 50));
  assert.equal(handle.worker.liveCount, 0, 'the worker killed its children');
  assert.deepEqual(registry.snapshot(), [], 'the entry is gone, so the next acquire starts a new worker');
});

test('tokens: unknown, reused, and a hello for another scope are refused', async () => {
  const url = `${registry.controllerUrl}/worker`;
  const status = (headers) => new Promise((resolve) => {
    const ws = new WebSocket(url, { headers });
    ws.on('unexpected-response', (_req, res) => { resolve(res.statusCode); ws.terminate(); });
    ws.on('open', () => resolve('open'));
    ws.on('error', () => {});
  });
  assert.equal(await status({}), 401);
  assert.equal(await status({ authorization: 'Bearer nope' }), 401);

  // A token the registry minted: grab it from the backend's start call, connect with the wrong scope.
  const captured = [];
  const spyBackend = {
    async start(o) { captured.push(o); return { workerId: o.workerId }; },
    async remove() {}, async listWorkers() { return []; }, describe() { return {}; },
  };
  const reg2 = createWorkerRegistry({ backend: spyBackend, host: '127.0.0.1', port: 0, startTimeoutMs: 1500, log: () => {} });
  await reg2.listen();
  try {
    const pending = reg2.acquire({ kind: 'run', id: 'other' }).then(() => 'connected', (e) => e.message);
    await new Promise((r) => setTimeout(r, 50));
    const { token, workerId } = captured[0];
    const refusal = await new Promise((resolve) => {
      const ws = new WebSocket(`ws://127.0.0.1:${reg2.port}/worker`, { headers: { authorization: `Bearer ${token}` } });
      ws.on('open', () => ws.send(encodeFrame({ type: 'hello', apiVersion: PROTOCOL_VERSION, workerId, scope: { kind: 'chat', id: 'other' }, pid: 1, capabilities: CAPS })));
      ws.on('message', (d) => resolve(JSON.parse(String(d))));
      ws.on('close', () => resolve(null));
      ws.on('error', () => resolve(null));
    });
    assert.equal(refusal?.type, 'refuse');
    assert.match(refusal.reason, /scope or worker id/);
    assert.match(await pending, /did not connect within/);
  } finally { await reg2.closeAll(); }
});

test('a hello with the right token and scope is accepted once; a second connection with the same token is refused', async () => {
  const captured = [];
  const spyBackend = { async start(o) { captured.push(o); return { workerId: o.workerId }; }, async remove() {}, async listWorkers() { return []; }, describe() { return {}; } };
  const reg2 = createWorkerRegistry({ backend: spyBackend, host: '127.0.0.1', port: 0, startTimeoutMs: 3000, log: () => {} });
  await reg2.listen();
  try {
    const pending = reg2.acquire({ kind: 'run', id: 'one' });
    await new Promise((r) => setTimeout(r, 50));
    const { token, workerId, controllerUrl } = captured[0];
    const w = startWorker({ url: controllerUrl, token, scope: { kind: 'run', id: 'one' }, workerId, home: tmp(), capabilities: CAPS });
    const session = await pending;
    assert.equal(session.workerId, workerId);
    const again = await new Promise((resolve) => {
      const ws = new WebSocket(`ws://127.0.0.1:${reg2.port}/worker`, { headers: { authorization: `Bearer ${token}` } });
      ws.on('unexpected-response', (_req, res) => { resolve(res.statusCode); ws.terminate(); });
      ws.on('open', () => resolve('open'));
      ws.on('error', () => {});
    });
    assert.equal(again, 401, 'a token is single-use');
    await reg2.release({ kind: 'run', id: 'one' });
    assert.equal(await w.done, 0);
  } finally { await reg2.closeAll(); }
});

test('a scope gets one worker per engine; a spawn that names none uses the one the scope has; release ends them all', async () => {
  const before = backend.started.length;
  const claude = await registry.acquire({ ...RUN, engine: 'claude' });
  const codex = await registry.acquire(RUN, { engine: 'codex' });
  assert.notEqual(codex, claude, "another engine is another worker (its own image under a container backend)");
  assert.equal(await registry.acquire(RUN, { engine: 'claude' }), claude);
  assert.equal(await registry.acquire(RUN, { engine: 'codex' }), codex);
  assert.equal(await registry.acquire(RUN), claude, 'no engine named (a script node): the worker the scope already has');
  assert.deepEqual(backend.started.slice(before).map((h) => h.engine), ['claude', 'codex'], 'the backend is told which engine to start');
  assert.deepEqual(registry.snapshot().map((e) => [e.scope.id, e.engine, e.state]).sort(), [['r-ws', 'claude', 'ready'], ['r-ws', 'codex', 'ready']]);
  assert.equal(await registry.release(RUN, { reason: 'test' }), true);
  assert.deepEqual(registry.snapshot(), [], 'both are gone');
});

test('a backend whose engines share one image keeps one worker per scope', async () => {
  const b2 = createInProcessBackend({ homeRoot: worcaHome(), capabilities: CAPS });
  b2.classFor = () => 'registry.example/worca-worker:1-all';
  const reg2 = createWorkerRegistry({ backend: b2, host: '127.0.0.1', port: 0, startTimeoutMs: 5000, pingMs: 200, log: () => {} });
  await reg2.listen();
  try {
    const a = await reg2.acquire({ kind: 'run', id: 'm' }, { engine: 'claude' });
    const c = await reg2.acquire({ kind: 'run', id: 'm' }, { engine: 'codex' });
    assert.equal(c, a);
    assert.equal(b2.started.length, 1);
  } finally { await reg2.closeAll(); }
});

test('every caller waiting on a start that fails is rejected, not only the first', async () => {
  let fire; const gate = new Promise((r) => { fire = r; });
  const b = { async start() { await gate; throw new Error('no image'); }, async remove() {}, async listWorkers() { return []; }, describe() { return {}; } };
  const reg2 = createWorkerRegistry({ backend: b, host: '127.0.0.1', port: 0, log: () => {} });
  await reg2.listen();
  try {
    const first = reg2.acquire({ kind: 'run', id: 'x' }).then(() => null, (e) => e);
    const second = reg2.acquire({ kind: 'run', id: 'x' }).then(() => null, (e) => e);   // a parallel node of the same run
    await new Promise((r) => setTimeout(r, 20));
    fire();
    for (const e of await Promise.all([first, second])) {
      assert.match(e?.message || '', /cannot start a worker for run:x: no image/);
      assert.equal(e.errorClass, 'network');
    }
    assert.deepEqual(reg2.snapshot(), []);
  } finally { await reg2.closeAll(); }
});

test('a stop while the worker is still starting ends the wait at once; the start goes on only while someone else waits', async () => {
  const starts = [];      // one per backend.start: { signal, finish }
  const removed = [];
  const b = {
    start({ workerId, signal }) { return new Promise((resolve) => starts.push({ signal, finish: () => resolve({ workerId }) })); },
    async remove(h, o) { removed.push([h.workerId, o.reason]); }, async listWorkers() { return []; }, describe() { return {}; },
  };
  const reg2 = createWorkerRegistry({ backend: b, host: '127.0.0.1', port: 0, log: () => {} });
  await reg2.listen();
  const tick = () => new Promise((r) => setTimeout(r, 20));
  try {
    // The only caller stops: an AbortError now, the backend's own wait is told to stop, and a container that still
    // arrives is removed.
    const ctl = new AbortController();
    const p = reg2.acquire({ kind: 'run', id: 'y' }, { signal: ctl.signal }).then(() => null, (e) => e);
    await tick();
    ctl.abort();
    assert.equal((await p).name, 'AbortError');
    assert.equal(starts[0].signal.aborted, true);
    assert.deepEqual(reg2.snapshot(), []);
    starts[0].finish();
    await tick();
    assert.equal(removed.length, 1);
    assert.equal(removed[0][1], 'start abandoned');
    // Already stopped: nothing is started at all.
    await assert.rejects(reg2.acquire({ kind: 'run', id: 'z' }, { signal: ctl.signal }), { name: 'AbortError' });
    assert.equal(starts.length, 1);
    // Two callers, one stops: its wait ends, the other's start goes on.
    const ctl2 = new AbortController();
    const stopper = reg2.acquire({ kind: 'run', id: 'w' }, { signal: ctl2.signal }).then(() => null, (e) => e);
    const waiter = reg2.acquire({ kind: 'run', id: 'w' }).then(() => null, (e) => e);
    await tick();
    ctl2.abort();
    assert.equal((await stopper).name, 'AbortError');
    assert.equal(starts[1].signal.aborted, false, 'someone still waits for this worker');
    assert.deepEqual(reg2.snapshot().map((e) => e.state), ['starting']);
    await reg2.release({ kind: 'run', id: 'w' }, { reason: 'test' });
    assert.match((await waiter).message, /released before it connected/);
    assert.equal(starts[1].signal.aborted, true, 'a released start stops the backend too');
  } finally { await reg2.closeAll(); }
});

test('an exec that cannot be sent says why: a malformed request is EPROTO, a worker that is gone is a lost worker', async () => {
  const s = await registry.acquire(RUN);
  const outcome = (spec) => new Promise((resolve) => s.exec(spec, { onStarted() {}, onLines() {}, onExit() {}, onLost() {}, onSpawnError: resolve }));
  const bad = await outcome({ file: '', args: [], cwd: tmpdir(), env: {}, stdin: null });
  assert.deepEqual([bad.code, bad.errorClass], ['EPROTO', undefined], 'not E2BIG: nothing about this request is too large');
  assert.match(bad.message, /invalid exec frame/);
  const big = await outcome({ file: '/bin/sh', args: [], cwd: tmpdir(), env: {}, stdin: 'x'.repeat(8 * 1024 * 1024 + 1) });
  assert.equal(big.code, 'E2BIG');
  await registry.release(RUN, { reason: 'test' });
  const gone = await outcome({ file: '/bin/sh', args: ['-c', 'true'], cwd: tmpdir(), env: {}, stdin: null });
  assert.deepEqual([gone.code, gone.errorClass], ['EWORKERLOST', 'network'], 'retried on a fresh worker by the failure policy');
});

test('reapAtBoot removes every worker the backend still lists, except the ones this controller started', async () => {
  const removed = [];
  const listed = [{ workerId: 'old-1', bootId: 'ctl-previous' }, { workerId: 'old-2' }, { workerId: 'mine-1', bootId: 'ctl-now' }, { workerId: 'mine-2', own: true }];
  const b = { async start() { throw new Error('unused'); }, async remove(h, o) { removed.push([h.workerId, o.reason]); }, async listWorkers() { return listed; }, describe() { return {}; } };
  const reg2 = createWorkerRegistry({ backend: b, host: '127.0.0.1', port: 0, bootId: 'ctl-now', log: () => {} });
  assert.deepEqual(await reg2.reapAtBoot(), { removed: 2 }, 'maintenance runs while the server serves: a worker of this boot is not an orphan');
  assert.deepEqual(removed, [['old-1', 'orphan at boot'], ['old-2', 'orphan at boot']]);
});

test('a worker whose container takes longer to come up than the token lives still gets in; the time to live counts from the container', async () => {
  // A cold node pulling the image: the backend needs far longer than tokenTtlMs before the worker process even exists.
  const b2 = createInProcessBackend({ homeRoot: worcaHome(), capabilities: CAPS, beforeStart: () => new Promise((r) => setTimeout(r, 200)) });
  const reg2 = createWorkerRegistry({ backend: b2, host: '127.0.0.1', port: 0, startTimeoutMs: 5000, tokenTtlMs: 40, pingMs: 200, log: () => {} });
  await reg2.listen();
  try {
    const s = await reg2.acquire({ kind: 'run', id: 'slow' });
    assert.equal(s.scope.id, 'slow');
  } finally { await reg2.closeAll(); }
});

test('a hello that arrives after its start was released is refused: a release is never undone by a late worker', async () => {
  const captured = [];
  const removed = [];
  const b = { start(o) { captured.push(o); return new Promise(() => {}); }, async remove(h) { removed.push(h); }, async listWorkers() { return []; }, describe() { return {}; } };
  const reg2 = createWorkerRegistry({ backend: b, host: '127.0.0.1', port: 0, log: () => {} });
  await reg2.listen();
  try {
    const scope = { kind: 'run', id: 'late' };
    const waiting = reg2.acquire(scope).then(() => null, (e) => e);
    for (let i = 0; i < 100 && !captured.length; i++) await new Promise((r) => setTimeout(r, 5));
    const { token, workerId } = captured[0];
    // The worker's socket is up (the upgrade passed while the start was live)…
    const ws = new WebSocket(`${reg2.controllerUrl}/worker`, { headers: { authorization: `Bearer ${token}` } });
    await new Promise((resolve, reject) => { ws.on('open', resolve); ws.on('error', reject); });
    // …the run is released before its hello…
    await reg2.release(scope, { reason: 'paused' });
    assert.match((await waiting).message, /released before it connected/);
    // …and the hello comes late.
    const closed = new Promise((resolve) => ws.on('close', (code) => resolve(code)));
    const frames = [];
    ws.on('message', (d) => frames.push(JSON.parse(String(d))));
    ws.send(encodeFrame({ type: 'hello', apiVersion: PROTOCOL_VERSION, workerId, scope, pid: 1, capabilities: CAPS }));
    assert.equal(await closed, 4001);
    assert.deepEqual(frames.map((f) => f.type), ['refuse']);
    assert.deepEqual(reg2.snapshot(), [], 'the released worker did not come back as ready');
  } finally { await reg2.closeAll(); }
});

test('a backend that fails to start a worker makes acquire reject with a network-class error', async () => {
  const b = { async start() { throw new Error('no image'); }, async remove() {}, async listWorkers() { return []; }, describe() { return {}; } };
  const reg2 = createWorkerRegistry({ backend: b, host: '127.0.0.1', port: 0, log: () => {} });
  await reg2.listen();
  try {
    const err = await reg2.acquire({ kind: 'run', id: 'x' }).then(() => null, (e) => e);
    assert.match(err.message, /cannot start a worker for run:x: no image/);
    assert.equal(err.errorClass, 'network');
    assert.deepEqual(reg2.snapshot(), []);
  } finally { await reg2.closeAll(); }
});
