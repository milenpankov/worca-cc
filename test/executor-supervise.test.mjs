// test/executor-supervise.test.mjs — the executor seam in superviseSpawn and spawnScript: a spawn
// inside a run scope under an installed remote executor runs on the scope's worker session (here
// a loopback session in this process), and behaves exactly like a local spawn to its caller.
import { test, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, chmodSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useTempHome } from './helpers/temp-home.mjs';
import { createLoopbackRegistry, createLoopbackSession } from './helpers/loopback-worker-session.mjs';
import { superviseSpawn, missingOnWorker, engineOfSpawn } from '../src/core/engines/spawn.mjs';
import { CODEX_CLI } from '../src/core/engines/cli-specs.mjs';
import { installExecutorFromEnv } from '../src/core/executor/controller.mjs';
import { spawnScript } from '../src/core/graph/script-runner.mjs';
import { installExecutor, isRemoteExecutor, stagingBase, executorKind, executorConfigError, spawnsRemotely } from '../src/core/executor/index.mjs';
import { withExecutorScope, currentExecutorScope, scopeKey, parseScopeKey } from '../src/core/executor/scope.mjs';
import { worcaHome } from '../src/core/projects.mjs';

useTempHome(after);
const POSIX = process.platform === 'win32' ? { skip: 'POSIX shell fixtures' } : {};
const dirs = [];
const tmp = () => { const d = mkdtempSync(join(tmpdir(), 'worca-exec-')); dirs.push(d); return d; };
after(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });
afterEach(() => installExecutor(null));

function script(body) {
  const d = tmp();
  const f = join(d, 'bin.sh');
  writeFileSync(f, `#!/bin/sh\n${body}\n`);
  chmodSync(f, 0o755);
  return f;
}
const base = (over = {}) => ({
  displayBin: 'fake', cwd: tmpdir(), env: undefined, stdin: null, signal: undefined, asAgent: false,
  stagedDir: null, cleanup: () => {}, onEvent: () => {}, onStdoutLine: () => {},
  stdoutErrorDetail: () => '', classify: () => null,
  spawnError: (err, prefix) => Object.assign(new Error(`${prefix}: ${err.message}`), err.code ? { code: err.code } : {}),
  onDone: (code) => ({ code }), ...over,
});
const RUN = { kind: 'run', id: 'r-1' };
const remote = (registry, fn) => { installExecutor({ kind: 'docker', registry }); return withExecutorScope(RUN, fn); };

test('an engine binary the worker image lacks is refused before the exec, naming the image to fix', POSIX, async () => {
  const caps = { engines: { claude: { bin: '/usr/local/bin/claude', version: 'x' }, codex: null, gemini: null } };
  // What the worker reports is the whole truth: absent engine → a message with the fix; present → nothing to say.
  assert.equal(missingOnWorker({ workerId: 'w-1', capabilities: caps }, '/opt/claude'), null);
  assert.match(missingOnWorker({ workerId: 'w-1', capabilities: caps }, 'codex'), /worker w-1 has no codex: the codex engine is not in its image \(it has claude\)\. Point the codex worker image .*WORCA_WORKER_IMAGE .*worker\.engines\.codex\.image/);
  assert.match(missingOnWorker({ workerId: 'w-1', capabilities: caps }, 'C:\\tools\\gemini.cmd'), /has no gemini/);
  assert.equal(missingOnWorker({ workerId: 'w-1', capabilities: caps }, '/bin/sh'), null, 'not an engine binary: the worker decides');
  assert.equal(missingOnWorker({ workerId: 'w-1', capabilities: null }, 'codex'), null, 'no capabilities reported: nothing to judge');
  assert.equal(missingOnWorker(null, 'codex'), null);
  // Through the seam: the spawn fails at once, cleanup runs, nothing reaches the worker.
  const session = createLoopbackSession({ workerId: 'w-caps', capabilities: caps });
  const registry = createLoopbackRegistry({ session });
  let cleaned = 0;
  await assert.rejects(
    () => remote(registry, () => superviseSpawn(base({ file: 'codex', args: ['--version'], displayBin: 'codex', cleanup: () => { cleaned++; } }))),
    (err) => { assert.match(err.message, /Failed to spawn codex: worker w-caps has no codex/); assert.equal(err.code, 'EWORKERNOBIN'); return true; });
  assert.equal(cleaned, 1);
  assert.equal(session.specs.length, 0, 'no exec was sent');
  // The binary the image has runs as before.
  const sh = script('echo from-worker');
  const lines = [];
  const r = await remote(registry, () => superviseSpawn(base({ file: sh, args: [], displayBin: 'sh', onStdoutLine: (l) => lines.push(l) })));
  assert.deepEqual([r.code, lines], [0, ['from-worker']]);
});

test("a spawn names its engine to the registry; a worker that cannot start keeps its recovery class; a stop stays an abort", POSIX, async () => {
  assert.equal(engineOfSpawn({ cli: CODEX_CLI, file: '/opt/anything' }), 'codex', "the adapter's CLI spec names the engine");
  assert.equal(engineOfSpawn({ file: '/usr/local/bin/cursor-agent' }), 'cursor', 'else the binary name does');
  assert.equal(engineOfSpawn({ file: '/bin/sh' }), null, 'a script is no engine');
  const sh = script('echo hi');
  const registry = createLoopbackRegistry();
  await remote(registry, () => superviseSpawn(base({ file: sh, args: [], cli: CODEX_CLI })));
  await withExecutorScope({ ...RUN, engine: 'gemini' }, () => superviseSpawn(base({ file: sh, args: [] })));
  assert.deepEqual(registry.asked, [{ engine: 'codex' }, { engine: 'gemini' }], "the spawn's own engine, else the scope's");
  // Six of the seven adapters build a fresh Error in spawnError: the class and code of what it wraps must survive it.
  const dropping = (err, prefix) => new Error(`${prefix}: ${err.message}`);
  const failing = createLoopbackRegistry({ fail: Object.assign(new Error('cannot start a worker for run:r-1: no image'), { code: 'EWORKERSTART', errorClass: 'network' }) });
  await assert.rejects(() => remote(failing, () => superviseSpawn(base({ file: sh, args: [], spawnError: dropping }))), (e) => {
    assert.match(e.message, /Failed to start a worker for fake: cannot start a worker/);
    assert.equal(e.errorClass, 'network', 'retried or paused by the failure policy, not failed hard');
    assert.equal(e.code, 'EWORKERSTART');
    return true;
  });
  const caps = { engines: { claude: { bin: '/usr/local/bin/claude', version: 'x' }, codex: null } };
  const lacking = createLoopbackRegistry({ session: createLoopbackSession({ workerId: 'w-c', capabilities: caps }) });
  await assert.rejects(() => remote(lacking, () => superviseSpawn(base({ file: 'codex', args: [], displayBin: 'codex', spawnError: dropping }))), (e) => {
    assert.equal(e.errorClass, 'auth', 'a missing engine is something the person fixes: paused with the fix named');
    assert.equal(e.code, 'EWORKERNOBIN');
    return true;
  });
  // Stopped while the worker was starting: the registry's AbortError passes through unwrapped, cleanup still runs.
  const stopped = createLoopbackRegistry({ fail: Object.assign(new Error('aborted'), { name: 'AbortError' }) });
  let cleaned = 0;
  await assert.rejects(() => remote(stopped, () => superviseSpawn(base({ file: sh, args: [], spawnError: dropping, cleanup: () => { cleaned++; } }))), { name: 'AbortError', message: 'aborted' });
  assert.equal(cleaned, 1);
  await assert.rejects(() => remote(stopped, () => spawnScript({ file: sh, args: [], cwd: tmpdir(), env: process.env, timeoutMs: 5000, onLine: () => {} })), { name: 'AbortError' });
});

test('a container executor without WORCA_CONTROLLER_URL refuses to install: workers cannot dial the controller on loopback', async () => {
  const saved = Object.fromEntries(['WORCA_EXECUTOR', 'WORCA_CONTROLLER_URL', 'WORCA_EXECUTOR_BACKEND'].map((k) => [k, process.env[k]]));
  process.env.WORCA_EXECUTOR = 'kubernetes';
  delete process.env.WORCA_CONTROLLER_URL; delete process.env.WORCA_EXECUTOR_BACKEND;
  try {
    await assert.rejects(() => installExecutorFromEnv({ log: () => {} }), /WORCA_CONTROLLER_URL is not set: with WORCA_EXECUTOR=kubernetes .* ws:\/\/worca:4319/);
    assert.equal(isRemoteExecutor(), false, 'nothing was installed');
  } finally {
    for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  }
});

test('scope helpers: normalization, keys, and the async context', async () => {
  assert.equal(currentExecutorScope(), null);
  assert.equal(scopeKey({ kind: 'chat', id: ' t1 ' }), 'chat:t1');
  assert.equal(scopeKey({ kind: 'job', id: 'x' }), null);
  assert.deepEqual(parseScopeKey('run:abc:def'), { kind: 'run', id: 'abc:def' });
  assert.equal(scopeKey({ kind: 'run', id: 'r', engine: 'codex' }), 'run:r', "the scope's engine is a hint, not part of its identity");
  assert.deepEqual(await withExecutorScope({ kind: 'run', id: 'r', engine: ' codex ' }, () => currentExecutorScope()), { kind: 'run', id: 'r', engine: 'codex' });
  assert.equal(parseScopeKey('nope'), null);
  await withExecutorScope(RUN, async () => {
    await new Promise((r) => setTimeout(r, 1));
    assert.deepEqual(currentExecutorScope(), RUN);
  });
  assert.equal(await withExecutorScope({ kind: 'bogus' }, () => currentExecutorScope()), null, 'an unusable scope leaves the context unset');
});

test('executor kind: unset or unknown is local; the config error names a bad value', () => {
  assert.equal(executorKind({}), 'local');
  assert.equal(executorKind({ WORCA_EXECUTOR: 'Docker' }), 'docker');
  assert.equal(executorKind({ WORCA_EXECUTOR: 'swarm' }), 'local');
  assert.equal(executorConfigError({}), null);
  assert.match(executorConfigError({ WORCA_EXECUTOR: 'swarm' }), /WORCA_EXECUTOR=swarm is not one of local, docker, kubernetes/);
  assert.equal(isRemoteExecutor(), false);
  installExecutor({ kind: 'local', registry: {} });
  assert.equal(isRemoteExecutor(), false, 'installing local is a no-op');
});

test('stagingBase is the OS tmpdir locally and under worca home with a remote executor', () => {
  assert.equal(stagingBase(), tmpdir());
  installExecutor({ kind: 'kubernetes', registry: {} });
  assert.equal(stagingBase(), join(worcaHome(), 'tmp', 'stage'));
});

test('without a scope a spawn stays local even under a remote executor; the registry is never asked', POSIX, async () => {
  const registry = createLoopbackRegistry();
  installExecutor({ kind: 'docker', registry });
  assert.equal(spawnsRemotely(), false);
  const seen = [];
  const r = await superviseSpawn(base({ file: script('echo local'), args: [], onStdoutLine: (l) => seen.push(l) }));
  assert.deepEqual(r, { code: 0 });
  assert.deepEqual(seen, ['local']);
  assert.deepEqual(registry.acquired, []);
  assert.equal(registry.session.specs.length, 0);
});

test('inside a run scope the spawn runs on the worker session: lines, stderr events, exit code, cleanup once', POSIX, async () => {
  const registry = createLoopbackRegistry();
  const seen = []; const events = []; let cleaned = 0;
  const file = script('echo one; echo two; echo warn 1>&2; read x; echo "got $x"');
  const r = await remote(registry, () => superviseSpawn(base({
    file, args: ['a1'], stdin: 'from-stdin', env: { PATH: process.env.PATH, MARK: 'm' },
    cleanup: () => { cleaned += 1; }, onStdoutLine: (l) => seen.push(l), onEvent: (e) => events.push(e),
  })));
  assert.deepEqual(r, { code: 0 });
  assert.deepEqual(seen, ['one', 'two', 'got from-stdin']);
  assert.deepEqual(events, [{ type: 'stderr', stream: 'err', text: 'warn' }]);
  assert.equal(cleaned, 1);
  assert.deepEqual(registry.acquired, [RUN]);
  const [spec] = registry.session.specs;
  assert.equal(spec.file, file);
  assert.deepEqual(spec.args, ['a1']);
  assert.equal(spec.stdin, 'from-stdin');
  assert.equal(spec.env.MARK, 'm', 'the composed env rides the exec as given');
  assert.equal(spec.detached, true);
});

test('with no env the worker gets this process\'s environment explicitly (a remote exec never inherits)', POSIX, async () => {
  const registry = createLoopbackRegistry();
  process.env.WORCA_TEST_INHERIT_MARK = 'yes';
  try {
    const seen = [];
    await remote(registry, () => superviseSpawn(base({ file: script('echo "$WORCA_TEST_INHERIT_MARK"'), args: [], onStdoutLine: (l) => seen.push(l) })));
    assert.deepEqual(seen, ['yes']);
    assert.equal(registry.session.specs[0].env.WORCA_TEST_INHERIT_MARK, 'yes');
  } finally { delete process.env.WORCA_TEST_INHERIT_MARK; }
});

test('non-zero exit keeps the stderr detail and class; a fatal stdout line terminates through the session (SIGTERM, then SIGKILL)', POSIX, async () => {
  const registry = createLoopbackRegistry();
  const err1 = await remote(registry, () => superviseSpawn(base({ file: script('echo "429 slow down" 1>&2; exit 2'), args: [], classify: (m) => (/429/.test(m) ? 'rate_limit' : null) })))
    .then(() => null, (e) => e);
  assert.match(err1.message, /^fake exited with code 2: 429 slow down/);
  assert.equal(err1.errorClass, 'rate_limit');
  assert.equal(err1.stream, 'err');

  const expected = Object.assign(new Error('terminal stream event'), { errorClass: 'usage_limit' });
  const prev = process.env.WORCA_SIGKILL_GRACE_MS;
  process.env.WORCA_SIGKILL_GRACE_MS = '50';
  try {
    const started = Date.now();
    const err2 = await remote(registry, () => superviseSpawn(base({
      file: script("trap '' TERM; echo fatal; exec sleep 30"), args: [],
      onStdoutLine: (line) => (line === 'fatal' ? expected : undefined),
    }))).then(() => null, (e) => e);
    assert.equal(err2, expected);
    assert.ok(Date.now() - started < 5000, 'does not wait for the lingering child');
    const sigs = registry.session.signals.map(([, s]) => s);
    assert.deepEqual(sigs.slice(-2), ['SIGTERM', 'SIGKILL'], `escalation relayed through the session: ${sigs}`);
  } finally {
    if (prev === undefined) delete process.env.WORCA_SIGKILL_GRACE_MS; else process.env.WORCA_SIGKILL_GRACE_MS = prev;
  }
});

test('abort rejects with AbortError, signalled through the session, and runs cleanup once', POSIX, async () => {
  const registry = createLoopbackRegistry();
  const ac = new AbortController(); let cleaned = 0;
  const p = remote(registry, () => superviseSpawn(base({ file: script('echo started; sleep 30'), args: [], signal: ac.signal, cleanup: () => { cleaned += 1; }, onStdoutLine: (l) => { if (l === 'started') ac.abort(); } })));
  const err = await p.then(() => null, (e) => e);
  assert.equal(err.name, 'AbortError');
  assert.equal(cleaned, 1);
  assert.ok(registry.session.signals.some(([, s]) => s === 'SIGTERM'));
});

test('a worker that vanishes mid-run rejects with a network-class error naming the worker', POSIX, async () => {
  const session = createLoopbackSession({ workerId: 'w-gone' });
  const registry = createLoopbackRegistry({ session });
  const p = remote(registry, () => superviseSpawn(base({ file: script('echo up; sleep 30'), args: [], onStdoutLine: (l) => { if (l === 'up') setTimeout(() => session.lose('container exited'), 5); } })));
  const err = await p.then(() => null, (e) => e);
  assert.match(err.message, /fake error: worker w-gone lost: container exited/);
  assert.equal(err.errorClass, 'network');
  assert.equal(err.code, 'EWORKERLOST');
});

test('a registry that cannot provide a worker rejects before anything spawns, with cleanup', POSIX, async () => {
  const registry = createLoopbackRegistry({ fail: new Error('no worker image') });
  let cleaned = 0;
  const err = await remote(registry, () => superviseSpawn(base({ file: script('echo never'), args: [], cleanup: () => { cleaned += 1; } }))).then(() => null, (e) => e);
  assert.match(err.message, /Failed to start a worker for fake: no worker image/);
  assert.equal(cleaned, 1);
  assert.equal(registry.session.specs.length, 0);
});

test('asAgent is ignored on a worker: no sudo wrapper, the file itself is the exec', POSIX, async () => {
  const registry = createLoopbackRegistry();
  const file = script('echo as-me');
  const seen = [];
  await remote(registry, () => superviseSpawn(base({ file, args: [], asAgent: true, onStdoutLine: (l) => seen.push(l) })));
  assert.deepEqual(seen, ['as-me']);
  assert.equal(registry.session.specs[0].file, file);
});

test('spawnScript runs a script node on the worker session with capture, exit code and abort', POSIX, async () => {
  const registry = createLoopbackRegistry();
  const lines = [];
  const r = await remote(registry, () => spawnScript({ file: 'sh', args: ['-c', 'echo a; echo b 1>&2; exit 4'], cwd: tmpdir(), env: { PATH: process.env.PATH }, timeoutMs: 10_000, onLine: (l) => lines.push(l), stdoutMode: 'capture' }));
  assert.equal(r.exitCode, 4);
  assert.ok(lines.includes('a') && lines.includes('b'));
  assert.match(r.capture, /a/);
  assert.equal(registry.session.specs[0].file, 'sh');

  const ac = new AbortController();
  const p = remote(registry, () => spawnScript({ file: 'sh', args: ['-c', 'echo go; sleep 30'], cwd: tmpdir(), env: { PATH: process.env.PATH }, timeoutMs: 10_000, signal: ac.signal, onLine: (l) => { if (l === 'go') ac.abort(); }, stdoutMode: 'capture' }));
  const r2 = await p;
  assert.equal(r2.aborted, true);
  assert.ok(registry.session.signals.some(([, s]) => s === 'SIGKILL'), 'killTree relayed SIGKILL through the session');
});

test('spawnScript without a scope is unchanged (local child, registry untouched)', POSIX, async () => {
  const registry = createLoopbackRegistry();
  installExecutor({ kind: 'docker', registry });
  const r = await spawnScript({ file: 'sh', args: ['-c', 'echo local'], cwd: tmpdir(), env: { PATH: process.env.PATH }, timeoutMs: 10_000, onLine: () => {}, stdoutMode: 'capture' });
  assert.equal(r.exitCode, 0);
  assert.deepEqual(registry.acquired, []);
  assert.equal(existsSync(join(worcaHome(), 'tmp', 'stage')), false, 'nothing staged under the home');
});
