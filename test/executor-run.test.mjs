// test/executor-run.test.mjs — the harness under a remote executor: every spawn inside the run's
// loop carries the run's scope (so it goes to the run's worker), and the worker is released —
// with its HOME removed — when the run ends. The agents are stubbed (runners); each stub spawns
// one real process through the supervisor seam, the way an engine adapter would.
import { test, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, chmodSync, rmSync, mkdirSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useTempHome } from './helpers/temp-home.mjs';
import { gitDir } from './helpers/git-dir.mjs';
import { ENGINES } from './helpers/engines.mjs';
import { createLoopbackRegistry } from './helpers/loopback-worker-session.mjs';
import { installExecutor, workerHomeDir } from '../src/core/executor/index.mjs';
import { currentExecutorScope } from '../src/core/executor/scope.mjs';
import { superviseSpawn } from '../src/core/engines/spawn.mjs';

useTempHome(after);
afterEach(() => installExecutor(null));
const POSIX = process.platform === 'win32' ? { skip: 'POSIX shell fixtures' } : {};
const dirs = [];
after(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });
function script(body) { const d = mkdtempSync(join(tmpdir(), 'worca-run-')); dirs.push(d); const f = join(d, 'bin.sh'); writeFileSync(f, `#!/bin/sh\n${body}\n`); chmodSync(f, 0o755); return f; }

test('a run under a remote executor: scoped spawns on the run\'s worker, released with its HOME when done', POSIX, { timeout: 120000 }, async () => {
  const registry = createLoopbackRegistry();
  installExecutor({ kind: 'docker', registry });
  const scopes = []; const outputs = [];
  let homeMade = null;
  const runner = async (ctx) => {
    const scope = currentExecutorScope();
    scopes.push(scope);
    if (scope && !homeMade) { homeMade = workerHomeDir(scope); mkdirSync(homeMade, { recursive: true }); }
    await superviseSpawn({
      file: script(`echo node:${ctx.node?.key || '?'}`), args: [], displayBin: 'stub', cwd: ctx.cwd || tmpdir(), env: { PATH: process.env.PATH }, stdin: null,
      cleanup: () => {}, onEvent: () => {}, onStdoutLine: (l) => outputs.push(l), stdoutErrorDetail: () => '', classify: () => null,
      spawnError: (e, p) => new Error(`${p}: ${e.message}`), onDone: (code) => ({ code }),
    });
    return {};
  };
  const runners = Object.fromEntries(['producer', 'reviewer', 'clarify', 'planner', 'refiner', 'decomposer', 'implementer', 'gate', 'agent'].map((k) => [k, runner]));
  const orch = ENGINES[0].create({ projectDir: gitDir('exec'), workflowId: 'wf_default', prompt: 'demo task', claude: { mock: true }, auto: true, runners });
  const res = await orch.run();
  assert.equal(res.status, 'done', JSON.stringify(res));
  const id = orch.pipeline.id;
  assert.ok(scopes.length >= 2, `agents ran through the stub (${scopes.length})`);
  assert.ok(scopes.every((s) => s && s.kind === 'run' && s.id === id), `every spawn carried the run's scope: ${JSON.stringify(scopes)}`);
  assert.ok(registry.acquired.length >= 2 && registry.acquired.every((s) => s.kind === 'run' && s.id === id), 'each spawn asked the registry for the run\'s worker');
  assert.ok(outputs.some((l) => l.startsWith('node:')), 'the stubs\' processes ran on the loopback session');
  assert.deepEqual(registry.released, [{ scope: { kind: 'run', id }, reason: 'done' }], 'released once, at the end, with the terminal status');
  assert.equal(existsSync(homeMade), false, 'the worker HOME went with the run');
});
