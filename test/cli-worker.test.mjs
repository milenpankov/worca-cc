// test/cli-worker.test.mjs — `worca worker`: the assignment comes from the environment, and an
// incomplete one is a configuration error (exit 78) that names what is missing.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readWorkerConfig, EX_CONFIG } from '../src/worker/main.mjs';

const CLI = fileURLToPath(new URL('../src/cli/worca-cc.mjs', import.meta.url));

test('readWorkerConfig names every missing variable', () => {
  const r = readWorkerConfig({});
  assert.deepEqual(r.errors, [
    'WORCA_CONTROLLER_URL is not set',
    'WORCA_WORKER_SCOPE is not set',
    'WORCA_WORKER_TOKEN (or WORCA_WORKER_TOKEN_FILE) is not set',
    'WORCA_WORKER_ID is not set',
  ]);
  const bad = readWorkerConfig({ WORCA_CONTROLLER_URL: 'http://x', WORCA_WORKER_SCOPE: 'job:1', WORCA_WORKER_TOKEN: 't', WORCA_WORKER_ID: 'w' });
  assert.match(bad.errors[0], /must be a ws:\/\/ or wss:\/\/ URL/);
  assert.match(bad.errors[1], /must be run:<id> or chat:<id>/);
});

test('a complete assignment resolves the scope, the home and the idle timeout', () => {
  const r = readWorkerConfig({ WORCA_CONTROLLER_URL: 'ws://worca:4319', WORCA_WORKER_SCOPE: 'chat:t-1', WORCA_WORKER_TOKEN: 'tok', WORCA_WORKER_ID: 'w-1', WORCA_WORKER_HOME: '/tmp/h', WORCA_WORKER_IDLE_MS: '60000' });
  assert.deepEqual(r.errors, []);
  assert.deepEqual(r.scope, { kind: 'chat', id: 't-1' });
  assert.equal(r.home, '/tmp/h');
  assert.equal(r.idleMs, 60000);
  assert.equal(r.token, 'tok');
});

test('`worca worker` with no assignment exits 78 and says what is missing', () => {
  const env = { ...process.env };
  for (const k of Object.keys(env)) if (k.startsWith('WORCA_WORKER') || k === 'WORCA_CONTROLLER_URL') delete env[k];
  const r = spawnSync(process.execPath, [CLI, 'worker'], { env, encoding: 'utf8', timeout: 30_000 });
  assert.equal(r.status, EX_CONFIG, r.stderr);
  assert.match(r.stderr, /worca worker: WORCA_CONTROLLER_URL is not set/);
  assert.match(r.stderr, /WORCA_WORKER_TOKEN/);
});

test('`worca worker --check` prints the engines found without connecting anywhere', () => {
  const r = spawnSync(process.execPath, [CLI, 'worker', '--check'], { env: { ...process.env, PATH: '/nonexistent' }, encoding: 'utf8', timeout: 30_000 });
  assert.equal(r.status, 0, r.stderr);
  const caps = JSON.parse(r.stdout);
  assert.equal(caps.engines.claude, null);
  assert.equal(caps.node, process.version);
});
