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

import { childEnv } from '../src/worker/exec.mjs';

test("a worker child's env is the controller's composed env with this container's own paths", () => {
  // What the controller sent: composed in ITS container, where the entrypoint built a CA bundle under its /tmp.
  const sent = {
    PATH: '/usr/local/bin:/usr/bin', HOME: '/home/worca', ANTHROPIC_API_KEY: 'k',
    SSL_CERT_FILE: '/tmp/worca-ca.AAAA/bundle.pem', GIT_SSL_CAINFO: '/tmp/worca-ca.AAAA/bundle.pem', CURL_CA_BUNDLE: '/tmp/worca-ca.AAAA/bundle.pem',
    NODE_EXTRA_CA_CERTS: '/etc/worca/ca.pem', npm_config_cache: '/home/worca/.npm',
    HTTPS_PROXY: 'http://127.0.0.1:3128', HTTP_PROXY: 'http://egress:3128', WORCA_WORKER_TOKEN: 't', WORCA_CONTROLLER_URL: 'ws://worca:4319',
  };
  // This worker: its entrypoint rebuilt the bundle under its own /tmp and pointed the caches at the shared volume.
  const workerEnv = { SSL_CERT_FILE: '/tmp/worca-ca.BBBB/bundle.pem', GIT_SSL_CAINFO: '/tmp/worca-ca.BBBB/bundle.pem',
    npm_config_cache: '/worca/.worca-cc/worker-cache/npm', UV_CACHE_DIR: '/worca/.worca-cc/worker-cache/uv' };
  const exists = (p) => p === '/tmp/worca-ca.BBBB/bundle.pem' || p === '/etc/worca/ca.pem';
  const e = childEnv(sent, { home: '/worca/.worca-cc/worker-homes/run/r1', workerPid: 4242, user: 'worca', workerEnv, exists });
  assert.equal(e.SSL_CERT_FILE, '/tmp/worca-ca.BBBB/bundle.pem', "this container's bundle, not the controller's path");
  assert.equal(e.GIT_SSL_CAINFO, '/tmp/worca-ca.BBBB/bundle.pem');
  assert.equal(e.CURL_CA_BUNDLE, undefined, 'a CA path that exists nowhere here is dropped rather than break every TLS call');
  assert.equal(e.NODE_EXTRA_CA_CERTS, '/etc/worca/ca.pem', 'a CA mounted at the same path in both containers stays');
  assert.equal(e.npm_config_cache, '/worca/.worca-cc/worker-cache/npm', "the worker entrypoint's shared caches reach children");
  assert.equal(e.UV_CACHE_DIR, '/worca/.worca-cc/worker-cache/uv');
  assert.equal(e.HTTPS_PROXY, undefined, "the controller's loopback proxy is unreachable here");
  assert.equal(e.HTTP_PROXY, 'http://egress:3128');
  assert.equal(e.WORCA_WORKER_TOKEN, undefined);
  assert.equal(e.WORCA_CONTROLLER_URL, undefined);
  assert.equal(e.ANTHROPIC_API_KEY, 'k', 'credentials the controller composed ride through');
  assert.deepEqual([e.HOME, e.USER, e.WORCA_HOST_PID], ['/worca/.worca-cc/worker-homes/run/r1', 'worca', '4242']);
  // A path the backend passed into this container that does not exist here either is no better: not used.
  assert.equal(childEnv({ SSL_CERT_FILE: '/tmp/x/bundle.pem' }, { workerEnv: { SSL_CERT_FILE: '/tmp/x/bundle.pem' }, exists: () => false }).SSL_CERT_FILE, undefined);
});

import { createExecRunner, lineSplitter } from '../src/worker/exec.mjs';
import { encodeFrame, MAX_FRAME_BYTES } from '../src/core/executor/protocol.mjs';
import { tmpdir } from 'node:os';

test('lineSplitter: lines as readline gives them, with a bounded buffer for a stream that has no newline', () => {
  const got = [];
  const s = lineSplitter((l) => got.push(l), { maxPartial: 8 });
  s.write(Buffer.from('one\ntw')); s.write('o\r\n\nlast');
  s.end();
  assert.deepEqual(got, ['one', 'two', '', 'last']);
  got.length = 0;
  const t = lineSplitter((l) => got.push(l), { maxPartial: 8 });
  t.write('abcdefghijklmnopqrstu'); t.write('vwx\ny'); t.end();
  assert.deepEqual(got, ['abcdefgh', 'ijklmnop', 'qrstuvwx', 'y'], 'never more than maxPartial characters held');
  got.length = 0;
  const u = lineSplitter((l) => got.push(l));
  const euro = Buffer.from('€uro\n');
  u.write(euro.subarray(0, 2)); u.write(euro.subarray(2)); u.end();
  assert.deepEqual(got, ['€uro'], 'a multi-byte character split across chunks is kept whole');
});

test('a worker survives output no frame can hold: long lines travel alone or in pieces, nothing throws', { timeout: 60000 }, async () => {
  const frames = [];
  // The real sender: a frame that does not encode throws, exactly as on the socket.
  const runner = createExecRunner({ send: (f) => { encodeFrame(f); frames.push(f); }, home: tmpdir(), log: () => {} });
  const program = [
    "process.stdout.write('small\\n');",
    "process.stdout.write('a'.repeat(6 * 1024 * 1024) + '\\n');",            // fits one frame: an engine's large JSON event
    "process.stdout.write('\\u0001'.repeat(3 * 1024 * 1024) + '\\n');",     // 6 bytes of JSON per character: fits none whole
    "process.stdout.write('tail\\n');",
    "process.stdout.write('z'.repeat(9 * 1024 * 1024));",                      // no newline at all
  ].join('');
  const done = new Promise((resolve) => runner.onIdle(resolve));
  runner.start({ type: 'exec', id: 'x1', file: process.execPath, args: ['-e', program], cwd: tmpdir(), env: { PATH: process.env.PATH }, stdin: null, detached: false });
  // Ack as the controller would, so the send window never holds the child back.
  const acker = setInterval(() => { const last = frames.filter((f) => f.type === 'out').at(-1); if (last) runner.ack('x1', last.seq); }, 5);
  await done;
  clearInterval(acker);
  const exit = frames.find((f) => f.type === 'exit');
  assert.deepEqual([exit?.code, exit?.signal], [0, null], 'the child ran to its end; the worker did not crash');
  const outFrames = frames.filter((f) => f.type === 'out' && f.stream === 'out');
  for (const f of outFrames) assert.ok(Buffer.byteLength(JSON.stringify(f), 'utf8') <= MAX_FRAME_BYTES);
  const lines = outFrames.flatMap((f) => f.lines);
  assert.equal(lines[0], 'small');
  assert.equal(lines[1].length, 6 * 1024 * 1024, 'a long line that fits a frame arrives whole');
  const tailAt = lines.indexOf('tail');
  const control = lines.slice(2, tailAt);
  assert.ok(control.length > 1, 'a line no frame can hold is cut into pieces');
  assert.equal(control.join('').length, 3 * 1024 * 1024, 'and none of it is lost');
  assert.equal(lines.slice(tailAt + 1).join('').length, 9 * 1024 * 1024, 'the newline-free tail arrives complete, in bounded pieces');
  assert.deepEqual(outFrames.map((f) => f.seq), outFrames.map((_, i) => i), 'sequence numbers stay dense');
});
