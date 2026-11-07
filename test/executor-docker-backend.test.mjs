// test/executor-docker-backend.test.mjs — the docker executor backend against a fake Docker Engine
// API on a unix socket: the controller's own container is the template for every worker (mounts
// minus the socket, the login volume as a read-only seed, network, user), the container spec, the
// labels the boot reap lists by, fast failure when a container dies at once, and removal.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  createDockerBackend, createDockerApi, dockerEndpoint, parseBytes, workerMountsFrom, workerImageFor, workerEnvFor, demuxLogs,
  CONTROLLER_LABEL, SCOPE_LABEL, WORKER_LABEL,
} from '../src/core/executor/backends/docker.mjs';
import { seedClaudeLogin } from '../src/worker/main.mjs';

const SELF = 'abc123';
const selfMounts = [
  { Type: 'volume', Name: 'worca_worca-home', Source: '/var/lib/docker/volumes/worca_worca-home/_data', Destination: '/worca', RW: true },
  { Type: 'volume', Name: 'worca_claude-config', Source: '/var/lib/docker/volumes/worca_claude-config/_data', Destination: '/home/worca/.claude', RW: true },
  { Type: 'bind', Source: '/Users/me/dev', Destination: '/Users/me/dev', RW: true },
  { Type: 'bind', Source: '/etc/ssl/corp.pem', Destination: '/etc/worca/ca.pem', RW: false },
  { Type: 'bind', Source: '/var/run/docker.sock', Destination: '/var/run/docker.sock', RW: false },
];

/** A fake Engine on a unix socket. `dieAtOnce` makes started containers exit immediately. */
function fakeDocker({ dieAtOnce = false, imageMissing = false } = {}) {
  const dir = mkdtempSync(join('/tmp', 'wd-'));
  const socketPath = join(dir, 's.sock');
  const containers = new Map();   // id -> { spec, name, running, labels }
  let n = 0;
  const calls = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : null;
      const url = new URL(req.url, 'http://docker');
      const path = url.pathname.replace(/^\/v[\d.]+/, '');
      calls.push([req.method, path]);
      const json = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(obj === undefined ? '' : JSON.stringify(obj)); };
      if (req.method === 'GET' && path === '/version') return json(200, { Version: '27.0.0' });
      if (req.method === 'GET' && path === `/containers/${SELF}/json`) {
        return json(200, { Id: SELF, Name: '/worca-worca-1', Mounts: selfMounts, Config: { Image: 'ghcr.io/sinishadjukic/worca:1.20.0', User: '1000:1000' }, NetworkSettings: { Networks: { worca_default: {}, worca_docker_proxy: {} } } });
      }
      if (req.method === 'POST' && path === '/containers/create') {
        if (imageMissing) return json(404, { message: `No such image: ${body.Image}` });
        const id = `c${++n}`.padEnd(12, '0');
        containers.set(id, { spec: body, name: url.searchParams.get('name'), running: false, exited: false });
        return json(201, { Id: id });
      }
      const m = /^\/containers\/([^/]+)(?:\/(\w+))?$/.exec(path);
      if (m && containers.has(m[1])) {
        const c = containers.get(m[1]);
        if (req.method === 'POST' && m[2] === 'start') { if (dieAtOnce) c.exited = true; else c.running = true; return json(204); }
        if (req.method === 'GET' && m[2] === 'json') return json(200, { Id: m[1], State: { Running: c.running, Status: c.exited ? 'exited' : c.running ? 'running' : 'created', ExitCode: c.exited ? 78 : 0 } });
        if (req.method === 'GET' && m[2] === 'logs') { const line = Buffer.from('worca worker: WORCA_CONTROLLER_URL is not set\n'); const head = Buffer.alloc(8); head[0] = 2; head.writeUInt32BE(line.length, 4); res.writeHead(200); return res.end(Buffer.concat([head, line])); }
        if (req.method === 'DELETE' && !m[2]) { containers.delete(m[1]); return json(204); }
      }
      if (req.method === 'GET' && path === '/containers/json') {
        const want = JSON.parse(url.searchParams.get('filters')).label;
        const rows = [...containers.entries()].filter(([, c]) => want.every((l) => c.spec.Labels && l in c.spec.Labels)).map(([id, c]) => ({ Id: id, Names: [`/${c.name}`], Image: c.spec.Image, Labels: c.spec.Labels }));
        return json(200, rows);
      }
      json(404, { message: `no such route ${req.method} ${path}` });
    });
  });
  return new Promise((resolve) => server.listen(socketPath, () => resolve({
    socketPath, containers, calls,
    close: () => new Promise((r) => { server.close(() => { rmSync(dir, { recursive: true, force: true }); r(); }); server.closeAllConnections?.(); }),
  })));
}

const env = (socketPath, extra = {}) => ({ WORCA_DOCKER_HOST: `unix://${socketPath}`, WORCA_CONTROLLER_CONTAINER: SELF, TZ: 'Europe/Sofia', WORCA_BROKER_URL: 'http://broker:8080', HTTPS_PROXY: 'http://127.0.0.1:3128', GH_TOKEN: 'ghp_secret', ANTHROPIC_API_KEY: 'sk-secret', ...extra });

test('pure helpers: bytes, endpoints, mounts, images, env', () => {
  assert.equal(parseBytes('4g'), 4 * 1024 ** 3);
  assert.equal(parseBytes('512m'), 512 * 1024 ** 2);
  assert.equal(parseBytes('1024'), 1024);
  assert.equal(parseBytes('lots'), null);
  assert.deepEqual(dockerEndpoint({}), { socketPath: '/var/run/docker.sock' });
  assert.deepEqual(dockerEndpoint({ WORCA_DOCKER_HOST: 'tcp://docker-proxy:2375' }), { host: 'docker-proxy', port: 2375 });
  assert.deepEqual(dockerEndpoint({ WORCA_DOCKER_HOST: 'unix:///run/podman/podman.sock' }), { socketPath: '/run/podman/podman.sock' });
  assert.throws(() => dockerEndpoint({ WORCA_DOCKER_HOST: 'ssh://x' }), /expected tcp:\/\/host:port/);
  assert.deepEqual(workerMountsFrom(selfMounts), [
    { Type: 'volume', Source: 'worca_worca-home', Target: '/worca', ReadOnly: false },
    { Type: 'volume', Source: 'worca_claude-config', Target: '/seed/claude', ReadOnly: true },
    { Type: 'bind', Source: '/Users/me/dev', Target: '/Users/me/dev', ReadOnly: false },
    { Type: 'bind', Source: '/etc/ssl/corp.pem', Target: '/etc/worca/ca.pem', ReadOnly: true },
  ], 'the socket never reaches a worker; the login volume rides read-only as the seed');
  assert.equal(workerImageFor('codex', { pattern: null, controllerImage: 'ghcr.io/sinishadjukic/worca:1.20.0' }), 'ghcr.io/sinishadjukic/worca-worker:1.20.0-codex');
  assert.equal(workerImageFor('codex', { pattern: null, controllerImage: 'registry:5000/worca' }), 'registry:5000/worca-worker:latest-codex');
  assert.equal(workerImageFor('codex', { pattern: 'my/worker:{engine}-x', controllerImage: 'a:b' }), 'my/worker:codex-x');
  assert.equal(workerImageFor('codex', { pattern: 'self', controllerImage: 'a:b' }), 'a:b');
  const e = workerEnvFor({ scopeKey: 'run:r1', workerId: 'w1', token: 't', controllerUrl: 'ws://worca:4319', idleMs: null, engine: 'claude', env: env('/x') });
  assert.equal(e.WORCA_WORKER_SCOPE, 'run:r1');
  assert.equal(e.WORCA_NO_DB, '1');
  assert.equal(e.TZ, 'Europe/Sofia');
  assert.equal(e.WORCA_BROKER_URL, 'http://broker:8080');
  assert.equal(e.HTTPS_PROXY, undefined, 'a loopback proxy is dropped');
  assert.equal(e.GH_TOKEN, undefined);
  assert.equal(e.ANTHROPIC_API_KEY, undefined, 'credentials never ride the container env');
  assert.equal(e.WORCA_WORKER_SEED_CLAUDE, '/seed/claude');
  assert.equal(demuxLogs(Buffer.from('plain')), 'plain');
});

test('the backend replicates the controller container and runs a worker on the same network with the right spec', async () => {
  const d = await fakeDocker();
  try {
    const logs = [];
    const b = await createDockerBackend({ env: env(d.socketPath, { WORCA_WORKER_MEM: '2g', WORCA_WORKER_CPUS: '1.5' }), log: (l) => logs.push(l) });
    assert.equal(b.kind, 'docker');
    assert.equal(b.template.network, 'worca_default');
    assert.equal(b.template.user, '1000:1000');
    assert.match(logs[0], /docker 27\.0\.0 .* 4 mount\(s\) replicated/);
    const h = await b.start({ scope: { kind: 'run', id: 'r-1' }, scopeKey: 'run:r-1', workerId: 'w-run-abcd1234', token: 'tok', controllerUrl: 'ws://worca:4319', idleMs: null });
    assert.match(h.name, /^worca-worker-run-r-1-abcd1234$/);
    assert.equal(h.image, 'ghcr.io/sinishadjukic/worca-worker:1.20.0-claude');
    const [, c] = [...d.containers.entries()][0];
    assert.deepEqual(c.spec.Cmd, ['worca', 'worker']);
    assert.equal(c.spec.User, '1000:1000');
    assert.equal(c.spec.HostConfig.NetworkMode, 'worca_default');
    assert.deepEqual(c.spec.HostConfig.CapDrop, ['ALL']);
    assert.deepEqual(c.spec.HostConfig.RestartPolicy, { Name: 'no' });
    assert.equal(c.spec.HostConfig.Memory, 2 * 1024 ** 3);
    assert.equal(c.spec.HostConfig.NanoCpus, 1.5e9);
    assert.equal(c.spec.HostConfig.Init, true);
    assert.deepEqual(c.spec.HostConfig.Mounts.map((m) => m.Target), ['/worca', '/seed/claude', '/Users/me/dev', '/etc/worca/ca.pem']);
    assert.equal(c.spec.Labels[SCOPE_LABEL], 'run:r-1');
    assert.equal(c.spec.Labels[WORKER_LABEL], 'w-run-abcd1234');
    assert.ok(c.spec.Labels[CONTROLLER_LABEL]);
    assert.ok(c.spec.Env.includes('WORCA_WORKER_TOKEN=tok'));
    assert.ok(c.spec.Env.includes('WORCA_CONTROLLER_URL=ws://worca:4319'));
    assert.ok(!c.spec.Env.some((e) => e.startsWith('GH_TOKEN=')));
    const listed = await b.listWorkers();
    assert.deepEqual(listed.map((w) => [w.workerId, w.scopeKey]), [['w-run-abcd1234', 'run:r-1']]);
    await b.remove(h, { reason: 'done' });
    assert.equal(d.containers.size, 0);
    assert.deepEqual(await b.listWorkers(), []);
    assert.equal(b.describe().image, 'ghcr.io/sinishadjukic/worca-worker:1.20.0-<engine>');
  } finally { await d.close(); }
});

test('a worker container that exits at once is reported with its log tail and removed; a missing image names itself', async () => {
  const d = await fakeDocker({ dieAtOnce: true });
  try {
    const b = await createDockerBackend({ env: env(d.socketPath), log: () => {} });
    const err = await b.start({ scope: { kind: 'chat', id: 't' }, scopeKey: 'chat:t', workerId: 'w-chat-1', token: 't', controllerUrl: 'ws://w:1', idleMs: 1000 }).then(() => null, (e) => e);
    assert.match(err.message, /exited at once \(code 78\)/);
    assert.match(err.message, /WORCA_CONTROLLER_URL is not set/, 'the log tail rides the error');
    assert.equal(d.containers.size, 0, 'removed');
  } finally { await d.close(); }
  const d2 = await fakeDocker({ imageMissing: true });
  try {
    const b = await createDockerBackend({ env: env(d2.socketPath, { WORCA_WORKER_IMAGE: 'nope/worker:{engine}' }), log: () => {} });
    const err = await b.start({ scope: { kind: 'run', id: 'r' }, scopeKey: 'run:r', workerId: 'w', token: 't', controllerUrl: 'ws://w:1', idleMs: null }).then(() => null, (e) => e);
    assert.match(err.message, /worker image nope\/worker:claude is not present on this host/);
  } finally { await d2.close(); }
});

test('an unreachable engine or an uninspectable self are refused with a hint', async () => {
  await assert.rejects(createDockerBackend({ env: { WORCA_DOCKER_HOST: 'unix:///nonexistent/docker.sock' } }), /cannot reach the Docker Engine .* Mount the socket or set WORCA_DOCKER_HOST/);
  const d = await fakeDocker();
  try {
    await assert.rejects(createDockerBackend({ env: env(d.socketPath, { WORCA_CONTROLLER_CONTAINER: 'ghost' }) }), /cannot inspect the controller's own container "ghost"/);
  } finally { await d.close(); }
});

test('the worker seeds a Claude login from the read-only seed dir once, never sessions', () => {
  const root = mkdtempSync(join(tmpdir(), 'worca-seed-'));
  after(() => rmSync(root, { recursive: true, force: true }));
  const seed = join(root, 'seed'); const home = join(root, 'home');
  mkdirSync(join(seed, 'projects'), { recursive: true }); mkdirSync(home, { recursive: true });
  writeFileSync(join(seed, '.credentials.json'), '{"tok":1}'); writeFileSync(join(seed, 'settings.json'), '{}'); writeFileSync(join(seed, 'projects', 'x.jsonl'), '');
  assert.deepEqual(seedClaudeLogin(home, seed), ['.credentials.json', 'settings.json']);
  assert.equal(readFileSync(join(home, '.claude', '.credentials.json'), 'utf8'), '{"tok":1}');
  assert.equal(existsSync(join(home, '.claude', 'projects')), false);
  assert.deepEqual(seedClaudeLogin(home, seed), [], 'already there: nothing copied again');
  assert.deepEqual(seedClaudeLogin(home, ''), []);
});

test('createDockerApi talks to the engine over the socket', async () => {
  const d = await fakeDocker();
  try {
    const api = createDockerApi(dockerEndpoint({ WORCA_DOCKER_HOST: `unix://${d.socketPath}` }));
    assert.equal((await api.version()).Version, '27.0.0');
    await assert.rejects(api.inspect('ghost'), /404/);
  } finally { await d.close(); }
});
