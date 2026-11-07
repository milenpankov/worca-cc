// src/core/executor/registry.mjs
// The controller's workers: one listener on its own port (never the UI's — the UI port is
// published to people and guarded by host/identity checks; this one is reachable only on the
// container network and authenticated by a one-time token), a map of scope -> worker session,
// and the lifecycle around a backend that starts and removes worker containers.
//
//   acquire(scope)  -> the scope's ready session, or: mint a token, backend.start(), wait for
//                      the worker's hello on this listener, then the session
//   release(scope)  -> shutdown frame, wait for bye/close, backend.remove()
//   reapAtBoot()    -> every worker the backend still knows is a previous controller's: removed
//                      (its token died with that process; a single-replica controller owns all)
//
// A backend: { start({ scope, scopeKey, workerId, token, controllerUrl, env, idleMs }) -> handle,
//              remove(handle, { reason }), listWorkers() -> handle[], describe() -> object }.
import http from 'node:http';
import { randomBytes, randomUUID } from 'node:crypto';
import { WebSocketServer } from 'ws';
import { decodeFrame, encodeFrame, PROTOCOL_VERSION } from './protocol.mjs';
import { createWorkerSession } from './session.mjs';
import { normalizeScope, scopeKey } from './scope.mjs';

export const DEFAULT_WORKER_PORT = 4319;
export const WORKER_WS_PATH = '/worker';
const HELLO_TIMEOUT_MS = 10_000;
const SWEEP_MS = 30_000;
const WAIT_POLL_MS = 2_000;

const envInt = (name, def) => { const n = Number(process.env[name]); return Number.isFinite(n) && n > 0 ? n : def; };

/** A refusal the acquire path throws: the harness's failure policy treats it like a lost network. */
function startFailure(message) {
  return Object.assign(new Error(message), { code: 'EWORKERSTART', errorClass: 'network' });
}

const sleep = (ms, signal) => new Promise((resolve) => {
  const t = setTimeout(done, ms);
  function done() { clearTimeout(t); signal?.removeEventListener('abort', done); resolve(); }
  signal?.addEventListener('abort', done, { once: true });
});

export function createWorkerRegistry({
  backend,
  host = process.env.WORCA_WORKER_HOST || '0.0.0.0',
  port = envInt('WORCA_WORKER_PORT', DEFAULT_WORKER_PORT),
  controllerUrl = process.env.WORCA_CONTROLLER_URL || null,
  maxWorkers = envInt('WORCA_WORKER_MAX', 20),
  startTimeoutMs = envInt('WORCA_WORKER_START_TIMEOUT_MS', 120_000),
  chatIdleMs = envInt('WORCA_WORKER_CHAT_IDLE_MS', 15 * 60_000),
  tokenTtlMs = 120_000,
  pingMs = undefined,
  bootId = `ctl-${randomUUID()}`,
  log = (msg) => console.log(`[worca-workers] ${msg}`),
  onRelay = null,
} = {}) {
  if (!backend) throw new Error('createWorkerRegistry: a backend is required');
  const entries = new Map();   // scopeKey -> { scope, key, state:'starting'|'ready'|'closing', workerId, token, handle, session, waiters:[], startedAt, idleMs }
  const tokens = new Map();    // token -> { key, workerId, mintedAt, used }
  let server = null;
  let wss = null;
  let boundPort = port;
  let sweeper = null;
  let closing = false;

  const mintToken = (key, workerId) => {
    const token = randomBytes(32).toString('base64url');
    tokens.set(token, { key, workerId, mintedAt: Date.now(), used: false });
    return token;
  };
  const tokenOf = (req) => {
    const m = /^Bearer\s+(\S+)$/i.exec(String(req.headers.authorization || ''));
    return m ? m[1] : null;
  };
  const usableToken = (token) => {
    const t = tokens.get(token);
    if (!t || t.used) return null;
    if (Date.now() - t.mintedAt > tokenTtlMs) { tokens.delete(token); return null; }
    return t;
  };
  const sweepTokens = () => { for (const [k, t] of tokens) if (t.used || Date.now() - t.mintedAt > tokenTtlMs) tokens.delete(k); };

  /** The URL a worker is told to connect to: the operator's (a service name), else this listener. */
  const urlForWorkers = () => controllerUrl || `ws://${host === '0.0.0.0' || host === '::' ? '127.0.0.1' : host}:${boundPort}`;

  function admit(ws, req) {
    const token = tokenOf(req);
    const t = token ? usableToken(token) : null;
    if (!t) { ws.close(4001, 'unknown token'); return; }
    const entry = entries.get(t.key);
    if (!entry || entry.state !== 'starting') { ws.close(4001, 'no worker expected'); return; }
    const timer = setTimeout(() => { ws.close(4003, 'no hello'); }, HELLO_TIMEOUT_MS);
    ws.once('message', (data) => {
      clearTimeout(timer);
      const hello = decodeFrame(data);
      if (!hello || hello.type !== 'hello') { ws.close(4002, 'hello expected'); return; }
      if (hello.apiVersion !== PROTOCOL_VERSION) { ws.send(encodeFrame({ type: 'refuse', reason: `protocol ${hello.apiVersion} is not ${PROTOCOL_VERSION}` })); ws.close(4004, 'protocol'); return; }
      if (scopeKey(hello.scope) !== t.key || hello.workerId !== t.workerId) { ws.send(encodeFrame({ type: 'refuse', reason: 'scope or worker id does not match the token' })); ws.close(4001, 'scope mismatch'); return; }
      t.used = true;
      tokens.delete(token);
      const session = createWorkerSession(ws, { workerId: hello.workerId, scope: hello.scope, capabilities: hello.capabilities, pid: hello.pid, pingMs, log, onRelay });
      ws.send(encodeFrame({ type: 'welcome', controllerBootId: bootId, pingMs: pingMs ?? 30_000, idleMs: entry.idleMs ?? null }));
      entry.session = session;
      entry.state = 'ready';
      session.onClose((reason) => {
        if (entries.get(t.key) !== entry) return;
        entries.delete(t.key);
        if (entry.state !== 'closing') {
          log(`worker ${entry.workerId} for ${t.key} is gone: ${reason}`);
          backend.remove(entry.handle, { reason: `lost: ${reason}` }).catch(() => {});
        }
      });
      const waiters = entry.waiters; entry.waiters = [];
      for (const w of waiters) w.resolve(session);
    });
  }

  async function start(scope, key, { signal }) {
    const workerId = `w-${scope.kind}-${randomBytes(4).toString('hex')}`;
    const entry = { scope, key, state: 'starting', workerId, token: null, handle: null, session: null, waiters: [], startedAt: Date.now(), idleMs: scope.kind === 'chat' ? chatIdleMs : null };
    entries.set(key, entry);
    const ready = new Promise((resolve, reject) => entry.waiters.push({ resolve, reject }));
    entry.token = mintToken(key, workerId);
    try {
      entry.handle = await backend.start({ scope, scopeKey: key, workerId, token: entry.token, controllerUrl: urlForWorkers(), idleMs: entry.idleMs, env: {} });
    } catch (err) {
      entries.delete(key); tokens.delete(entry.token);
      throw startFailure(`cannot start a worker for ${key}: ${err?.message || err}`);
    }
    let timer = null;
    const onAbort = () => fail(new Error('aborted'));
    const fail = (err) => { const ws = entry.waiters; entry.waiters = []; for (const w of ws) w.reject(err); };
    timer = setTimeout(() => fail(startFailure(`worker ${workerId} for ${key} did not connect within ${Math.round(startTimeoutMs / 1000)} s`)), startTimeoutMs);
    signal?.addEventListener('abort', onAbort, { once: true });
    try {
      return await ready;
    } catch (err) {
      if (entries.get(key) === entry) entries.delete(key);
      tokens.delete(entry.token);
      backend.remove(entry.handle, { reason: 'did not connect' }).catch(() => {});
      throw err;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    }
  }

  const registry = {
    get port() { return boundPort; },
    get bootId() { return bootId; },
    get controllerUrl() { return urlForWorkers(); },

    async listen() {
      server = http.createServer((_req, res) => { res.statusCode = 404; res.end(); });
      wss = new WebSocketServer({
        server, path: WORKER_WS_PATH, maxPayload: 8 * 1024 * 1024 + 1024,
        verifyClient: (info, done) => { const token = tokenOf(info.req); done(!!(token && usableToken(token)), 401); },
      });
      wss.on('error', () => {});
      wss.on('connection', admit);
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => { server.off('error', reject); boundPort = server.address().port; resolve(); });
      });
      sweeper = setInterval(() => registry.sweep(), SWEEP_MS);
      sweeper.unref?.();
      return boundPort;
    },

    /** The scope's worker session: the live one, or a freshly started one. */
    async acquire(scope, { signal } = {}) {
      const s = normalizeScope(scope);
      if (!s) throw new Error('acquire: a run or chat scope is required');
      if (closing) throw startFailure('the server is shutting down');
      const key = scopeKey(s);
      for (;;) {
        const entry = entries.get(key);
        if (entry?.state === 'ready') { entry.session.touch(); return entry.session; }
        if (entry?.state === 'starting') return new Promise((resolve, reject) => entry.waiters.push({ resolve, reject }));
        if (entries.size < maxWorkers) return start(s, key, { signal });
        if (signal?.aborted) throw Object.assign(new Error('aborted'), { name: 'AbortError' });
        await sleep(WAIT_POLL_MS, signal);
      }
    },

    /** End the scope's worker (no-op when there is none). */
    async release(scope, { reason = 'released', graceMs = 5000 } = {}) {
      const key = scopeKey(scope);
      const entry = key ? entries.get(key) : null;
      if (!entry) return false;
      entry.state = 'closing';
      entries.delete(key);
      if (entry.token) tokens.delete(entry.token);
      const ws = entry.waiters; entry.waiters = [];
      for (const w of ws) w.reject(startFailure(`worker for ${key} released before it connected (${reason})`));
      if (entry.session && !entry.session.closed) await entry.session.shutdown({ graceMs });
      if (entry.handle) await backend.remove(entry.handle, { reason }).catch((err) => log(`remove ${entry.workerId}: ${err?.message || err}`));
      return true;
    },

    touch(scope) { entries.get(scopeKey(scope) ?? '')?.session?.touch(); },

    /** Idle chat workers are released; stale tokens are dropped. */
    async sweep() {
      sweepTokens();
      const now = Date.now();
      for (const entry of [...entries.values()]) {
        if (entry.state !== 'ready' || entry.idleMs == null) continue;
        const s = entry.session;
        if (s.liveExecCount === 0 && now - s.lastActiveAt > entry.idleMs) await registry.release(entry.scope, { reason: 'idle' });
      }
    },

    /** Remove every worker the backend still knows about: with one controller, all are orphans. */
    async reapAtBoot() {
      let handles = [];
      try { handles = await backend.listWorkers(); } catch (err) { log(`cannot list workers: ${err?.message || err}`); return { removed: 0 }; }
      let removed = 0;
      for (const h of handles) { try { await backend.remove(h, { reason: 'orphan at boot' }); removed += 1; } catch (err) { log(`cannot remove ${h?.workerId || h?.id || 'a worker'}: ${err?.message || err}`); } }
      if (removed) log(`removed ${removed} orphan worker(s) from a previous controller`);
      return { removed };
    },

    async closeAll(reason = 'shutdown') {
      closing = true;
      clearInterval(sweeper);
      await Promise.all([...entries.values()].map((e) => registry.release(e.scope, { reason })));
      await new Promise((resolve) => {
        if (!server) return resolve();
        wss?.clients.forEach((c) => { try { c.terminate(); } catch { /* gone */ } });
        wss?.close();
        server.close(() => resolve());
        server.closeAllConnections?.();
      });
      server = null; wss = null;
    },

    snapshot() {
      return [...entries.values()].map((e) => ({
        scope: e.scope, workerId: e.workerId, state: e.state, since: e.startedAt,
        liveExecs: e.session?.liveExecCount ?? 0, engines: e.session ? Object.keys(e.session.capabilities?.engines || {}).filter((k) => e.session.capabilities.engines[k]) : [],
      }));
    },

    describe() { return { backend: backend.describe?.() ?? null, port: boundPort, workers: entries.size, maxWorkers }; },
  };
  return registry;
}
