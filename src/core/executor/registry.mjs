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
// A scope may have more than one worker: one per worker CLASS, which is the backend's answer to "what runs
// this engine" (its image). One image serving every engine is one worker; a run whose helper jobs use another
// engine than its agents gets a second one. release(scope) ends them all.
//
// A backend: { start({ scope, scopeKey, workerId, token, controllerUrl, env, idleMs, engine, signal }) -> handle,
//              remove(handle, { reason }), listWorkers() -> handle[], describe() -> object,
//              classFor?(engine) -> string (default: the engine name) }.
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
  const entries = new Map();   // `${scopeKey}|${class}` -> { scope, key, ek, engine, state:'starting'|'ready'|'closing', workerId, token, handle, session, waiters:[], startedAt, idleMs, timer, ctl, abandoned }
  const tokens = new Map();    // token -> { key, ek, workerId, mintedAt, used }
  const abortError = () => Object.assign(new Error('aborted'), { name: 'AbortError' });
  const scopeEntries = (key) => [...entries.values()].filter((e) => e.key === key);
  /** What tells a scope's workers apart: the backend's class for the engine (its image), else the engine name. */
  const classOf = async (engine) => {
    try { const c = await backend.classFor?.(engine); if (c) return String(c); } catch { /* the engine name will do */ }
    return String(engine || 'default');
  };
  let server = null;
  let wss = null;
  let boundPort = port;
  let sweeper = null;
  let closing = false;

  const mintToken = (key, ek, workerId) => {
    const token = randomBytes(32).toString('base64url');
    tokens.set(token, { key, ek, workerId, mintedAt: Date.now(), used: false });
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
    const entry = entries.get(t.ek);
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
      clearTimeout(entry.timer);
      session.onClose((reason) => {
        if (entries.get(t.ek) !== entry) return;
        entries.delete(t.ek);
        if (entry.state !== 'closing') {
          log(`worker ${entry.workerId} for ${t.key} is gone: ${reason}`);
          backend.remove(entry.handle, { reason: `lost: ${reason}` }).catch(() => {});
        }
      });
      const waiters = entry.waiters; entry.waiters = [];
      for (const w of waiters) w.resolve(session);
    });
  }

  /** A start that will not be used (it failed, timed out, or nobody waits for it any more): forget the entry
   *  and remove its container, now or when the backend hands it over. */
  function abandon(entry, reason) {
    entry.abandoned = true;
    clearTimeout(entry.timer);
    if (entries.get(entry.ek) === entry) entries.delete(entry.ek);
    if (entry.token) tokens.delete(entry.token);
    try { entry.ctl.abort(); } catch { /* nothing listening */ }
    if (entry.handle) backend.remove(entry.handle, { reason }).catch(() => {});
  }
  /** Every caller waiting on this start gets `err`; the start is abandoned. */
  function fail(entry, err) {
    const ws = entry.waiters; entry.waiters = [];
    abandon(entry, 'did not connect');
    for (const w of ws) w.reject(err);
  }

  /**
   * Begin starting a worker: the entry exists at once (so concurrent callers wait on it), the backend starts the
   * container in the background, and the hello on the listener resolves the waiters. A backend failure or a
   * worker that never connects rejects ALL of them. Nothing here depends on one caller's abort signal.
   */
  function begin(scope, key, ek, engine) {
    const workerId = `w-${scope.kind}-${randomBytes(4).toString('hex')}`;
    const entry = { scope, key, ek, engine, state: 'starting', workerId, token: null, handle: null, session: null, waiters: [], startedAt: Date.now(),
      idleMs: scope.kind === 'chat' ? chatIdleMs : null, timer: null, ctl: new AbortController(), abandoned: false };
    entries.set(ek, entry);
    entry.token = mintToken(key, ek, workerId);
    Promise.resolve()
      .then(() => backend.start({ scope, scopeKey: key, workerId, token: entry.token, controllerUrl: urlForWorkers(), idleMs: entry.idleMs, engine, env: {}, signal: entry.ctl.signal }))
      .then((handle) => {
        entry.handle = handle;
        if (entry.abandoned || entry.state === 'closing') { backend.remove(handle, { reason: 'start abandoned' }).catch(() => {}); return; }
        if (entry.state === 'starting') entry.timer = setTimeout(() => fail(entry, startFailure(`worker ${workerId} for ${key} did not connect within ${Math.round(startTimeoutMs / 1000)} s`)), startTimeoutMs);
      }, (err) => {
        if (entry.abandoned) return;
        fail(entry, startFailure(`cannot start a worker for ${key}: ${err?.message || err}`));
      });
    return entry;
  }

  /** Wait for a starting entry's session. The caller's abort ends its own wait at once (an AbortError); when it was
   *  the last one waiting, the start itself is abandoned. */
  function waitFor(entry, signal) {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) { reject(abortError()); if (entry.state === 'starting' && !entry.waiters.length) abandon(entry, 'aborted'); return; }
      const off = () => signal?.removeEventListener('abort', onAbort);
      const w = { resolve: (s) => { off(); resolve(s); }, reject: (e) => { off(); reject(e); } };
      function onAbort() {
        const i = entry.waiters.indexOf(w);
        if (i >= 0) entry.waiters.splice(i, 1);
        reject(abortError());
        if (entry.state === 'starting' && !entry.waiters.length) abandon(entry, 'aborted');
      }
      signal?.addEventListener('abort', onAbort, { once: true });
      entry.waiters.push(w);
    });
  }

  /** End one worker: shutdown frame, then the backend removes it. */
  async function releaseEntry(entry, { reason = 'released', graceMs = 5000 } = {}) {
    entry.state = 'closing';
    clearTimeout(entry.timer);
    if (!entry.session) { try { entry.ctl.abort(); } catch { /* nothing listening */ } }   // still starting: stop the backend's wait
    if (entries.get(entry.ek) === entry) entries.delete(entry.ek);
    if (entry.token) tokens.delete(entry.token);
    const ws = entry.waiters; entry.waiters = [];
    for (const w of ws) w.reject(startFailure(`worker for ${entry.key} released before it connected (${reason})`));
    if (entry.session && !entry.session.closed) await entry.session.shutdown({ graceMs });
    if (entry.handle) await backend.remove(entry.handle, { reason }).catch((err) => log(`remove ${entry.workerId}: ${err?.message || err}`));
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

    /**
     * The worker session a spawn of `engine` in this scope runs on: the live one, or a freshly started one.
     * `engine` (else the scope's own) picks the worker class; with neither, any worker the scope has will do.
     */
    async acquire(scope, { signal, engine = null } = {}) {
      const s = normalizeScope(scope);
      if (!s) throw new Error('acquire: a run or chat scope is required');
      const key = scopeKey(s);
      const want = (typeof engine === 'string' && engine.trim()) || s.engine || null;
      for (;;) {
        if (signal?.aborted) throw abortError();
        if (closing) throw startFailure('the server is shutting down');
        let entry = want ? null : (scopeEntries(key)[0] || null);
        const ek = entry ? entry.ek : `${key}|${await classOf(want)}`;
        entry ||= entries.get(ek) || null;
        if (entry?.state === 'ready') { entry.session.touch(); return entry.session; }
        if (entry?.state === 'starting') return waitFor(entry, signal);
        if (entries.size < maxWorkers) return waitFor(begin(s, key, ek, want), signal);
        await sleep(WAIT_POLL_MS, signal);
      }
    },

    /** End the scope's workers (no-op when there is none). */
    async release(scope, { reason = 'released', graceMs = 5000 } = {}) {
      const key = scopeKey(scope);
      const mine = key ? scopeEntries(key) : [];
      if (!mine.length) return false;
      await Promise.all(mine.map((entry) => releaseEntry(entry, { reason, graceMs })));
      return true;
    },

    touch(scope) { for (const e of scopeEntries(scopeKey(scope) ?? '')) e.session?.touch(); },

    /** Idle chat workers are released; stale tokens are dropped. */
    async sweep() {
      sweepTokens();
      const now = Date.now();
      for (const entry of [...entries.values()]) {
        if (entry.state !== 'ready' || entry.idleMs == null) continue;
        const s = entry.session;
        if (s.liveExecCount === 0 && now - s.lastActiveAt > entry.idleMs) await releaseEntry(entry, { reason: 'idle' });
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
      await Promise.all([...entries.values()].map((e) => releaseEntry(e, { reason })));
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
        scope: e.scope, engine: e.engine || null, workerId: e.workerId, state: e.state, since: e.startedAt,
        liveExecs: e.session?.liveExecCount ?? 0, engines: e.session ? Object.keys(e.session.capabilities?.engines || {}).filter((k) => e.session.capabilities.engines[k]) : [],
      }));
    },

    describe() { return { backend: backend.describe?.() ?? null, port: boundPort, workers: entries.size, maxWorkers }; },
  };
  return registry;
}
