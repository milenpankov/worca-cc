// src/worker/exec.mjs
// What a worker does with an exec frame: spawn the process in its own group, frame its
// stdout/stderr into batched `out` frames with a send window (the controller acks; past the
// window the pipes are paused so a chatty child cannot outrun the socket), relay signals to
// the group, and report the exit. Dependency-light: the worker runs in a container that has
// the worca package but no database and no server.
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { userInfo } from 'node:os';
import { existsSync } from 'node:fs';
import { OUT_WINDOW, OUT_BATCH_LINES, OUT_FLUSH_MS } from '../core/executor/protocol.mjs';

/** Env vars a worker never hands to a child: its own credentials, and a proxy that points at the
 *  controller's loopback (the in-process proxy is unreachable from another container). */
const WORKER_ONLY_ENV = ['WORCA_WORKER_TOKEN', 'WORCA_WORKER_TOKEN_FILE', 'WORCA_CONTROLLER_URL', 'WORCA_WORKER_SCOPE'];
const PROXY_VARS = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy'];

export function isLoopbackUrl(value) {
  try {
    const h = new URL(String(value)).hostname.replace(/^\[|\]$/g, '');
    return h === 'localhost' || h === '::1' || /^127\./.test(h) || h === '0.0.0.0';
  } catch { return false; }
}

/**
 * The env a child gets: the controller's composed env, with this worker's HOME (the scope's
 * home on the shared volume), its own pid as the host-guard pid, and nothing that only the
 * worker should hold.
 */
/** Variables whose value is a path INSIDE a container: the controller composed the env in its own container, where
 *  its entrypoint built a CA bundle under its /tmp and (on a worker) this one's entrypoint set shared cache dirs. */
const CA_FILE_VARS = ['SSL_CERT_FILE', 'GIT_SSL_CAINFO', 'CURL_CA_BUNDLE', 'REQUESTS_CA_BUNDLE', 'NODE_EXTRA_CA_CERTS'];
const WORKER_LOCAL_ENV = [...CA_FILE_VARS, 'npm_config_cache', 'UV_CACHE_DIR', 'PLAYWRIGHT_BROWSERS_PATH'];

export function childEnv(env, { home, workerPid = process.pid, user = null, workerEnv = process.env, exists = existsSync } = {}) {
  const out = { ...env };
  for (const k of WORKER_ONLY_ENV) delete out[k];
  for (const k of PROXY_VARS) if (out[k] && isLoopbackUrl(out[k])) delete out[k];
  // This container's own value wins (its entrypoint rebuilt the CA bundle and set the caches); a CA file path that
  // exists nowhere here — the controller's /tmp/worca-ca.*/bundle.pem — is dropped rather than break every TLS call.
  const usable = (k, v) => !!v && (!CA_FILE_VARS.includes(k) || exists(v));
  for (const k of WORKER_LOCAL_ENV) {
    if (usable(k, workerEnv?.[k])) out[k] = workerEnv[k];
    else if (out[k] !== undefined && !usable(k, out[k])) delete out[k];
  }
  if (home) out.HOME = home;
  if (user) { out.USER = user; out.LOGNAME = user; }
  out.WORCA_HOST_PID = String(workerPid);
  return out;
}

/** How long a killed child may keep its pipes before the worker stops waiting for 'close'. */
const KILL_GRACE_MS = 2000;

/**
 * @param {{ send: (frame: object) => void, home: string, log?: (msg: string) => void, platform?: string }} o
 */
export function createExecRunner({ send, home, log = () => {}, platform = process.platform }) {
  const live = new Map();   // id -> { child, pending, seq, lastAck, paused, flushTimer, grace }
  let user = null;
  try { user = userInfo().username || null; } catch { user = null; }
  const idleFns = new Set();

  const flush = (e, id) => {
    clearTimeout(e.flushTimer); e.flushTimer = null;
    for (const stream of ['out', 'err']) {
      const lines = e.pending[stream];
      if (!lines.length) continue;
      e.pending[stream] = [];
      send({ type: 'out', id, seq: e.seq++, stream, lines });
    }
    if (!e.paused && e.seq - e.lastAck > OUT_WINDOW) { e.paused = true; try { e.child.stdout.pause(); e.child.stderr.pause(); } catch { /* ended */ } }
  };
  const queueLine = (e, id, stream, line) => {
    e.pending[stream].push(line);
    if (e.pending[stream].length >= OUT_BATCH_LINES) { flush(e, id); return; }
    if (!e.flushTimer) e.flushTimer = setTimeout(() => flush(e, id), OUT_FLUSH_MS);
  };

  const runner = {
    get liveCount() { return live.size; },
    onIdle(fn) { idleFns.add(fn); },

    start(frame) {
      const { id } = frame;
      let child;
      try {
        child = spawn(frame.file, frame.args, {
          cwd: frame.cwd,
          env: childEnv(frame.env, { home, user }),
          stdio: [frame.stdin != null ? 'pipe' : 'ignore', 'pipe', 'pipe'],
          detached: frame.detached !== false && platform !== 'win32',
          windowsHide: true,
        });
      } catch (err) {
        send({ type: 'spawn-error', id, code: err.code ?? null, message: err.message });
        return;
      }
      const e = { child, pending: { out: [], err: [] }, seq: 0, lastAck: -1, paused: false, flushTimer: null, grace: null, started: false, reported: false };
      live.set(id, e);
      child.on('spawn', () => { e.started = true; send({ type: 'started', id, pid: child.pid }); });
      child.on('error', (err) => {
        if (e.reported) return;
        e.reported = true;
        live.delete(id);
        send({ type: 'spawn-error', id, code: err.code ?? null, message: err.message });
        if (!live.size) for (const fn of idleFns) fn();
      });
      for (const [stream, input] of [['out', child.stdout], ['err', child.stderr]]) {
        createInterface({ input }).on('line', (line) => queueLine(e, id, stream, line));
      }
      child.on('close', (code, signal) => {
        if (e.reported) return;
        e.reported = true;
        clearTimeout(e.grace);
        flush(e, id);
        live.delete(id);
        send({ type: 'exit', id, code, signal });
        if (!live.size) for (const fn of idleFns) fn();
      });
      if (frame.stdin != null) { child.stdin.on('error', () => {}); child.stdin.end(frame.stdin, 'utf8'); }
    },

    ack(id, seq) {
      const e = live.get(id);
      if (!e) return;
      e.lastAck = Math.max(e.lastAck, seq);
      if (e.paused && e.seq - e.lastAck <= OUT_WINDOW) { e.paused = false; try { e.child.stdout.resume(); e.child.stderr.resume(); } catch { /* ended */ } }
    },

    signal(id, sig) {
      const e = live.get(id);
      if (!e) return;
      runner.signalEntry(e, sig);
    },

    signalEntry(e, sig) {
      const { child } = e;
      if (typeof child.pid !== 'number') return;
      if (platform === 'win32') { try { child.kill(sig === 'SIGKILL' ? 'SIGKILL' : 'SIGTERM'); } catch { /* gone */ } }
      else { try { process.kill(-child.pid, sig); } catch { try { child.kill(sig); } catch { /* gone */ } } }
      if (sig === 'SIGKILL' && !e.grace) {
        // A descendant that escaped the group can hold the pipes open; release our ends so 'close' fires.
        e.grace = setTimeout(() => { try { child.stdout.destroy(); child.stderr.destroy(); } catch { /* closed */ } }, KILL_GRACE_MS);
        e.grace.unref?.();
      }
    },

    /** Stop everything: SIGTERM, wait up to graceMs, SIGKILL what is left. Resolves when all have closed. */
    async shutdown({ graceMs = 5000 } = {}) {
      if (!live.size) return;
      for (const e of live.values()) runner.signalEntry(e, 'SIGTERM');
      const deadline = Date.now() + graceMs;
      while (live.size && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
      for (const e of live.values()) runner.signalEntry(e, 'SIGKILL');
      const hard = Date.now() + KILL_GRACE_MS + 500;
      while (live.size && Date.now() < hard) await new Promise((r) => setTimeout(r, 50));
      if (live.size) log(`${live.size} process(es) did not exit after SIGKILL`);
    },

    killAll() { for (const e of live.values()) runner.signalEntry(e, 'SIGKILL'); },
  };
  return runner;
}
