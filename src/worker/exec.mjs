// src/worker/exec.mjs
// What a worker does with an exec frame: spawn the process in its own group, frame its
// stdout/stderr into batched `out` frames with a send window (the controller acks; past the
// window the pipes are paused so a chatty child cannot outrun the socket), relay signals to
// the group, and report the exit. Dependency-light: the worker runs in a container that has
// the worca package but no database and no server.
import { spawn } from 'node:child_process';
import { StringDecoder } from 'node:string_decoder';
import { userInfo } from 'node:os';
import { existsSync } from 'node:fs';
import { OUT_WINDOW, OUT_BATCH_LINES, OUT_FLUSH_MS, MAX_FRAME_BYTES } from '../core/executor/protocol.mjs';
import { isLoopbackUrl, PROXY_NAMES } from '../core/executor/shared.mjs';

/** Env vars a worker never hands to a child: its own credentials, and a proxy that points at the
 *  controller's loopback (the in-process proxy is unreachable from another container). */
const WORKER_ONLY_ENV = ['WORCA_WORKER_TOKEN', 'WORCA_WORKER_TOKEN_FILE', 'WORCA_CONTROLLER_URL', 'WORCA_WORKER_SCOPE'];
const PROXY_VARS = PROXY_NAMES;
export { isLoopbackUrl };

/**
 * The env a child gets: the controller's composed env, with this worker's HOME (the scope's
 * home on the shared volume), its own pid as the host-guard pid, and nothing that only the
 * worker should hold.
 */
/** Variables whose value is a path INSIDE a container: the controller composed the env in its own container, where
 *  its entrypoint built a CA bundle under its /tmp and (on a worker) this one's entrypoint set shared cache dirs. */
/**
 * Which of the worker's own variables an engine child may get: model-provider credentials and their settings, by
 * name, plus whatever WORCA_WORKER_PASS_ENV lists. Never worca's own (`WORCA_*`: the token, the controller URL) and
 * never a Git host token: pushes and pull requests are the controller's, and agents get no Git credential.
 */
const FROM_WORKER = [/_API_KEY$/, /_AUTH_TOKEN$/, /^ANTHROPIC_/, /^CLAUDE_CODE_/, /^OPENAI_/, /^GEMINI_/, /^GOOGLE_/, /^VERTEX/, /^AWS_/, /^AZURE_/, /^COPILOT_/, /^CURSOR_/, /^GOOSE_/, /^DASHSCOPE_/, /^OPENROUTER_/];
const NEVER_FROM_WORKER = [/^WORCA_/, /^(GH|GITHUB)_TOKEN$/, /^GH_ENTERPRISE_TOKEN$/];
export function passesFromWorker(name, extra = []) {
  if (NEVER_FROM_WORKER.some((re) => re.test(name))) return false;
  return extra.includes(name) || FROM_WORKER.some((re) => re.test(name));
}

const CA_FILE_VARS = ['SSL_CERT_FILE', 'GIT_SSL_CAINFO', 'CURL_CA_BUNDLE', 'REQUESTS_CA_BUNDLE', 'NODE_EXTRA_CA_CERTS'];
const WORKER_LOCAL_ENV = [...CA_FILE_VARS, 'npm_config_cache', 'UV_CACHE_DIR', 'PLAYWRIGHT_BROWSERS_PATH'];

export function childEnv(env, { home, workerPid = process.pid, user = null, workerEnv = process.env, exists = existsSync } = {}) {
  const out = { ...env };
  for (const k of WORKER_ONLY_ENV) delete out[k];
  for (const k of PROXY_VARS) if (out[k] && isLoopbackUrl(out[k])) delete out[k];
  // The worker's OWN engine credentials (worker.credentialsSecret on Kubernetes puts them in this container, not in
  // the controller) reach the child when the controller sent none of that name. Only names that look like one: the
  // worker's container baseline (PATH, service links, its assignment) is not the child's business.
  const extra = String(workerEnv?.WORCA_WORKER_PASS_ENV || '').split(',').map((s) => s.trim()).filter(Boolean);
  for (const [k, v] of Object.entries(workerEnv || {})) {
    if (out[k] !== undefined || v === undefined || v === '' || !passesFromWorker(k, extra)) continue;
    out[k] = v;
  }
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

// Output bounds. One frame holds at most MAX_FRAME_BYTES of JSON, and a frame that does not fit throws in the
// sender — from a stream or timer callback that would take the whole worker down, and the retry with it. So:
// small lines are batched up to BATCH_CHARS; a long line travels alone, whole when it fits a frame (a multi-megabyte
// JSON event of an engine does) and in pieces of SPLIT_CHARS when it cannot; a stream with no newline at all is cut
// at MAX_PARTIAL_CHARS so the buffer never holds more. Every piece but a line's last is marked `more`, and the
// controller joins them without a newline: the line arrives as the one line it was.
const BATCH_CHARS = 512 * 1024;
const BIG_LINE_CHARS = 256 * 1024;
const SPLIT_CHARS = 1024 * 1024;              // worst case 6 bytes of JSON per character: still one frame
const MAX_PARTIAL_CHARS = 8 * 1024 * 1024;
const FRAME_BUDGET = MAX_FRAME_BYTES - 64 * 1024;
const fitsAFrame = (line) => Buffer.byteLength(JSON.stringify(line), 'utf8') <= FRAME_BUDGET;

/**
 * Lines out of a byte stream, as readline gives them (`\n`, `\r\n` or a lone `\r` end a line; the last line with or
 * without one), but with a bounded buffer: a run of `maxPartial` characters without a line end is handed over as
 * `onLine(piece, true)` — "more of this line follows" — so the receiver can put the line back together.
 */
export function lineSplitter(onLine, { maxPartial = MAX_PARTIAL_CHARS } = {}) {
  const decoder = new StringDecoder('utf8');
  let partial = '';
  let heldCR = false;   // the chunk ended on `\r`: the line is over, but a `\n` opening the next chunk belongs to it
  const feed = (text) => {
    let start = 0;
    if (heldCR) { heldCR = false; if (text.charCodeAt(0) === 10) start = 1; }
    for (let i = start; i < text.length; i += 1) {
      const c = text.charCodeAt(i);
      if (c !== 10 && c !== 13) continue;
      onLine(partial + text.slice(start, i), false);
      partial = '';
      if (c === 13) { if (i + 1 === text.length) heldCR = true; else if (text.charCodeAt(i + 1) === 10) i += 1; }
      start = i + 1;
    }
    partial += text.slice(start);
    while (partial.length > maxPartial) { onLine(partial.slice(0, maxPartial), true); partial = partial.slice(maxPartial); }
  };
  return {
    write(chunk) { feed(typeof chunk === 'string' ? chunk : decoder.write(chunk)); },
    end() { const rest = partial + decoder.end(); partial = ''; heldCR = false; if (rest) onLine(rest, false); },
  };
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

  /** Send an output frame; never throw. What still cannot be sent is replaced by one line saying so. */
  const sendOut = (frame) => {
    try { send(frame); }
    catch (err) {
      log(`output frame dropped (${err?.message || err})`);
      try { send({ ...frame, lines: [`[worca worker: ${frame.lines.length} line(s) of output dropped — too large for one frame]`] }); } catch { /* the socket is gone: its close reports it */ }
    }
  };
  const pauseIfBehind = (e) => {
    if (!e.paused && e.seq - e.lastAck > OUT_WINDOW) { e.paused = true; try { e.child.stdout.pause(); e.child.stderr.pause(); } catch { /* ended */ } }
  };
  const flush = (e, id) => {
    clearTimeout(e.flushTimer); e.flushTimer = null;
    for (const stream of ['out', 'err']) {
      const lines = e.pending[stream];
      if (!lines.length) continue;
      e.pending[stream] = [];
      sendOut({ type: 'out', id, seq: e.seq++, stream, lines });
    }
    e.pendingChars = 0;
    pauseIfBehind(e);
  };
  /** `more`: this is not the end of the line (the splitter cut a newline-free stream); the rest follows. */
  const queueLine = (e, id, stream, line, more = false) => {
    if (more || line.length > BIG_LINE_CHARS) {
      flush(e, id);   // what was pending goes first: order within the stream is kept
      const pieces = fitsAFrame(line) ? [line] : Array.from({ length: Math.ceil(line.length / SPLIT_CHARS) }, (_, i) => line.slice(i * SPLIT_CHARS, (i + 1) * SPLIT_CHARS));
      pieces.forEach((piece, i) => sendOut({ type: 'out', id, seq: e.seq++, stream, lines: [piece], ...(more || i < pieces.length - 1 ? { more: true } : {}) }));
      pauseIfBehind(e);
      return;
    }
    if (e.pendingChars + line.length > BATCH_CHARS) flush(e, id);
    e.pending[stream].push(line);
    e.pendingChars += line.length;
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
          cwd: frame.cwd || undefined,   // none named: this worker's own (as a local spawn without cwd)
          env: childEnv(frame.env, { home, user }),
          stdio: [frame.stdin != null ? 'pipe' : 'ignore', 'pipe', 'pipe'],
          detached: frame.detached !== false && platform !== 'win32',
          windowsHide: true,
        });
      } catch (err) {
        send({ type: 'spawn-error', id, code: err.code ?? null, message: err.message });
        return;
      }
      const e = { child, pending: { out: [], err: [] }, pendingChars: 0, seq: 0, lastAck: -1, paused: false, flushTimer: null, grace: null, started: false, reported: false };
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
        const lines = lineSplitter((line, more) => queueLine(e, id, stream, line, more));
        input.on('data', (chunk) => lines.write(chunk));
        input.on('end', () => lines.end());
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
