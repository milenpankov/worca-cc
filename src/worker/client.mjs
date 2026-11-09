// src/worker/client.mjs
// The worker's side of the socket: connect outbound to the controller with the one-time
// token, announce the scope and what is installed, then serve exec frames until the
// controller says shutdown, the scope idles out (chat workers), or the socket drops — in
// which case every child is killed and the process exits non-zero: a worker never outlives
// its controller connection (there is no reconnect; the controller starts a fresh worker).
import { WebSocket } from 'ws';
import { encodeFrame, decodeFrame, PROTOCOL_VERSION, WORKER_WS_PATH, WS_MAX_PAYLOAD } from '../core/executor/protocol.mjs';
import { createExecRunner } from './exec.mjs';

export { WORKER_WS_PATH };
const DEFAULT_PING_MS = 30_000;
const CONNECT_TIMEOUT_MS = 15_000;

/**
 * Start a worker. Resolves `{ done, close }`: `done` settles with the exit code once the worker
 * has finished (0 after a shutdown, a refusal or an idle timeout it chose itself; 1 when the
 * socket dropped under it); `close()` ends it early (a test, or a signal).
 * @param {{ url: string, token: string, scope: {kind:string,id:string}, workerId: string, home: string,
 *   capabilities: object, idleMs?: number|null, pingMs?: number, log?: (msg:string) => void,
 *   onRelayResult?: (frame: object) => void }} o
 */
export function startWorker({ url, token, scope, workerId, home, capabilities, idleMs = null, pingMs = DEFAULT_PING_MS, lostGraceMs = 5000, log = () => {} }) {
  const base = String(url).replace(/\/+$/, '');
  const target = base.endsWith(WORKER_WS_PATH) ? base : base + WORKER_WS_PATH;
  let resolveDone;
  const done = new Promise((r) => { resolveDone = r; });
  let exitCode = null;
  let finished = false;
  let idleTimer = null;
  let pingTimer = null;
  let missed = 0;
  let serverPingMs = pingMs;
  let shuttingDown = false;
  let connected = false;        // the socket opened at least once
  let refusedStatus = null;     // the controller answered the upgrade with an HTTP status (a dead token: 401)
  let endedWith = null;

  const ws = new WebSocket(target, { headers: { authorization: `Bearer ${token}` }, handshakeTimeout: CONNECT_TIMEOUT_MS, maxPayload: WS_MAX_PAYLOAD });
  const send = (frame) => { if (ws.readyState === WebSocket.OPEN) ws.send(encodeFrame(frame)); };
  const runner = createExecRunner({ send, home, log });
  // Relay (relay-shim.mjs): a chat's worca tools run on the controller; each JSON-RPC line goes up as a
  // `relay` frame and its answer comes back as `relay-result`.
  const relays = new Map();   // id -> { resolve }
  let relaySeq = 0;
  const relay = (relayToken, line) => new Promise((resolve) => {
    const id = `r${++relaySeq}`;
    if (ws.readyState !== WebSocket.OPEN) return resolve({ lines: null, error: 'worker is not connected' });
    relays.set(id, { resolve });
    send({ type: 'relay', id, token: relayToken, line });
  });

  const finish = (code, why) => {
    if (finished) return;
    finished = true;
    exitCode = code;
    endedWith = why;
    clearTimeout(idleTimer); clearInterval(pingTimer);
    for (const [id, r] of relays) { relays.delete(id); r.resolve({ lines: null, error: `worker done: ${why}` }); }
    try { ws.terminate(); } catch { /* gone */ }
    // Whatever still runs (the controller went away mid-step) is stopped as a stop would: SIGTERM, a grace, then
    // SIGKILL. An engine killed inside a write strands temp files in a HOME that is kept for the run's resume.
    Promise.resolve(runner.liveCount ? runner.shutdown({ graceMs: lostGraceMs }) : null).catch(() => {}).then(() => {
      runner.killAll();
      log(`worker ${workerId} done: ${why}`);
      resolveDone(code);
    });
  };

  // The controller releases an idle worker (its sweep, with a shutdown frame); this timer is the backstop for a
  // controller that never does, so it fires well after the controller's own moment and never races it.
  const IDLE_BACKSTOP = 2;
  const armIdle = () => {
    clearTimeout(idleTimer);
    if (idleMs == null || runner.liveCount > 0) return;
    idleTimer = setTimeout(async () => {
      if (runner.liveCount > 0) { armIdle(); return; }
      shuttingDown = true;
      send({ type: 'bye' });
      try { ws.close(1000, 'idle'); } catch { /* gone */ }
      finish(0, `idle for ${idleMs * IDLE_BACKSTOP} ms`);
    }, idleMs * IDLE_BACKSTOP);
    idleTimer.unref?.();
  };
  runner.onIdle(armIdle);

  const leave = async (graceMs) => {
    if (shuttingDown) return;
    shuttingDown = true;
    await runner.shutdown({ graceMs });
    send({ type: 'bye' });
    try { ws.close(1000, 'shutdown'); } catch { /* gone */ }
    finish(0, 'shutdown');
  };

  ws.on('open', () => {
    connected = true;
    send({ type: 'hello', apiVersion: PROTOCOL_VERSION, workerId, scope, pid: process.pid, capabilities });
  });
  ws.on('message', (data) => {
    const frame = decodeFrame(data);
    if (!frame) { log('protocol violation from the controller: leaving'); finish(1, 'protocol violation'); return; }
    switch (frame.type) {
      case 'welcome':
        serverPingMs = frame.pingMs || pingMs;
        if (frame.idleMs != null) idleMs = frame.idleMs;
        clearInterval(pingTimer);
        pingTimer = setInterval(() => {
          if (missed >= 2) { finish(1, 'controller stopped answering pings'); return; }
          missed += 1;
          send({ type: 'ping', id: `w${Date.now()}` });
        }, serverPingMs);
        pingTimer.unref?.();
        armIdle();
        return;
      case 'refuse': log(`refused by the controller: ${frame.reason}`); finish(1, `refused: ${frame.reason}`); return;
      case 'exec': clearTimeout(idleTimer); runner.start(frame); return;
      case 'ack': runner.ack(frame.id, frame.seq); return;
      case 'signal': runner.signal(frame.id, frame.sig); return;
      case 'ping': send({ type: 'pong', id: frame.id }); return;
      case 'pong': missed = 0; return;
      case 'shutdown': leave(frame.graceMs ?? 5000); return;
      case 'relay-result': { const r = relays.get(frame.id); if (r) { relays.delete(frame.id); r.resolve({ lines: frame.lines ?? null, error: frame.error ?? null }); } return; }
      default: return;
    }
  });
  ws.on('close', (code, reason) => finish(shuttingDown ? 0 : 1, `socket closed (${code}${reason?.length ? ` ${reason}` : ''})`));
  ws.on('error', (err) => finish(1, `socket error: ${err?.message || err}`));
  ws.on('unexpected-response', (_req, res) => { refusedStatus = res.statusCode; finish(1, `controller answered ${res.statusCode}`); });

  return {
    done,
    get exitCode() { return exitCode; },
    /** Whether the socket ever opened, the HTTP status of a refused upgrade, and why it ended: a first connect that
     *  could not be MADE (the controller's address not answering yet) is worth another try; anything else is final. */
    get connected() { return connected; },
    get refusedStatus() { return refusedStatus; },
    get endedWith() { return endedWith; },
    get liveCount() { return runner.liveCount; },
    send,
    relay,
    /** Removed from outside before it left on its own: not a clean exit. */
    close: (why = 'closed') => finish(1, why),
    leave,
  };
}
