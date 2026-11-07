// src/worker/client.mjs
// The worker's side of the socket: connect outbound to the controller with the one-time
// token, announce the scope and what is installed, then serve exec frames until the
// controller says shutdown, the scope idles out (chat workers), or the socket drops — in
// which case every child is killed and the process exits non-zero: a worker never outlives
// its controller connection (there is no reconnect; the controller starts a fresh worker).
import { WebSocket } from 'ws';
import { encodeFrame, decodeFrame, PROTOCOL_VERSION } from '../core/executor/protocol.mjs';
import { createExecRunner } from './exec.mjs';

export const WORKER_WS_PATH = '/worker';
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
export function startWorker({ url, token, scope, workerId, home, capabilities, idleMs = null, pingMs = DEFAULT_PING_MS, log = () => {} }) {
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

  const ws = new WebSocket(target, { headers: { authorization: `Bearer ${token}` }, handshakeTimeout: CONNECT_TIMEOUT_MS, maxPayload: 8 * 1024 * 1024 + 1024 });
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
    clearTimeout(idleTimer); clearInterval(pingTimer);
    for (const [id, r] of relays) { relays.delete(id); r.resolve({ lines: null, error: `worker done: ${why}` }); }
    runner.killAll();
    log(`worker ${workerId} done: ${why}`);
    try { ws.terminate(); } catch { /* gone */ }
    resolveDone(code);
  };

  const armIdle = () => {
    clearTimeout(idleTimer);
    if (idleMs == null || runner.liveCount > 0) return;
    idleTimer = setTimeout(async () => {
      if (runner.liveCount > 0) { armIdle(); return; }
      shuttingDown = true;
      send({ type: 'bye' });
      try { ws.close(1000, 'idle'); } catch { /* gone */ }
      finish(0, `idle for ${idleMs} ms`);
    }, idleMs);
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
  ws.on('unexpected-response', (_req, res) => finish(1, `controller answered ${res.statusCode}`));

  return {
    done,
    get exitCode() { return exitCode; },
    get liveCount() { return runner.liveCount; },
    send,
    relay,
    /** Removed from outside before it left on its own: not a clean exit. */
    close: (why = 'closed') => finish(1, why),
    leave,
  };
}
