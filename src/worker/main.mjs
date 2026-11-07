// src/worker/main.mjs
// `worca worker`: the process that runs in a worker container. It reads its assignment from the
// environment (set by the controller's backend when it started the container), connects to the
// controller, and executes what it is sent until released. It opens no database and serves no
// HTTP; the shared volume is mounted at the same paths as on the controller.
//
//   WORCA_CONTROLLER_URL     ws://<controller host>:<worker port>   (required)
//   WORCA_WORKER_TOKEN       the one-time token, or WORCA_WORKER_TOKEN_FILE  (required)
//   WORCA_WORKER_SCOPE       run:<pipeline id> | chat:<thread id>     (required)
//   WORCA_WORKER_ID          the id the controller expects in the hello (required)
//   WORCA_WORKER_HOME        the HOME children run with; default <worcaHome>/worker-homes/<kind>/<id>
//   WORCA_WORKER_IDLE_MS     leave after this long with nothing to run (chat workers)
//
// Exit codes: 0 after a shutdown, an idle timeout or a refusal it was told about; 1 when the
// connection dropped; 78 (EX_CONFIG) when the assignment is incomplete; `--check` prints the
// capabilities as JSON and exits 0 without connecting (image smoke tests).
import { readFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { worcaHome } from '../core/projects.mjs';
import { parseScopeKey } from '../core/executor/scope.mjs';
import { probeCapabilities } from './capabilities.mjs';
import { startWorker } from './client.mjs';

export const EX_CONFIG = 78;

/** The assignment from the environment, or `{ errors }`. Pure over `env`. */
export function readWorkerConfig(env = process.env) {
  const errors = [];
  const url = String(env.WORCA_CONTROLLER_URL || '').trim();
  if (!/^wss?:\/\//.test(url)) errors.push(url ? `WORCA_CONTROLLER_URL must be a ws:// or wss:// URL (got ${url})` : 'WORCA_CONTROLLER_URL is not set');
  const scope = parseScopeKey(String(env.WORCA_WORKER_SCOPE || '').trim());
  if (!scope) errors.push(env.WORCA_WORKER_SCOPE ? `WORCA_WORKER_SCOPE must be run:<id> or chat:<id> (got ${env.WORCA_WORKER_SCOPE})` : 'WORCA_WORKER_SCOPE is not set');
  let token = String(env.WORCA_WORKER_TOKEN || '').trim();
  if (!token && env.WORCA_WORKER_TOKEN_FILE) {
    try { token = readFileSync(env.WORCA_WORKER_TOKEN_FILE, 'utf8').trim(); } catch (err) { errors.push(`WORCA_WORKER_TOKEN_FILE: ${err.message}`); }
  }
  if (!token) errors.push('WORCA_WORKER_TOKEN (or WORCA_WORKER_TOKEN_FILE) is not set');
  const workerId = String(env.WORCA_WORKER_ID || '').trim();
  if (!workerId) errors.push('WORCA_WORKER_ID is not set');
  const idle = Number(env.WORCA_WORKER_IDLE_MS);
  const idleMs = Number.isFinite(idle) && idle > 0 ? idle : null;
  if (errors.length) return { errors };
  const home = String(env.WORCA_WORKER_HOME || '').trim() || join(worcaHome(), 'worker-homes', scope.kind, scope.id);
  return { url, scope, token, workerId, home, idleMs, errors: [] };
}

/** `worca worker [--check]` entry. Returns the exit code. */
export async function runWorkerCli(args = [], { env = process.env, out = (l) => process.stdout.write(`${l}\n`), err = (l) => process.stderr.write(`${l}\n`) } = {}) {
  if (args.includes('--check')) {
    out(JSON.stringify(await probeCapabilities({ env }), null, 2));
    return 0;
  }
  const cfg = readWorkerConfig(env);
  if (cfg.errors.length) { for (const e of cfg.errors) err(`worca worker: ${e}`); return EX_CONFIG; }
  try { mkdirSync(cfg.home, { recursive: true, mode: 0o700 }); } catch (e) { err(`worca worker: cannot create HOME ${cfg.home}: ${e.message}`); return EX_CONFIG; }
  const capabilities = await probeCapabilities({ env });
  const installed = Object.entries(capabilities.engines).filter(([, v]) => v).map(([k, v]) => `${k} ${v.version || '?'}`);
  out(`worca worker ${cfg.workerId} for ${cfg.scope.kind}:${cfg.scope.id} — engines: ${installed.join(', ') || 'none'}; HOME ${cfg.home}`);
  // The token leaves the environment before any child can read it (children get their env from the controller anyway).
  delete env.WORCA_WORKER_TOKEN;
  const w = startWorker({ ...cfg, capabilities, log: out });
  const graceMs = Number(env.WORCA_SIGTERM_GRACE_MS) > 0 ? Number(env.WORCA_SIGTERM_GRACE_MS) : 5000;
  const onSignal = () => { w.leave(graceMs); };
  process.once('SIGTERM', onSignal);
  process.once('SIGINT', onSignal);
  return w.done;
}

/** A worker id for a backend that lets the worker choose one (tests). */
export function randomWorkerId(kind = 'run') { return `w-${kind}-${randomBytes(4).toString('hex')}`; }
