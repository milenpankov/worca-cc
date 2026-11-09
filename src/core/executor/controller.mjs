// src/core/executor/controller.mjs — the controller side of worker containers (docs/workers.md), kept
// out of ui/server.mjs: install the executor WORCA_EXECUTOR names (its backend, the worker listener,
// the registry chat relays reach), answer a chat turn's relayed MCP lines, say what /api/health shows
// about where agents run, reap the previous controller's workers at boot, close them at shutdown.
// Also re-exports what the server wires with, so the server imports one module.
import { randomUUID } from 'node:crypto';
import { executorKind, executorConfigError, installExecutor, sweepWorkerHomes, sweepStaged } from './index.mjs';
import { createWorkerRegistry } from './registry.mjs';
import { createBackend } from './backends/index.mjs';
import { worcaHome } from '../projects.mjs';
import { readEgressPolicy } from '../egress-proxy.mjs';
import { withBillTo } from '../billing.mjs';
import { DEFAULT_RELAY_PORT } from '../../worker/relay-shim.mjs';

export { isRemoteExecutor, releaseWorker, touchWorker } from './index.mjs';
export { withExecutorScope } from './scope.mjs';
export { bridgedModelRefusal } from './gates.mjs';
/** The loopback port of a worker's chat relay shim. Set on the CONTROLLER (it writes the URL into the chat's MCP
 *  config) and handed to every worker (shared.mjs WORKER_PASS_THROUGH), so both sides always agree. */
export const WORKER_RELAY_PORT = Number(process.env.WORCA_WORKER_RELAY_PORT) > 0 ? Number(process.env.WORCA_WORKER_RELAY_PORT) : DEFAULT_RELAY_PORT;

/** The worker registry under a remote executor (src/core/executor/registry.mjs), else null. */
let WORKERS = null;
/** The UI server's live chat relays (token -> the turn's entry), bound once at module load. */
let askRelays = null;

export function workerRegistry() { return WORKERS; }

/** The server hands over its relay map (ui/server.mjs askRelays) so relayAskLine can answer. */
export function bindAskRelays(map) { askRelays = map; }

/**
 * One JSON-RPC line from a chat turn's MCP child, answered by the turn's worca tools in the
 * controller. The route POST /api/ask/relay and a worker's `relay` frame both land here.
 * @returns {Promise<{lines: string[]|null, error: string|null}>}
 */
export async function relayAskLine(token, line) {
  const entry = askRelays?.get(String(token || ''));
  if (!entry) return { lines: null, error: 'relay: unknown or finished turn' };
  if (typeof line !== 'string') return { lines: null, error: 'relay: line is required' };
  // One line at a time per turn (the child sends sequentially), so the output collected
  // between feed and idle is this line's answer.
  await withBillTo(entry.billTo, async () => {
    entry.out = [];
    await entry.rpc.feed(line);
    await entry.rpc.idle();
  }, { owner: entry.owner });
  return { lines: entry.out, error: null };
}

/**
 * Install the executor WORCA_EXECUTOR names: the backend, the worker listener, and the registry
 * chat relays reach. Local: nothing to do. A misconfiguration throws with the reason; the boot
 * refuses on it (exit 78). Exported for the server's own tests too.
 * @returns {Promise<{kind:string, registry:object|null}>}
 */
export async function installExecutorFromEnv({ log = (l) => console.log(l), port, host } = {}) {
  const cfgErr = executorConfigError();
  if (cfgErr) throw new Error(cfgErr);
  const kind = executorKind();
  if (kind === 'local') return { kind, registry: null };
  // Workers in other containers are told where to connect. Without WORCA_CONTROLLER_URL that would be this
  // listener's own loopback, which no container can dial: every run would time out with no hint. Refuse now.
  // (The in-process test backend runs its worker in this process, where loopback is right.)
  const inProcess = String(process.env.WORCA_EXECUTOR_BACKEND || '').trim().toLowerCase() === 'in-process';
  if (!inProcess && !String(process.env.WORCA_CONTROLLER_URL || '').trim()) {
    throw new Error(`WORCA_CONTROLLER_URL is not set: with WORCA_EXECUTOR=${kind} the workers run in other containers and cannot reach this process on loopback. Set it to the address they dial, e.g. ws://worca:4319 (the Compose service) or ws://<service>.<namespace>.svc:4319`);
  }
  // One boot id for the registry and the backend: a worker container's label names this controller process.
  const bootId = `ctl-${randomUUID()}`;
  const backend = await createBackend(kind, { log: (l) => log(`[worca-ui] executor: ${l}`), homeRoot: worcaHome(), bootId });
  const registry = createWorkerRegistry({
    backend, bootId, log: (l) => log(`[worca-ui] workers: ${l}`), onRelay: (f) => relayAskLine(f.token, f.line),
    ...(port != null ? { port } : {}), ...(host ? { host } : {}),
  });
  await registry.listen();
  WORKERS = registry;
  installExecutor({ kind, registry });
  log(`[worca-ui] executor: ${kind} (${backend.describe?.()?.kind || kind}); workers connect to ${registry.controllerUrl}, up to ${registry.describe().maxWorkers} at once`);
  if (readEgressPolicy(process.env).enforced) log('[worca-ui] executor: the outbound network policy proxy runs in this process and does not reach workers; use the egress sidecar (docker/compose.egress.yml) or a cluster network policy');
  return { kind, registry };
}

/** What /api/health says about where agents run: null under the local executor. */
export function workersHealth() {
  if (!WORKERS) return null;
  const d = WORKERS.describe();
  return { kind: executorKind(), backend: d.backend?.kind ?? null, workers: d.workers, maxWorkers: d.maxWorkers, port: d.port };
}

/**
 * Whether a run's worker HOME (its engine sessions) is still needed, from its row: a run that is paused, or still
 * marked live, or that a crash interrupted with its resume point intact. Boot maintenance relabels every stale
 * `running` row `interrupted` BEFORE this is asked, and such a run resumes (auto-resume, or by hand) with `--resume`.
 */
export function keepsWorkerHome(row) {
  if (!row) return false;
  if (['paused', 'pausing', 'running'].includes(row.status)) return true;
  return row.status === 'interrupted' && row.resume_point != null && row.resume_point !== '';
}

/**
 * Boot maintenance: with one controller, every worker still around at boot belonged to the previous
 * one; their HOMEs stay only for runs a resume will continue (`keepRun(id)` says which), and staged
 * spawn folders older than this process are nobody's. Returns
 * `{ workers: <reap summary> }` to merge into the boot summary, or `{}` under the local executor.
 * @param {{ rowOf?: (id:string) => object|null, keepRun?: (id:string) => boolean, log?: Function, error?: Function }} o
 */
export async function reapWorkersAtBoot({ rowOf = null, keepRun = (id) => keepsWorkerHome(rowOf?.(id)), log = (l) => console.log(l), error = (l) => console.error(l) }) {
  if (!WORKERS) return {};
  const out = {};
  try { out.workers = await WORKERS.reapAtBoot(); } catch (err) { error(`[worca-ui] worker reap failed: ${err && err.message ? err.message : err}`); }
  try {
    const removed = sweepWorkerHomes({ keep: ({ kind, id }) => kind === 'chat' || keepRun(id) });
    if (removed.length) log(`[worca-ui] removed ${removed.length} worker home(s) of finished runs`);
  } catch (err) { error(`[worca-ui] worker-home sweep failed: ${err && err.message ? err.message : err}`); }
  try {
    const staged = sweepStaged();
    if (staged) log(`[worca-ui] removed ${staged} staged spawn folder(s) a previous controller left`);
  } catch (err) { error(`[worca-ui] staged-file sweep failed: ${err && err.message ? err.message : err}`); }
  return out;
}

/** Shutdown step: every worker told to leave (after the drain paused their runs). */
export function closeWorkers(reason = 'shutdown') {
  return WORKERS ? WORKERS.closeAll(reason) : undefined;
}
