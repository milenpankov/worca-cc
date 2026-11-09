// src/core/executor/index.mjs
// Where harness processes run. `local` (the default, and the only kind the CLI, the tests and a
// worker itself ever have) spawns them in this process exactly as before. A remote kind
// (`docker`, `kubernetes`) is INSTALLED by the UI server at boot — never read from the
// environment by a spawn — and routes every spawn that carries an executor scope
// (scope.mjs) to a worker session the registry acquired for that run or chat.
//
// The seam is deliberately tiny: spawn.mjs and script-runner.mjs ask `sessionFor()` and get
// either null (spawn here) or a session with `exec()`/`signal()`; everything else about a
// spawn — the argv, the env, the cwd, line framing, abort escalation, classification — is
// unchanged. See docs/workers.md.
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { readdirSync, rmSync, statSync } from 'node:fs';
import { worcaHome } from '../projects.mjs';
import { currentExecutorScope, normalizeScope } from './scope.mjs';

export const EXECUTOR_KINDS = Object.freeze(['local', 'docker', 'kubernetes']);

/** The configured executor kind: WORCA_EXECUTOR, `local` when unset or unknown. */
export function executorKind(env = process.env) {
  const v = String(env.WORCA_EXECUTOR || 'local').trim().toLowerCase();
  return EXECUTOR_KINDS.includes(v) ? v : 'local';
}

/** Why `WORCA_EXECUTOR` is unusable, or null when it is fine (the server refuses to boot on a reason). */
export function executorConfigError(env = process.env) {
  const raw = String(env.WORCA_EXECUTOR || '').trim();
  if (!raw) return null;
  return EXECUTOR_KINDS.includes(raw.toLowerCase()) ? null
    : `WORCA_EXECUTOR=${raw} is not one of ${EXECUTOR_KINDS.join(', ')}`;
}

let installed = null;   // { kind, registry } — set once by the server; null = local

/**
 * Install the process-wide executor. `registry` must offer
 * `acquire(scope, { signal }) -> Promise<session>` where a session has `workerId`,
 * `exec(spec, handlers) -> id` and `signal(id, sig)` (see remote-child.mjs).
 * Installing `{ kind: 'local' }` (or null) restores local spawning.
 */
export function installExecutor(exec) {
  installed = exec && exec.kind && exec.kind !== 'local' ? exec : null;
}

/** The installed executor `{ kind, registry }`, or null under local. */
export function currentExecutor() {
  return installed;
}

export function isRemoteExecutor() {
  return !!installed;
}

/**
 * The worker session the spawn in this async context runs on, or null to spawn here: null
 * under local, and null for a spawn outside any scope (an auxiliary spawn stays on the
 * controller). Rejects when no worker could be acquired.
 */
export async function sessionFor({ signal, engine = null } = {}) {
  if (!installed) return null;
  const scope = currentExecutorScope();
  if (!scope) return null;
  // The spawn's own engine picks the worker (its image); a spawn that names none — a script node — runs on the
  // scope's engine's worker, or on whichever worker the scope already has.
  return installed.registry.acquire(scope, { signal, engine: engine || scope.engine || null });
}

/** True when the spawn in this async context would go to a worker. */
export function spawnsRemotely() {
  return !!installed && !!currentExecutorScope();
}

/**
 * Where per-spawn files (a staged prompt, a settings file) are written: the local tmpdir, or
 * under worca's home — the shared volume — when a worker in another container must read them.
 */
export function stagingBase() {
  return installed ? join(worcaHome(), 'tmp', 'stage') : tmpdir();
}

/**
 * Staged per-spawn folders (stagingBase) that a crashed controller left on the shared volume: each spawn removes
 * its own, so anything older than this process is nobody's. Removes them; returns how many. Under the local
 * executor staging is the OS temp folder and there is nothing of ours to sweep.
 */
export function sweepStaged({ before = Date.now() - Math.round(process.uptime() * 1000), home = worcaHome() } = {}) {
  const dir = join(home, 'tmp', 'stage');
  let names = [];
  try { names = readdirSync(dir); } catch { return 0; }
  let removed = 0;
  for (const name of names) {
    const p = join(dir, name);
    try { if (statSync(p).mtimeMs < before) { rmSync(p, { recursive: true, force: true }); removed += 1; } } catch { /* next boot */ }
  }
  return removed;
}

/** The HOME a scope's worker runs its children with: on the shared volume, so a replacement worker
 *  (after a pause, a lost container) finds the engine's sessions where the previous one left them. */
export function workerHomeDir(scope, home = worcaHome()) {
  const s = normalizeScope(scope);
  return s ? join(home, 'worker-homes', s.kind, s.id) : null;
}

/** Remove a scope's worker HOME. Best effort; never throws. */
export function removeWorkerHome(scope, home = worcaHome()) {
  const dir = workerHomeDir(scope, home);
  if (dir) { try { rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ } }
}

/**
 * Release a scope's worker and, unless `keepHome`, its HOME. A no-op under the local executor,
 * so the harness and the chat code call it unconditionally.
 */
export async function releaseWorker(scope, { reason = 'released', keepHome = false, graceMs } = {}) {
  // A HOME made while workers were on is removed even when they are off now (a thread deleted after a config change).
  if (!installed) { if (!keepHome) removeWorkerHome(scope); return false; }
  const released = await installed.registry.release(scope, { reason, ...(graceMs != null ? { graceMs } : {}) });
  if (!keepHome) removeWorkerHome(scope);
  return released;
}

/** Keep a scope's worker from idling out (a chat turn just ran). A no-op under the local executor. */
export function touchWorker(scope) {
  installed?.registry.touch?.(scope);
}

/**
 * The worker-homes that no longer belong to a live or paused run: removed. `keep` says which ids
 * stay (paused runs, whose sessions a resume needs). Returns the ids removed.
 */
export function sweepWorkerHomes({ keep = () => false, home = worcaHome() } = {}) {
  const removed = [];
  for (const kind of ['run', 'chat']) {
    const dir = join(home, 'worker-homes', kind);
    let names = [];
    try { names = readdirSync(dir); } catch { continue; }
    for (const id of names) {
      if (keep({ kind, id })) continue;
      try { rmSync(join(dir, id), { recursive: true, force: true }); removed.push(`${kind}:${id}`); } catch { /* next boot */ }
    }
  }
  return removed;
}
