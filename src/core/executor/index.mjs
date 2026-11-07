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
import { worcaHome } from '../projects.mjs';
import { currentExecutorScope } from './scope.mjs';

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
export async function sessionFor({ signal } = {}) {
  if (!installed) return null;
  const scope = currentExecutorScope();
  if (!scope) return null;
  return installed.registry.acquire(scope, { signal });
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
