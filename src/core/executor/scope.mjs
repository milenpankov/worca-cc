// src/core/executor/scope.mjs
// Which run or chat a spawn belongs to, carried by the async context the same way billing.mjs
// carries the acting person: the harness enters the scope once around a run's loop and the
// server once around a chat turn, and every engine or script spawn started inside sees it —
// no parameter threaded through the adapters. A spawn with no scope (a title, a PR
// description, a model test) is not attributable to a worker and stays on this process.
import { AsyncLocalStorage } from 'node:async_hooks';

const als = new AsyncLocalStorage();

export const SCOPE_KINDS = Object.freeze(['run', 'chat']);

/** A usable scope `{ kind: 'run'|'chat', id }`, or null. */
export function normalizeScope(scope) {
  if (!scope || typeof scope !== 'object') return null;
  const kind = SCOPE_KINDS.includes(scope.kind) ? scope.kind : null;
  const id = typeof scope.id === 'string' ? scope.id.trim() : '';
  return kind && id ? { kind, id } : null;
}

/** The scope as one string (`run:<id>`), the form workers echo back in their hello. */
export function scopeKey(scope) {
  const s = normalizeScope(scope);
  return s ? `${s.kind}:${s.id}` : null;
}

/** Parse `run:<id>` / `chat:<id>` back into a scope, or null. */
export function parseScopeKey(key) {
  if (typeof key !== 'string') return null;
  const i = key.indexOf(':');
  if (i < 1) return null;
  return normalizeScope({ kind: key.slice(0, i), id: key.slice(i + 1) });
}

/** Run `fn` with `scope` as the current executor scope (an unusable scope leaves it unset). */
export function withExecutorScope(scope, fn) {
  const s = normalizeScope(scope);
  return s ? als.run(s, fn) : fn();
}

/** The executor scope of this async context, or null. */
export function currentExecutorScope() {
  return als.getStore() ?? null;
}
