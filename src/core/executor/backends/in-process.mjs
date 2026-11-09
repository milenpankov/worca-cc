// src/core/executor/backends/in-process.mjs
// A backend for tests: "starts a worker" by running the worker client in THIS process, over a
// real socket to the registry's listener, so the whole path — token, hello, exec frames,
// backpressure, signals, shutdown — runs without a container. The execs are real children of
// the test process.
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { startWorker } from '../../../worker/client.mjs';
import { probeCapabilities } from '../../../worker/capabilities.mjs';

export function createInProcessBackend({ homeRoot, capabilities = null, log = () => {}, beforeStart = null } = {}) {
  const workers = new Map();   // workerId -> handle
  const started = [];
  return {
    kind: 'in-process',
    started,
    get live() { return [...workers.values()]; },
    async start({ scope, scopeKey, workerId, token, controllerUrl, idleMs, engine = null, signal = null }) {
      if (beforeStart) await beforeStart({ scope, scopeKey, workerId, engine, signal });
      const home = join(homeRoot, 'worker-homes', scope.kind, scope.id);
      mkdirSync(home, { recursive: true });
      const caps = capabilities ?? await probeCapabilities({ engines: [] });
      const w = startWorker({ url: controllerUrl, token, scope, workerId, home, capabilities: caps, idleMs, log });
      const handle = { workerId, scopeKey, engine, home, worker: w, done: w.done, own: true };   // cannot outlive this process: never an orphan
      workers.set(workerId, handle);
      started.push(handle);
      w.done.then(() => workers.delete(workerId));
      return handle;
    },
    async remove(handle, { reason } = {}) {
      if (!handle) return;
      workers.delete(handle.workerId);
      handle.worker.close(reason || 'removed');
      await handle.done;
    },
    async listWorkers() { return [...workers.values()]; },
    describe() { return { kind: 'in-process', live: workers.size }; },
  };
}
