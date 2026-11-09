// src/core/executor/backends/kubernetes.mjs
// The worker backend for a controller that runs in Kubernetes: one Pod per worker, created from
// the operator's template (k8s-pod-spec.mjs) through the API server (k8s-api.mjs), watched until
// it runs, deleted when the registry releases it. The registry waits for the worker's hello on
// its own listener; `start` here resolves as soon as the pod is Running — so what it rejects
// FAST is what the cluster knows before any process starts: a pod the API refuses, an image
// that cannot be pulled, a container that cannot be configured, a pod that exits before it ever
// connects (with its log tail when the service account may read logs), and a pod nothing will
// schedule within WORCA_K8S_PENDING_TIMEOUT_MS.
//
// Configuration (the controller's environment):
//   WORCA_K8S_POD_TEMPLATE       path of the JSON Pod template, default /etc/worca/worker-pod.json;
//                                re-read on every start, so a ConfigMap edit applies to the next worker
//   WORCA_INSTANCE_ID            label value that separates this controller's pods from another's (default worca)
//   WORCA_K8S_WORKER_IMAGE       image pattern with {version} {engine} {variant} placeholders
//   WORCA_K8S_WORKER_TAG         the {version}; default: this package's version
//   WORCA_K8S_WORKER_VARIANT     slim (default) | full
//   WORCA_K8S_WORKER_IMAGES      JSON map engine -> image, overriding the pattern per engine
//   WORCA_K8S_DEFAULT_ENGINE     the engine every worker gets; default: the `run.engine` setting (claude)
//   WORCA_K8S_PENDING_TIMEOUT_MS how long a pod may stay Pending (default 300000)
//   WORCA_K8S_ACTIVE_DEADLINE_S  spec.activeDeadlineSeconds of every worker pod (default 86400)
//   WORCA_K8S_RESOURCES          JSON { requests, limits } for the worker container (replaces the template's)
//   plus the API client's WORCA_K8S_API_URL / WORCA_K8S_TOKEN_FILE / WORCA_K8S_CA_FILE / WORCA_K8S_NAMESPACE.
//
// The worker container's environment carries its assignment (controller URL, one-time token,
// scope, worker id, HOME and projects root — the same paths as on the controller) and the
// controller's own network/identity settings that a worker needs (broker URL, proxy and CA
// settings, git identity, TZ, mock mode). Credentials never travel in a pod spec: GH_TOKEN,
// WORCA_GH_*, WORCA_BROKER_SECRET*, ANTHROPIC_API_KEY and CLAUDE_CODE_OAUTH_TOKEN are refused
// outright — the credential broker hands them to the worker per exec.
//
// Pre-warm hook: `start({ …, prewarmed: { name } })` skips the create and watches the named,
// already-created pod until it runs (a pool that creates pods ahead of demand and delivers the
// token through a mounted secret can hand one over this way). Everything else — the handle,
// the rejection paths, the cleanup — is the same.
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { createK8sApi, K8sApiError } from './k8s-api.mjs';
import { startFailure, passThroughEnv, defaultWorkerEngine, fullVariantEngines, envInt, abortError } from '../shared.mjs';
import {
  renderPodSpec, resolveImage, normalizeVariant, podName, workerSelector, workerContainer, assertAllowedEnv,
  DEFAULT_IMAGE_PATTERN, ANNOTATION_SCOPE, LABEL_ENGINE, LABEL_BOOT, scopeLabel,
} from './k8s-pod-spec.mjs';

export const DEFAULT_POD_TEMPLATE = '/etc/worca/worker-pod.json';
export const DEFAULT_PENDING_TIMEOUT_MS = 300_000;
export const DEFAULT_ACTIVE_DEADLINE_S = 86_400;
export const REMOVE_GRACE_S = 30;
export const WORKER_HOME = '/worca';
export const WORKER_PROJECTS_ROOT = '/projects';
const LOG_TAIL_LINES = 50;

/** Container waiting reasons that no amount of waiting fixes: the pod is rejected on the first sight of one. */
export const FATAL_WAITING_REASONS = Object.freeze(['ImagePullBackOff', 'ErrImagePull', 'ErrImageNeverPull', 'InvalidImageName', 'CreateContainerConfigError', 'CreateContainerError']);
const FATAL_WAITING = new Set(FATAL_WAITING_REASONS);

const packageVersion = () => { try { return createRequire(import.meta.url)('../../../../package.json').version; } catch { return 'latest'; } };
const envJson = (env, name) => {
  const raw = String(env[name] || '').trim();
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (err) { throw new Error(`${name} is not valid JSON: ${err.message}`); }
};


/**
 * The worker container's environment (pure). `controllerEnv` is the controller's own environment,
 * read for the pass-through variables; `extra` comes from the registry's start call and may not
 * carry credentials.
 */
export function workerEnvFor({ controllerUrl, token, scopeKey, workerId, idleMs = null, controllerEnv = {}, extra = {} } = {}) {
  assertAllowedEnv(extra);
  const out = {
    WORCA_CONTROLLER_URL: controllerUrl,
    WORCA_WORKER_TOKEN: token,
    WORCA_WORKER_SCOPE: scopeKey,
    WORCA_WORKER_ID: workerId,
    WORCA_HOME: WORKER_HOME,
    WORCA_PROJECTS_ROOT: WORKER_PROJECTS_ROOT,
    ...passThroughEnv(controllerEnv, { controllerUrl }),   // the same inherited set as under Docker (shared.mjs)
  };
  if (Number.isFinite(Number(idleMs)) && Number(idleMs) > 0) out.WORCA_WORKER_IDLE_MS = String(Math.floor(Number(idleMs)));
  for (const [k, v] of Object.entries(extra)) if (v !== undefined && v !== null) out[k] = String(v);
  return out;
}

/**
 * What a pod's state means for a worker that must reach Running:
 * `{ kind: 'running' | 'exited' | 'fatal' | 'unschedulable' | 'pending', reason, message, image }`.
 */
export function classifyPod(pod) {
  const status = pod?.status || {};
  const statuses = [...(status.initContainerStatuses || []), ...(status.containerStatuses || [])];
  const spec = pod?.spec || {};
  const containers = [...(spec.initContainers || []), ...(spec.containers || [])];
  const imageOf = (s) => s?.image || containers.find((c) => c?.name === s?.name)?.image || null;
  if (status.phase === 'Running') return { kind: 'running', reason: 'Running', message: '' };
  if (status.phase === 'Failed' || status.phase === 'Succeeded') {
    const term = statuses.map((s) => s.state?.terminated).find(Boolean);
    const reason = status.reason || term?.reason || status.phase;
    const bits = [status.message, term?.message, term?.exitCode != null ? `exit code ${term.exitCode}` : null].filter(Boolean);
    return { kind: 'exited', reason, message: bits.join('; ') };
  }
  const fatal = statuses.find((s) => s.state?.waiting && FATAL_WAITING.has(s.state.waiting.reason));
  if (fatal) return { kind: 'fatal', reason: fatal.state.waiting.reason, message: fatal.state.waiting.message || '', image: imageOf(fatal) };
  const sched = (status.conditions || []).find((c) => c.type === 'PodScheduled' && c.status === 'False');
  if (sched?.reason === 'Unschedulable') return { kind: 'unschedulable', reason: 'Unschedulable', message: sched.message || '' };
  const waiting = statuses.map((s) => s.state?.waiting).find(Boolean);
  const cond = [...(status.conditions || [])].reverse().find((c) => c.status !== 'True');
  const message = waiting ? `${waiting.reason}${waiting.message ? `: ${waiting.message}` : ''}`
    : cond ? `${cond.type}=${cond.status}${cond.reason ? ` (${cond.reason})` : ''}${cond.message ? `: ${cond.message}` : ''}`
      : (status.phase || 'Pending');
  return { kind: 'pending', reason: waiting?.reason || cond?.reason || status.phase || 'Pending', message };
}

export function createKubernetesBackend({ env = process.env, log = (msg) => console.log(`[worca-k8s] ${msg}`), api = null, bootId = `ctl-${randomUUID()}` } = {}) {
  const client = api || createK8sApi({ env, log });
  const namespace = client.namespace;
  const templatePath = String(env.WORCA_K8S_POD_TEMPLATE || '').trim() || DEFAULT_POD_TEMPLATE;
  const instanceId = String(env.WORCA_INSTANCE_ID || '').trim() || 'worca';
  const imagePattern = String(env.WORCA_K8S_WORKER_IMAGE || '').trim() || DEFAULT_IMAGE_PATTERN;
  const version = String(env.WORCA_K8S_WORKER_TAG || '').trim() || packageVersion();
  const variant = normalizeVariant(env.WORCA_K8S_WORKER_VARIANT);
  const fullEngines = fullVariantEngines(env);
  const images = envJson(env, 'WORCA_K8S_WORKER_IMAGES');
  if (images !== null && (typeof images !== 'object' || Array.isArray(images))) throw new Error('WORCA_K8S_WORKER_IMAGES must be a JSON object { engine: image }');
  // Per-engine overrides, the form the Helm chart renders from `worker.engines`: { <engine>: { image?, variant?, resources? } }.
  const engines = envJson(env, 'WORCA_K8S_ENGINES');
  if (engines !== null && (typeof engines !== 'object' || Array.isArray(engines))) throw new Error('WORCA_K8S_ENGINES must be a JSON object { engine: { image, variant, resources } }');
  const resources = envJson(env, 'WORCA_K8S_RESOURCES');
  /** What one engine's worker runs as: the per-engine entry over the defaults. */
  const settingsFor = (engine) => {
    const o = engines?.[engine] && typeof engines[engine] === 'object' ? engines[engine] : {};
    return {
      // The install-wide `full` exists only for some engines' worker images (shared.mjs FULL_VARIANT_ENGINES); an
      // engine's own setting is taken as given.
      variant: o.variant ? normalizeVariant(o.variant) : (variant && !fullEngines.includes(engine) ? '' : variant),
      resources: o.resources && typeof o.resources === 'object' ? o.resources : resources,
      overrides: o.image ? { ...(images || {}), [engine]: String(o.image) } : images,
    };
  };
  const pendingTimeoutMs = envInt(env, 'WORCA_K8S_PENDING_TIMEOUT_MS', DEFAULT_PENDING_TIMEOUT_MS);
  const activeDeadlineSeconds = envInt(env, 'WORCA_K8S_ACTIVE_DEADLINE_S', DEFAULT_ACTIVE_DEADLINE_S);
  const live = new Map();   // workerId -> handle

  function readTemplate() {
    let text;
    try { text = readFileSync(templatePath, 'utf8'); } catch (err) { throw startFailure(`worker pod template ${templatePath}: ${err.message}`); }
    let template;
    try { template = JSON.parse(text); } catch (err) { throw startFailure(`worker pod template ${templatePath} is not valid JSON: ${err.message}`); }
    try { workerContainer(template); } catch (err) { throw startFailure(`worker pod template ${templatePath}: ${err.message}`); }
    return template;
  }

  const engineFor = () => defaultWorkerEngine(env, ['WORCA_K8S_DEFAULT_ENGINE', 'WORCA_WORKER_ENGINE']);

  const handleOf = (pod, { workerId = null, engine = null, scopeKey = null } = {}) => {
    const c = (pod?.spec?.containers || []).find((x) => x?.name === 'worker') || pod?.spec?.containers?.[0];
    const envVal = (name) => (c?.env || []).find((e) => e?.name === name)?.value || null;
    return {
      workerId: workerId || envVal('WORCA_WORKER_ID') || pod?.metadata?.name,
      name: pod?.metadata?.name,
      uid: pod?.metadata?.uid || null,
      namespace,
      scopeKey: scopeKey || pod?.metadata?.annotations?.[ANNOTATION_SCOPE] || envVal('WORCA_WORKER_SCOPE') || null,
      engine: engine || pod?.metadata?.labels?.[LABEL_ENGINE] || null,
      bootId: pod?.metadata?.labels?.[LABEL_BOOT] || null,
      own: !!bootId && pod?.metadata?.labels?.[LABEL_BOOT] === scopeLabel(bootId),   // started by this controller process: never an orphan
      image: c?.image || null,
      startedAt: pod?.metadata?.creationTimestamp ? Date.parse(pod.metadata.creationTimestamp) || Date.now() : Date.now(),
    };
  };

  async function createPod(scope, render) {
    let manifest = render(podName(scope, Date.now()));
    for (let attempt = 0; ; attempt += 1) {
      try {
        return { pod: await client.createPod(namespace, manifest), manifest };
      } catch (err) {
        if (err instanceof K8sApiError) {
          if (err.status === 409 && attempt === 0) {
            log(`pod ${manifest.metadata.name} already exists; retrying with a new name`);
            manifest = render(podName(scope, Date.now() + 1 + Math.floor(Math.random() * 1000)));
            continue;
          }
          throw startFailure(`cannot create worker pod: ${err.message}`, { status: err.status, reason: err.reason });
        }
        throw startFailure(`cannot reach the Kubernetes API at ${client.apiUrl}: ${err?.cause?.message || err?.message || err}`);
      }
    }
  }

  /** Resolve with the pod once it is Running; reject on anything that means it never will be. */
  function awaitRunning(initial, name, image, signal = null) {
    const ctl = new AbortController();
    let settled = false;
    let last = null;
    const unschedulable = new Set();
    return new Promise((resolve, reject) => {
      const finish = (ok, value) => { if (settled) return; settled = true; clearTimeout(timer); signal?.removeEventListener('abort', onAbort); ctl.abort(); (ok ? resolve : reject)(value); };
      // The registry gave up on this start (nobody waits for it any more): stop watching; start() deletes the pod.
      const onAbort = () => finish(false, abortError());
      if (signal?.aborted) { settled = true; reject(abortError()); return; }
      signal?.addEventListener('abort', onAbort, { once: true });
      const rejectWithLog = async (headline) => {
        if (settled) return;
        settled = true; clearTimeout(timer); signal?.removeEventListener('abort', onAbort); ctl.abort();
        let tail = null;
        try { tail = await client.podLog(namespace, name, { tailLines: LOG_TAIL_LINES, container: 'worker' }); } catch (err) { log(`log of ${name}: ${err.message}`); }
        const trimmed = typeof tail === 'string' ? tail.trim() : '';
        reject(startFailure(trimmed ? `${headline}\n${trimmed}` : headline, { pod: name }));
      };
      const timer = setTimeout(() => {
        const state = last ? `${last.reason}${last.message ? `: ${last.message}` : ''}` : 'Pending';
        finish(false, startFailure(`worker pod ${name} did not start within ${Math.round(pendingTimeoutMs / 1000)} s (last state: ${state})`, { pod: name }));
      }, pendingTimeoutMs);
      const consider = (type, pod) => {
        if (settled) return;
        if (type === 'DELETED') return finish(false, startFailure(`worker pod ${name} was deleted before it connected`, { pod: name }));
        const c = classifyPod(pod);
        last = c;
        switch (c.kind) {
          case 'running': return finish(true, pod);
          case 'exited': return void rejectWithLog(`worker pod exited before connecting: ${c.reason}${c.message ? ` (${c.message})` : ''}`);
          case 'fatal': return finish(false, startFailure(`worker pod ${name} cannot start: ${c.reason} for image ${c.image || image}${c.message ? ` (${c.message})` : ''}`, { pod: name, reason: c.reason }));
          case 'unschedulable':
            if (!unschedulable.has(c.message)) { unschedulable.add(c.message); log(`pod ${name} is unschedulable: ${c.message || 'no message'}; waiting up to ${Math.round(pendingTimeoutMs / 1000)} s`); }
            return;
          default: return;
        }
      };
      consider('ADDED', initial);
      if (settled) return;
      client.watchPod(namespace, name, { resourceVersion: initial?.metadata?.resourceVersion, signal: ctl.signal, onEvent: consider })
        .then(() => { if (!settled) finish(false, startFailure(`watch on worker pod ${name} ended`, { pod: name })); })
        .catch((err) => finish(false, startFailure(`watch on worker pod ${name} failed: ${err?.message || err}`, { pod: name })));
    });
  }

  const backend = {
    kind: 'kubernetes',
    bootId,

    /** What tells a scope's workers apart: the image an engine resolves to (one image for every engine = one worker). */
    async classFor(engine) {
      const e = engine || await engineFor();
      const { variant, overrides } = settingsFor(e);
      return resolveImage({ pattern: imagePattern, engine: e, variant, version, overrides });
    },

    async start({ scope, scopeKey, workerId, token, controllerUrl, idleMs = null, env: extra = {}, prewarmed = null, engine: asked = null, signal = null } = {}) {
      if (!scope?.kind || !scope?.id || !scopeKey || !workerId) throw startFailure('start: scope, scopeKey and workerId are required');
      const workerEnv = workerEnvFor({ controllerUrl, token, scopeKey, workerId, idleMs, controllerEnv: env, extra });
      const engine = asked || await engineFor();   // the spawn's engine (the run's, the chat's); the install default only when none is named
      const { variant: engineVariant, resources: engineResources, overrides } = settingsFor(engine);
      const image = resolveImage({ pattern: imagePattern, engine, variant: engineVariant, version, overrides });
      let pod; let name;
      if (prewarmed?.name) {
        name = prewarmed.name;
        try { pod = await client.getPod(namespace, name); } catch (err) { throw startFailure(`pre-warmed worker pod ${name}: ${err.message}`, { pod: name }); }
      } else {
        const template = readTemplate();
        const render = (podNameFor) => renderPodSpec({ template, instanceId, scope, scopeKey, engine, variant: engineVariant, resources: engineResources, env: workerEnv, image, activeDeadlineSeconds, bootId, name: podNameFor });
        ({ pod } = await createPod(scope, render));
        name = pod?.metadata?.name;
      }
      log(`pod ${name} created for ${scopeKey} (${image})`);
      let running;
      try {
        running = await awaitRunning(pod, name, image, signal);
      } catch (err) {
        client.deletePod(namespace, name, { gracePeriodSeconds: 0 }).catch((e) => log(`cleanup of ${name}: ${e.message}`));
        throw err;
      }
      const handle = handleOf(running, { workerId, engine, scopeKey });
      handle.uid ||= pod?.metadata?.uid || null;
      handle.image ||= image;
      live.set(workerId, handle);
      return handle;
    },

    async remove(handle, { reason } = {}) {
      if (!handle?.name) return;
      live.delete(handle.workerId);
      log(`removing pod ${handle.name}${reason ? ` (${reason})` : ''}`);
      await client.deletePod(handle.namespace || namespace, handle.name, { gracePeriodSeconds: REMOVE_GRACE_S });
    },

    async listWorkers() {
      const pods = await client.listPods(namespace, { labelSelector: workerSelector(instanceId) });
      return pods.map((p) => handleOf(p));
    },

    describe() {
      return { kind: 'kubernetes', namespace, instanceId, template: templatePath, image: imagePattern, variant: variant || 'slim', version, live: live.size };
    },

    async close() { await client.close?.(); },
  };
  return backend;
}
