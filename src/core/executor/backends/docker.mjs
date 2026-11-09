// src/core/executor/backends/docker.mjs
// The `docker` executor backend: the controller runs in a container and starts one worker
// container per run or chat through the Docker Engine API (the socket, or a socket proxy at
// WORCA_DOCKER_HOST). Path parity is the whole trick — a worker must see the run's checkout,
// the store and the staged files at the paths the controller wrote into the run — so the
// controller's OWN container is the template: its mounts (minus the docker socket; the Claude
// login volume read-only as a seed), its network and its user are replicated onto every worker.
// Nothing here is Compose-specific; `docker run` and Podman's socket work the same.
import http from 'node:http';
import { readFileSync } from 'node:fs';
import { hostname } from 'node:os';
import { CONTROLLER_LABEL, SCOPE_LABEL, WORKER_LABEL, INSTANCE_LABEL, NETWORK_ROLE_LABEL, passThroughEnv, defaultWorkerEngine } from '../shared.mjs';

export const DOCKER_API = 'v1.43';
export { CONTROLLER_LABEL, SCOPE_LABEL, WORKER_LABEL };
const SEED_CLAUDE_DIR = '/seed/claude';
const START_WAIT_MS = 10_000;

/** `4g` / `512m` / `1024` -> bytes; null when unset or unparsable. */
export function parseBytes(v) {
  const m = /^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*$/i.exec(String(v ?? ''));
  if (!m) return null;
  const n = Number(m[1]);
  const mult = { '': 1, k: 1024, m: 1024 ** 2, g: 1024 ** 3, t: 1024 ** 4 }[m[2].toLowerCase()];
  return Number.isFinite(n) ? Math.round(n * mult) : null;
}

/** The Docker endpoint: a unix socket (default) or `tcp://host:port` (a socket proxy). */
export function dockerEndpoint(env = process.env) {
  const h = String(env.WORCA_DOCKER_HOST || env.DOCKER_HOST || '').trim();
  if (!h || h.startsWith('unix://')) return { socketPath: h ? h.slice('unix://'.length) : '/var/run/docker.sock' };
  const m = /^(?:tcp|http):\/\/([^/:]+)(?::(\d+))?\/?$/.exec(h);
  if (!m) throw new Error(`WORCA_DOCKER_HOST=${h}: expected tcp://host:port or unix:///path`);
  return { host: m[1], port: Number(m[2] || 2375) };
}

/** The controller's own container id: compose sets it as the hostname; an operator may name it. */
export function selfContainerId(env = process.env) {
  const explicit = String(env.WORCA_CONTROLLER_CONTAINER || '').trim();
  if (explicit) return explicit;
  try {
    const m = /\/([0-9a-f]{64})(?:\.scope)?(?:\/|$)/.exec(readFileSync('/proc/self/cgroup', 'utf8'));
    if (m) return m[1];
  } catch { /* no cgroup file */ }
  return String(env.HOSTNAME || hostname());
}

/** A tiny Docker Engine API client over node:http (no dependency). */
export function createDockerApi(endpoint, { api = DOCKER_API } = {}) {
  const request = (method, path, body, { raw = false } = {}) => new Promise((resolve, reject) => {
    const payload = body === undefined ? null : JSON.stringify(body);
    const req = http.request({
      ...endpoint, method, path: `/${api}${path}`,
      headers: { ...(payload ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } : {}), host: 'docker' },
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        if (res.statusCode >= 400) {
          let message = buf.toString('utf8').trim();
          try { message = JSON.parse(message).message || message; } catch { /* plain text */ }
          return reject(Object.assign(new Error(`docker ${method} ${path}: ${res.statusCode} ${message}`), { status: res.statusCode }));
        }
        if (raw) return resolve(buf);
        if (!buf.length) return resolve(null);
        try { resolve(JSON.parse(buf.toString('utf8'))); } catch { resolve(buf.toString('utf8')); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
  return {
    version: () => request('GET', '/version'),
    inspect: (id) => request('GET', `/containers/${encodeURIComponent(id)}/json`),
    create: (name, spec) => request('POST', `/containers/create?name=${encodeURIComponent(name)}`, spec),
    start: (id) => request('POST', `/containers/${encodeURIComponent(id)}/start`),
    remove: (id) => request('DELETE', `/containers/${encodeURIComponent(id)}?force=1&v=1`),
    list: (labels) => request('GET', `/containers/json?all=1&filters=${encodeURIComponent(JSON.stringify({ label: labels }))}`),
    network: (name) => request('GET', `/networks/${encodeURIComponent(name)}`),
    logsTail: (id, n = 50) => request('GET', `/containers/${encodeURIComponent(id)}/logs?stdout=1&stderr=1&tail=${n}`, undefined, { raw: true }).then(demuxLogs).catch(() => ''),
  };
}

/** Docker's multiplexed log stream (8-byte frame headers) -> text. */
export function demuxLogs(buf) {
  if (!Buffer.isBuffer(buf)) return String(buf ?? '');
  if (buf.length < 8) return buf.toString('utf8');
  const parts = [];
  let i = 0;
  while (i + 8 <= buf.length) {
    const type = buf[i];
    const len = buf.readUInt32BE(i + 4);
    if (type > 2 || i + 8 + len > buf.length) return buf.toString('utf8');   // not multiplexed (a tty container)
    parts.push(buf.subarray(i + 8, i + 8 + len).toString('utf8'));
    i += 8 + len;
  }
  return parts.join('');
}

/**
 * The worker container's mounts, from the controller's: every volume and bind at the same
 * destination, except the docker socket, and the Claude login volume (the controller's
 * `~/.claude`) which rides read-only at /seed/claude for the worker to copy a login from.
 */
export function workerMountsFrom(selfMounts, { seedClaude = '/home/worca/.claude' } = {}) {
  const out = [];
  for (const m of selfMounts || []) {
    if (!m || !m.Destination) continue;
    if (/docker\.sock$/.test(m.Destination) || /docker\.sock$/.test(m.Source || '')) continue;
    const source = m.Type === 'volume' ? m.Name : m.Source;
    if (!source) continue;
    if (m.Destination === seedClaude) { out.push({ Type: m.Type, Source: source, Target: SEED_CLAUDE_DIR, ReadOnly: true }); continue; }
    out.push({ Type: m.Type, Source: source, Target: m.Destination, ReadOnly: m.RW === false });
  }
  return out;
}

/** The worker image for an engine: WORCA_WORKER_IMAGE (with `{engine}`), `self` for the controller's own image,
 *  or `<controller repo>-worker:<tag>-<engine>`. */
export function workerImageFor(engine, { pattern, controllerImage }) {
  if (pattern && pattern !== 'self') return pattern.includes('{engine}') ? pattern.replaceAll('{engine}', engine) : pattern;
  if (pattern === 'self' || !controllerImage) return controllerImage || null;
  const at = controllerImage.lastIndexOf(':');
  const slash = controllerImage.lastIndexOf('/');
  const [repo, tag] = at > slash ? [controllerImage.slice(0, at), controllerImage.slice(at + 1)] : [controllerImage, 'latest'];
  return `${repo}-worker:${tag}-${engine}`;
}

/** The worker container's env: its assignment, plus what every backend lets a worker inherit (shared.mjs
 *  passThroughEnv: connectivity and identity, never a credential). The controller's own home and projects paths
 *  are the worker's too, since its mounts are replicated at the same destinations. */
export function workerEnvFor({ scopeKey, workerId, token, controllerUrl, idleMs, engine, env = process.env, seedClaude = true }) {
  return {
    WORCA_CONTROLLER_URL: controllerUrl, WORCA_WORKER_TOKEN: token, WORCA_WORKER_SCOPE: scopeKey, WORCA_WORKER_ID: workerId,
    WORCA_WORKER_ENGINE: engine, WORCA_CONTAINER: '1', WORCA_NO_DB: '1',
    ...(idleMs ? { WORCA_WORKER_IDLE_MS: String(idleMs) } : {}),
    ...(seedClaude ? { WORCA_WORKER_SEED_CLAUDE: SEED_CLAUDE_DIR } : {}),
    ...passThroughEnv(env, { controllerUrl, also: ['WORCA_HOME', 'WORCA_PROJECTS_ROOT', 'WORCA_DATA_DIR'] }),
  };
}

/**
 * The network worker containers join, chosen from the controller's own. Never the Docker API network (the socket
 * proxy's, labelled `worca.role=docker-api`): a worker there could drive the Engine. An INTERNAL network is
 * preferred over an open one: with the egress overlay the controller is on `internal`, and its workers — where the
 * agents run — must have no other route out than the allowlist proxy. `WORCA_WORKER_NETWORK` names one outright.
 * When the Engine does not answer about a network, its name decides (`…_docker-proxy`, `…_internal`).
 */
export async function pickWorkerNetwork({ attached = [], inspect = async () => null, override = null, log = () => {} } = {}) {
  const names = [...attached].sort();
  const want = String(override || '').trim();
  if (want) {
    if (!names.includes(want)) throw new Error(`WORCA_WORKER_NETWORK=${want} is not a network of the controller's container (it is on: ${names.join(', ') || 'none'})`);
    return want;
  }
  if (!names.length) return 'bridge';
  const info = await Promise.all(names.map(async (name) => {
    let meta = null;
    try { meta = await inspect(name); } catch { meta = null; }
    return {
      name,
      api: meta ? meta.Labels?.[NETWORK_ROLE_LABEL] === 'docker-api' : /(^|[_-])docker[_-]proxy$/.test(name),
      internal: meta ? meta.Internal === true : /(^|[_-])internal$/.test(name),
    };
  }));
  const usable = info.filter((n) => !n.api);
  if (!usable.length) throw new Error(`the controller's container is only on the Docker API network (${names.join(', ')}): workers need a network they share with it`);
  const confined = usable.filter((n) => n.internal);
  const open = usable.filter((n) => !n.internal);
  if (confined.length && open.length) log(`the controller is on ${open.map((n) => n.name).join(', ')} as well as the internal network ${confined[0].name}: its own traffic is not confined to the egress proxy (with the workers overlay, set WORCA_UPLINK_NETWORK=internal). Workers join ${confined[0].name} only`);
  return (confined[0] || usable[0]).name;
}

export async function createDockerBackend({ env = process.env, log = () => {}, api = null, controllerId = null, bootId = `ctl-${process.pid}-${Date.now().toString(36)}` } = {}) {
  const docker = api || createDockerApi(dockerEndpoint(env));
  let version;
  try { version = await docker.version(); } catch (err) { throw new Error(`cannot reach the Docker Engine (${describeEndpoint(env)}): ${err.message}. Mount the socket or set WORCA_DOCKER_HOST to a socket proxy.`); }
  const selfId = controllerId || selfContainerId(env);
  let self;
  try { self = await docker.inspect(selfId); } catch (err) { throw new Error(`cannot inspect the controller's own container "${selfId}" (${err.message}); set WORCA_CONTROLLER_CONTAINER to its name or id, and run the controller as a container`); }
  const template = {
    mounts: workerMountsFrom(self.Mounts),
    network: await pickWorkerNetwork({ attached: Object.keys(self.NetworkSettings?.Networks || {}), inspect: (n) => docker.network(n), override: env.WORCA_WORKER_NETWORK, log }),
    user: String(env.WORCA_WORKER_USER || self.Config?.User || ''),
    image: self.Config?.Image || null,
  };
  // Which installation these workers belong to: the Compose project (or WORCA_INSTANCE_ID), so a second worca stack
  // on the same Docker host never lists or reaps them.
  const instance = String(env.WORCA_INSTANCE_ID || self.Config?.Labels?.['com.docker.compose.project'] || String(self.Name || selfId).replace(/^\//, '')).trim() || 'worca';
  const pattern = String(env.WORCA_WORKER_IMAGE || '').trim() || null;
  const mem = parseBytes(env.WORCA_WORKER_MEM || '4g');
  const cpus = Number(env.WORCA_WORKER_CPUS || 2);
  const pidsLimit = Number(env.WORCA_WORKER_PIDS || 1024);
  const live = new Map();   // workerId -> handle
  log(`docker ${version?.Version || '?'} via ${describeEndpoint(env)}; controller ${self.Name || selfId} on network ${template.network}, ${template.mounts.length} mount(s) replicated, user ${template.user || '(image default)'}`);

  const engineFor = () => defaultWorkerEngine(env);

  return {
    kind: 'docker',
    template,
    /** What tells this scope's workers apart: the image an engine runs on (one image for every engine = one worker). */
    async classFor(engine) { return workerImageFor(engine || await engineFor(), { pattern, controllerImage: template.image }) || 'none'; },
    async start({ scope, scopeKey, workerId, token, controllerUrl, idleMs, engine: asked = null }) {
      const engine = asked || await engineFor();   // the spawn's engine (the run's, the chat's); the install default only when none is named
      const image = workerImageFor(engine, { pattern, controllerImage: template.image });
      if (!image) throw new Error('no worker image: set WORCA_WORKER_IMAGE');
      const name = `worca-worker-${scope.kind}-${String(scope.id).replace(/[^A-Za-z0-9_.-]/g, '-').slice(0, 40)}-${workerId.slice(-8)}`;
      const spec = {
        Image: image, Cmd: ['worca', 'worker'], Hostname: name.slice(0, 63),
        ...(template.user ? { User: template.user } : {}),
        Env: Object.entries(workerEnvFor({ scopeKey, workerId, token, controllerUrl, idleMs, engine, env })).map(([k, v]) => `${k}=${v}`),
        Labels: { [CONTROLLER_LABEL]: bootId, [INSTANCE_LABEL]: instance, [SCOPE_LABEL]: scopeKey, [WORKER_LABEL]: workerId, 'worca.engine': engine },
        HostConfig: {
          Mounts: template.mounts, NetworkMode: template.network,
          Tmpfs: { '/tmp': 'rw,size=2g' }, SecurityOpt: ['no-new-privileges:true'], CapDrop: ['ALL'],
          PidsLimit: pidsLimit, ...(mem ? { Memory: mem } : {}), ...(Number.isFinite(cpus) && cpus > 0 ? { NanoCpus: Math.round(cpus * 1e9) } : {}),
          RestartPolicy: { Name: 'no' }, Init: true,
        },
      };
      let created;
      try { created = await docker.create(name, spec); } catch (err) {
        if (err.status === 404) throw new Error(`worker image ${image} is not present on this host (pull it, or set WORCA_WORKER_IMAGE)`);
        throw new Error(`cannot create worker container: ${err.message}`);
      }
      const id = created?.Id;
      const handle = { workerId, id, name, scopeKey, image, engine, startedAt: Date.now() };
      try {
        await docker.start(id);
        // A container that dies at once (a bad image, a missing binary): say why now, not after the start timeout.
        const deadline = Date.now() + START_WAIT_MS;
        for (;;) {
          const info = await docker.inspect(id);
          if (info?.State?.Running) break;
          if (info?.State?.Status === 'exited' || info?.State?.Dead) {
            const tail = await docker.logsTail(id, 50);
            throw new Error(`worker container exited at once (code ${info.State.ExitCode}) from ${image}${tail ? `:\n${tail.trim()}` : ''}`);
          }
          if (Date.now() > deadline) break;
          await new Promise((r) => setTimeout(r, 250));
        }
      } catch (err) {
        await docker.remove(id).catch(() => {});
        throw err;
      }
      live.set(workerId, handle);
      log(`started ${name} (${image}) for ${scopeKey}`);
      return handle;
    },
    async remove(handle, { reason } = {}) {
      if (!handle?.id) return;
      live.delete(handle.workerId);
      try { await docker.remove(handle.id); log(`removed ${handle.name || handle.id} (${reason || 'released'})`); }
      catch (err) { if (err.status !== 404) throw err; }
    },
    async listWorkers() {
      const rows = await docker.list([`${INSTANCE_LABEL}=${instance}`]);   // this installation's only
      return (rows || []).map((c) => ({ id: c.Id, workerId: c.Labels?.[WORKER_LABEL] || c.Id.slice(0, 12), name: (c.Names || [])[0]?.replace(/^\//, '') || c.Id.slice(0, 12), scopeKey: c.Labels?.[SCOPE_LABEL] || null, image: c.Image,
        bootId: c.Labels?.[CONTROLLER_LABEL] || null, own: c.Labels?.[CONTROLLER_LABEL] === bootId }));
    },
    describe() { return { kind: 'docker', endpoint: describeEndpoint(env), instance, network: template.network, image: pattern || workerImageFor('<engine>', { pattern: null, controllerImage: template.image }), live: live.size }; },
  };
}

function describeEndpoint(env) {
  try { const e = dockerEndpoint(env); return e.socketPath ? `unix://${e.socketPath}` : `tcp://${e.host}:${e.port}`; } catch { return 'docker'; }
}
