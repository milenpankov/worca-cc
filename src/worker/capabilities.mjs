// src/worker/capabilities.mjs
// What a worker can run: for each engine Worca knows, the binary found on this worker's PATH
// (honouring the WORCA_<ENGINE>_BIN overrides the adapters honour) and its `--version`. Sent
// in the hello; the controller reads it for its own checks and shows it in a workers list.
// No sign-in probes here: credentials reach a worker per spawn.
import { execFile } from 'node:child_process';
import { statSync } from 'node:fs';
import { delimiter, isAbsolute, join } from 'node:path';
import { ENGINE_CLIS, DEFAULT_ENGINE_BINS } from '../core/engines/cli-specs.mjs';

/** The binary name each engine's adapter spawns when nothing overrides it (cli-specs.mjs). */
export const DEFAULT_BINS = DEFAULT_ENGINE_BINS;

const isFile = (p) => { try { return statSync(p).isFile(); } catch { return false; } };

/** The absolute path a bare command resolves to on PATH (or the path itself when absolute); null when absent. */
export function findOnPath(bin, pathVar = process.env.PATH || '', platform = process.platform) {
  if (!bin) return null;
  if (isAbsolute(bin)) return isFile(bin) ? bin : null;
  if (bin.includes('/') || bin.includes('\\')) return null;
  const exts = platform === 'win32' ? ['.exe', '.cmd', '.bat', ''] : [''];
  for (const dir of String(pathVar).split(delimiter)) {
    if (!dir) continue;
    for (const ext of exts) { const p = join(dir, bin + ext); if (isFile(p)) return p; }
  }
  return null;
}

const versionOf = (bin, timeoutMs) => new Promise((resolve) => {
  execFile(bin, ['--version'], { timeout: timeoutMs, windowsHide: true, maxBuffer: 64 * 1024 }, (err, stdout) => {
    if (err) return resolve(null);
    const line = String(stdout || '').split(/\r?\n/).map((l) => l.trim()).find(Boolean) || '';
    resolve(line.slice(0, 256) || null);
  });
});

/**
 * Probe the engines. `engines` limits the probe (default: every engine Worca knows; `[]` probes
 * none, for tests). Each engine maps to `{ bin, version }` or null when its binary is absent.
 */
export async function probeCapabilities({ engines = Object.keys(ENGINE_CLIS), env = process.env, timeoutMs = 5000, platform = process.platform } = {}) {
  const found = {};
  await Promise.all(engines.map(async (name) => {
    const spec = ENGINE_CLIS[name];
    const configured = (spec?.envVar && env[spec.envVar]) || (name === 'claude' && env.ORCH_CLAUDE_BIN) || DEFAULT_BINS[name] || name;
    const bin = findOnPath(configured, env.PATH, platform);
    found[name] = bin ? { bin, version: await versionOf(bin, timeoutMs) } : null;
  }));
  return { engines: found, platform, node: process.version, user: env.USER || env.LOGNAME || null };
}
