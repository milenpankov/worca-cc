# AGENTS.md

Instructions for coding agents (Claude Code, Codex, Copilot, Cursor, …) working on this repository. Full human-facing detail lives in [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/](docs/); this file is the short version.

## Branch lines

- All work targets **`dev`** (Worca 1.x, pure Node, `@worca/app`). Branch from `dev`, open PRs with base `dev`.
- `master` is the separate worca-cc 0.x line (Python + Node dashboard). Never port or cherry-pick commits, skills or docs between the two.

## Setup

- Node **>= 22.19.0** (`.nvmrc` pins 24; `nvm use`). `npm install`. The `claude` CLI is only needed for real (non-mock) runs.
- **No build step**: plain Node ESM (`.mjs`) backend, vanilla HTML/CSS/JS in `ui/public/`. Runtime deps are `express` + `ws` only.

## Testing

- While working: `npm run test:related` (runs the test files your uncommitted changes can affect; add `-- --base origin/dev` for the whole branch).
- Before handing over: full `npm test` (node:test, isolated `WORCA_HOME`; no network, Claude or API keys needed). One full suite per machine at a time — a second one waits; `WORCA_TEST_NO_LOCK=1` skips the wait.
- Tiers: `npm run test:fast` / `npm run test:slow` (assignment in `test/tiers.json`).
- Smoke (offline, mock): `npm run smoke`, `npm run smoke:workspace`, `npm run smoke:plugin` — run the relevant one when touching engine, workspace or plugin paths.
- Container: `npm run docker:build` then `npm run docker:smoke` when touching `docker/**`, package.json `files`, or anything the CLI needs at runtime (see [docs/docker.md](docs/docker.md)).
- Say which of these you ran in the PR description.

## Mock mode and dev state

- `WORCA_MOCK=1` (or `--mock`) runs the whole pipeline offline with a deterministic mock that still writes real artifacts. Use it for UI/engine work and bug repros.
- Keep dev state out of your real home: set `WORCA_HOME` to a throwaway dir. For a second server also set a separate `HOME` (settings live in `$HOME/.worca-cc`).
- **Never `npm start` from a run's own checkout** (`~/.worca-cc/runs/<id>/…` or `<project>/.worca-cc/worktrees/<id>`) — Worca deletes those folders and the server would lose its code. Running tests there is fine. To try such a branch, `git worktree add --detach ../try-X X` and start it from there (see CONTRIBUTING.md).

## Project map

- `src/core/` — engine: protocol, SQLite store, artifacts, preflight, claude-runner, phases, orchestrator.
- `src/cli/` — `worca-cc.mjs`, the `worca` bin.
- `src/shared/` — code shared across engine, UI and CLI (feature flags, connections, cost, …).
- `src/broker/` — `worca broker`, the credential broker (proxy + key page).
- `ui/` — `server.mjs` (express + ws) and `public/` (vanilla single-page UI).
- `agents/` — the **shipped pipeline agent set**: prompt (`worca-cc-<role>.md`) + `<key>.meta.json` per agent.
- `skills/` — installable skills (e.g. `skills/worca/SKILL.md`).
- `scripts/` — built-in script cards: `<key>.meta.json` + the program it names.
- `tools/` — installer, test runner, smoke runners, docker build/smoke, CDP verifiers.
- `test/` — node:test suite (`test/*.mjs`, tiers in `test/tiers.json`).
- `docs/` — user and contributor docs (`docs/RELEASING.md`, `docs/ui-levels.md`, …).
- `docs-site/` — docs website; **separate package with its own lockfile**.
- `extension/` — browser extension sources; `e2e/` — Playwright specs for it (`e2e/extension/`).
- `plugins/` — bundled Worca plugins (task-source connectors, MCP bundles, …).
- `assets/` — bundled asset kits (deck/promo).

**Trap:** `agents/` is not `.claude/agents/`. The latter are Claude Code agents for developing this repo; pipeline behaviour changes belong in `agents/`.

## Rules

- **UI levels:** every user-visible element added to `ui/public/` needs a level (Simple/Advanced/Expert). See [docs/ui-levels.md](docs/ui-levels.md); update its catalogue. `test/ui-levels.test.mjs` fails on a nav item, Settings tab/card or detail tab without one.
- **Feature flags:** experimental UI ships behind a flag, off by default: an entry in `src/shared/flags/registry.mjs` (owner + review date ≤ 90 days out), `data-flag="key"` / `flagOn('key')` in UI, `requireFlag('key')` on routes. See [docs/feature-flags.md](docs/feature-flags.md).
- **Pull requests:** merge commits only (no squash/rebase-merge). Once a PR is under review, never rebase or force-push — merge `dev` into the branch instead. Stacked PRs: base the child on the parent branch, retarget to `dev` after the parent merges.
- **Contributor terms:** no `Signed-off-by` or `git commit -s` needed; opening a PR accepts the [DCO](DCO). AI-assisted contributions are fine; the PR opener is responsible. Don't submit code you may not submit.
- **Releases** are tag-driven (`worca-app-v<version>`); never run `npm publish` by hand. See [docs/RELEASING.md](docs/RELEASING.md).
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md).
