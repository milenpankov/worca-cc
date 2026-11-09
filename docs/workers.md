# Workers: agents in their own containers

By default Worca is one process: the orchestrator spawns every engine CLI (`claude`, `codex`,
…) and every script node as a local child. With a **remote executor** the same process — the
**controller** — keeps the scheduler, the database, the UI and the questions, and the processes
run in **worker containers**: one per run, one per chat thread, created when the first process is
needed and removed when the run ends or the thread has been idle. Nothing but the controller
runs while Worca is idle; ten concurrent runs are ten worker containers. A run whose processes
need two worker images — its agents on Cursor, its title and PR description on Claude Code —
gets one worker per image, and they end together.

```
             UI :4317 (people)               workers :4319 (container network only, token auth)
   ┌──────────────────────────────┐   ws    ┌──────────────────────────────┐
   │ controller (worca ui)        │◄───────►│ worker  run:<id>             │
   │ orchestrator · scheduler     │  exec/  │ worca worker → claude, …     │
   │ SQLite · worker registry     │  out/   └──────────────────────────────┘
   │ backend: docker | kubernetes │  exit   ┌──────────────────────────────┐
   └──────────────────────────────┘◄───────►│ worker  chat:<thread>        │
          │ shared volume: <worcaHome> (store, runs + worktrees, ask, plugins, worker-homes) + projects
          └──────────────── mounted at the same paths in every container
```

## How it works

- **One seam.** Every engine adapter spawns through `superviseSpawn` (`src/core/engines/spawn.mjs`),
  script nodes through `spawnScript` (`src/core/graph/script-runner.mjs`). Both ask the executor
  (`src/core/executor/`) for the current scope's worker session; with none (the default, every
  CLI run, every test) they spawn locally, line for line as before.
- **Scopes.** A run's loop and a chat turn each carry an executor scope (`run:<id>`, `chat:<id>`)
  in the async context, the way the acting person is carried for billing. A spawn outside any
  scope (a title, a PR description, a model test) stays on the controller.
- **Workers.** `worca worker` connects *outbound* to the controller's worker port with a
  one-time token, says which scope it serves and which engines it found, and executes what it is
  sent: the complete argv, cwd and env, stdin, signals. Output comes back line by line with flow
  control. A worker never opens the database (`WORCA_NO_DB=1` in its image).
- **Which image.** Every engine spawn names its engine, and the backend resolves it to a worker
  image (`…-claude`, `…-codex`, or one `…-all` for every engine). A scope's workers are told apart
  by that image: the same image is the same worker, another image is another worker. A script node
  names no engine and runs on the worker of the run's own engine. A worker that lacks the binary
  it is asked for refuses the spawn, naming the image setting to fix; the run pauses on it.
- **Starting and stopping.** Callers that need the same worker while it starts all wait on the one
  start; if it fails, each of them gets the failure. Stopping or pausing a run ends its wait at
  once, and a container or pod that arrives after nobody waits for it is removed.
- **Files.** Worker and controller see the same files at the same paths: the run's checkout,
  the pipeline's outputs and verdicts, staged prompts (`<worcaHome>/tmp/stage`), and the
  worker's HOME (`<worcaHome>/worker-homes/<kind>/<id>`), where the engine keeps the run's
  sessions — so a replacement worker after a pause or a lost container can `--resume` them.
- **Loss.** When a worker's connection drops it kills its children and exits; the controller
  reports the step as a network failure, which the run's failure policy retries on a fresh worker.
  There is no reconnect.
- **Boot.** A controller removes every worker a previous controller left (their tokens died with
  it), the worker homes of runs that are not paused, and staged spawn folders older than itself
  (each spawn removes its own; what a crashed controller left is nobody's).

## Configuration

| Variable | Where | Meaning |
| --- | --- | --- |
| `WORCA_EXECUTOR` | controller | `local` (default), `docker`, `kubernetes` |
| `WORCA_EXECUTOR_BACKEND=in-process` | controller | the worker runs inside the server process over a real socket, no container: for trying the path on one machine, and for tests |
| `WORCA_WORKER_PORT` | controller | the worker listener (default 4319); never publish it to people |
| `WORCA_CONTROLLER_URL` | controller | the URL workers are told to connect to (`ws://<service>:4319`). **Required** with `docker` and `kubernetes` (the Compose overlay and the chart set it): a worker in another container cannot dial the controller's loopback, and the server refuses to start without it |
| `WORCA_WORKER_MAX` | controller | containers at once (default 20); runs are also bounded by Settings › Runs |
| `WORCA_WORKER_ENGINE` | controller | the engine a worker gets when a spawn names none and the scope has none (default: the `run.engine` setting) |
| `WORCA_WORKER_CHAT_IDLE_MS` | controller | a chat worker leaves after this long with nothing to run (default 15 min) |
| `WORCA_WORKER_START_TIMEOUT_MS` | controller | how long a worker may take to connect (default 2 min) |
| `WORCA_DB_DIR` | controller | the database's own directory, off the shared volume (SQLite's WAL needs a local filesystem) |
| `WORCA_CONTROLLER_URL`, `WORCA_WORKER_TOKEN`, `WORCA_WORKER_SCOPE`, `WORCA_WORKER_ID` | worker | the assignment, set by the controller's backend |
| `WORCA_WORKER_HOME`, `WORCA_WORKER_IDLE_MS`, `WORCA_WORKER_RELAY_PORT` | worker | optional |

`/api/health` carries an `executor` object under a remote executor.

A worker **container** inherits the same few controller settings under both backends: `TZ`, `WORCA_BROKER_URL`,
`WORCA_MOCK`, the corporate CA (`WORCA_CA_CERT_FILE`, `NODE_EXTRA_CA_CERTS`; the worker's entrypoint builds its
own bundle from it), `WORCA_SIGTERM_GRACE_MS`, the `WORCA_<ENGINE>_BIN` overrides, the git identity, and a proxy
another container can reach, with the controller's and the broker's hosts added to `NO_PROXY`. A loopback proxy
is not passed on, and no credential ever is: those reach an agent per spawn
([Credentials on workers](#credentials-on-workers)).

## Credentials on workers

A worker's HOME (`<worcaHome>/worker-homes/<kind>/<id>`) is created for one run or one chat thread
and removed afterwards, so nothing an engine stores there outlives the worker. Two things reach
an agent on a worker:

- **The controller's environment.** The controller composes every spawn's environment and sends
  it with the exec (minus its own secrets: the worker token, the broker secret, the GitHub
  tokens it keeps for pushes and pull requests). A credential set where the controller runs —
  `.env` on Compose, the controller's Secret on Kubernetes — is therefore in the agent's
  environment on the worker too, unless the run's guardrail set scrubs the environment.
- **The worker's own environment.** On Kubernetes, `worker.credentialsSecret` is the worker pod's
  `envFrom`; on Compose the controller's `.env` is the only source.

What that means per engine:

| Engine | Works on a worker | Does not persist on a worker |
| --- | --- | --- |
| Claude Code | `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`; the [credential broker](credential-broker.md); on Compose also the interactive login (`worca container login`), which workers copy once from the controller's `claude-config` volume (`/seed/claude`) | — |
| Codex | `OPENAI_API_KEY`, or an OpenAI-compatible endpoint with its key set up on the Models page | the ChatGPT sign-in (`codex login`) |
| Gemini CLI | `GEMINI_API_KEY` (or Vertex AI's variables) | the Google account sign-in |
| Qwen Code | a provider in the environment (`OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_MODEL`) | its OAuth sign-in |
| GitHub Copilot CLI | `COPILOT_GITHUB_TOKEN` | the device-code sign-in |
| Cursor | `CURSOR_API_KEY` | `cursor-agent login` |
| Goose | the provider's key (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`, …) with the provider and model named as [models.md](models.md#goose) describes | `goose configure` |
| OpenCode | the provider's key in the environment | `opencode auth login` |

Each engine's section in [models.md](models.md) has the exact variables and the models to pick.
Seeding other engines' sign-ins the way Claude's is seeded is not implemented. The Compose option
of a Claude key as a compose secret read through `apiKeyHelper` is for the one-container box; on
workers use the token or the broker.

## What is different under a remote executor

- **Chat tools** run on the controller: the turn's MCP child posts each line to the worker's
  loopback relay shim, which forwards it over the worker socket (the same path agent isolation
  uses on one machine).
- **Bridged models** (an entry with `upstream`, served by the controller's loopback bridge) are
  refused at run and turn start: the bridge is not reachable from another container yet.
- **The in-process outbound policy proxy** (`WORCA_EGRESS_MODE`) does not reach workers; use the
  egress sidecar (`docker/compose.egress.yml`) or a cluster network policy. A proxy URL that is
  not loopback (`http://egress:3128`) rides through to workers unchanged.
- **The credential broker** must be reachable from the worker network (`WORCA_BROKER_URL` names
  a service, not loopback).
- **Agent isolation** (`WORCA_AGENT_USER`, sudo) is not used on a worker: the container is the isolation.
- **Container-local paths.** The controller composes each spawn's environment in its own container. On the
  worker, that container's own CA bundle and cache directories replace the controller's, and a CA file path that
  does not exist there is dropped. A corporate CA must be mounted in the workers too (`WORCA_CA_CERT_FILE`; the
  Docker backend replicates the controller's mounts, on Kubernetes add it to the worker pod template).

## Backends

- `docker` — the controller starts containers through the Docker Engine, replicating its own
  container's mounts, network and user so paths match. See `docs/docker.md`.
- `kubernetes` — the controller creates a Pod per worker from a template. See `docs/kubernetes.md`.
