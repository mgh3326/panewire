# panewire

Panewire is a daemon, hub and CLI for agent placement, launch, job lifecycle, pane observation and verified prompt delivery. The launcher packages and job ledger formerly owned by runtug are now part of panewire.

## Lane routing

The hub defaults informational events to a durable per-lane inbox. See
[quiet delivery](docs/quiet-delivery.md) for `emit --deliver`, inbox commands,
policy overrides, digest windows, and display-only Claude Code examples. The
inbox is a SQLite file by default or Postgres when `PANEWIRE_HUB_DATABASE_URL`
is set; see [hub inbox on Postgres](docs/hub-postgres.md).

The hub can relay terminal and escalation records to a small hierarchy of
operator-owned lanes. Start it with `--lanes /etc/panewire/lanes.json`; the
file is reloaded for every relay decision. `job.completed` goes to its
`owner_lane`; `job.escalate` and `job.joined` go to that lane's `parent`.
`--report-relay-routes` and a legacy `routes` key remain supported while lane
files are migrated. See [the lane contract](docs/r19-lanes.md) for the exact
file and acknowledgement contract.

Idle-wake can turn a real `working -> idle|done` transition into one durable
direct notification for the pane's owner after a default 60-second settle. It
does not decide job completion or alter job lifecycle state. See the
[idle-wake contract and isolated E2E runbook](docs/idle-wake.md).

Claude seats start under their job id (`--name`). A trial, off by default, can
deliver a lane's inbox wake line to its Claude Code session as a session message
instead of typing it; see [Claude session names and session messages](docs/claude-session-message.md).

Escalation `question` text is capped at 240 characters for hub relay; read the
full question from the referenced events file (or its explicit report file).

Relay is independent of hub job registration: a `job.completed`, `job.escalate`
or `job.joined` record from an authenticated node is relayed on its own merits,
even for a job the hub never saw in a heartbeat. Epoch fencing decides
redispatch, not delivery; unregistered terminal records are counted as
`unfenced_completions`, never as `unknown_messages`.

R2 adds `prompt` delivery with target preflight, identity checks, submission proof, optional uptake confirmation, and SQLite delivery audit records.

## Scope

Pane observation and delivery commands include:

- `wait` for a durable inbox file or a stable herdr agent state.
- `prompt` for verified delivery to an already-running pane.
- A local SQLite log of observed herdr and inbox events.

The durable inbox, job ledger and verified pane operations preserve evidence as the hub coordinates placement and lifecycle.

Spawn requests are submitted through `panewire spawn` and `work`; the hub coordinates placement and execution on the selected node. Panewire also observes sessions and delivers messages.

See [docs/design.md](docs/design.md) for the complete contract and acceptance criteria.

What the node relay does per harness when it types a lane event into a worker pane, and when it may report it delivered, is in [relay delivery per harness](docs/relay-harness-delivery.md).

## Installation and use

R1 ships one executable, `panewire`, with a `daemon` subcommand. This avoids two separately versioned Go artifacts while keeping the launchd entry point explicit (`panewire daemon`).

The daemon uses `modernc.org/sqlite` to keep the build cgo-free and `log/slog` for structured startup/reconnect diagnostics. Inbox watching uses fsnotify with one registration per directory: macOS kqueue is not recursive, so the watcher walks the root and adds newly created directories immediately. This is simpler to operate and test on the required macOS runner than relying on a separate FSEvents backend.

```sh
go install github.com/mgh3326/panewire/cmd/panewire@latest
panewire daemon --herdr-socket "$HOME/.config/herdr/herdr.sock" \
  --db "$HOME/Library/Application Support/panewire/panewire.sqlite3" \
  --inbox-root "$HOME/work/herdr-inbox"
```

Install [deploy/dev.panewire.panewired.plist](deploy/dev.panewire.panewired.plist) as `~/Library/LaunchAgents/dev.panewire.panewired.plist` after replacing its user-specific paths. It uses the same `panewire` executable and restarts on exit.

Release installs should pin a tag and stamp the same version the hub sees, e.g.
`go install -ldflags "-X main.version=<tag>" github.com/mgh3326/panewire/cmd/panewire@<tag>`.
Builds from a checkout get that value from `scripts/dist-version.sh`, which
prints `git describe --always --dirty` and refuses anything outside the hub
version pattern.

```sh
panewire wait --file "$HOME/work/herdr-inbox/jobs/example/report.md" --settle 2s --timeout 10m
panewire wait --agent rob1320-r1 --status idle --settle 2s --timeout 10m
panewire prompt --from 'sender (role, pane-id)' --to orch-mock \
  --file "$HOME/work/herdr-inbox/jobs/example/prompt.md" --uptake status-transition
```

Prompt files begin with recipient metadata such as `expect: name=orch-mock cwd=/work`; `name=` or `cwd=` is required. `cwd=` uses exact absolute-path matching. Panewire records the prompt SHA-256, path, target identity, revisions, and evidence states, but does not store prompt text by default. To opt in, pass `--store-prompt-body` to `panewire prompt` or configure the daemon with `--store-prompt-body`; the body is then kept only in the separate `delivery_bodies` table.

The CLI never falls back to herdr directly. If the daemon socket is absent it exits 4. `PANEWIRE_SOCKET` is available for tests and non-default local installations.

Quota readings, refreshes, and observations use `panewire quota`; see [the quota command contract](docs/quota.md) and [exhausted API credit](docs/placement.md#exhausted-api-credit). The bench catalog, its grades and Artificial Analysis annotations use `panewire catalog`; see [the catalog command contract](docs/catalog.md). Reps, manual credit, and automatic model outcomes use `panewire reps` and `panewire manual`; see [reps and outcomes](docs/reps.md).

## Status

R2: prompt target safety, verified submission/uptake, privacy-preserving delivery audit, schema guard, events, inbox watcher, and wait. Live prompt smoke remains prohibited in the fixture test job; use an operator-approved scratch pane separately.

## R8 hub: presence, checks, and notification

`panewire hub` is the optional, always-on live channel for presence and
operator event push.

- The hub evaluates node presence and local heartbeat checks, and can send
  optional Telegram incident/recovery notification.
- The hub is useful only while both sides are connected. `panewired` retries
  the connection as a non-blocking side channel.

### Transport: outbound-only nodes, tailnet + Cloudflare

Nodes always initiate the WebSocket, so laptops and company-managed machines
need no inbound SSH, port forwarding, or publicly reachable service. The hub
may bind both its loopback address for Cloudflare Tunnel and its tailnet
address with the same mux and token file:

```sh
panewire hub --hub-auth /etc/panewire/hub.env \
  --listen 127.0.0.1:9377 --listen 100.64.0.1:9377
```

`100.64.0.1` is documentation-only CGNAT example space; never publish a real
tailnet address, token, or pane ID. A node lists its preferred tailnet URL
first and its Cloudflare URL second (each flag may also contain a comma list):

```sh
panewire daemon --hub-url wss://100.64.0.1:9377 --hub-url wss://hub.example.invalid \
  --hub-token-env /etc/panewire/node.env --hub-cf-env /etc/panewire/cf.env
```

It attempts URLs in order, backs off only after all fail, and every
`HUB_PREFER_RETRY` (default `10m`) probes a higher-priority route before
switching. Cloudflare Access headers apply only to non-tailnet URLs.

Quota collection at the hub is retired: nodes no longer run a quota tool
and a legacy hub's `quota.request` is answered `unsupported` so a
mixed-version pair completes the round trip instead of timing out.

By default every authenticated node is watched for presence and heartbeat-check
alerts. Pass `--alert-nodes machine-a,machine-b` on the hub to limit alerts to
those authenticated machine IDs; other nodes remain visible as
`presence-only` in `hub-status` and `/v1/nodes`.

See [docs/hub-r6.md](docs/hub-r6.md) for token-file formats, CLI examples, the
closed WebSocket vocabulary, and the NCP/systemd + tunnel deployment runbook.
How the hub stops on SIGTERM and how a node redials: [docs/hub-drain.md](docs/hub-drain.md).
[docs/runbooks/hub-log-access.md](docs/runbooks/hub-log-access.md) covers how a
non-root operator reads hub logs on NCP.

A hub restart does not fail the idempotent calls: inbox commands, the spawn
submit and the node's repeatable hub calls are sent again through a transport
error, 502 or 503 for up to a minute. See
[hub calls that ride through a restart](docs/hub-uninterrupted-calls.md).

Operator tools reach a node's herdr panes and git worktrees through the hub
instead of ssh: `GET /v1/nodes/agents` lists every node's agents, and
`POST /v1/nodes/{machine}/command` runs one of a closed set of ops
(`pane.close` closes only an exact re-read match). See
[docs/node-commands.md](docs/node-commands.md). A service such as the reaper
uses its own scoped token instead of the operator's; see
[docs/hub-scopes.md](docs/hub-scopes.md). `pane.read` and `job.done` admit only
such a service; `panewire node pane-read` and `panewire node job-done` call them.

`panewire pr status` and `panewire pr findings` read a PR's gate state and open
review findings from handoffkeep's change record (mergewake's last decision when
hk has none it can trust, or with `--fresh`) through the `pr.status` node op, so a worker needs no GitHub request or token
for them; see [docs/pr-status.md](docs/pr-status.md).

The hub also keeps the latest fleet state, meaning per-account quota, per-host
load and publisher liveness. It serves that state as a snapshot
(`GET /v1/state`), so panewire reads current state instead of measuring it on
every spawn. See [docs/fleet-state.md](docs/fleet-state.md) for the
message contract, the freshness rule, rollout and rollback.

The node closes a worker's pane once its hk task is merged, done or dropped, or the job was closed, and the pane has stayed not working past a grace; a worker that reported READY keeps its pane while its PR is open. See [the task pane sweep](docs/pane-sweep.md). Job directories hk has confirmed can expire after a TTL, off by default; see [the job dir TTL](docs/job-dir-ttl.md).

Harness conversation references in job events are described in the [job record contract](docs/jobs-resilience.md#harness-session-identity).

## Hub job inbox compatibility

For node job heartbeats, the local inbox contract is the arbiter envelope as
the source of truth, with the prior flat event shape retained for compatibility.
The default `PANEWIRE_JOB_ACTIVE_MAX_AGE` is **72h**; see
[the job resilience contract](docs/jobs-resilience.md#node-inbox-event-contract)
for the event-field table and selection rules.

### Completion commands (`panewire job`)

`panewire job done|escalate|joined` owns the job record contract once held
by the retired spawner, and takes the same arguments. It leaves the
contract's bytes behind — the event record, the `panewire emit` socket
request, the handoffkeep report upload, the `OK` line, the exit status and
the `emit-failures.log` / `completion-suppressed.log` lines — pinned by
[testdata/job_golden](testdata/job_golden) (see `job_golden_test.go`). Relay
text is still normalized once, on the node.

`panewire job probe` prints `panewire-job/1`, the delegation answer a caller
can probe before handing the three commands over.

### Owner close (`panewire job close`)

```sh
panewire job close JOB --lane LANE --outcome completed|abandoned|superseded --reason TEXT [--operator-override]
```

A job whose worker ended without `panewire job done` has no terminal event, so no
reaper selects its pane. The owner lane can declare the job ended with this
command. See [job close](docs/job-close.md) for ownership, remote routing,
refusals, and the record contract.

### A lane's own jobs on other hosts (`panewire jobs mine`, `jobs close`)

```sh
panewire jobs mine --lane LANE [--json]
panewire jobs close --lane LANE --job JOB [--force] [--json]
```

A session that spawned panes on another host lists and closes them through the
local node daemon and its own node credential, with no operator token. `close`
asks the job's machine to close its pane with reap's safety rules and retires
the lane route that pointed at it. See [a lane's own jobs](docs/jobs-own.md)
for the authorization rule, the refusals, and the rollout order.

## Agent launch and lifecycle

Use `panewire spawn` with the existing task, role, quota and landing checks, or
`panewire work TASK_ID` for task-derived defaults. The delegated lead seat
(`panewire lead`, `--role builder`) is retired and refused: lead is an
operator-opened session that works through subagents.
Explicit flags override preset defaults. `--dry-run` stays read-only and shows
the same admission and placement plan as the equivalent long command.

```sh
panewire work 42 --dry-run
panewire spawn -c /work/project.feature -p /brief.md -l worker-a --task 42 --t T1 --owner caller-lane --report /jobs/worker-a/report.md --dry-run
panewire done worker-a --report /jobs/worker-a/report.md
panewire reap --lane caller-lane
```

Hosts and workspaces remain configuration-driven. Short host aliases resolve
to configured machine ids; workspace labels resolve on the executing host.
Without `-w`, the executing host's configured workspace is used. Confirmed
pre-landing failures permit a same-id retry on that host, while live duplicates
still refuse with exit 74. Refusals include the corrective action.

See [spawn configuration and exit statuses](docs/config.md) and
[import evidence](docs/spawn-import.md). This replaces runtug; no compatibility
binary or forwarding shim is installed. The remaining hub orchestration and
caller changes ship in the coordinated cutover before old binaries are removed.

## Delegation

`panewire delegate --role reader --prompt /brief.md --cwd /work/project`
assigns a bounded reading task through the same quota and role policy. Use
`--headless` when the selected harness supports it, `--patch` for an isolated
scratch checkout, or `--panel` for independent readers. See
[Delegation configuration](docs/config.md) for roles, results and exit codes.


Spawn execution now belongs to the hub. `panewire spawn` and `work TASK` print ACCEPTED after a durable reservation; this is not landing proof. `--dry-run` stays synchronous and `--wait` preserves the job-completion contract. See [durable spawn operations](docs/hub-spawn-operations.md) for replay, timeout, recovery and Sentry behavior, and [observability](docs/observability.md) for the relay, held-reservation and job-result fleet events and their Sentry fingerprints.
