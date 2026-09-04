# panewire

Panewire is a local daemon and CLI for watching agent panes, waiting on stable state, and delivering prompts with verified evidence.

## Lane routing

The hub can relay terminal and escalation records to a small hierarchy of
operator-owned lanes. Start it with `--lanes /etc/panewire/lanes.json`; the
file is reloaded for every relay decision. `job.completed` goes to its
`owner_lane`; `job.escalate` and `job.joined` go to that lane's `parent`.
`--report-relay-routes` and a legacy `routes` key remain supported while lane
files are migrated. See [the lane contract](docs/r19-lanes.md) for the exact
file and acknowledgement contract.

R2 adds `prompt` delivery with target preflight, identity checks, submission proof, optional uptake confirmation, and SQLite delivery audit records.

## Scope

The first usable slice is deliberately small:

- `wait` for a durable inbox file or a stable herdr agent state.
- `prompt` for verified delivery to an already-running pane.
- A local SQLite log of observed herdr and inbox events.

The file inbox and pane injection remain the system of record. panewire automates the surrounding watching, checking, waiting, and recording procedures; it does not turn session-to-session coordination into server communication.

Spawning remains the responsibility of `wrk`, including scopefuel gating, arbiter decisions, and landed verification. panewire is a messaging and monitoring layer for sessions that are already alive.

See [docs/design.md](docs/design.md) for the complete contract and acceptance criteria.

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

```sh
panewire wait --file "$HOME/work/herdr-inbox/jobs/example/report.md" --settle 2s --timeout 10m
panewire wait --agent rob1320-r1 --status idle --settle 2s --timeout 10m
panewire prompt --from 'sender (role, pane-id)' --to orch-mock \
  --file "$HOME/work/herdr-inbox/jobs/example/prompt.md" --uptake status-transition
```

Prompt files begin with recipient metadata such as `expect: name=orch-mock cwd=/work`; `name=` or `cwd=` is required. `cwd=` uses exact absolute-path matching. Panewire records the prompt SHA-256, path, target identity, revisions, and evidence states, but does not store prompt text by default. To opt in, pass `--store-prompt-body` to `panewire prompt` or configure the daemon with `--store-prompt-body`; the body is then kept only in the separate `delivery_bodies` table.

The CLI never falls back to herdr directly. If the daemon socket is absent it exits 4. `PANEWIRE_SOCKET` is available for tests and non-default local installations.

## Stage 2 offline R1

`panewire submit` writes a metadata-only stage 2 outbox record; it does not
open a remote connection or load a credential file. The stage 2 publisher and
receiver loop is an explicit `Stage2.Enabled` daemon configuration gate and is
off by default, so the existing launchd invocation remains a stage 1 daemon.

```sh
panewire submit --db /safe/local/stage2.sqlite3 --file ./brief.md \
  --from-machine sender --destination-machine receiver --namespace jobs \
  --logical-path jobs/example/brief.md --classification personal_non_company
```

The R1 implementation is fixture-only: its Supabase adapter is exercised
against an in-process HTTP fake. Live Supabase credentials and smoke testing
are intentionally deferred to R2.

### Credential rotation and optional receiving `wrk` gate

When a stage 2 daemon refreshes a Supabase session, it atomically rewrites the
access and refresh-token values in its explicit `--stage2-client-env` file. The
replacement is a mode-0600 temporary file in the same directory followed by a
rename, so another mode-0600 env consumer such as `smoke-supabase` sees either
the complete old file or the complete new file, never a partial file. The
daemon continues with its in-memory credentials if that rewrite fails, but it
logs a warning; repair the file before restarting the daemon. A smoke command
reads its env at startup and does not itself persist a refresh, so restart the
smoke command if a concurrently running daemon rotates the shared credentials.

Remote spawning remains disabled unless the receiving daemon is started with
`--stage2-wrk-gate` and a local `--stage2-spawn-policy` file. A sender may send
only a request and label:

```sh
panewire submit ... --request-wrk --wrk-label rob1330-task
```

The receiver owns the launch context. Its plain JSON policy maps allowed label
prefixes to the local model, workspace, tier, and absolute working directory:

```json
{"rules":[{"label_prefix":"rob1330-","model":"codex-terra","workspace":"workers","t":"T1","cwd":"/safe/local/work"}]}
```

The materialized logical inbox path is supplied to `wrk` as `-p`; no sender
field can choose the model, workspace, tier, or CWD. A missing `wrk`, absent or
invalid policy, or unmatched label is terminal and fail-closed.

## Status

R2: prompt target safety, verified submission/uptake, privacy-preserving delivery audit, schema guard, events, inbox watcher, and wait. Live prompt smoke remains prohibited in the fixture test job; use an operator-approved scratch pane separately.

## R8 hub: presence, checks, and notification

`panewire hub` is the optional, always-on live channel for presence and
operator event push. It is deliberately **not** the durable transport:

- Supabase remains the stage 2 file relay and the offline recipient buffer.
- The hub evaluates node presence and local heartbeat checks, and can send
  optional Telegram incident/recovery notification.
- The hub is useful only while both sides are connected. Its process restart or
  outage loses no stage 1/2 state, and `panewired` retries it as a non-blocking
  side channel.

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

### Self-update runbook

Build and attach a release asset named `panewire_<goos>_<goarch>` (for example
`panewire_darwin_arm64`) to a GitHub Release, calculate its SHA-256, then
publish an explicit target list:

```sh
panewire update publish --hub-url https://hub.example.invalid \
  --hub-token-env /etc/panewire/operator.env --version r19b \
  --url https://github.com/org/panewire/releases/download/r19b/panewire_darwin_arm64 \
  --sha256 <asset-sha256> --machines company-m1,desktop
```

Nodes accept only GitHub release URLs: `github.com` plus the GitHub release
asset redirect host `objects.githubusercontent.com`. Redirects must remain
HTTPS, stay on that allowlist, and are limited to three hops. Nodes verify
SHA-256 before changing anything, retain the prior binary as
`.bak-<timestamp>`, then atomically rename the verified replacement over the
existing executable. Their supervisor must restart them: launchd needs
`KeepAlive=true`; systemd needs `Restart=always`. Publishing records the
expected version per target: only a later hello with that exact version emits
`update.succeeded`; no matching hello within ten minutes emits
`update.unconfirmed`. Release builds inject this version with
`-ldflags "-X main.version=<tag>"`, and `panewire version` prints it. Update or
download verification failures leave the existing binary untouched.

Quota is on-demand rather than a 60-second poll: `POST /v1/quota/<machine>`
asks the connected GUI-session node to execute `scopefuel --json` once and
caches its result for `QUOTA_CACHE_TTL` (default `5m`). Stdout is limited to
16 KiB and an oversized result is discarded with `output_too_large`; a timeout
kills the command's complete process group. The child environment is rebuilt
from the allowlist `PATH`, `HOME`, `USER`, `LANG`, `CODEX_HOME`, and
`CLAUDE_CONFIG_DIR`. The last two are scopefuel's documented credential
location overrides; hub tokens, Cloudflare headers, and all other daemon
environment values are never inherited.

By default every authenticated node is watched for presence and heartbeat-check
alerts. Pass `--alert-nodes machine-a,machine-b` on the hub to limit alerts to
those authenticated machine IDs; other nodes remain visible as
`presence-only` in `hub-status` and `/v1/nodes`.

See [docs/hub-r6.md](docs/hub-r6.md) for token-file formats, CLI examples, the
closed WebSocket vocabulary, and the NCP/systemd + tunnel deployment runbook.

## Hub job inbox compatibility

For node job heartbeats, the local inbox contract is the arbiter envelope as
the source of truth, with the prior flat event shape retained for compatibility.
The default `PANEWIRE_JOB_ACTIVE_MAX_AGE` is **72h**; see
[the job resilience contract](docs/jobs-resilience.md#node-inbox-event-contract)
for the event-field table and selection rules.
