# Hub execution

Real spawn/work/lead commands now use [durable hub operations](hub-spawn-operations.md). Configure the hub operator credential in the fleet hosts config and provision node spawn/work-root policy. There is no launcher subprocess or SSH launcher fallback. The imported planning and dry-run policy details below still apply.

# Spawn configuration

Panewire reads `~/.config/fleet/hosts.toml`. `FLEET_HOSTS_CONFIG` overrides the file;
otherwise `XDG_CONFIG_HOME/fleet/hosts.toml` is used when XDG_CONFIG_HOME is set.
For one release a config found only at the retired `~/.config/wrk/hosts.toml`
path (or through `WRK_HOSTS_CONFIG`) still loads with a one-line warning naming
the new location. The same applies to retired environment names: `WRK_LANE_STRICT`,
`WRK_COMPLETION_TIMEOUT_S`, `WRK_MOCK_MCP_CONFIG` and `SCOPEFUEL_E6_ARM` fall back
to their `PANEWIRE_*` replacements with a warning; a set `SCOPEFUEL_BIN` is ignored
with a warning. The workspace trust lock stays `.wrk.lock` until old spawner
binaries are drained from every host.
A missing file means a local host with standard harnesses. Malformed TOML,
duplicate keys, root assignments, BOMs, invalid safety booleans and contradictory
hub quota settings refuse with exit 70. Existing tables keep their key names.
Credentials belong in the referenced private credential files, never this repo.

```toml
[local]
machine_id = "host-a"
spawn = true
max_load_ratio = 1.5
max_active = 4
shell_ready_timeout_ms = 120000
brief_submit_timeout_ms = 120000
brief_submit_retry_ms = 15000
cmd_dsflash41 = false
clinepass = false

[hub]
hub_url = "https://hub.example.invalid"
hub_token_env = "~/.config/example/operator.env"
operator_token_env = "~/.config/example/operator.env"
lanes_token_env = "~/.config/example/lanes.env"
hub_cf_env = "~/.config/example/access.env"
machine_id = "host-a"
session_machine_ids = { default = "host-a", workers = "host-a-workers" }

[hosts.host-b]
ssh = "host-b"
via = "ssh"
herdr_session = "workers"
workspace = "workers"
capacity = 4
tags = ["linux", "gpu", "unity"]
presence_id = "host-b"
cwd_map = { "/work/project" = "/srv/work/project" }

[placement]
load_threshold = 1.0
wake_after_s = 600

[quota]
url = "https://hk.example.invalid"
credential_env = "HANDOFFKEEP_TOKEN"
presence_ttl_s = 86400
fresh_ttl_s = 300
accounts = { pool-a = "0123456789abcdef" }

[artifacts]
drive_folder = "~/Drive"
fpbulk = "~/.local/bin/fpbulk"
exclude = [".env*", "*.pem", "*.key", "*secret*", "*credential*", ".git/**"]
drivefs_log_dir = "~/Library/Application Support/Google/DriveFS/Logs"
scratch_dir = "~/.local/share/fleet-upload/scratch"
local_delete_at = "drive_confirmed"
```

## `[local]`

| Key | Default | Meaning |
| --- | --- | --- |
| `machine_id` | hub identity, then `local` | This execution host's identity in receipts and policy selectors. |
| `spawn` | `true` | Literal boolean; `false` refuses local admission with 80, including explicit local requests. |
| `max_load_ratio` | unset | Nonnegative load5 / CPU threshold for fallback spillover when hub placement is unavailable. |
| `max_active` | unset | Nonnegative active-agent threshold for fallback spillover. |
| `heavy_max` | legacy | Accepted and ignored; the `heavy` command was removed. |
| `heavy_load_wait` | legacy | Accepted and ignored; the `heavy` command was removed. |
| `shell_ready_timeout_ms` | `120000` | Startup readiness window, integer 30000–300000. |
| `brief_submit_timeout_ms` | `120000` | How long a spawn waits for the brief to start a turn before it refuses with 76, integer 30000–600000. Raise it on a host that runs under sustained load. |
| `brief_submit_retry_ms` | `15000` | How often the submit key repeats during that wait, at least 1000 and below the timeout. It goes only to an idle pane; Claude and Codex panes must also still show the brief in the composer. The brief is never pasted twice. |
| `cmd_dsflash41` | `false` | Literal capability opt-in required by cmd policy rows. The provisioned cmd account must have no persisted MCP servers. |
| `clinepass` | `false` | Literal capability opt-in required by the `cc-*` seats, which route the Claude Code harness through the host's local ClinePass proxy (`CLINEPASS_GATE_URL`, key read at spawn time from `CLINEPASS_GATE_KEY_FILE`; defaults are a loopback URL and `~/.config/cliproxyapi-key.txt`). |
| `tags` | unset | Capability tags offered by this host for job placement (`spawn --tags`). Lowercase tag names. `caps` is an alias; both lists merge. |
| `roles`, `grades` | unset | Light-host limits, as for [`[hosts.NAME]`](#hostsname): the spawn roles and task grades this machine accepts. |
| `presence_id` | `machine_id` | Identity this host uses in pacelever host-presence records. |
| `brief_policy` | embedded policy | Path to the versioned brief-policy JSON; leading `~/` expands. |
| `brief_preamble` | selected policy block | Explicit named preamble block for this host. |
| `pool_policy` | embedded pool-order policy | Path to the versioned pool-order JSON used by automatic model selection. |
| `fleet_usage_alert_gb` | legacy/read-only | Retained config key; standalone disk alerts are not emitted in v0.1. |

Additional boolean capabilities can be named by custom seat policy rows. A
missing or nonboolean capability fails closed. `standard` in seat policy means
no additional host capability beyond local admission and proven harness startup.

The embedded OMP profiles use the `omp` binary with standard host admission:

| Profile | Harness | Model selector | Pool class | Pool |
| --- | --- | --- | --- | --- |
| `omp-opus` | `omp` | `anthropic/claude-opus-5-5` | `credit` | `anthropic-api` |
| `omp-sonnet` | `omp` | `anthropic/claude-sonnet-5-5` | `credit` | `anthropic-api` |
| `omp-sol` | `omp` | `openai/gpt-6.1-sol` | `credit` | `openai-api` |
| `omp-terra61` | `omp` | `openai/gpt-6.1-terra` | `credit` | `openai-api` |

OMP launches with `--model <provider/model>:<effort>` and uses `-p` for a
headless delegate. Startup and brief delivery require a visible footer that
matches the model, effort, and working directory. The catalog seed at
[`catalog/omp-credit-seed.json`](catalog/omp-credit-seed.json) mirrors native
grades and gates for each effort. The desk publishes the seed after merge.

## `[hub]`

| Key | Meaning |
| --- | --- |
| `hub_url` | Panewire hub URL. |
| `hub_token_env` | Operator credential-file path for placement/quota. Configuring it opts into the hub quota gate and requires hub_url. |
| `operator_token_env` | Operator credential file for control operations and builder lane fallback. |
| `lanes_token_env` | Dedicated machine-bound lane credential; preferred for lane registration/removal. |
| `hub_cf_env` | Optional access-service credential file passed to panewire. |
| `machine_id` | Host identity fallback. |
| `session_machine_ids` | Herdr session → machine identity table used for lane placement. |
| `session_lanes_token_envs` | Herdr session → lane credential-file path. Overrides `lanes_token_env` for that session; pair it with `session_machine_ids` when sessions use separate node identities. |
| `quota_gate` | Only `"local"` is accepted: explicit local-only quota mode. It contradicts hub_token_env; remove one of those keys. |
| `state` | `"on"` (default) reads the hub's fleet state for placement and the quota gate; `"off"` skips it and measures directly as before (the rollback switch). See [Fleet state](#fleet-state). |
| `state_publisher_ttl_s` | Seconds a publisher heartbeat stays alive. Default 90. |

Worker lane credentials use lanes_token_env, then hub_token_env. Builder lane
credentials use lanes_token_env, then operator_token_env, then hub_token_env.
Panewire owns authentication and relay transport; panewire never prints credentials.
All herdr calls during a spawn use one frozen `HERDR_SESSION` (default `default`).
For example, a node serving session `fleet` can use
`session_machine_ids = { fleet = "host-a", default = "host-a-default" }` and
`session_lanes_token_envs = { fleet = "/config/node.env", default = "/config/node-default.env" }`.
The registration warning includes session, machine, pane and panewire's error text;
it is emitted for workers and builders. A credential explicitly declaring another
machine fails locally with `LANE_SESSION_MACHINE_MISMATCH` (70); configure both session
maps rather than relabeling the route. `job.spawned` and the success line also
record the herdr session, so a pane id is never the only delivery address.
If no machine identity can be read locally, panewire validates the credential
and its error is preserved in the registration warning.

## `[hosts.NAME]`

| Key | Default | Meaning |
| --- | --- | --- |
| `ssh` | `NAME` | SSH config target; BatchMode is used. |
| `via` | `ssh` | `ssh` (default; unchanged when unset) or `hub`: spawn, worktree preparation and quota refresh go through the panewire hub (see [Hub transport](#hub-transport-via--hub)). A `hub` host never falls back to ssh. |
| `herdr_session` | `worker` | Remote HERDR_SESSION; set it to the session served by the target node. |
| `workspace` | target `[local] workspace` | Workspace on this remote host. The caller's `-w` / `--workspace` applies only to local placement. Missing target workspace configuration refuses with `WORKSPACE_UNCONFIGURED` (70). Automatic SSH placement tries the next candidate on this early refusal; forced placement still stops. |
| `cwd_map` | explicit override | Inline table of local prefix → remote prefix; longest prefix wins. Only needed as an exception: the default remote cwd is `$HOME/work/<repo>` on the target, cloned when missing. For a job run from a linked git worktree the default is `<work root>/<worktree dir>`, where the work root is derived from entries that map a checkout directly under the local work root (else `$HOME/work`). |
| `cwd_keys` | legacy | Kept for legacy hub configurations; not used. A hub host's cwd key is the canonical repo name. |
| `hub_wait` | `240` | `via = "hub"` only: seconds (1..300) the hub waits for the node to answer a spawn. It bounds the whole node run, so it must exceed the node's `brief_submit_timeout_ms` plus launch time; a shorter budget cuts the submit window. A first spawn that clones a large repo may need more. |
| `brief_doc_prefix` | `briefs/` | `via = "hub"` only: handoffkeep key prefix the brief is uploaded under; it must match the node's own `brief_doc_prefix` (ends in `/`). |
| `tags` | unset | Capability tags offered by this host (`caps` is an alias; both lists merge). A job's required `--tags` must be a subset for the host to be a placement candidate. A capability the host does not declare is unknown and fails closed for this host only: the host is excluded for jobs that need it and stays eligible for every other job. See [Host capabilities and light hosts](#host-capabilities-and-light-hosts). |
| `roles` | unset (all) | Nonempty list of spawn roles (`worker`, `builder`, `tester`, `reviewer`, `counsel`) this host accepts. Other roles record `role_not_accepted:<role>`. |
| `grades` | unset (all) | Nonempty list of task grades (`C`, `B`, `A`, `A+`, `S`, `S+`) this host accepts. Other grades record `grade_not_accepted:<grade>`; a lead or consult has no grade and records `grade_not_accepted:none`. |
| `presence_id` | host `NAME` | Identity this host uses in pacelever host-presence records. |
| `capacity` | unset | Inventory value displayed by hosts. Authoritative capacity comes from panewire placement and remote admission. |
| `heavy` | legacy | Accepted and ignored; the `heavy` command and its host lock were removed. |
| `wake` | off | `"panewire"` marks the host wakeable: the placement policy may request `panewire node wake` when asleep, then waits for the node to connect. |
| `wake_hold` | `10` | Burst hold in minutes. |
| `wake_wait` | `90` | Burst command timeout in seconds; wake refusal returns 81. |
| `brief_policy`, `brief_preamble` | host policy default | Optional initiator-side preview settings for this host; deploy matching policy on the execution host. |

When the job cwd is a linked git worktree and placement picks a remote host,
panewire prepares the same worktree there before spawning: it ensures the repo
clone (from the local `origin` URL) under the host's work root, runs
`git fetch origin`, and runs `git worktree add` for the local branch, tracking
`origin/<branch>` when it exists (a PR head) and otherwise creating it from
origin's default branch. An existing remote worktree is reused when it is on
that branch; a different branch, a detached local HEAD, local commits missing
on the target or any failing step refuses the spawn with exit 81 — it never
tries another host or falls back to local. An explicit `cwd_map` entry decides
the worktree path (its clone is the canonical repo name beside it); an
unreachable host is skipped as before. `--dry-run` prepares nothing.

### Host capabilities and light hosts

Declare what each machine can run, and limit small machines to light work.
Capabilities use the `--tags` vocabulary: `macos`, `linux`, `unity`,
`xcode`, `gpu`, `heavy-go`. A task needing a capability goes only to hosts
that declare it; the others still get the work they can do.

In `[local]` and `[hosts.NAME]`, `caps` must be a TOML array containing only
the capability strings listed above. An empty array declares no capabilities.
Booleans, numbers, strings, tables, nonstring array entries, and unknown
capability names refuse config loading with exit 70. The error names the host's
`caps` key and the invalid value.

Recommended declarations by machine class:

| Machine class | `tags` | Limits |
| --- | --- | --- |
| macOS development machines | `["macos", "unity", "xcode"]` | none |
| Linux desktop | `["linux", "unity", "gpu"]` | none |
| Small cloud VM | `["linux"]` | none; no `unity` |
| One-core cloud VM | `["linux"]` | `roles = ["worker", "tester", "reviewer"]`, `grades = ["C", "B", "A"]` (no lead, no heavy grade) |
| Raspberry Pi | `["linux"]` | `roles = ["worker", "tester"]`, `grades = ["C", "B"]` (Python and docs work) |

```toml
[hosts.mac-b]
tags = ["macos", "unity", "xcode"]

[hosts.linux-desk]
tags = ["linux", "unity", "gpu"]

[hosts.cloud-small]
tags = ["linux"]

[hosts.cloud-tiny]
tags = ["linux"]
roles = ["worker", "tester", "reviewer"]
grades = ["C", "B", "A"]

[hosts.pi]
tags = ["linux"]
roles = ["worker", "tester"]
grades = ["C", "B"]
```

These are recommendations, not code defaults: a host without `tags` declares
no capability. An invalid `roles` or `grades` value refuses the config with
exit 70. Automatic selection applies the limits like `tags`, also under
`--host NAME`. A pinned `-m` with a forced `--host NAME` skips placement, so
neither tags nor limits are checked there; that override needs
`--override-reason`, which is recorded.

### Hub transport (`via = "hub"`)

A host with `via = "hub"` is spawned through the panewire hub (`POST /v1/spawn`,
`spawn_v2`) instead of ssh. It needs `[hub] hub_url` and an operator credential
file (`operator_token_env`, else `hub_token_env`; `hub_cf_env` if the hub sits
behind access); the files must be private and are never echoed. For one spawn
panewire:

1. uploads the brief as an hk document `<brief_doc_prefix><job>.md`
   (`handoffkeep doc put --kind brief --job <job>`) and sends the key as
   `brief.doc`; the hub never sees the text. A brief over 64 KiB is refused
   (exit 2); a retry overwrites the same key.
2. for a job started from a linked worktree, sends a `checkout` spec that the
   node's `worktree.prepare` turns into the same worktree: `mode: branch` for
   a branch whose local HEAD is already contained in `origin/<branch>` (the
   node tracks origin's tip), otherwise `mode: detached` at the exact `sha`
   with `fetch: pull/<n>/head` when a local `origin/pull/<n>/head` ref points
   at HEAD, or `fetch: refs/heads/<branch>` of an origin branch containing it
   (a detached HEAD, or a branch whose commits sit on another origin ref).
   Only refs already known locally are read; nothing is fetched. Commits on no
   origin ref, an origin URL without an https form (`git@host:o/r` and
   `ssh://` become `https://host/o/r`), or a subdirectory start refuse with
   exit 81 before anything is uploaded. The worktree directory name is the local
   one. A job from a canonical checkout sends no `checkout` and runs in the
   node's `cwd_map[<repo name>]`.
3. sends the flags the hub's allowlist carries (`-m -l --t --job --owner
   --effort --role --lane --parent --task|--parent-job --report-relative -L --urgency
   --builder-family --builder-job`). The node supplies workspace, host and the
   override reason. A flag the allowlist cannot carry (`--keep`, `--job-dup-ok`,
   `--task-hk-bypass`, `--allow-escalation`, `--operator-request`,
   `--requested-by`, `--purpose`, `--lane-defer`) refuses the spawn (exit 2)
   instead of being dropped. A report is sent as `--report-relative <path>`,
   relative to the jobs root, and the node resolves it under its own jobs root,
   so the two hosts' home directories may differ. This applies to `--report`
   under the caller's jobs root and to a caller-given `--report-relative`
   alike; no absolute `--report` crosses the hub. A report outside the
   jobs root is refused (exit 2), as is an absolute or `..` `--report-relative`
   or a path with characters the hub does not accept. A hub without
   `--report-relative` answers 400 `invalid_request`; panewire then stops with a
   message naming the missing capability instead of retrying with an absolute
   path. Upgrade panewire on the hub to a version whose spawn allowlist carries the
   flag.
   `--builder-family` may be sent without `--builder-job` for `--role reviewer`
   only (the author's job is not bound to the task): the reviewer then runs as
   a top-level job, still excludes that family, and neither claims nor links
   the task, so several passes on one task all spawn. Any other role keeps the
   "go together" refusal (exit 2).
4. the node's `spawn.json` must map the repo name as a `cwd_key` and set
   `work_root`; `spawn_disabled`, `cwd_unmapped`, `panewire_missing` and
   `brief_doc_unavailable` refuse that host (recorded as `hub_<reason>`) and
   `--host auto` moves to the next candidate.

The target account is refreshed with the `quota.refresh` node command instead
of `ssh <host> pacelever refresh`; `measure_timeout_s` still bounds it.

Failures never fall back to ssh (design D4):

| Hub answer | Result |
| --- | --- |
| Hub unreachable, timed out, 5xx | Host refused, reason `hub_unavailable`. `--host auto` continues with the other candidates; with none left the usual refusal applies (80 when local spawning is off, else the job runs locally). `--host NAME` exits 81 `hub unavailable`. A quota refresh meets the same outage the same way. |
| 503 `spawn_capability_required` | Node predates `spawn_v2`: host refused, reason `hub_spawn_capability_required`. |
| 503 `node_unavailable` | Node asleep or not accepting: treated like an unreachable host, so a wakeable host is woken (and waited for through the hub connection, never ssh). |
| 401 / 403 / 400 | Host refused, reasons `hub_auth` / `hub_invalid_request` (exit 70 semantics). |
| `checkout_refused:<outcome>` (rc 81) | Exit 81 naming the outcome (`branch-elsewhere`, `sha-missing`, `origin-mismatch`, `dirty`, `locked`, `git-failed`, `git_too_old`); no other host is tried. `busy` is retried once. |
| 504 pending, `lost`, node `timeout` | Exit 75: the node may still run the job, so nothing is spawned elsewhere. |
| Node spawn rc 0 | The `OK pane=… job=…` receipt is printed with `host=NAME`. Other rc values follow the ssh rules (2, 70, 74, 75, 76, 79 and 82 stop the round; others refuse the host). |

`--dry-run` uploads and sends nothing.

`--host auto` runs the local placement policy: required `--tags` must be a
subset of a host's tags (a job with no tags may go anywhere), hosts with fresh
presence evidence for the needed quota account rank before unknown logins
(degraded reads count as unknown, never absent), and awake candidates order by
load5/ncpu with any host at or above `load_threshold` tried only when nothing
else qualifies. The spawn-time quota gate on the chosen host remains the final
admission check; a refusal moves to the next candidate. Asleep `wake` hosts
are only awakened under the documented rules — a capability no awake host
has, every awake candidate over threshold after `wake_after_s`, the needed
account measured only on the sleeper, or an operator request — then panewire
waits for the node and places the job. Explicit `--host NAME` stays a forced
override; an unreachable wakeable target is woken once. Every decision is
recorded as a `job.placement` event in the job record.

`--exclude-host a,b` removes the named hosts from `--host auto` placement: an
excluded host — the local machine included, by its machine id — is never
ordered, gated or woken, and its candidate records `excluded_host`. When no
candidate remains the spawn fails with the usual placement refusal. An explicit
`--host NAME` (or `--host local`) that names an excluded host is refused with
exit 2. The list is initiator-local and is not sent to a delegated host.

Automatic selection measures an account the same way with or without
`--host NAME` or `--exclude-host`: a stale shared reading is refreshed on any
holder of that account, the local machine first, and a host those flags keep
out of placement may still be that holder. It is never placed or gated. So
`panewire plan` and a restricted spawn judge the same evidence, and a failed
refresh on the only placeable holder (for example over ssh that is down) no
longer refuses an account another holder can measure.

Before trying a remote candidate (including an explicit `--host NAME`), panewire
resolves that host's most recent fresh presence for the launch pool, then asks
`pacelever gate --account-key POOL:FINGERPRINT --json` to judge its shared
observation. The spawner's `[quota.accounts]` does not pin remote accounts.
Missing, ambiguous, stale or degraded target evidence refuses that host; auto
placement tries the next candidate. A pinned `-m` with a forced remote
`--host NAME` that has no selected account resolves the pool from
`pacelever policy launch` for the seat's effort; when that refuses (an
unplaced rung, for example) the spawn refuses with that cause
(`quota pool for PROFILE@EFFORT unresolved: …`), not as missing presence. Local placement keeps the local gate path.
The selection's `quota_gates` and placement candidate's `quota_gate` record the
host, account key, evidence source, outcome and exit code of each attempt.
On the executing host, login confirmation accepts an observed, readable,
account-matching shared observation even past `fresh_ttl_s`. It still refuses
missing presence, an account mismatch, missing observation and degraded reads.
The account-bound quota gate retains responsibility for freshness and refusal;
an old observation cannot skip that gate.

When the target's shared observation is missing or stale and the spawning host
does not hold that account (no `[quota.accounts]` pin or local presence record
for the key), pacelever cannot refresh it on demand. panewire then runs
`pacelever refresh POOL` on the target over SSH, bounded by `measure_timeout_s`,
re-reads the shared observation and judges it (for a `via = "hub"` host the
refresh is the hub's `quota.refresh` node command, not ssh). A failed, timed-out or
unpublished measurement keeps the refusal; `quota_gates` records
`measured` (`ok` or `failed`) and a `measure_*` reason. A gate that reports its
`gate_basis` is also recorded as `basis`.

The spawning host needs a pacelever version that supports account-bound gate
receipts. A successful response must confirm the requested `account_key` and
pool; versions that silently ignore the flag are refused. Pane and headless
launches recheck that binding before recording the quota lease, including
when a quota adapter bypasses the CLI client. A selected account requires a
JSON receipt; legacy text cannot confirm its identity. The target still runs
its own final local gate before spawning. Upgrade pacelever on spawning
hosts before rolling out this change; roll back panewire to restore the earlier
placement path if needed. No remote checkout provisioning changes are included.

Panewire must already be installed on SSH targets. `_spawn-stdin` transports a
structured request and original brief through stdin, uses a private temporary
brief file and deletes it after spawn. The target composes its own preamble.
No host installation or service management is performed by spawn.

## `[placement]`

| Key | Default | Meaning |
| --- | --- | --- |
| `load_threshold` | `1.0` | load5/ncpu value above which a host takes no new job while anything else qualifies. |
| `wake_after_s` | `600` | How long the task must have waited before saturated awake hosts justify waking a sleeper. |

## `[quota]`

Shared-quota read settings for presence-aware placement. URL and credential
follow the pacelever convention: `HANDOFFKEEP_URL`/`HANDOFFKEEP_TOKEN` in the
environment or `~/.config/handoffkeep/config.env`; the keys below are explicit
overrides.

| Key | Default | Meaning |
| --- | --- | --- |
| `url` | `HANDOFFKEEP_URL` | Shared quota cache endpoint; HTTPS required outside loopback. |
| `credential_env` | `HANDOFFKEEP_TOKEN` | Name of the environment variable holding the bearer token. |
| `presence_ttl_s` | `86400` | How recent a host-presence record must be to count as login evidence. |
| `fresh_ttl_s` | `300` | How recent a shared observation must be to count as fresh. A spawn delegated over ssh with a selected account carries the placing host's value, and the executing host rechecks that account with it (up to 86400). |
| `measure_timeout_s` | `60` | Bound on the on-target `pacelever refresh` run for a remote account the spawning host does not hold. |
| `accounts` | unset | Optional pool → 16-hex account-fingerprint table for exact account keys. |
| `node_env` | `~/.config/panewire/hub-node.env` | Panewire node env file; `HUB_MACHINE_ID` in it is the host id written to job records. `PANEWIRE_NODE_ENV` overrides the path. |

## Job records in handoffkeep

Each spawn writes a record to hk `/v1/jobs` on the executing host (the design
is `design/2026-10-30/cross-host-jobs`): the claim `POST {job_id, task_id,
host}` runs before the local claim and a taken id refuses with exit 74, like
the ledger's duplicate. `host` is the node's `HUB_MACHINE_ID`. hk unreachable
or unconfigured fails the spawn closed. The task precheck also fails closed before launch.

After the brief lands, panewire merges `{state:"spawned", role, label, family,
profile, model, pool, owner_lane, parent_lane, parent_job, pane_id,
worktree:{origin, branch|null, head}}` — the allowlisted fields only; brief
text, command lines, pane output and absolute local paths are never written.
`panewire done` writes `{state:"completed", report_doc}` plus `result` when
`--result FILE` (a JSON object of at most 64 KiB) is given, in the same PUT,
and emits the relay only after the PUT returns; `escalate` writes
`{state:"escalated"}` and the watcher writes `{state:"lost"}` after two
missing-pane observations. A failed write after the pane exists degrades to a
`job.record_failed` event and a warning; the relay still goes out so readers
fall back.

`--parent-job` resolves the local job directory first, then `GET
/v1/jobs/<job>` on any host — waiting up to 30 s for a lagging record before
refusing with exit 75. `spawn --wait` on a delegated job also watches the hk
record: `completed` exits 0 (the result is read from the record by the
caller), `escalated` exits 83, `lost` exits 84, and a record still pending,
absent or unreadable leaves the local signals and the usual deadline to
decide (the unreadable case warns once with reason `hk-unavailable`).

After the brief lands, a failed task claim leaves the pane running and the spawn
landed. The `job.task_link` event records `status: failed` and a bounded reason;
panewire warns and sends an owner-lane notice so the task can be linked manually.
A successful claim records `status: linked`. Other spawn failures retain their
normal cleanup behavior.

An inherited spawn normally leaves the task bound to its parent job. It rebinds
the task to the new job, through `handoffkeep tasks rebind`, only after the brief
lands and only when all of these hold: the parent's hk record state is `lost`,
`failed` or `completed`; the task's `refs.job_id` is the parent; the new job id
differs; and the spawn is a replacement seat (routing role `implementer` or
`lead`), not a reviewer, tester, consult or `panewire delegate` child. The
`job.task_link` event then says `status: rebound`. Otherwise it says `inherited`,
adding `bound_job` when the task is bound to a third job and `rebind_error` when
the rebind failed or the parent's state was unreadable; neither fails the landed
spawn.

Automatic model selection (`spawn` with `-m` omitted or `-m auto`) reads the
live bench catalog and pool status from the quota CLI, then orders candidates
under the versioned pool-order policy (`[local].pool_policy` or
`PANEWIRE_POOL_POLICY`, else the embedded copy): paid pools at waste risk
first (see below), then other WASTE subscription pools, then free pool/model
sets, then ranked subscription pools, then credit pools. API credit is spent only
after eligible subscription capacity. Inside a tier the lowest catalog grade that satisfies the task wins,
then the pool with the higher waste urgency — the unused projected share
(100 − projected) per hour until its longest account window's reset (7d,
monthly; sub-daily windows such as a 5h rate window never rank a pool, they
stay admission gates only). A pool whose longest window's reset can be paced
always ranks ahead of one whose cannot; unpaceable pools keep the lower
projected use at reset (the used percentage when it cannot be paced yet) —
then the smaller effort, then the catalog score.

The pool-order policy is version `2`. A version `1` file supplied through
`[local].pool_policy`, `PANEWIRE_POOL_POLICY`, or `simulate --policy` is refused
with exit 70 and a migration error. To migrate, copy
`internal/fleet/pool-order.json`, retain any custom thresholds and free
model sets, and set `version` to `2`. The order is
`["waste", "free", "ranked", "credit"]`, and `credit_pools` includes both
`anthropic-api` and `openai-api`. Version 2 requires credit last, after ranked.
This version changes credit admission as well as the tier order: credit no
longer requires a balance observation, but its pool class must remain readable.

The projection is pacelever's pace: for each account-scope bucket in the
quota status, used percentage ÷ elapsed share of its window, paced at the
status document's `generated_at`; a bucket past its reset or under 5% into its
window has none, and a pool's projection is its highest bucket's. Two policy
keys use it, both defaulting to 100:

| Key | Default | Meaning |
| --- | --- | --- |
| `waste_below_projected_pct` | `100` | A non-credit subscription pool projected (over its windows of a day or more) below this at reset is WASTE: the rest is lost at reset, so it is spent before credit and ranked pools. Without a projection the quota status `verdict.waste` flag decides. An explicit `0` disables projected waste. |
| `exclude_at_projected_pct` | `100` | A waste-risk pool projected at or above this over its windows of a day or more is dropped as `projected_exhaust`. A pool not at waste risk is never dropped on projection: it ranks behind credit instead (credit pacing, below). A free-tier rung is kept for admission instead and survives only when the gate's `gate_basis` is a free bucket (`free-bucket:…`); any other basis drops it as `projected_exhaust`. Must be above 0 and not below `waste_below_projected_pct`. An over-consumed reading (used above 100%) still projects, so it is excluded. |

A paid pool is at **waste risk** when the projected unused share of its
longest window (a day or more) at that window's reset — waste urgency ×
hours to reset, i.e. 100 − its projection — is at least
`waste_risk.min_unused_pct`, however far the reset is. Free seats and
`credit_pools` are never at waste risk by this rule (a credit balance in
its final window ranks first instead, below), and neither is a pool whose
longest window cannot be paced (for example the first 5% of a fresh
window). Waste-risk pools rank ahead of every tier, free included, and
among themselves by waste urgency first (a pool resetting sooner with the
same unused share comes first), then grade fit, effort and score; every
other pool keeps the tier order above. For a waste-risk pool sub-daily
windows (e.g. 5h) are used up rather than paced: they never contribute to
its `projected_exhaust` check and refuse it only when exhausted — used at
or above 100% (dropped as `window_exhausted`) or the quota gate's own
exhausted refusal. The same holds for every other subscription pool: only a
window already used up (a sub-daily window or the blocking share at or above
100%) drops it, and sub-daily windows never project. The quota gate itself never refuses on
pace (pacelever: "pace ranks and warns; it never denies"), so no gate flag
is passed.

| Key | Default | Meaning |
| --- | --- | --- |
| `waste_risk.min_unused_pct` | `20` | Minimum projected unused share (percent) of the longest window at its reset for a paid pool to be at waste risk. `0`–`100`. |
| `waste_risk.horizon_hours` | `0` | Optional: when above 0 the longest window must also reset within this many hours. `0` means no horizon limit. |

Both keys live in the pool-order policy under `"waste_risk": {...}`; absent
keys take the defaults, and `panewire simulate --policy`/`--policy-set` try
other values before the policy is changed.

Subscription candidates additionally need a readable `ok` nonstale spend
status. The named credit pools, `anthropic-api` and `openai-api`, remain
admissible without a balance observation. An operator pool class of `exclude`
refuses them. Their selection record carries `tier=credit`, with absent
balance values left unset.

Credit ranks below every admitted subscription pool, which resets sooner,
and is used when none is admitted (window used up, limit, no login on the
host); how much credit has been spent does not matter — except for credit
pacing.

**Credit pacing** (decision/2026-11-04/pool-order-owner): credit also
expires. A subscription pool whose longest window (a day or more: weekly,
monthly) is projected at or above 100% at that window's reset at its current
pace will use that quota up anyway, so it ranks **behind credit**: the
candidate records `paced=true` and `behind_credit=true`, and credit takes the
work first. This applies only when a credit candidate with a recorded balance
above zero (pacelever's manual store) is in the ranking; with no balance
recorded, or a zero balance, the paced pool keeps its tier and
`behind_credit=false`. A credit gate refusal falls through to the paced pool
next. Free seats, credit pools and waste-risk pools are never paced. There is
no threshold beyond "used up" (100%); sub-daily windows (e.g. 5h) never pace a
pool, they only refuse it once exhausted. Candidates also record
`long_projected_pct`, the projection over windows of a day or more; tiering
and the unpaceable fallback order use it, never a sub-daily pace. In the final
`credit_final_days` (default `4`; `0` disables) before the credit's expiry a
pool with a balance left ranks first, ahead of waste-risk pools and the free
seats, so the rest is used up: tier `waste`, `waste_reason=credit_expiring`.
panewire reads the balance (`100 − used_pct`, plus the dollar balance when
recorded) and the expiry from the pool's current credit entry in pacelever's
manual store (`pacelever manual list`). `panewire plan` prints
`credit_expiring final_window_days=… days_left=… balance_pct=…` (and
`balance_usd=…` when recorded); `panewire plan --credit-at TIME` judges the
window as of another RFC3339 time or date, read-only, with quota and presence
still read at the clock. A credit with no entry, already expired or used up,
or outside the window stays in tier `credit`, and so does every credit pool
when the store is unreadable.
If the pool-status query fails, credit candidates are dropped as
`QUOTA_UNKNOWN` with reason `quota_unknown` and the selection records
`credit_pool_status_unavailable` in `degraded`. Healthy subscription candidates
remain eligible. Remote placement and executing-host admission recheck credit
pool classes and the selected login, then use the account-bound gate without
requesting a balance refresh.
The pool comes from the launch response, the selected account key, or the gate
receipt. Every supplied pool must agree; a different pool refuses with exit 70,
regardless of its class. The receipt may omit its pool when the launch or
account key already provides one. If all three omit it, admission refuses.
The resolved pool is passed to hub admission and the quota lease, with credit
class checks before execution. Account-bound receipts still require the exact
account key, v1 schema, approval flag, and exit status. Their `provider_id` is
optional when the account key provides the pool, and must match when present.

Every candidate needs fresh
host-presence evidence for its quota account on a spawn-eligible host and a
passing quota gate. For subscription pools, any unknown,
stale or degraded evidence drops the candidate — selection never guesses and
fails closed. Every catalog row's outcome (grade, gate, policy tier,
presence, quota or seat reason) is recorded on the `job.placement` event;
`--dry-run` prints the full record and emits nothing. Explicit `-m`/`--host`
overrides require `--override-reason`, which is recorded; escalation and
consult-only gates stay excluded unless `--allow-escalation` is given, and
quota admission still applies to overrides.

Selection records urgency. `spawn --urgency normal|urgent` sets it; when the
flag is omitted the bound hk task's `priority` is read from the existing task
precheck (priority 1 or 2 resolves to urgent, anything else or unreadable to
normal), and an explicit flag always wins. Urgency does not change the
order: the waste-risk override, the policy `order` and the grade-fit rule
apply to every task, and urgent work reaches a paid pool that is not at
waste risk when the free rung is refused or excluded. The `job.placement` selection
record carries `urgency`, `urgency_source` (`flag`, `task-priority` or
`default`), the `tiers` order used, a one-line `decision`, the ordering
`rule` with both thresholds and the waste-risk rule, `waste_risk` (the
`horizon_hours`/`min_unused_pct` in force), `projected_pct` and `used_pct`
per candidate, `waste_risk` (`true`/`false`) on every candidate and
`unused_at_reset_pct` (its input) whenever the longest window could be
paced,
`waste_urgency`, `reset_hours` and `waste_order` (`reset` or
`projected_fallback`) per ranked candidate,
and on `chosen` its `tier`, `grade`, `waste_risk`, `projected_pct`,
`waste_urgency` and a one-line `why`;
`--dry-run` prints `urgency=<v> source=<s> tiers=<a>><b>...` before the full
record.

### `panewire simulate`

`panewire simulate --selection FILE [--now RFC3339] [--policy FILE]
[--policy-set key=value] ... [--set
pool:used=N,reset=RFC3339|+Nh[,window=D][,projected=N]] ...` replays the
selection ranking on a recorded input and prints the decision record
(`mode: "simulate"`). It is hermetic: no quota reads, no gate calls, no
spawn, no job writes. The input file is a minimal JSON document — or a
`job.placement` event whose `payload.selection` carries the same fields:

```json
{
  "now": "2026-10-31T07:24:00Z",
  "grade": "A",
  "role": "worker",
  "effort": "",
  "allow_escalation": false,
  "credit_pools": ["anthropic-api"],
  "pools": [
    {"id": "claude", "status": "ok", "class": "spend", "waste": true,
     "used_pct": 31, "projected_pct": 31.8,
     "resets_at": "2026-10-31T16:48:00Z", "window": "7d"},
    {"id": "anthropic-api", "class": "credit", "credit_left_pct": 80}
  ],
  "candidates": [
    {"profile": "sonnet", "seat": "sonnet", "effort": "high", "grade": "A",
     "pool": "claude", "gate": "default", "model_id": "m",
     "free": false, "free_limit": false, "score": 90}
  ]
}
```

`--now` wins over the document's `now`, which wins over the clock. A pool's
projection is `projected_pct` when given, else pacelever's pace over
`used_pct`/`window`/`resets_at` (or `reset_hours` relative to now); waste
urgency is (100 − projected) ÷ hours-to-reset of the longest window of a
day or more (sub-daily windows never rank), and pools without a readable
reset rank by projected order. `credit_left_pct` records a credit pool's
balance left (percent of its total) so credit pacing can be replayed; absent
means no balance is recorded. `free` marks a free-tier rung; `free_limit`
records an observed free limit — the only exclusion a free rung honors, so a
shared pool's `used_pct` or projection never drops it. `--set` (repeatable)
overrides or creates a pool and repaces it: `used` and `projected` are
percentages, `reset` an RFC3339 instant or `+Nh`/`+Nd` relative to now, and
`window` a quota duration (the entry's own window, else 7d). Candidates are
the catalog row fields plus `seat`; a recorded `job.placement` selection's
`candidates` decode directly, and when the input is the recorded
`payload.selection` record itself the request comes from its `requested`
(`at` is now), fields beyond the candidate schema are ignored, and the pool
table is rebuilt from the candidate rows' `used_pct`/`projected_pct` — with
no recorded reset those pools rank by the projected fallback until `--set
pool:reset=…` supplies one.

A pool's `resets_at` must be an RFC3339 instant or `+Nh`/`+Nd`, and a
non-empty `window` a quota duration; anything else fails with exit 70
instead of silently ranking by the projected fallback. A sub-daily `window`
is a short window (it gates a waste-risk pool only when `used_pct` reaches
100); any other window is the pool's longest.

`--policy FILE` loads the pool-order policy for this run instead of
`[local].pool_policy`/`PANEWIRE_POOL_POLICY`. `--policy-set key=value`
(repeatable, applied after `--policy`) overrides one threshold:
`waste_risk.horizon_hours`, `waste_risk.min_unused_pct`,
`waste_below_projected_pct` or `exclude_at_projected_pct`. A malformed
term or unknown key exits 2; a value the policy rejects exits 70. The record
carries `policy_file` and `policy_overrides`.

### `panewire plan`

`panewire plan [--grade G] --t T0..T3 [--role R] [--tags a,b] [--effort E]
[--urgency normal|urgent] [--exclude-host a,b] [--builder-family F]
[--routing-role R] [--allow-escalation] [--json]` answers "right now, which host and which
model would a spawn get" without a task or brief. It is live and read-only:
it runs the spawn's selection core with catalog, account observations,
presence, and account-bound gates. Worker roles use `--grade` instead of a
task title. Lead and consult omit the grade. Selection then walks the same placement plan for the chosen seat — the local launch
resolution, seat check and gate for this host, the shared-account quota
gate for each remote host — and stops at the first host that admits it. It
never spawns, wakes a host, claims a local job, writes an hk task or job
record or emits a job event; a stale remote account is measured on its
host (`pacelever refresh`) exactly as a spawn would. Remote-only spawn
checks (repository mapping, the remote seat check) are not run.

The text output is `plan …` (request), `chosen host=… seat=…@… model=…
pool=… grade=… tier=… waste_risk=…`, one `runner-up …` line per other ranked
rung in rank order, one `dropped …@… pool=… reason=…` line per filtered
rung, one `host NAME outcome=…` line per placement candidate (`would_spawn`,
`quota_refused`, `missing_tags:…`, `not_tried`, …), `would_wake [...]` when
only a host a spawn would wake remains, and `result refused exit=N: …` on
failure. `--json` prints `{"mode": "plan", "selection": {...},
"placement": {...}, "chosen": {"host", "seat", "model_id", "effort",
"pool", "tier", "grade", "waste_risk"}}` (plus `error`), where `selection`
is the spawn selection record with `mode: "plan"` and `placement` the
placement record.

| Exit | Meaning |
| --- | --- |
| 0 | A host and model were found. |
| 2 | Usage: missing or invalid `--grade`/`--t`/`--role`/`--urgency`/tags, or `--builder-family` without tester/reviewer (or the reverse). |
| 70 | Configuration, unreadable catalog or policy. |
| 77 | No catalog rung was admitted. |
| 80 | Local spawning is disabled and no remote host admitted the seat. |
| 81 | No host admitted the seat; hosts a spawn would wake are listed. |
| 86 / 87 | `QUOTA_EXHAUSTED`: all eligible accounts are measured exhausted (`wait` / `escalate`); see "Role routing". |
| 88 | `AUTH_UNAVAILABLE`: no usable account binding. |
| 89 | `QUOTA_UNKNOWN`: unknown quota after a bounded refresh. |

A local launch or gate refusal keeps its own code, as in `spawn`. Plan
applies role routing like `spawn`; its text output adds a `routing role=…
version=… policy_source=…` line.

## Role routing

`policy/model-routing` supplies the v1 role allowlists. Selection filters
profiles by role, retirement, gate, seat, and family before applying the
`policy/quota-stop` order. `spawn`, `plan`, `resolve`, and `delegate` enforce
this boundary. An explicit `-m PROFILE` requires `--override-reason`, but
that reason overrides placement only. A retired or role-ineligible profile
is refused before placement.

### Lead and consult

`spawn --role builder` maps to routing role `lead`. `--role counsel` maps
to `consult`. Both admit only `opus` and `fable` through `claude`, or
`codex-sol` and `codex-astra` through `codex`. OMP also admits the exact model
selectors `anthropic/claude-opus-5-5`, `anthropic/claude-fable-5-1`,
`openai/gpt-6.1-sol`, and `openai/gpt-6-astra`.
The shipped `omp-opus` and `omp-sol` profiles qualify. `omp-sonnet` and
`omp-terra61` cannot lead or consult. An `agy` or `pi` seat running an
Anthropic model is ineligible. These constraints also apply to a wildcard
in the v1 table and to explicit profiles.

The routing table's profile allowlist still applies. OMP profiles must be
listed by name or covered by `"*"`. Catalog and launch model IDs are checked,
so a top-profile name cannot admit a non-top OMP model.

Task grade is recorded but does not filter or rank a lead or consult.
`spawn --role builder`, `spawn --role counsel`, and their `plan` forms
accept no `--grade`. An explicit grade returns exit 70 and the named rule
`lead_consult_take_no_grade`. `resolve --role lead` and
`resolve --role consult` also take no grade. Other resolve roles default to
grade `C`. Other plan roles require a grade.

### Role effort for lead and consult

A lead or consult never runs above its role's `effort` (`high` in the
current table) unless the request passes `--allow-escalation`. This rule
applies the same way to `spawn -m auto`, `plan`, and `resolve`. `delegate`
routes only as implementer, explorer, verifier, or reviewer, so the cap does
not apply to it.
It does not use task grade. Other roles keep grade-driven selection and
today's ordering.

- A catalog rung above the role effort is not a candidate. Its row records
  `above_role_effort`.
- Suppose the family's role-effort rung is not placed. The seat rules drop
  it, its gate refuses it, or the catalog has no such rung. For example,
  `codex-sol@high` is an unplaced E6 seat and its gate refuses
  `e6_arm_required`. Then panewire skips that family's higher rungs with
  `role_effort_unplaced`. Selection falls through to the next family at the
  role effort, for example `opus@high`. It does not climb to
  `codex-sol@max`. The `routing.skipped` list names each skipped family,
  the role effort, and the cause.
- With `--allow-escalation`, a higher rung is a candidate again and ranks
  by the usual pool order. If one is chosen, `routing.escalation` records
  the profile, `role_effort`, `chosen_effort`, `by: allow_escalation`, and
  the cause from the family's role-effort rung. `resolve` then prints
  `effort=<chosen rung>` instead of the role effort.
- An explicit `--effort` above the role effort also needs
  `--allow-escalation`.

#### Per-family role effort (`family_effort`)

`roles.lead.family_effort` (or `roles.consult.family_effort`) maps a
profile name to the role effort that family leads at, in place of the
role's `effort`. The current table carries `{"codex-sol": "xhigh"}`.

- For a mapped family, the mapped rung is the role-effort rung. Every rule
  above applies to it: rungs above it are `above_role_effort`, and if it is
  unplaced the family is skipped `role_effort_unplaced` with
  `role_effort` set to the mapped effort.
- The entry is the operator's explicit opt-in for that exact rung. panewire
  admits it even when its catalog gate is `escalation`, and a builder seat
  may launch it. Without `--allow-escalation`, other escalation rungs stay
  `gate_class`, and rungs above the mapped one stay excluded.
  With `family_effort` `codex-sol=xhigh`, a lead admits `codex-sol@xhigh`
  (gate `escalation`). It still drops `codex-sol@max` as
  `above_role_effort`.
- Family rule: a key matches a catalog row whose profile is the key. A key
  that names a top profile also matches an OMP row whose catalog model is
  that profile's top model. `omp-sol` runs `openai/gpt-6.1-sol`, so a
  `codex-sol` entry also sets the `omp-sol` role effort. Likewise, an
  `opus` entry covers `omp-opus`. If the map
  names both the profile and its top profile, the profile's own entry wins.
  An `omp-sol` key applies to `omp-sol` only, never to `codex-sol`.
  The role's `allowed` list still decides whether `omp-sol` is a
  candidate at all.
- The selection record's `routing.family_effort` carries the policy map.
  When the chosen rung's role effort came from it,
  `routing.family_effort_chosen` records `profile`, `key`, `effort` and
  `gate`, and text `plan` prints
  `family_effort <key>=<effort> chosen=<profile>@<effort> gate=<gate>`.
  Such a rung is not an escalation, so `routing.escalation` is absent.
  `resolve` prints `effort=<mapped rung>`.

The document remains v1. If it has no `consult` entry, panewire uses its
`lead` rule with the distinct `consult` identity. Lead and consult always
wait on exhaustion and never downgrade. Neither seat role can override its
routing role to obtain weaker eligibility.

### The v1 table

panewire reads the first fenced `json` block of `policy/model-routing`.
`version` must be `1`. `roles.R.allowed` lists catalog profile names, or
`"*"`. The table also supplies `effort`, `on_exhaustion`, `downgrade_to`,
`family_rule`, `fixed`, and, on lead and consult, `family_effort`. Unknown
fields are ignored. Duplicate keys, invalid exhaustion modes, and role maps
naming absent roles are refused. A `family_effort` entry is refused with
`family_effort_invalid` in three cases: it sits on a role other than lead
or consult, its key is neither a top profile nor a profile the role
allows (any profile ID when the role allows `"*"`), or its effort is not
one of `low`, `medium`, `high`, `xhigh`, `max`, or `ultra`.
This PR does not parse the proposed v2 schema.

The seat-role map is read from `panewire_role_map` or its pre-#155 name
`runtug_role_map` (the name hk publishes today); two maps that differ are
refused, and the cache stores the map as `panewire_role_map`. A seat role
the map does not name takes its builtin routing role (`builder` lead,
`worker` implementer, `tester` verifier, `reviewer` reviewer, `counsel`
consult) instead of running unfiltered.

`--routing-role lead|consult|implementer|explorer|verifier|reviewer|judge`
on `spawn` or `plan` selects the routing role. Seat admission still uses
`--role`. Builder and counsel retain their required routing roles.
Delegate maps `reader` to `explorer`, `verifier` to `verifier`, and
`reviewer` to `reviewer`. `--patch` uses `implementer` eligibility.

A valid document is cached atomically at
`$XDG_STATE_HOME/panewire/model-routing.json`, or
`~/.local/state/panewire/model-routing.json`. If hk is unreachable or
unreadable, a usable cache supplies `policy_source=cache@v<N>`.
Without either source, lead, consult, and verifier return
`POLICY_UNAVAILABLE`. The existing v1 fallback for other roles uses
`builtin_default`.

### Accounts and placement

Selection enumerates each eligible profile and effort's current account
bindings on eligible hosts. It groups the same opaque account key across
hosts before applying quota policy. Two logins to one account supply one
quota. Provider-level status never exhausts an unrelated account.

Each account's shared observation supplies its projection, waste risk, and
quota ranking. Unknown observations receive one bounded refresh per account
for the selection round. A transport or host-tool failure can move that
refresh to another holder within the same deadline. A completed measurement
ends the refresh operation. Local preference only reorders holders already
eligible for that binding; it never adds an excluded, disabled, pinned-away,
or capability-ineligible host. If the observation remains unknown, selection
cannot call it exhausted. The exact profile's account-bound gate remains
the final check, including model and free buckets. A host-scoped gate failure
tries the next holder of that account. An account-scoped quota refusal ends
that account attempt without retrying the same quota on another host. Local
judgments retain `source=local` and the selected account key.

Admission is host-first. Hosts that fit the work — required `--tags`
declared, role and grade accepted, not excluded, awake or wakeable — are
ranked: awake hosts under the load threshold, then awake hosts without a load
reading, then awake hosts at or over it (the local `max_load_ratio` guard or
`placement.load_threshold`), then sleepers; inside a bucket by the hub score
(free capacity weighted by host power) when every ranked host has one, else
by load, then configuration order. On each ranked host the ranked account
candidates present there are judged in the pool order above, through the
unchanged quota gate, and the first admitted (host, account) pair wins. An
account-scoped refusal holds on every host, so that candidate is not judged
again; a host-scoped failure only skips the host, and a host a refresh could
not reach is skipped for the round. A host whose present accounts are all
refused is skipped, not fatal. The selection record's `hosts` list explains
each host: `caps`, `caps_match`, `missing_caps`, `load_ratio`, `score`,
`over_threshold`, `rank`, each `tried` account and rung with its `result`
(`admitted`, `refused`, `skipped`) and `reason`, and the host `outcome`
(`chosen`, `all_refused`, `no_candidate_present`, `not_tried`, or why the
host does not fit: `missing_caps:…`, `role_not_accepted:…`,
`grade_not_accepted:…`, `excluded`, `disconnected`). When no host declares a
required capability, the `NO_ELIGIBLE_HOST` refusal names it (`no host
declares cap unity`).

After choosing an account, placement considers only hosts holding it, and
tries the host selection admitted it on first among hosts in the same load
bucket. The
executing host checks the current binding again before launch. An explicit
`--host` limits the eligible accounts. A refusal names that restriction and
does not claim fleet exhaustion. Exhausted Claude accounts do not prevent
selection of an eligible Codex lead, and exhausted Codex accounts do not
prevent selection of an eligible Claude lead.

### Fleet state

Nodes publish the quota readings they measure and their host load to the
panewire hub (panewire `docs/fleet-state.md`). Placement and the quota gate
read the hub's snapshot (`GET /v1/state`, with the `[hub]` operator
credential) once per run, and read it again once it is older than 15 seconds.
With a fresh hub value panewire does not measure on the spawn path.

A hub quota reading is used when both of these hold:

1. **Its publisher is alive.** The publisher's latest heartbeat says
   connected and is at most `state_publisher_ttl_s` (90 s) older than the
   snapshot.
2. **The reading is fresh.** Its `measured_at` is within `[quota]`
   `fresh_ttl_s` of now, the same rule as the shared cache.

When several hosts publish the same account key, the freshest alive reading
wins. The hub host list replaces `panewire place` only when every placement
host is connected with an alive publisher. A sleeping host sends placement back
to `panewire place`, because that call also triggers the hub's wake-on-spill.

Otherwise panewire falls back to the direct path: `panewire place`, the shared
quota cache, and the on-target `pacelever refresh` when that reading is stale.
It never falls back silently. The placement record, and the selection record
inside it, carry a `state` object:

```json
"state": {
  "hosts": {"source": "hub"},
  "quota": {"claude:0123456789abcdef": {"source": "direct", "reason": "publisher_dead"}}
}
```

Each `quota_gate` judgment also carries its own `state`. The reasons are
`hub_state_off`, `hub_unavailable`, `hub_absent` (no value for the key),
`publisher_dead`, `hub_stale` (alive, but past the TTL) and `hub_incomplete`.

Rollout order is hub, then nodes, then panewire. To roll back panewire alone, set
`[hub] state = "off"`.

### Outcomes

Every selection returns an `outcome_code`. Success is `ADMITTED`.
When no candidate is admitted, the codes are distinct:

| Outcome | Exit | Meaning |
| --- | --- | --- |
| `QUOTA_EXHAUSTED` | 86 or 87 | Every eligible account is measured exhausted. Exit 86 retains wait semantics; exit 87 retains escalation semantics. |
| `QUOTA_UNKNOWN` | 89 | At least one eligible account remains unknown after a bounded refresh. |
| `AUTH_UNAVAILABLE` | 88 | Eligible hosts have no usable current account binding. |
| `NO_ELIGIBLE_HOST` | 81 | Host, tag, capability, connectivity, exclusion, or transport constraints leave no eligible host. |
| `POLICY_UNAVAILABLE` | 70 | Required policy, catalog evidence, or gate-tool configuration is unavailable. |
| `NO_ROLE_CANDIDATE` | 77 | No active profile passes the role and static admission constraints. |

Exhaustion ranges over actual eligible account bindings. If every bound
account is measured exhausted, another profile with no login or capable
host does not mask QUOTA_EXHAUSTED. Unknown quota or an authentication,
transport, or policy failure on a bound account still prevents that claim.
If no account is bound, missing-login or missing-host outcomes apply. An exhausted v1 role with `on_exhaustion=downgrade_to` retries
once using only its listed profiles and records `downgraded_from`.
Lead and consult never take this path. Exit 86 reports a refusal, not a
scheduled retry.

Account-bound gate refusals validate the same schema, account, pool, and
process/receipt result as successful gates. Recognized `outcome_code`,
`reason_code`, or machine refusal flags determine their category. Diagnostic
prose never determines exhaustion. An ambiguous or unreadable exit-3 receipt
remains `QUOTA_UNKNOWN`, even when `used_pct` is present. This includes prose,
invalid JSON, missing or inconsistent identity/result fields, unsupported
schemas, and conflicting refusal codes. Selection refreshes that account once
within its bounded deadline, then reports `QUOTA_UNKNOWN` if evidence remains
unknown. An invalid success receipt or an actual non-3 configuration failure
remains `POLICY_UNAVAILABLE`. Publishing stable typed reason codes from
pacelever is tracked separately as hk 2477.

Legacy `QuotaGate` adapters can use raw gate exit 3 for quota-blocked
compatibility. That interpretation is attached only at the gate boundary;
a refresh process returning 3 is an infrastructure error, not measured
exhaustion. Adapters with broader exit-3 meanings must provide a typed
refusal.

### Record and resolve

`plan --json`, `resolve --json`, and the durable
`job.placement.selection` record carry `routing_role`, `account_key`,
`host`, and `outcome_code`. `account_key` contains only the opaque quota
key. Account candidates and gate attempts retain their individual evidence.
The nested `routing` object carries `routing_version`, `policy_source`,
the allowlist, and the exhaustion rule. The hk job metadata schema is
unchanged.

`panewire resolve --role lead|consult|implementer|explorer|verifier|reviewer|judge
[--grade G] [--t T0..T3] [--builder-family F] [--harness KIND] [--allow-escalation] [--json]` runs the same
selection core without a launch or job write. `--t` defaults to `T0`.
Reviewer and judge require `--builder-family` to exclude that family.
The result names the account and host selected now; it is not a reservation.
The text answer ends with `profile=<profile> effort=<effort>`.
`effort` is the role's starting effort, or the selected rung's when absent.
For a lead or consult rung chosen above the role effort with
`--allow-escalation`, `effort` is the chosen rung's.

`--harness omp` restricts selection to OMP seats before quota ordering. For
`--role implementer` or `--role explorer`, an OMP choice also returns
`omp_agent` when a user agent's frontmatter `model:` exactly matches the
selected `model_id`. Panewire reads only the frontmatter of regular Markdown
files in `~/.omp/agent/agents/`, in filename order. The first matching agent's
`name:` supplies the hint. Missing or malformed frontmatter leaves the JSON
field absent. Text output appends `omp_agent=<name>` when available.

## Delegation

`panewire delegate` is the one-call form of "ask another model": a lead session
(Claude Code, Codex) hands over reading, verification or a cross-check and gets
one answer back. In-session subagents run only on their own vendor's models;
`delegate` reaches any seat in the policy, so the work can run on free or
spare-quota models. It reuses the spawn, wait and job-result machinery and
writes nothing in the caller's tree.

```sh
panewire delegate --role reader|verifier|reviewer --prompt FILE [--grade G] [--cwd DIR]
  [--timeout D] [--headless] [--patch] [--panel 2..4] [--family F] [--workspace W] [--json]
```

| Flag | Meaning |
| --- | --- |
| `--role` | `reader` reads and answers, `verifier` runs only the acceptance commands in the prompt, `reviewer` reviews. Reader and verifier use `worker` seats; `reviewer` uses `reviewer` seats and excludes the caller's model family. |
| `--prompt FILE` | The task. The role preamble and the steps for filing the answer are put in front of it. |
| `--grade G` | The catalog grade the seat must cover (`C`, `B`, `A`, `A+`, `S`, `S+`; default `C`). The task title is not read. |
| `--cwd DIR` | The tree the delegate reads (default: the current directory). |
| `--timeout D` | A duration or seconds, default 15m: the pane wait, or the headless CLI call. |
| `--headless` | Run the selected seat's CLI once on this host with the brief, instead of a pane. |
| `--patch` | The delegate works in a temporary detached worktree and the result carries a patch path (below). |
| `--panel N` | N from 2 to 4: see Panel. |
| `--family F` | The caller's model family, for a reviewer outside a panewire job. Inside a job the job's own family is used. |
| `--workspace W` | The herdr workspace for the pane, or `[local] workspace`. Not needed with `--headless`. |
| `--json` | Print one JSON document instead of text. |

**Selection.** The seat is chosen by the same code as `spawn` auto-selection
and `panewire plan`: catalog rungs that cover the grade and the role, ordered by
the pool policy (waste-risk pools first, then the tier order), admitted through
presence and the quota gate. Free models are one tier of that policy, not a
default target. A reviewer drops every seat of the caller's family.

**Job record.** Each delegation is a job: `delegate-<parent>-<n>`, where
`<parent>` is the enclosing job (`ARBITER_JOB`), and `<n>` the next free number
in the local jobs dir. Outside a job the id is `delegate_<host>_<n>`, where
`<host>` is `HUB_MACHINE_ID`, so two hosts never hand out the same id to hk;
without a node id the delegation refuses. It is claimed in hk and the local ledger,
holds a quota lease, carries the parent job, task and owner lane, and ends with
`done`, so placement, quota and hk records match any other job. A task is
required: run inside a panewire job or set `HK_TASK_ID`. The job directory holds
`brief.md`, `result.json`, `report.md` and, with `--patch`, `changes.patch`.

**Pane mode (default).** `delegate` runs `spawn --wait` with automatic
selection (the pane may be placed on another host), reads the `result` of the
hk job record, which the delegate files with `panewire done JOB --result`
(`{"answer": TEXT}`), then closes the pane after checking it is still the one
the job spawned. If only the report exists because the completion watcher
finished the job on the delegate's terminal signal, the report text is the
answer. A delegate placed on
another host is answered the same way; its pane is left for that host's `reap`.
When the wait fails (escalated, lost, timeout), the pane stays for
`panewire reap` and nothing is printed on stdout.

**Headless mode.** The seat's CLI runs once, in `--cwd`, with the brief and a
deadline of `--timeout`; its output is the answer. The job is claimed, leased
and completed with the same `done` call. Kinds with a one-shot form: `claude`,
`codex`, `devin`, `cmd`, `omp`, `opencode`; any other kind is refused
with exit 70 before anything is claimed. The one-shot flags are fixed in code
and have been exercised only against a fake runner.

**No writes.** The role preambles forbid editing: `reader` and `reviewer` run
read-only commands, `verifier` runs only the acceptance commands it was given.
A role that must try a change uses `--patch`: delegate adds a detached worktree
of `--cwd` in a temporary directory (at `HEAD` plus the caller's uncommitted
tracked changes), runs the delegate there, writes `git diff` against that base
to `changes.patch` in the job directory and removes the worktree. The result
carries `patch` (omitted when nothing changed). The caller decides whether to
apply it (`git apply`). The worktree registration in `--cwd`'s `.git` is added
and removed; no file in the tree changes.

**Panel.** `--panel N` picks N seats of N distinct model families, then runs
all N at once, each as its own delegate job (pane or `--headless`, and its own
scratch tree with `--patch`). Families are chosen one selection at a time with
the families already chosen excluded, so the panel is the N best admitted
families under the normal policy. When fewer than N are admitted, the panel
runs with those and says why (`reason`, and a warning on stderr). A member that
fails, is lost or times out does not stop the others; it is listed as `failed`
with its exit status. The panel tells each model to file `verdict` (one word)
and `points` (short claims, `file:line` where they have one) next to its
`answer`; without points, lines of the answer are compared. The table lists:

- **agreements** — points two or more members made (word overlap of at least
  half);
- **disagreements** — differing verdicts, and `file:line` locations that members
  cite with different claims;
- **unique** — points only one member made.

**Output.** The answer is the last thing printed. Text: a header line
`delegate job=… role=… mode=… profile=…`, `patch=PATH` when there is one, then
the answer. Spawn and wait chatter goes to stderr. `--json` prints one object:
`job`, `role`, `mode` (`pane` or `headless`), `profile`, `family`, `patch`,
`result` (the filed object) and `answer`. A panel prints a header line, each
member (`== A job=… profile=… family=… status=answered ==` and its answer),
then `== side by side ==` with verdicts, agreements, disagreements and unique
points last. Its `--json` object has `requested`, `answered`, `reason`,
`members` (each the single-call object plus `label`, `status`, `exit`, `error`)
and `table`.

**Exit statuses.** 0 the answer was printed (a panel: at least one member
answered; check `answered` and `reason` for a short panel). 2 usage. 70
configuration, or a seat kind with no headless form. 75 the parent job is
unreadable. 77 or 81 no seat or host was admitted (a panel with no admitted
family at all). 83, 84, 85 the pane job escalated, was lost or timed out. 124
the headless CLI hit `--timeout`. 1 the job finished without an answer, or
the headless CLI failed. Spawn and quota refusals keep their own statuses. A
panel with no answered member returns the first failed member's status.

**When to delegate.** Delegate reading, verification and cross-family review.
Keep tight design loops, where the next step depends on the last answer, in
the session's own subagents.

`plan` also prints the remote-spawn preflight (below) of every remote host it
evaluated, as `preflight host=NAME ok=… reasons=…` lines and, with `--json`, a
`preflight` array. It is informational and never changes the plan's exit status.

### `panewire preflight`

`panewire preflight [--host NAME] [--pool P] [--json]` answers "would a remote
spawn to this host work" before one is tried. Six remote-spawn failures each
needed their own fix; the preflight reports all of them per host, each with a
named reason. It is read-only: it reads the hub host list, hk presence and the
shared observation, and makes one SSH probe per host that tests two paths,
reads one boolean from the remote `hosts.toml` and asks `panewire` and `panewire`
for their versions. It never prints a URL or a file's contents, and writes no
task, job or event.

| Check | Fails with | Meaning |
| --- | --- | --- |
| `hub_acceptance` | `hub_unavailable`, `not_listed`, `not_accepting`, `disconnected` | The hub lists the host, it is connected and not `not_accepting`; the detail carries the hub's reason string. |
| `quota_observation` | `POOL:no_usable_presence`, `POOL:degraded`, `POOL:not_observed`, `POOL:stale`, `presence_unavailable` | Per pool the host's presence names (or `--pool`), the shared observation of its account exists and is no older than `fresh_ttl_s`; the detail shows `age=` against `fresh_ttl=`. |
| `quota_transport` | `hk_url_unset`, `hk_url_invalid`, `plaintext_non_tailnet`, `plaintext_tailnet_not_allowed`, `remote_config_missing` | With an `https` hk URL nothing is needed. A plain-`http` tailnet URL needs `[quota] allow_plaintext_tailnet = true` in the remote `hosts.toml`. |
| `hub_load` | `load_unknown`, `memory_pressure`, `load_over_threshold` | The hub has a load reading for the host (its metrics source lists it), reports no memory, swap or pressure reason, and the load is under `placement.load_threshold`. The memory match is on the hub's reason string and is a heuristic. |
| `review_config` | `absent` | `~/.config/mergewake/config.yaml` exists on the host (existence only; the file is not read). |
| `version_skew` | `panewire_skew`, `panewire_skew`, `panewire_missing`, `panewire_missing`, `*_local_unknown` | `panewire --version` and `panewire --version` on the host equal this machine's. |

A host that cannot run the probe (`unreachable`, `via_hub_unsupported`,
`invalid_ssh_target`, `probe_failed_rcN`) fails the checks that need it
(`quota_transport` when the URL is plain http, `review_config`,
`version_skew`).

The text output is one `preflight host=NAME ok=… [reasons=check:reason,…]`
line and one indented `check NAME ok|refused reason=… detail=…` line per check.
`--json` prints `{"mode": "preflight", "ok", "hosts": [{"host", "ok",
"checks": [{"name", "ok", "reason", "detail"}]}]}`. Exit 0 when every checked
host passes, 78 when any fails, 2 for usage or an unknown `--host`, 70 when no
remote host is configured.

`--host auto` runs the same preflight for each remote candidate it refused and
records it on that candidate's row of the `job.placement` record as
`preflight: {ok, reasons, failed}`. A host the quota gate refused was never
contacted, so its SSH-probe checks are `skipped` and only the hub and hk
checks report. When no host accepts, the reasons are also appended to the
placement error, because a failed spawn writes no placement record.

## `[artifacts]`

| Key | Default | Meaning |
| --- | --- | --- |
| `drive_folder` | required for move | Absolute or HOME-relative Drive root; destination must be beneath it as host/name. |
| `fpbulk` | required for apply | Executable accepting `status`/`evict` with newline-separated paths on stdin. |
| `exclude` | required for move | Glob array; built-in credential/.git exclusions also always apply. |
| `batch_bytes` | 3 GiB | Maximum bytes per batch, except one oversized file may occupy its own batch. |
| `batch_files` | 2000 | Maximum files per batch. |
| `poll_seconds` | 30 | Positive upload-status polling interval. |
| `confirm_timeout_seconds` | 21600 | Total batch-confirmation window; must be at least confirm_gap_seconds. |
| `confirm_gap_seconds` | 120 | Two successful fresh reads must be separated by at least this duration; values below 120 refuse. |
| `stale_minutes` | 10 | Newest DriveFS log must be within this freshness window. |
| `drivefs_log_dir` | required for apply | DriveFS log directory. |
| `app_check` | `pgrep -x 'Google Drive'` | Operator-owned shell predicate proving the Drive client runs. This is the only configured shell predicate. |
| `scratch_dir` | `~/.local/share/fleet-upload/scratch` | Temporary per-batch remote staging; removed after the batch or on error. |
| `free_floor_bytes` | 20 GiB | Space that must remain after remote batch staging. |
| `local_delete_at` | `drive_confirmed` | Sweep hatch: `drive_confirmed`, or `uploaded` for uploaded nonevidence rows without a mirror requirement. |
| `state_dir` | legacy | Retired uploader state is left untouched. Panewire uses handoffkeep records. |
| `alert_lane` | legacy | Existing value is retained; panewire returns explicit confirmation failure instead of sending uploader alerts. |

Artifact policy is the embedded
[artifact-policy.json](../internal/fleet/artifact-policy.json), or `sweep --policy`.
Its compatible schema names relative work roots and ordered classification rows
(`class`, `root`, `glob`, optional `host`, `job`, `keep_newest`, `mirror_required`).
Only evidence/final-build classes are sweep candidates; all other classes and
unmatched files are retained. `keep_newest` is accepted for policy compatibility
and cannot widen sweep eligibility. This version limits deletion to the selected
job's own inbox subtree even if a wider root is configured.

## Policy files and environment

`PANEWIRE_POLICY` selects the seat-policy JSON. Each profile requires `canon`,
`kind`, `family`, `capability` and nonempty `roles`. Optional `effort` and `pin`
fix an alias to a rung; `e6` requires an exact marker. A nonempty `rungs` map is
closed: unlisted efforts are refused. A rung has `roles` and optional `e6`.
Roles are worker, builder, tester, reviewer and counsel. Model IDs and grades
are read from the quota tool, never this file.

`PANEWIRE_BRIEF_POLICY` overrides `[local].brief_policy`. Brief policy schema:

```json
{
  "version": 1,
  "default": "fleet-v1",
  "blocks": { "fleet-v1": "Short host safety instructions", "host-strict": "Host-specific safety instructions" },
  "hosts": { "host-a": "host-strict" },
  "profiles": { "example-profile": "fleet-v1" }
}
```

Use the complete shipped block when creating a custom policy. Selection order
is default → host → profile → local.brief_preamble. Missing block names refuse.
The policy version and selected block appear in every brief and dry-run output.
See [DESIGN.md](../DESIGN.md#standard-brief-preamble-the-single-source-of-fleet-rules)
for ownership and distribution.

| Environment | Meaning |
| --- | --- |
| `PANEWIRE_POOL_POLICY` | Overrides the pool-order policy used by `spawn` automatic model selection; `[local].pool_policy` takes precedence. |
| `HERDR_BIN`, `PANEWIRE_BIN` | CLI executable overrides. |
| `HANDOFFKEEP_BIN` | Must resolve to the canonical current-user ~/.local/bin executable in production. |
| `XDG_DATA_HOME` | Data home of the local ledger (`$XDG_DATA_HOME/arbiter/state.db`); production guard requires the canonical `~/.local/share`. |
| `XDG_STATE_HOME` | State home of the role routing cache (`$XDG_STATE_HOME/panewire/model-routing.json`); default `~/.local/state`. |
| `PANEWIRE_QUOTA_BIN` | Quota executable override; default `pacelever`. |
| `PANEWIRE_NODE_ENV` | Panewire node env file for `HUB_MACHINE_ID` (job record host id); default `~/.config/panewire/hub-node.env`, or `[quota].node_env`. |
| `ARBITER_INBOX_ROOT` | Jobs root; production guard requires the canonical inbox. |
| `HERDR_SESSION` | Herdr session passed through or selected for remote execution. |
| `PANEWIRE_E6_ARM` | Exact canon@effort experiment marker when policy requires one. |
| `ARBITER_JOB`, `HK_TASK_ID` | Calling job/task inheritance; populated in spawned panes. |
| `PANEWIRE_LANE_STRICT=1` | A committed builder with unregistered lane returns 82. |
| `PANEWIRE_COMPLETION_TIMEOUT_S` | Positive watcher lifetime; duration or bare seconds, default 1h. Expiry retains the job. The watcher completes a job only on a [terminal signal](r21-lane-event.md#job-completion-signal), never on a stable report alone. |
| `MOCK_MCP_PROFILE`, `PANEWIRE_MOCK_MCP_CONFIG` | Required reviewed mock profile name and exclusive stdio mcpServers JSON. |
| `CLAUDE_CONFIG_DIR`, `CODEX_HOME` | Harness config homes for selected-workspace trust; mock Codex uses its private job home. |

Legacy periodic prune environment switches are not read. Panewire never
silently enables automatic pruning.

## Local job ledger

Since RT-arb the host-local ledger lives in panewire (`internal/arbiter`); panewire
no longer executes the agent-skills `arbiter` script, and `ARBITER_BIN` is not
read. hk job records hold the cross-host job state; the ledger keeps what they
do not replace:

| Part | What it holds |
| --- | --- |
| Jobs | One row per job claimed on this host (`claimed → leased → released`), written with its `job.claim`/`job.reclaim` event. The duplicate arbiter on this host; cross-host duplicates are refused by the hk claim first. |
| quota_pool records | One execution record per job and pool, written at admission and deleted at `done`, `reap` or a failed spawn. No TTL. |
| path, linear_permit leases | Exclusive leases with a TTL (90 and 30 minutes by default), a monotonic fencing token, archived history and expiry by `gc`. Library only: panewire itself takes no such lease today. |
| Job events | `job.*`, `lease.*` and `quota_pool.*` events, one artifact each. |
| gc | `panewire gc [--json]`: expires leases, cleans quota_pool records and releases leased jobs whose exact agent label is gone from Herdr. |

State is read and written in place; no migration or import is needed:

| What | Path |
| --- | --- |
| Database (SQLite, schema version 2) | `$XDG_DATA_HOME/arbiter/state.db`, default `~/.local/share/arbiter/state.db` |
| Event artifacts | `<jobs root>/<job>/events/NNNNN-<kind>.json`, jobs root `~/work/herdr-inbox/jobs` or `ARBITER_INBOX_ROOT` |
| Events lock | `<jobs root>/<job>/.wrk-events.lock` |

The script and panewire may share one database: both write with `BEGIN
IMMEDIATE`, a 10 s busy timeout and the same events lock, and artifact numbers
continue after the highest existing prefix. A schema version other than 2 is
refused (exit 6 from the ledger, no migration). The script stays installed for
its remaining readers (for example the quota CLI's `arbiter status --json`).

Behaviour differences from the script:

- **No local claim is needed to record an event.** A job placed on another host
  has no jobs row here; its `job.placement` event is recorded anyway, which ends
  the `placement decision record failed: arbiter: job not registered` warning on
  a remote spawn. Claim event kinds are still refused, and the first event
  creates `state.db` if it does not exist (the script created it only on a claim).
- **The pool comes from the gate output panewire just received.** The profile
  named there must match and conflicting audit tokens are refused, as before,
  but the second cross-check of the pool against the quota tool's provider
  list (`scopefuel --json`) is not repeated. The gate output is passed in
  memory, not through a temporary file.
- Event artifacts are the same JSON objects with sorted keys; the separators are
  compact (`,` and `:`) where the script wrote `, ` and `: `.
- A read of the quota records of a ledger that does not exist yet answers none
  (the script failed with exit 6), so `done` and `reap` on a job that never ran
  here do not fail.
- **A job is released only when it holds nothing.** Releasing one quota_pool
  record or lease leaves the job `leased` while it still has another record or
  an unexpired lease (the script marked it released at once), so it cannot be
  reclaimed while a pool is still consumed. An expired lease holds nothing.
- **A new ledger is owner-only** (state directory 0700, `state.db` and its
  `-wal`/`-shm` 0600) because the database holds the full fencing tokens. A
  ledger that already exists keeps its modes; tighten it with `chmod 700` on
  `~/.local/share/arbiter` if it was created with the script's default.
- Failures keep the script's exit statuses (3 denied, 4 duplicate, 5 fencing,
  6 storage, 7 not found) and surface as `arbiter <operation>: ...`.
- The trust guard no longer checks an `arbiter` binary (there is none to run);
  `HOME`, `ARBITER_INBOX_ROOT` and `XDG_DATA_HOME` must still select the
  canonical paths. `ARBITER_TEST_NOW` is not read; tests inject a clock.
- Not ported: the script's own command line. Operators keep using the script for
  `status`, `job-get`, `lease`, `release` and `event` by hand.

## Fleet events

panewire appends one fleet-event v1 line (JSON: `ts, host, tool, kind, job,
outcome, reason, detail`) to `$XDG_STATE_HOME/panewire/events/fleet-events.jsonl`
(default `~/.local/state/panewire/events/fleet-events.jsonl`) for:

| `kind` | `outcome` | `reason` | When |
| --- | --- | --- | --- |
| `spawn`, `plan` | `refused` or `failed` | the exit status name (`config`, `seat_refused`, `placement`, ...) | the command exits non-zero; `detail` is `exit=N <message>` |
| `placement` | `placed` | ranking metric: `score`, `raw_load` (some host had no hub score), `single_candidate`, `wake`, `forced` | a non-dry-run spawn chose a host; `detail` names `chosen=`, `candidates=`, `pool=` and the hosts that `refused=` first |
| `quota_observation` | `stale` or `unusable` | the shared-observation reason (`stale`, `not_observed`, `measure_failed`, ...) | a target account's quota observation refused a host; `detail` carries `target=`, `pool=` and `age_min=` |

Every field is redacted (tokens, URL credentials) and bounded (`detail` at
1 KiB); the file rotates to `.1` at 4 MiB. Writing an event never changes the
command's result. Grafana Alloy ships the file to Loki; the shipping, dashboard
and alerts live in panewire `deploy/alloy` and `deploy/grafana`.

## Placement record producer

Every real spawn decision produces one immutable handoffkeep record through
`POST /v1/placements`. This includes automatic and forced placement, local and
remote sessions, headless delegation, and refusals or failures before launch.
Dry runs produce no record. See handoffkeep's
[Linear work board contract](https://github.com/mgh3326/handoffkeep/blob/main/docs/linear-work-board.md).

The record contains `job_id`, a local per-job `attempt`, numeric `task_id` when
known, `decided_at`, `tool_version`, `source` (`auto` or `forced`), `ranking`,
`state_source`, `chosen_host`, `outcome` (`placed`, `refused`, or `failed`), and
a reason code. Candidates include scores, available hub signals, and skip
reasons. Missing signals stay absent. Forced placements can have an empty
candidate list. The server derives `decided_on` from the node token's host grant.

Execution metadata includes `family`, the admitted `model`, `role` (`lead`,
`worker`, `review`, or `consult`), `profile`, `pool`, `pane_id`, the repository
name, and an opaque `account_key`. Account fingerprints remain fingerprints.
Other account aliases become hashes. Briefs, local paths, login identities,
credentials, and terminal output do not enter the record. Mergewake passes the
owning task with `--task` and `--role reviewer`; the producer records that task
and `role=review`.

The initiating panewire owns delivery. SSH executors are marked delegated, and
panewire hub executors use the node-owned `hub-directed` override reason. These
executors return final launch metadata in the `placement=` part of their `OK`
receipt. They do not create a second decision. For an older executor, panewire
resolves the shared hk job record before posting and freezes that payload for
retries. A successful placement stays `placed` if a later `--wait` fails.

Contacting a remote host does not count as starting a launch. An executor that
fails after attempting its launch returns `FAILED placement=` metadata; only
that evidence or a success receipt marks the host as chosen. A refusal keeps
the contacted host in the candidate trace. Placement clears launch state when
it moves to another candidate. Older executors without failure receipts are
conservatively recorded as refusals when no acceptance evidence is available.

Delivery uses the existing hk client and `[quota]` URL and credentials. Each
delivery pass has a two-second budget and cannot change the spawn result.
Records first enter owner-only JSON files under
`$XDG_STATE_HOME/panewire/placements` (default
`~/.local/state/panewire/placements`). Each subsequent panewire invocation retries
up to 32 pending records within the same budget; unrelated files do not count.
A saved delivery cursor rotates attempts across invocations so unresolved
legacy jobs cannot starve later records. A failed delivery warns and
keeps the file. An identical `(job_id, attempt)` payload replays as HTTP 200.
HTTP 409 warns and drops the conflicting file. Local attempt markers under
`placement-attempts` survive delivery so later decisions do not reuse keys.

## Exit statuses

| Code | Meaning |
| --- | --- |
| 0 | Success or preview. |
| 1 | Generic failure: no, ambiguous or multiple `find` matches; restore receipt failure after reclaim. |
| 2 | Usage/task/policy target refusal. |
| 3 / 4 | Quota-tool refusal, unchanged; artifacts use 3 for upload confirmation timeout and 4 for unconfirmed Drive eviction. |
| 5 / 6 | Hub quota denial / authentication refusal. |
| 70 | Configuration or trust refusal, including `POLICY_UNAVAILABLE` and `lead_consult_take_no_grade`. |
| 74 | Active job duplicate, including an hk `job_id_taken` claim refusal. |
| 75 | Unreadable duplicate state (the ledger's record of a duplicate job has no state or an unreadable `job.spawned` receipt; a released job has no receipt and is reclaimed), changed artifact, or a `--parent-job` that resolves neither locally nor in hk within the bound. |
| 76 | `BRIEF_SUBMISSION_UNCONFIRMED`: no working transition or submitted brief turn on the launch session and pane. Codex retries a submit key once only for composer residue; other harnesses retain one submit-key retry when submission remains unconfirmed. |
| 77 | `NO_ROLE_CANDIDATE` or seat, grade, family, or E6 refusal; quota gate exit 5 is remapped here. |
| 78 | `preflight`: at least one checked remote host failed a check; the reasons are on stdout and in the error. |
| 79 | Mock isolation refusal. |
| 80 | Local spawning disabled/unavailable without a remote fallback. |
| 81 | `NO_ELIGIBLE_HOST`, wake/capacity placement failure, the remote worktree for a linked-worktree job could not be prepared (also a `via = "hub"` checkout refusal), or the hub is unavailable for a forced `via = "hub"` host. |
| 82 | Builder pane committed but lane unregistered under strict lane reporting. |
| 83 | `spawn --wait` (and `delegate`): the job escalated. |
| 84 | `spawn --wait`: the job was lost or revoked. |
| 85 | `spawn --wait`: the wait deadline expired; the job is retained. |
| 86 | `QUOTA_EXHAUSTED`: every eligible account is measured exhausted and the role waits, including an exhausted downgrade retry. |
| 87 | `QUOTA_EXHAUSTED`: every eligible account is measured exhausted and the role escalates to the parent lane. |
| 88 | `AUTH_UNAVAILABLE`: no usable current account binding on eligible hosts. |
| 89 | `QUOTA_UNKNOWN`: quota remains unknown after one bounded account refresh. |
| 90 | `WORKSPACE_TRUST_REQUIRED`: Codex is waiting at its directory trust prompt; initial brief is withheld. Or `HOOKS_REVIEW_REQUIRED`: Codex is waiting at its "Hooks need review" screen; initial brief is withheld. |
| 124 | A time-bounded CLI call reached its context deadline. |
| 255 | SSH could not reach a remote host (repository check, clone or spawn leg). Never returned for a `via = "hub"` host. |

`HOOKS_REVIEW_REQUIRED` shares status 90 with the trust prompt: both mean Codex is
waiting for the host owner at a first-run screen and the brief was withheld. Codex shows
"Hooks need review" at launch when hooks are new or changed. panewire fails at the first
read of the screen instead of waiting for the footer deadline (rc 70, `Codex model/effort
footer not proven`). It never accepts, dismisses or answers the screen. Fix: the host
owner reviews the hooks in Codex on the executing host (the executing `CODEX_HOME`), then
repeats the same spawn. The hub records the refusal as `spawn_failed:hooks_review`.

Unexpected dependency failures preserve their subprocess exit code. A pane
success receipt is always authoritative for avoiding a second spawn, even with exit 82.

## Task presets

`panewire work TASK_ID` and `panewire lead TASK_ID` expand task metadata into
normal spawn flags. Both use `-m auto --host auto -w workers`, derive `--t`
from the title marker, and pass the numeric task binding. The task marker
continues to supply grade to admission. Lead uses the existing builder role
and lead routing policy; it does not turn grade into a lead policy override.

Job/label default to `refs.job_id`, otherwise `task<ID>-work` or
`task<ID>-lead`. Lead's lane defaults to the job id. Working directory comes
from `refs.worktree.path`, `refs.worktree`, `refs.cwd`, then the current directory.
Brief/report use `refs.brief_path` / `refs.report_path`, otherwise
`<jobs>/<job>/brief.md` and `report.md`. Caller lane comes from `PANEWIRE_LANE`,
`local.lane`, or the calling `ARBITER_JOB` metadata. Work uses it as `--owner`;
lead uses it as `--parent`. An unknown caller lane names the flag to supply.
Flags after the task id override defaults. Save the brief first; presets do
not invent or overwrite instructions. A dry run uses the same planning path
as the equivalent long spawn command and creates no claim or worker.

## Host aliases and workspaces

Canonical `[hosts.<machine-id>]` keys name hub machines. Set
`aliases = ["short-name"]` in that table; an existing SSH config alias also
resolves to its host key. No fleet-specific mappings are built in. Ambiguous
aliases refuse and name the canonical-key fix. Resolution prints the alias
and resolved machine id. The optional `local.aliases` list names this machine.

Omitting `-w` uses the executing host's `local.workspace`. Remote delegation
uses the target host's configured workspace, not the caller's label. A label
such as `workers` is resolved through that host/session's `herdr workspace
list`; zero or multiple matches refuse and tell the caller to pass the exact
workspace id. An explicit workspace id is used directly.

## Failed spawn retries

A failed launch with confirmed pane cleanup releases its local claim and
quota evidence and marks the identical host/task claim retryable instead of
terminal. Repeat the same command on the same executing host. A live duplicate
still exits 74. Cleanup failures remain fail-closed and preserve the quota
record; inspect the pane before retrying. An old terminal job id is never
revived. Cross-host accepted-spawn orchestration is handled by the hub phase
of this migration, rather than changing the external job store's claim rules.

Spawn refusals retain their exit codes and print an action to repair the
specific missing flag, workspace, account, quota evidence, task binding, trust
approval or harness launch failure. `panewire heavy` remains removed and points
to load-based `panewire place`; it creates no host lock.

### Lane job listing

`panewire jobs list --lane LANE [--json]` lists active, orphaned, and completed
retained jobs owned by the lane. Text rows contain job ID, pane, machine, state,
start time, and last event kind. The command accepts `--hub-url`,
`--hub-token-env`, and `--hub-cf-env`, defaulting to `PANEWIRE_HUB_URL`,
`PANEWIRE_HUB_TOKEN_ENV`, and `PANEWIRE_HUB_CF_ENV` respectively.

A node token may read only a lane registered on its own machine; jobs owned by
that lane can have run on other machines. Requests for other lanes receive 403,
and node requests without `owner_lane` receive 401. The hub trusts reporting
nodes to attribute jobs to lanes: an active heartbeat or a late terminal event
can name a lane registered on another machine, and the hub does not verify that
the job belongs to that lane. A compromised node can therefore insert misleading
entries into another lane’s listing. Authoritative job-to-lane proof is tracked
in hk task 2720.

An operator can list all
active jobs as before with `panewire jobs jobs`; add `--include-completed` to
include retained completed records. The hub does not track a separate reaped
state, so a retained completed record can still appear after reap.
