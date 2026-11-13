# Hub execution

Real spawn/work commands now use [durable hub operations](hub-spawn-operations.md). Configure the hub operator credential in the fleet hosts config and provision node spawn/work-root policy. There is no launcher subprocess or SSH launcher fallback. The imported planning and dry-run policy details below still apply.

# Spawn configuration

Panewire reads `~/.config/fleet/hosts.toml`. `FLEET_HOSTS_CONFIG` overrides the file;
otherwise `XDG_CONFIG_HOME/fleet/hosts.toml` is used when XDG_CONFIG_HOME is set.
For one release a config found only at the retired `~/.config/wrk/hosts.toml`
path (or through `WRK_HOSTS_CONFIG`) still loads with a one-line warning naming
the new location. The same applies to retired environment names: `WRK_LANE_STRICT`,
`WRK_COMPLETION_TIMEOUT_S`, `WRK_MOCK_MCP_CONFIG` and `SCOPEFUEL_E6_ARM` fall back
to their `PANEWIRE_*` replacements with a warning; a set `SCOPEFUEL_BIN` is ignored
with a warning. The workspace trust lock stays `.wrk.lock` until old spawner
binaries are drained from every host. An own `.wrk.lock` left group-writable (for
example by umask 002) is narrowed to 0600 through the open fd with a warning; any
other insecure lock is refused naming its path, owner, mode and the failed rule.
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
omp_commandcode = false

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
tags = ["linux", "gpu", "unity"]
presence_id = "host-b"
cwd_map = { "/work/project" = "/srv/work/project" }

[placement]
load_threshold = 1.0
wake_after_s = 600

[quota]
url = "https://hk.example.invalid"
presence_ttl_s = 86400
fresh_ttl_s = 300
accounts = { pool-a = "0123456789abcdef" }
pool_max_concurrent = { "openai-api" = 2 }

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
| `workspace` | unset | Herdr workspace for local spawns, named by label (`workers`), not by id. Herdr deletes a workspace when its last pane closes; spawn recreates a missing label with `herdr workspace create --label` before creating the tab. A configured id is used only while it still exists. |
| `heavy_load_wait` | legacy | Accepted and ignored; the `heavy` command was removed. |
| `shell_ready_timeout_ms` | `120000` | Startup readiness window, integer 30000–300000. |
| `brief_submit_timeout_ms` | `120000` | How long a spawn waits for the brief to start a turn before it refuses with 76, integer 30000–600000. Raise it on a host that runs under sustained load. |
| `brief_submit_retry_ms` | `15000` | How often the submit key repeats during that wait, at least 1000 and below the timeout. It goes only to an idle pane; Claude and Codex panes must also still show the brief in the composer. The brief is never pasted twice. |
| `cmd_dsflash41` | `false` | Literal capability opt-in required by cmd policy rows. The provisioned cmd account must have no persisted MCP servers. |
| `clinepass` | `false` | Literal capability opt-in required by the `cc-*` seats, which route the Claude Code harness through the host's local ClinePass proxy (`CLINEPASS_GATE_URL`, key read at spawn time from `CLINEPASS_GATE_KEY_FILE`; defaults are a loopback URL and `~/.config/cliproxyapi-key.txt`). |
| `omp_commandcode` | `false` | Literal capability opt-in required by `omp-dsflash41`, `omp-musespark13`, `omp-argon4`, `omp-mimo26pro`, `omp-cc-opus55`, `omp-cc-sonnet55`, and `omp-cc-sol61`: the host's omp has the Command Code provider configured (`omp models` lists the selected model). Unrated rows admit only with `PANEWIRE_E6_ARM`; Command Code rules are in [quota](quota.md#command-code-seats). |
| `report_user_idle` | `false` | Literal boolean, read on each heartbeat. `true` makes a macOS node report whole seconds since the last keyboard or pointer input, as `host_activity.user_idle_s`, to a hub that accepts `host_activity`. Linux never sends it. The node reads no app names, window titles, input content or process lists. See [placement telemetry](placement.md#telemetry). |
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
| `omp-haiku` | `omp` | `anthropic/claude-haiku-5-5` | `credit` | `anthropic-api` |
| `omp-luna` | `omp` | `openai/gpt-6-luna` | `credit` | `openai-api` |

OMP launches with `--model <provider/model>:<effort>` and uses `-p` for a
headless delegate. Startup and brief delivery require a visible footer that
matches the model, effort, and working directory. The catalog seed at
[`catalog/omp-credit-seed.json`](catalog/omp-credit-seed.json) mirrors native
grades and gates for each effort. The `omp-haiku` and `omp-luna` rungs are
`unrated`: a graded `-m auto` spawn never picks them, and only a pinned spawn
with `--containment` reaches them until routing-policy measures them. The desk
publishes the seed after merge.

## `[hub]`

| Key | Meaning |
| --- | --- |
| `hub_url` | Panewire hub URL. |
| `hub_token_env` | Operator credential-file path for placement/quota. Configuring it opts into the hub quota gate and requires hub_url. |
| `operator_token_env` | Operator credential file for control operations and the lane-list fallback. |
| `lanes_token_env` | Dedicated machine-bound lane credential; preferred for lane registration/removal. |
| `hub_cf_env` | Optional access-service credential file passed to panewire. |
| `machine_id` | Host identity fallback. |
| `session_machine_ids` | Herdr session → machine identity table used for lane placement. |
| `session_lanes_token_envs` | Herdr session → lane credential-file path. Overrides `lanes_token_env` for that session; pair it with `session_machine_ids` when sessions use separate node identities. |
| `quota_gate` | Only `"local"` is accepted: explicit local-only quota mode. It contradicts hub_token_env; remove one of those keys. |
| `state` | `"on"` (default) reads the hub's fleet state for placement and the quota gate; `"off"` skips it and measures directly as before (the rollback switch). Cooling cannot be checked when hub state is off or unavailable; the selection record reports the condition in `degraded`. See [Fleet state](#fleet-state). |
| `state_publisher_ttl_s` | Seconds a publisher heartbeat stays alive. Default 90. |

Worker lane credentials use lanes_token_env, then hub_token_env. Lane listing
and lanes of legacy builder jobs use lanes_token_env, then operator_token_env, then hub_token_env.
Panewire owns authentication and relay transport; panewire never prints credentials.
All herdr calls during a spawn use one frozen `HERDR_SESSION` (default `default`).
For example, a node serving session `fleet` can use
`session_machine_ids = { fleet = "host-a", default = "host-a-default" }` and
`session_lanes_token_envs = { fleet = "/config/node.env", default = "/config/node-default.env" }`.
The registration warning includes session, machine, pane and panewire's error text;
it is emitted for workers. A credential explicitly declaring another
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
| `workspace` | target `[local] workspace` | Workspace on this remote host, named by label (`workers`), not by id. Herdr deletes a workspace when its last pane closes; spawn recreates a missing label, but an id cannot be recreated. The caller's `-w` / `--workspace` applies only to local placement. Missing target workspace configuration refuses with `WORKSPACE_UNCONFIGURED` (70). Automatic SSH placement tries the next candidate on this early refusal; forced placement still stops. |
| `cwd_map` | explicit override | Inline table of local prefix → remote prefix; longest prefix wins. Only needed as an exception: the default remote cwd is `$HOME/work/<repo>` on the target, cloned when missing. For a job run from a linked git worktree the default is `<work root>/<worktree dir>`, where the work root is derived from entries that map a checkout directly under the local work root (else `$HOME/work`). |
| `cwd_keys` | legacy | Kept for legacy hub configurations; not used. A hub host's cwd key is the canonical repo name. |
| `hub_wait` | `240` | `via = "hub"` only: seconds (1..300) the hub waits for the node to answer a spawn. It bounds the whole node run, so it must exceed the node's `brief_submit_timeout_ms` plus launch time; a shorter budget cuts the submit window. A first spawn that clones a large repo may need more. |
| `brief_doc_prefix` | `briefs/` | `via = "hub"` only: handoffkeep key prefix the brief is uploaded under; it must match the node's own `brief_doc_prefix` (ends in `/`). |
| `tags` | unset | Capability tags offered by this host (`caps` is an alias; both lists merge). A job's required `--tags` must be a subset for the host to be a placement candidate. A capability the host does not declare is unknown and fails closed for this host only: the host is excluded for jobs that need it and stays eligible for every other job. See [Host capabilities and light hosts](#host-capabilities-and-light-hosts). |
| `roles` | unset (all) | Nonempty list of spawn roles (`worker`, `tester`, `reviewer`, `counsel`) this host accepts. Other roles record `role_not_accepted:<role>`. A list that still names the retired `builder` role is a config error (exit 70) that names the key to edit: delete `"builder"` from `<host>.roles`. |
| `grades` | unset (all) | Nonempty list of task grades (`C`, `B`, `A`, `A+`, `S`, `S+`, `SS`) this host accepts. Other grades record `grade_not_accepted:<grade>`; a consult has no grade and records `grade_not_accepted:none`. |
| `presence_id` | host `NAME` | Identity this host uses in pacelever host-presence records. |
| `heavy` | legacy | Accepted and ignored; the `heavy` command and its host lock were removed. |
| `wake` | off | Retired with `panewire node wake`: a wake attempt now fails with "no waker configured" and starts nothing. Still accepted. |
| `wake_hold` | `10` | Retired with `wake`; still accepted. |
| `wake_wait` | `90` | Retired with `wake`; still accepted. |
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
| One-core cloud VM | `["linux"]` | `roles = ["worker", "tester", "reviewer"]`, `grades = ["C", "B", "A"]` (no consult, no heavy grade) |
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

Explicit `-m PROFILE` (except `auto`) or `--host NAME` (except `auto` or
`local`) requires `--containment hk:ID` or `--operator-request REF`.
`--exclude-host` is not gated: a standing exclusion (mergewake keeps
reviewers off its own host on every spawn) is policy, not a containment.
`--exclude-pool` is the same kind of standing exclusion for quota pools (below).
`--containment` may still be passed with it and is recorded. The containment ID must match `hk:` followed by a nonzero decimal
task ID of at most nine digits. It is recorded in `job.placement` and does
not replace the existing `--override-reason` requirement.

### Hub transport (`via = "hub"`)

A host with `via = "hub"` is spawned through the panewire hub (`POST /v1/spawn`,
`spawn_v2`) instead of ssh. It needs `[hub] hub_url` and an operator credential
file (`operator_token_env`, else `hub_token_env`; `hub_cf_env` if the hub sits
behind access); the files must be private and are never echoed. For one spawn
panewire:

1. uploads the brief as an hk document `<brief_doc_prefix><job>.md`
   (`PUT /v1/documents/{key}`, kind `brief`, the job) and sends the key as
   `brief.doc`; the hub never sees the text. A brief over 64 KiB is refused
   (exit 2); a retry overwrites the same key.
2. for a job started from a linked worktree, sends a `checkout` spec that the
   node's `worktree.prepare` turns into the same worktree: `mode: branch` for
   a branch whose local HEAD is already contained in `origin/<branch>` (the
   node tracks origin's tip), otherwise `mode: detached` at the exact `sha`
   with `fetch: pull/<n>/head` when a local `origin/pull/<n>/head` ref points
   at HEAD, or `fetch: refs/heads/<branch>` of an origin branch containing it
   (a detached HEAD, or a branch whose commits sit on another origin ref).
   A writing role (any role but `reviewer` and `counsel`) whose `branch` has no
   local `refs/remotes/origin/<branch>` is refused (exit 81) before anything is
   uploaded, naming the `git -C <cwd> push -u origin <branch>` command to run.
   The same roles are refused (exit 81) when HEAD is a strict ancestor of that
   ref (the worktree is behind origin), naming the
   `git -C <cwd> fetch && git -C <cwd> merge --ff-only origin/<branch>` command
   to run. HEAD equal to or ahead of the ref keeps today's behaviour. The
   submitter never fetches, so the check sees only the remote-tracking ref as of
   the last fetch.
   Only refs already known locally are read; nothing is fetched. Commits on no
   origin ref, an origin URL without an https form (`git@host:o/r` and
   `ssh://` become `https://host/o/r`), or a subdirectory start refuse with
   exit 81 before anything is uploaded. The worktree directory name is the local
   one. A job from a canonical checkout sends no `checkout` and runs in the
   node's `cwd_map[<repo name>]`.
3. sends the flags the hub's allowlist carries (`-m -l --t --job --owner
   --effort --role --lane --parent --task|--parent-job --report-relative -L --urgency
   --builder-family --builder-job --review-pass`). The node supplies workspace, host and the
   override reason. A flag the allowlist cannot carry (`--keep`, `--job-dup-ok`,
   `--task-hk-bypass`, `--allow-escalation`, `--operator-request`,
   `--requested-by`, `--purpose`) refuses the spawn (exit 2)
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
   "go together" refusal (exit 2). The hub still holds one reviewer slot per
   task, so passes that start together name theirs: `--review-pass N` (1..9,
   `--role reviewer` only) keys the slot `task:reviewer:p<N>`
   ([hub-spawn-operations.md](hub-spawn-operations.md)).
4. the node's `spawn.json` must map the repo name as a `cwd_key` and set
   `work_root`; `spawn_disabled`, `cwd_unmapped`, `panewire_missing` and
   `brief_doc_unavailable` refuse that host (recorded as `hub_<reason>`) and
   `--host auto` moves to the next candidate.

대상 계정은 `quota.refresh` 노드 명령으로 갱신합니다. `claude`, `codex`,
`commandcode`, `devin`, `agy`만 native 측정 대상이며, 다른 풀은 측정하지 않습니다.
`measure_timeout_s`가 갱신 제한 시간을 정합니다.

Failures never fall back to ssh (design D4):

| Hub answer | Result |
| --- | --- |
| Hub unreachable, timed out, 5xx | Host refused, reason `hub_unavailable`. `--host auto` continues with the other candidates; with none left the usual refusal applies (80 when local spawning is off, else the job runs locally). `--host NAME` exits 81 `hub unavailable`. A quota refresh meets the same outage the same way. |
| 503 `spawn_capability_required` | Node predates `spawn_v2`: host refused, reason `hub_spawn_capability_required`. |
| 503 `node_unavailable` | Node asleep or not accepting: treated like an unreachable host, so a wakeable host is woken (and waited for through the hub connection, never ssh). |
| 401 / 403 / 400 | Host refused, reasons `hub_auth` / `hub_invalid_request` (exit 70 semantics). |
| `checkout_refused:<outcome>` (rc 81) | `dirty`, `sha-missing`, or `branch-elsewhere` with verified cleanup (`retryable`) moves `--host auto` to the next candidate; if all candidates refuse, the operation ends as `admission_refused` with the tried hosts (exit 80 when local spawning is disabled). Without `NextHost`, a checkout refusal exits 81 naming the outcome. Pinned hosts and unverified cleanup keep the refusal as the answer. `busy` is retried once on the same host. |
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

`--exclude-pool P[,P]` removes the named quota pools from automatic selection
for this one request (`claude`, `codex`, `devin-free`, ... as the catalog names
them). It is a standing exclusion like `--exclude-host`, not a pin: it needs no
containment and no override reason. Every candidate of an excluded pool drops
with reason `pool_excluded`, visible in `--dry-run` and in the `job.placement`
selection record; when no candidate remains the spawn fails with the usual
placement refusal. It travels with the submission to the hub and on to the node
(`--exclude-pool` is on the hub's operator allowlist beside `--exclude-host`);
`panewire plan` takes it too. The hub adds it itself when it replaces a
quota-stopped job (below).

### A quota-stopped job, by role

When a running pane stops on a quota or rate-limit screen the job is handled by
its role. The wait/hand-over policy is hk `policy/quota-stop`; it is not
restated here. `docs/quota-stop.md` has the mechanics.

| Role | What happens |
| --- | --- |
| reviewer | The hub submits a replacement at once: same task, role, review pass, owner and brief, job id `<old>-q<n>` and `--exclude-pool <pool>`. The old hk record ends `quota_stop pool=<pool> replaced_by=<new job>`; `replaced_by` is the last `key=value` token (mergewake follows it). At most 2 replacements per review pass (`<old>-q1`, then `<old>-q1-q2`). When no other pool can take it, there is no replacement and the watcher ends the job as before. |
| worker, tester | The quota-stop manager acts even with `--quota-stop-act` off. It waits and resumes the same pane once at the reset when the reset is at or before the job's start plus the task's expected time; otherwise (reset later, no expected time, an urgent task, or a second stop after one resume) the job is handed over like a reviewer, with `end_reason` `quota_stop pool=<pool> handed_over_to=<new job>`, the same worktree and branch, and one brief line naming the earlier job's report and branch. |

The owner gets one info lane event for either action. `--quota-stop-act` still
gates every other role.

Automatic selection measures an account the same way with or without
`--host NAME` or `--exclude-host`: a stale shared reading is refreshed on any
holder of that account, the local machine first, and a host those flags keep
out of placement may still be that holder. It is never placed or gated. So
`panewire plan` and a restricted spawn judge the same evidence, and a failed
refresh on the only placeable holder (for example over ssh that is down) no
longer refuses an account another holder can measure.

Before trying a remote candidate (including an explicit `--host NAME`), panewire
resolves that host's most recent fresh presence for the launch pool, then asks
panewire's native gate to judge its shared observation using the exact
`POOL:FINGERPRINT` account key. The spawner's `[quota.accounts]` does not pin remote accounts.
Missing, ambiguous, stale or degraded target evidence refuses that host; auto
placement tries the next candidate. A pinned `-m` with a forced remote
`--host NAME` that has no selected account resolves the pool from
the launch lookup (`[quota].launch`, [docs/quota.md](quota.md#launch)) for the seat's effort; when that refuses (an
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
for the key), the gate cannot refresh it on demand. panewire then runs
`panewire quota refresh POOL` on the target over SSH for a pool the target
measures itself, only for one of the five native pools, bounded by `measure_timeout_s`,
re-reads the shared observation and judges it (for a `via = "hub"` host the
refresh is the hub's `quota.refresh` node command, not ssh). A failed, timed-out or
unpublished measurement keeps the refusal; `quota_gates` records
`measured` (`ok` or `failed`) and a `measure_*` reason. A gate that reports its
`gate_basis` is also recorded as `basis`.

실행 호스트는 native gate receipt에서 요청한 `account_key`와 pool을 확인합니다.
선택 계정은 JSON receipt가 필요하며, pane 및 headless launch는 quota lease를
기록하기 전에 계정 연결을 다시 검사합니다. 대상 호스트도 spawn 전에 마지막
local gate를 수행합니다. 원격 checkout provisioning은 변경하지 않습니다.

Panewire must already be installed on SSH targets. `_spawn-stdin` transports a
structured request and original brief through stdin, uses a private temporary
brief file and deletes it after spawn. The target composes its own preamble.
No host installation or service management is performed by spawn.

## `[placement]`

| Key | Default | Meaning |
| --- | --- | --- |
| `load_threshold` | `1.0` | load5/ncpu value above which a host takes no new job while anything else qualifies. |
| `wake_after_s` | `600` | How long the task must have waited before saturated awake hosts justify waking a sleeper. |

`[placement.shadow]` and `[placement.capability.<class>]` tune the placement
shadow comparison, which records how other rankings would have ordered the
hosts and never changes a placement. Every key is optional; see
[Shadow comparison](placement.md#shadow-comparison) for what each does and its
default.

| Key | Default | Meaning |
| --- | --- | --- |
| `shadow.enabled` | `true` | `false` stops recording the `placement_shadow` event. |
| `shadow.cpu_busy_over` | `load_threshold` | CPU-busy fraction at or above which a host counts as loaded in the shadow arms. |
| `shadow.hysteresis` | `1.25` | Factor a rival's hub score must reach over the policy-order host's to take the decision. |
| `shadow.all_loaded_below` | `15` | Best score under which every host counts as loaded. |
| `shadow.mem_refuse_below_pct` | `30` | The measured arm refuses a host whose available memory is below this percentage. |
| `shadow.pressure_refuse_level` | `2` | The measured arm refuses a macOS host at or above this pressure level (0 normal, 1 warn, 2 critical). |
| `shadow.pressure_warn_mem` | `0.5` | The memory term of a macOS host at warn pressure. |
| `shadow.mem_floor`, `shadow.mem_span` | `0.20`, `0.20` | The memory term is 0 at `mem_floor` available and 1 at `mem_floor + mem_span`. |
| `shadow.swap_in_half_per_s` | `5000` | Swap-ins per second (pages) that halve the swap term. |
| `shadow.load_knee`, `shadow.load_cap` | `0.75`, `4` | Load per core below the knee costs nothing; a higher reading stops costing more at the cap. |
| `shadow.jobs_half`, `shadow.claim_cost` | `8`, `10` | Live job count that halves the jobs term; points one claim in the spawn bridge costs. |
| `capability.light.<host>`, `capability.heavy.<host>` | none | The host's capability for light and heavy work, in the unit of the hub's `power`. A host with no entry keeps its `power`. |

## `[quota]`

Panewire's pool and profile settings are read from
`~/.config/panewire/config.toml` (or `$XDG_CONFIG_HOME/panewire/config.toml`).
`PANEWIRE_CONFIG` selects an explicit file. During migration, panewire also
checks `~/.config/pacelever/config.toml` and the older scopefuel path when the
panewire file is absent; `PACELEVER_CONFIG` remains a fallback override.
Create the panewire file before removing the legacy file so host behavior
continues unchanged.

Quota cache records use `<XDG cache>/panewire/` (or `~/.cache/panewire/`).
`PANEWIRE_CACHE` is a file path override; records are stored beside it.
`PACELEVER_CACHE` is checked as a legacy fallback. Native writes go to the
panewire location.

Shared-quota read settings for presence-aware placement. The URL and bearer
are panewire's svc-panewire credential
([handoffkeep service credential](#handoffkeep-service-credential)); `url`
below overrides the URL.

| Key | Default | Meaning |
| --- | --- | --- |
| `url` | `HANDOFFKEEP_URL` | Shared quota cache endpoint; HTTPS required outside loopback. |
| `credential_env` | retired | Ignored since hk 3570; the bearer is always `HANDOFFKEEP_TOKEN_SVC_PANEWIRE` from the [service credential](#handoffkeep-service-credential). Still accepted so an old file loads, with a one-line warning that names the key. |
| `presence_ttl_s` | `86400` | How recent a host-presence record must be to count as login evidence. |
| `fresh_ttl_s` | `900` for `claude`, `300` otherwise | How recent a shared observation must be to count as fresh, for every pool without its own `pool_fresh_ttl_s` entry. A reading is valid as of the later of `measured_at` and `confirmed_at`: handoffkeep keeps `measured_at` when a re-measurement publishes the same values and only advances `confirmed_at`. This is pacelever's rule, so `pacelever --json` and the gate agree on a reading. A spawn delegated over ssh with a selected account carries the placing host's window for that account's pool, and the executing host rechecks that account with it (up to 86400). |
| `native_pools` | 폐기 | migration 경고를 출력한 뒤 무시합니다. `claude`, `codex`, `commandcode`, `devin`, `agy`는 항상 native 측정하며 다른 풀은 측정하지 않습니다. |
| `gate` | `"native"` | 폐기 키입니다. 남아 있으면 경고 후 native verdict를 사용합니다. `"native"` 이외의 값은 설정 오류입니다. |
| `launch` | `"native"` | 폐기 키입니다. 남아 있으면 경고 후 native catalog lookup을 사용합니다. `"native"` 이외의 값은 설정 오류입니다. |
| `pool_fresh_ttl_s` | unset | Optional pool → seconds table (`[quota.pool_fresh_ttl_s]`, nonnegative integers) that overrides `fresh_ttl_s` for one pool. |
| `pool_max_concurrent` | unset | Optional pool → positive integer table (`pool_max_concurrent = { "openai-api" = 2 }`; pool name as in the catalog's `pool` column) that caps live spawns on a pool fleet-wide. A candidate whose pool is at its cap is dropped `pool_at_cap` and selection moves to the next pool; a spawn never waits for a slot. A pool not named has no cap. Read at each placement. A value that is not a positive integer is refused at config load with the key named (`quota.pool_max_concurrent.openai-api must be a positive integer`). When the live count cannot be read from the hub the cap does not apply and the reason is logged. The reserved key `devin-free` caps only the SWE-2 free rungs (`devin-swe2`, `devin-swe2-medium`, `devin-swe2-max`, `devin-swe2-flash`, `devin-swe2-flash-medium`, `devin-swe2-flash-max`) by their live spawns: those rungs draw on Devin's free-model quota while paid `devin-ds41` draws on the account's paid window, so capping them must not cap paid devin. `devin` stays a whole-pool cap; when both keys are set a free rung must pass both. How the count is made and the refusal text: [placement](placement.md#pool-concurrency-cap). |
| `credit_total_usd` | unset | Optional pool → dollars table (`[quota.credit_total_usd]`, positive numbers): each prepaid credit pool's grant total. A shared manual entry records only its used percentage, so panewire derives the dollar balance `total × (100 − used_pct) / 100` for credit ranking, dollars at risk and burn; an entry's own `remaining_usd` still wins. Set it on the hub and every node, and **update it whenever a new grant is bought** — a stale total gives wrong dollars. Keeping the dollars in the shared record needs hk 3418; until it lands this table stays the source. |
| `measure_timeout_s` | `60` | 대상 계정의 native refresh(`panewire quota refresh <pool>`) 제한 시간입니다. 다른 풀은 측정 소스가 없습니다. |
| `accounts` | unset | Optional pool → 16-hex account-fingerprint table for exact account keys. For `agy` it is the account the node measures: agy has no login to derive a key from ([docs/quota.md](quota.md#native-measurement-agy)). |
| `measurable_accounts` | `[]` | `pool:<16-hex>` account keys this host may measure. A host measures agy, announces it and publishes under its key only when `accounts.agy` is set and `agy:<fp>` is listed here; otherwise it does not measure agy and logs one line. Read at each use. |
| `login_probe_interval` | unset (off) | Duration string such as `"30m"`. When set, the node measures each pool in `--state-quota-pools` whose account this host lists in `measurable_accounts` with this host's own credential at most once per interval, even when the shared reading is fresh, and marks the harness login missing on a login failure. Nothing is published. Read at each round. Enable it per host only after checking that a probe does not rotate a token other hosts share. A value that is not a non-negative duration is refused at config load. See [fleet-state.md](fleet-state.md#login-probe). |
| `node_env` | `~/.config/panewire/hub-node.env` | Panewire node env file; `HUB_MACHINE_ID` in it is the host id written to job records. `PANEWIRE_NODE_ENV` overrides the path. |

## `[sweep]`

Node sweep behaviour (see [pane-sweep.md](pane-sweep.md)).

| Key | Default | Meaning |
| --- | --- | --- |
| `remove_worktrees` | `true` | After the sweep closes a job, remove the worktree the node prepared under `work_root` and its local branch: a clean, merged worktree when the task completed; any worktree, after saving its changes, when the end was not a merge (see [pane-sweep.md](pane-sweep.md#the-worktree)). `false` keeps every worktree. |

## `[reap]`

The job dir TTL (see [job-dir-ttl.md](job-dir-ttl.md)).

Set `[reap] job_dir_ttl` only after every host that reads this hosts config
runs a binary that knows the `[reap]` table. An older binary refuses an
unknown table: the whole config fails to load there, and that node's sweep
loop stops.

| Key | Default | Meaning |
| --- | --- | --- |
| `job_dir_ttl` | unset | The node's opt-in to deleting job directories: when set (`"72h"`, `"3d"`; at least `24h`), the node sweep loop runs the job dir TTL with this age, at most once an hour, and deletes each directory its gate lets go. Unset, the node deletes nothing. `panewire reap --expire-job-dirs` uses it as its TTL, else 72 h. A value under 24 h or one that does not parse is a config error. |

## handoffkeep service credential

Every panewire hk HTTP path that is not handed an explicit file authenticates
as panewire's own hk client id, `svc-panewire` (hk 3526 §4, hk 3570). That
covers the bench catalog (`panewire catalog`, `aa-sync`, the launch rung), job
records, bench reps, host presence, the shared quota reads, task status and the
placement record producer, and the node's default `handoffkeep_env`. One loader
(`internal/hkcred`) resolves it:

1. `HANDOFFKEEP_URL` and `HANDOFFKEEP_TOKEN_SVC_PANEWIRE` both set in the
   environment win, with the optional `HANDOFFKEEP_CF_ACCESS_CLIENT_ID` /
   `HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET` pair (both or neither).
2. Otherwise the panewire-owned file `~/.config/panewire/hk-svc.env` supplies
   the same keys. It must be a regular mode-0600 file (not a symlink); anything
   else is refused. `PANEWIRE_HK_ENV` names another path.
3. The hk CLI's `~/.config/handoffkeep/config.env` and `HANDOFFKEEP_TOKEN` are
   never read. `HANDOFFKEEP_CONFIG` and `[quota] credential_env` are retired:
   an old `hosts.toml` that sets `credential_env` still loads, the key is
   ignored, and each `panewire` command prints one warning naming the key
   (not its value): `panewire: warning: [quota] credential_env is retired;
   panewire reads HANDOFFKEEP_TOKEN_SVC_PANEWIRE or
   ~/.config/panewire/hk-svc.env`.

```
# ~/.config/panewire/hk-svc.env (mode 0600)
HANDOFFKEEP_URL=https://hk.example.invalid
HANDOFFKEEP_TOKEN_SVC_PANEWIRE=…
# optional, both or neither
HANDOFFKEEP_CF_ACCESS_CLIENT_ID=…
HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=…
```

`[quota] url` in `hosts.toml` still overrides the URL for the jobs, bench and
quota reads; the token always comes from the loader.

**Spawn path over HTTP (hk 3591).** The spawn path calls hk over HTTP through
hk's public client package (`github.com/mgh3326/handoffkeep/client`) with this
svc-panewire credential, never through the `handoffkeep` CLI or its
`config.env`: the task precheck and the link check (`GET /v1/tasks/{id}`), the
task claim (`POST /v1/tasks/{id}/claim`), the respawn rebind
(`POST /v1/tasks/{id}/rebind`), the owner lookup for an omitted `--owner`
(`GET /v1/tasks/projects/{project}/owner`), the routing policy read
(`GET /v1/documents/policy%2Fmodel-routing`) and the via=hub brief upload
(`PUT /v1/documents/{key}`, kind `brief`, the job id), and the read of a
`-p hk:doc/KEY` brief (`GET /v1/documents/{key}`, see [Brief from
hk](#brief-from-hk--p-hkdockey)). The URL is the loader's
(`[quota] url` does not apply), and the HTTP settings are those of the other hk
clients: 8 s per call, no redirect followed, HTTPS off loopback and the
tailnet, no proxy over plain HTTP; the brief upload alone has 60 s, as the CLI's
`doc put` had no bound of its own. Refusal texts and exit classes are the CLI
path's: hk's error reads as `handoffkeep: <code>[: reason]`, a claim refusal is
still `task claim exited with status 1`, and a routing doc that fails its schema
never replaces the last good cache, which is used with a warning. Task and owner
answers are read as the CLI path read its output (a string id, an unparseable
timestamp), and a 2xx answer that cannot be read at all is never covered by
`--bypass`: the precheck refuses with `unreadable task precheck` (exit 70) and
the claim records `task claim did not confirm claimed_by and refs.job_id`.

**The other hk calls over HTTP (hk 3607).** No panewire command runs the
`handoffkeep` CLI any more; the calls hk 3591 left on it go through the same
client and credential:

| Caller | hk route | Was |
| --- | --- | --- |
| `panewire work TASK_ID` | `GET /v1/tasks/{id}` | `tasks show` |
| Artifact record write (move, upload, sweep) | `PUT /v1/documents/{key}`, kind `evidence`, no job (hk has no `artifact` kind) | `doc put --kind artifact` |
| Artifact sweep task read and recheck | `GET /v1/tasks/{id}` | `tasks show` |
| Artifact sweep listing | `GET /v1/documents?prefix=…&limit=100` | `doc list --prefix` |
| Artifact sweep record read | `GET /v1/documents/{key}` | `doc get` |
| `done` / `escalate` report upload (lifecycle and `panewire job`) | `PUT /v1/documents/reports%2F{job}%2F{file}`, kind `report`, the job | `doc put --kind report --job` |
| `done` / `escalate` task comment | `POST /v1/tasks/{id}/comments` | `tasks comment --body` |
| Quota stop policy read | `GET /v1/documents/policy%2Fquota-stop` | `doc get policy/quota-stop` |
| Quota stop task lookup | `GET /v1/tasks?limit=1000` | `tasks list --limit 1000` |
| Quota stop note | `PUT /v1/documents/quota-stop%2F{sha}`, kind `note`, the job | `doc put --kind note --job` |

Each read keeps hk's answer and parses it as the site parsed the CLI's output,
so the same answer gives the same result; an answer that parser cannot read
takes the branch unparseable output took (for the sweep listing, `invalid
artifact doc listing`, exit 70), never success. `panewire work` reads
`refs.job_id` and `refs.report_path`; its `refs.worktree`, `refs.cwd` and
`refs.brief_path` fallbacks are kept, though hk keeps no such task refs. The
sweep re-checks each listed key against its prefix, which hk matches with SQL
`LIKE`. Failures are handled and worded as before: a failed report upload or
task comment is a warning and the job still completes; a failed artifact
record write or sweep recheck ends the sweep with exit 1 and `handoffkeep:
handoffkeep: <error>` (the CLI's stderr under panewire's prefix); a failed
quota-stop note is retried after a minute; the quota-stop policy keeps its
five-minute memo and its last good copy (`quota-stop-policy-cache.json`).
Report, artifact and brief uploads get 60 s (the CLI's `doc put` had no bound;
the `panewire job` upload had 3 s, and a timed-out upload still warns
`rc=124`); the other calls get 8 s. hk checks a comment's body as the CLI did
before sending it.

**Explicit files.** A file the operator names (the hub's `--handoffkeep-env`, and a node's `spawn.json` `handoffkeep_env`) is read as before, and its bearer is `HANDOFFKEEP_TOKEN_SVC_PANEWIRE`
when present, else `HANDOFFKEEP_TOKEN`, so files written before hk 3570 keep
working. **Node on defaults.** With no `spawn.json`, the node's hk credential
is not an explicit file: it resolves through the same loader, in the same
order — the environment pair of step 1, else the panewire file
(`PANEWIRE_HK_ENV`, else `~/.config/panewire/hk-svc.env`) with
`HANDOFFKEEP_TOKEN_SVC_PANEWIRE` only. A legacy-only file there is refused as
unconfigured. A `spawn.json` `handoffkeep_env` that names the same path is
still an explicit file and keeps the `HANDOFFKEEP_TOKEN` fallback: the node
decides by where the setting came from, never by the path.

**Unconfigured.** Each caller behaves as it did with no hk credential, and the
one-line error names the missing keys or the file, never a value (for example
`hk service credential unconfigured: set HANDOFFKEEP_URL and
HANDOFFKEEP_TOKEN_SVC_PANEWIRE, or create ~/.config/panewire/hk-svc.env`):

| Caller | Outcome when unconfigured |
| --- | --- |
| `panewire catalog …` | Refused with the condition-invalid exit and `catalog: <error>`. |
| `panewire catalog aa-sync` | Refused, `aa-sync: cannot read catalog`. |
| Launch rung (`[quota] launch = "native"`) | The bundled catalog, judged stale: a known rung is refused stale (exit 70). The shadow comparison has no cache to read. |
| Spawn job-record claim | The spawn is refused before launch (exit 70, `job record claim unavailable: …`). |
| Respawn parent check | Refused, `parent job record unavailable`. |
| Job-record updates (`done`, lifecycle, usage, quota stop, sweep) | Skipped, as before. |
| Placement record producer | Records stay spooled (`placement delivery unavailable`). |
| Native admission shared reads, presence, manual quota, task status | Unavailable: the account reads as unknown or degraded. |
| Bench reps (`panewire reps`, merged-PR outcomes) | `panewire reps` is refused (exit 70); the sweep's merged-PR outcome is not recorded. |
| Node brief fetch (default `handoffkeep_env`) | `brief_doc_unavailable` / `brief_doc_fetch`. |
| Spawn task precheck | Refused, `task precheck unavailable: handoffkeep: hk service credential unconfigured: …` (exit 70); a bypassed spawn proceeds. |
| Spawn task claim (after delivery) | `job.task_link` `failed`, `TASK_LINK_FAILED … reason=task claim exited with status 1` to the owner lane. |
| Owner lookup (omitted `--owner`) | Refused with exit 2, naming the error and `--owner LANE`. |
| Routing policy read | The last good cache, with a warning; no cache gives the builtin rules for implementers and refuses consult and verifier. |
| via=hub brief upload | The hub candidate is refused, `brief upload to <key> failed: …`. |
| `panewire work TASK_ID` | Refused, `cannot read task <id>; run handoffkeep tasks show <id> and restore task access` (exit 70). |
| Artifact sweep | Every job's task is unreadable, so nothing is swept; an artifact record write fails with exit 1. |
| `done` / `escalate` report upload and task comment | Warnings (`report upload failed; local report retained`, `task comment failed: … unconfigured …`); the job completes without a report document. An interactive `done` then exits non-zero (`completed; report not in hk; rerun the same command`); the rerun publishes the report ([Completion receipts](#completion-receipts)). |
| `panewire job` done, escalate and joined report upload | Warning `<unconfigured error>; report document not uploaded (job=<job>)`; no document link. |
| Quota stop manager | No policy (the last good copy, else hand over), no task, and the note is retried each minute. |

**Rollout.** Put `hk-svc.env` (the `svc-panewire` token, Infisical `/panewire`)
on every panewire host before deploying this; otherwise hk reads fail and
spawns that need hk are refused.

## Job records in handoffkeep

Each spawn writes a record to hk `/v1/jobs` on the executing host (the design
is `design/2026-10-30/cross-host-jobs`): the claim `POST {job_id, task_id,
host}` runs before the local claim and a taken id refuses with exit 74, like
the ledger's duplicate. `host` is the node's `HUB_MACHINE_ID`. hk unreachable
or unconfigured fails the spawn closed. The task precheck also fails closed before launch.

After the brief lands, panewire merges `{state:"spawned", role, label, family,
profile, model, harness, effort, pool, owner_lane, parent_lane, parent_job,
pane_id, worktree:{origin, branch|null, head}}` — the allowlisted fields only
(hk's `JobUpdate` accepts `harness` and `effort` since handoffkeep v0.0.39;
production runs v0.0.63); brief
text, command lines, pane output and absolute local paths are never written.
The optional `selection` object records why the model and host were chosen.
`chosen.why` is the one-line reason; `chosen.grade`, `tier`, `projected_pct`,
`reset_in_s`, and `waste_urgency` describe the quota choice, and
`chosen.model_id` supplies the model when `model` is absent. `policy.urgency_source`
names the urgency source. `candidates[]` lists other model candidates by `seat`,
`canon`, or `model_id` and their `outcome`; `override_reason` explains an explicit
model or host override. `host.chosen` and `host.source` identify the machine and
placement path, with `host.candidates[]` giving ranked machines and their outcome
or exclusion reason when a ranking exists. `host.chosen` is the execution machine,
including `local` when an automatic request falls back to local execution; an
invalid host id is stored as `unknown` rather than being cut into another id.
Candidate lists are trimmed in rank order to keep `selection` below hk's 32 KiB
limit.
### Completion receipts

A report's identity is `(job, S)`, `S` the sha256 of its bytes. `panewire done`
publishes it to the content-addressed hk document `reports/<job>/sha256/<S>.md`
(kind `report`, linked to the job's task), so one key names one byte string and
a retry PUTs the same key. `done` then runs four steps in order. Each step has
one authority and one local receipt (an event in the job's ledger), and a step
whose receipt exists is skipped:

| Step | Fact | Authority | Receipt |
| --- | --- | --- | --- |
| 1 publish | hk holds the bytes of `S` | the hk document | `job.report_published {report_sha256, document}` |
| 2 settle | the hk record is terminal, holding the caller's `result` | the hk job record | `job.record_settled {report_sha256, pinned, result_sha256}` |
| 3 pin | the record's `report_doc` and `report_sha256` name the completing report | the hk job record | `job.report_pinned {report_sha256, document, outcome}` |
| 4 notify | the parent was told about `S` | the local ledger, then the relay | `job.completed {report_sha256, document}` |

Step 1 confirms hk holds exactly these bytes, by the sha256 in hk's answer or a
read-back. The key lies under `reports/<job>/sha256/`, which a basename key can
never reach (a file name holds no slash), so a key there only ever holds the
bytes with that sha256; `escalate` and the legacy `panewire job` keep
uploading under the literal file name `reports/<job>/<file name>`.

Step 2 is the only writer of the record, `strictCompletedPutUntil`:
one PUT of `{state:"completed", result?, report_doc?, report_sha256?}` (the pin
only when the report is published and is the completing one), read back when
the answer is ambiguous or the record is already terminal, retried at most twice
more with the same payload within a 30 s budget. The single spawned repair PUT is
also read back after an ambiguous reply but never repeated. Step 3 is the late
write hk accepts once on a terminal record (hk 4354): `200` is pinned, `409`
means the record already names another report. A `409` is an outcome, logged
(`report pin kept`) and receipted as `held`, never an error. Step 4 writes
`job.completed` and the relay once per `S`, carrying the exact document key when
step 1 had succeeded by then, and comments on the job's hk task.

The completing report is the one the record's pin belongs to: the sha256 of the
settling receipt (for a job that completed before settlements were receipted,
the first `job.completed`). A later report, other bytes, gets its own document
and its own `job.completed` and relay, and never writes the record.

"Duplicate" is not a branch: it is the case where every step has its receipt,
and it makes no hk call and writes no event (`OK … duplicate=yes`). A repeat that
finishes a step prints the steps it ran (`published=yes pinned=yes`).

Publication and the pin never block completion. The only blocker is the rule
for a strict `--result`: hk takes a `result` only in the spawned-to-completed
write, never later, so the record must be confirmed holding it. For CLI `done
--result` that is a completed record read back with the same result; a refusal
or unresolved state exits non-zero, emits no completion event or relay and
records `job.record_failed` with `op` and a safe `reason` token. Conflicts use
exit 75. A `--result` on a record that is completed without one is
`terminal_without_result` on both paths: the interactive `done` fails, the
headless delegate warns and continues. Safe reasons are `secret_like_content`
or `secret_like_content:<pattern>` (pattern matches `[a-z0-9_]{1,40}`),
`terminal_without_result`, `result_conflict`, `job_escalated`, `job_lost`,
`no_spawn_evidence`, a validated hk code matching `[a-z0-9_]{1,64}`,
`http_<status>`, `hk_unreachable` or `hk_unconfigured`.

An interactive `done` that completed but left the report out of hk, or its pin
unwritten, prints its `OK` line and exits 1 with `completed; report not in hk;
rerun the same command` (or `report not pinned in hk`). That is reporting only:
the job completed, and the same command run again finishes the missing step.
The watcher and the headless delegate warn and carry on. `escalate` is not part
of this: it uploads under `reports/<job>/<file name>` and writes
`{state:"escalated", report_doc, report_sha256}`; the watcher writes
`{state:"lost"}` after two missing-pane observations, or when a reviewer pane is
idle on a quota or rate-limit screen with no report (see
`docs/quota-stop.md`; a worker or tester is the quota-stop manager's). Their
failed record writes remain best effort. The artifact sweep confirms a local
report by hashing it and reading the content-addressed key, then the legacy key
`reports/<job>/<file name>` ([job-dir-ttl.md](job-dir-ttl.md)).

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
A successful claim records `status: linked`. A claim that fails while hk's task
record already has `refs.job_id` equal to this job (a design session rebound
the task before the spawn) records `status: already_linked`, with no warning and
no notice: there is nothing to link (hk 3426). Other spawn failures retain their
normal cleanup behavior.

An inherited spawn normally leaves the task bound to its parent job. It rebinds
the task to the new job, through `handoffkeep tasks rebind`, only after the brief
lands and only when all of these hold: the parent's hk record state is `lost`,
`failed` or `completed`; the task's `refs.job_id` is the parent; the new job id
differs; and the spawn is a replacement seat (routing role `implementer`),
not a reviewer, tester, consult or `panewire delegate` child. The
`job.task_link` event then says `status: rebound`. Otherwise it says `inherited`,
adding `bound_job` when the task is bound to a third job and `rebind_error` when
the rebind failed or the parent's state was unreadable; neither fails the landed
spawn.

Automatic model selection (`spawn` with `-m` omitted or `-m auto`) reads the
live bench catalog from handoffkeep and pool status from the quota CLI, then
orders candidates under the versioned pool-order policy (`[local].pool_policy` or
`PANEWIRE_POOL_POLICY`, else the embedded copy): subscription pools at waste
risk first (see below), then other WASTE subscription pools, then the free
pool/model sets and the credit pools **together**, then ranked subscription
pools. A credit pool is a candidate in the free band: an omp credit rung ranks
beside the SWE-2 free rung, after waste and before ranked. Inside that band the
lowest catalog grade that satisfies the task wins, then the pool with the higher waste urgency — the unused projected share
(100 − projected) per hour until its longest account window's reset (7d,
monthly; sub-daily windows such as a 5h rate window never rank an account's
pool, they stay admission gates only). A pool whose longest window's reset can
be paced always ranks ahead of one whose cannot; unpaceable pools keep the
lower projected use at reset (the used percentage when it cannot be paced yet) —
then the smaller effort, then the catalog score. On a full tie inside the free
band a credit rung goes before a free rung: unspent credit is lost at expiry
and the free model is not. Among credit rungs, one with a dollar balance
outranks one without, ordered by dollars at risk per hour; a percent-only entry
keeps the remaining-percent-per-hour rule (below). A pool whose reading has no
account window (agy publishes only group windows) is paced, ranked and put at
waste risk on the group the rung draws from: `gemini` or `3p`, from the catalog
row's `group` or its model id. That group's longest window is used exactly as an
account window is, even when it is sub-daily — the gemini group publishes only a
5h window, and that window's reset is the one point where the rest of it is
lost. So one exhausted group blocks only its own rungs, a group whose window
projects unused quota at reset puts its rungs at waste risk, and a rung with no
group keeps the conservative rule: it is blocked and paced on the strictest
group (highest used share, then highest projection). The selection record names
the group as `waste_group`.

The pool-order policy is version `3`. A version `1` or `2` file supplied
through `[local].pool_policy`, `PANEWIRE_POOL_POLICY`, or `simulate --policy` is
refused with exit 70 and a migration error. To migrate, copy
`internal/fleet/pool-order.json`, retain any custom thresholds and free
model sets, and set `version` to `3`. The order is
`["waste", "free", "ranked"]`, and `credit_pools` includes both
`anthropic-api` and `openai-api`. A credit pool is not a band of its own:
version 3 puts it in the free band, and naming `credit` in `order` is refused.
The selection record keeps `tier: credit` on a credit rung (the pool class
inside that band) and lists the bands in `tiers`. Credit admission is unchanged
from version 2: credit no longer requires a balance observation, but its pool
class must remain readable.

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
`credit_pools` are never waste-risk: a credit pool is ordered by its balance
and expiry inside the free band (below), and a free seat by the free bucket.
Neither is a pool whose longest window cannot be paced (for example the first
5% of a fresh window). Waste-risk subscription pools rank ahead of every band,
free and credit included, and
among themselves by waste urgency first (a pool resetting sooner with the
same unused share comes first), then grade fit, effort and score; every
other pool keeps the band order above. For a waste-risk pool sub-daily
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

**Headroom for a known job length** (`plan --expected DURATION`, or the task's
`--expected` on spawn, e.g. `6h` or `2d`). After the band order and before
waste urgency, a subscription candidate whose shortest in-view window fits the
job ranks ahead of one that does not. Nothing is refused: the key only orders.
A window fits when its remaining share is at least `burn_pct_per_h × hours`,
or when its reset comes within half the job, so a window that resets mid-job
counts as room. Without an expected duration the key is off and the order is
unchanged. An unreadable task `--expected` value turns the key off on spawn
(degraded note `expected_unreadable`); `plan` rejects it with a usage error.

Burn rates come from `"burn": {"short_pct_per_h": 15, "long_pct_per_h": 2}` in
the pool-order policy. Windows of a day or less use the short rate, longer
windows the long rate, and a window of unknown length uses the short rate.
These are defaults, not measurements: no burn history is kept yet, so the
per-pool values in effect are UNVERIFIED until measured burn lands (hk 4327).
Waste-first and host-first placement are not changed by this key.

Subscription candidates additionally need a readable `ok` nonstale spend
status. The named credit pools, `anthropic-api` and `openai-api`, remain
admissible without a balance observation. An operator pool class of `exclude`
refuses them. Their selection record carries `tier=credit`, with absent
balance values left unset.

An operator pool class of `exclude` on the `devin` pool does not refuse the
SWE-2 free rungs (`devin-swe2`, `devin-swe2-medium`, `devin-swe2-max`,
`devin-swe2-flash`, `devin-swe2-flash-medium`, `devin-swe2-flash-max`). They
draw on Devin's free-model bucket, not the pool, so they are judged on that
bucket exactly as when the pool is not excluded, both for `-m auto` and for a
pin. Every other devin rung, such as the paid `devin-ds41`, stays refused as
`pool excluded by policy`. The bucket cap `devin-free` is set separately, see
[placement](placement.md#pool-concurrency-cap).

A credit pool with a recorded balance and future expiry competes for work
inside the free band. With a dollar balance (`credit.remaining_usd`) its rank
there is the money at risk: the balance less the spend projected before expiry
(the burn rate times the hours to expiry). The burn rate is the fall in dollar
balance between the two latest credit entries of the pool, divided by the hours
between their `measured_at`; it is `0` when fewer than two entries record
dollars or the balance rose. `at_risk_usd = max(0, balance − projected spend)`
and `usd_urgency = at_risk_usd ÷ hours to expiry`. A credit pool is never
waste-risk: the waste band is subscription pools only, and a credit rung stays
in the free band with `tier: credit` and `waste_risk: false`. Among credit
pools a dollar balance outranks one without, ordered by `usd_urgency`. An entry
without dollars keeps the percentage rule (remaining percent per hour until
expiry), ranks after any credit that has dollars, and the plan logs
`credit_usd_missing pool=<pool>` once so it can be recorded:

```sh
panewire manual set --pool <pool> --credit-balance <usd> --credit-total <usd> --expires <YYYY-MM-DD>
```

`panewire manual set|list|clear` writes the same hk shared manual observation
pacelever writes. The command's JSON reply keeps the dollar fields; the shared
wire carries only the used percentage, so `credit_total_usd` supplies the
dollars on read. Keeping `remaining_usd`/`total_usd` in hk itself is hk 3418.
The entry is written under the pool's `[quota].accounts` fingerprint, so that
key is required before a credit can be recorded.

A credit pool is placeable on a host only through that host's presence record
for the pool's account. The node publishes presence once per presence interval
(10 minutes) when all three hold: `omp` is installed on the host, the pool's
class is not `exclude`, and the shared store carries an unexpired manual
`window=credit` entry for the pool's account. The key is the pool's
`[quota].accounts` fingerprint, else pacelever's `[quota.accounts]`, so the key
the pacelever era published still matches. `panewire quota status` shows the
class, the manual entry's state and the presence for each credit pool on a host
that publishes it. See [quota.md](quota.md#credit-pool-presence).

The selection record and the chosen `why` text carry `balance_usd`,
`burn_usd_per_h`, `at_risk_usd` and `usd_urgency`.

Its account-bound gate still decides admission. Credit without a recorded
balance or expiry stays in the free band (`tier: credit`) and the gate decides
it. A credit entry whose expiry has passed is dropped as `credit_expired`; an
entry with no expiry at all is left to the gate. `credit_final_days` is
deprecated, has no effect, and emits a warning when set.
`panewire plan --credit-at TIME` evaluates expiry at the supplied time.

A free-tier rung is dropped as `free_limit` while the devin account's
published free bucket reads `exhausted` with its reset still ahead. The bucket
is the shared reading's `free_bucket` (the hub's fleet state carries it too).
A devin pane that reports "Reached free model rate limit" writes this host's
`devin-free-limit.json` — the same record `panewire quota observe
devin-free-limit` writes — and the next devin measurement publishes it to the
shared store. An unknown reset holds for one hour. The rung returns on its own
once the reset passes. The quota gate refuses the same rung on the same
reading; selection drops it before the gate.

A subscription pool on pace to use its longest window ranks behind the free
band, credit with a recorded positive balance and future expiry included.
Sub-daily windows do not pace a pool, and exhausted windows still refuse it.

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
`default`), the `tiers` bands used (`waste>free>ranked` in version 3), a
one-line `decision`, the ordering
`rule` with both thresholds and the waste-risk rule, `waste_risk` (the
`horizon_hours`/`min_unused_pct` in force), `projected_pct` and `used_pct`
per candidate, `waste_risk` (`true`/`false`) on every candidate and
`unused_at_reset_pct` (its input) whenever the longest window could be
paced,
`waste_urgency`, `reset_hours` and `waste_order` (`usd`, `reset` or
`projected_fallback`) per ranked candidate,
`waste_group` (`gemini` or `3p`) on a candidate of a pool that publishes only
group windows,
`tier: credit` and the `credit` object (`balance_usd`, `burn_usd_per_h`,
`at_risk_usd`, `usd_urgency`, `days_left`, `balance_pct`, `expires_at`) on a
credit rung,
`free_limit` as the drop reason of a held free rung and `credit_expired` on an
entry past its expiry,
and on `chosen` its `tier`, `grade`, `waste_risk`, `waste_group`,
`projected_pct`, `waste_urgency`, `credit` and a one-line `why`;
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
balance left (percent of its total); `resets_at` or `reset_hours` supplies its
expiry for urgency ranking. Without either value the credit rung still ranks in
the free band, with no expiry signal. A pool with `class: credit` is in the
free band, and the record's `tier` for its rung is `credit`. `free` marks a free-tier rung; `free_limit`
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
[--urgency normal|urgent] [--exclude-host a,b] [--exclude-pool P,P] [--builder-family F]
[--routing-role R] [--allow-escalation] [--json]` answers "right now, which host and which
model would a spawn get" without a task or brief. It is live and read-only:
it runs the spawn's selection core with catalog, account observations,
presence, and account-bound gates. Worker roles use `--grade` instead of a
task title. Consult takes an optional `--grade` that sets its floor (see
[Consult](#consult)); absent, the floor is `S+`. Selection then walks the same placement plan for the chosen seat — the local launch
resolution, seat check and gate for this host, the shared-account quota
gate for each remote host — and stops at the first host that admits it. It
never spawns, wakes a host, claims a local job, writes an hk task or job
record or emits a job event; a stale remote account is measured on its
host with `panewire quota refresh` when its pool has a native source. Other
pools are unmeasurable. Remote-only spawn
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

### Consult

`--role counsel` maps to routing role `consult`. It admits only `opus` and `fable` through `claude`, or
`codex-sol` and `codex-astra` through `codex`. OMP also admits the exact model
selectors `anthropic/claude-opus-5-5`, `anthropic/claude-fable-5-2`,
`openai/gpt-6.1-sol`, and `openai/gpt-6-astra`. The Fable 5.1 selector
`anthropic/claude-fable-5-1` is still accepted as the rollback path.
The shipped `omp-opus` and `omp-sol` profiles qualify. `omp-sonnet` and
`omp-terra61`, `omp-haiku`, and `omp-luna` cannot consult. An `agy` or `pi` seat running an
Anthropic model is ineligible. These constraints also apply to a wildcard
in the v1 table and to explicit profiles.

The routing table's profile allowlist still applies. OMP profiles must be
listed by name or covered by `"*"`. Catalog and launch model IDs are checked,
so a top-profile name cannot admit a non-top OMP model.

Consult grade is a floor on the rung, not the seat grade. The floor is one grade above the asker's task grade, capped at `SS`. A consult without a task grade takes `S+`. `plan --role counsel`, `resolve --role consult`, and `spawn --role counsel` accept `--grade C|B|A|A+|S|S+|SS` as the asker grade; an empty or unknown value exits 2 and names the accepted grades.

Within the floor:

- A rung graded `unrated` is never chosen and never asks its quota gate (`grade_unrated`).
- A row is dropped (`grade_insufficient`) when the rung it launches at is below the floor. That rung is the row's own effort; a rung-less row launches at its profile's default effort.
- `--builder-family F` on `plan` or `resolve` drops consult rungs of family `F` (`family_excluded`). With no family, nothing is excluded and the stamp records `unknown`.
- If no rung passes at the floor but a below-floor rung passes every other rule, consult retries once at the asker's own grade and stamps `fallback: no_rung_at_floor`. A below-floor rung on a cooling pool counts as such a rung. Unrated, absent-rung, and family-excluded rungs never trigger the retry. A session without a task grade never retries.

The selection record carries a `consult` object with `asker_grade`, `asker_family`, `grade_source` (`flag`, `task`, or `without_task`), `floor`, `used_floor`, and `fallback`. `resolve --role consult` prints `floor=<G>` on its text line.

`resolve` defaults other roles to grade `C`. Other `plan` roles require a grade.

### Role effort for consult

A consult never runs above its role's `effort` (`high` in the
current table) unless the request passes `--allow-escalation`. This rule
applies the same way to `spawn -m auto`, `plan`, and `resolve`. `delegate`
routes only as implementer, verifier, or reviewer, so the cap does
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

`roles.consult.family_effort` maps a
profile name to the role effort that family leads at, in place of the
role's `effort`. The current table carries `{"codex-sol": "xhigh"}`.

- For a mapped family, the mapped rung is the role-effort rung. Every rule
  above applies to it: rungs above it are `above_role_effort`, and if it is
  unplaced the family is skipped `role_effort_unplaced` with
  `role_effort` set to the mapped effort.
- The entry is the operator's explicit opt-in for that exact rung. panewire
  admits it even when its catalog gate is `escalation`. Without `--allow-escalation`, other escalation rungs stay
  `gate_class`, and rungs above the mapped one stay excluded.
  With `family_effort` `codex-sol=xhigh`, a consult admits `codex-sol@xhigh`
  (gate `escalation`). It still drops `codex-sol@max` as
  `above_role_effort`.
- The floor applies to the mapped rung too. If the catalog grades that rung
  below the asker's floor (for example `codex-sol@xhigh` graded `S` for an
  `S+` asker), it is dropped `grade_insufficient`, and its family's `max`
  rung is `role_effort_unplaced` with that cause. The choice is then
  `role_no_candidate` (exit 77) unless `--allow-escalation` admits
  `codex-sol@max`. panewire does not move the cap above `family_effort`
  for the floor.
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

The document remains v1. panewire reads no `lead` entry: a `consult` entry
is required, and a `lead` entry left in the table is ignored. Consult always
waits on exhaustion and never downgrades. The seat role cannot override its
routing role to obtain weaker eligibility.

### The v1 table

panewire reads the first fenced `json` block of `policy/model-routing`.
`version` must be `1`. `roles.R.allowed` lists catalog profile names, or
`"*"`. The table also supplies `effort`, `on_exhaustion`, `downgrade_to`,
`family_rule`, `fixed`, and, on consult, `family_effort`. A role may also set
`min_effort` (one of the effort ranks): selection drops a rung below it as
`below_role_min_effort`, and the routing stamp records it. Unknown
fields are ignored. Duplicate keys, invalid exhaustion modes, and role maps
naming absent roles are refused. A `family_effort` entry is refused with
`family_effort_invalid` in three cases: it sits on a role other than
consult, its key is neither a top profile nor a profile the role
allows (any profile ID when the role allows `"*"`), or its effort is not
one of `low`, `medium`, `high`, `xhigh`, `max`, or `ultra`.
This PR does not parse the proposed v2 schema.

The seat-role map is read from `panewire_role_map` or its pre-#155 name
`runtug_role_map` (the name hk publishes today); two maps that differ are
refused, and the cache stores the map as `panewire_role_map`. A seat role
the map does not name takes its builtin routing role (`worker` implementer, `tester` verifier, `reviewer` reviewer, `counsel`
consult) instead of running unfiltered.

`--routing-role consult|implementer|verifier|reviewer|judge`
on `spawn` or `plan` selects the routing role. Seat admission still uses
`--role`. Counsel retains its required routing role.
Delegate maps `reader` to `implementer`, `verifier` to `verifier`, and
`reviewer` to `reviewer`. `--patch` uses `implementer` eligibility.

A valid document is cached atomically at
`$XDG_STATE_HOME/panewire/model-routing.json`, or
`~/.local/state/panewire/model-routing.json`. If hk is unreachable or
unreadable, a usable cache supplies `policy_source=cache@v<N>`.
Without either source, consult and verifier return
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
selection of an eligible Codex consult, and exhausted Codex accounts do not
prevent selection of an eligible Claude consult.

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
quota cache, and the on-target native quota refresh when that reading is stale.
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
Consult never takes this path. Exit 86 reports a refusal, not a
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

`panewire resolve --role consult|implementer|verifier|reviewer|judge
[--grade G] [--t T0..T3] [--builder-family F] [--harness KIND] [--allow-escalation] [--json]` runs the same
selection core without a launch or job write. `--t` defaults to `T0`.
Reviewer and judge require `--builder-family` to exclude that family.
The result names the account and host selected now; it is not a reservation.
The text answer ends with `profile=<profile> effort=<effort>`.
`effort` is the role's starting effort, or the selected rung's when absent.
For a consult rung chosen above the role effort with
`--allow-escalation`, `effort` is the chosen rung's.

`--harness omp` restricts selection to OMP seats before quota ordering. For
`--role implementer`, an OMP choice also returns
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
| `--grade G` | The catalog grade the seat must cover (`C`, `B`, `A`, `A+`, `S`, `S+`, `SS`; default `C`). The task title is not read. |
| `--cwd DIR` | The tree the delegate reads (default: the current directory). |
| `--timeout D` | A duration or seconds, default 15m: the pane wait, or the headless CLI call. |
| `--headless` | Run the selected seat's CLI once on this host with the brief, instead of a pane. |
| `--patch` | The delegate works in a temporary detached worktree and the result carries a patch path (below). |
| `--panel N` | N from 2 to 4: see Panel. |
| `--family F` | The caller's model family, for a reviewer outside a panewire job. Inside a job the job's own family is used. |
| `--workspace W` | The herdr workspace for the pane, by label (`workers`), or `[local] workspace`. Not needed with `--headless`. |
| `--json` | Print one JSON document instead of text. |

**Selection.** The seat is chosen by the same code as `spawn` auto-selection
and `panewire plan`: catalog rungs that cover the grade and the role, ordered by
the pool policy (waste-risk pools first, then the band order), admitted through
presence and the quota gate. Free models are one band of that policy, not a
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
Roles are worker, tester, reviewer and counsel. Model IDs and grades
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

Use the complete shipped block when creating a custom policy; a custom file
does not pick up later bullets, such as the test-host rule that names
`mbp-server` by `hostname -s` (hk 3733). Selection order
is default → host → profile → local.brief_preamble. Missing block names refuse.
The policy version and selected block appear in every brief and dry-run output.
See [DESIGN.md](../DESIGN.md#standard-brief-preamble-the-single-source-of-fleet-rules)
for ownership and distribution.

| Environment | Meaning |
| --- | --- |
| `PANEWIRE_POOL_POLICY` | Overrides the pool-order policy used by `spawn` automatic model selection; `[local].pool_policy` takes precedence. |
| `HERDR_BIN`, `PANEWIRE_BIN` | CLI executable overrides. |
| `HANDOFFKEEP_BIN` | Must resolve to the canonical current-user ~/.local/bin executable in production (the spawn trust check); panewire no longer runs it. |
| `XDG_DATA_HOME` | Data home of the local ledger (`$XDG_DATA_HOME/arbiter/state.db`); production guard requires the canonical `~/.local/share`. |
| `XDG_STATE_HOME` | State home of the role routing cache (`$XDG_STATE_HOME/panewire/model-routing.json`); default `~/.local/state`. |
| `PANEWIRE_QUOTA_BIN` | 폐기된 환경 변수입니다. 설정되어 있어도 무시하며 한 번 migration 경고를 출력합니다. |
| `HANDOFFKEEP_URL`, `HANDOFFKEEP_TOKEN_SVC_PANEWIRE` | panewire's svc-panewire hk credential; both must be set to win over the file ([handoffkeep service credential](#handoffkeep-service-credential)). `HANDOFFKEEP_TOKEN` and `HANDOFFKEEP_CONFIG` are not read. |
| `PANEWIRE_HK_ENV` | The svc-panewire credential file; default `~/.config/panewire/hk-svc.env` (regular, mode 0600, read through a descriptor opened without following a leaf symlink). A node with no `spawn.json` reads it too, after the environment pair and with the svc key only. |
| `XDG_CACHE_HOME` | Cache home of the bench catalog read (`$XDG_CACHE_HOME/panewire/bench`); default `~/.cache`. |
| `ARTIFICIAL_ANALYSIS_API_KEY` | Free Artificial Analysis Data API key for `panewire catalog aa-sync`; else the mode-0600 `$XDG_CONFIG_HOME/panewire/aa.env`. |
| `PANEWIRE_NODE_ENV` | Panewire node env file for `HUB_MACHINE_ID` (job record host id); default `~/.config/panewire/hub-node.env`, or `[quota].node_env`. |
| `ARBITER_INBOX_ROOT` | Jobs root; production guard requires the canonical inbox. |
| `HERDR_SESSION` | Herdr session passed through or selected for remote execution. |
| `PANEWIRE_E6_ARM` | Exact experiment marker when policy requires one: `<canon>@<effort>`, or `<canon>@` (nothing after the `@`) for a rung whose catalog effort is empty, such as `devin-ds41-max@`. The marker names the rung the spawn resolves to: the seat gate refuses `<canon>@` for a rung with an effort and `<canon>@<effort>` for an effortless rung. A hub spawn carries it to the hub and node, so it applies to `--host auto|local|NAME` alike; a marker whose canon differs from `-m` is refused at submit (exit 2). |
| `ARBITER_JOB`, `HK_TASK_ID` | Calling job/task inheritance; populated in spawned panes. |
| `PANEWIRE_LANE_STRICT=1` | A committed spawn whose hub lane registration failed returns 82 (see [Lane registration failure](#lane-registration-failure)). |
| `PANEWIRE_COMPLETION_TIMEOUT_S` | Positive watcher lifetime; duration or bare seconds, default 1h. Expiry retains the job. The watcher completes a job only on a [terminal signal](r21-lane-event.md#job-completion-signal), never on a stable report alone. |
| `MOCK_MCP_PROFILE`, `PANEWIRE_MOCK_MCP_CONFIG` | Required reviewed mock profile name and exclusive stdio mcpServers JSON. |
| `CLAUDE_CONFIG_DIR`, `CODEX_HOME` | Harness config homes for selected-workspace trust; mock Codex uses its private job home. |

Legacy periodic prune environment switches are not read. Panewire never
silently enables automatic pruning.

## Delivery policy relay keys

The hub's delivery policy (`--delivery-policy`, `PANEWIRE_DELIVERY_POLICY`,
embedded `delivery-policy.yaml`) carries the lane classes under `relay:`. See
[inbox-only lanes](quiet-delivery.md#inbox-only-lanes) for the behaviour.

| Key | Default | Meaning |
| --- | --- | --- |
| `relay.design_lanes` | `["*-design"]` | Design lanes: inbox-only, quieted worker results, no idle-wake or digest. |
| `relay.desk_lanes` | `["operator-desk", "*-operator-desk"]` | Desk lanes: inbox-only. A desk lane is never a design lane. |
| `relay.inbox_wake_window` | `5m` | Coalescing window of the one-line wake, `10s` to `1h`. |
| `relay.worker_inbox_trial` | `[]` | Worker lane patterns that are inbox-only with the wake during the trial. |
| `relay.worker_inbox_trial_until` | none | RFC3339 end of the trial. It is required with a trial list, and the trial is off from this instant. |
| `relay.drain_wake_interval` | `1h` | Least gap between two drain lines for one design lane, `10m` to `24h`. |
| `relay.drain_wake_threshold` | `0` | A design lane counts as drained at or below this many in-flight jobs, `0` to `10`. |

The wake line is
`[inbox] <lane>: N unread (most urgent: <kind>) | panewire inbox list --lane <lane>`.
Policy changes need a hub restart. The trial's end needs none.

## Local job ledger

Since RT-arb the host-local ledger lives in panewire (`internal/arbiter`); panewire
no longer executes the agent-skills `arbiter` script, and `ARBITER_BIN` is not
read. hk job records hold the cross-host job state; the ledger keeps what they
do not replace:

| Part | What it holds |
| --- | --- |
| Jobs | One row per job claimed on this host (`claimed → leased → released`), written with its `job.claim`/`job.reclaim` event. The duplicate arbiter on this host; cross-host duplicates are refused by the hk claim first. |
| quota_pool records | One execution record per job and pool, written at admission and deleted at `done`, `reap` or a failed spawn. No TTL. |
| path, linear_permit leases | Exclusive leases with a TTL (90 and 30 minutes by default), a monotonic fencing token and archived history. Library only: panewire itself takes no such lease today. |
| Job events | `job.*`, `lease.*` and `quota_pool.*` events, one artifact each. |

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
| `spawn` (hub) | `stuck` | the request's status, `reconciling` or `deadline_exceeded` | a hub spawn request is still pending 30 minutes past its deadline; written once per attempt with `request_id`, and `detail` is `host=<node> past_deadline_s=N`. See [hub-spawn-operations.md](hub-spawn-operations.md#stranded-requests) |
| `placement` | `placed` | ranking metric: `score`, `raw_load` (some host had no hub score), `single_candidate`, `wake`, `forced` | a non-dry-run spawn chose a host; `detail` names `chosen=`, `candidates=`, `pool=` and the hosts that `refused=` first |
| `placement` | `refused` | automatic selection's outcome code, such as `quota_exhausted` or `quota_unknown` | selection refused before execution placement; `selection` retains the ordered host and rung attempts |
| `quota_observation` | `stale` or `unusable` | the shared-observation reason (`stale`, `not_observed`, `measure_failed`, ...) | a target account's quota observation refused a host; `detail` carries `target=`, `pool=` and `age_min=` |
| `relay` | `delivered`, `dropped:<reason>` or `unconfirmed:<reason>` | the drop code, or the first token of the report | the hub saw one worker or design lane row's outcome |
| `spawn_held` | the held branch (`succeeded_pane_held`, `failed_pane_held`, ...) | the pane-probe reason | a refused reclaim kept a role reservation |
| `job_result` | `conflict` or `missing` | `different_result` or `no_result` | a `done` conflicted with, or lacked, a result |
| `inbox_wake` | `sent` or `skipped` | the lane class (`design`, `desk`, `trial`), or `startup` for the line a hub start owes, when sent; `count_failed` or `unrouted` when skipped | the hub typed, or could not type, an inbox-only lane's one-line wake; `job` is the lane and `detail` names `unread=`, `most_urgent=`, `machine=` and `pane=`. See [inbox-only lanes](quiet-delivery.md#inbox-only-lanes) |

The relay, held-reservation and job-result kinds and their Sentry fingerprints
are in [observability](observability.md).

Every field is redacted (tokens, URL credentials) and bounded (`detail` at
1 KiB); the file rotates to `.1` at 4 MiB. Writing an event never changes the
command's result. Grafana Alloy ships the file to Loki; the shipping, dashboard
and alerts live in panewire `deploy/alloy` and `deploy/grafana`.

Automatic `placement` lines also carry the bounded host and rung `selection`
trace described in [Automatic selection](placement.md#automatic-selection).

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

Execution metadata includes `family`, the admitted `model`, `role` (`worker`, `review`, or `consult`), `profile`, `pool`, `pane_id`, the repository
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
| 70 | Configuration or trust refusal, including `POLICY_UNAVAILABLE`. |
| 74 | Active job duplicate, including an hk `job_id_taken` claim refusal. |
| 75 | Unreadable duplicate state (the ledger's record of a duplicate job has no state or an unreadable `job.spawned` receipt; a released job has no receipt and is reclaimed), changed artifact, or a `--parent-job` that resolves neither locally nor in hk within the bound. |
| 76 | `BRIEF_SUBMISSION_UNCONFIRMED`: no working transition or submitted brief turn on the launch session and pane. Codex retries a submit key once only for composer residue; other harnesses retain one submit-key retry when submission remains unconfirmed. |
| 77 | `NO_ROLE_CANDIDATE` or seat, grade, family, or E6 refusal; quota gate exit 5 is remapped here. `role_restricted:` preserves the gate reason and names the rule that refused. 역할 제한 이유가 표시되면 그 사유가 가리키는 역할 또는 프로필을 선택하세요. |
| 78 | `preflight`: at least one checked remote host failed a check; the reasons are on stdout and in the error. |
| 79 | Mock isolation refusal. |
| 80 | Local spawning disabled/unavailable without a remote fallback. |
| 81 | `NO_ELIGIBLE_HOST`, wake/capacity placement failure, the remote worktree for a linked-worktree job could not be prepared (also a `via = "hub"` checkout refusal), or the hub is unavailable for a forced `via = "hub"` host. |
| 82 | Pane committed but lane unregistered under strict lane reporting (`PANEWIRE_LANE_STRICT=1`). |
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

`panewire work TASK_ID` expands task metadata into
normal spawn flags. It uses `-m auto --host auto -w workers`, derive `--t`
from the title marker, and pass the numeric task binding. The task marker
continues to supply grade to admission.

`panewire lead TASK_ID` and `--role builder` are refused with a usage error
(exit 2) and one line: the delegated lead seat is retired, and lead is an
operator-opened session that works through subagents.

Job/label default to `refs.job_id`, otherwise `task<ID>-work`.
Working directory comes
from `refs.worktree.path`, `refs.worktree`, `refs.cwd`, then the current directory.
Brief/report use `refs.brief_path` / `refs.report_path`, otherwise
`<jobs>/<job>/brief.md` and `report.md`. `--owner` is resolved from the task's
project, not from the caller lane. When it is omitted, `panewire work` and
`panewire spawn` read the task's `project`, ask handoffkeep
(`handoffkeep tasks owner PROJECT`, the
`GET /v1/tasks/projects/{project}/owner` endpoint) for `effective_owner`, and
use it, printing `owner=<lane> source=hk project=<p>`. An explicit `--owner`
always wins. If hk cannot answer (unreachable, no such project, the task names
no project, or `effective_owner` is empty), the command is refused with exit 2
and a message to pass `--owner`; panewire never guesses or silently falls back
to operator-desk. A spawn with no `--task` and no `--parent-job` keeps its
previous default. Flags after the task id override defaults. Save the brief first; presets do
not invent or overwrite instructions. A dry run uses the same planning path
as the equivalent long spawn command and creates no claim or worker.
A `refs.brief_path` of `hk:doc/KEY` reads the brief from hk, as `-p` does
below.

## Brief from hk (`-p hk:doc/KEY`)

`panewire spawn` and `panewire work` take the brief from an hk document when
`-p` starts with `hk:doc/`: the rest of the value is the document key, for
example `-p hk:doc/brief/2026-11-09/my-job`, the key a design session stores
its briefs under. Every other `-p` value is a file path and reads exactly as
before. `panewire delegate --prompt FILE` has its own reader and takes only a
file.

The key must be nonempty, at most 512 bytes of UTF-8, and must not start with
`/` or contain a control character or `..`; any other key is refused with
exit 2 before hk is asked. The body is read once, on the command's host, over
hk HTTP with the [svc-panewire credential](#handoffkeep-service-credential)
(`GET /v1/documents/{key}`), before any side effect: no job directory, no
`submission.json`, no hub submission and no pane. The submission then carries
the body inline, as it carries a file's, so the hub and nodes need no hk
access for it and no newer wire.

| Failure | Exit | Refusal names |
| --- | --- | --- |
| Invalid key | 2 | the key and the `hk:doc/KEY` grammar |
| Credential unconfigured or endpoint refused | 70 | the key and what is missing, never a value |
| Key not found | 2 | the key |
| hk answered 2xx with something that is not that document | 75 | the key, `unreadable hk answer` |
| hk unreachable or an hk error | 75 | the key and hk's error |
| Empty or whitespace-only body | 2 | the key |
| Body over 64 KiB (`spawn` and `work` without `--dry-run`) | 2 | the key and its size |

The same limits apply as to a file: the submit path takes at most 64 KiB of
UTF-8, and `--dry-run` has no size bound. A read brief prints
`brief_doc=KEY source=hk bytes=N`. The submission keeps `-p hk:doc/KEY` in its
args, so `jobs/JOB/submission.json` (and the hub operation built from it)
records that the brief lives in hk under KEY; the brief body is kept inline
there too, and the executing host still writes `brief-delivered.md` as for a
file brief. The hk job record has no brief field, so it does not name the key.

## Host aliases and workspaces

Canonical `[hosts.<machine-id>]` keys name hub machines. Set
`aliases = ["short-name"]` in that table; an existing SSH config alias also
resolves to its host key. No fleet-specific mappings are built in. Ambiguous
aliases refuse and name the canonical-key fix. Resolution prints the alias
and resolved machine id. The optional `local.aliases` list names this machine.

Omitting `-w` uses the executing host's `local.workspace`. Remote delegation
uses the target host's configured workspace, not the caller's label. Name a
workspace by label (`workers`), not by id: herdr deletes a workspace when its
last pane closes, so spawn recreates a label that matches zero workspaces with
`herdr workspace create --label` and uses the new id. Two spawns may race; the
re-list keeps the oldest match and closes nothing. A label with more than one
match still refuses and tells the caller to pass the exact workspace id. An
explicit workspace id is used only while that host/session's `herdr workspace
list` still contains it; a vanished id refuses and names the label fix.

A spawn that created a workspace closes only its own tab there when the launch
fails, never the workspace, so a workspace another spawn adopted keeps that
spawn's tab and live session. Herdr closes the workspace with its last tab, so
an unused created workspace still disappears.

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

## Task pane sweep

`panewire daemon` closes the pane of a worker or tester whose bound hk task is
merged, done or dropped, or whose job was closed, once the pane has stayed not
working for a grace; a completed author's pane waits while its PR is open. The
rules are in [pane-sweep.md](pane-sweep.md). Two daemon flags configure it:

- `--task-pane-sweep-interval DURATION` — how often the sweep runs. Default
  `5m`; `0` turns it off; any other value must be at least `10s`.
- `--task-pane-sweep-grace DURATION` — how long a pane must have stayed not
  working before it is closed. Default `10m`; `0` closes at the first sweep that
  finds the task ended.

The sweep needs `--hub-url`, `--hub-token-env` and `--hub-jobs-root`, and reads
hk through `handoffkeep_env` in the node's `spawn.json`. After a close it also
removes the worktree the node prepared, unless `[sweep].remove_worktrees` is
`false`; see [pane-sweep.md](pane-sweep.md#the-worktree).

## Lane registration failure

A spawn registers the new pane's lane with the hub (`panewire lanes add`). A
failed registration is retried once after a one-second pause; a local
configuration refusal (exit 70) is not retried. If the second attempt fails
too, the pane is already running, so the spawn still commits, but it says so:

- `job.spawned` carries `lane_unregistered: true` and no `hub_lane`.
- A `LANE_UNREGISTERED` lane event (type `blocked`, event id
  `<job>-lane-unregistered`) goes to the parent lane: the owner of the job. Findings and escalations from this pane have no hub
  route until the lane is added by hand (the event text names the command).
- The `OK` line, the `--wait` output (`wait job=… lane=UNREGISTERED`), the
  `SPAWNED job=…` line of a hub-submitted spawn and the hub's `[panewire] SPAWNED`
  notice (type `automation-failed`) name the problem.
- The exit status is 0, unless `PANEWIRE_LANE_STRICT=1` is set: then it is 82.
  A success receipt (`landed=yes`) is authoritative even with exit 82; do not
  spawn again.
