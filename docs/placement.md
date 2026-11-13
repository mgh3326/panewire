# Fleet placement API

`GET /v1/placement?class=worker&cwd=repo-key` is an operator-token protected,
read-only placement judgement. `class` is `worker` or `verifier`; `cwd` is an
optional opaque repository key used only as request metadata. It does not send
a brief, a path, a token, or a command to a node.

The hub first reads the operator-owned `/etc/panewire/placement.json` (or the
path passed to `panewire hub --placement-policy`) and uses its local machine
first. A local candidate spills only when it is disconnected or not
accepting, its free memory is below `memory_free_pct_min`, its thermal speed
limit is below one, or active heartbeat jobs reach `max_active_jobs`.
`spill_targets` are considered in policy order. Load never filters a
candidate; it ranks candidates through `score` (see
[Measured-load score](#measured-load-score)), and callers that rank hosts —
panewire's host-first placement — read that score.

```json
{
  "local_machine": "mac-work",
  "spill_targets": ["desktop"],
  "max_active_jobs": 5,
  "memory_free_pct_min": 30,
  "spawn_bridge_seconds": 300,
  "machines": {
    "desktop": {"task_slots": 2}
  }
}
```

`load_ratio`, `swap_used_mb_max`, `swap_in_per_s_max`, `psi_some_avg10_max`,
`pressure_level_min`, `swap_in_half_mbps` and `capacity_weight` are retired. A
policy file that still sets them loads, and the values are ignored (reload logs
a warning for `capacity_weight`; set `power` instead). The score constants have
their own optional `score` object, see [Constants](#constants).

## Automatic selection

For `-m auto`, selection ranks eligible hosts before choosing an account or
model rung. Host capability tags, accepted roles and grades, accepting state,
and harness readiness filter the candidates. Within each load bucket,
selection uses score v2 when every eligible host has a score, then the hub's
decided host as a tie-breaker; if any host lacks a score, the hub decision
breaks ties before the raw-load fallback. Score v2 includes the free fraction
weighted by host power and recent-spawn bridge cost. Selection uses that
capacity signal before choosing a seat because placement cannot change the
profile after selection pins it to the hosts that hold that seat. This lets an
auto request choose a seat on a less loaded host even while the hub's
hysteresis still names another host.

On each host in that order, selection considers only the accounts it holds
and the rungs its harnesses can launch. Grade, role, routing, effort, and seat
checks still apply. The quota policy orders those rungs within that host:
waste, then free and credit together, then ranked, including the existing
waste-risk and credit-pacing rules. A credit pool is never waste-risk: it
ranks in the free band and is ordered there by the dollars at risk per hour
until expiry — the balance less the spend projected before expiry — or, for an
entry without a recorded dollar balance, by the percentage rule (remaining
percent per hour). On a full tie inside the band a credit rung goes before a
free rung.

In the ranked band each pool offers one seat for the task's grade (its
lowest-grade eligible row that fits, or the row the pool-order policy's `seats`
names), and pools are ordered by their seat's waste urgency: the pool with the
most room first, ties in lowest-grade order. See `docs/config.md`.

A manually recorded dollar balance stays dollar-ranked for four hours after
`measured_at`; after that it is stale, loses its dollar value, and ranks behind
subscription quota. Entries with only a percentage never become stale because
they have no dollar source to age; they continue to rank by remaining percent
per hour. A configured total supplies a dollar source for an entry with only a
percentage. Plan output records `state=stale` and `stale_since` for aged dollar
balances.

Selection walks the ranked hosts twice. Pass 1 tries every candidate except a
subscription pool paced behind credit (a subscription on pace to use up its
window, on a host that also holds credit worth spending). Pass 2 walks the same
hosts again with those candidates and runs only when pass 1 admitted nothing on
any host. Credit is a pass-1 candidate with the free band. The first admitted host,
rung, and account wins. A host whose accounts are exhausted or unknown is
skipped. A credit candidate left unchosen because pass 1 admitted another
account records `below_choice`. A pool is paced behind credit only when that credit is present on its own host
(T2997): credit on another host never demotes a paced pool. A paced pool on a
higher-ranked host is therefore chosen in pass 1 even when the only credit is
on a later host. Pass 1 is also host-first: a higher-ranked host's paced pool
is admitted before a later host's ranked rung is reached.

Measured quota exhaustion is cached by account for the decision. Several
hosts holding one account share one quota and do not repeat its exhausted
gate. Rung-specific policy refusals still allow other rungs, and a free
bucket can still admit a rung despite the subscription's projected use.
Explicit `-m PROFILE`, `--host`, and hub-directed spawns keep their existing
placement and admission paths.

The `job.placement` event's `selection` object and `plan --json` include
`host_order` and each host's `rank`, `pass` (1, or 2 for a subscription pool
paced behind credit), `tried` rungs, and `admission_outcome`. `plan --json`
lists a host once per pass that tried it; pass 2 rows exist only for hosts that
hold a paced candidate. The `job.placement` selection and the placement record list each
host once, keeping the pass 2 row (the record keeps pass 1 when it tried
something).
An attempt records `admitted`, `quota_exhausted`, `quota_unknown`, or `no_rung`.
Hosts after the winner record `not_tried`. The existing `outcome`, `result`,
and readiness-drop fields retain the more specific host and rung reasons.
Fleet `placement` events also carry a bounded `selection` array in host rank
order, with each host's outcome and tried profile, effort, pool, and outcome.
It omits account identities and gate output. Refused automatic selections
emit this trace too. The handoffkeep placement API payload is unchanged.

### Exhausted API credit

An idle omp job closes its own API credit account when its session records a
provider credit rejection. The watcher reads the final structured assistant
error, not pane text, and uses omp's completion timestamp (or the session entry
timestamp when completion time is absent). This is the harness host's time;
the provider response does not include a recorded rejection timestamp. The
watcher records a pending publication locally first, then writes a zero balance
to the shared manual credit store before it announces the quota stop. It makes
at most three attempts in one watcher run. If the account cannot be identified,
or those attempts fail, it sends one owner notice and pauses while keeping the
pending rejection and job directory. The node retries a paused publication
when the account can be identified. Before a recovered publication hands off a
job, the watcher requires Herdr to be reachable and the pane to remain idle at
the state-change sequence recorded with the rejection; it still publishes the
account closure if the pane has resumed, but does not end that job. The stored
observation has no billing expiry; a later manual credit measurement reopens
the account. Replaying an older rejection cannot replace a newer entry.
Selection and `plan` show `credit_exhausted` and its `since` time. The shared
store keeps the close across a hub restart and when `/v1/state` is unavailable.
If the shared store cannot be read, admission fails closed.

### Pool concurrency cap

`[quota].pool_max_concurrent = { "openai-api" = 2 }` limits live spawns on a pool,
fleet-wide ([config](config.md#quota)). Selection drops a candidate whose pool
is at its cap with the reason `pool_at_cap`, naming the cap in `cap` (the
candidate's pool, or the reserved `devin-free` key). The selection record in
`job.placement` and JSON plan output also includes `cap_live` and `cap_limit`:
the count and limit used for that decision. Text plan output shows `live` and
`limit` beside the cap. These counts are a snapshot, not a reservation.
Selection then goes on to the next candidate or pool as for any
other drop. A spawn never waits for a slot. If every candidate is dropped and
at least one only for `pool_at_cap`, the refusal names the caps and their
limits (`pool_at_cap: openai-api=2 at [quota].pool_max_concurrent`).

The reserved key `devin-free` caps only the SWE-2 free rungs
(`devinFreeRungs`): `devin-swe2`, `devin-swe2-medium`, `devin-swe2-max`,
`devin-swe2-flash`, `devin-swe2-flash-medium`, and `devin-swe2-flash-max`;
their live spawns are counted by catalog profile, seat
aliases included. It exists because those rungs spend Devin's free-model quota
while paid `devin-ds41` spends the account's paid window. `devin` remains a
whole-pool cap, so a free rung must pass both keys when both are set.

The hub counts, because it places every hub spawn
([durable spawn operations](hub-spawn-operations.md)). A live spawn on a pool
is a hub spawn operation that is accepted, bound or dispatching, or succeeded with a
job that a node still lists as active, whose placed profile is in that pool.
A spawn that succeeded less than two minutes ago counts before its first
heartbeat. An auto spawn that is still being placed has no profile yet and
is not counted, and neither is the spawn being placed. The hub's own selection
reads the count from its operations. Any other selection (`plan`, a direct
spawn) reads it from the hub with `GET /v1/spawns/live` and the operator
credential.

When the count cannot be read the cap does not apply. Selection records
`pool_cap_count_unavailable` under `degraded` and the node writes a warning
with the cause. The cap is checked at each placement, so two spawns placed at
the same moment can both see one free slot.

## Readiness

Placement used to rank hosts on load and account presence only. Every
non-m1b spawn that failed in the 7 days to 2026-11-04 failed on readiness
(19 of 19: `brief_doc_unavailable`, `cwd_unmapped`, Codex "Hooks need review",
the omp setup wizard), and a host that had just failed a launch was ranked
again unchanged. A node now reports what it can launch, and placement drops a
(host, harness) pair that is not ready **before it ranks hosts**.

### What a node reports

The heartbeat's `readiness` object (type `fleet.Readiness`). It holds names,
versions and booleans. It never holds a credential, a path, a file's contents
or screen text; the hub re-checks every field against a closed vocabulary and
drops what does not fit.

| Field | Meaning |
|---|---|
| `harnesses.<kind>` for `claude`, `codex`, `omp`, `agy`, `devin`, `cmd` | `installed` (the binary is found; `cmd` is always installed), `version` (the first dotted number of `--version`), `herdr_integration` (`herdr integration status` says `current`), `login` (`present` / `missing` / `unknown`) with `account` (the pacelever account key), `dialog` |
| `harnesses.<kind>.login` | From the node's own refresh of the pool named like the kind (`--state-quota-pools`): `claude`, `codex`, `commandcode`, `devin` and `agy` are measured by the node itself. Other pools are unmeasurable and read `unknown`. `present` follows a successful refresh. `missing` follows a refresh failure with an `error_kind` of `auth`, `auth_failed`, `credentials` or `token_expired`. Any other failure (timeout, transient error, `excluded`, unreadable status) keeps a present account while its last successful refresh is under 5 minutes old, and otherwise reads `unknown`. |
| `harnesses.<kind>.dialog` | The first-run dialog the **last launch** stopped at: `codex_hooks_review`, `codex_directory_trust`, `omp_setup_wizard` or `claude_trust`. It is the last known state, set by a failed launch and cleared by the next launch of that kind that succeeds. The node does not open a harness to look, and a dropped pair is not launched again, so a reported dialog also expires after 30 minutes: the pair then gets one more launch, and a launch that stops at the dialog again reports it for another 30 minutes |
| `repos` | The node's `cwd_map` keys (on defaults: the directories under the work root) |
| `brief.handoffkeep_env` | The handoffkeep env file is present, mode 0600 and holds a URL and token |
| `toolchains` | `go`, `node`, `bun` with their versions, when installed |
| `panewire_version` | The node's build |
| `config` | The effective spawn configuration: `source` (`defaults` or `spawn.json`), `workspace`, `herdr_session` (the built-in default name, else `custom`: spawn.json names are free text and never travel), `work_root` (whether it exists). See [r26-hub-spawn.md](r26-hub-spawn.md) |

The report is cached on the node. It is refreshed every 5 minutes and after
every failed launch, probed in the background so a probe never delays a
heartbeat, and re-sent every 45 seconds while unchanged so the hub's value
stays fresh. The hub drops a stored report that was not re-sent for 5 minutes
(the node reads `readiness_unknown` until the next one). A node sends it only to a hub that advertised the `readiness`
feature in `hub.features` (an older hub rejects a heartbeat with an unknown
field), and starts over on every connection.

### The filter

The hub publishes each host's report as the `readiness` fleet-state value
([fleet-state.md](fleet-state.md)). Host-first selection reads it and drops a
(host, harness) pair that fails a check. The first failing check names the
outcome, in this order:

| Outcome | The pair is dropped when |
|---|---|
| `launch_backoff` | The hub counted 2 consecutive launch failures of this (host, kind) in the last 30 minutes ([below](#launch-failure-backoff)) |
| `harness_missing` | The harness binary is not installed |
| `harness_version` | `placement.harness_min_version.<kind>` (in the spawner's `hosts.toml`) is set and the version is lower or unreadable |
| `dialog_pending` | The last launch of this kind stopped at a first-run dialog ([desk notice](#first-run-refusals-and-the-desk-notice)) |
| `login_missing` | The node's refresh of the kind's pool failed with a login error kind, or its login probe did, or its last launch ended rc=76 at the harness's login screen ([fleet-state.md](fleet-state.md#login-probe)); that verified agy login-screen result is sent as rc=88 `auth_unavailable` so auto placement can try another host; `cmd` needs no login ([desk notice](#first-run-refusals-and-the-desk-notice)) |
| `repo_unmapped` | The spawn's repository is not in the node's `repos` |
| `brief_unavailable` | The spawn goes through the hub with a brief document and the node cannot fetch one |
| `toolchain_missing` | `placement.repo_toolchains.<repo>` lists a toolchain the node lacks |

A drop removes one pair, not the host: another harness on the same host stays
a candidate, and the dropped host still measures an account it holds. The
outcome is recorded where the placement event already records a host: the
host's `outcome` in the selection record names the check, `readiness_drops`
lists each dropped profile with its code, and the placement event's candidate
`reason` carries the same code. A kind the node does not report on
(`kiro`, `opencode`) is not judged.

`repo_unmapped` is about the node's `cwd_map`, not about what it can clone. A
spawn that carries a linked-worktree checkout (origin URL and a branch or
commit) is cloned into `<work_root>/<repo>` by `worktree.prepare`, so a node
whose readiness reports `config.work_root = true` is not dropped for
`repo_unmapped` even when the repository is absent from `repos`. The drop
remains when the spawn has no checkout (a local-cwd spawn that relies on
`cwd_map`) and when the node has no work root. When every candidate is dropped
this way the refusal is `NO_ELIGIBLE_HOST` with the dominant reason and a
matching repair, for example `repo_unmapped: no node maps repo X; add it to
cwd_map or ensure work_root`, never `AUTH_UNAVAILABLE`.

The node follows the same rule: a spawn that carries a checkout on a node
with a work root proceeds to `worktree.prepare` even when the repository is
absent from `cwd_map`.

**Old nodes.** A node that never sent readiness is `readiness_unknown`, and
no readiness check drops it: it is placed as before. A readiness value that
is stale, or whose publisher is dead, reads the same way. Only the
launch-failure backoff, which the hub observes itself, still applies to it.

### Launch-failure backoff

The hub sees every node answer to a spawn. It counts consecutive launch
failures per (host, harness kind). Two in a row exclude that pair for 30
minutes; a success clears the count at once. Only a failure that shows the host
failed to launch counts:

| Node reason | Counts | Why |
|---|---|---|
| `spawn_failed`, with or without its engine code (`spawn_failed:<code>`) | yes | the engine did not launch on this host |
| `spawn_disabled` | yes | the host's spawn policy refuses every launch |
| `cwd_unmapped` | yes | the host has no path for the repository |
| `brief_doc_unavailable` | yes | the host cannot fetch or resolve its brief document |
| `timeout` (landing or queue) | no | the answer was late; the launch state is unknown |
| `deadline_not_started` | no | the node claimed the request after its deadline |
| `unknown`, `reconciling` | no | the hub could not tell whether the launch happened |
| `journal_unavailable`, `journal_conflict` | no | the node's spawn journal, not the launch |
| `invalid_args`, `invalid_request` | no | the request is malformed |
| `checkout_refused:<code>` (`busy`, `dirty`, `sha-missing`, `branch-elsewhere`, and the rest) | no | checkout state, not the harness's ability to launch, on either the terminal or next-host path |
| any other reason, or none | no | not a host launch fault |

A profile with no harness kind is not counted. The
constants live in `placement.json`:

```json
{"launch_failure_threshold": 2, "launch_backoff_minutes": 30}
```

Both are optional (0 or absent means 2 and 30). The counts are in hub memory:
a hub restart forgets them, which ends a backoff early and never starts one.

### First-run refusals and the desk notice

A first-run dialog or a missing login rules out one host, not the request. When
the hub places a spawn under `--host auto` and a node refuses with one of these
engine codes, the hub unbinds the request and the candidate loop tries the next
host:

| Code | Cause | Desk action |
|---|---|---|
| `hooks_review` | Codex "Hooks need review" | `run codex once and choose "Trust all and continue"` |
| `trust_prompt` | Codex or Claude workspace trust | `trust the workspace` |
| `omp_setup_wizard` | omp setup wizard | `run omp once and finish its setup` |
| `auth_unavailable` | No usable login for the account, including a verified rc=76 launch at a harness login screen | `log in` |

`cwd_unmapped` moves on the same way. The refusing host still counts the
failure in its launch backoff. A refusal moves on only when the node verified
its pane cleanup (`retryable`); otherwise a pane may still hold the job id and
the refusal is the request's answer. A pinned `--host` keeps its refusal.
Nothing answers the dialog: trust stays a one-time human action on the host.

The first time a host reports a need for a harness kind, the hub sends one
`lane.event` to `operator-desk` and one Sentry warning (when hub Sentry is
configured). The Sentry event is titled `ACTION NEEDED: <host> <kind> — <action>`
(quotes dropped from the action) with fingerprint
`[action-needed, host, kind, action]`, so the existing Sentry→Discord rule posts
one message per host and action. The hub resolves nothing: a recurrence after the
operator resolves the issue alerts again as a regression. The desk line:

```
[oci] codex needs a one-time action: run codex once and choose "Trust all and continue"
```

A need is a readiness `dialog` value or `login: missing`; a refusal above counts
as a report of the same need. The text carries the host, the kind and the
action only, never screen content. The hub tells each (host, kind, need) once
and forgets it when a later readiness report no longer shows it, so the need
coming back is told again. A report computed before the notice cannot forget
it. The told set is in hub memory: a hub restart tells the needs that are still
reported once more.

### Spawner configuration

Two optional `[placement]` tables in the spawner's `hosts.toml` feed the
filter:

```toml
[placement.harness_min_version]
codex = "0.166.0"

[placement.repo_toolchains]
panewire = ["go"]
```

## Measured-load score

The score answers "which host will lose the fewest spawns", from what the
hosts' exporters send to Prometheus and from the jobs the hub sees. Score v2
replaced the v1 formula on 2026-11-04. v1 ranked on CPU busy, but on the
busiest host CPU busy did not predict spawn failures: load per core, page-ins
and available memory below about 40% did (hk:doc
`research/2026-11-04/placement-score-vs-failures`, §4). The host sat at 0.24
CPU busy with load1 at 20–44 on 10 cores and its score stayed near 70.

```text
free  = cpu * mem * io * load * jobs
score = free * power - bridge_cost * 100 / power * claims_in_the_bridge_window
        - 1000 if disconnected, - 500 if load_unknown, - 200 if not accepting,
        - 100 if throttled
```

Each term runs from 0 to 1, and is 1 when its signal is absent or harmless.
`power` stays multiplicative. `capacity_weight` is gone.

| Term | Formula | Signal (PromQL, aggregated by `machine_id`) |
|---|---|---|
| `cpu` | `1 - max(cpu_busy_1m, cpu_busy_5m)` | `1 - avg(irate(node_cpu_seconds_total{mode="idle"}[3m]))`, and the same with `rate(...[5m])`. A job that just went heavy shows at once; a sustained one is not forgotten when one minute dips. |
| `mem` | `clamp((mem_available - 0.20) / 0.20, 0, 1)` | Linux `MemAvailable / MemTotal`; macOS `1 - (internal - purgeable + wired + compressed) / total`. 0 at 20% available, 1 at 40%. |
| `io` | `1 / (1 + pagein_MBps / 10)` | macOS: `sum(rate(node_memory_swapped_in_bytes_total[5m])) / 1048576`. On macOS node_exporter maps this counter to VM page-ins, which include file-backed reads. Linux: `sum(rate(node_vmstat_pswpin[5m])) * 4096 / 1048576`, with the half point at 20 MB/s (real swap-ins; 4 KiB pages assumed). A host that reports both uses the macOS one. |
| `load` | `1 / (1 + max(0, min(load1_per_core, 4) - 0.75))` | `max(avg_over_time(node_load1[3m])) / count(count(node_cpu_seconds_total{mode="idle"}) by (machine_id, cpu))`. Costs nothing up to 0.75 per core; the reading stops counting at 4. |
| `jobs` | `1 / (1 + live_jobs / 8)` | The node heartbeat's active-job list (below). |

Why these shapes (the evidence is one host, one day and 16 failures; the
constants are the knobs to tune with the telemetry below):

| Signal band on the busiest host | Load-plausible failures per hour |
|---|---|
| load1 per core < 0.5 (63% of the time) | 0.1 |
| 1–2 | 2.4 |
| ≥ 2 (9% of the time, 10 of 16 failures) | 5.7 |
| mem available ≥ 0.4 / 0.3–0.4 / < 0.3 | 0.2 / 1.3 / 4.5 |
| page-ins < 2 / 5–10 / ≥ 10 MB/s | 0 / 1.2 / 4.5 |
| cpu1 0.15–0.25 / 0.25–0.35 / ≥ 0.35 | 2.2 / 1.5 / 0 (not monotone) |

Load and page-ins are correlated, so the terms overlap on purpose. CPU stays
as a term but carries no special weight.

**Spawn bridge.** A metric lags a spawn by minutes, so for
`spawn_bridge_seconds` (default 600; 1–3600) after a job's claim the job
itself costs `bridge_cost * 100 / power` points (`bridge_cost` defaults to 10, so
10 on a power-100 host, 20 on power 50, 33 on power 30 and 83 on power 12). The
same job is a larger share of a weak host, so a weak host is held back longer. That keeps the next spawn off the same host until the
metrics cover the first one. After the window the job costs nothing: its load
is in the metrics, and the `jobs` term counts it for as long as it lives.

A job counts from the placement decision, not from the claim (hk 4448). The
claim reaches the hub only after the node has checked out, started and landed
the job, which took over a minute in the 2026-11-13 burst that sent six jobs to
one host. The hub's spawn engine tells the hub which host it is trying at the
moment it decides (`App.PlacementTry`), and the hub holds that job against the
host (`placement_count.go`) until one of these happens:

- the node's heartbeat lists the job: the claim is the count from then on, so
  the job is never counted twice and the count never exceeds one claim;
- the request ends without a launch (refused, never started, timed out): the
  count is released, and a refusal that moves on to the next candidate moves it
  to that host;
- `spawn_bridge_seconds` pass without a claim, as for any claim.

A request still reconciling, and a launched one whose claim has not shown, keep
their count. The count adds to the host's `bridged_jobs` and so to the
bridge cost only. It does not change `active_jobs`, the `jobs` term or
task slots. The count is only that: a claim from a node other than the decided
host does not release it.

One decision at a time reads the hub's view and counts its job: the engine takes
the hub's decision gate (`App.PlacementGate`) before it reads hosts and gives it
up at its first `PlacementTry`, so jobs submitted together see each other. Each
decision is a new plan read under the gate: the first, the fallback after a
refusal (a host that filled its task slots, stopped accepting or fell under the
load threshold in the meantime is judged as it is now) and the one after a wake
(the woken host is counted only if it is still a place the job may go). The new
plan keeps what the attempts proved about the hosts already tried, in the
placement record. The gate is never held through a wake or a launch. The fleet
snapshot is read again for every decision, and `SelHost`, the host host-first
selection named before the gate on reads that did not yet carry other jobs'
counts, does not outrank the scores this decision reads (the account it chose
still limits the hosts). A host the hub left out for a full task cap stays out
of the plan (`task_slots_full`) on both the fleet-state and the direct read. A
pinned `--host` is counted through the gate too. A request bound to a node that
timed out keeps its count (the node may still launch it) until a claim, a
definitive failure or the bridge window. The count is in hub memory, so a hub
restart forgets the jobs decided and not yet claimed until their claims show.

Every spawn decision also takes the hub's latest view: before the engine reads
the hub's fleet state the hub recomputes the hosts' `load` values, and the
fallback read (`panewire place`'s endpoint) skips the 30 s cache. No decision
ranks on a value published for an earlier one.

Stale or missing data never reads as idle:

- **Freshness.** `cpu_busy_1m` exists only while the host sent a sample in the
  last ~3 minutes. A host with only a lingering `cpu_busy_5m` is
  `load_unknown` (−500, and not the local decision).
- **Memory.** With no Prometheus memory reading, the node-reported
  `memory_free_pct` stands in.
- A signal no host of that kind exports (load per core, page-ins) leaves its
  term at 1.
- **Exception: a Prometheus outage.** When the read fails the decision is
  `source=hub-only`: every candidate counts as having known metrics and no
  `load_unknown` penalty applies. The Prometheus terms (`cpu`, `io`, `load`)
  read 1. `mem` still follows the node-reported `memory_free_pct`, and the
  `jobs` term and the claim bridge still apply, so
  hosts can still rank differently. An idle-looking score in that state
  reflects missing data, not a measured idle host; the hub-only fallback is
  unchanged by score v2 and described under Prometheus below.

`power` (default 100, see below) is the host's speed **unloaded**; load comes
only from the live signals. At equal load the stronger host scores higher, and
at equal power the less loaded one does.

### The decision

`decision` follows policy order (`local_machine`, then `spill_targets`) among
usable machines, then the rules below adjust it. They look only at usable
candidates (connected, accepting, above the memory floor, not throttled, under
the job cap, with a free task slot). A host with no terms (an old hub) has an
unknown `free`, never a low one: the floor does not apply to it.

1. **Free floor.** A host whose `free` (the product of the terms, before
   `power` and the bridge) is under `free_floor` (default 0.05) is not chosen
   while another usable host has `free` at or above it. When the policy-order
   winner is under the floor, the decision moves to the first host at or above
   it in policy order, and `reason` is `below_free_floor`. A below-floor host
   never takes the decision from a host at the floor, whatever its score: a
   score under `free * power - bridge` can be the least negative one on a
   saturated host (2026-11-13 04:00:57Z, task 4172: oci at 99.8 % CPU busy
   scored +0.05 because it alone had no claims, while rpi at `free` 0.218 scored
   -17.4).
2. **All below the floor.** If every usable host is under it, no host is a good
   place. `reason` is `all_loaded` and the decision goes to the highest
   `free * power` less the bridge cost of the jobs already decided onto the
   host, which is the score (ties: higher `free * power`, then policy order). The
   stronger of equally saturated hosts is used first, and a burst spreads: the
   bridge term moves with each decided job, while `free` and `power` do not move
   until the claims and the metrics catch up. This replaces the earlier
   "highest `mem` term when the best score is under 15" rule.
3. **Hysteresis.** Among the hosts at or above the floor, the winner (the
   policy-order winner, or the first host at the floor in policy order when the
   floor moved the decision) keeps the decision unless the best other host at
   the floor scores at least 1.25 times as much (for a winner scoring under
   zero, a rival whose score is the winner's divided by 1.25 or better). Then
   `reason` is `challenger` and that host takes it. A rival that is merely ahead does not flip the
   decision, so back-to-back spawns do not bounce between hosts.

**The spawn engine applies the floor before the load bucket.** The floor is an
eligibility rule (decision 231), so the engine's host ranking
(`rankHosts` and the `plan` ordering) puts it first. It looks only at awake
hosts: a sleeper has no metrics and stays behind every awake host through the
bucket order. Awake hosts rank by tier: eligible (usable by the hub's verdict and
at or above the floor) first, then usable hosts under the floor, then the hub's
unusable hosts (throttled, at the job cap, slot-full, not accepting; a
slot-full host the direct hub path left out is carried as a view with no terms,
which reads as above the floor and is still unusable). An unusable host is a
fallback and never outranks a usable one, whatever its `free` or bucket. Within a
tier, bucket, the hub's decision and the score order the hosts as before. When
every usable host is under the floor, the highest score ranks first (as in rule
2, so decided jobs count) and the bucket does not apply. With no awake host under
the floor, or none usable, the ranking is unchanged. A host under the floor is
ranked behind, not removed: it is still tried if every other host refuses the
job. The hub's verdict covers the local host like any other.

The hub marks each candidate `below_free_floor` in `signals`. The flag is
offered only to a caller that asks for it: `GET /v1/placement?signals=1&floor=1`
(`panewire place --signals --floor`, which the spawn engine's host list uses).
A caller that asks for `signals=1` alone, such as the previous release's
`place --signals`, decodes strictly and would reject a field it does not know, so
it gets the signals without the flag. The fleet-state `load` values carry the
flag; their readers do not decode strictly. A hub that predates the flag sends
none, and no host is then under the floor.

A decision the rules left alone has no `reason`. Tagged requests (below) have
no policy-order winner and always take the highest score.

The spawn engine ranks hosts by the same scores, and prefers the host the hub
named within a load bucket, so the margin holds there too. A spawn that does
not go through the hub (`panewire spawn` on a node without a hub decision)
ranks on score alone.

### Constants

All of them default as shown and live under an optional `score` object in
`placement.json`. A bad value refuses the file; a missing one keeps its
default.

```json
{"score": {
  "mem_floor": 0.20, "mem_span": 0.20,
  "pagein_half_mbps": 10, "linux_swap_in_half_mbps": 20,
  "load_knee": 0.75, "load_cap": 4,
  "jobs_half": 8, "bridge_cost": 10,
  "hysteresis": 1.25, "free_floor": 0.05
}}
```

`bridge_cost` is the points one claim costs a power-100 host (0..10000); a host
of power `p` pays `bridge_cost * 100 / p`. `free_floor` (0..1) is the free
fraction under which a host is not chosen while another is at or above it. The
earlier `claim_cost` and `all_loaded_below` still load and are ignored.

**Rollout and rollback.** The policy decoder refuses unknown keys, so an older
binary rejects a `placement.json` that sets `bridge_cost` or `free_floor`
("placement policy is invalid"). Update the config only after the rollback
window of the release that reads these keys; until then leave the defaults
(unset) in place. A config that still sets `claim_cost` or `all_loaded_below`
loads on both sides. The fleet's `[placement.shadow]` keys `free_floor` and
`bridge_cost` in `hosts.toml` are refused the same way by an older binary
(the fleet commands and the spawn app then fail to start): set them only after
every reader of that file has this release and delete them before a rollback
([Fleet config](config.md#placement)).

### Telemetry

Every decision leaves two kinds of Loki fleet event (`tool=panewire`, see
`deploy/alloy/README.md`), so a later tuning can replay it:

- `kind=placement`, one per placement decision, with `request_id` (the hub
  spawn request), `job`, `chosen`, `reason` (`policy_order`, `challenger`,
  `below_free_floor`, `all_loaded`, else the ranking metric) and `candidates`:
  for every host its `score`, the terms `cpu`, `mem`, `io`, `load`, `jobs`,
  their product `free`, its `power`, its
  `bridge` count (claims inside the bridge window) and `outcome`.
- `kind=node_spawn`, for failed and refused spawns as before and now for
  **successful** ones: `outcome=ok`, `reason=spawned`, with `request_id`,
  `job`, `profile` and `duration_ms`; `host` is the executing node.

Neither carries screen text, a brief or argv.

**Live node signals.** A candidate's `signals` object, which `signals=1` and the
hk fleet state `load` value both carry, also holds the node's last heartbeat
readings: `swap_in_per_s` and `swap_out_per_s` (page rates from the deltas of
`pswpin`/`pswpout` or vm_stat `Swapins`/`Swapouts`), `pressure`, `power_source`,
`cpu_busy_short` (the hub's 1-minute CPU busy), `signal_age_s` (seconds since the
heartbeat they came from; a cached answer can be up to 30 seconds older) and
`user_idle_s`. Placement records these and never reads them: admission, ranking
and the score are unchanged. The user-activity value is privacy-bounded. A macOS
node sends `user_idle_s`, the whole seconds since the last keyboard or pointer
input, only when its `[local] report_user_idle` is `true` (default off); it reads
no app names, window titles, input content or process lists, and Linux never
sends it. A node sends `host_activity` and `host_memory.swap_out_per_s` only after
its hub sends `hub.features` with `host_activity`, because an older hub rejects
those names.

`panewire place --explain` prints the terms and the readings behind each score
(columns `FREE CPU MEM IO LOAD JOBS CPU_BUSY MEM_AVAIL PAGEIN_MBPS LOAD1_CORE
POWER BRIDGED`); see `testdata/place_explain.golden`. It asks the hub with
`signals=1`. Without that parameter the hub omits the `signals` object, so a
pre-change CLI that decodes candidates strictly keeps working during a rolling
upgrade.

**Replay.** `placement_replay_test.go` replays v1 and v2 on the 2026-11-02..04
extract (`PLACEMENT_REPLAY_DIR`): of the 16 load-plausible failures on the
busiest host, 14 would have gone elsewhere, and 5.9% of the minutes with load1
per core under 0.5 flip. The extract has no claim history or live job counts,
so `jobs` and the bridge read zero there.

**Memory floor.** `memory_free_pct_min` (default 30) is the only admission
threshold left. It is a crash guard on the node-reported `memory_free_pct`
(macOS `memory_pressure` free percentage, Linux `MemAvailable`), not a load
signal.

### Shadow comparison

hk 3650. The static `power` is not re-tuned by hand
(`decision/2026-11-09/placement-measurement-direction` §1, §3). Instead, each
time the spawn engine has ranked the hosts for a job (spawn selection
ranks in `admitHostFirst`), it ranks the same hosts again under two
alternative rules and writes one fleet event, `kind=placement_shadow`
([observability](observability.md#fleet-events)). The arms are pure functions of
the readings the live ranking already had: no extra call, the App's clock, and
nothing they compute is read by the live path. A dry run records nothing. A
panic in an arm is caught and recorded as `shadow_error`; the spawn goes on.
`placement.shadow.enabled = false` turns the event off.

**Two confirmed defects, recorded here and not fixed live.**

- *A, unit mismatch.* A remote host's `load_ratio` is the hub's five-minute
  CPU-busy fraction; the local host's is load5 divided by cores. Host-first
  compares both with one `placement.load_threshold`, and the load bucket
  outranks the score.
- *B, hub rules dropped.* The hub's hysteresis and all-loaded rules reach the
  spawn engine only through `HostView.Decided`, which only the `/v1/placement`
  parser sets. The fleet-state path (the default, also for hub spawns) never
  sets it, and the hub's `usable` verdict (job cap, not accepting, unknown
  metrics) is ignored.

**Arms.**

| Arm | What it changes against the live order |
| --- | --- |
| `fixed` | Defect A: one load measure for every host, never mixed: the hub's CPU-busy fraction when any candidate has one (`load_measure=cpu_busy`, threshold `shadow.cpu_busy_over`, default `load_threshold`), else load5 per core for every host alike (`load5_per_core`; a host without a reading ranks as unknown). The local `max_load_ratio` guard keeps its own unit. Defect B: a host the hub marks unusable leaves the order, a slot-full host included (the fleet-state host list omits it, so the spawn engine keeps its name for the shadow only); the hub's decision (policy order = configuration order, local first; `shadow.hysteresis`, `shadow.free_floor`, the same rules as the hub's decision above; none for a tagged request) is applied when the hub did not already send one. The score is the hub's. |
| `waste_first` | Not a host order, so it is not a key of `arms`: the account a pool-first order would have spent, recorded beside the host arms (hk 3651, [below](#the-waste_first-account-arm-hk-3651)). It does not enter the event `outcome`. |
| `measured` | `fixed`, plus: one memory signal for admission and rank, the capability map, and each of CPU busy and load per core used once. Memory signal: the pressure level (normal, warn, critical) of a host that declares the `macos` capability when it sent one (the wire does not say which OS a pressure value is from, so any other host's pressure is ignored), else available memory (Linux `MemAvailable`/`MemTotal`; macOS Prometheus available); with the swap-in rate (node `swap_in_per_s`, else the Linux `pswpin` rate). Critical pressure, or available memory under `shadow.mem_refuse_below_pct`, refuses the host; a host the hub dropped only for its own memory floor is judged again by this signal when it is known; a host with no memory reading stays dropped. The score is rebuilt as `mem * swap * load * jobs * capability - bridged * bridge_cost * 100 / capability` (the `cpu` term is dropped: CPU busy already decides the bucket), without the hub's hold and burst bonuses; a host with no score signals keeps the hub's score. |

`capability` is `placement.capability.light.<host>` or
`placement.capability.heavy.<host>`; a host with no entry for the class falls
back to the hub's `power` (100 when the hub sent none). The map is empty by
default. The work class comes from the spawn role in one function,
`workClassOf`: `light` for reviewer, consult and design-type seats,
`heavy` for everything else (implementer). It is a recorded guess, to be
checked by measurement; the role is recorded with it.

**Reading the event.** `live` is the live host order, each host with its bucket
(0 awake under the threshold, 1 awake without a reading, 2 awake over it, 3
asleep) and score. Each arm in `arms` has its own `order`, its `first` host and
an `outcome`:

- `agree`: the same order as live.
- `order_differs`: the same first host, another order behind it. The placement
  would not have moved; the fallback order would have.
- `disagree`: another first host. This is the one that matters: the arm would
  have started the job elsewhere.
- `shadow_error`: the arm failed (`shadow_error` holds the reason); the live
  path was not touched.

The event `outcome` is the worst of the arms. `inputs` has one row per host with
what the arms read: `cpu_busy`, `load5_per_core` (local host only),
`load1_per_core`, `mem_available`, `mem_free_pct`, `pressure`, `swap_in_per_s`,
`jobs`, `power`, `capability` (only when the policy has an entry),
`signal_age_s` and `memory_source` (`pressure`, `available` or `none`). A
missing value is a reading the hub did not send: the fleet-state load value
carries no `pressure`, `memory_free_pct` or node swap rate, only the
Prometheus signals, so on that path a macOS host reads `available`; the
`/v1/placement` path carries the node memory fields but no `signals`. Orders and
inputs are cut at 16 hosts (`truncated`). `arms` is a map: later host-order arms
add a key. The account arm `waste_first` is a separate object beside it.

#### The waste_first account arm (hk 3651)

The live walk ranks hosts first and then takes the best account on the best
host that admits one. An account at waste risk that exists only on a
lower-ranked host is never reached, and waste risk never moves a host's rank.
The `waste_first` object records which account a pool-first order would have
spent instead, next to the live choice. Live placement is unchanged.

- **Candidates.** The pass-1 candidates of the job: every profile, rung and
  account the live run bound to a host. A candidate joins at the first usable
  host whose walk ranks it in pass 1, judged with that host's own credit-pacing
  flag (T2997), not the selection-wide one. They are sorted by the live comparator
  (`sortSelCands`: waste-risk pools first, then band, the headroom fit for a known job length (`plan --expected`, config.md), grade, urgency and the
  rest), the same comparison the live path uses. **Pass 2 is not modelled.**
  Pass 2 is the paced-pool band the live walk tries only when pass 1 chose
  nothing. A live choice that came from pass 2 is recorded with `pass: 2`.
- **Usable hosts.** The ranked hosts the walk could still use. A host the walk
  found unreachable, or that reported no eligible host, is left out.
- **Quota state.** Read from the readings the live run already holds; the arm
  makes no call. The state is read per usable host. `admitted`: the live gate
  passed it on that host. `admissible`: its readings passed and the live run
  did not refuse it there. `refused`: it was refused before admission; or the
  gate refused it on some host (the live walk then skips it on every host); or
  an account-wide exhaustion is cached for its account; or the gate failed on
  that host (a host-scoped failure holds on that host only). `unknown`: no
  reading reached it. The first `admitted` or `admissible` (candidate, host)
  pair in pool-first order is the choice. An `unknown` account is never chosen
  and never assumed admitted; it is listed in `unknown`.
- **Failure.** A panic in the arm, or a failed snapshot, is `shadow_error` with
  the reason; the spawn goes on on the live choice.

`waste_first` carries:

- `outcome`: `agree` (the same account as live), `differs` (another account),
  `no_choice` (no admitted or admissible account; the live choice is still
  recorded), or `shadow_error` (`shadow_error` holds the reason).
- `live` and `waste_first`: a choice each, with `host`, `profile`, `seat`,
  `account_key`, `pool`, `tier`, `pass`, `waste_risk`, `unused_at_reset_pct`
  and `reset_hours`. The last two are present only when the reading has them.
- `left_unspent`, when the outcome is `differs`: the first waste-risk account
  the arm could spend that the live choice did not, with `host`, `pool`,
  `account_key`, `unused_at_reset_pct` and `reset_hours`.
- `unknown`: account keys on a usable host whose quota state the live run never
  observed (at most 16).

The event's `outcome`, `detail` and host arms are the same as before this arm.
The line is now written once admission has run, not before it. Its fields are
unchanged; only its place in the events file moves. `placement.shadow.enabled =
false` turns this arm off with the rest of the event.

Example, from a fixture run (three hosts; `mac-b` ranks first and holds the
ranked-tier account, `cloud-tiny` ranks last and holds the account at waste
risk, 60% unused at an 8 h reset). The live walk spends `mac-b`; the arm would
have spent `cloud-tiny`. The event's own `outcome` is still the host arms'
(`agree`):

```json
"waste_first":{"outcome":"differs","live":{"host":"mac-b","profile":"sonnet","seat":"sonnet","account_key":"claude:00000000000000b2","pool":"claude","tier":"ranked","pass":1,"waste_risk":false},"waste_first":{"host":"cloud-tiny","profile":"sonnet","seat":"sonnet","account_key":"claude:00000000000000a1","pool":"claude","tier":"waste","pass":1,"waste_risk":true,"unused_at_reset_pct":60,"reset_hours":8},"left_unspent":{"host":"cloud-tiny","pool":"claude","account_key":"claude:00000000000000a1","unused_at_reset_pct":60,"reset_hours":8}}
```

One real event (macOS `mac-b` at warn pressure, 60% free, swapping hard; the
hub scores it 100, so live and `fixed` keep it first and `measured` does not):

```json
{"ts":"2026-11-09T00:00:00Z","host":"node","tool":"panewire","kind":"placement_shadow","job":"job-a","outcome":"disagree","reason":"heavy","detail":"work_class=heavy role=implementer fixed=agree measured=disagree","request_id":"req-1","shadow":{"role":"implementer","work_class":"heavy","live":{"first":"mac-b","order":[{"host":"mac-b","bucket":0,"score":100},{"host":"cloud-small","bucket":0,"score":90},{"host":"cloud-tiny","bucket":1,"score":1},{"host":"pi","bucket":1,"score":1}]},"arms":{"fixed":{"outcome":"agree","first":"mac-b","order":[{"host":"mac-b","bucket":0,"score":100},{"host":"cloud-small","bucket":0,"score":90},{"host":"cloud-tiny","bucket":1,"score":1},{"host":"pi","bucket":1,"score":1}]},"measured":{"outcome":"disagree","first":"cloud-small","order":[{"host":"cloud-small","bucket":0,"score":100},{"host":"mac-b","bucket":0,"score":10},{"host":"cloud-tiny","bucket":1,"score":1},{"host":"pi","bucket":1,"score":1}]}},"load_measure":"cpu_busy","inputs":[{"host":"mac-b","cpu_busy":0.2,"load1_per_core":0.1,"mem_available":0.8,"mem_free_pct":60,"pressure":1,"swap_in_per_s":20000,"jobs":0,"power":100,"memory_source":"pressure"},{"host":"cloud-small","cpu_busy":0.1,"load1_per_core":0.1,"mem_available":0.8,"jobs":0,"power":100,"memory_source":"available"},{"host":"cloud-tiny","memory_source":"none"},{"host":"pi","memory_source":"none"}]}}
```

hk 3649 adds `swap_out_per_s` and `user_idle_s` to the node heartbeat. Until it
lands the arms read the swap-in rate only; swap-out and user idle are not used
by any arm (TODO hk 3649).

### Active jobs

The node heartbeat's active-job list feeds the bridge, `active_jobs`, task
slots and `--explain`. A job leaves it when any of these is true:

- its journal records `job.completed`, `job.revoked`, `job.lost`,
  `job.reaped` or `quota_pool.release` (panewire writes the release whenever a
  run ends: done, failed spawn, reap, lost pane);
- herdr no longer lists an agent on its pane;
- herdr has reported its pane `idle` or `done` for 30 minutes (the clock
  restarts when the pane works again, and when the node restarts).

The default is the same local `mac-work` / `desktop` spill shape. Policy files
must be regular files; unknown fields and duplicate targets are rejected. A
changed valid file hot-reloads without changing the last known-good policy.

## Laptop power

`laptops` is optional and lists the machines placement treats as laptops:

```json
{"laptops": ["mac-work"]}
```

A node on macOS reports its power source on the heartbeat, in
`host_signals.power_source`, as `ac` or `battery`. It reads `pmset -g batt`. A
UPS counts as `battery`.

| Host | Report | Result |
|---|---|---|
| Listed | `battery` | Refused: `not_accepting(on_battery)` |
| Listed | `ac` | Eligible |
| Listed | None for more than 3 minutes since the later of its last report and its connect | Refused: `not_accepting(power_unknown)` |
| Not listed | Any, or none | Unaffected |

A refused host is not `usable`. The reason appears in `GET /v1/placement` and in
the `load` value of `GET /v1/state`. panewire refuses on the text `not_accepting`.
A forced spawn (`panewire spawn --host local`, or a named host) does not go through
placement, so it is not refused.

### Rollout

Hub and nodes deploy in either order. A node sends `host_signals` only after its
hub sends `{"type":"hub.features","features":["host_signals"]}`, and it forgets
that on every new connection. An older hub rejects a heartbeat with an unknown
field, so this is what keeps a new node working against it. An older node ignores
the message.

**Set `laptops` last.** An older hub rejects the policy file because of the
unknown key, and a new hub refuses a listed laptop whose node doesn't report yet.

1. Deploy the hub.
2. Deploy the nodes.
3. Confirm the laptop reports: the hub logs a `host power source` line with the
   machine and its source when the node first reports, and on each change.
4. Add `laptops` to the policy file. It hot-reloads.

To add a new signal, add it to `HubHostSignals`. The hub ignores names it doesn't
know inside `host_signals`, and an invalid value loses only that signal.

## Per-machine task slots

`machines` is optional and maps a machine id to `{"task_slots": N}` (1–100). A
machine not listed there is uncapped; a policy file without `machines` behaves
exactly as before.

The hub counts **tasks**, not jobs. A task is the `owner_lane` bundle on that
machine: a builder job (whose claim `owner_lane` is its own lane) plus every
implementer job that builder spawned with `--owner <builder lane>`. Two jobs
of the same task consume one slot. A job whose `owner_lane` is empty or the
legacy `"default"` sentinel cannot be attributed to a task and counts as a
task of its own — unowned jobs therefore over-count rather than slip through.
Counts come from the node heartbeat's active-job list, the same source as
`active_jobs`.

A machine whose task count reaches `task_slots` is removed from `candidates`
entirely — it is never the decision, and is invisible to consumers that walk the candidate list. When
every machine is full the response is explicit: `"decision": null`,
`"candidates": []`, `"reason": "task_slots_exhausted"`. A narrower form —
the decision machine is slot-full but other candidates remain — answers
`"decision": null` with `"reason": "task_slots_full"` and a non-empty list.
Surviving candidates
for capped machines report `tasks` and `task_slots` so the counting is
visible in the response itself; uncapped machines omit both fields so the
wire stays byte-identical for deployments without a `machines` map.

Two counting edge cases are deliberate. Jobs spawned with `--owner
<lane>` where the lane is a coordinator (e.g. `director-N` spawning workers
directly) all share that lane, so they count as one task — the same rule as
builder bundles. And the node heartbeat truncates its active-job list at 32
entries, so `task_slots` values above 32 can never bind; keep configured caps
at or below that.

## Machine power weights

`power` is optional and maps a machine id to a relative speed weight, greater
than 0 and at most 10000. 100 is the strongest host; a machine without an
entry weighs 100, so a policy without `power` ranks machines on measured
load alone. It is a separate top-level map (not part of `machines`) so a
weight never implies a task-slot cap.

```json
{"power": {"host-a": 100, "host-b": 79, "host-c": 8}}
```

**How to measure.** Run the same all-cores CPU benchmark on every host — for
example `openssl speed -multi <cores> rsa2048` and read signatures per second
— then set each weight to `100 * host_rate / strongest_rate`. Use all cores:
the weight is the host's total capacity, not its per-core speed. Re-measure
when hardware changes; a rounded value is fine. Measure with the host
unloaded: load enters through the live signals, so a weight measured on a
busy host would count that load twice.

**Fleet measurements (2026-11-02).** The workload benchmark in
`handoffkeep doc get fleet/bench/2026-11-02-hosts` ran the same Go workload
once per host:

| Machine | Seconds | Conditions |
|---|---|---|
| mac-dev | 37.6 | idle |
| mac-work | 40.6 | under load: 18% memory available (node_exporter), 43% free by `memory_pressure`, paging in about 25 MB/s |
| oci | 87.2 | idle |
| desktop | 96.0 | nearly idle |
| rpi | 308.8 | idle |

mac-dev and mac-work are two machines of the same model (M1
Pro, 10 cores, 32 GB), not one machine; mac-work-default is a second node
daemon on the mac-work machine. Each figure is a single run of one build and
test workload, so it is not a speed for every kind of job. The live `power`
map is the operator's; it is not re-tuned by hand while a measured, per-work
class estimate is compared with it in shadow (hk 3650, decision
`decision/2026-11-09/placement-measurement-direction`). `capacity_weight` is
ignored, so leave it out.

Power multiplies the measured free fraction (`free * power`), so an idle
power-100 host outranks an idle power-40 one, and a claim's 10 points are a
larger share of a weak host's score. Low-power hosts are ranked lower,
never filtered out. The `decision` field
still follows policy order among usable machines, adjusted by the
[decision rules](#the-decision); power shapes `score` and therefore the order
of `candidates`.

## Draft NCP policy file (not deployed)

The production hub (`panewire-hub.service` on NCP) currently runs with no
policy file, so it uses the compiled default (`local_machine` `mac-work`,
which matches no real machine). The operator-approved draft for the real
fleet — 3/3/2 task slots per `decision/2026-09-23/task-slots-per-machine` —
is:

```json
{
  "local_machine": "mac-personal",
  "spill_targets": ["desktop", "mac-dev"],
  "max_active_jobs": 5,
  "memory_free_pct_min": 30,
  "machines": {
    "mac-personal": {"task_slots": 3},
    "mac-dev": {"task_slots": 3},
    "desktop": {"task_slots": 2}
  }
}
```

`local_machine` is the operator's primary mac (`mac-personal`); the
`spill_targets` order follows the new-task preference desktop → mac-dev.

### Startup-option change procedure (requires deploy approval)

`panewire hub` accepts `--placement-policy <path>`; the flag default is
`/etc/panewire/placement.json`. When the flag is not given **and** the default
path does not exist at startup, the hub runs with `PlacementPolicyPath` empty —
in that state dropping a file later does nothing, because the hot-reload loop
only watches a configured path. The procedure is therefore:

0. **Upgrade `panewire` on every host before enabling `machines`.**
   Once any machine is capped, `/v1/placement` emits `tasks`/`task_slots`
   on that machine's candidates, and pre-change CLIs reject the response
   (`DisallowUnknownFields`) — a caller would read that as a rejected hub answer.
   With no `machines` key the wire is unchanged, so writing the file early
   is safe; the caps are what require the fleet upgrade.
1. Write the draft to `/etc/panewire/placement.json` on the hub host
   (`root:panewire`-readable regular file, mode `0644`; it must not be a
   symlink).
2. Restart `panewire-hub.service`. A restart is required only the first time —
   once the path is configured, later edits hot-reload on the placement
   modtime check.
3. Verify: `GET /v1/placement?class=worker` reports `"policy_status":"current"`
   and each capped machine with a free slot appears in `candidates` with its
   `tasks` and `task_slots`. A capped machine at its limit is omitted, so this
   check can't confirm a cap while that machine is full.
4. If the file is rejected at startup the service refuses to boot; fix the
   JSON rather than bypassing validation (`machines` machine ids must match
   the node id pattern and `task_slots` must be 1–100).

`memory_free_pct_min` is the one optional admission threshold. It defaults to
30 percent; an explicit zero remains zero. A candidate is excluded when its
reported free memory is below it. Swap use, swap-in, PSI and macOS pressure
no longer exclude a candidate; swap-in lowers its score. The node still
reports them in its heartbeat (`swap_in_per_s`, `pressure`), and the
candidate still echoes them. Missing telemetry is fail-open.

The hub queries `PANEWIRE_PROM_URL`'s `/api/v1/query` endpoint with the
signal queries in the table above (the Linux swap-in rate is a query of its
own) and the thermal speed-limit query, in parallel; one failed query makes the whole read hub-only. Every query
aggregates by `machine_id`, so remote-write scrape labels such as `instance`
and `job` cannot break vector matching. CPU busy replaced `node_load5` / cores
in v1 because the macOS load average counts blocked threads and can read
several times the core count on a host that is barely busy; v2 reads
`node_load1` per core again, as one term of five, because on the busiest host
that count is what tracked the failures. The `cpu`, `loadavg` and `meminfo`
node-exporter collectors must stay enabled on every host (see
`ALLOY_DISABLE_COLLECTORS` in `deploy/alloy/README.md`); a candidate's
`load_ratio` field is the five-minute busy fraction. Set either `PANEWIRE_PROM_BEARER` or
`PANEWIRE_PROM_BASIC_USER`/`PANEWIRE_PROM_BASIC_PASS` outside the repository.
Prometheus samples must have `machine_id`. A missing load sample is explicitly
`load_unknown`: it cannot select the local machine; if no safe spill candidate
exists the decision is `unavailable`. The operator read (`panewire place`) is
cached for 30 seconds; a spawn's own placement never reads that cache.
If Prometheus is unavailable or malformed, the endpoint always returns a 200
hub-only decision based on connected/accepting state and heartbeat active-job
counts; it never returns a scheduler 500.

```json
{"decision":"mac-work","candidates":[{"machine":"mac-work","score":37,"load_ratio":0.53,"throttled":false,"active_jobs":1,"connected":true,"metrics_known":true,"memory_free_pct":null,"swap_used_mb":null,"memory_known":false,"reason":"memory_unknown"}],"source":"prometheus","asof":"2026-09-04T00:20:00Z"}
```

A node that does not send memory telemetry records `memory_unknown` rather
than `available`; this remains fail-open and does not change its score.

`panewire` invokes the stable CLI contract:

```sh
panewire place --class worker --cwd repo-key \
  --hub-url https://hub.example --hub-token-env /secure/operator.env
panewire place --class worker --explain \
  --hub-url https://hub.example --hub-token-env /secure/operator.env
```

The normal form prints the JSON response and exits zero for both Prometheus and
hub-only decisions. `--explain` prints the selected machine/source followed by
a candidate table. Invalid flags, a non-operator credential, or an unavailable
hub are non-zero.

## Tagged recommendations for movable work

`panewire place --class worker --tags dagu,macos` sends
`GET /v1/placement?class=worker&tags=dagu,macos` with the usual operator credential.
Assign capabilities in the operator-owned placement policy:

```json
"tags": {
  "machine-a": ["dagu", "macos"],
  "machine-b": ["dagu", "linux"]
}
```

These keys must also be in the policy's local/spill candidate set. Tags do not
add a host to that set. A query requires **all** named tags. Names use lowercase
letters/digits plus dot, underscore or hyphen, at most 64 characters each and
16 per request or host. The CLI canonicalizes order and duplicates; malformed
queries and duplicate HTTP `tags` parameters are refused.

Tagged requests choose the highest existing measured-load score among matching,
connected, accepting candidates that pass the same memory, thermal, freshness,
active-job and task-slot guards. They do not fall back to an untagged local host,
an unknown capability, a full host or a disconnected wake target. No matching
capacity returns `decision: null`, an empty candidate list and
`reason: no_tagged_capacity`; the CLI exits nonzero with a repair hint. If Prometheus is unavailable, the existing hub-only presence/score fallback still applies; this is not a fresh load or quota guarantee. The cache
key includes the canonical tag set. Untagged requests retain the existing
local/spill policy order.

This is a recommendation, not a job reservation, quota grant or execution lease.
A movable Dagu step can consume `decision` and pass it to its execution adapter;
Dagu keeps scheduling, retries, concurrency and cancellation. The hub does not
retry a Dagu run or create another scheduler. The separate hk2661 integration
must still configure its private pool tags and connect recommendation to a Dagu
execution identity; this change does not install or alter DAGs.

## Quota service boundary

The absorbed spawn engine now reads the hub's timestamped fleet-state snapshot
in-process and dispatches `quota.refresh` directly to the selected node. Existing
provider adapters and their credentials remain on that node; stale or unknown
quota remains fail-closed. `GET /v1/state` and node refresh already provide the
shared service needed by placement. A second provider-measurement service and a
`panewire quota` command are not added: they would duplicate those interfaces or
centralize credentials without a demonstrated caller need. `pacelever` remains
the provider measurement tool; this decision does not bypass its final gate.

A quota-stop job end cools its pool across hosts until its reset, or for five minutes when no reset is known. Candidates are recorded as `pool_cooling` during that period. Cooling is held in hub memory and is lost on hub restart.

When hub state is unavailable or disabled, the selection record reports `pool_cooling_hub_unavailable` or `pool_cooling_hub_state_off` in `degraded`; cooling cannot be checked.

## Gate refusals

A refusal the quota gate decides from the request, the catalog or policy
alone, never from a quota reading, is not a quota refusal. `placeAuto` asks
the gate once for the chosen rung before it judges any host
(`requestGate.RequestRefusal`, admission_native.go); a refusal ends the spawn
with the gate's reason verbatim and the exit status the gate maps it to, and no
host is judged or contacted. `-m auto` keeps skipping such a rung in selection
and trying the next; a selection such refusals decide fails
`POLICY_UNAVAILABLE` and names the codes (`; gate refused: <code>, …`).

The refusals decided without a reading are the first judgment of
`admissionInputs.judgeWith` (admission_verdict.go) with no reading: the role
check, a subscription, a request shape (`requested_by_requires_operator_request`,
`operator_request_ref_invalid`, `requested_by_invalid`,
`operator_request_not_applicable`, `consult_only`), the catalog resolution
(`resolveRow`: unknown effort, credit effort, retired model identity, unknown
or retired profile, `e6_arm_required`, retired or missing rung) and a pool
excluded by policy. The SWE-2 free rungs of an excluded `devin` pool are not
refused by that class: they pass this judgment and are decided on their free
bucket ([config](config.md)).

A host the gate refuses with such a reason is recorded in the placement
record's host row `reason` as `gate_blocked:<code>`, where `<code>` is the
reason's leading word before the first colon (`gate_blocked:operator_request_not_applicable`),
or the whole reason as a code when it has no colon. `quota_refused` stays for a
refusal the gate derives from a reading: a window past its cutoff, a missing
window, stale or unknown evidence. The placement record has no field for the
gate code besides this reason.
