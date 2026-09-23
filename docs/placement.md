# Fleet placement API

`GET /v1/placement?class=worker&cwd=repo-key` is an operator-token protected,
read-only placement judgement. `class` is `worker` or `verifier`; `cwd` is an
optional opaque repository key used only as request metadata. It does not send
a brief, a path, a token, or a command to a node.

The hub first reads the operator-owned `/etc/panewire/placement.json` (or the
path passed to `panewire hub --placement-policy`) and uses its local machine
first. A local candidate spills only when its recent five-minute load ratio is
at least `load_ratio`, thermal speed limit is below one, or active heartbeat
jobs reach `max_active_jobs`. `spill_targets` are considered in policy order.

```json
{
  "local_machine": "mac-work",
  "spill_targets": ["desktop"],
  "max_active_jobs": 5,
  "load_ratio": 0.5,
  "memory_free_pct_min": 30,
  "swap_used_mb_max": 1536,
  "wake_on_spill": false,
  "machines": {
    "desktop": {"task_slots": 2}
  }
}
```

The default is the same local `mac-work` / `desktop` spill shape. Policy files
must be regular files; unknown fields and duplicate targets are rejected. A
changed valid file hot-reloads without changing the last known-good policy.

## Per-machine task slots

`machines` is optional and maps a machine id to `{"task_slots": N}` (1–100). A
machine not listed there is uncapped; a policy file without `machines` behaves
exactly as before.

The hub counts **tasks**, not jobs. A task is the `owner_lane` bundle on that
machine: a builder job (whose claim `owner_lane` is its own lane) plus every
tester/worker job that builder spawned with `--owner <builder lane>`. Two jobs
of the same task consume one slot. A job whose `owner_lane` is empty or the
legacy `"default"` sentinel cannot be attributed to a task and counts as a
task of its own — unowned jobs therefore over-count rather than slip through.
Counts come from the node heartbeat's active-job list, the same source as
`active_jobs`.

A machine whose task count reaches `task_slots` is removed from `candidates`
entirely — it is never the decision, is skipped by the `wake_on_spill`
shortcut, and is invisible to consumers that walk the candidate list. When
every machine is full the response is explicit: `"decision": null`,
`"candidates": []`, `"reason": "task_slots_exhausted"`. Surviving candidates
for capped machines report `tasks` and `task_slots` so the counting is
visible in the response itself; uncapped machines omit both fields so the
wire stays byte-identical for deployments without a `machines` map.

`GET /v1/placement/slots` answers "which machine has room" without running a
placement decision. It shares `/v1/placement`'s operator-token boundary
(unauthenticated requests get `401`) and returns each machine's used count and
configured cap (`task_slots` is `null` when uncapped):

```json
{"machines":[{"machine":"desktop","tasks_used":1,"task_slots":2},{"machine":"mac-dev","tasks_used":0,"task_slots":3},{"machine":"mac-personal","tasks_used":2,"task_slots":3}],"policy_status":"current","asof":"2026-09-23T09:40:00Z"}
```

The slot list covers the policy's `local_machine` + `spill_targets`, every
`machines` entry, and every machine with a node record.

Two counting edge cases are deliberate. Jobs spawned with `--owner
<lane>` where the lane is a coordinator (e.g. `director-N` spawning workers
directly) all share that lane, so they count as one task — the same rule as
builder bundles. And the node heartbeat truncates its active-job list at 32
entries, so `task_slots` values above 32 can never bind; keep configured caps
at or below that.

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
  "load_ratio": 0.5,
  "memory_free_pct_min": 30,
  "swap_used_mb_max": 1536,
  "wake_on_spill": false,
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

0. **Upgrade `panewire` on every wrk host before enabling `machines`.**
   Once any machine is capped, `/v1/placement` emits `tasks`/`task_slots`
   on that machine's candidates, and pre-change CLIs reject the response
   (`DisallowUnknownFields`) — wrk would read that as a rejected hub answer.
   With no `machines` key the wire is unchanged, so writing the file early
   is safe; the caps are what require the fleet upgrade.
1. Write the draft to `/etc/panewire/placement.json` on the hub host
   (`root:panewire`-readable regular file, mode `0644`; it must not be a
   symlink).
2. Restart `panewire-hub.service`. A restart is required only the first time —
   once the path is configured, later edits hot-reload on the placement
   modtime check.
3. Verify: `GET /v1/placement?class=worker` reports `"policy_status":"current"`
   and `GET /v1/placement/slots` shows `task_slots` 3/3/2.
4. If the file is rejected at startup the service refuses to boot; fix the
   JSON rather than bypassing validation (`machines` machine ids must match
   the node id pattern and `task_slots` must be 1–100).

`memory_free_pct_min` and `swap_used_mb_max` are optional admission thresholds.
They default to 30 percent and 1536 MB respectively; an explicit zero remains
zero. A candidate is excluded when reported free memory is below the first
threshold or reported swap use is above the second. Missing memory telemetry is
fail-open, so older nodes and individual unavailable measurements keep the
existing placement behavior.

The hub queries `PANEWIRE_PROM_URL`'s `/api/v1/query` endpoint with the
five-minute node load ratio and thermal speed-limit PromQL queries. Load is
aggregated by `machine_id` before CPU-count division, so remote-write scrape
labels such as `instance` and `job` cannot break vector matching. Set either `PANEWIRE_PROM_BEARER` or
`PANEWIRE_PROM_BASIC_USER`/`PANEWIRE_PROM_BASIC_PASS` outside the repository.
Prometheus samples must have `machine_id`. A missing load sample is explicitly
`load_unknown`: it cannot select the local machine; if no safe spill candidate
exists the decision is `unavailable`. Results are cached for 30 seconds.
If Prometheus is unavailable or malformed, the endpoint always returns a 200
hub-only decision based on connected/accepting state and heartbeat active-job
counts; it never returns a scheduler 500.

```json
{"decision":"desktop","candidates":[{"machine":"mac-work","score":37,"load_ratio":0.53,"throttled":false,"active_jobs":1,"connected":true,"metrics_known":true,"memory_free_pct":null,"swap_used_mb":null,"memory_known":false,"holds_active":false,"burst_ready":false,"reason":"load_ratio>=0.50,memory_unknown"}],"source":"prometheus","asof":"2026-09-04T00:20:00Z"}
```

A node that does not send memory telemetry records `memory_unknown` rather
than `available`; this remains fail-open and does not change its score.

When `wake_on_spill` is true and the selected spill target is disconnected, the
hub starts the existing R16 on-demand burst request in the background with a
short placement hold. The returned decision remains advisory: callers must
wait for the target's normal authenticated heartbeat before assigning work.

`wrk` invokes the stable CLI contract:

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
