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
runtug's host-first placement — read that score.

```json
{
  "local_machine": "mac-work",
  "spill_targets": ["desktop"],
  "max_active_jobs": 5,
  "memory_free_pct_min": 30,
  "spawn_bridge_seconds": 300,
  "swap_in_half_mbps": 300,
  "capacity_weight": {"mac-work": 1.0, "desktop": 2.0},
  "wake_on_spill": false,
  "machines": {
    "desktop": {"task_slots": 2}
  }
}
```

`load_ratio`, `swap_used_mb_max`, `swap_in_per_s_max`, `psi_some_avg10_max`
and `pressure_level_min` are retired. A policy file that still sets them
loads, and the values are ignored.

## Measured-load score

The score answers "which host is really the most idle", from what the
hosts' exporters send to Prometheus, not from counting jobs:

```text
free  = (1 - max(cpu_busy_1m, cpu_busy_5m))
        * min(1, mem_available / 0.10)
        / (1 + swap_in_mbps / swap_in_half_mbps)        # swap_in_half_mbps default 300
score = (free * power - 10 * bridged_jobs) * capacity_weight
        - 1000 if disconnected, - 200 if not accepting, - 500 if load_unknown,
        - 100 if throttled, + 5 for a placement hold, + 5 if burst-ready
```

| Signal | PromQL (aggregated by `machine_id`) | Why |
|---|---|---|
| `cpu_busy_5m` | `1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))` | Sustained CPU use. Also the candidate's `load_ratio` field. |
| `cpu_busy_1m` | `1 - avg(irate(node_cpu_seconds_total{mode="idle"}[3m]))` | The last scrape interval (60 s): a job that just went heavy shows at once. |
| `mem_available` | Linux `MemAvailable / MemTotal`; macOS `1 - (internal - purgeable + wired + compressed) / total` | Agent sessions mostly cost memory; it fell after 70–100% of spawns. It scales `free` only below 10% available, linearly to 0: above that the benchmark below ran at full speed. |
| `swap_in_mbps` | `rate(node_memory_swapped_in_bytes_total[5m])` (macOS), `rate(node_vmstat_pswpin[5m]) * 4096` (Linux), in MB/s | Memory contention. `swap_in_half_mbps` (default 300) halves `free`. On macOS node_exporter maps this counter to VM page-ins, which include file-backed reads, not only swap; across the fleet it still rises as available memory falls. Linux assumes 4 KiB pages. |

The signals come from a survey of every series the fleet's exporters send to
Grafana Cloud (2026-11-02, 24 h, 7 hosts, 134 spawns). It kept the four
signals above and rejected these:

- `node_load1` / cores: noise on macOS, where it counts blocked threads (one
  host read 36 per core).
- `node_procs_running`, PSI (`node_pressure_*`), `node_disk_io_time_*`:
  Linux only, and the Linux hosts were never contended.
- `node_thermal_cpu_speed_limit_ratio`: one macOS host exports it. It stays
  as the `throttled` hardware guard, not as load.
- Swap used: sticky. A host keeps gigabytes swapped out long after the
  pressure is gone. Swap-out is near zero everywhere.

At +2 minutes after a spawn, `cpu_busy_1m` had risen +0.043 on the desk host
and `cpu_busy_5m` +0.017. `mem_available` fell after 70–100% of spawns.
Across hosts, swap-in followed memory available: 15% available swapped in
42 MB/s, 27% swapped in 8 MB/s, 37% swapped in 2 MB/s.

**Calibration (2026-11-02, one run, n=1).** The desk timed the same Go
workload (runtug 0758a0b: module download, cold build, vet, tests) on three
hosts: mac-dev 37.6 s, mac-work 40.6 s, desktop 96.0 s
(`handoffkeep doc get fleet/bench/2026-11-02-hosts`). mac-work and mac-dev
are the same machine. mac-work ran 8% slower while it paged in ~25 MB/s with
about 18% memory available. That sets the curve: a swap factor of at least
0.926 at 25 MB/s puts the half point at ~310 MB/s, hence 300, and memory at
18% must cost nothing, hence the 10% knee. The first draft used
`min(cpu, memory)` and a 20 MB/s half point. Those scored mac-work at 37
against desktop's 209, the opposite of the measured speed. This is a single
CPU-heavy run. A memory-heavy workload (Unity) may suffer more from paging;
`swap_in_half_mbps` is the knob for tightening it.

Stale or missing data never reads as idle:

- **Freshness.** `cpu_busy_1m` exists only while the host sent a sample in
  the last ~3 minutes. A host with only a lingering `cpu_busy_5m` is
  `load_unknown` (−500, and not the local decision). In the survey this
  never fired on an always-on host; it caught 11 minutes of a sleeping
  laptop.
- **Memory.** With no Prometheus memory reading, the node-reported
  `memory_free_pct` stands in.
- A signal no host of that kind exports (PSI on macOS) is simply not part
  of the score. Every
job is treated alike: a heavy job (a Unity build, a full test suite) moves
the measured signals, and the measurement pushes the next placement away.

`power` (default 100, see below) is the host's speed **unloaded**; load
comes only from the live signals. It makes a strong idle host win: at equal load
the stronger host scores higher, and at equal power the less loaded one
does. `capacity_weight` is the operator's extra multiplier.

**Spawn bridge.** A metric lags a spawn by a few minutes, so for
`spawn_bridge_seconds` (default 300, the CPU window; 1–3600) after a job's
claim the job itself costs 10 points. That keeps the next spawn off the same
host until the metrics cover the first one. After the window the job costs
nothing: its load is in the metrics. The hub learns about a claim at the
node's next heartbeat (every 10 s); two spawns placed before that heartbeat
can still pick the same host.

**Memory floor.** `memory_free_pct_min` (default 30) is the only admission
threshold left. It is a crash guard on the node-reported `memory_free_pct`
(macOS `memory_pressure` free percentage, Linux `MemAvailable`), not a load
signal.

`panewire place --explain` prints every signal behind each score (columns
`FREE CPU_1M CPU_5M MEM_AVAIL SWAP_IN_MBPS POWER WEIGHT BRIDGED`); see
`testdata/place_explain.golden`. It asks the hub with `signals=1`. Without
that parameter the hub omits the `signals` object, so a pre-change CLI that
decodes candidates strictly keeps working during a rolling upgrade.

### Active jobs

The node heartbeat's active-job list feeds the bridge, `active_jobs`, task
slots and `--explain`. A job leaves it when any of these is true:

- its journal records `job.completed`, `job.revoked`, `job.lost`,
  `job.reaped` or `quota_pool.release` (runtug writes the release whenever a
  run ends: done, failed spawn, reap, lost pane);
- herdr no longer lists an agent on its pane;
- herdr has reported its pane `idle` or `done` for 30 minutes (the clock
  restarts when the pane works again, and when the node restarts).

The idle-shutdown probe keeps its own scan without the 30-minute rule, so a
lead waiting on its workers still keeps its host awake.

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
`"candidates": []`, `"reason": "task_slots_exhausted"`. A narrower form —
the decision machine is slot-full but other candidates remain — answers
`"decision": null` with `"reason": "task_slots_full"` and a non-empty list.
Surviving candidates
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

## Machine power weights

`power` is optional and maps a machine id to a relative speed weight, greater
than 0 and at most 10000. 100 is the strongest host; a machine without an
entry weighs 100, so a policy without `power` ranks machines on measured
load times `capacity_weight`. It is a separate top-level map (not part of `machines`) so a
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

**Recommended fleet values (2026-11-02, not deployed).** These come from the
workload benchmark above, and replace `capacity_weight`:

```json
{"power": {"mac-dev": 100, "mac-work": 100, "desktop": 39,
           "mac-personal": 35, "oci": 11, "rpi": 7, "ncp": 9}}
```

mac-dev and desktop are measured (desktop nearly idle; mac-work is the same
machine as mac-dev). mac-personal, oci, rpi and ncp are unmeasured. They keep
their current `capacity_weight` relative to mac-dev's 4.6 (×100 / 4.6), so
their standing does not change until the desk's oci and rpi runs land in the
same hk doc. Remove `capacity_weight` when setting these values; keeping
both would multiply the weights twice.

Power multiplies the measured free fraction (`free * power`), so an idle
power-100 host outranks an idle power-40 one, and a bridged job's 10 points
are a larger share of a weak host's score. Low-power hosts are ranked lower,
never filtered out. The `decision` field
still follows policy order (`local_machine`, then `spill_targets`) among
usable machines; power shapes `score` and therefore the order of `candidates`.

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
   and `GET /v1/placement/slots` shows `task_slots` 3/3/2.
4. If the file is rejected at startup the service refuses to boot; fix the
   JSON rather than bypassing validation (`machines` machine ids must match
   the node id pattern and `task_slots` must be 1–100).

`memory_free_pct_min` is the one optional admission threshold. It defaults to
30 percent; an explicit zero remains zero. A candidate is excluded when its
reported free memory is below it. Swap use, swap-in, PSI and macOS pressure
no longer exclude a candidate; swap-in lowers its score. The node still
reports them in its heartbeat (`swap_in_per_s`, `pressure`), and the
candidate still echoes them. Missing telemetry is fail-open.

`capacity_weight` is an optional map from machine id to a positive relative
capacity weight; it defaults to 1.0 per machine. The score multiplies the
candidate's measured headroom by that weight, so a faster idle machine ranks
higher. It changes ranking only: disconnected, non-accepting, memory-pressured,
or otherwise refused candidates remain unusable regardless of weight.

The hub queries `PANEWIRE_PROM_URL`'s `/api/v1/query` endpoint with the
signal queries in the table above and the thermal speed-limit query, in
parallel; one failed query makes the whole read hub-only. Every query
aggregates by `machine_id`, so remote-write scrape labels such as `instance`
and `job` cannot break vector matching. CPU busy replaces `node_load5` /
cores: the macOS load average counts blocked threads and can read several
times the core count on a host that is barely busy. The `cpu` and `meminfo`
node-exporter collectors must stay enabled on every host (see
`ALLOY_DISABLE_COLLECTORS` in `deploy/alloy/README.md`); a candidate's
`load_ratio` field is the five-minute busy fraction. Set either `PANEWIRE_PROM_BEARER` or
`PANEWIRE_PROM_BASIC_USER`/`PANEWIRE_PROM_BASIC_PASS` outside the repository.
Prometheus samples must have `machine_id`. A missing load sample is explicitly
`load_unknown`: it cannot select the local machine; if no safe spill candidate
exists the decision is `unavailable`. Results are cached for 30 seconds.
If Prometheus is unavailable or malformed, the endpoint always returns a 200
hub-only decision based on connected/accepting state and heartbeat active-job
counts; it never returns a scheduler 500.

```json
{"decision":"mac-work","candidates":[{"machine":"mac-work","score":37,"load_ratio":0.53,"throttled":false,"active_jobs":1,"connected":true,"metrics_known":true,"memory_free_pct":null,"swap_used_mb":null,"memory_known":false,"holds_active":false,"burst_ready":false,"reason":"memory_unknown"}],"source":"prometheus","asof":"2026-09-04T00:20:00Z"}
```

A node that does not send memory telemetry records `memory_unknown` rather
than `available`; this remains fail-open and does not change its score.

When `wake_on_spill` is true and the selected spill target is disconnected, the
hub starts the existing R16 on-demand burst request in the background with a
short placement hold. The returned decision remains advisory: callers must
wait for the target's normal authenticated heartbeat before assigning work.

`runtug` invokes the stable CLI contract:

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
