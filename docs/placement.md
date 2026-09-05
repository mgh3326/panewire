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
  "wake_on_spill": false
}
```

The default is the same local `mac-work` / `desktop` spill shape. Policy files
must be regular files; unknown fields and duplicate targets are rejected. A
changed valid file hot-reloads without changing the last known-good policy.

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
{"decision":"desktop","candidates":[{"machine":"mac-work","score":47,"load_ratio":0.53,"throttled":false,"active_jobs":1,"connected":true,"reason":"load_ratio>=0.50"}],"source":"prometheus","asof":"2026-09-04T00:20:00Z"}
```

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
