# Quota commands

`panewire quota` is the quota entry point. Pacelever is an internal dependency
scheduled for removal; callers should use panewire's interface.

- `panewire quota status [--json]` reads the local host's cached readings.
- `panewire quota refresh POOL` measures the named pool now, with a 60-second bound.
- `panewire quota observe KIND ...` forwards the observation kind and all remaining arguments, including `devin-free-limit` options, and returns the dependency's exit code.

Status and version calls each have a 30-second bound; gate calls have a
30-second bound and catalog calls a 15-second bound. A timeout reports
`<what>: timed out after <duration>`. Warnings from the dependency are omitted.

## Status JSON v1

`status --json` emits an object with `schema` set to
`panewire.quota.status.v1`, `generated_at` (the source timestamp), and a
`hosts` array. Each host contains `host`, `pacelever_version`, and `readings`.
Each reading has `pool`, `account` (empty if unknown), `source`, `age_seconds`
(number or null), `stale`, and `buckets`. Each bucket contains `window`,
`used_pct` (number or null), optional `resets_at`, and `scope` with `kind` and
optional `name`. Age is measured by the source, in seconds. A refresh that
returns a JSON reading emits the same reading shape; text responses pass through.

### Artificial Analysis annotations

`panewire quota aa-sync --out catalog.json` reads the current catalog through
pacelever, fetches the free Data API, and writes a catalog artifact for review
and a later operator-controlled catalog push. `--dry-run` reports the result
without writing the artifact. The catalog must be fresh and server-sourced when
using `--out`. Only explicit model mappings are annotated; unmapped or ambiguous
rows are reported. Annotations are model priors, not grade evidence.

Set `ARTIFICIAL_ANALYSIS_API_KEY` in the environment, or place it in the optional
mode-0600 `panewire/aa.env` under the user configuration directory. Data is
provided by [Artificial Analysis](https://artificialanalysis.ai/); see its
[Data API reference](https://artificialanalysis.ai/data-api/docs/reference).
