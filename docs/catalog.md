# Catalog commands

`panewire catalog` owns the bench catalog and Artificial Analysis annotations.
The catalog lives in handoffkeep (`GET`/`PUT /v1/bench/catalog`); no subcommand
runs pacelever.

- `panewire catalog list [--json]` reads the catalog and prints one line per
  rung, then `catalog=<source> stale=<bool>`. `--json` prints the same rows as
  the push payload plus a `source` object with `source`, `stale` and `age_s`.
- `panewire catalog push FILE [--dry-run]` validates a catalog document and
  writes it. `--dry-run` prints the payload without a request. A row in a pool
  panewire does not serve is ignored with `warning: unknown pool [NAME]
  ignored`.
- Grades change only through `panewire catalog push FILE`, which writes full
  catalog rows.
- `panewire catalog aa-sync [--out FILE] [--dry-run]` fetches the free
  Artificial Analysis Data API and annotates the explicit model mappings.
  Annotations are written to `FILE` for review; sync itself never pushes.
  `--out` requires a fresh server catalog.

## Credentials

Every command authenticates as panewire's `svc-panewire` hk client
([config.md](config.md#handoffkeep-service-credential)): `HANDOFFKEEP_URL` and
`HANDOFFKEEP_TOKEN_SVC_PANEWIRE` in the environment win when both are set,
else the mode-0600 `~/.config/panewire/hk-svc.env` (`PANEWIRE_HK_ENV`
overrides the path) supplies them. The hk CLI's `config.env` and
`HANDOFFKEEP_TOKEN` are never read. The optional
`HANDOFFKEEP_CF_ACCESS_CLIENT_ID` and `HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET`
pair adds Cloudflare Access headers; both or neither must be set. The catalog
endpoint must be HTTPS, or HTTP on loopback or a tailnet address for tests.

`pacelever bench push-catalog FILE` becomes `panewire catalog push FILE`.

## Cache

A catalog read reuses a cache entry younger than one hour and otherwise reads
the endpoint. A cache entry is served for up to one day when the endpoint
fails, and is marked `cache-stale` after that. Admission and selection refuse
a stale catalog; `catalog list` prints it with `stale=true`. The cache is
per endpoint under
`$XDG_CACHE_HOME/panewire/bench/<endpoint-id>-catalog.json`, written mode 0600
through the durable file helper.

## Validation

The decoder skips rows without a profile, model, known grade or known gate and
reports each skipped row as `catalog_row_invalid:<profile>@<effort>`. A catalog
with no supported rows remains unreadable. Valid rows are unique by
`profile@effort`; grades must not improve as effort rises for one profile
(unrated and retired rungs are excluded); and only pools panewire serves are
kept. A push additionally
requires a non-empty catalog, `decided_by`, `deviation_ref` and an RFC 3339
`decided_at` with a timezone. The wire payload carries `profile`, `effort`,
`model_id`, `pool`, `grade`, `score`, `gate`, `gate_reason`,
`boundary_version`, `deviation_ref`, `decided_at`, `decided_by`, `retired_at`
and, when present, `benchmark_source` and `benchmark_annotation`; local
placement fields never leave the caller.

`catalog grade set` was removed because its grade-only endpoint can create an
incomplete catalog row. Use `catalog push` with complete rows instead.

## Artificial Analysis

The key comes from `ARTIFICIAL_ANALYSIS_API_KEY` in the environment, or from
the optional mode-0600 `panewire/aa.env` under the user configuration
directory. The endpoint is the free v2 models route, and only the explicit
mapping table is annotated; unmapped or ambiguous rows are reported and left
unchanged. Data is provided by
[Artificial Analysis](https://artificialanalysis.ai/).

The weekly dagu job switches from `pacelever bench aa-sync` to
`panewire catalog aa-sync`, with the same `--out` argument.

## Exit status

`0` success, `2` usage, `5` invalid input or an unavailable/unreachable
catalog, `70` an internal write failure.
