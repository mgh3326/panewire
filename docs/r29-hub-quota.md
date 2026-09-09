# R29 hub quota cache and placement gate

R29 adds bounded quota observations to node heartbeats, keeps them only in hub
memory, exposes an operator-authenticated read API, and applies operator quota
policy to the existing placement gate. Tokens, credential files, paths, and
scopefuel command output remain on the node.

## Heartbeat field

`quota` is an optional top-level sibling of `host_memory`. A new node always
sends the object. Complete omission means a legacy node; collection failure is
the explicit object `{"status":"unavailable","collected_at":null,"pools":[]}`.

| field | JSON type | `null` meaning | source |
| --- | --- | --- | --- |
| `status` | string | never null | `ok` or `unavailable` |
| `collected_at` | RFC3339 string or null | collection itself was unavailable | scopefuel `generated_at` |
| `pools` | array | never null | normalized provider/window rows |
| `pool` | string | never null | scopefuel `providers[].id` |
| `account_fp` | 8-character lowercase hex string | never null | local credential-location identity hash |
| `window` | string | never null | scopefuel bucket window; `unknown` for an unavailable provider with no buckets |
| `used_pct` | number or null | usage measurement is unknown | scopefuel bucket `used_pct` |
| `reset_at` | RFC3339 string or null | reset time is unknown | scopefuel bucket `resets_at` |
| `source` | string | never null | scopefuel provider source, or `unavailable` |

All nullable fields are serialized. In particular, null `used_pct` is not zero
and is never a healthy observation. A present object is rejected as a whole for
missing or extra fields, wrong JSON types, invalid identifiers, a reset time
that is not RFC3339, duplicate tuple rows, or a non-finite/negative/greater-than-
100 `used_pct`. Rejection does not erase last-good data.

The node invokes the existing cached `scopefuel --json` reader without
`--no-cache`; this is not a model invocation. Provider status other than `ok`,
or an empty bucket list, becomes one explicit unavailable row. No credential
file is opened.

`account_fp` follows scopefuel's provider-specific location-selector identity:
Claude uses `CLAUDE_CONFIG_DIR` then `HOME`, Codex uses `CODEX_HOME` then
`HOME`, Grok uses `GROK_HOME` then `HOME`, and other pools use `HOME`. The
`KEY=value` parts are joined with `|`, SHA-256 is computed locally, and only the
first 8 hex characters leave the node. Scopefuel's display bridge uses 10
characters; R29 intentionally uses sha8.

This fingerprint is a path-derived hint, not proof of account identity. One
real account used from different homes can look different, while different
accounts using the same location shape can look identical. Policy must not
treat it as cryptographic account attestation.

## Hub cache and read API

The cache key is `(machine_id, pool, account_fp, window)`. Each machine keeps a
`latest` observation and an independent `last_good` observation, including the
provider collection time and hub receive time. The node can update only the key
selected by its authenticated WebSocket machine ID; the heartbeat has no
machine field.

`GET /v1/quota` reuses operator Bearer authentication and returns one entry for
each configured node:

| node state | `latest` | `last_good` | meaning |
| --- | --- | --- | --- |
| `unknown` | null | null or prior good | hub restart or rejected observation; never healthy zero |
| `legacy` | null | null or prior good | heartbeat omitted the quota object |
| `unavailable` | explicit unavailable observation | retained if one exists | collection ran but failed |
| `ok` | current observation | same or prior good | accepted collection |

Rows from different machines are never collapsed. Two machines reporting the
same pool and fingerprint remain two rows because they are two observations of
one likely account. Rows with different fingerprints also remain distinct.
There is no fleet sum or average. If a future display needs an account summary,
it must choose the newest `collected_at` for `(pool, account_fp, window)` rather
than add or average percentages.

The cache is process memory only. A hub restart loses latest and last-good
observations and reports configured nodes as `unknown` until their next
heartbeat. No quota file, database row, or R19 cache entry is used. The existing
`GET/POST /v1/quota/{machine}` R19 on-demand opaque-payload API remains separate
with its five-minute TTL.

Compared with R22 memory admission, reset, stale, and partial semantics differ:

- R22 has nullable per-signal measurements and fails open for a partial memory
  observation; it does not retain a last-good snapshot.
- R29 preserves accepted null quota measurements as unknown rows, but an
  invalid object makes latest unknown while retaining last-good.
- `reset_at` is the provider window reset, not cache expiry. Passing that time
  does not rewrite a cached number; freshness remains visible through
  `collected_at` and `received_at` until another heartbeat arrives.
- An explicit unavailable collection is different from a legacy omission and
  from restart-empty state. A stale last-good value is evidence only, never a
  fabricated current healthy value.

## Quota policy

The placement policy accepts two optional arrays:

| field | entry | gate effect |
| --- | --- | --- |
| `quota_exclude` | `{pool, account_fp?, until?}` | matching active entry denies placement |
| `quota_boost` | `{pool, account_fp?, until?}` | matching active entry marks and boosts the candidate score |

Omitted `account_fp` matches the whole pool. A specific fingerprint matches
only when the caller supplies the same fingerprint. Omitted `until` is
indefinite; an RFC3339 `until` equal to or earlier than gate time is expired.
Exclude takes precedence over boost. Existing load, memory, spill, and wake
rules are otherwise unchanged.

`GET /v1/placement` accepts `pool` and optional `account_fp` query parameters.
The pool comes from scopefuel gate output. Panewire has no profile-to-pool map
and must never add one. “Hub first” means the hub owns policy judgement, not
profile mapping.

Quota-aware placement responses contain a `quota` object with the selector,
`allow`/`boost`/`deny`/`unknown` decision, reason, and policy status:

| policy status | meaning |
| --- | --- |
| `default` | no operator policy path is configured |
| `current` | configured policy parsed successfully |
| `stale` | reload failed; the last valid policy remains active |
| `invalid` | a configured path has never yielded a valid policy |

Policy modtime is checked before the 30-second placement cache, and any change
invalidates that cache. Parse/read failure is logged without reflecting file
contents. A stale policy keeps its quota rules. A never-valid configured policy
makes only the quota axis fail closed (`unknown`/no decision); legacy placement
without a pool continues its existing default load/memory/spill behavior.

## wrk adapter contract

wrk always runs local `scopefuel gate` because that output is the canonical
profile/pool mapping and local role gate. Its fixed contract is:

- allow: exit 0, exactly two stdout lines. Line one is
  `profile=<p> pool=<id> used_pct=<n> class=<preserve|spend>` and line two is the
  reason;
- deny: exit 3, with reason and alternatives on stderr;
- measurement unavailable: exit 4.

wrk then supplies the reported pool (and optional sha8) to `panewire place`.
Only DNS/connect/TLS/timeout failure or HTTP 5xx is hub `unavailable`, which may
use the local result. HTTP 401/403 is an authentication error. A 200 explicit
deny remains deny. Malformed JSON or another unexpected HTTP response is
fail-closed, not unavailable.

The composition is deliberately monotone: hub allow plus local exit 3 is deny;
hub allow plus local exit 4 is unknown/deny, never allow. Hub deny plus local
allow is deny. Hub unavailable plus local exit 0 is the one approved fallback;
hub unavailable plus local exit 3 remains deny. Thus a hub response cannot
revive a profile rejected by the local role gate.

## Deployment order

Deploy the hub first. Older hubs reject the new heartbeat field and older hubs
also reject the new placement-policy fields because policy parsing disallows
unknown fields. After the new hub is healthy, roll out nodes, then the wrk
adapter. Simultaneous fleet deployment is unnecessary.

Policy migration is operator-run and keeps rollback available:

1. Record the current live `scopefuel policy list` output outside the repository
   and leave all existing scopefuel policy entries active.
2. Deploy the new hub code with no quota policy entries and verify legacy
   placement is unchanged.
3. Copy each intended pool/account exclude or boost into the operator placement
   policy, then verify the quota-aware placement response is `current` and has
   the expected reason. Do not create a profile map.
4. Roll out nodes and confirm `GET /v1/quota` distinguishes `ok`, legacy, and
   unavailable nodes. Then roll out the wrk adapter and verify the 0/3/4
   composition fixture.
5. Only after that verification, the operator may remove the corresponding
   legacy scopefuel policy entries with the scopefuel policy subcommand.

For rollback, restore the recorded scopefuel policy first, roll back the wrk
adapter, and remove `quota_exclude`/`quota_boost` from the placement file before
rolling the hub back to a version that rejects those fields. Node rollback may
then proceed; its omission is reported as legacy rather than zero.

## Example JSON

Generic identifiers are used deliberately:

```json
{
  "local_machine": "host-a",
  "spill_targets": ["host-b"],
  "max_active_jobs": 5,
  "load_ratio": 0.5,
  "memory_free_pct_min": 30,
  "swap_used_mb_max": 1536,
  "quota_exclude": [
    {"pool": "claude", "account_fp": "a1b2c3d4", "until": "2026-09-10T00:00:00Z"}
  ],
  "quota_boost": [
    {"pool": "codex"}
  ]
}
```

```json
{
  "nodes": [
    {
      "machine_id": "host-a",
      "state": "ok",
      "latest": {
        "status": "ok",
        "collected_at": "2026-09-09T05:58:50Z",
        "pools": [
          {"pool":"claude","account_fp":"a1b2c3d4","window":"5h","used_pct":24,"reset_at":"2026-09-09T10:29:59Z","source":"oauth-usage-api"}
        ],
        "received_at": "2026-09-09T05:59:00Z"
      },
      "last_good": {
        "status": "ok",
        "collected_at": "2026-09-09T05:58:50Z",
        "pools": [
          {"pool":"claude","account_fp":"a1b2c3d4","window":"5h","used_pct":24,"reset_at":"2026-09-09T10:29:59Z","source":"oauth-usage-api"}
        ],
        "received_at": "2026-09-09T05:59:00Z"
      },
      "observed_at": "2026-09-09T05:59:00Z"
    }
  ]
}
```
