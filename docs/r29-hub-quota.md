# R29 hub quota and placement gate

R29 originally added bounded quota observations to node heartbeats, kept them
in hub memory, and exposed an operator read API. **Heartbeat quota collection
is retired** (hk task 2300): the ~10s polling of `scopefuel --json` on every
node hammered a shared account's usage API. Quota observations now come from
the quota v2 store (`/v2/quota/*`, see the quota-v2 contract) populated by the
handoffkeep shared quota store, not from heartbeats. The operator placement
quota policy below is unchanged.

## Heartbeat field

Removed. New nodes send no `quota` field. For rolling-deploy compatibility the
hub decoder still admits the field name but drops its value unread, so an
older node that still sends it is never rejected; an older hub treats a new
node's omission as a legacy node.

The retired shape is recorded here only so old wire captures still parse:

| field | JSON type | `null` meaning | source |
| --- | --- | --- | --- |
| `status` | string | never null | `ok` or `unavailable` |
| `collected_at` | RFC3339 string or null | collection itself was unavailable | scopefuel `generated_at` |
| `pools` | array | never null | normalized provider/window rows |
| `pool` | string | never null | scopefuel `providers[].id` |
| `account_fp` | 8-character lowercase hex string | never null | local credential-location identity hash |
| `window` | string | never null | scopefuel bucket window |
| `used_pct` | number or null | usage measurement is unknown | scopefuel bucket `used_pct` |
| `reset_at` | RFC3339 string or null | reset time is unknown | scopefuel bucket `resets_at` |
| `source` | string | never null | scopefuel provider source, or `unavailable` |

`account_fp` was a path-derived hint, not proof of account identity: one real
account used from different homes could look different, while different
accounts using the same location shape could look identical. Policy must not
treat it as cryptographic account attestation.

## Hub cache and read API

Removed. `GET /v1/quota` and `GET/POST /v1/quota/{machine}` are gone; the
node-side `quota.request` handler answers `unsupported` so a legacy hub's
request completes instead of timing out. The `latest`/`last_good` states
(`unknown`, `legacy`, `unavailable`, `ok`) described per-node heartbeat
observations and no longer exist.

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
and must never add one. "Hub first" means the hub owns policy judgement, not
profile mapping.

Every placement response contains top-level `policy_status`, including requests
without a pool selector. Quota-aware responses additionally contain a `quota`
object with the selector, `allow`/`boost`/`deny`/`unknown` decision, reason, and
the same policy status for quota-aware clients:

| policy status | meaning |
| --- | --- |
| `default` | no operator policy path is configured |
| `current` | configured policy parsed successfully |
| `stale` | reload failed; the last valid policy remains active |

At startup, a configured policy path must yield a valid policy. An unreadable or
unparseable startup policy makes `NewHubServer` return an error, and the hub CLI
reports the invalid-condition exit instead of starting with hard-coded defaults.
After a valid startup, policy modtime is checked before the 30-second placement
cache, and any change invalidates that cache. A runtime read or parse failure is
logged without reflecting file contents, reports `stale`, and keeps the last
valid policy and its quota rules. Thus a running hub cannot have a configured
policy that has never been valid, while existing load, memory, spill, and wake
decisions remain unchanged.

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

Deploy the hub first: a new hub accepts heartbeats from older nodes that still
send `quota` (the value is dropped unread), and it no longer issues
`quota.request`. Then roll out nodes; a new node answers a legacy hub's
`quota.request` with `unsupported`, and an older hub treats its quota-less
heartbeat as a legacy node. Rollback is the same order in reverse; no data
migration exists because the retired cache was process memory only.

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
