# Fleet status snapshot

The hub writes one handoffkeep document, `fleet/status/latest`, every few
minutes with the data the console fleet-status page reads (hk 4148). The page
(handoffkeep hk 4149) is the only intended reader. The snapshot is read-only:
it changes nothing, measures no account and dispatches no work.

`--fleet-status-interval` sets the period (default 3 minutes; `0` turns the
snapshot off). The hub writes once at startup and then on every tick. A failed
write is logged once and the next tick tries again; there is no tighter retry
loop, so an hk outage costs one log line per cycle.

## Document

The body is one JSON object, bounded to 64 KiB. Lists are capped first (64
hosts, 64 quota rows, 8 windows per row); if the body still exceeds the byte
cap, trailing quota rows are dropped, then trailing hosts, then trailing plan
rows, until it fits. Entries sort so two snapshots of the same state are byte
identical.

```json
{
  "v": 1,
  "at": "2026-11-15T12:00:00Z",
  "hosts": [
    {"host":"node-a","connected":true,"version":"panewire-r13","accepting":true,"lanes_used":2,"lanes_cap":64}
  ],
  "quota": [
    {"host":"node-a","pool":"claude","account":"claude:0123456789abcdef",
     "windows":[{"window":"5h","used_pct":41,"resets_at":"2026-11-15T15:00:00Z"}],
     "measured_at":"2026-11-15T11:58:00Z","age_s":120,"fresh_ttl_s":900,
     "fresh":true}
  ],
  "plan": [
    {"grade":"C","t":"T1","seat":"omp-dsflash41","host":"node-a","pool":"commandcode",
     "account":"commandcode:747042b3498054b9","tier":"ranked","outcome":"ADMITTED"}
  ]
}
```

| Section | Fields |
|---|---|
| `hosts` | one row per known host: a node record (a retained disconnected record included) and any machine that owns a lane row. `connected` is the hub's live presence, `version` the node's hello version, `accepting` the effective acceptance, and `lanes_used`/`lanes_cap` the lane rows the host owns against `--lanes-node-row-cap`. |
| `quota` | one row per `(host, account)` reading the hub holds: `pool`, `account` (the shared cache's `<pool>:<16-hex>` fingerprint), `windows` (`window`, `used_pct`, `resets_at`), the reading's `measured_at`, its `age_s`, the applied `fresh_ttl_s` and `fresh`, and `free_state` (that pool's free tier after `shared.NormalizeFree`, when the reading has one). Rows are measured facts, not a selection band. |
| `plan` | one row per grade, `C`, `B`, `A`, `A+`, `S`, weakest first. `t` names the task scale the plan assumed (`T1`; grade and scale are separate inputs). `outcome` is the selection code (`ADMITTED`, `NO_ELIGIBLE_HOST`, `QUOTA_UNKNOWN`, …); a row without an admitted seat may omit `seat`, `host`, `pool`, `account` and `tier`. |

`at` is when the hub assembled the document. `measured_at` is when the quota
reading was taken; `age_s` is measured from the later of its measurement and
the shared store's re-confirmation, so a republished reading does not read as
stale merely because its value did not change.

## Freshness

`fresh` applies the shared quota cache's rule: the reading is fresh when its
age is at least −1 minute and at most `fresh_ttl_s`. The hub takes
`fresh_ttl_s` from its own fleet config (`quota.pool_fresh_ttl_s[pool]`, else
`quota.fresh_ttl_s`), the same window a spawn gate reads, so the label cannot
disagree with placement; when no config or fleet App is available it uses the
defaults (`claude` 900 s, every other pool 300 s). `fresh_ttl_s` travels with
the row so the page says how stale is stale. A reading that fails the rule is
still published, with `fresh: false`.

## Free state and the plan's band

A quota row carries no `band`. One stored reading has no single band: the same
devin account is `free` for its SWE-2 rungs and `ranked`, `waste` or refused
for its paid rungs at once, because selection gives a tier to a catalog rung,
not to a reading. Computing a per-reading band outside the selector is what
kept producing findings (class, group reduction, gate order, validity,
model-scoped free, `NormalizeFree`, `[local].pool_policy`), so the field is
cut.

`free_state` is the one band input a reading owns: `shared.NormalizeFree`
over the reading's `free_bucket`, so an exhausted bucket whose reset has
passed shows `available` and a malformed one `unknown`. Placement's
`free_limit` refusal applies to the same normalized state.

The selection-true band is the plan: each row's `tier`, `pool`, `account`,
`seat` and `outcome` come from the real selector (`selectFor`) over the hub's
own fleet App, its `[local].pool_policy`, admission classes, `NormalizeFree`,
publisher liveness and clock. A console that wants a band reads the plan row
for the grade it cares about, and the quota rows as its evidence. A
per-account band for every rung is a follow-up inside `fleet`, not a second
implementation here.

## Plan

The plan runs the selection core (`internal/fleet`, the code behind
`panewire plan`) for each grade with role `implementer`, scale `T1`, the hub's
own fleet App and the hub's placement view. It is a dry run: no account is
measured (a reading that would need measuring keeps its stored value and the
row may report `QUOTA_UNKNOWN` or a refusal), no host or slot is reserved, no
load is refreshed or republished, and the plan transport refuses both dispatch
and refresh. When the hub cannot build its fleet App, every grade still gets a
row with `outcome` `POLICY_UNAVAILABLE` so the document's shape does not
change.

## No secrets

The document carries no credential, token, brief text, pane content or account
email. An account appears only as the shared quota cache's fingerprint key,
the same short id `panewire quota status` prints. The tests pin the document's
keys to a closed list and refuse an email-shaped value.
