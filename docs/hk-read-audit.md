# hk reads from the hub and nodes: spawn trace steps and polling audit (hk 5585)

Scope: `GET /v1/jobs/{job}`, `GET /v1/quota/accounts/{key}/latest` and
`GET /v1/quota/presence`, read from panewire's hub and nodes. Code as of
`cf0d9ee3` (main). Each row names its call site (`file:line`), what triggers it,
its fan-out, and an estimate of calls per day at today's fleet size: 6 nodes
and about 10 live jobs. Estimates come from reading code, not from a runtime
count. Where the code does not settle a count, the row says so.

## Measured volume (Sentry, sampled span counts, 2 days)

| Endpoint | 2-day count | Per day |
| --- | ---: | ---: |
| `GET /v1/jobs/{job}` | 602,000 | ~301,000 |
| `GET /v1/quota/accounts/{key}/latest` | 272,600 | ~136,300 |
| `GET /v1/quota/presence` | 101,530 | ~50,765 |

These are sampled span counts, not request counts. The sample rate is not
recorded in this audit, so the true request count may be higher.

The timers below explain only a small part of that volume. Job reads from the
pane sweep are at most ~4,300/day, and latest reads from the node quota round
are ~8,600/day per configured pool. Presence has no timer at all. The rest must
come from per-attempt paths: placement admission, and re-attempts of parked
spawns. Attributing it needs a runtime count per call site, which this PR does
not add.

## Periodic reads (timers)

| # | Endpoint | Call site | Trigger and interval | Fan-out | Calls/day, estimate |
| --- | --- | --- | --- | --- | --- |
| 1 | `GET /v1/quota/accounts/{key}/latest` | node quota round: `fleet_state_node.go:82` ticker, `:96` `refreshFleetQuota`, `:202` `RefreshAccounts`, `quota_native.go:304` `flow.Refresh`, `internal/quota/shared/flow.go:264` `c.Latest` | every `--state-quota-interval`, default 60 s (`fleet_state_node.go:25`, `cli.go:380`; floor 10 s, `cli.go:453`) | per configured pool, per node that publishes quota | 1,440 per pool per node: about 8,640/day per pool across 6 nodes. Lease waits add reads (`flow.go:317`, `:342`, `:364`), not counted. |
| 2 | `GET /v1/quota/accounts/{key}/latest` | credit presence publisher: `fleet_state_node.go:91` `Publish` each round, `quota_credit_presence.go:134` `store.Latest` | same round as #1 | per credit pool, per host that holds credit | 1,440 per credit pool per host. Holders not counted. |
| 3 | `GET /v1/jobs/{job}` | node task pane sweep: `node_pane_sweep.go:45` ticker, `internal/fleet/pane_sweep.go:954` (job's own record, when its claim names no parent), `:978` (parent, cached per pass), `:1392` (`markSweptJob`, only when closing a pane) | every `--task-pane-sweep-interval`, default 5 min (`node_pane_sweep.go:18`) | per node sweep, per candidate job; the parent read is shared within one pass | 288 sweeps/day per node. Upper bound with every live job a candidate: 288 × 10 ≈ 2,880/day. |
| 4 | `GET /v1/jobs/{job}` | hub-triggered `pane.sweep`: `hub_pane_sweep.go:34` ticker, the same node sweeper (#3) | every 10 min (`hub_pane_sweep.go:13`) | each known node, then as #3 | 144 sweeps/day. Upper bound: 144 × 10 ≈ 1,440/day, on top of #3. |
| 5 | `GET /v1/jobs/{job}` | job dir TTL: `node_pane_sweep.go:73` ticker, `internal/fleet/job_dir_ttl.go:592`, `:645` | hourly (`jobDirTTLEvery`, `node_pane_sweep.go`) | per node, per job dir past its TTL | 24/day per node × expired dirs. Count not measured here. |
| 6 | `GET /v1/quota/presence` | none on a timer. Node loops publish presence (`PUT`, `quota_credit_presence.go` via `Flow.Announce`) and do not read it. | — | — | 0 from timers. |
| 7 | hub fleet status | `fleet_status.go:102` `runFleetStatus`, ticker `:38` (3 min) | every 3 min | one pass | Not traced here: the file makes no direct hk read, so its reads sit behind helpers I did not follow. |

## Per-attempt reads (no timer; the fan-out is what matters)

| Endpoint | Call site | Trigger | Fan-out | Note |
| --- | --- | --- | --- | --- |
| presence | `placement.go:527` `remoteAccount` → `:531` `Source.Presence`; called at `placement.go:1562`, `:1915`, `plan.go:209`, `spawn.go:1715` | every placement candidate host, per pool, per attempt | candidates × pools reads of the same list | **Proposal 3.** The list is identical for every candidate in one pass; the reads are not shared. |
| presence | `placement.go:602`, `preflight.go:132`, `select.go:802`, `quota.go:574` | per placement/preflight/selection call | one read each | Each reads the list separately within one spawn. |
| latest | `fleet.go:264` `sharedAccount`; `admission_native.go:376` | per candidate account, per attempt | candidates per attempt | Event-driven. |
| job | `placement_record.go:781` in `deliverPlacementFile` | runs from `flushPlacements`, `app.go:335`, once per `panewire` CLI invocation, only while a spool entry needs job metadata | pending spool entries | Event-driven by CLI use. Not a timer; not the 2 s loop. |
| all three | parked spawns re-attempt at their next check (`hub_spawn_park.go:37`, `spawnDefaultBackoff` 30 s, capped at 10 min by `:38`; the coordinator ticks at 2 s, `hub_spawn_async.go:470`) | each re-attempt runs admission, so presence and latest reads per candidate | one parked spawn at 30 s back-off ≈ 2,880 attempts/day | **Proposal 2.** Whether this path dominates the measured volume is unverified. |

## Proposals (not implemented in this PR)

1. **Pane sweep: longer or event-driven** (row #3, #4). A 15 min local interval
   with the hub's 10 min trigger gives 240 sweeps/day, so about 2,400 job reads
   rather than up to 4,320. Event-driven, on job end, drops it to roughly the
   count of ended jobs. Saves most of rows #3 and #4.
2. **Parked spawns: wake on quota, not on a 30 s timer.** The quota-refresh kick
   (`spawnParkQuota`, `hub_spawn_async.go:483`) already wakes parked spawns.
   A longer back-off for quota-parked spawns removes most re-attempts. At 10 min,
   a parked spawn drops from 2,880 attempts/day to 144. This needs a check that
   the kick covers every quota wait reason.
3. **Presence once per placement pass.** `remoteAccount` reads presence per
   candidate host and pool. A memo scoped to one placement pass turns N reads
   into one. Doing it safely means threading the snapshot through
   `placement.go` and `plan.go`, so it is a separate PR.
4. **Node quota round: skip latest while the local reading is fresh.** Row #1
   reads latest every minute, but the shared reading's TTL is 300 s
   (`fleet.go:277`, `ttl := 300 * time.Second`). Skipping reads inside the TTL
   would cut row #1 to about one read per 5 min, roughly 1,728/day per pool
   across 6 nodes. The round's interval also bounds staleness
   (`fleet_state_node.go:70`, comment on `runFleetQuota`), so this needs a decision on the
   staleness bound before it ships.
5. **Dead code:** `fleetQuotaConfirmedAt` (`fleet_state_node.go:279`) has no
   production caller; only `fleet_state_confirmed_test.go` calls it. It saves
   nothing today. Remove it in a cleanup PR, since it would issue a latest read
   if anyone called it.
6. **Measure before cutting per-attempt paths.** A counter per call site (or a
   span attribute on the existing spawn trace) would show which per-attempt
   path makes most of the measured 301k/136k/51k per day. Proposals 2 and 3 are
   the likely ones, but this audit cannot confirm it.

## What the spawn trace can and cannot show (item 1)

- Children now cover node claim (`claim`), pane start (`launch`), brief submit
  (`submit`, new) and landing proof (`landing`, now only the confirmation,
  model proof and task link), after placement (`hub_spawn_trace.go`).
- Child durations are end-aligned to the transaction's end, as before. They
  show how long each step took, not the order they ran.
- A step appears only if the node reports it. A trace whose only child is
  `placement` (1 s) held its time in one of two places: before a node was
  bound (parked or queued, which these spans do not cover), or in a node step
  that never reported because the node did not reply. Proposal: a child from
  the spawn's park start (`park_since_ms` on the park row) to the bind, so a
  parked wait is visible too.
- Dashboards that compare `landing` durations will see them drop for spawns
  served by a node that sends `submit` to a hub that accepts it, because
  `landing` no longer includes the brief submit.

## Wire compatibility

- `submit` is a new key in `stage_ms`. An older hub rejects a result with an
  unknown key (`validSpawnStages`), so a node sends `submit` only to a hub that
  lists `spawn_stage_submit` in `hub.features` (`spawn_stage.go`,
  `hubFeaturesFrame`). For any other hub, `spawnStageMSForHub`
  (`hub_spawn_node.go`) folds submit into landing, which is the old figure.
- The coarse `Stage` never becomes `submit`: `fleet.CoarseSpawnStage` reports
  landing, so the hub's stage rules (`postLaunch`, the unconfirmed-landing check
  in `hub_spawn_async.go`) see the same values as before.
- No deploy order is required between hub and nodes.
