# Loop ledger (hk 5116, track A1)

One in-memory ledger per hub process records every hub mechanism's liveness:
`ticks`, `last_tick`, `made`, `done`, `errors` (with `silent_errors` for the
previously silent paths) and the oldest pending age. A daily hk document
carries one row per mechanism, and a lane.event notice names a mechanism that
stopped ticking, stopped finishing work, or hit a previously silent error
path. Contract: `ops/panewire-loops/<YYYY-MM-DD>`; machine is `hub` here —
hk 5117 adds per-node rows beside these.

## One counting discipline: flow and scan

Each mechanism is one of two kinds. The progress rule applies to flow
mechanisms only.

- **flow:** items with an identity enter and leave. `made` = an item seen
  for the first time that day (its entry edge, deduped by id within the
  day); `done` = the item's exit transition that day (terminal, sent or
  cleared). A steady population observed on every pass counts once.
- **scan:** a periodic pass with no item lifecycle. Ticks and errors only;
  no progress rule.

Every rule reads the current UTC date's bucket. Each date's bucket starts
empty at midnight; the previous date's row is written from its bucket at
the first tick of the next date, then the bucket is dropped.

## Mechanisms, kind, item, and edges

| Mechanism | Kind | Item (one per) | Entry edge (made) | Exit edge (done) |
|---|---|---|---|---|
| `spawn_coordinator` | flow | pending `spawn_operations` row (request id) | first sighting in the scan or the start-condition check that day | handed to a worker or a node dispatch; or a start-condition release, give-up, refusal or cancellation |
| `consult_coordinator` | flow | active consult (consult id) | first sighting in the reconcile pass that day | answered and notified, timed out, or failed (recoveries, respawns and live-job settles are not exits) |
| `lane_stall_drain_wake` | flow | owed drain line (lane) | the sweep edge into owed (`pending` false → true) | the line sent (`last_sent` moves) or cleared (`pending` true → false) |
| `inbox` | flow | batch (batch id), inbox wake (lane) | batch listed for flush; wake lane due at a window end | batch marked sent; wake line sent |
| `quota_ladder` | flow | quota successor (job), expired ladder stage (request id) | successor or expired stage seen that day | hk record with `replaced_by`; stage activated, discarded or cancelled |
| `pool_triggers` | scan | — | — | — |
| `pool_exhausted_sweep` | flow | job on an exhausted pool (job) | first listing on an exhausted pool that day | job notified or queued |
| `pool_actions` | flow | `pool_actions` row (action id) | first listing in the reconcile pass that day | action reconciled to final |
| `placement_records` | flow | placement snapshot (request id and attempt) | snapshot listed as pending | hk placement record delivered, sealed or quarantined |
| `fleet_status` | scan | — | — | — |
| `pane_sweep` | scan | — | — | — |
| `reviewer_end` | scan | — | — | — |

`last_tick` is the hub clock (`HubServer.now`); no `time.Now()` inline.
Every pass ticks at its top, including passes that find nothing to do.
`oldest_pending` is the oldest pending spawn's age on the spawn scan; other
mechanisms report 0 until they observe backlog.

## Silent error paths

Every error path counts in the current date's bucket. The paths that were
silent before this change also log, rate-limited to one line per failure
streak per date, and count once per streak per date (a latched outage
counts one error per day it keeps failing, no matter how many sweeps it
spans; the next failure after a success counts and logs again):

- `hub_spawn_async.go`: the pending-scan page read, and the ordinary pending read
- `hub_spawn_start_conditions.go`: the waiting-rows read
- `hub_spawn_park.go`: the parked-wake write
- `hub_consults.go`: the active-consults read
- `hub_quota_replace.go`: the successor scan and the spawn-app build
- `hub_escalate_author.go`: the expired-stages scan and the spawn-app build
- `hub_drain_wake.go`: the owed-lines read, and the unreported-host skip
- `hub_pool_triggers.go`: the catalog read (`Debug` became `Warn`)
- `hub_inbox.go`: the `persistDisabled` latch, and the batch listing

## Daily row

At the first tick after each UTC midnight the hub writes the previous
date's row, `ops/panewire-loops/<YYYY-MM-DD>`, from that date's bucket
through the hub's hk document writer (the same path the fleet status
writer uses):

```json
{"v":1,"at":"2026-11-19T00:00:01Z","machine":"hub","rows":[
  {"machine":"hub","mechanism":"spawn_coordinator","interval_s":2,"ticks":1,
   "made":0,"done":0,"errors":0,"silent_errors":0,
   "last_tick":"2026-11-18T00:00:00Z","oldest_pending_s":0}]}
```

One document per date: already written is never rewritten (an existing
document counts as written), and each date's bucket is dropped after its
row is written. A failed write or an unreadable row counts against
`fleet_status` in the ledger and backs off for a minute before the next
attempt, so an hk outage cannot hold the maintenance tick on every pass.

## Notice

The notice goes to `--loops-notice-lane` (default `panewire-design`), one
lane.event line per hit mechanism. It fires when any of these holds on the
current date's bucket:

- `last_tick` is older than three intervals for the mechanism;
- `made` ≥ 20 and `done` = 0, or `done/made` < 10% (flow mechanisms only);
- a previously silent error path counted ≥ 1 today.

Thresholds (`3`, `20`, `10%`) live in `hub_loop_ledger.go` (`loopStaleIntervals`,
`loopMadeFloor`, `loopDoneRatio`). At most one notice per mechanism per day;
several reasons for one mechanism share its one line.
