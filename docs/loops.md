# Loop ledger (hk 5116, track A1)

One in-memory ledger per hub process records every hub mechanism's liveness:
`ticks`, `last_tick`, `made`, `done`, `errors` (with `silent_errors` for the
previously silent paths) and the oldest pending age. A daily hk document
carries one row per mechanism, and a lane.event notice names a mechanism that
stopped ticking, stopped finishing work, or hit a previously silent error
path. Contract: `ops/panewire-loops/<YYYY-MM-DD>`; machine is `hub` here —
hk 5117 adds per-node rows beside these.

## Mechanisms, made and done

| Mechanism | Interval | Made: items picked up | Done: items finished |
|---|---|---|---|
| `spawn_coordinator` | 2 s | pending `spawn_operations` rows seen in the scan (plus waiting rows released by the start-condition check) | rows handed to a worker or a node dispatch (a start-condition release, give-up, refusal or cancellation also counts) |
| `consult_coordinator` | 5 s | active consult rows read | consults answered, timed out, failed, respawned or settled |
| `lane_stall_drain_wake` | keepalive (10 s) | stall candidates observed, plus drain sweeps where a lane owed a line | stall alarms activated plus drain lines queued to a design lane's pane |
| `inbox` | 100 ms | batches listed for flush, plus inbox-wake lanes due at a window end | batches handed to the relay (marked sent), plus wake lines sent |
| `quota_ladder` | 2 m | quota successor rows plus expired ladder stages seen | hk records written with `replaced_by`, plus ladder stages activated, discarded or cancelled |
| `pool_triggers_exhausted` | 10 m | live pools checked | exhausted-pool sweeps started (retired or credit-exhausted) |
| `pool_actions` | 10 s | active `pool_actions` rows read | actions reconciled to final |
| `placement_records` | 2 s | placement snapshots pending | hk placement records delivered (spool flush or `deliverPlacementRecord`, sealed or quarantined) |
| `fleet_status` | 3 m (`--fleet-status-interval`) | snapshot ticks | snapshots written to hk |
| `pane_sweep` | 10 m (`--pane-sweep-interval`) | nodes swept | panes a node closed |
| `reviewer_end` | 5 m | succeeded reviewer operations seen in the window | reviewer jobs ended (`mark_lost` written) |

`last_tick` is the hub clock (`HubServer.now`); no `time.Now()` inline.
`oldest_pending` is the oldest pending spawn's age on the spawn scan; other
mechanisms report 0 until they observe backlog.

## Silent error paths

Every error path counts in the ledger. The paths that were silent before this
change also log, rate-limited to one line per failure streak (the next failure
after a success logs again):

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

At each UTC midnight the hub writes `ops/panewire-loops/<YYYY-MM-DD>` through
the hub's hk document writer (the same path the fleet status writer uses):

```json
{"v":1,"at":"2026-11-18T00:00:00Z","machine":"hub","rows":[
  {"machine":"hub","mechanism":"spawn_coordinator","interval_s":2,"ticks":1,
   "made":0,"done":0,"errors":0,"silent_errors":0,
   "last_tick":"2026-11-18T00:00:00Z","oldest_pending_s":0}]}
```

One document per day: the writer records the date it wrote, skips a date it
already wrote, and treats an existing document (for example after a hub
restart) as written — already written is never rewritten. A failed write
counts against `fleet_status` in the ledger and is retried on the next sweep.

## Notice

The notice goes to `--loops-notice-lane` (default `panewire-design`), one
lane.event line per hit mechanism. It fires when any of these holds:

- `last_tick` is older than three intervals for the mechanism;
- `made` ≥ 20 and `done` = 0, or `done/made` < 10%;
- a previously silent error path counted ≥ 1.

Thresholds (`3`, `20`, `10%`) live in `hub_loop_ledger.go` (`loopStaleIntervals`,
`loopMadeFloor`, `loopDoneRatio`). At most one notice per mechanism per day;
several reasons for one mechanism share its one line.
