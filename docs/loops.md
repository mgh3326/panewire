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

## Ticks and store reads (hk 5302)

`interval_s` is how often a loop wakes, and `ticks` counts those wakes, not
store reads. The spawn coordinator (2 s), the placement-record probe (2 s) and
the consult loop (5 s) keep ticking while idle; each reads its store only on a
kick, at a due time, or at the 60 s backstop (`read_gate.go`,
`hub-spawn-operations.md` "Read gates"). `made` is counted at a read, so a row
is first seen when its kick or due time opens the loop, not at the next tick.

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

### Selection drops group

The document also carries a `selection_drops` group after `rows`: one entry
per drop reason and Devin account, counting the SWE-2 candidates that a
placed selection on the hub dropped that UTC day (decision 427):

```json
"selection_drops":[{"machine":"hub","reason":"pool_at_cap","account":"devin:<16 hex>","dropped":2}]
```

Reasons are `pool_at_cap` (the per-account free-band cap, `[quota].free_band_per_account`,
or `[quota].pool_max_concurrent`), `free_limit` and `pool_cooling`. Only a
placed spawn counts: a dry run, a refusal and a plan query count nothing, and
an admitted launch is never a drop. A direct `panewire spawn` on a node runs
its own selection and counts nothing here. The group does not change the
closed `rows` list above; an unreadable account reads as `unknown`.
Code: `internal/fleet/selection_drops.go`, `hub_loop_ledger.go`
(`selectionDropRowsLocked`).

### Spawn notices group

The document carries a `spawn_notices` group after `selection_drops`: one entry
per owner lane and cause, counting the owner notices the spawn notice rules
recorded instead of sending that UTC day (hk 5360, operator decision 433):

```json
"spawn_notices":[{"machine":"hub","lane":"mergewake-design","cause":"checkout_refused:locked","silenced":2}]
```

A cause is the refusal code after `spawn_failed:` (`checkout_refused:locked`,
`role_escalate`, …), or `spawned`, `lane_unregistered`, `job_completed` or
`delivery_unconfirmed` for the other notices a `--owner-notify` spawn withholds.
A retryable failure that a requester replays by itself is counted here and
never reaches a design lane's inbox, unless its cause is on
`relay.spawn_notice_must_fix`, its third repeat in 30 minutes, or its request
is given up. A requester's own copy is never counted. Code:
`hub_spawn_notice_silence.go`, `hub_loop_ledger.go` (`spawnNoticeRowsLocked`).
The group is in-process like `selection_drops`: a hub restart clears it.

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

## Node mechanisms (hk 5117, track A2)

Each node keeps the same discipline: flow or scan, one bucket per UTC date,
ticks at the top of every pass, silent errors counted. The node sends its
current-day bucket on the heartbeat, as the optional `loops` field
(`node_loops` in `hub.features`): the heartbeat is a closed payload, so an
older hub rejects an unknown field and the node sends the ledger only to a
hub that advertised it. Which carrier was chosen: the heartbeat, because it
already runs on a short cadence and already gates optional fields on hub
features (`ended_panes` is the precedent).

| Mechanism | Kind | Item (one per) | Entry edge (made) | Exit edge (done) |
|---|---|---|---|---|
| `task_pane_sweep` | scan | — | — | — |
| `worktree_flow` | flow | worktree of an ended job (job id) | worktreePlan clears it for removal; the daily revisit attempts its removal | guarded removal takes it |
| `quota_measure` | scan | — | — | — |
| `job_dir_ttl` | scan | — | — | — |
| `quota_pool_reconcile` | flow | stale quota record or orphan job (job) | first listing that day | orphan released |
| `lane_reconcile` | scan | — | — | — |
| `spawn_settle` | flow | executing spawn row (request id) | first listing that day | row settled |
| `credit_rearm` | scan | — | — | — |
| `hub_close_settle` | flow | job with an open hub-close intent (job) | pass lists it | close settled |

`last_tick` is the node clock (`HubClient.nowUTC()`); no `time.Now()` inline.
Every pass ticks at its top, including passes that find nothing to do.
`spawn_settle` runs at node start and while rows remain, so it carries no
interval and the stale rule skips it; `quota_measure` carries an interval
only while `--state-publish` sets pools. The node clock skews against the
hub's; the hub files each report under the date the report names.

### Silent error paths (node)

Every error path counts in the current date's bucket. The paths that were
silent before this change also log, rate-limited to one line per failure
streak per date, and count once per streak per date:

- `hub_lane_reconcile.go`: the swallowed lane-list read (`lanes` stays nil);
- `hub_spawn_settle.go`: the silent spawn-journal acquire failure;
- `node_pane_close_op.go`: the silent pending-close listing read.

### Merge into the daily row

The hub files each node's bucket under the report's date and merges it into
the same document, `ops/panewire-loops/<YYYY-MM-DD>`: hub rows first, then
one row set per machine in machine-id order. A node that was disconnected
for the whole day writes no row. One document per date still holds when node
rows arrive late in the day: reports buffer in memory and merge at the
midnight write, and an already written document is never rewritten.
A node restart keeps the day's ticks, errors and silent errors across process
epochs; made, done and oldest_pending restart with the process, as the hub's
own ledger does on a hub restart.

An older node sends heartbeats with no `loops` field. Its row reads
`mechanism: "unknown"`, with no ticks and no counts — never zeros, which
would clear a live machine to idle — and the hub raises no notice for it.

### Notice (per machine)

The hub applies the same three rules to every machine's rows, through the
same `--loops-notice-lane`, one line per machine and mechanism:
`loop <mechanism>: <reasons> (machine <id>, day <date>)`. A fresh node gets
the same grace a fresh hub gets: a never-ticked loop is stale only once the
node has been seen longer than three intervals. A known machine with no
heartbeat at all on a closed date gets one `no_report` line when that date's
row is written, and no row.

### Daily kept-worktree revisit

Once a day the node runs the `node sweep-checkouts` logic itself (hk 5091)
over the worktrees the sweep tail kept, through the same guarded removal —
including the in-use guard — and never touches a worktree in use. The revisit
holds `nodeCommandMu`, so its git mutations never interleave with a pane
close. A checkout kept for a live user leaves on its own and never enters the
flow; every other judged checkout does, out when the guarded removal takes
it. The manual command stays.
