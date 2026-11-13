# Restarting a node daemon without losing work

Applies to `panewire daemon` on a fleet node. Hub restarts are a different
runbook (`docs/hub-postgres.md`). Part of the uninterrupted-deploys work
(hk 4361, slice R3).

## What a stop does

`SIGTERM` (systemd stop, `launchctl kickstart -k`) calls `Daemon.Stop`, which:

1. **Drains.** It refuses new `job.spawn` messages (the hub replays them to the
   restarted node; no journal row is claimed) and waits, up to 30 seconds
   (`defaultStopDrain`), for the work already in flight: a spawn launch with its
   brief and result upload, a relay injection into a pane, a node command such
   as `job.done` that uploads a result, and the queued events for the hub.
2. **Cancels** the node's loops and closes the store, as before.

An idle node stops in microseconds. Work that outlasts the bound is not lost:
see the next section.

## What a start does

- **Settles `executing` spawn journal rows** (`hub_spawn_settle.go`). A row is
  `executing` while a node process runs the spawn; a restart leaves it behind.
  The new process reads the job's local `job.spawned` record and asks herdr
  for both the agent list and the pane list:
  - the recorded pane is an agent of the spawn's name: the spawn **landed**; the
    row completes with a success result and the job's receipt (including
    `lane_unregistered`), and the hub's replay is answered at once;
  - the recorded pane is in neither list, or its id now names another agent:
    the row completes **lost** (`error: "lost"`, tail `lost: pane_gone`);
  - no record and no agent or pane bearing the spawn's label: `lost:
    no_pane_launched`. Neither lost answer is retryable;
  - anything unproven stays `executing`, the replay answers `unknown`, and the
    hub's own pane read and orphan rule decide: a pane only in the pane list, a
    label match without a record (labels are not job-unique), herdr or the pane
    list unreadable. The pass repeats every 30 seconds.
- **Re-arms completion watchers** for live jobs, as before.

## The watcher must survive the stop

`panewire _watch JOB` records a job's result. It already starts in its own
session (`Setsid`), which protects it from a signal to the daemon's process
group. The service manager must not kill it either:

| Host | Setting | File |
|---|---|---|
| Linux, systemd | `KillMode=process` (`mixed` is not enough: it SIGKILLs the cgroup's leftovers after the main process exits) | `deploy/panewired.service` |
| macOS, launchd | `AbandonProcessGroup` true | `deploy/dev.panewire.panewired.plist` |

`TimeoutStopSec` / `ExitTimeOut` are 60 seconds, above the 30-second drain.
Install the file, `systemctl daemon-reload` (Linux) or reload the plist
(macOS), and restart once: the first restart after a unit change still runs
under the old settings.

## Rollout wait, before and after

Before: a rollout waited per node for executing spawns (up to 11 minutes) and
for live reviewer jobs (up to 15 minutes since v0.0.295), because a restart
killed the watchers and orphaned the journal rows.

After: a restart needs no wait for live work. Measured on the test harness
(`TestStopDrain*`, `go test -v`): an idle stop is under 10 µs; a stop with a
two-second upload in flight takes two seconds and the upload completes; the
bound is 30 seconds. The rollout step that waits for live jobs can be removed
once the unit or plist above is installed on the host.

What remains outside this slice: hub-side behaviour during its own restart
(R2, hk 4363), callers retrying across it (R1, hk 4362), restarting only the
component that changed (R0, hk 4365), and the rollout tool itself, which is not
in this repository. A spawn cut off past the 30-second bound ends `landed` or
`lost` as above, not delayed to the request deadline.
