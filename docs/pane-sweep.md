# Task pane sweep

An implementer stays in its pane after READY while it is the PR author and fixes
later review findings. Once a job has ended, though, the node that created its
pane owns the close: the sweep proves the pane is still that job's own, saves
its last screen to handoffkeep, and closes it. A pane that proves nothing is
kept and counted by reason.

## Candidate selection

The local pass considers jobs that are not reaped whose role is `implementer`,
`reviewer` or `consult`. A reviewer or consult is a candidate once its own job
record ended it — a `job.revoked`, `job.completed`, `job.handed_off`, or
`job.lost`, including a quota stop the hub hands to a replacement in a new
pane (watch_quota_stop.go). An implementer is a candidate while it could end:
its bound hk task is read, and the job's own record decides too.

An implementer's task is the claim's task id, or, for a fix author whose claim
names no task, the parent job's task from the node's job record or handoffkeep
record. If no task resolves, a live job stays with `reason=no_task`. A task
ends when its category is `completed` or `canceled`; if no category is
present, `merged`/`done` and `dropped` state names decide. A task read that
fails keeps the pane as `reason=task_read_failed`. A job's own `job.revoked`,
`job.completed` or `job.handed_off` record is an end; a reprompted job is
live again. A lost implementer ends only when its `job.lost` announces a quota
hand-over — the replacement works a new pane; any other loss keeps the pane.

A task-derived end, and a completed author (implementer READY), waits while its
worktree is on an open PR. The sweep reads the worktree HEAD and its branch tip
on origin, then the task's handoffkeep changes. A matching open PR is logged as
`reason=open_pr`; an unreadable worktree, origin or change list is
`reason=pr_state_unknown`. A completed fix author's task is its parent's; if a
named parent cannot resolve to a task, its pane stays with the owner. A task's
change list is read at most once per pass.

A `--keep` pane closes a day after its end (operator decision 268): an
implementer's task-derived end counts, and an open PR or a live task does not
hold it once that end is a day old.

A job that never emitted `job.spawned` — a design session, a desk pane, a
shell — has no recorded pane and is never a candidate.

## Ownership proof

A pane is the job's own only when both hold at close time (operator decision
265):

- the pane id the job's `job.spawned` payload recorded equals a pane id herdr
  lists, and
- the harness session id `job.spawned` recorded
  (`payload.harness_session.id`) equals that row's `agent_session.value`.

The proof runs twice: before the pane's last screen is read, and again under
the node's command lock on a fresh agent listing immediately before
`herdr pane close`. A row with a different session id keeps the pane as
`reason=session_mismatch`; a receipt with no recorded session keeps it as
`reason=session_unrecorded`; no row keeps it as `reason=pane_unlisted` — unless
`herdr pane list` also lost the pane, in which case the job is reaped as
`pane_gone`.

## Timing

The local pass runs every `--task-pane-sweep-interval` (default `5m`). A
candidate closes once its end lies at least an hour back — its terminal
event's time, or the first pass that saw a task-derived end — whatever herdr
shows the pane doing or showing (decision 266). A claim, spawn or reprompt
clears the terminal time with the rest of the terminal state, so a job that
completed once and was then revived (another fix round, or a respawn after a
quota stop) dates its new end from that revival, never from the old
completion. Inside that hour the pane may
still close the old way: after it stayed `idle` or `done` for
`--task-pane-sweep-grace` (default `10m`; `blocked` never counts). A working,
renamed or missing pane resets the grace, as does a daemon restart.

The hub's maintenance ticker additionally orders `pane.sweep {"age_s": N}`
every `--pane-sweep-interval` (default `10m`) with `N` from `--pane-sweep-age`
(default `60m`); the node runs the same unified pass with `N` as the deadline.
Only the hub may issue `pane.sweep`. The ordered and local passes share one
mutex on the node, and the close itself holds the node command lock, so a
close never interleaves with another acting node command.

## Close

Before `herdr pane close` runs, the pane's last screen is read and uploaded
to handoffkeep as `screens/<job>/pane-screen.txt` (the explicit pane.capture
document contract, capped at hk's document bound). A screen hk stores only as
a cut tail keeps its whole copy in `<job dir>/pane-screen.txt`, mode 0600.
A read or upload that fails keeps the pane as `reason=screen_save_failed`; a
close that fails keeps it as `reason=close_failed`. Both retry on the next
pass.

A pane re-proven is closed regardless of its status, composer contents,
screen changes, or a question-like display: the job's end plus the session
proof is the authority for this path. That differs from the explicit
`pane.close` node command, whose `question_job` guard (#563) keeps a pane a
finished reviewer may have left a question on for the general case — the
sweep's proof is stronger than what an arbitrary close request carries, so
the guard does not apply here.

After the close the job's end state is written to its hk record when it never
reported one (`completed` for a completed task, `lost` for a canceled one),
its usage figures are recorded, its quota lease and lane are released,
`job.reaped` is recorded, and the worktree tail runs as below.

## Kept-pane accounting

Every pass writes the node_exporter textfile
`<node state dir>/metrics/panewire_panes.prom` atomically, holding

    panewire_ended_job_panes_open{reason="..."} N

for each reason an ended-job pane past its deadline is still open —
`session_mismatch`, `session_unrecorded`, `pane_unlisted`,
`task_read_failed`, `pr_state_unknown`, `screen_save_failed`,
`close_failed`, `herdr_unreadable`, `job_unreadable`. Reasons
that are a final state in themselves (`session_mismatch`,
`session_unrecorded`) are counted whenever they keep a candidate, not only
past the deadline; `open_pr`, `grace`, `no_task`, `job_changed`, `pane_gone`
and the `pane-<status>` idle reasons are never counted. A pass that judged no
candidate writes an empty file so a stale reason clears. The host label comes
from Alloy's `instance` label; panewire writes none. Each pass also logs one
structured `ended job panes open` line with the counts. The node has no
`/metrics` endpoint; Alloy's textfile collector reads the file.

Alloy's textfile collector reads `*.prom` from `FLEET_TEXTFILE_DIR`, and on
Linux it runs as its own user, which cannot read that 0600 state-dir copy. When
the node daemon's own environment sets `FLEET_TEXTFILE_DIR`, each pass also
publishes the same body to `$FLEET_TEXTFILE_DIR/panewire_panes.prom`, atomically
and at mode 0644; the state-dir copy stays. The directory must already exist
and be writable by the node's user and readable by Alloy; panewire does not
create it. A failed publication never fails the pass and logs one warning per
reason (`dir_missing`, `dir_unwritable`, `write_failed`) per daemon run.
Setting the variable for Alloy does not set it for the daemon; add it to the
daemon's service environment too (deploy/alloy/README.md).

## Worktree

After it closes a job's pane, the sweep removes the worktree the node
prepared for that job, and the job's local branch with it. A merged end
removes only a clean worktree whose HEAD merged; a non-merged end (a canceled
task, a job closed abandoned or superseded) saves the worktree's changes to
`<job dir>/abandoned-worktree.patch` first and removes it whatever its state.
Only a worktree of the clone under the node's own work root is the node's; a
requester's design worktree, a worktree another job still uses, a dirty one
on a merged end, and every remote branch stay.

## Configuration

Daemon flags (see [config.md](config.md#task-pane-sweep)):

| Flag | Default | Meaning |
|---|---|---|
| `--task-pane-sweep-interval` | `5m` | How often the local pass runs. `0` turns it off; the minimum is `10s`. |
| `--task-pane-sweep-grace` | `10m` | How long a candidate pane must have stayed not working before it closes inside the deadline. |
| `--pane-sweep-interval` | `10m` | (hub) How often the hub asks each node to sweep ended jobs. `0` turns it off; otherwise at least `10s`. |
| `--pane-sweep-age` | `60m` | (hub) How long a job must have been ended before the ordered pass may close its pane. At most 365 days. |

The local pass runs only on a hub-connected daemon with `--hub-jobs-root`. It
reads handoffkeep with the node's own credential (`handoffkeep_env` in
`spawn.json`), the same one `job.mark_lost` uses.
