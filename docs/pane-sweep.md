# Task pane sweep

An implementer stays in its pane after READY while it is the PR author and fixes
later review findings. Once a job has ended, though, the node that created its
pane owns the close: the sweep proves the pane is still that job's own, saves
its last screen to handoffkeep, and closes it. A pane that proves nothing is
kept and counted by reason.

## Reviewer and consult ends

Before it judges any pane, the pass ends a reviewer or consult job that never
ran `panewire done` (hk 4747). Who owns a posted result differs by role: only a
reviewer has a second ender, the hub's posted-result pass
(docs/hub-spawn-operations.md#ending-a-reviewer-job-that-posted-a-result). A
consult's answer is its `report.md`, no pass completes it, and its consumer
reads a lost record as a failed consult, so cleanup never stands in for
completion.

| Role | Result evidence | Pane proven gone | Pane present |
|---|---|---|---|
| reviewer | posted | keep: the hub's end | keep: the hub's end |
| reviewer | absent | `pane_gone`, reaped | timeout at the limit plus 15 min |
| reviewer | unjudgeable | `pane_gone`, reaped | keep; retry the read |
| consult | posted | `pane_gone`, reaped | keep: no timeout |
| consult | absent | `pane_gone`, reaped | timeout at the limit plus 15 min |
| consult | unjudgeable | `pane_gone`, reaped | keep; retry the read |

A **posted** result is a findings object — an object whose `findings` is an
array, valid UTF-8, at most the hub reader's 24 KiB (`node_job_result.go`) — or
a regular `report.md`. A file that is present but cannot be inspected (a
non-regular name, a read error) is **unjudgeable**: it blocks only the quiet
timeout, because the pass never claims "no result" it cannot prove, while a pane
both herdr lists lost is proof enough to clean the job up. Both artifacts are
inspected, and a posted one wins over an unjudgeable one.

A pane is proven gone only when neither `herdr agent list` nor `herdr pane list`
carries its id. The node writes the job's final `job.lost` and its terminal hk
record (`lost`, with `end_reason` `pane_gone` or `reviewer_no_result_timeout`)
through its own JobStore, then reaps a `pane_gone` job at once; a timeout leaves
the pane to the ended-job close an hour later. The limit is the request's
`expires_at`: the spawn record carries it, written from the hub's request expiry
when the hub sent one, else the role's default from the spawn. Only a record
from before that field existed falls back to the role's default after the
spawn. The timeout waits a 15-minute grace past that limit.

A `--keep` reviewer or consult is ended the same way: its live pane keeps its
day (Timing), and a gone pane is reaped at once.

The node never ends an implementer here, a job whose pane asks an unanswered
question (the #563 guard), or a job whose attempt announced a quota stop the hub
hands to a replacement (watch_quota_stop.go).

The proof and both writes run under the job's operation lock, the one
`panewire done` and the quota-stop record take, so a completion cannot land
between them; the result files are re-read there, so a result that lands while
the pass waits for the lock cancels a reviewer's end. The hk record is written
first: the pane sweep never sees a `job.lost` hk does not hold, and a committed
hk end whose ledger event failed is repaired with hk's own reason. Each end logs
once with the job and the reason.

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

## Report republish

The local pass republishes reports an ended job announced but never put in
handoffkeep: a `done` whose upload failed still writes `job.completed` with the
report's `report_sha256`, so the completion exists while hk holds no document
for it. Before the panes are judged, the pass PUTs exactly the announced bytes
under `done`'s content-addressed key, writes the `job.report_published`
receipt, and pins the terminal record (`report_doc`, `report_sha256`) when the
report is the completing one, as step 3 of `done` does.

A report whose file no longer hashes to the announced sha256 is left alone with
`reason=report_changed`: it is still being written, or it belongs to a later
round that only a new `done` announces, and the pass never pins bytes no
completion named. A file that is gone is left with `reason=report_missing`. At
most five uploads run per pass, a failed upload is retried by the next pass,
and each republished report or refusal is logged once. The ordered
`pane.sweep` command does not republish; only the node's own local pass does.

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
