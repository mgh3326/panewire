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
worktree is on an open PR. The sweep reads the task's handoffkeep changes: if
the task has no open PR (an empty change list or all PRs merged), nothing waits
and the close goes ahead without inspecting git (hk 4962). When an open PR exists,
the sweep reads the worktree HEAD and its branch tip on origin; a matching open PR is
logged as `reason=open_pr`. An unreadable change list, or an unreadable
worktree or origin while an open PR exists, is `reason=pr_state_unknown`. A completed
fix author's task is its parent's; if a named parent cannot resolve to a task,
its pane stays with the owner. A task's change list is read at most once per pass.

A `--keep` pane closes a day after its end (operator decision 268): an
implementer's task-derived end counts, and an open PR or a live task does not
hold it once that end is a day old.

A job that never emitted `job.spawned` — a design session, a desk pane, a
shell — has no recorded pane and is never a candidate. A job a successor
replaced is a candidate whatever its state (see Replaced jobs).

## Ownership proof

A pane is the job's own when herdr's row with the recorded pane id proves it
(operator decision 265). When `job.spawned` recorded a harness session id, that
row's `agent_session.value` must equal it: a different session is another job's
agent, and the pane is kept as `reason=session_mismatch`.

A receipt that recorded no session proves the pane by the facts `panewire reap`
uses, because there is no session to compare. It recorded none when it predates
the field, or when the spawner could not read the pane's session and wrote the
placeholder `id: "unknown"` with `source: "unavailable"`
(`internal/fleet/harness_session.go`). The fallback needs the row to carry the
job's exact name and tab and not to be working — every status but `working`, as
`panewire reap` decides, so a `blocked` or `unknown` row closes and only a
`working` one keeps the pane. A row that fails any of the three keeps the pane
as `reason=session_unrecorded`, past the deadline too: a renamed row is another
agent's, and a working one is a pane still in use. Unlike `paneSweepMatches`, an
unnamed row that merely sits in the job's worktree does not satisfy it: with no
session, the name is the only identity the close has.

Merging the placeholder as if it were a session is why the sweep left ended
reviewer panes open on desktop (hk 4904): `unknown` matched no live row, so
every pass kept the pane `session_mismatch` and no pass ever closed it.

The proof runs twice: before the pane's last screen is read, and again under
the node's command lock on a fresh agent listing immediately before
`herdr pane close`.

No row at all keeps the pane as `reason=pane_unlisted` — unless `herdr pane
list` also lost the pane, in which case the job is reaped as `pane_gone`.

## Report republish

The local pass republishes reports an ended job announced but never put in
handoffkeep: a `done` whose upload failed still writes `job.completed` with the
report's `report_sha256`, so the completion exists while hk holds no document
for it. Before the panes are judged, the pass PUTs exactly the announced bytes
under `done`'s content-addressed key, writes the `job.report_published`
receipt, and pins the terminal record (`report_doc`, `report_sha256`) when the
report is the completing one, as step 3 of `done` does. A report the record
already pins is nothing to republish: hk holds its bytes, and `done` deletes the
local file once the pin lands (hk 3685), so the receipts, not the file, decide.

A report whose file no longer hashes to the announced sha256 is left alone with
`reason=report_changed`: it is still being written, or it belongs to a later
round that only a new `done` announces, and the pass never pins bytes no
completion named. A file that is gone is left with `reason=report_missing`. A
pin write hk did not answer is repeated by a later pass. At most five uploads
and, separately, five pin attempts that write or that hk does not answer run per
pass — a read that answered costs nothing — so a pin hk will not answer can
never hold the upload another job needs; after three unanswered pin attempts,
and at once for a record hk answers as not completed, the report waits twelve
passes between probes rather than holding a pin slot on every one. Each
republished report or refusal is logged once. The upload runs without the
job's operation lock so a worker's `done` never waits behind it; the receipt and
pin writes take that lock only briefly, and a pass that finds a `done` holding it
leaves both writes to the next pass. The ordered `pane.sweep` command does
not republish; only the node's own local pass does.

## Replaced jobs

A job whose job dir holds a `job.replaced` event (its `payload.replaced_by`
names the successor; spawn.go writes it when a successor takes the job's task)
counts as ended, whatever its PR or pane state: the successor owns the task and
the PR. This rule runs before the open-PR keep. Without it, a replaced parent
sitting on a quota error screen, whose successor owns the PR, would be kept
forever (hk 5308).

- The pane closes on the first pass that finds its row matching the job and
  not `working`; `blocked` counts. No grace and no owner-age wait. A working
  pane is kept (`reason=pane-working`) and closes on the first pass after it
  stops. A pane with no row closes through the ownership proof below.
- A pane another job's label or session has taken is refused, as for any
  ended job (`pane-mismatch`, or the proof's reason).
- The close is the same close as any ended pane: the ownership proof, the job's
  end lock (a hub close that owns the end is respected, `hub_close_owns_end`),
  then `herdr pane close`. The sweep issues no close op itself.
- The worktree is kept (`task worktree kept reason=replaced`). The hk task is
  left alone: the last screen goes to `<job dir>/pane-screen.txt` only, and no
  failure rep is recorded for the replaced job.
- The owner lane gets one info line, `[panewire] closed <job> (replaced by
  <successor>) on <host>`, under the event id `replaced-close-<job>`, so a
  repeat is one event.
- A `--keep` job keeps its own rule (hk 5295).

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
a cut tail keeps its whole copy in `<job dir>/pane-screen.txt`, mode 0600. A
replaced job's screen is never uploaded: it stays in `<job dir>/pane-screen.txt`
(see Replaced jobs).

Only a failed pane read, or a failed local write, keeps the pane as
`reason=screen_save_failed`; a close that fails keeps it as
`reason=close_failed`. Both retry on the next pass. hk's secret guard is the one
refusal that is a property of the screen's bytes (`secret_like_content`), so it
is not retried: the whole screen is written to the job directory and the close
goes on — the rule `pane.capture` already applies to a failed upload (hk 4904).
Every other hk answer is about the moment — a 5xx, a rejected credential, a
Cloudflare login redirect — and may succeed on the next pass, so it keeps the
pane as `screen_save_failed` and retries. So does an hk that is unreachable or
unconfigured.

A pane re-proven by a recorded session is closed regardless of its status,
composer contents, screen changes, or a question-like display: the job's end
plus the session proof is the authority for this path. (A pane re-proven by its
name and tab alone needs the not-working status the ownership proof above
requires.) That differs from the explicit
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

## Stale-pane alarm

The textfile needs Alloy, and a host without it kept 14 finished panes with
nothing said. Each pass therefore also counts the panes a host is collecting —
the count the hub alarms on (hk 4904).

A **stale pane** is one the pass admits as a candidate whose job its own record
ends (`paneSweepClosed`) at least 24 hours back, whose row herdr lists as the
job's own by the same ownership proof the close uses (`paneOwnershipProof`) and
shows not working (`idle` or `done`), and which this pass neither closed nor kept
for a reason that is not a failure. One predicate decides ownership on both
sides: the close takes a pane only when `paneOwnershipProof` accepts it, and the
count counts a pane only when it accepts it. Using the close's proof matters: a
pane the close refuses to own — another session, or a nameless worktree row with
no recorded session — would otherwise be counted on every pass while the sweep
keeps it for good. The close proves the pane twice, on the pass's listing and
again on a fresh listing under the acting lock after the screen is read, and a
refusal seen by either check also keeps the pane out of the count: a session
reassigned while the screen was read is another agent's pane now, and the pass's
older snapshot must not report it as a failure. The `idle`/`done` rule is read
on the row carrying the job's pane id, and it is the only extra test the count
applies: the grace's name/tab match (`paneSweepMatches`) is not, because the
close past the deadline does not consult it, so a row whose tab or name drifted
while the proof still owns it counts when its close failed. Per-reason counts in
`panewire_panes.prom` still name those refusals; they are the diagnostic gauge,
not the alarm. The count also requires the candidate's own close deadline to
have passed — the pass's age (an hour locally, or the hub's `--pane-sweep-age`,
up to 365 days) or a kept pane's day — so a sweep age longer than the alarm's
day never makes the alarm fire on a pane the sweep is still waiting on. A stale
pane is therefore one the sweep should already have closed. The job's own end
is the predicate, not any terminal event: a job whose terminal record
does not end it — an implementer loss with no quota hand-over, a `job.joined` —
is judged by its live task and may be kept legitimately, which must never latch
the alarm. So is a pane the pass kept for a reason that is not a failure: a
completed author waiting on its open PR (`open_pr`), an implementer with no
task (`no_task`), a pane inside its grace. Those are deliberate keeps that hold
for as long as their reason does, and counting them would keep the alarm raised
on a host the sweep is handling correctly. The count is taken after the pass's
closes, so a pane closed in the pass is not reported either. A pass that could
not read herdr reports nothing and leaves the last count standing.

The pass hands the count to the node, which publishes it on its heartbeat as
`ended_panes` — only to a hub that advertised `hub.features` `ended_panes`,
since the heartbeat is a closed payload and an older hub drops it. A node that
has not run a pass reports nothing; a sweep turned off keeps reporting the last
count it measured, which is the count the hub then cannot lower.

The hub turns the count into one alarm per host: any value above zero is an
incident, delivered through the hub's alert path with reason
`ended_panes_open` (WARN, and INFO `recovered_ended_panes_open` when it drops),
after the shared two-observation debounce, like a failed local check. The
count is not placement capacity: a host is not admitted fewer jobs because it
holds finished panes (operator decision for hk 4904).

Each pass logs the count as `stale` on its `ended job panes open` line.

## Worktree

After it closes a job's pane, the sweep removes the worktree the node
prepared for that job, and the job's local branch with it. A merged end
removes only a clean worktree whose HEAD merged; a non-merged end (a canceled
task, a job closed abandoned or superseded) saves the worktree's changes to
`<job dir>/abandoned-worktree.patch` first and removes it whatever its state.
Only a worktree of the clone under the node's own work root is the node's; a
requester's design worktree, a worktree another job still uses, a dirty one
on a merged end, and every remote branch stay.

A `<job>-checkout` the node prepared from a checkout spec — a consult, a
canonical checkout or a spec spawn — belongs to that job alone: the job's
receipt names the directory and it carries the job-derived name
(`checkoutWorktreeName`), so ownership is proven from the record, never from
the directory name alone. It ends with the job whatever the task did —
answered, failed, lost, abandoned or escalated — because it holds no task's
work: the tail discards it through the same patch-first path, and a detached
checkout's HEAD is kept under `refs/panewire/abandoned/<job>` instead of a
branch delete. A branch checkout — a successor spec's local branch (hk 5118) —
keeps its commits on that branch: the branch is deleted only when its tip is
already on origin. A `-c` worktree keeps its requester's own directory name and
stays under the rules above.

## Checkout sweep

`panewire node sweep-checkouts [--host <machine>] [--dry-run]` is the
operator's pass over the checkouts that already lie on a node (hk 5091). It
lists every `*-checkout` directory under the node's work root that is a
linked worktree of one of its clones, attributes each to a job — a readable
job record's receipt cwd first, then the spawn journal for a record that is
gone — and judges it live or not: a job that has not ended, a terminal job
whose pane herdr still lists, or a pane working inside an orphaned checkout
keeps it (`job_live`, `pane_open`). Everything else goes through the same
guarded path as the tail — the node work root, no other job using it, not a
symlink, no ignored state — and a job-owned checkout is discarded with its
patch saved; a directory that is not the owning job's own `<job>-checkout`
is kept `task_open_kept`. `--dry-run` reports each row as `would_remove`
without changing anything. The answer is one JSON document of
`{path, job, outcome, reason}` rows: `removed`, `kept` or `would_remove`, in
path order. A checkout whose job cannot be attributed, whose job record or
herdr list cannot be read, or whose patch cannot be saved is kept with the
reason, so the pass never removes blind.

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
