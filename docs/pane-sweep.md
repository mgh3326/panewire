# Task pane sweep

A worker stays in its pane after READY, so `panewire reap` (terminal jobs
only) never reaches it, and mergewake's reap, which runs once at merge time,
skips a worker that is busy then and never retries. The node daemon therefore
sweeps its own jobs: it closes the pane of a worker or tester once the hk task
the job is bound to has ended, or the job itself was closed, and the pane has
stayed not working for a grace.

## Which jobs

A local job is swept when all of these hold:

- it is not reaped, and not `--keep`;
- its role is `worker` or `tester` (never `builder`, `captain`, `lead` or
  `reviewer`; the hub's [finished-reviewer release](hub-spawn-operations.md)
  keeps reviewer panes);
- and one of these ends it:
  - **its task ended.** The task is the claim's `--task`, or, for a fix author
    spawned with `--parent-job` whose claim names none, the task of the parent
    job: read from the parent's job record on this node, else from the hk job
    record's `task_id` (the parent is the claim's `task_inherited_from`, else
    the hk record's `parent_job`). If no task resolves the pane stays, logged
    `no_task`. A task has ended when its hk **category** is `completed` (the
    job ends `completed`; any state such as `merged`, `done` or `deployed`) or
    `canceled` (the job ends `lost`). Only when the record carries no category
    do the state names `merged`, `done` (completed) and `dropped` (lost) decide;
  - **the job was closed.** Its latest record is terminal: a `job.revoked`
    with any outcome (`panewire job close`), or a `job.completed`. This holds
    whatever the task's state, and whether or not hk can be read. The job ends
    `completed` for outcome `completed` or a `job.completed`, else `lost`. A
    job reprompted after its close is live again and is kept.

A task-derived end waits while the job's worktree is on an open PR. The sweep
reads hk's changes for the task (`GET /v1/changes?task=<id>`, the source
`panewire pr status` uses) and keeps the pane when an open change names the
worktree's `HEAD`, or the tip of the worktree's branch on origin (hk's change
carries the PR head, not its branch). The line says `reason=open_pr` and the PR
number. A job whose own record ended (`job.completed` or `job.revoked`) is
still swept: its result is already in hk. An hk read that fails keeps the pane
(`reason=pr_state_unknown`), so a failure never closes a pane that may still
own an open PR.

Each job is judged alone. Another live job on the same task, such as a fix
author, does not keep this job's pane open.

## Idle time

The daemon keeps, in memory, the first time a sweep saw each job's pane **not
working**. A pane counts as not working when herdr lists it with agent status
`idle` or `done` under the job's own pane id, agent name and tab. A `blocked`
pane never counts: it may be waiting on an ESC answer. Any sweep that finds the
pane working, renamed, or gone starts the count again.

This is real idle time, unlike the finished-reviewer release, which counts from
the spawn deadline because the hub has no state-change time. A daemon restart
also starts the count again.

## The close

When the pane has been not working for at least the grace, the sweep:

1. saves the pane's screen to `<job dir>/pane-screen.txt` (mode 0600, replaced
   atomically); a pane whose screen cannot be saved is kept;
2. re-reads the job and the pane immediately before closing, as
   `panewire reap` does, and keeps the pane if either changed;
3. reads the job's token usage while herdr still lists the pane, then closes
   the pane; a failed close keeps the job as it was, with nothing written to
   hk, and the next sweep tries again;
4. runs reap's tail: sets the hk job state (`completed` or `lost`, as
   above; a record that is already completed, escalated
   or lost is left alone), records the usage it read, releases the job's quota,
   removes its lane, and records `job.reaped` with reason `task_<state>` (the
   task's state, such as `task_merged` or `task_deployed`) or, for a closed job
   whose task has not ended, `job_<outcome>` (`job_abandoned`,
   `job_superseded`, `job_completed`; `job_revoked` when no outcome is named). If the hk write fails, the pane is
   already closed: the sweep logs a warning and records `job.record_failed`, as reap
   does, and does not retry the write.

The close runs under the node's acting-command lock, so it never interleaves
with a `pane.close` or `job.done` from the hub.

Unlike `pane.close`, the sweep does not read the pane's composer, and it does
not compare the pane's cwd; the task's ended state and the grace stand in for
them.

## The worktree

After a close, the sweep removes the worktree the node prepared for the job,
and the job's local branch with it (see [config.md](config.md#sweep)). Only
all of these make it the node's to remove:

- the receipt cwd is under the node's `work_root` and is a linked worktree of
  a clone that is a direct child of that root, the clone `worktree.prepare`
  creates. Anything else is kept and logged `not_node_worktree`;
- no other job still uses the path, logged `shared_kept`: one that is not
  terminal, one that is terminal and not reaped while herdr still lists its
  pane, or one whose history cannot be read when its event files name the path
  (or cannot be read at all). An unreadable job list or herdr counts as in use;
- the path is not reached through a symlink, and is not the canonical checkout.

What happens next depends on how the job ended:

- **A merged end** (the task's category is `completed`): the worktree must be
  clean and its HEAD an ancestor of `origin/main` after a `git fetch origin` in
  the clone. It then runs `git worktree remove <cwd>`, and `git branch -d
  <branch>` (never `-D`) only while no other worktree holds the branch. A dirty
  or unmerged worktree is kept (`dirty_kept`, `unmerged_kept`).
- **A non-merged end** (a `canceled` task, or a job closed `abandoned` or
  `superseded`): the sweep first writes `git status --porcelain` and `git diff
  --binary HEAD` to `<job dir>/abandoned-worktree.patch`, followed by each
  untracked, non-ignored file as a new-file diff (`git diff --no-index`), since
  the removal deletes them (mode 0600, replaced atomically, written before
  anything is removed; a patch, or an untracked file that cannot be captured
  (one that is not a regular file or a symlink, such as a named pipe, is never
  read), keeps the worktree, `patch_failed_kept`). It then runs `git worktree remove
  --force <cwd>`, which frees the directory for the next attempt. The local
  branch is deleted with `-D` only when no other worktree holds it and, after a
  `git fetch --prune origin`, its tip is contained in some `origin/*` ref (`task branch
  removed`, reason `on_origin`); otherwise the branch stays and the line says
  why (`task branch kept`, reason `not_on_origin_kept` or `fetch_failed_kept`).
  A merged task wins over an abandoned job: the clean-only rule applies.
- **A closed job whose task is still open** keeps its worktree for the task's
  owner (`task_open_kept`).

A remote branch, a requester's design worktree (`~/work/<repo>.<slug>`) and a
reviewer's worktree stay with mergewake.

A removal that fails is a warning and a keep line, never a failed pass. One
INFO line per removal (`task worktree removed`) or keep (`task worktree kept`,
with a `reason`) carries the job and the path's basename.

It fails closed. A task that cannot be read, a herdr that cannot be listed, a
pane that does not match its job, and a close that fails each keep the pane;
a git call that cannot be read keeps the worktree.

## Log lines

One INFO line per closed pane (`task pane closed`: job, task, task_state, pane,
idle_seconds) and one per kept candidate (`task pane kept`, with a `reason`).
A candidate is a job whose task is in an ending state, whose task could not be
read, whose task did not resolve (`no_task`), or that was closed. A job whose
task is still live logs nothing. A `task pane closed` and a `task worktree
removed` line carry the `reason` (`task_<state>` or `job_<outcome>`); a kept
line whose worktree is on an open PR also carries its `pr` number. Lines carry ids only: no
screen text, no task body, no credentials.

## Configuration

Daemon flags (see [config.md](config.md#task-pane-sweep)):

| Flag | Default | Meaning |
|---|---|---|
| `--task-pane-sweep-interval` | `5m` | How often the sweep runs. `0` turns it off; the minimum is `10s`. |
| `--task-pane-sweep-grace` | `10m` | How long a pane must have stayed not working. |

The sweep runs only on a hub-connected daemon with `--hub-jobs-root`. It reads
hk with the node's own credential (`handoffkeep_env` in `spawn.json`), the same
one `job.mark_lost` uses.

Worktree removal runs only when the node's `work_root` is known from
`spawn.json`; without it every worktree is kept.
