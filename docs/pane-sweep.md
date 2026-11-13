# Task pane sweep

A worker stays in its pane after READY: it is its PR's author and fixes later
review findings until the PR merges or its owner releases it (the wake
method). mergewake's reap, which runs once at merge time, skips a worker that
is busy then and never retries. The node daemon therefore sweeps its own jobs:
it closes the pane of an implementer once the hk task the job is bound to
has ended, or the job itself was closed, and the pane has stayed not working
for a grace. A completed author's pane waits while its PR is open; a job its
owner gave up or released does not.

## Which jobs

A local job is swept when all of these hold:

- it is not reaped, and not `--keep`;
- its role is `implementer` (never `builder`, `lead`, `reviewer` or
  `consult`; the hub's [finished-reviewer release](hub-spawn-operations.md)
  keeps reviewer panes);
- and one of these ends it:
  - **its task ended.** The task is the claim's `--task`, or, for a fix author
    spawned with `--parent-job` whose claim names none, the task of the parent
    job: read from the parent's job record on this node, else from the hk job
    record's `task_id` (the parent is the claim's `task_inherited_from`, else
    the hk record's `parent_job`). If no task resolves the pane of a live job
    stays, logged `no_task`. A task has ended when its hk **category** is `completed` (the
    job ends `completed`; any state such as `merged`, `done` or `deployed`) or
    `canceled` (the job ends `lost`). Only when the record carries no category
    do the state names `merged`, `done` (completed) and `dropped` (lost) decide;
  - **the job was closed.** Its latest record is terminal: a `job.revoked`
    with any outcome (`panewire job close`), or a `job.completed`. This holds
    whatever the task's state, and whether or not the task can be read. The
    job ends `completed` for outcome `completed` or a `job.completed`, else
    `lost`. A job reprompted after its close is live again and is kept.

### A pane that waits on its open PR

Two ends wait while the job's worktree is on an open PR:

- a **task-derived end** (the task ended; the job has no terminal record of its
  own);
- a **completed author**: the job's own latest record is the worker's
  `job.completed` (READY), and the job is not a reviewer (spawn role
  `reviewer`, or a mergewake `mwr-*` label), or a consult. The task's
  state does not matter: a task still `reviewing` and one already `deployed`
  both wait.

A job its owner closed with `panewire job close` (`job.revoked` with outcome
`abandoned`, `superseded` or `completed`) was given up or released, and is
swept whatever its PRs say. So is a completed reviewer or consult,
whose worktree may sit at the PR head it looked at.

A completed fix author's task is its parent's (see above). Its pane closes
without a PR read only when it names no parent anywhere, since hk links PRs to
a task: the claim names none, and hk's record of the job names none or hk
holds no record of it. A parent that is named must resolve to a task. When it
does not (the parent's record on this node names no task, and hk holds no
record of the parent or its record names no task), or when hk cannot be read
for the job or its parent, the task is unknown and the pane is kept
(`reason=pr_state_unknown`). hk can answer `not_found` for a record that has
not propagated yet, as `--parent-job` resolution allows for, so the next pass
reads the parent again and judges the job on its task once it resolves. A
parent whose records never name a task keeps the pane until the job's owner
closes it (`panewire job close`).

The sweep first reads the worktree: its `HEAD`, and the tip of its branch on
origin (hk's change carries the PR head, not its branch). The tip is read from
origin with `git ls-remote`, not from the clone's
`refs/remotes/origin/<branch>`, which can be older than a PR head pushed from
another checkout; a branch origin does not have leaves `HEAD` alone. It then
reads hk's changes for the task (`GET /v1/changes?task=<id>`, the source
`panewire pr status` uses) and keeps the pane when an open change names one of
those commits. The line says `reason=open_pr` and the PR number. A worktree or
origin that cannot be read, or an hk read that fails, keeps the pane
(`reason=pr_state_unknown`) whatever the change list says, so a failure never
closes a pane that may still own an open PR.

`panewire reap --apply` (and `prune`) apply the same rule to a completed
author they would close: the job line says `action=keep reason=open_pr` or
`reason=pr_state_unknown`. They read hk with the host's svc-panewire
credential (`~/.config/panewire/hk-svc.env`, as the CLI's other hk reads do);
a host without it keeps such a pane, `reason=pr_state_unknown`.

A task's change list is read at most once per pass: the first read, for this
check or for recording a merged outcome, serves every later job on the task in
that pass. The close does not read git or hk's changes again, so a PR opened
for the worktree after that first read does not keep the pane. The window runs
from the task's first PR read in the pass through the close, and has no fixed
bound: the close waits for the node's acting-command lock, saves the pane's
screen and re-reads the job and herdr first.

The close's re-read of the job (step 2 below) detects one transition: a job
that did not wait on its PR when the pass judged it (one its owner closed)
and does now (it was revived, or revived and then reported READY). Such a job
is kept (`reason=job-changed`) and judged again, PR read included, on the
next pass that finds it past the grace. A live job that reports READY in the
window is not such a transition: it already waited on its PR, and the pass's
PR read applied to it.

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

1. uploads the screen to hk as `screens/<job>/pane-screen.txt` (kind `evidence`,
   the job, `origin_class` `job`, and `task_id` for a numeric task), cut to its
   tail as `pane.capture` does (see [node-commands.md](node-commands.md#panecapture)).
   A stored upload of a screen within hk's cap writes no local copy: the
   screen document is the evidence, and an existing `<job dir>/pane-screen.txt`
   from an earlier pass is left alone. A failed upload is one warning, and the
   screen is then written to `<job dir>/pane-screen.txt` (mode 0600, replaced
   atomically) as its only copy. A screen over the cap is stored as its tail,
   so its whole copy is written locally too. The close goes on either way. A
   pane whose screen cannot be read is kept, and so is a pane whose local write
   failed where a local copy is needed;
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
read, whose task did not resolve (`no_task`), or that was closed. A completed
author kept for its PR logs `reason=open_pr` (with `pr`) or
`reason=pr_state_unknown` at every pass while that holds. A job whose
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
