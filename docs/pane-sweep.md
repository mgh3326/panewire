# Task pane sweep

A worker stays in its pane after READY, so `panewire reap` (terminal jobs
only) never reaches it, and mergewake's reap, which runs once at merge time,
skips a worker that is busy then and never retries. The node daemon therefore
sweeps its own jobs: it closes the pane of a worker or tester once the hk task
the job is bound to has ended and the pane has stayed not working for a grace.

## Which jobs

A local job is swept when all of these hold:

- it is not reaped, and not `--keep`;
- its role is `worker` or `tester` (never `builder`, `captain`, `lead` or
  `reviewer`; the hub's [finished-reviewer release](hub-spawn-operations.md)
  keeps reviewer panes);
- its claim names an hk task (`--task`, or the task inherited from the parent
  job);
- the hk task state is `merged`, `done` or `dropped`.

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
3. runs reap's tail: records usage, sets the hk job state (`completed` for
   `merged` and `done`, `lost` for `dropped`; a record that is already
   completed, escalated or lost is left alone), closes the pane, releases the
   job's quota, removes its lane, and records `job.reaped` with reason
   `task_merged`, `task_done` or `task_dropped`.

The close runs under the node's acting-command lock, so it never interleaves
with a `pane.close` or `job.done` from the hub.

Unlike `pane.close`, the sweep does not read the pane's composer, and it does
not compare the pane's cwd; the task's ended state and the grace stand in for
them.

It fails closed. A task that cannot be read, a herdr that cannot be listed, a
pane that does not match its job, and a close that fails each keep the pane.

## Log lines

One INFO line per closed pane (`task pane closed`: job, task, task_state, pane,
idle_seconds) and one per kept candidate (`task pane kept`, with a `reason`).
A candidate is a job whose task is in an ending state, or whose task could not
be read. A job whose task is still live logs nothing. Lines carry ids only: no
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

Worktree and branch removal is not part of the sweep.
