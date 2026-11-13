# Task pane sweep

An implementer stays in its pane after READY while it is the PR author and fixes
later review findings. The node's idle pass and the hub's ended-job pass find
jobs that appear eligible for cleanup, but herdr's current interface cannot
bind a pane read or close to an authoritative owner and pane incarnation. Both
passes therefore retain eligible panes and save only unattributed local
diagnostics until herdr provides an ownership-bound close operation.

## Candidate selection

The local pass considers jobs that are not reaped or `--keep` and whose role
is `implementer`, `reviewer` or `consult`. For implementers, a
candidate has either an ended task or its own terminal job record. A task is
the claim's task id, or, for a fix author whose claim names no task, the
parent job's task from the node's job record or handoffkeep record. If no task
resolves, a live job stays with `reason=no_task`. A task ends when its category
is `completed` or `canceled`; if no category is present, `merged`/`done` and
`dropped` state names decide. A job's own `job.revoked` or `job.completed`
record is an end; a reprompted job is live again. Lost reviewers and consults
are ends, while lost implementers and quota stops are not.

A task-derived end, and a completed author (implementer READY, excluding reviewer
and consult roles), waits while its worktree is on an open PR. The
sweep reads the worktree HEAD and its branch tip on origin, then the task's
handoffkeep changes. A matching open PR is logged as `reason=open_pr`; an
unreadable worktree, origin or change list is `reason=pr_state_unknown`. A
completed fix author's task is its parent's; if a named parent cannot resolve
to a task, its pane stays with the owner. A task's change list is read at most
once per pass. These checks decide whether a local diagnostic is useful; they
do not authorize a close or attribute captured bytes to a job.

## Idle time

The local daemon tracks the first time it sees a candidate pane with herdr
status `idle` or `done`. `blocked` never counts as idle. A working, renamed or
missing pane resets the grace. A daemon restart also starts the count again.
After the grace, the sweep may read the pane for an unattributed diagnostic.

## Ownership boundary

The herdr 0.8 interface provides neither an ownership predicate bound to
`pane read` nor a conditional close bound to the pane's owner and incarnation.
Agent listings can change independently of the read: A→B→A during capture
passes both pre-read and post-read comparisons. The sweep therefore never
uploads a captured screen, writes it into a job directory, or closes a pane by
ID. This applies to both the ended-job pass and the idle pass.

A readable candidate capture is stored under
`$XDG_STATE_HOME/panewire/pane-sweep-diagnostics/<pane-hash>/latest.json`
(or `~/.local/state/panewire` when `XDG_STATE_HOME` is unset). The directory
mode is 0700 and the file mode is 0600. The record contains the pane id,
capture id, capture time, screen bytes, and `ownership: unknown`; it contains
no job or task id. Only the latest capture per pane is retained and it is never
uploaded later. This diagnostic is an observation and cannot be attributed to
a job.

The sweep logs `reason=ownership-unproven` and leaves the pane, job record,
quota reservation, lane and worktree in place. Automatic cleanup requires an
authoritative ownership-bound close operation.

## Hub-ordered sweep

The hub orders a sweep of ended jobs even when they never went idle. The local
pass runs on the node daemon (`--task-pane-sweep-interval`); this pass is
ordered by the hub.

- The hub's maintenance ticker runs every `--pane-sweep-interval` (default
  `10m`; `0` turns it off; otherwise at least `10s`). Each tick sends
  `pane.sweep` with `{"age_s": N}` to every known node, where `N` is
  `--pane-sweep-age` (default `60m`, at most 365 days). A disconnected node is
  counted as `disconnected`; one that does not answer within five minutes is
  `node_timeout`.
- The node considers unkept, unreaped jobs in roles `implementer`,
  `reviewer` or `consult` whose own terminal record is at least `age` old.
  Completed implementers still wait while their worktree is on an open
  PR, or when PR state cannot be read.
- Eligible panes are read only for local diagnostics. The node retains every
  pane and replies with an empty `closed` list. A node-side failure (no jobs
  root, or an hk or herdr read that fails) replies `sweep_failed`.
- Only the hub's ticker may issue `pane.sweep`; a `node.command` caller cannot.
  The ordered and idle passes share one mutex on the node.

| Flag | Default | Meaning |
|---|---|---|
| `--pane-sweep-interval` | `10m` | How often the hub asks each node to inspect ended jobs. `0` turns it off. |
| `--pane-sweep-age` | `60m` | How long a job must have been ended before the node captures a local diagnostic. At most 365 days. |

## Configuration

Daemon flags (see [config.md](config.md#task-pane-sweep)):

| Flag | Default | Meaning |
|---|---|---|
| `--task-pane-sweep-interval` | `5m` | How often the local pass runs. `0` turns it off; the minimum is `10s`. |
| `--task-pane-sweep-grace` | `10m` | How long a candidate pane must have stayed not working before capture. |

The local pass runs only on a hub-connected daemon with `--hub-jobs-root`. It
reads handoffkeep with the node's own credential (`handoffkeep_env` in
`spawn.json`), the same one `job.mark_lost` uses.
