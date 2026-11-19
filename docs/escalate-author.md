# Escalate author — the round-5 ladder step

The review-round rule (`hk:doc policy/model-families` §The escalation ladder,
operator decisions 137, 146 and 148) gives mergewake three numbers, read from
its configuration:

- round 3 ends without a pass: mergewake tells the worker to stop patching and
  ask a consult one grade above its seat, in another family. panewire is not
  called.
- round 5 ends without a pass: mergewake runs
  `panewire escalate-author --task ID --job JOB --pr owner/repo#N --round N --findings-file PATH`.
- round 7 ends without a pass: mergewake holds the PR and sends the design
  lane one decision event.

This document is the panewire half of the round-5 step.

## The wire

```
panewire escalate-author --task ID --job JOB --pr owner/repo#N --round N --findings-file PATH
```

`--job` is the author job the PR's task is bound to. `--findings-file` is
mergewake's JSON array of findings (`ref`, `severity`, `path`, `line`, `title`,
`body`), at most 32 KiB.

| rc | meaning |
| --- | --- |
| 0 | placement succeeded and the task binding names the successor; stdout is `ESCALATED job=<new> rung=<profile>@<effort>` |
| 2 | refusal before the author was ended |
| 1 | the old author was ended, the predecessor ended independently, or the successor's task rebind is unconfirmed; inspect the named code and design-lane event |
| 124 | the operation is pending; retry the same request |
| other | an error |

The hub returns `author_ended: true` for a refusal after the pane closes.
A `rebind_unconfirmed` result means the successor placed but its task ownership
is not confirmed. Refusal events identify the phase and code, so an ended
refusal cannot be hidden behind an earlier intact refusal. An ended refusal is
a decision event (ESC) naming the PR as authorless; panewire starts no
successor on its own, only the owner lane's reply does. A refusal by the hub's
read-only plan keeps the plan's own code and text — a `NO_ROLE_CANDIDATE` seat
refusal at rc 77 reports `seat_refused`, an unknown pinned host `host_alias` —
so it is never reported as a quota drop.

A stored submission with the retired `worker` role is refused as
`legacy_worker_submission` before any successor is staged. The owner lane
receives a decision event; the existing author's reservation remains until the
job ends and the ordinary cleanup releases it.

## A completed author is not a gone author

An author's first READY completes its hk job record (and frees its task role),
and the author then fixes the PR's findings in the same pane through every
later review round. By round 5 a completed record is the normal case, not a
refusal (hk 5521: 7 of the 19 round-5 calls on the 32 PRs with 7+ reviewed heads
stopped at `job ... is not live (state "completed")`, and the CLI sent no lane
event). So the step looks at the pane, not at the record's state:

- `completed` record, pane present: the step goes on exactly as for a live
  author, except that hk keeps the record terminal. No lost mark is written
  over it and no reservation is carried over from it (it is free already): the
  successor claims the task role fresh, and a refusal before the close
  discards the staged successor instead of restoring a role to a terminal job.
  The successor's `--parent-job` rebind moves the task from the completed
  parent as it does for a hand-over.
- `completed` record, pane gone: `job_not_live` ("the job completed and its
  pane is gone"), an info event, `author_ended` false: the author ended on its
  own and the step never touched it.
- A replay that finds a staged successor of a completed author with the pane
  gone cannot tell this step's close from an independent one, so it answers
  `parent_ended` (rc 1) and releases the stage.

The CLI no longer judges the job's state or task binding. It reads the record
only for the rung to step from; `job_not_live` (lost, escalated), `job_mismatch`
(the stored submission or the record names another task) and the rest are the
hub's refusals, and every hub refusal files one lane event for the owner (see
the table below). The CLI itself still refuses, with rc 2 and no event, only
what the hub cannot see: usage errors, an unreadable or malformed findings file
and an unreadable job record.

## The CLI's half

The CLI reads the author's hk job record (`profile`, `effort`, `pool`,
`family`, `owner_lane`) and the task's `(grade X, Tn)` title marker, then:

1. resolves the consult seat: the consult floor of the policy (one grade above
   the asker, capped at SS, another family, never unrated);
2. computes the next ladder rung, in the policy's order:
   - the same model one effort step up, up to the family's ladder cap
     (`internal/fleet/families.json`, the policy's machine-readable block);
   - at the cap, the next model of the same family at its entry effort;
   - at the top of the family, one try on a seat of another family at the same
     task grade, resolved by the ordinary selection core.

A step never goes to a rung graded below the task and never to a consult-only
rung. When no step is left the CLI still sends the request with no rung, so
the hub owns the design-lane record for that outcome too.

## The hub's half

`POST /v1/jobs/escalate-author` (operator credential) receives the resolved
rung. The hub owns what only it has: the replaced job's stored submission, its
checkout and its pane. It:

1. Reads the existing operation and record, and checks that the request is for
   the same implementer, task and review round.
2. Inspects the live pane's actual checkout and pins its HEAD and holder host.
   It validates the successor and stages the complete request and ladder-step
   identity, transferring the implementer reservation in the same transaction.
3. Checks the checkout again using read-only inspection: the branch and HEAD
   must match, the tree must be clean and unlocked, and origin must contain the
   staged commit. A read-only hub placement plan checks admission on the pinned
   host; it does not reserve capacity or guarantee later placement. A refusal before close cancels the staged row and restores
   the reservation atomically. An expired stage is cancelled on replay or by the periodic reconciler only
   after the original pane is confirmed present.
4. Closes the pane, marks the old job lost, then activates the staged
   successor. Once the pane is closed, replay moves the step forward.
5. Waits for placement and confirms the task's job binding; it retries an
   unconfirmed rebind before reporting `rebind_unconfirmed`.

Quota can change after the plan; a refusal during placement after close
reports the ended author. An unknown pane or placement result is `pending`;
it is not reported as a successful escalation.

## Replay

The successor job id is `<old>-l1`. Replay finds its operation before testing
whether the old record is live, and uses the frozen identity, rung, host and
checkout. A newly resolved rung is ignored for the same staged job, round and PR, and a selection outage still reaches the hub. A staged successor with a present pane repeats preflight; if the pane
is gone or the job is already lost, replay finishes the mark and activation.
Active placement returns `pending` until it settles, and a placed successor is
successful only after the task binding is confirmed. A staged step's identity
must match the same job, round and PR. If the predecessor ended for a different reason, the hub discards the stage and releases its reservation; if it ended for this step, reconciliation activates the staged successor.

The policy allows one ladder step per job chain; a job whose id already
carries a ladder step (`-l1`, with or without later quota-stop `-q<n>`
replacements) gets `no_rung`.

## Reading the refusals

Every refusal the hub answers also files one lane event for the job's owner
lane, so a refusal is countable without the hub database:

| refusal | event id | type | rc |
| --- | --- | --- | --- |
| before the pane closed (`job_not_live`, `job_mismatch`, `pane_busy`, `pane_not_closed`, `worktree_*`, `staged_expired`, `no_rung`, `quota_refused`, ...) | `ladder-step:<job>:<round>:preflight:<code>` | info | 2 |
| after the pane closed (the PR is authorless) | `ladder-step:<job>:<round>:ended:<code>:esc` | decision | 1 |
| step done | `ladder-step:<successor>` | info | 0 |

`pane_not_closed` names why the close was refused after the colon
(`question_open`, `pane_changed`, `close_unreadable`, `capture_failed`): a pane
that went back to work or waits on an answer is worth retrying, the others are
not. `panewire inbox status --event-id ladder-step:<job>:<round>:preflight:<code>`
answers whether a given refusal was filed.

## Counting step-ups

A step that placed its successor writes one `ladder_step` rep (docs/reps.md
§Ladder steps) for the replaced rung: `notes.job` is the replaced author,
`notes.pr` and `notes.repo` name the PR (read from the step's own override
reason, so an author that never ran `job joined` is counted), `notes.rounds`
and `notes.takes_over` the successor. Step-ups per PR:

```sh
panewire reps list --limit 5000 --json > reps.json
jq -r '.source' reps.json      # must not end in "-truncated": the rep window is 5000 server rows
jq -r '.reps[] | (.notes | fromjson? // {}) | select(.kind == "ladder_step")
       | "\(.repo // "?")#\(.pr)"' reps.json | sort | uniq -c
```

The first line prints the rep window's source: when it ends in `-truncated`
the newest 5000 rows are all the read sees, and a count is a lower bound. The
second counts ladder steps per repository and PR, one rep per step.

A refused call leaves no rep; its record is the refusal event above, and on
the mergewake side its `ladder-intact`, `ladder-escalated` and
`ladder-ended-no-successor` audit rows.
