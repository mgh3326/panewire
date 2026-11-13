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
refusal cannot be hidden behind an earlier intact refusal.

A stored submission with the retired `worker` role is refused as
`legacy_worker_submission` before any successor is staged. The owner lane
receives a decision event; the existing author's reservation remains until the
job ends and the ordinary cleanup releases it.

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
