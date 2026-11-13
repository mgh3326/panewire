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
| 0 | the author runs again at a higher rung; stdout is `ESCALATED job=<new> rung=<profile>@<effort>` |
| 2 | a refusal: `no_rung`, `quota_refused`, `pane_busy`, `job_not_live`, `job_changed`, ... Nothing about the PR changes; mergewake goes on to round 7 |
| other | an error mergewake logs and continues past |

A refusal that names `no_rung` or `quota_refused` also writes one `info` event
for the design lane, because the policy's "say so in the record" applies to
the cases where nothing can be done.

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

1. reads the replaced job's spawn operation and hk record; refuses a job that
   is not a live worker on that task, or whose recorded rung no longer matches
   the request (`job_changed`);
2. checks the named rung is still a live catalog rung with an ordinary or
   escalation gate that covers the task grade;
3. probes the pane, and refuses one that is busy or unreadable;
4. lists the worktree through the holder's node and refuses one with
   uncommitted changes (`worktree_dirty`): `worktree.prepare` would refuse it
   after the pane was closed, and an author with uncommitted work must keep its
   pane;
5. closes the pane (screen capture first) so one job holds the task;
6. ends the old record `lost` with
   `ladder step round=<n> replaced_by=<new job>`;
7. releases the old `(task, worker)` reservation and claims the successor as an
   inherited spawn (`--parent-job <old>`), with the pinned rung, a
   `ladder step ...` override reason (the ladder-step rep row,
   [reps](reps.md#ladder-steps)), and the old submission's repository and
   checkout **unchanged** — that is what puts it on the same worktree and
   branch;
8. composes the successor's brief from the original brief, the findings and the
   consult seat;
9. waits for placement. A successor that does not place is `quota_refused`;
10. writes one `info` event for the design lane:
    `ladder step <old> -> <new> on <profile>@<effort> (round <n>, <pr>)`.

The successor job id is `<old>-l1`. A replay after a lost answer finds that
operation: a placed one answers rc 0 with its rung, a failed one answers its
refusal, and one still running answers the step it will report. The policy
allows one ladder step per job chain; a job whose id already ends `-l1` gets
`no_rung`.
