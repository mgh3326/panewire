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
| 2 | a refusal, in two kinds (below): the author still runs, or the author was ended |
| other | an error mergewake logs and continues past |

mergewake reads every rc 2 as "no step happened", so the two kinds differ in
text only:

- **The author still runs.** The refusal came before the pane was touched:
  `no_rung`, `job_not_live`, `job_changed`, `job_mismatch`, `pane_busy`,
  `pane_unavailable`, `pane_not_closed`, `successor_invalid`, `worktree_dirty`,
  `worktree_locked`, `worktree_unpushed`, `worktree_unreadable`. Nothing about
  the PR changes; mergewake goes on to round 7.
- **The author was ended.** The refusal came after the pane was closed:
  `job_not_marked`, `claim_refused`, `quota_refused`. The old pane is closed
  and the old record is `lost`; **no author runs on the task**. The CLI's
  message ends `(the author's pane was closed and its record ended; no author
  runs on task N)`, the design-lane info says `no author runs on this PR now`,
  and the hub's answer carries `author_ended: true`. A `job_not_marked` or
  `claim_refused` step is finished by running the same command again (see
  Replay); a `quota_refused` one answers the same refusal until a person
  places an author.

Every refusal writes one `info` event for the design lane, because the
policy's "say so in the record" applies to the cases where nothing can be done.

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
2. answers a replay (below) before anything else is checked;
3. checks the named rung is still a live catalog rung with an ordinary or
   escalation gate that covers the task grade;
4. builds the successor's submission and validates it the way the claim will
   (`successor_invalid`), so a submission the store would refuse never costs
   the author its pane. An experiment arm that no longer names the successor's
   canon is dropped;
5. probes the pane and refuses one that is busy or unreadable. A pane that is
   verifiably gone (an earlier attempt closed it) is not an error;
6. runs `worktree.prepare` on the successor's checkout through the holder's
   node **while the author's pane is still open**. It applies the refusals the
   successor's spawn would meet once the pane is gone, so they are refusals
   with a live author: `worktree_dirty`, `worktree_locked`,
   `worktree_unpushed` (commits origin does not have, or the branch checked
   out elsewhere), `worktree_unreadable`. The live pane is inspected first;
   its checkout must match the requested branch and the fetched origin tip.
   Verification refuses a mismatch without moving the worktree and pins the
   verified HEAD and holder host for the successor;
7. closes the pane (screen capture first) so one job holds the task;
8. ends the old record `lost` with
   `ladder step round=<n> replaced_by=<new job>`;
9. atomically transfers the `(task, worker)` reservation while claiming the successor as an
   inherited spawn (`--parent-job <old>`), with the pinned rung, a
   `ladder step ...` override reason (the ladder-step rep row,
   [reps](reps.md#ladder-steps)), and the old submission's repository and
   checkout (same worktree and branch);
10. composes the successor's brief from the original brief, the findings and the
    consult seat;
11. waits for placement. A successor that does not place is `quota_refused`,
    with the author ended;
12. writes one `info` event for the design lane:
    `ladder step <old> -> <new> on <profile>@<effort> (round <n>, <pr>)`.

Whether the account quota admits the successor's rung is known only at
placement, after the close; steps 4 and 6 and the replay are what keep every
other refusal on the safe side of it.

## Replay

The successor job id is `<old>-l1`. The hub finds a step it already took by
that id, before it reads the old record's state, and the CLI accepts a `lost`
record whose `end_reason` is this round's `ladder step round=<n> replaced_by=`:

- a placed successor answers rc 0 with its rung, a failed one answers its
  refusal (with the author ended), one still running answers the step it will
  report when it settles;
- an old record this round's step already ended with no successor claimed (a
  hub restart between steps 8 and 9) is finished: only the claim is made;
- a pane closed with the record still live (`job_not_marked`) is finished: the
  record is written, then the claim is made.

The policy allows one ladder step per job chain; a job whose id already
carries a ladder step (`-l1`, with or without later quota-stop `-q<n>`
replacements) gets `no_rung`.
