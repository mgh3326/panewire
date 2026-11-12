# Reps, manual credit, and automatic outcomes

pacelever's `reps` and `manual` commands moved into panewire. mergewake can
switch by changing the binary name only: `panewire reps add ...` takes the same
flags and prints the same `recorded rep id=srv:<n>` receipt. The hk bench store
is unchanged.

## Reps

- `panewire reps add` writes `PUT /v1/bench/reps` with pacelever's 17-key
  envelope. Outcome fields (first CI green, findings by severity, revert,
  time to merge, experiment) ride inside `notes`, as pacelever sends them.
  `origin_id` is the upsert key, so a retry cannot duplicate a successful write.
- `panewire reps list` reads `GET /v1/bench/reps?limit=5000` and merges local
  pending intents; `--profile`, `--grade`, `--effort`, `--area` and `--limit`
  filter client side. `--json` prints `{"reps": [...], "source": ...}`.
- `panewire reps compare` groups by `profile@effort` with `count`, `completed`,
  `rounds` and `blockers`.
- `panewire reps pending list|retry origin:N|drop origin:N` manages intents a
  failed write left in `~/.local/state/panewire/reps.json`.

## Manual credit

`panewire manual set|list|clear` validates like pacelever's `manual.go` and
writes the same hk shared manual observation (lease, publish, release). The
shared wire carries only `used_pct`; the command's own JSON reply keeps
`credit.remaining_usd`, `credit.total_usd` and `credit.expires_on`. On read,
`[quota].credit_total_usd` supplies the dollars (#309). Keeping the dollars in
the shared record itself needs an hk field and is carried as hk 3418.

## Automatic outcomes

The task pane sweep already reads hk's change record for each job's task. When
a change reports `merged` and matches the job's own `job.joined` PR (by head or
PR number), panewire writes one bench rep for that job:

- `profile`, `effort`, `model_id`, `harness`, `grade` and `tier` from the job
  receipt and the task title's `(grade X, Tn)` marker;
- `rounds` from the job's `job.completed`/`job.escalate` records;
- `findings_by_severity`, `critical_findings` and `high_findings` from the job
  result or the job directory's `findings.json`/`result.json`;
- `time_to_merge_s` and `spawn_to_merge_s` from the change's `opened_at` and
  `merged_at`;
- `task_ref` `hk<task>`.

The origin id is derived from the job id, so a second merged signal is an
idempotent resend: hk returns the existing row, or a conflicting resend is
treated as already recorded. Unknown fields stay NULL, never zero.

### Failure rows

A worker job that ends without a merged PR of its own leaves one failure row:
`completed = 0`, the launch rung (`profile`, `effort`, `model_id`, `harness`),
`grade` and `tier` from the task title marker, and `notes.ended` set to
`escalated`, `lost`, `reaped` or `superseded`. Unknown fields stay NULL, except
`blockers_found`, which the pacelever envelope cannot send as NULL and so goes
as 0.

A failure row is written only once the job's outcome is final:

- the job joined no PR: the row is written when the job ends, unless the job
  completed and was only reaped (a completed job with no PR of its own is a
  success and leaves no row);
- the job's joined PR is closed unmerged: the row is written then;
- the joined PR is open, or its state cannot be read: nothing is written. The
  pane sweep asks again on each pass.
- the joined PR merged: the merged path writes the job's one row, and no
  failure row is written.

Every row shares the job's origin id (`panewire-outcome:<job id>`, the same
derivation as a merged row), so a job has at most one row. hk never replaces a
row (`PUT /v1/bench/reps` returns 409 `bench_rep_conflict` for a changed body),
and `PATCH` only fills NULL columns, so the merged row can never replace a
failure row. Holding failure rows until the PR closes is what keeps one row per
job. A job spawned with `--override-reason` starting `catalog experiment` writes
no automatic row, failed or merged.

## Ladder steps

A ladder step is a pinned re-spawn that replaces a rung on a PR that is still
open: `panewire spawn --parent-job JOB` with an override reason that starts
`ladder step` (for example `ladder step sonnet high → opus high, PR 77`). The
replaced job is `JOB`; the PR is the one it joined. When that spawn lands (its
`job.spawned` event is committed), panewire writes one bench rep for the
replaced rung. The write is best effort: the spawn is kept whatever the write
does, and a failure is logged as a warning.

The row:

- `completed = 0`, with `profile`, `effort`, `model_id` and `role` of the
  replaced job, `task_ref` `hk<task>`, `tier` from the spawn and `grade` from
  the task title marker;
- origin id `panewire-ladder:<replaced job id>`. A job has at most one ladder
  row, so a retry of the same step gets a 409 from hk and keeps the first row;
- `notes` with `kind = "ladder_step"`, `job` (the replaced job), `pr`,
  `rounds`, `replaced` (profile, effort, model, harness of the replaced rung),
  `takes_over` (job, profile, effort, model, harness of the rung that takes
  over) and `reviewed_heads`.

`reviewed_heads` is always `null`. panewire keeps no record of the PR heads
mergewake reviewed, so the count is unknown, never zero. `blockers_found` goes
as 0, as in failure rows, because the envelope cannot send it as NULL.

Two things hold regardless of what happens to the PR later:

- `panewire reps list --profile P --effort E` shows the row under the replaced
  rung. The rung that takes over does not claim it.
- A later merge writes the merging job's own outcome row (origin
  `panewire-outcome:<job id>`). The ladder row is not altered, and the replaced
  job keeps its own outcome or failure row under its own origin id.

`panewire reps compare` counts the ladder row in `count` like any other
`completed = 0` rep, so a rung can show one more attempt than it had. The row
is a real failed attempt, but a replaced job that later closes unmerged also
gets a failure row, so the two can both count against one rung.

A spawn that names no parent job, or a parent that joined no PR, writes no
ladder row. The spawn still succeeds and a warning is logged.
