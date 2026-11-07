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
