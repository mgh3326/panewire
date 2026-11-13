# Intermittent-node job resilience

This is an operator-assist path for jobs whose authoritative state remains the
local `jobs/<id>/events/` files and the corresponding pane injection. The hub
does not dispatch work, carry a brief, or bypass the spawner.

## Node heartbeat

Run the node daemon with its local inbox root supplied explicitly:

```sh
panewire daemon --hub-url wss://hub.example --hub-token-env ~/.config/panewire/hub.env \
  --hub-jobs-root "$HOME/work/herdr-inbox"
```

Every normal hub heartbeat scans `jobs/*/events/` and sends at most 32 active
claim records. Each record contains only job ID, claim agent label, event
sequence, optional push SHA, and epoch. A claim is `job.claimed` (the
legacy-compatible `job.claim` is also read); a terminal `job.completed`,
`job.completion`, or `job.revoked` event removes it from the active set.
`job.completed`/`job.completion` also produce one fenced `job.completed` hub
event with the local epoch. Briefs, command text, and pane output are never
sent.

### Node inbox event contract

The node treats the arbiter event envelope as canonical. The older flat form
remains accepted only for compatibility with existing `done` writers.
Top-level metadata takes precedence when both forms provide a value.

| Form | Required event discriminator | Metadata read by node |
| --- | --- | --- |
| Arbiter envelope (canonical) | top-level `kind` | `payload.agent_label`, `payload.owner_lane`, `payload.label`, `payload.report_path`, `payload.report_last_line`, `payload.host` |
| Flat (compatibility) | `type`, `kind`, or `event` | corresponding top-level metadata fields |

An envelope claim without `epoch` is reported as epoch 1, the hub's first-seen
epoch; a node still cannot self-promote to a higher epoch. Active scans keep
the 32-record limit but select by each job's latest event time (top-level
`created_at`, otherwise file mtime), newest first. Claims older than
`PANEWIRE_JOB_ACTIVE_MAX_AGE` are excluded; its default is **72h**. Completed
events use the same cutoff based on the completion event time, preventing old
terminal files from being resent after node restart.

The hub waits `--hub-grace` after a disconnected/stale presence transition
before emitting one `job.orphaned` event. It appears in the authenticated
`/v1/events` stream and UI Recent events.

## Operator flow

First inspect candidates using an operator token:

```sh
panewire jobs orphaned --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
```

The dispatcher remains the separate orch worker. After it has selected a
safe receiving node and submits through that node's ordinary admission
gate, record the decision at the hub:

```sh
panewire jobs reassign --job-id example-42 --to node-b \
  --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
```

This increments a hub-issued job epoch and records `job.reassigned`. The hub
retains every prior owner in its in-memory fence history; a heartbeat cannot
raise its own epoch or retake ownership. While a fenced predecessor is online,
or after it reconnects and heartbeats, the hub retries `job.revoked` until that
node confirms its local marker write. Its daemon then writes
`NNNNN-job.revoked.json` locally. The spawner-side hook owns the resulting pane
message:

`[REVOKED — 다른 노드에 재배정됨, 즉시 중단·push 금지]`

An old-epoch `job.completed` event is rejected by the hub. A completion from a
node that returns before reassignment turns the prior orphan record into
`job.recovered`.

When an orphaned owner returns while its local claim is still active, the hub
clears the orphan marker and emits `job.recovered`; it no longer appears in the
reassignable list. The receiving node receives the hub-issued epoch as
`job.assigned` immediately when connected and again with heartbeat directives,
then uses `max(local_epoch, assigned_epoch)` in both its active-job heartbeat
and its completion event.

The hub job/fence view and retry queue are intentionally in memory. A hub
restart loses orphan, epoch, reassignment, and pending-revocation state; local
job event files remain the system of record, but the hub does not yet
reconstruct fences from them. Avoid restarting the hub during an active
redispatch until a later durable reconstruction design is deployed.

## WIP push convention

Worker briefs should explicitly say: **push a `wip:` commit every 30 minutes,
and before any risky step.** This gives a redispatched worker a bounded,
inspectable resume point; it does not change the spawn gate or make the hub a
source of truth.

## Harness session identity

A `job.spawned` payload contains `harness_session` with `kind`, `id`, and
`source`. This identifies the harness conversation, not the herdr session
that contains the pane. On `job.completed`, panewire refreshes the reference
from herdr and retains the landing reference if the pane is unavailable.
Older jobs without this field keep their existing completion record format.

Herdr's `agent list` reports `agent_session` from its harness integrations:
Claude and Codex provide an ID; omp provides a session path or ID. `source`
is herdr's reported source (for example, `herdr:codex`). Other harnesses can
also supply a reference through the same metadata. A reference is accepted only
for the matching pane and harness. If none is available at landing, `id` is
`unknown` and `source` is `unavailable`; the spawn still proceeds. No session
or credential file is read to discover this value.

## Reap closes the hk record of a revoked job

A job that ends by `job.revoked` or `job.lost` writes nothing to hk, so its
record would stay `claimed` or `spawned` and hold the task's role against every
later spawn. `panewire reap --apply` therefore writes `state=lost` to the hk
record before it closes the pane and writes `job.reaped`, when the job's newest
terminal event since its last claim is `job.revoked` or `job.lost`. A job that
completed or joined, a record that is already `completed` or `lost` and a job hk
does not know are not written. A failed hk read or write is a warning plus a
`job.record_failed` event (`op: reap`); reap still closes the pane and writes
`job.reaped`.

## Token usage capture

When the node completes a job (`panewire done`, `escalate`, and the completion
watcher) it reads that job's token usage and writes it with a second
`PUT /v1/jobs/{job}`, after the terminal record. `panewire reap` and the
delegate close read again and write again, before the pane closes. The `usage`
object replaces the stored one as a whole, so the later read wins; a reap read
with no figures is not written, so it cannot erase the completion read. A job
whose `job.spawned` carries no `harness_session` gets no usage write. A failed
usage write is a warning (HTTP status only) and changes nothing about the
job's completion. The hk side is `usage` and `model` in
hk:doc `handoff/2026-11-05/handoffkeep-design-usage-outcome-contract` §1.

The read is made on the node that ran the pane, from the record the job's
`harness_session` names. It never picks a file by working directory or
recency. An `unknown` id, a missing, unreadable, ambiguous or symlinked file,
or a file over 8 MiB gives unknown for every count (`usage` carries only
`source`), never 0. Lines over 2 MiB and malformed lines are skipped. Only
integers and a model id (matched against a strict character set) leave the
parser.

| Harness | Record | Counts | `source` |
|---|---|---|---|
| claude | `~/.claude/projects/<cwd, non-alphanumerics as "-">/<id>.jsonl` | `message.usage` of assistant entries, once per `message.id` (a message is written once per content block): `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens`; reasoning unknown | `claude-session-jsonl` |
| codex | the one `~/.codex/sessions/*/*/*/rollout-*-<id>.jsonl` | `total_token_usage` of the **last** `token_count` event (cumulative, never summed): `input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`; cache write unknown | `codex-token-count` |
| omp | the `path` reference, inside the home directory | `message.usage` of assistant `message` entries: `input`, `output`, `cacheRead`, `cacheWrite`; reasoning unknown | `omp` |
| devin, agy, cmd, other | none documented | all unknown | the harness kind |

Known limits, from the harnesses' own documentation: Claude Code builds each
assistant entry's `output_tokens` from the count reported when the response
began, so the claude output total can undercount (the Agent SDK cost-tracking
guide says to read output from the result message, which a transcript
reader has no access to). Codex `input_tokens` includes `cached_input_tokens`.
The code is `internal/fleet/usage_reader.go` and `usage_record.go`.
