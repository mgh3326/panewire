# R31 session reap — stage 1: report only

Stage 1 answers one question per node: which local agent panes could a
future cleanup step close? It never closes anything. A node judges its own
panes, and when an operator has switched the report on it sends that
judgment to the hub. The hub keeps the latest report per machine and serves
it at `GET /v1/session-reap`. The operator console shows it as cleanup
candidates.

Closing is stage 2, a separate task. It starts only after operators have
watched the candidate list for several days and seen zero misjudgments.
Nothing in this stage issues a pane, tab, or process-ending call:
`session_reap.go` and `hub_session_reap.go` are pinned by a test that fails
if one appears.

## Who created the session decides

A pane is judged by the job record that created it. That record is the
arbiter/wrk inbox under `jobs/<id>/events`.

| case | verdict |
|---|---|
| live agent pane that no job's newest `job.spawned` receipt names | human session. It is only counted (`summary.no_job`), never a row |
| herdr `agent.list` name is empty | `held` / `label-unknown` |
| receipt `label` ≠ agent name | `held` / `label-mismatch` |
| two or more jobs match both pane and name | `held` / `pane-ambiguous` |
| claim `agent_label` ≠ receipt `label` | `held` / `label-conflict` |
| a later receiptless, unfinished claim reuses the label | `held` / `label-reused` |
| `"keep": true` on any claim, reclaim, or receipt (`wrk spawn --keep`) | `held` / `protected` |
| claim role outside `worker` / `builder` / `captain` | `held` / `protected-role` |
| worker (testers are workers) | `candidate` only when `wrk reap` dry-run would close it; otherwise `held` with wrk's reason |
| builder (or legacy captain) whose pane passes the same pane gates | `builder-task-gate`. It is never a candidate at the node |

The job-to-pane key is an **exact match on both `pane_id` and label**. The
receipt's `pane_id` must equal the `agent.list` `pane_id`, and the receipt's
`label` must equal the `agent.list` `name` byte for byte. A name-only match
once closed a working session whose name was empty; the pane_id check is
what prevents that. If either value is unknown, the pane is held.

The worker verdict is not re-implemented. It calls the fleet-census port of
`wrk reap` (`fleetCensusJobVerdict`), whose equivalence with wrk is pinned by
a vendored-oracle test. The equivalence covers every gate:

- terminal event (`job.completed`, `job.joined`, `job.revoked`)
- no revive after the terminal event
- grace (`age < grace` holds)
- `idle`/`done` status
- recorded tab equals the live tab
- single-pane tab
- the protected skip

A builder's final gate is its task, not its job. The node cannot see task
state, so it reports builders as `builder-task-gate`. The console, which owns
task state, lists one as a candidate only when exactly one task has
`refs.job_id` equal to the builder's job, that task is `merged` or `dropped`,
and it has been in that state for longer than the report's grace.

### Hub-side downgrade

When serving `GET /v1/session-reap`, the hub may only move rows *down*:

- a non-held row whose pane is the route of some other non-sink lane in the
  hub's lanes file becomes `held` / `lane-route`. This covers resident
  sessions such as a director or checker lane.
- when the lanes file cannot be read, every non-held row becomes `held` /
  `lanes-unreadable`.

The stored report is not modified; the downgrade is applied only when the
report is served.

## Wire

The node sends a separate event kind, `session.reap.report`. The heartbeat is
unchanged. The hub's heartbeat and `active_jobs` decoders reject unknown
fields, so adding the report or a keep flag there would make an older hub
drop the whole heartbeat. An older hub counts the new kind in
`unknown_messages` and keeps the connection.

```json
{"type":"event","kind":"session.reap.report","payload":{
  "schema":1,"generated_at":"2026-09-23T13:00:00Z","grace_seconds":600,
  "observed":true,"jobs_readable":true,"truncated":false,
  "rows":[{"pane_id":"w1:p1","tab_id":"w1:t1","workspace_id":"w1",
           "agent_name":"t601-verify","status":"idle","job_id":"601-verify",
           "owner_lane":"b601-builder","role":"worker","class":"candidate",
           "terminal_kind":"job.completed","terminal_at":"2026-09-23T12:00:00Z",
           "terminal_age_seconds":3600}],
  "summary":{"panes":7,"no_job":6,"candidate":1,"builder_task_gate":0,"held":0}}}
```

The hub rejects the report when any of these hold:

- any unknown or null field
- `schema` other than 1
- a `candidate` with a reason, a non-worker role, no terminal event, an
  empty agent name, or a status other than `idle`/`done`
- a summary that does not add up
- rows while `observed` or `jobs_readable` is false

An invalid report counts in `unknown_messages` and is not stored. A transient
(CLI) connection may not publish a report.

Rows are capped at 64 and at the hub message budget. Candidates sort first,
so truncation drops held rows before candidates.

`GET /v1/session-reap` requires the operator token and returns
`{"nodes":[{"machine_id","state","stale","received_at","report"}]}`.
`stale` is true when the node is not currently connected. An older hub
answers 404, which the console shows as unsupported.

## Turning it on (operator approval required)

The periodic report is a new schedule. It is **off by default** and must not
be enabled without the operator's approval.

| env (node) | meaning |
|---|---|
| `PANEWIRE_SESSION_REAP_REPORT_INTERVAL` | Go duration such as `15m`. Unset, empty, `off`, zero, negative, or unparseable means off. Values below `1m` are raised to `1m`. |
| `PANEWIRE_SESSION_REAP_GRACE` | wrk duration grammar (`600`, `600s`, `10m`, `2h`, `1d`). The default is `10m`, the same as `wrk reap`. |

`panewire session-reap [--json] [--grace D] [--jobs-root P] [--herdr-socket S]`
prints the same judgment once, locally. It sends nothing and runs no
schedule. Its only herdr calls are `agent.list`, `pane.list`, and `tab.list`.
It exits 5 when herdr or the jobs inbox cannot be read.
