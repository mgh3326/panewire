# Observability

Fleet-event v1 lines and Sentry fingerprints for the spawn, relay and job-result
failures that would otherwise leave no trace. The record shape, redaction and
Alloy pipeline are in `deploy/alloy/README.md` ("Fleet events"); the hub's spawn
telemetry is in [durable spawn operations](hub-spawn-operations.md).

All lines are written by `panewire` (the hub or a node) to
`$XDG_STATE_HOME/panewire/events/fleet-events.jsonl`. Only `host`, `tool`, `kind`
and `outcome` become Loki labels; `job`, `reason` and `detail` stay in the line
and are read with `| json`.

## Fleet events

| `kind` | `outcome` | `reason` | Writer | When |
| --- | --- | --- | --- | --- |
| `relay` | `delivered` | the relay kind (`lane.event`) | hub | a node proved the row reached its pane |
| `relay` | `dropped:<reason>` | the drop code (`working_wait_expired`, `inject_failed_max_attempts`, `maybe_in_pane`, `lane_rerouted`, `job_ended`, a stale-lease code) | hub | a node gave up on a held row |
| `relay` | `unconfirmed:<reason>` | the first token of the report (`maybe_in_pane`, `ack_timeout`, ...) | hub | a node reported it could not prove delivery, or its ack window expired |
| `spawn_held` | the held branch (`succeeded_pane_held`, `failed_pane_held`, `recordless_pane_held`, `failed_record_missing`, `hk_read_error`, ...) | the pane-probe reason (`pane_listed`, `node_unreachable`, `not_probed`, ...) | hub | a reclaim refused to release a role reservation |
| `job_result` | `attached` | `duplicate_done` | node | the duplicate `done --result` attached a result (the duplicate-completion fix) |
| `job_result` | `conflict` | `different_result` | node | a later `done --result` met a different stored result |
| `job_result` | `missing` | `no_result` | node | a reviewer's completed `done` carried no result |

`relay` lines are emitted for worker lanes (a lane whose route names a parent)
and design lanes (a lane another route reports to). There is one line per row
outcome, not one per retry. `detail` names `lane=`, `machine=`, `pane=` and the
harness when the node reported one.

## Sentry

Reporting uses the `panewire` project, `Environment=fleet`, level `warning`.
Only these states send an event; every other failure stays a journal line or a
fleet event.

| Fingerprint | Tags | When |
| --- | --- | --- |
| `relay-dropped/<reason>/<harness>` | `job`, `lane`, `machine`, `pane`, `reason`, `harness` | a relay row was dropped for a worker lane; `harness` is `unknown` when the node could not read it |
| `spawn-held-loop/<branch>` | `role_key`, `branch` | the same `role_key` was held five times in 30 minutes; at most one event per `role_key` per hour |
| `job-result-conflict` | `job` | a `done --result` conflicted with the stored result |
| `spawn/<stage>/admission_refused:<class>` | `stage`, `reason`, `request`, `attempt` | an admission refusal before a host was bound; `<class>` is the refusal table's code (`seat_policy_missing`, `quota_catalog`, `containment_required`, ...) or the reason text's first token |

The existing `SPAWN_FAILED` fingerprint keeps `<stage>` and a normalized reason;
the admission class splits the former single `admission_refused` group. Free
refusal text never enters a fingerprint, a tag or an event message.
