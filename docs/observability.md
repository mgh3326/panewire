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
| `job_result` | `conflict` | `different_result` | node | a later `done --result` met a different stored result; strict CLI completion exits 75 and keeps the stored result |
| `job_result` | `missing` | `no_result` | node | a reviewer's completed `done` carried no result |
| `placement_shadow` | `agree`, `order_differs`, `disagree` or `shadow_error` (the worst of the arms) | the work class (`light`, `heavy`) | node | the spawn engine ranked the hosts for a job; the line compares the live order with the shadow arms and changes nothing live. See [Shadow comparison](placement.md#shadow-comparison) |

Strict CLI `done --result` completion write refusals write one
`job.record_failed` event with `{op:"completed", reason:<token>}` and print the
same safe token to stderr. Tokens are
`secret_like_content[:<pattern>]`, `terminal_without_result`,
`result_conflict`, `job_escalated`, `job_lost`,
`no_spawn_evidence`, a validated hk code (`[a-z0-9_]{1,64}`), `http_<status>`,
`hk_unreachable` or `hk_unconfigured`. A secret pattern is included only
when it matches `[a-z0-9_]{1,40}`. The response body and hk refusal reason are
never logged. The completion receipts `done` writes are in
[config.md](config.md#completion-receipts).

A `placement_shadow` line carries `job`, `request_id` when known, and a `shadow`
object: `role`, `work_class`, `live` (the live order), `arms` (a map of arm name
to `outcome`, `first`, `order` and `shadow_error`), `inputs` (one row of
readings per host) and `load_measure`. Orders and inputs are cut at 16 hosts
(`truncated`). It carries numbers and host ids only. Loki labels stay `kind` and
`outcome`; read the arms with `| json`.

## Grafana에서 차트로 보는 kind

`deploy/grafana/fleet-dashboard.json`은 다음 kind를 차트로 표시합니다:
`spawn`, `placement`, `spawn_held`, `quota_gate_shadow`, `quota_pools_shadow`,
`node_spawn`, `quota_observation`, `reviewer`, `review`. 최근 실패 로그 패널은
이 kind 목록에 한정하지 않고 실패 outcome을 표시합니다.

배치 후보의 `candidates`와 자동 선택의 `selection`은 이벤트 안의 배열입니다.
현재 LogQL은 배열 원소마다 별도 시계열을 만들지 못하므로 대시보드는 제외
결과가 담긴 배치 원본 행을 보여 주며, 후보 호스트·결과별 건수로 해석하면 안
됩니다. refusal 패널과 alert는 coordinator가 기록한 `request_id`가 있는 `spawn`
결과만 세어 같은 호스트에 기록될 수 있는 노드 측 `spawn` 거절을 제외합니다.
`request_id`가 없는 `kind=spawn` 거절은 별도 진단 패널에 표시하며, 직접 CLI 실행과
허브 실행 노드의 기록이 포함될 수 있어 고유 시도 수로 해석하면 안 됩니다.
배치 차트는 허브 `placement` 이벤트의 `request_id` 필드가 비어 있지
않은 행만 세어 request_id가 없는 노드의 `reason=forced` 실행 echo를 제외합니다.
`node-local placement records (direct spawn + hub spawn node record)` 패널은
request_id 없는 placement 성공 행을 따로 보여 줍니다. 배치 결과 패널은 호스트별 `placed`와
최종 `refused`를 비교하며, 현재 완료 이벤트가 없으므로 완료 수를 나타내지
않습니다.

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
| `relay-dropped/<reason>/<harness>` | `job`, `lane`, `machine`, `pane`, `reason`, `harness` | a relay row was dropped for a worker lane for a failure reason; `harness` is `unknown` when the node could not read it. The routine `job_ended` and `lane_rerouted` drops stay fleet events only |
| `spawn-held-loop/<branch>` | `role_key`, `branch` | the same `role_key` was held five times in 30 minutes; at most one event per `role_key` per hour |
| `job-result-conflict` | `job` | a `done --result` conflicted with the stored result |
| `spawn/<stage>/admission_refused:<class>` | `stage`, `reason`, `request`, `attempt` | an admission refusal before a host was bound; `<class>` is the refusal table's code (`seat_policy_missing`, `quota_catalog`, `containment_required`, ...), or `other` when the table does not classify the text |

The existing `SPAWN_FAILED` fingerprint keeps `<stage>` and a normalized reason;
the admission class splits the former single `admission_refused` group. Free
refusal text never enters a fingerprint, a tag or an event message.
