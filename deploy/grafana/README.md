# Fleet observability dashboard and alerts

Grafana as code for the fleet events that Alloy ships to Loki (see
`../alloy/README.md`, "Fleet events"). Provisioning is manual: the desk applies
these files once after merge and again whenever they change.

| File | What |
| --- | --- |
| `fleet-dashboard.json` | Dashboard `fleet-observability`; the panels are listed below. |
| `fleet-alert-rules.json` | Rule group `fleet-observability` in folder `fleet-observability`; the rules are listed below. |
| `apply.sh` | Idempotent apply: creates the folder (tolerates an existing one), then overwrites the dashboard and the rule group by uid. |

대시보드 패널:

- 호스트와 사유별 spawn 최종 거절 (`spawn`); 같은 시도의 `node_spawn`은 중복 집계하지 않음
- 선택 호스트와 순위별 배치 (`placement`); `request_id`가 있는 허브 기록만 선택하며 `raw_load`는 대체 순위
- 호스트와 사유별 placement 거절 (`placement`, `outcome=refused`)
- `spawn`의 `outcome=stuck` 및 `spawn_held` 분기별 추이
- kind별 quota shadow 합의 결과 (`quota_gate_shadow`, `quota_pools_shadow`)
- 후보 제외 배열이 포함된 배치 기록 로그; LogQL이 배열을 후보별 행으로 펼치지 못해 호스트·결과별 집계 대신 원본 배열을 표시
- 호스트별 배치 결과 (`placement`, `placed` 대 최종 `refused`); 완료 수가 아님
- request_id 없는 node-local placement records (직접 스폰 + 허브 스폰의 노드 기록); 허브 배치 성공 집계와 분리
- review 결과;
- reviewer별 실패;
- outcome이 `refused` 또는 `failed`인 node spawn 거절 (`node_spawn`)
- 거절 시점의 quota observation 경과 시간;
- 최근 실패 로그.

대시보드가 집계하는 fleet-event `kind`는 `spawn`, `placement`, `spawn_held`,
`quota_gate_shadow`, `quota_pools_shadow`, `reviewer`, `review`, `node_spawn`,
`quota_observation`입니다. 최근 실패 로그 패널은 `kind`를 제한하지 않고 실패
`outcome`을 선택합니다.

The rule group has these rules:

| Rule | Fires when |
| --- | --- |
| Repeated spawn refusals | More than 3 refusals for the same host and reason within 30 m. |
| Review missing | No reviewer produced a pass within 1 h. |
| Reviewer failing repeatedly | The same reviewer on the same fleet host fails 3 or more times within 2 h. |
| Quota observation stale | A refusing quota observation is older than 30 min (`age_min`). |
| Fleet events pipeline silent | No fleet event reached Loki in 6 h. |

"Reviewer failing repeatedly" groups by `host` and `reviewer`. `host` is the
fleet machine id of the mergewake host that recorded the failure (see
`../alloy/README.md`). `reviewer` comes from the event's `detail`
(`reviewer=`). For a headless reviewer it is the mergewake review profile,
which ran on that host. For a panewire pane it is the transport, `panewire`
(`runtug` in older events): mergewake spawns the pane with `-m auto` and
`--host auto`, and its event does not record the profile or the host that
panewire chose. Keying pane failures by profile and pane host needs mergewake
to add both to the reviewer event's `detail` when the spawn result names them.

The rules count events, so "no data" is the healthy state for the first four,
and they keep `noDataState: OK`. A dead pipeline (alloy down, missing grants,
a writer bug) would leave all of them OK forever. The fifth rule catches that
case with `absent_over_time`, which returns a value only when nothing arrived.
Grouping labels that a line lacks become `unspecified`, never an empty
string.

```sh
./apply.sh --dry-run    # prints each request and body; reads no credentials
GRAFANA_CLOUD_URL=https://<stack>.grafana.net GRAFANA_CLOUD_SA_TOKEN=<token> ./apply.sh
```

`GRAFANA_CLOUD_SA_TOKEN` is a Grafana service-account token. The writes need
the Editor role; with a lower role, `apply.sh` stops at the first write and
says so. `apply.sh` passes the token to curl through a mode-0600 header file,
never on the command line.

The Loki data source uid is looked up with `GET /api/datasources`:

- with one data source of type `loki`, that one is used;
- with several, the one named `grafanacloud-*-logs` is used;
- otherwise the script stops and lists the names it found.

`apply.sh` needs `curl` and `python3`.

Thresholds (3, 1 h, 2 h, 30 min) are plain numbers in `fleet-alert-rules.json`.
Edit them there and re-apply.
