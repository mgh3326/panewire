# Control-plane transfer

`POST /v1/control-plane/transfer` moves the authority lane bundle between
machines as one transaction. The two lane routes, the control epoch, and the
idempotency record are replaced in a single `lanes.json` rename, so a partially
transferred bundle is never observable.

## 상태기계

```
ACTIVE --prepare--> PREPARED --commit--> TRANSFERRED_UNACKED --ack--> ACTIVE
ACTIVE --drain--> DRAINING --handover_ready--> HANDOVER_READY --commit--> TRANSFERRED_UNACKED --ack--> ACTIVE
```

`commit` 만 route 를 옮기고 `control_epoch` 를 +1 한다. 나머지 action 은 control
상태만 바꾸고 epoch 는 건드리지 않는다. 일반 레인 `PUT`/`DELETE` 도 epoch 를
증가시키지 않는다(보존만 한다).

`expected_epoch` 가 현재 epoch 와 다르면 409 `stale_epoch`, expected route 가
현재 route 와 다르면 409 `route_mismatch` 다. 같은 `request_id` 를 같은 body 로
재시도하면 저장된 결과를 그대로 돌려주고(전환 0회 추가), 다른 body 로 재시도하면
409 `request_id_conflict` 다. history 는 최근 8건만 보관하므로 그보다 오래된
`request_id` 의 재시도는 `stale_epoch` 로 안전하게 떨어진다.

## A3 readiness

`model_login` · `tools` · `handoffkeep_read` · `hub_read` · `target_pane` 다섯
개가 전부 있고 전부 true 여야 `commit` 이 통과한다. 하나라도 없거나 false 면
409 `target_not_ready` 이고 **route 는 움직이지 않는다**. `target_pane` 은 서버가
독립적으로 다시 확인한다 — 클라이언트가 true 라고 주장해도 target machine 이
hub 가 아는 머신이 아니면 거부된다.

readiness 게이트는 **`commit` 경로에만** 적용된다. 계약(A3)이 요구하는 순서는
"route commit 보다 먼저"이고, `drain`·`handover_ready` 는 target 이 아직 스폰되는
중에 실행되는 단계라 여기에 같은 게이트를 걸면 순서가 뒤집히거나 호출자가 갖고
있지 않은 readiness 를 주장하게 된다. route 를 커밋하는 함수는 readiness 게이트만
만들 수 있는 clearance 값을 파라미터로 요구한다 — 순서는 호출 규율이 아니라
시그니처가 강제한다.

## 권위 레인 직접 쓰기

권위 레인 집합은 운영 설정에서 지정한다(standby 대응·업무 함대 동급 레인 포함).
소스에는 레인 이름을 두지 않는다.

```json
{"authority_lanes": ["<lane>", "<lane>", "<lane>"]}
```

이 파일을 `--control-plane-lanes` 로 지정하면 그 레인들에 대한 직접
`PUT`/`DELETE /v1/lanes/{lane}` 이 409 `authority_lane_direct_write` 로 거부되고,
응답 본문이 대체 경로(`use`)를 알려준다. 파일은 modtime 기반으로 hot-reload 된다.

| 상황 | `authority_lane_protection` | 동작 |
|---|---|---|
| 경로 미지정 | `disabled` | 보호 없음 — 직접 쓰기 거부 0 |
| 지정됨 · 한 번도 로드 성공 못 함 | `invalid` | **fail-closed**: 모든 lane 직접 쓰기를 409 `authority_lane_policy_unavailable` 로 거부 |
| 지정됨 · 로드 후 reload 실패 | `stale` | last-known-good 집합만 거부, 일반 레인 정상 |
| 지정됨 · 로드 성공 | `current` | 설정된 집합만 거부, 일반 레인 정상 |

`invalid` 에서 모든 lane 을 막는 이유는 파일을 못 읽으면 **어느 레인이 권위
레인인지 알 수 없기** 때문이다. 그 상태에서 통과시키면 가드가 없는 것과 같다.

보호가 비활성이면 hub 가 기동 로그에 1줄 남기고(`authority-lane protection
disabled`), `GET /v1/lanes` envelope 의 `authority_lane_protection` 이 `disabled`
로 나온다. 기본값이 비활성인 가드는 꺼진 사실이 보이지 않으면 영원히 꺼져 있다.

`HubServerConfig.ControlPlaneLanes`(Go 주입)는 테스트 seam 이다.
`ControlPlaneLanesPath` 가 설정되면 파일이 우선하고 주입값은 무시된다.

## 배포 시 할 일

배포 절차에 다음 한 단계를 포함한다: `/etc/panewire/control-plane-lanes.json` 을
배치하고 `--control-plane-lanes /etc/panewire/control-plane-lanes.json` 으로 경로를
지정한다. 지정하지 않으면 권위 레인 보호는 비활성이다.

## 읽기 확장

`GET /v1/lanes` envelope 에 `control_epoch` · `control_owner` · `control_state` ·
`last_request_id` · `authority_lane_protection` 이 추가된다. `lanes[]` 원소의
필드는 하나도 바뀌지 않았고 새 read endpoint 도 없다.

## self-check (AC-3b 관측 훅)

```
panewire lanes self-check --lane <lane> --expect-machine <m> --expect-pane <p> --expect-epoch <n>
```

읽기 전용이다 — 아무것도 쓰지 않는다. stdout 에 witness record JSON 한 줄을 내고
exit code 로 판정을 알린다.

| verdict | exit | action |
|---|---|---|
| `owner` | 0 | `proceed` |
| `not_owner` | 3 | `self_stop` |
| `unknown` (레인이 투영에 없거나 투영이 불량) | 5 | `self_stop` |

exit 0 은 "여전히 owner" 일 때만 나온다. 확인할 수 없는 상태는 0 이 아니다.

이것은 강제가 아니라 1회 관측이며, 확인을 건너뛴 세션은 여전히 행동할 수 있다.

self-check 통과를 '차단됨'으로 쓰지 말 것.
`verdict:"not_owner"` 는 세션이 스스로 멈춘 기록이지 서버가 막은 기록이 아니다.

## 잔여 한계

hub API 를 통한 권위 레인 route 변경은 transfer 로 단일화된다. 파일 직접 편집 경로는 남아 있다.
(lanes 파일이 정본이고 hub 가 매 호출 재읽으므로, 호스트에서 파일을 직접 고치는
운영자·installer 의 수동 경로는 그대로 남는다. 이는 A2 보류와 같은 성질의 잔여다.)
