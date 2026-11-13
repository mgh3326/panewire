# 권위 레인 보호 (authority-lane guard)

권위 레인은 hub 의 `lanes.json` 에서 제어 권한을 가진 레인이다. 이 문서는 그 레인을
보호하는 가드, 배포 설정, 읽기 확장, 그리고 self-check 관측 훅을 설명한다.

> **transfer 제거 (hk 4442).** 예전에는 `POST /v1/control-plane/transfer` 가 권위
> 레인 묶음을 한 트랜잭션으로 옮겼다. 이 endpoint 와 그 상태기계, A3 readiness 게이트,
> 묶음 일치 검사는 제거되었다. 지금 hub API 로 권위 레인의 route 를 바꾸는 경로는 없다.
> 이 경로는 404 다.

## 권위 레인 직접 쓰기

권위 레인 집합은 운영 설정에서 지정한다(standby 대응·업무 함대 동급 레인 포함).
소스에는 레인 이름을 두지 않는다.

```json
{"authority_lanes": ["<lane>", "<lane>", "<lane>"]}
```

이 파일을 `--control-plane-lanes` 로 지정하면 그 레인들에 대한 직접
`PUT`/`DELETE /v1/lanes/{lane}` 이 409 `authority_lane_direct_write` 로 거부되고,
응답 본문의 `use` 가 다음 단계를 알려준다: 호스트에서 lanes 파일을 직접 고친다.
파일은 modtime 기반으로 hot-reload 된다.

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

## 제어 블록과 읽기 확장

lanes 파일의 `control` 블록(`epoch`, `owner`, `state`, `last_request_id`,
`handover_doc_key`, `history`)은 hub 가 읽고 검증한다. 유효하지 않은 블록은 쓰기 경로에서
덮어쓰지 않는다. 일반 레인의 `PUT`/`DELETE` 는 이 블록을 그대로 보존한다.

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
`authority_lane_protection` 이 `invalid` 면 route 와 epoch 가 기대값과 같아 보여도 `unknown`
으로 간다 — hub 가 검증하지 못한다고 말한 control 상태에서 나온 값이기 때문이다. `stale` 은
last-known-good 집합이 있고 control 블록 자체는 정상이므로 평소대로 판정한다.

이것은 강제가 아니라 1회 관측이며, 확인을 건너뛴 세션은 여전히 행동할 수 있다.

self-check 통과를 '차단됨'으로 쓰지 말 것.
`verdict:"not_owner"` 는 세션이 스스로 멈춘 기록이지 서버가 막은 기록이 아니다.

## 잔여 한계

hub 는 더 이상 control 블록을 쓰지 않는다. 권위 레인의 route 와 owner 를 바꾸는 방법은
호스트의 lanes 파일을 직접 고치는 운영자 절차뿐이다. 이 경로는 hub API 가 아니므로
가드가 막지 않는다. lanes 파일이 정본이고 hub 가 매 호출 재읽으므로 이 수동 경로는
그대로 남는다.
