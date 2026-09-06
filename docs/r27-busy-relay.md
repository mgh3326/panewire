# R27 busy-aware relay

노드는 `relay.inject` 직전에 `herdr agent get <pane>`의 `agent_status`를
읽는다. `idle`과 `done`은 즉시 넣고, `working`과 `blocked`는 node-local
SQLite의 `relay_held`로 보류한다. `unknown`, JSON 파싱 실패,
`agent_not_found`, 그리고 명령 실패는 pane 작업을 막아서는 안 되므로
fail-open으로 즉시 넣는다.

정책은 이벤트별 `deliver` 값이다. `now`는 상태 조회 없이 즉시 넣고,
`idle`은 기본 정책이며 최대 1800초를 기다린다. `max_wait=<초>`는 그
이벤트의 한도를 바꾼다. 대기는 폴링이 아니라
`herdr agent wait <pane> --until idle --until done --timeout <ms>` 한 번으로
재무장한다. `--timeout`은 밀리초이므로 초를 ms로 환산한다. `--until`을
명시하지 않으면 herdr 기본값이 `blocked`도 완료로 취급하기 때문에 두
상태만 명시한다. 만료 강제 주입에는 실제 대기 시간을 내림한
`[대기 만료 N분] ` 머리표를 붙인다.

같은 pane에서 해제될 때 보류가 둘 이상이면 수신 순서(`recv_seq`)로
`[batch N건] 1) ... 2) ...` 한 번에 보낸다. UTF-8 바이트 기준 8KB를 넘는
묶음은 순서대로 나누며, 한 항목이 자체로 8KB보다 크면 자르지 않고 단독
주입한다. 한 batch prompt가 성공해도 각 원 이벤트는 별도
`relay.delivered`를 보내므로 durable 행도 각각 닫힌다.

노드는 `relay.held`, `relay.released`, `relay.cancelled`, `relay.batched`를
허브에 보내고 허브는 같은 kind를 `/v1/events`에 브로드캐스트한다.
`relay.released`에는 `final_text`, `edited`, `original_event_id`가 있으므로
편집되지 않은 경우에도 실제 pane 입력을 기록한다.

`GET /v1/relay/held?lane=<lane>`은 노드 보고의 무상태(in-memory) 투영을
돌려준다. `PATCH /v1/relay/events/{id}`는 held 중인 텍스트만 소유 노드에
전달하고, 이미 주입된 이벤트는 409이다. `DELETE /v1/relay/events/{id}`는
취소를 소유 노드에 전달한다. 셋 모두 operator Bearer 인증을 요구한다.
허브는 held 본문을 저장하지 않는다.

`relay_held`는 `pane, lane, event_id, job_id, text, held_since,
deliver_policy, max_wait, recv_seq`와 편집 여부를 노드에만 저장한다. 재시작
시 이를 복원해 wait를 다시 건다. 허브의 undelivered 재생과 동시에 와도
`(lane,event_id)`로 중복을 막아 한 번만 주입한다.

handoffkeep 행의 `text`는 발신 원본이고, 실제 주입된 텍스트의 정본은
`relay.released` 이벤트다. handoffkeep에는 갱신 API가 없으므로 held 편집본을
그 정본 행에 반영할 수 없다.

취소는 `pane="cancelled"` 센티널로 delivered 마킹해 재생에서 제외한다.
이 값은 pane이 아니며 노드는 절대 pane으로 해석하지 않는다. handoffkeep의
명시적 cancel 상태는 후속 v8 additive 작업으로 남긴다.
