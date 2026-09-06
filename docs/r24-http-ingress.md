# R24 hub HTTP ingress

`POST /v1/relay/events` is the hub's only producer-node-free path for an
operator web console to put a decision or requirement into a destination lane
pane. It requires the operator Bearer token.

## Contract

`POST /v1/relay/events` — hub, operator token Bearer.

Request JSON (v1 contract):

```json
{"kind":"lane.event","lane":"<destination lane>","event_id":"<producer id, required>","text":"<≤2048B; sink lane ≤8192B>","label":"<producer label, e.g. operator-web>","host":"<optional, default hub>"}
```

### Contract text (verbatim)

```json
{"kind":"lane.event","lane":"<목적 레인>","event_id":"<producer id, 필수>",
 "text":"<≤2048B; sink 레인은 ≤8192B>","label":"<producer 라벨, 예 operator-web>","host":"<선택, 기본 hub>"}
```

`kind` 는 v1에서 `lane.event` 만. 다른 값은 400.

처리 순서: lanes.json 해석 → handoffkeep 영속(kind `lane.event`, **owner_lane = 목적 레인**,
**reason = `http_ingress:<label>`**, machine = host, pane_id = `""`) → 목적 pane 주입
(기존 inject·delivered·attempts 기계 그대로; 미등록/미접속 레인이면 undelivered 영속만).
**producer 노드가 없으므로 `relay.persisted` ack 없음.**

dedupe = `(lane, event_id)`. 중복은 **409**
`{"error":"duplicate_event_id","id":<기존 id>}`.

응답 201:
`{"id":<handoffkeep row id>,"event_id":…,"lane":…,"routed":true|false,"machine":…}`.

오류: 400(스키마/크기/미지원 kind), 401(토큰), 409(중복), 502(handoffkeep 영속 실패 —
**이 경우 주입도 하지 않는다**: 영속 우선 원칙).

운영자 피드 `/v1/events` 에 `relay.http_ingress` 브로드캐스트(`label`·`lane`·`event_id`·`routed`).

`kind` is `lane.event` only in v1; every other value is `400`.

Processing order is fixed, verbatim in its material fields:

1. `lanes.json` 해석.
2. handoffkeep 영속: `kind="lane.event"`, `owner_lane=<목적 레인>`,
   `reason="http_ingress:<label>"`, `machine=<host>`, `pane_id=""`.
3. 목적 pane 주입: 기존 inject·delivered·attempts 기계를 그대로 쓴다.
   미등록 또는 미접속 레인은 undelivered 영속만 한다.

There is no `relay.persisted` acknowledgement: HTTP ingress has no producer
node. The handoffkeep deployment must provide schema v7.

The dedupe key is `(lane, event_id)`. A duplicate returns `409`:

```json
{"error":"duplicate_event_id","id":123}
```

A new row returns `201`:

```json
{"id":123,"event_id":"producer-42","lane":"lane-a","routed":true,"machine":"host-a"}
```

`machine` is the injection target: the route machine for a pane, `"sink"` for
a sink, and `""` when unrouted or when its injection queue is full. It pairs
with `routed`.

Error codes are: `400` for schema, size, or unsupported kind; `401` for a
missing or invalid operator token; `409` for dedupe; and `502` when handoffkeep
persistence fails. On `502`, the hub does not inject: persistence always precedes delivery.
The same `502` plus `relay.unpersisted` behavior applies if the hub has no
handoffkeep configuration.

Only successful `201` requests broadcast `relay.http_ingress` on `/v1/events`.
Its payload includes `label`, `lane`, `event_id`, and `routed`. The `400`,
`401`, `409`, and `502` paths do not broadcast that event.

## Producer example

```sh
curl --fail-with-body \
  -X POST http://hub:8080/v1/relay/events \
  -H 'Authorization: Bearer <operator-token>' \
  -H 'Content-Type: application/json' \
  --data '{"kind":"lane.event","lane":"lane-a","event_id":"producer-42","text":"approval requested","label":"operator-web","host":"host-a"}'
```

The host is optional; omitting it persists `machine="hub"`.

## Deliberate HTTP decisions

a. HTTP ingress never truncates. Node producers truncate once before sending;
the hub validates their final text. Here there is no producer node and the web
form knows the limit, so an over-limit request is rejected with `400` and the
existing `relay.rejected` event with `reason="text_too_long"`. The limit is
route-based: `8192B` for a sink, and `2048B` for every other route, including
an unregistered lane. This keeps the durable row and injected text identical
without inventing a second truncation point.

b. The `machine` response field is the resolved pane machine, `"sink"`, or
`""` for an unrouted or queue-failed injection, as described above.

c. A hub with `h.handoffkeep == nil` responds `502`, broadcasts
`relay.unpersisted`, and performs zero injection.

d. Concurrent requests with the same `(lane, event_id)` reserve one active
claim. If the first is still persisting and has no durable ID yet, the second
returns `409` with `"id":0`; retrying it after the first completes returns the
durable ID.

e. After a hub restart its memory is empty, so the first repeated POST reaches
handoffkeep once. A `200` existing-row reply becomes `409` with that row ID and
increments handoffkeep `attempts`, the same resend POST cost documented for the
node path. The hub records that durable ID for future local duplicates, does
not inject on this `200` path, and releases the active relay claim so ordinary
undelivered replay can inject the row later.

`reason='http_ingress:<label>'` is the durable source mapping for all HTTP
ingress rows.
