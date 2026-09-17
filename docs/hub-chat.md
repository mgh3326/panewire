# Operator chat hub (`/chat`)

The operator chat screen lets the operator read durable desk-session questions
and answer them once from a browser. Persistence lives in handoffkeep's
`/v1/chat/*` API (the store is the authority for the state vocabulary:
questions `pending → resolved|withdrawn`, messages `stored → delivered|failed`);
the hub owns authentication, CSRF, and delivery into the existing lane.event
relay path.

## Endpoints

- `GET /chat` — page. `GET /chat/data` — pending questions + a bounded recent
  tail of resolved/withdrawn questions, plus the message timeline.
  `POST /chat/messages` — operator answer (`{lane, body, question_id?}`).
  `POST /chat/messages/{id}/retry` — re-send a `failed` row as a new row using
  the lane and question recorded server-side when the original was created;
  client-supplied lane/question values are ignored.
  `POST /chat/messages/{id}/cancel` — close out a `stored` row.
  `POST /chat/questions/{id}/transition` — `resolved|withdrawn`.
  All of these sit behind `authorizeChat`: a request must carry either the
  operator bearer token or a `Cf-Access-Jwt-Assertion` that verifies against
  the configured Cloudflare Access team (RS256 signature against the team
  certs, `aud` match, unexpired). Unsigned identity headers
  (`Cf-Access-Authenticated-User-Email`, `Cf-Ray`, `Cf-Connecting-Ip`) grant
  nothing, and loopback alone is not trusted — local processes are not
  trusted. POSTs additionally require a same-origin request: `Origin`, when
  present, must match the request host, and `Sec-Fetch-Site` must be
  `same-origin`/`none`. Cross-origin POSTs get 403; unauthenticated requests
  get 404. Browser deployments therefore need `--cf-access-team` and
  `--cf-access-aud`; without them only the operator token opens `/chat`. The
  Access application must protect `/chat*` as well as `/ui*` — one
  application's AUD covers all its paths, so no second `--cf-access-aud` is
  needed, but a `/ui*`-only application issues no JWT on `/chat` and every
  browser request there fails closed.
  Key verification is fail closed and never on the request path: fetched
  certs are cached for an hour and re-fetched by a background loop (proactive
  refresh plus a kick whenever a request sees a missing/stale key). A request
  that finds no usable cached key is rejected — it never waits on the
  network — so a certs/DNS outage cannot stall requests, only reject them
  until the background refresh lands keys again.
- `POST /v1/chat/questions` — the desk-session hook path, authenticated with
  the operator bearer token like every other `/v1` API. Unchanged.

The browser bundle carries no token of any kind. The hub never puts the
operator token in the page; the hook path is for desk sessions, not browsers.

## Delivery flow

`POST /chat/messages` stores the row first (`relay_state=stored`), then
returns; a dispatcher (started with `RunMaintenance`, kicked per POST, 5s
backstop tick) persists a durable handoffkeep `relay_events` row and relays
through the existing `lane.event` machinery with text `[chat] <body>`.

If no destination node is connected, the durable relay row is the queue: the
chat row stays `stored` and the normal replay path injects it when the node
registers, then marks the chat row `delivered`. Replay is bounded
(`relayReplayMaxAttempts`); exhaustion marks the chat row `failed`. A `failed`
chat row is never injected — replay retires its stale relay row instead. The
relay `event_id` embeds a hash of the message body (`chat-<id>-<hash8>`) so a
retried row can never collide with a different body under handoffkeep's
`(owner_lane, event_id)` idempotency.

Answers longer than the non-sink 2048-byte lane limit reach the lane as the
reference line `[chat] 긴 메시지 id=<n>`; the full body is read back from the
store. Relay text is a single-line rendering (control characters become
spaces); the store keeps the original body.

## Retry and cancel

handoffkeep's message state machine makes `failed` a sink — only `stored` rows
can transition. Retry therefore records the same body as a **new** row bound
to the original lane and question (both recorded server-side at create time),
and relays it; the failed row stays as the permanent failure record. A retried
answer resolves its original question on delivery regardless of which question
is selected in the UI.

Cancel transitions `stored → delivered`, the only retention-terminal message
state, and retires the pending durable relay row (marked delivered with a
`chat-cancel` note) so it is never injected later. A cancel that lands while
the dispatcher is mid-relay is still safe: the inject step re-checks the chat
row's state and refuses once the row has closed. The UI marks the row
"취소됨" through a hub-side flag rather than showing a delivery that never
happened (the flag is hub-memory; after a restart a cancelled row shows as
delivered again — the store has no fourth state). Cancel of a `failed` row
is answered with 409 `chat_message_terminal`: the store has already closed
it. Cancel of a `delivered` row is idempotent.

## Failure isolation

Chat runs behind the `ChatStore` interface with its own HTTP client (5s
timeout, 8-connection pool cap). No chat call is ever made with `h.mu` held,
every chat handler recovers panics into a 500, and the dispatcher recovers per
drain. A dead chat store yields 503 on chat endpoints and changes nothing for
`/v1/relay/events` or hub startup. `--chat-env` (mode-0600
`HANDOFFKEEP_URL`/`HANDOFFKEEP_TOKEN`, defaulting to `--handoffkeep-env`)
keeps the chat failure domain separable from relay persistence ahead of any
later storage split.
