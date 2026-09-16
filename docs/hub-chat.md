# Operator chat hub (`/chat`)

The operator chat screen lets the operator read durable desk-session questions
and answer them once from a browser. Persistence lives in handoffkeep's
`/v1/chat/*` API (the store is the authority for the state vocabulary:
questions `pending → resolved|withdrawn`, messages `stored → delivered|failed`);
the hub owns authentication, CSRF, and delivery into the existing lane.event
relay path.

## Endpoints

- `GET /chat` — page. `GET /chat/data` — question list + message timeline.
  `POST /chat/messages` — operator answer (`{lane, body, question_id?}`).
  `POST /chat/messages/{id}/retry` — re-send a `failed` row's body as a new
  row (`question_id` may be re-supplied so the resent answer still resolves
  its question). `POST /chat/messages/{id}/cancel` — close out a `stored` row.
  `POST /chat/questions/{id}/transition` — `resolved|withdrawn`.
  All of these sit behind `authorizeUI` (the same Cloudflare Access / loopback
  gate as `/ui`); POSTs additionally require a same-origin request: `Origin`,
  when present, must match the request host, and `Sec-Fetch-Site` must be
  `same-origin`/`none`. Cross-origin POSTs get 403.
- `POST /v1/chat/questions` — the desk-session hook path, authenticated with
  the operator bearer token like every other `/v1` API.

The browser bundle carries no token of any kind. The hub never holds the
operator token in the page; the hook path is for desk sessions, not browsers.

## Delivery flow

`POST /chat/messages` stores the row first (`relay_state=stored`), then returns;
a dispatcher (started with `RunMaintenance`, kicked per POST, 5s backstop tick)
relays through the existing `lane.event` machinery with text `[chat] <body>`
and marks the row `delivered` only when the relay routed it. Every other
outcome marks `failed`. Because handoffkeep's message rows carry no lane, the
delivery target is hub-memory until the attempt settles; a stored row whose
lane mapping was lost to a restart is failed by the orphan sweep after a grace
period so it never displays as "전송 중" forever.

Answers longer than the non-sink 2048-byte lane limit reach the lane as the
reference line `[chat] 긴 메시지 id=<n>`; the full body is read back from the
store. Relay text is a single-line rendering (control characters become
spaces); the store keeps the original body.

## Retry and cancel

handoffkeep's message state machine makes `failed` a sink — only `stored` rows
can transition. Retry therefore records the same body as a **new** row and
relays it; the failed row stays as the permanent failure record. Cancel
transitions `stored → delivered`, the only retention-terminal message state,
so a cancelled row enters the daily retention prune instead of accumulating
forever. Cancel of a `failed` row is answered with 409 `chat_message_terminal`:
the store has already closed it. Cancel of a `delivered` row is idempotent.

## Failure isolation

Chat runs behind the `ChatStore` interface with its own HTTP client (5s
timeout, 8-connection pool cap). No chat call is ever made with `h.mu` held,
every chat handler recovers panics into a 500, and the dispatcher recovers per
drain. A dead chat store yields 503 on chat endpoints and changes nothing for
`/v1/relay/events` or hub startup. `--chat-env` (mode-0600
`HANDOFFKEEP_URL`/`HANDOFFKEEP_TOKEN`, defaulting to `--handoffkeep-env`)
keeps the chat failure domain separable from relay persistence ahead of any
later storage split.
