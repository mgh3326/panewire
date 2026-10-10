# Assistant MCP surface (panewire-assistant)

panewire-assistant is the thin, stateless read+write surface the external
assistant (berry) uses to see decision work and apply operator answers
(MGH-36 PR-2 + PR-3). It is a separate binary in the panewire repo
(cmd/panewire-assistant), speaks MCP as JSON-RPC 2.0 over streamable HTTP
on a single POST /mcp endpoint, and holds no durable state of its own —
every read is proxied to handoffkeep at call time and every write is
durable in handoffkeep or the hub before the call returns, so the process
can be killed and restarted at any moment without losing or duplicating
anything.

## The polling contract

This server never pushes. Being registered only means the tools can be
called; nothing is delivered unless you call. Missing a poll means missing
a deadline. A deadline passing is never consent.

There is no subscription, no SSE stream, no callback. The poll tool returns
a snapshot keyed by `<stable id>:<revision>` with the handoffkeep
server_time so the consumer can dedupe and drop stale items; liveness is
the consumer's responsibility. Write calls likewise push nothing back:
answer tools return the applied outcome and a lane notice drains
asynchronously through the outbox drainer.

## Tools

### Read tools (always listed)

- targets — list the operator-verified delivery targets as opaque ids. A
  caller references a target only by the opaque id; unknown ids fail
  closed. No lane, URL, route or shell string is ever accepted as input.
- pending_list — every open decision request and pending chat question
  from handoffkeep GET /v1/assistant/pending that falls inside the targets
  allowlist, each with stable id (`dr-<task>-<revision>`, or the Q id),
  revision, human_only, status and the server clock. A decision request on
  an unmapped lane and a question outside every mapped lane or
  conversation never appear — the same scope pending_detail and progress
  enforce; a mapped conversation target keeps exposing that conversation's
  questions. Merged or dropped tasks never appear — filtered upstream and
  again at this trust boundary. The response carries
  chat_questions_at_cap=true when the question list sits at handoffkeep's
  1000-row cap, meaning the list may be truncated.
- pending_detail — one open item by stable id. Only items still open on an
  allowlisted lane or conversation answer: a decision request must be the
  task's live open request on a non-terminal mapped lane, a question must
  still be pending on a mapped lane or conversation. Every other id —
  unknown, stale, superseded, settled, merged, dropped or out of scope —
  fails closed with the same request_not_found, so the tool cannot be used
  as an existence oracle. Answers carry server_time.
- progress — the state of one opaque target id or one request id, in the
  closed vocabulary accepted, delivered, in-progress, decision-pending,
  done, failed, derived only from handoffkeep fields (task state, decision
  resolution, relay delivered_at/delivered_to/attempts, outbox
  sent_at/hub_row_id where visible) with the receipts that justify it. For
  a lane or conversation target only unanswered items count as
  decision-pending: an answered-but-pending question follows the same
  notice chain a request id reports, folded across the target's answered
  set. A relay row that is only persisted is never done; a delivered_to
  value counts as delivery only when it names a `<machine>/<pane>` the
  hub's pane loader could accept — the retired, cancelled and
  chat-terminal stamps (resolve/*, hub/*, cancellation and replay stamps)
  and the sink stamp all report failed, with sink/* additionally carrying
  reason sink_lane. A view that reaches its read bound (a tasks walk that
  hits its page bound, a capped pending or outbox list, a relay walk at
  its page bound) can never report done or delivered — it reports
  in-progress with reason view_truncated. Request ids are scoped to the
  allowlist the same way pending_detail is.
- poll — the dedupe snapshot described above; carries the same at-cap
  indicator pending_list does.

### Write tools (listed only when enabled)

PANEWIRE_ASSISTANT_WRITES selects the enabled subset: `answer` lists
answer_decision + answer_question, `deliver` lists deliver, `all` lists
all three, and anything else (including unset) lists none. A tool not in
the set is absent from tools/list and a call to it fails closed with
writes_disabled before the targets file is even read. Write calls are
rate-limited per caller identity (token bucket; see configuration).

- answer_decision — apply the operator's answer to one open decision
  request. Input: `request_id` (dr-<task>-<revision> exactly as
  pending_list shows it), `option` (an option key), `text` (the owner's
  quoted answer text; ≤1000 bytes). The call resolves the task, enforces
  the same allowlist scope the read tools use (missing and out-of-scope
  ids answer the same generic request_not_found), and posts
  POST /v1/assistant/decisions/resolve with kind answered. Attribution is
  pinned server-side: handoffkeep records responder operator-via-berry and
  the by identity itself — the request schema has no field that could
  carry them. Repeating the identical answer is a duplicate success
  (`duplicate: true`) with no new effect; a moved revision answers
  decision_request_stale; a human_only request answers
  decision_request_human_only; a disposition item answers
  disposition_operator_only; a request resolved differently answers
  decision_request_resolved. The returned `event_id` is the deterministic
  `<request-id>-answered` the lane notice will carry.
- answer_question — answer one pending chat question. Input: `id` (the Q
  id), `revision` (the expected revision), `text` (≤4096 bytes). The
  question row is pre-read first: an already-held answer slot answers
  question_slot_taken (the slot is single-use — do not retry), a
  non-pending or out-of-scope question answers request_not_found, a
  revision mismatch answers stale_revision carrying `current_revision`.
  The answer then posts through handoffkeep's assistant chat channel —
  POST /v1/chat/messages with source_channel=assistant, author operator
  and the expected-revision compare-and-swap — never through the hub chat
  route and never as source_channel web. A CAS race answers the same
  stale_revision; a same-origin replay dedupes to `duplicate: true`. The
  deterministic notice event id is `<question-id>-rev<revision>-answered`.
- deliver — inject one instruction into a target's operator-mapped lane as
  a hub lane.event. Input: `target` (an opaque id from the targets file),
  `idempotency_key` (`^[A-Za-z0-9._-]{8,64}$`), `text` (trimmed non-empty,
  ≤2036 bytes, valid UTF-8, no control characters — no newlines — and
  never starting with `[`, so a forged tag cannot ride the prefix; over-
  or mis-shaped text is rejected, never truncated). The lane comes only
  from the targets file: a lane target delivers to its own lane, a
  conversation target to its mapped `deliver_lane`. The event id is
  `berry:<target>:<key>` — deterministic, never random, never time-based —
  and the text is posted prefixed `[via berry]` with the fixed hub label
  panewire-assistant. The POST is the dedupe probe: a 201 is a new durable
  row; a 409 naming a row id >0 fetches the stored row and compares
  byte-for-byte — equal text is `duplicate: true`, different text is
  idempotency_conflict with nothing sent, a row the bounded relay walk
  cannot verify is duplicate_unverified (call progress; never mint a new
  key); a 409 with id 0 is the in-flight window, answering
  in_flight_retry (retry the same key). A conversation target without a
  deliver_lane, or a mapped lane the hub's label rule rejects, answers
  target_not_deliverable. There is no hub POST /chat/messages call: that
  route hardcodes web/operator attribution.

## The outbox drainer

Writes that owe a lane notice (an applied decision or chat answer) are
recorded by handoffkeep into its notification_outbox; this binary's
drainer is what turns them into lane text. It is stateless and
crash-safe:

1. Read GET /v1/assistant/outbox — the unsent rows.
2. Transform each row's notice text deterministically: chat-answer rows
   get their revision injected (`Q-… rev<n>`) and every text passes a
   deterministic sanitizer (control characters become spaces, then the
   same UTF-8-safe 2048-byte "…" re-cap handoffkeep applies) so two
   drainer instances produce byte-identical output for the same row.
3. POST the row to hub /v1/relay/events as a lane.event to the row's
   target_lane with the row's own event_id — never a new id.
4. Mark it sent via POST /v1/assistant/outbox/sent with the hub row id —
   only on a real receipt: a 201 with id >0, or a 409 duplicate naming an
   existing id >0. An id of 0 or missing is never a receipt and is never
   marked; a mark-sent 409 (a racing drainer already recorded a different
   hub id) counts the row sent and is never re-posted; a hub failure logs
   once and the pass moves on — one poison row cannot stall the rest.

The drain runs one pass at startup, one per interval ±25% jitter
(PANEWIRE_ASSISTANT_DRAIN_INTERVAL, default 30s, bounded 1s–1h), and one
kicked pass coalesced per burst of writes — the kick never blocks the
write call and a drain failure never reaches it. Passes are serialized
inside a process. A crash anywhere is recoverable: the row stays owed in
handoffkeep, the retry reuses the same event id, and hub dedupe makes the
re-post a no-op.

## Caller-facing identity and errors

Every request needs the berry bearer token (Authorization: Bearer …),
read from a mode-0600 file and compared in constant time. When the
optional Cloudflare Access gate is configured, a verified Access JWT is
additionally required and its common_name must be on the configured
service-name allowlist — the signature alone never suffices, which is the
identity binding the hub's verifier deliberately lacks.

Tool failures return named errors (unknown_target, request_not_found,
targets_file_invalid, invalid_arguments, writes_disabled, rate_limited,
hk_unreachable, hk_rejected_http_*, hk_response_*, plus the write names
above). Resolution text, receipt text and resolver identity are
human-channel data: the resolution receipt carries only kind, responder
and at. No error string, tool result, log line or redirect ever carries a
token, a CF secret, URL userinfo or a redirect Location — upstream
validation errors that could quote caller input collapse onto
invalid_answer.

## Server-side configuration

One mode-0600 env file (-config):

- HANDOFFKEEP_URL, HANDOFFKEEP_TOKEN — the handoffkeep credential;
  required.
- HANDOFFKEEP_CF_ACCESS_CLIENT_ID / _SECRET — optional outbound Access
  service-token pair for an hk URL behind Access; all-or-nothing,
  attached only to the configured origin, and redirects are never
  followed.
- PANEWIRE_ASSISTANT_HUB_URL / _TOKEN — the hub operator credential the
  deliver tool and the drainer post lane.events with; required together
  or neither. Without them the binary still serves reads and answers
  (the drainer and deliver are inert) — writes that enable deliver or
  leave rows owed need the pair.
- PANEWIRE_ASSISTANT_WRITES — off/answer/deliver/all; unset means off.
- PANEWIRE_ASSISTANT_DRAIN_INTERVAL — drain period, default 30s, bounded
  1s–1h.
- PANEWIRE_ASSISTANT_WRITE_RATE_PER_MIN — sustained write limit per
  caller identity, default 10.
- PANEWIRE_ASSISTANT_WRITE_BURST — per-identity burst, default 5.
- PANEWIRE_ASSISTANT_TOKEN_FILE — the berry bearer token file (mode
  0600). The token is read once at startup and held hashed in memory, so
  rotating it requires a process restart (the binary is stateless;
  restart is free).
- PANEWIRE_ASSISTANT_TARGETS_FILE — the targets mapping (mode 0600); see
  below.
- PANEWIRE_ASSISTANT_LISTEN — bind address; default 127.0.0.1:9471.
- PANEWIRE_ASSISTANT_CLIENT_NAME — audit label for bearer-only requests.
- PANEWIRE_ASSISTANT_CF_TEAM, PANEWIRE_ASSISTANT_CF_AUD,
  PANEWIRE_ASSISTANT_CF_SERVICE_NAMES — the optional inbound Access
  gate; all-or-nothing, comma-separated common_name allowlist.
- PANEWIRE_ASSISTANT_CF_CERTS_URL — optional certs endpoint override for
  the inbound Access verifier.

Every request is audit-logged (service identity, RPC/tool, target or
request id, result). Every write attempt additionally logs the tool,
subject id, idempotency key or event id, revision, outcome class, hub row
id and a sha-256/8 text hash — never the text itself, a token or an
upstream URL.

## Targets file

Operator-edited JSON, mode 0600, loaded and re-validated before every
tool call — a chmod, a symlink swap or a broken edit makes every tool,
not just the target-taking ones, fail closed with targets_file_invalid:

```json
{"targets": {
  "ops-lane": {"kind": "lane", "lane": "ops", "description": "ops desk lane"},
  "desk-42": {"kind": "conversation", "conversation": "desk:42", "deliver_lane": "ops", "description": "desk 42 thread"}
}}
```

kind is "lane" or "conversation"; each id must match
^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$. `deliver_lane` is the operator-mapped
lane a conversation target's deliver call posts to — conversation rows
carry no lane of their own, so without one the target is
target_not_deliverable — and it must spell a lane the hub's agent-label
rule accepts, or the file itself is invalid. Lane targets never carry
deliver_lane (their own lane is the route). The file is the whole routing
truth: the mapping is opaque to callers and is the only way a tool
reaches a lane or conversation.

## Endpoints used

The binary dials exactly these and nothing else — hk through one
credential-bound client path, hub through a second, both with the same
no-redirect, body-capped, no-secret-in-errors rules:

handoffkeep:

- GET /v1/assistant/pending — the pending list, and the only endpoint
  that carries server_time (pending_detail borrows its clock).
- GET /v1/assistant/outbox — the unsent notice rows (read-tool receipts
  and the drainer's work list).
- POST /v1/assistant/outbox/sent — the drainer's mark-sent.
- POST /v1/assistant/decisions/resolve — answer_decision's only write.
- GET /v1/chat/questions/{id} — resolve a Q id for scope (read tools) and
  the answer_question pre-read.
- POST /v1/chat/messages — answer_question's assistant-channel answer
  post.
- GET /v1/tasks/{id} — resolve a `dr-<task>-<revision>` id to its task.
- GET /v1/tasks?lane=…&after_id=… — the lane live-task walk inside
  progress(target), paged by after_id to a fixed bound.
- GET /v1/relay/events — the only relay evidence. It lists oldest-first
  by id with lane/undelivered/after_id/limit filters; there is no
  event-id lookup and no newest-first order, so relay walks are bounded
  (8 × 500 rows per call) and a walk that reaches its bound reports
  view_truncated rather than claiming a settled lane. A first-class
  event-id or DESC endpoint in handoffkeep would retire that bound;
  until then it is a known gap.

hub:

- POST /v1/relay/events — the deliver tool's injection and the drainer's
  notice post.

Known gap: the outbox list endpoint returns only the oldest ≤1000 unsent
rows and has no cursor, so a drain pass re-reads that head window until
it sees nothing new — more than a thousand simultaneously-owed rows wait
for the head to clear; and only unsent rows appear, so a drained
notification's receipt is read through the relay row it became (same
event_id). When neither is visible yet the state reports in-progress
with reason notice_row_not_visible rather than done.

## Hub single notice channel

Chat rows stored in handoffkeep with source_channel=assistant are the
assistant answer path. The hub never relays them as chat-{id} relay
rows, the orphan sweep never marks them failed (they count as settled
for its cursor), and the console refuses retry and cancel on them — the
handoffkeep notification_outbox (drained by this binary) is their only
lane notice. The chat page renders them labelled 어시스턴트 with an
assistant badge instead of the operator delivery state. Human operator
and desk chat behavior is unchanged.
