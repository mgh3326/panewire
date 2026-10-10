# Assistant MCP surface (panewire-assistant)

panewire-assistant is the thin, stateless read surface the external assistant
(berry) uses to see decision work (MGH-36 PR-2). It is a separate binary in
the panewire repo (cmd/panewire-assistant), speaks MCP as JSON-RPC 2.0 over
streamable HTTP on a single POST /mcp endpoint, and holds no durable state of
its own — every read is proxied to handoffkeep at call time, so the process
can be killed and restarted at any moment without losing or duplicating
anything.

## The polling contract

This server never pushes. Being registered only means the tools can be
called; nothing is delivered unless you call. Missing a poll means missing a
deadline. A deadline passing is never consent.

There is no subscription, no SSE stream, no callback. The poll tool returns a
snapshot keyed by `<stable id>:<revision>` with the handoffkeep server_time so
the consumer can dedupe and drop stale items; liveness is the consumer's
responsibility.

## Tools

Read-only. There is no write tool: no deliver, no answer, no resolve, no
outbox mark-sent. PR-3 adds the write surface.

- targets — list the operator-verified delivery targets as opaque ids. A
  caller references a target only by the opaque id; unknown ids fail closed.
  No lane, URL, route or shell string is ever accepted as input.
- pending_list — every open decision request and pending chat question from
  handoffkeep GET /v1/assistant/pending that falls inside the targets
  allowlist, each with stable id (`dr-<task>-<revision>`, or the Q id),
  revision, human_only, status and the server clock. A decision request on
  an unmapped lane and a question outside every mapped lane or conversation
  never appear — the same scope pending_detail and progress enforce; a
  mapped conversation target keeps exposing that conversation's questions.
  Merged or dropped tasks never appear — filtered upstream and again at
  this trust boundary. The response carries
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
  sent_at/hub_row_id where visible) with the receipts that justify it. For a
  lane or conversation target only unanswered items count as
  decision-pending: an answered-but-pending question follows the same
  notice chain a request id reports, folded across the target's answered
  set. A relay row that is only persisted is never done; a delivered_to value
  counts as delivery only when it names a real `<machine>/<pane>` — the
  retired, cancelled and chat-terminal stamps and the sink stamp all report
  failed, with sink/* additionally carrying reason sink_lane. A view that
  reaches its read bound (a full tasks page, a capped pending or outbox
  list, a relay walk at its page bound) can never report done or delivered —
  it reports in-progress with reason view_truncated. Request ids are scoped
  to the allowlist the same way pending_detail is.
- poll — the dedupe snapshot described above.

## Caller-facing identity and errors

Every request needs the berry bearer token (Authorization: Bearer …), read
from a mode-0600 file and compared in constant time. When the optional
Cloudflare Access gate is configured, a verified Access JWT is additionally
required and its common_name must be on the configured service-name
allowlist — the signature alone never suffices, which is the identity
binding the hub's verifier deliberately lacks.

Tool failures return named errors (unknown_target, request_not_found,
targets_file_invalid, invalid_arguments, hk_unreachable,
hk_rejected_http_*, hk_response_*). Resolution text, receipt text and resolver identity
are human-channel data: the resolution receipt carries only kind, responder
and at. No error string, tool result, log line or redirect ever
carries a token, a CF secret, URL userinfo or a redirect Location.

## Server-side configuration

One mode-0600 env file (-config):

- HANDOFFKEEP_URL, HANDOFFKEEP_TOKEN — read credentials; required.
- HANDOFFKEEP_CF_ACCESS_CLIENT_ID / _SECRET — optional outbound Access
  service-token pair for an hk URL behind Access; all-or-nothing, attached
  only to the configured origin, and redirects are never followed.
- PANEWIRE_ASSISTANT_TOKEN_FILE — the berry bearer token file (mode 0600).
  The token is read once at startup and held hashed in memory, so rotating
  it requires a process restart (the binary is stateless; restart is free).
- PANEWIRE_ASSISTANT_TARGETS_FILE — the targets mapping (mode 0600); see
  below.
- PANEWIRE_ASSISTANT_LISTEN — bind address; default 127.0.0.1:9471.
- PANEWIRE_ASSISTANT_CLIENT_NAME — audit label for bearer-only requests.
- PANEWIRE_ASSISTANT_CF_TEAM, PANEWIRE_ASSISTANT_CF_AUD,
  PANEWIRE_ASSISTANT_CF_SERVICE_NAMES — the optional inbound Access gate;
  all-or-nothing, comma-separated common_name allowlist.
- PANEWIRE_ASSISTANT_HUB_URL / _TOKEN — reserved for the PR-3 outbox drain;
  validated in pairs but never dialed by this binary.

Every request is audit-logged (service identity, RPC/tool, target or request
id, result). Tokens and secrets are never logged.

## Targets file

Operator-edited JSON, mode 0600, loaded and re-validated before every tool
call — a chmod, a symlink swap or a broken edit makes every tool, not just
the target-taking ones, fail closed with targets_file_invalid:

```json
{"targets": {"ops-lane": {"kind": "lane", "lane": "ops", "description": "ops desk lane"}}}
```

kind is "lane" or "conversation"; each id must match
^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$. The file is the whole routing truth: the
mapping is opaque to callers and is the only way a tool reaches a lane or
conversation.

## Handoffkeep endpoints used

GET only, each retained for exactly one job:

- /v1/assistant/pending — the pending list, and the only endpoint that
  carries server_time (pending_detail borrows its clock).
- /v1/assistant/outbox — the unsent-notice receipt for the assistant answer
  chain.
- /v1/tasks/{id} — resolve a `dr-<task>-<revision>` id to its task so the
  lane can be checked against the allowlist (progress) and the open-item
  rule enforced (pending_detail).
- /v1/tasks?lane=… — the lane live-task check inside progress(target).
- /v1/chat/questions/{id} — resolve a Q id to its question so the lane or
  conversation can be checked against the allowlist.
- /v1/relay/events — the only relay evidence. It lists oldest-first by id
  with lane/undelivered/after_id/limit filters; there is no event-id lookup
  and no newest-first order, so relay walks are bounded (8 × 500 rows per
  call) and a walk that reaches its bound reports view_truncated rather
  than claiming a settled lane. A first-class event-id or DESC endpoint in
  handoffkeep would retire that bound; until then it is a known gap.

No write endpoint is called anywhere in this binary.

Known gap: the outbox list endpoint returns only unsent rows, so a drained
notification's receipt is read through the relay row it became (same
event_id); when neither is visible yet the state reports in-progress with
reason notice_row_not_visible rather than done.

## Hub single notice channel

Chat rows stored in handoffkeep with source_channel=assistant are the
assistant answer path. The hub never relays them as chat-{id} relay rows,
the orphan sweep never marks them failed, and the console refuses retry and
cancel on them — the handoffkeep notification_outbox (drained by PR-3) is
their only lane notice. The chat page renders them labelled 어시스턴트 with
an assistant badge instead of the operator delivery state. Human operator
and desk chat behavior is unchanged.
