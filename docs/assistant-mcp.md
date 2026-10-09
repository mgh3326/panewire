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
snapshot keyed by <stable id>:<revision> with the handoffkeep server_time so
the consumer can dedupe and drop stale items; liveness is the consumer's
responsibility.

## Tools

Read-only. There is no write tool: no deliver, no answer, no resolve, no
outbox mark-sent. PR-3 adds the write surface.

- targets — list the operator-verified delivery targets as opaque ids. A
  caller references a target only by the opaque id; unknown ids fail closed.
  No lane, URL, route or shell string is ever accepted as input.
- pending_list — every open decision request and pending chat question from
  handoffkeep GET /v1/assistant/pending, each with stable id
  (dr-<task>-<revision>, or the Q id), revision, human_only, status and the
  server clock. Merged or dropped tasks never appear — filtered upstream and
  again at this trust boundary. The response carries
  chat_questions_at_cap=true when the question list sits at handoffkeep's
  1000-row cap, meaning the list may be truncated.
- pending_detail — one item by stable id. Returns the item's current state
  with a pending flag; a stale or unknown id fails closed with a named
  error.
- progress — the state of one opaque target id or one request id, in the
  closed vocabulary accepted, delivered, in-progress, decision-pending,
  done, failed, derived only from handoffkeep fields (task state, decision
  resolution, relay delivered_at/delivered_to/attempts, outbox
  sent_at/hub_row_id where visible) with the receipts that justify it. A
  relay row that is only persisted is never done.
- poll — the dedupe snapshot described above.

## Caller-facing identity and errors

Every request needs the berry bearer token (Authorization: Bearer …), read
from a mode-0600 file and compared in constant time. When the optional
Cloudflare Access gate is configured, a verified Access JWT is additionally
required and its common_name must be on the configured service-name
allowlist — the signature alone never suffices, which is the identity
binding the hub's verifier deliberately lacks.

Tool failures return named errors (unknown_target, request_not_found,
request_not_current, invalid_arguments, hk_unreachable, hk_rejected_http_*).
No error string, tool result, log line or redirect ever carries a token, a
CF secret, URL userinfo or a redirect Location.

## Server-side configuration

One mode-0600 env file (-config):

- HANDOFFKEEP_URL, HANDOFFKEEP_TOKEN — read credentials; required.
- HANDOFFKEEP_CF_ACCESS_CLIENT_ID / _SECRET — optional outbound Access
  service-token pair for an hk URL behind Access; all-or-nothing, attached
  only to the configured origin, and redirects are never followed.
- PANEWIRE_ASSISTANT_TOKEN_FILE — the berry bearer token file (mode 0600).
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

Operator-edited JSON, mode 0600, checked on every read — a chmod or a broken
edit revokes the surface immediately:

```json
{"targets": {"ops-lane": {"kind": "lane", "lane": "ops", "description": "ops desk lane"}}}
```

kind is "lane" or "conversation"; each id must match
^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$. The file is the whole routing truth: the
mapping is opaque to callers and is the only way a tool reaches a lane or
conversation.

## Handoffkeep endpoints used

GET only: /v1/assistant/pending, /v1/assistant/outbox, /v1/tasks/{id},
/v1/tasks?lane=…, /v1/chat/questions/{id}, /v1/relay/events. No write
endpoint is called anywhere in this binary.

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
