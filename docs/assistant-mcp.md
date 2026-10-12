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

PANEWIRE_ASSISTANT_WRITES selects the enabled subset — exact values
only: `answer` lists answer_decision + answer_question, `deliver` lists
deliver, `all` lists all three; `0`, `false`, `no`, `off` or unset
select none. Anything else — a typo, a case variant, quoted padding —
refuses startup. A tool not in the set is absent from tools/list and a
call to it fails closed with writes_disabled before the targets file is
even read. Write calls are rate-limited per caller identity (token
bucket; see configuration).

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
  `<request-id>-answered` the lane notice will carry. Optional provenance
  fields (the PR-4 contract): `provenance` is `assistant_judgement` (the
  default) or `operator_decided` — any other value answers
  invalid_arguments. `operator_decided` relays the operator's exact
  verbatim words for this decision: `operator_quote` carries them and
  `decision_ref` must equal `request_id` exactly — a missing or different
  ref is refused as provenance_ref_mismatch before any handoffkeep call.
  The fields cross to handoffkeep unchanged and a quote is never
  synthesized. handoffkeep treats operator_decided without a non-empty
  quote as assistant_judgement, so on a human_only request that answer
  form replies decision_request_human_only with reason no_operator_quote;
  with a quote and a matching ref even a human_only request resolves and
  is recorded decided_by=operator.
- answer_question — answer one pending chat question. Input: `id` (the Q
  id), `revision` (the expected revision), `text` (≤4096 bytes) — the
  request body to handoffkeep is exactly this pre-PR-4 field set; the
  contract's amendment 1 keeps provenance out of the chat path entirely
  (the chat endpoint rejects unknown fields). The
  question row is pre-read first: an already-held answer slot answers
  question_slot_taken (the slot is single-use — do not retry), a
  non-pending or out-of-scope question answers request_not_found, a
  revision mismatch answers stale_revision carrying `current_revision`.
  The answer then posts through handoffkeep's assistant chat channel —
  POST /v1/chat/messages with source_channel=assistant, author operator
  and the expected-revision compare-and-swap — never through the hub chat
  route and never as source_channel web. A CAS race answers the same
  stale_revision. A sequential same-origin replay observes the held slot
  on the pre-read and answers question_slot_taken; `duplicate: true` is
  only the upstream endpoint's verdict on a replay its own dedupe window
  still sees racing. The deterministic notice event id is
  `<question-id>-rev<revision>-answered`.
- deliver — inject one instruction into a target's operator-mapped lane as
  a hub lane.event. Input: `target` (an opaque id from the targets file),
  `idempotency_key` (`^[A-Za-z0-9._-]{8,64}$`), `text` (trimmed non-empty,
  ≤2036 bytes, valid UTF-8, no control characters — no newlines — and no
  invisible runes: no Unicode format characters (category Cf — zero-width
  spaces, word joiners, ZWJ/ZWNJ, Mongolian vowel separators, BOM, bidi
  overrides), no default-ignorable code points (combining grapheme joiner,
  Hangul fillers, tag characters) and no variation selectors — an
  emoji+VS16 pair such as ❤️ is refused too. The first rune of the
  trimmed text is checked directly, nothing skipped: it may be neither a
  combining mark (category M), a conjoining Hangul jamo of
  Hangul_Syllable_Type V or T (U+1160..U+11FF, U+D7B0..U+D7C6,
  U+D7CB..U+D7FB — invisible when uncomposed) nor a blank-glyph rune
  (U+2800, U+1D159, U+303F, U+13441, U+13442), and neither `[` nor its
  fullwidth `［`, so a forged tag
  cannot ride the prefix in either glyph; over- or mis-shaped text is
  rejected, never truncated). The lane comes only
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

`PANEWIRE_ASSISTANT_AUTH_MODE` selects the credential. The exact values
are `bearer`, `service`, and `access-user`. Unset keeps today's
inference: a configured service gate is service mode, otherwise bearer.
Any other spelling refuses startup (`auth mode unknown`).

Bearer mode requires `Authorization: Bearer …`, read from a mode-0600
file and compared in constant time. Service mode requires that bearer
and, additionally, a verified Access JWT whose `common_name` is on the
configured service-name allowlist — the signature alone never suffices,
which is the identity binding the hub's verifier deliberately lacks.
An empty `iss` is still accepted in service mode, matching assertions
minted before issuer enforcement.

### Access-user mode

Config value: `access-user`. This is the Cloudflare Managed OAuth (Beta)
path for GPT Dots. The only credential is a verified
`Cf-Access-Jwt-Assertion`. The static bearer is not required and is not
a fallback: a request with a valid bearer and a missing or invalid
assertion is rejected, and a valid assertion with no bearer is allowed.

The assertion must verify as follows. The signature is checked against
the configured team JWKS. `iss` is required and must equal
`https://<team>.cloudflareaccess.com` exactly — an empty issuer is
rejected (`iss_missing`), and a different issuer is rejected
(`iss_mismatch`). `aud` must include this assistant's application AUD.
`exp` and `nbf` use the same strict comparison as the service verifier
(no added leeway). The stable principal is `(issuer, subject)`. `sub`
must be on `PANEWIRE_ASSISTANT_CF_SUBJECTS`. Email is display only and
is never the allowlist key. A service-token assertion (empty `sub`,
`common_name` set) is rejected. Caller-supplied identity headers
(`Cf-Access-Authenticated-User-Email`, `Cf-Access-Authenticated-User-Id`,
and anything else the client can set) are ignored. The profile is not
taken from a query parameter, `clientInfo`, `User-Agent`, a tool
argument, or an `X-Client-*` header.

On success the verifier returns a structured principal that request
handling can read:

- `mode` — `access-user`
- `issuer` — the verified `iss`
- `subject` — the verified `sub`
- `audiences` — every `aud` value from the token, whether `aud` was a
  string or a list, not only the value that matched this assistant
- `email` — display only

The auth audit line records that principal as a `principal` object
(`mode`, `issuer`, `subject`, `audiences`, `email`). The line's
`subject` field stays the tool's target or request id, so the two do
not share a key. The line also records flat `auth_mode`, `issuer`,
`audiences`, `iss`, `sub`, and `email`. The rate-limit and identity key is
`access-user|<issuer>|<subject>` and contains neither the email nor the
audiences. Rejections record a distinct reason on that audit line
(`iss_missing`, `iss_mismatch`, `aud_mismatch`, `expired`,
`not_yet_valid`, `bad_signature`, `unknown_kid`,
`subject_not_allowlisted`, `service_token`, `assertion_missing`,
`jwks_unavailable`) and return the generic `{"error":"unauthorized"}`
to the caller. A JWKS fetch failure denies the request. No log line,
error, or tool result contains the bearer, the assertion, or the JWKS
body.

Access-user does not forward that principal to handoffkeep. Write calls
do not run in this mode, so there is no resolve body and no chat body
to attach it to. Bearer and service writes still do not add a principal
field: hk has no field that stores one, and an unknown field is rejected.

Access-user startup refuses, with a named error, when the team domain,
AUD, JWKS URL, or subject allowlist is missing. The JWKS URL is not
defaulted in this mode.

Writes are forcibly blocked in this mode. `answer`, `deliver`, and `all`
do not list `answer_decision`, `answer_question`, or `deliver`, and
initialize does not advertise them. A call to any of those tools returns
`writes_blocked_access_user` before the targets file is read and before
any handoffkeep or hub call. The hub client and the outbox drainer are
not started. Bearer and service modes keep today's write behaviour,
including `writes_disabled` for a tool outside the configured set. The
literal flag value `1` is not legal and still refuses startup
(`assistant writes flag must be answer, deliver or all`). This mode does
not enable writes. Profiles, tool families, a policy engine, and an
OAuth server are later work; this principal is the input they will use.

#### Origin paths

The origin serves `POST /mcp` only. That path is the MCP handshake
(`initialize`, `ping`, `tools/list`) and the tool calls. A request that
carries a valid `Cf-Access-Jwt-Assertion` and no `Authorization` header
and no extra identity header reaches `initialize` and the read tools.

`GET /mcp` and `DELETE /mcp` answer 405. Every other path answers 404.
There is no health check. Every request is authenticated first, except
the protected-resource path below.

The exception is `/.well-known/cloudflare-access-protected-resource/mcp`.
Cloudflare Access serves that metadata at the edge, in front of the
origin, for the public hostname. The origin does not authenticate that
path and does not write a metadata body: an unauthenticated request that
reaches the origin answers 404 `{"error":"not_found"}`, with no
`WWW-Authenticate` header. A 401 would block discovery. A metadata
document would shadow the edge. The path is client discovery at the
edge. It is not an authorization input, and it is not proof of which
product holds the token. The public hostname (Access and Managed OAuth
at the edge) and the tunnel ingress (this process's listen address,
`127.0.0.1:9471`) are recorded in the example config, not here.

#### Example config

The binary does not special-case the team or the AUD. The issuer shape
is `https://<team>.cloudflareaccess.com`. Public markdown cannot repeat
a live issuer host (`TestDocsContainNoOperationalLiterals` rejects
host-shaped tokens in every `.md` file). The concrete dots values —
public hostname, issuer, AUD, ingress, and protected-resource path —
are in the example config
`docs/assistant-access-user.example`, which this binary does not read.
The hostname and the issuer host are only in that file.

The dots AUD is
`f405a0dce64192eff061a4a0acef6a145242fd94684f39d547c5e6f7a59012fb`.

Protected-resource path, served by Cloudflare Access and not by this
binary: `/.well-known/cloudflare-access-protected-resource/mcp`.

#### Operator console steps (later, not part of this change)

Do these in the Cloudflare dashboard after this binary is deployed.
Never reuse the hub's Access application.

1. Create a new Public DNS application for this assistant only.
2. Attach an operator-only Allow policy.
3. In the application's advanced settings, turn Managed OAuth on.
4. Turn localhost and loopback OAuth clients off.
5. Set the redirect URI to the ChatGPT callback only.
6. Set the access-token lifetime to 15 minutes.

Tool failures return named errors (unknown_target, request_not_found,
targets_file_invalid, invalid_arguments, writes_disabled,
writes_blocked_access_user, rate_limited,
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
  or neither. With any enabled write value (answer, deliver or all) the
  pair is mandatory — startup is refused without it, including in
  access-user mode. With writes off the pair may be absent, or present
  and entirely unused (no hub client is built and no drainer starts).
  In access-user mode the pair may be required by that check and is
  still unused: the hub client is not built and the drainer does not
  start, and write calls return writes_blocked_access_user.
- PANEWIRE_ASSISTANT_WRITES — exact values only: `answer`, `deliver`,
  `all`, or off via `0`/`false`/`no`/`off`/unset; anything else refuses
  startup.
- PANEWIRE_ASSISTANT_DRAIN_INTERVAL — drain period, default 30s, bounded
  1s–1h.
- PANEWIRE_ASSISTANT_WRITE_RATE_PER_MIN — sustained write limit per
  caller identity, default 10.
- PANEWIRE_ASSISTANT_WRITE_BURST — per-identity burst, default 5.
- PANEWIRE_ASSISTANT_AUTH_MODE — exact values `bearer`, `service`,
  `access-user`, or unset (inferred). Anything else refuses startup
  with `auth mode unknown`.
- PANEWIRE_ASSISTANT_TOKEN_FILE — the berry bearer token file (mode
  0600), required in bearer and service modes. Not required in
  access-user mode; a file that is present is checked and then ignored
  for authorization. The token is read once at startup and held hashed
  in memory, so rotating it requires a process restart (the binary is
  stateless; restart is free).
- PANEWIRE_ASSISTANT_TARGETS_FILE — the targets mapping (mode 0600); see
  below.
- PANEWIRE_ASSISTANT_LISTEN — bind address; default 127.0.0.1:9471.
- PANEWIRE_ASSISTANT_CLIENT_NAME — audit label for bearer-only requests.
- PANEWIRE_ASSISTANT_CF_TEAM, PANEWIRE_ASSISTANT_CF_AUD,
  PANEWIRE_ASSISTANT_CF_SERVICE_NAMES — the service-mode Access gate;
  all-or-nothing, comma-separated common_name allowlist. In access-user
  mode the team and AUD are required and the service-name list is not
  consulted.
- PANEWIRE_ASSISTANT_CF_CERTS_URL — JWKS URL. Optional override in
  service mode (the team default is used when unset). Required in
  access-user mode (`access-user mode requires jwks url` when missing).
- PANEWIRE_ASSISTANT_CF_SUBJECTS — access-user allowlist of stable
  `sub` values, comma-separated. The principal key is `(iss, sub)`.
  Email is not an entry. Empty refuses startup
  (`access-user mode requires subject allowlist`). Ignored unless the
  mode is `access-user`.

Every request is audit-logged (identity, auth_mode, the principal
object, issuer, audiences, iss, sub, email display, RPC/tool, target or
request id in `subject`, result). Access-user tool calls carry the
verified principal there. `audiences` is every aud value from the token.
Every write attempt
additionally logs the tool,
subject id, idempotency key or event id, revision, outcome class, hub row
id and a sha-256/8 text hash; answer_decision calls also log the
provenance — a vocabulary value verbatim, anything else as the fixed token `invalid`,
never the caller-supplied string — and the operator quote's length and
sha-256/8 — never the text or the quote itself, a token or an upstream
URL.

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

## Trust boundary notes

- hk follow-up: chatQuestionUpsert should clear answer_message_id when
  body or lane changes.
- The drainer relays every unsent notification_outbox row it can read —
  rows written by any handoffkeep-token holder are posted to lanes with
  this binary's hub token and the panewire-assistant label. The token
  pair is the whole trust boundary; anyone who can write the outbox can
  speak to a lane through it.
- Mapping the operator-desk conversation in the targets file lets
  answer_question answer every lane's question that desk sees — scope
  it to the conversations the operator actually wants answerable.
- The provenance quote is as strong as the bearer credential:
  handoffkeep records operator_quote verbatim but cannot prove the words
  were the operator's. Audit keeps only length and sha8, so a leaked
  credential could still write a fabricated operator_decided claim.
- PR-4 precondition: human_only must be set on merge, deploy and spend
  decision requests in handoffkeep before PANEWIRE_ASSISTANT_WRITES is
  turned on — otherwise those requests are answerable through
  answer_decision.
