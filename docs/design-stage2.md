# panewire stage 2 D0 — machine-to-machine transport design

Status: D0 design for ROB-1324. This document selects the stage 2a transport and
defines the contract that R1 must implement. It does not implement, provision,
deploy, or carry company data.

## Decision summary

- Use **Supabase Postgres queue rows as the bounded durable buffer and Realtime
  only as a wake-up hint**. Every machine is an outbound TLS client; no machine
  accepts a new inbound connection.
- Keep the system of record on each machine: an atomically written local inbox
  file, followed by verified local pane delivery. A transport row is temporary
  delivery state, not authoritative history.
- Stage 2a is **inline-only**: carry an inline body only for allow-listed
  non-company payloads up to 192 KiB. Realtime carries only a message identifier
  and never carries the body.
- Store only paths, hashes, sizes, identities, state transitions, and audit data
  in local SQLite. Never store a prompt or file body there.
- A remote spawn request ends at the receiving machine's `wrk` admission gate.
  Transport and panewire have no alternate spawn path.

The decisive trade-off is durability without a new inbound port. NATS
JetStream can provide queue semantics but would add a self-hosted broker and a
new listener on NCP. The measured `herdr --remote` path is an SSH thin-client
bridge to a Herdr session, not a durable machine inbox. Supabase has free-tier
pause and quota risks, but polling plus bounded Postgres rows gives stage 2a the
required offline-receiver behavior while preserving the outbound-only topology.

## Confidence labels

- **실측 (measured)** means observed during ROB-1324 against local Herdr 0.8.2 and the
  allowed NCP host.
- **확정 (fixed fact)** means supplied by the approved execution brief and not
  re-verified here.
- **추정 (estimate)** means an architectural prediction to validate in R1
  fixtures or later operational work. Every estimated comparison cell carries
  the `추정` marker directly.

## Hard invariants

### I1 System of record: local file plus pane injection

Each machine's local inbox file and verified local pane injection are the
system of record. The transport may carry a body and provide a bounded buffer,
but it never becomes the authoritative record.

### I2 wrk gate: no remote-spawn bypass

A remote spawn request is submitted only to the receiving machine's `wrk`
admission gate. A transport adapter, panewire daemon, retry path, and completion
handler cannot start an agent directly.

### I3 Outbound-only: no new inbound port

No stage 2a node opens a new inbound port. Every panewire daemon initiates
outbound TLS connections to Supabase. The selected design contains no NCP
broker listener.

### I4 No company data transport

Company data is not transported. Missing, unknown, or company classification
fails closed before publish and again before local materialization or spawn.

### I5 No SQLite body persistence

Passing or briefly buffering a body in the transport is distinct from storing
it in SQLite. SQLite stores path, hash, size, content type, identities, state,
timestamps, and audit results only; it stores no prompt or file body.

### I6 No secret or key in the repository

Secrets and keys live only in permission-restricted files under `~/.config/`.
They are never committed, copied into fixtures, written to reports, or included
in message metadata.

### I7 Core remains transport-agnostic

Core types and state machines contain no Supabase, NATS, or Herdr SDK type.
Transport SDK imports and wire representations remain inside adapters.

## Candidate comparison

| Comparison axis | NATS JetStream on NCP | `herdr --remote` | Supabase Postgres queue + Realtime |
| --- | --- | --- | --- |
| Operational burden | **추정:** high; operate, patch, back up, monitor, and recover a single broker and its storage. | **실측:** the 0.8.2 client discovers or installs a matching remote binary and starts/attaches a server; each remote session still needs Herdr lifecycle care. | **추정:** low-to-medium; managed database, but schema/RLS, expiry cleanup, quota monitoring, and normal periodic database activity remain ours. |
| Durability / offline receiver buffer | **추정:** file-backed streams and durable consumers retain unacked messages while a receiver is offline, subject to configured limits. | **실측:** the server and panes survived client/SSH loss. Its in-memory `EventHub` replayed retained events and the 0.8.2 source caps the process-local history at 512 events; it is not a durable recipient queue and restart durability was not established. | **추정:** an unexpired Postgres row remains claimable while a receiver is offline; Realtime is only a hint and polling repairs missed pushes. |
| Latency | **추정:** lowest of the three on a healthy nearby broker, normally one persistent connection and no database poll. | **실측:** one NCP `api snapshot` invocation over a fresh SSH connection took 0.25 s end to end; interactive attach was usable. | **추정:** WAN database/RPC latency is higher than NATS but acceptable for coordination; Realtime avoids waiting for the next poll when healthy. |
| Failure mode | **추정:** the one-node broker or its disk going down stops publish/consume until recovery; bad retention can evict pending work. | **실측:** client termination removed the SSH bridge but not the detached server/session; server loss would also lose the measured in-memory event replay. | **확정:** a free project can pause after 7 inactive days. **추정:** service outage, pause, rate limiting, or missed Realtime pushes delay delivery; durable rows and polling recover after service returns. |
| Security / outbound-only / authentication | **추정:** clients can be outbound-only with TLS and NKey or mTLS subject ACLs, but NCP must accept a new broker connection. | **실측:** it reused normal OpenSSH authentication and the existing SSH path; the remote API socket stayed local and no additional credential was observed. | **추정:** outbound HTTPS/WSS with per-machine JWTs, private channels, least-privilege RPCs, and destination-based RLS; no client listener. |
| Free-limit risk | **추정:** no SaaS message quota, but NCP CPU, disk, egress, backup, and operator time are the effective limits. | **실측:** no separate message quota was involved, but the primitive does not supply durable queue semantics. | **확정:** 2 projects, 500 MB DB, 2M Realtime messages/month, 256 KB/message, 200 concurrent connections, 5 GB egress/month, and 7-day inactivity pause. **추정:** ID-only pushes, 192 KiB body cap, TTL deletion, and normal claim polling keep usage bounded. |
| Implementation cost | **추정:** medium-to-high; broker deployment plus stream, consumer, credential, reconnect, and disaster-recovery work. | **추정 (measured basis):** high and misaligned; a separate durable spool, remote CLI bridge, and recipient protocol would still have to be invented. | **추정:** medium; one queue RPC/claim adapter, RLS policy, Realtime wake-up, polling fallback, and contract fixtures. |

## 선정 권고 (정확히 1개): Supabase bounded queue

Use Supabase Postgres rows for bounded, durable delivery state and Realtime for
an optional `message_id` wake-up. The receiver always confirms work by querying
and claiming the row; it never trusts a push payload as the message. This gives
an offline machine a recoverable backlog without opening an inbound port and
keeps transport details behind one adapter.

The free-tier facts are constraints, not reliability guarantees. Stage 2a uses
a 192 KiB raw payload ceiling, a default 72-hour TTL with a 7-day hard maximum,
delete-on-ack body cleanup, and periodic polling. Seventy-two hours covers a
normally offline machine across a weekend or holiday without relaxing the hard
maximum. Every healthy daemon's normal periodic claim poll/database access is
also its daily activity heartbeat; there is no separate heartbeat process. R1
must remain correct when Realtime is absent and when the project resumes after a
pause.

## 기각 대안 (정확히 2개)

### 기각 1 — `herdr --remote`: session attach is not a delivery queue

The measured implementation is a local thin client that uses SSH stdio to
proxy a remote Herdr client socket and render the TUI. `--remote` cannot be
combined with API subcommands. A remote Herdr server keeps panes alive across
an SSH disconnect and has a bounded in-memory event replay, but it has no
recipient-addressed durable ack/redelivery contract. Building those missing
pieces would reproduce a message broker around a session-control primitive.

### 기각 2 — NATS JetStream: avoid the broker listener and single-node duty

JetStream is the technically strongest low-latency queue candidate, but the
self-hosted NCP topology requires a new reachable broker listener plus TLS
credentials, patching, storage monitoring, backups, and single-node recovery.
Those costs conflict with stage 2a's outbound-only and no-new-inbound-port
constraints. NATS remains a later migration target behind the adapter if
managed-service limits become material.

## Stage 2a topology and node roles

```text
sender machine
  local canonical file
    -> metadata-only durable sender outbox
    -> panewired core -> Supabase adapter --outbound TLS-->
       Postgres bounded queue row
       + Realtime message_id hint
    <-outbound TLS poll/claim-- receiving panewired
       -> private message_id staging
       -> local expect checks
       -> atomic validated logical-path inbox file
       -> optional wrk admission
       -> correlated completion through the same pipeline
```

- **Personal MacBook:** primary orchestration producer and a normal consumer.
  It does not host a transport server.
- **Company MacBook:** a normal outbound client only. It may exchange only
  explicitly allow-listed non-company control/test artifacts; classification
  is not inferred from the machine name.
- **NCP, OCI, and Raspberry Pi:** normal outbound clients when separately
  enrolled. NCP has no broker role in the selected design. D0 does not connect
  to, configure, or deploy OCI or Raspberry Pi.
- **Supabase:** managed transport endpoint and bounded offline buffer. It is not
  a session host and not a system of record.
- **Each receiving panewired:** claims messages for its stable machine identity,
  validates them, writes the local inbox atomically, records metadata-only
  audit/dedupe state, and optionally calls `wrk`.
- **`wrk`:** the only spawn admission authority. It owns scopefuel, arbiter,
  worktree, model, and landed-verification decisions.

## Message and payload contract

### Envelope

The transport-neutral envelope uses JSON-compatible standard types. The
illustrative field names below are core names, not Supabase row or SDK types.

```yaml
schema_version: 2
message_id: "stable UUIDv7 created once by the sender"
delivery_id: "sha256(delivery:v1 + message_id + destination.machine_id)"
message_kind: "inbox.delivery | workflow.completion"
source:
  machine_id: "stable configured machine identity"
  instance_id: "ephemeral daemon boot identity"
destination:
  machine_id: "exact intended receiving machine"
  inbox_namespace: "approved local inbox namespace"
  logical_path: "normalized namespace-relative consumer-visible path, such as jobs/<id>/brief.md"
expect:
  machine_id: "must equal receiver configuration"
  pane:
    name: "optional exact local agent name"
    label: "optional exact local tab label"
    cwd: "optional exact absolute local cwd"
    workspace_id: "optional exact local workspace identity"
payload:
  mode: "inline (the only permitted stage 2a value)"
  content_type: "text/markdown; charset=utf-8"
  size_bytes: 123
  sha256: "lowercase hex digest of the exact decoded bytes"
  classification: "public | personal_non_company"
created_at: "RFC3339 UTC"
expires_at: "RFC3339 UTC"
correlation_id: "originating message_id or workflow id"
causation_id: "immediate predecessor message_id"
reply:
  destination_machine_id: "source machine for completion"
  correlation_id: "original message_id"
  requested: true
```

Required validation order is schema, exact destination machine, logical path,
expiry, classification, declared size, inline-body retrieval, actual size, hash,
and only then consumer-visible publication. A machine mismatch is therefore
rejected before body materialization. Raw values of unknown fields are never
persisted to SQLite, audit JSON, or logs. Audit may record only allowlisted field
names/types plus non-body metadata such as schema version or a digest. An unknown
field never relaxes a known guard: closed or required envelope regions reject it
as a poison message; explicitly forward-compatible regions ignore it without
preserving its raw value. An unknown schema version is also a poison message and
fails closed.

### Stage 2a inline-only payload

The allowed body is placed in a bounded Postgres transport row and the receiver
obtains that row over authenticated HTTPS. Realtime is an ID-only wake-up hint;
its 256 KB message limit is not the rationale for the body ceiling because the
body must never be present in that hint. The 192 KiB raw-body cap is instead a
bounded Postgres row, process-memory, and free-quota practicality contract.
Raising it requires a separate design decision and quota/memory fixtures.
Transport bodies have bounded TTL and size, are deleted or nulled after terminal
ack, and are never treated as the historical source of truth.

### Logical path and private materialization contract

`destination.logical_path` is the required, consumer-visible path relative to
the selected `inbox_namespace`; it preserves contracts such as
`jobs/<id>/brief.md` and `answer-...md`. It must be non-empty, already normalized,
and relative. Validation rejects an absolute path, an empty or dot target, any
`..` segment, NUL, backslash or other platform-separator ambiguity, a symlink
component or target, and every namespace escape.

Resolution and creation are race-safe: keep a trusted namespace directory file
descriptor, walk components with no-follow/beneath semantics (for example,
`openat2` constraints or iterative `openat` with `O_NOFOLLOW`), and perform the
final rename relative to trusted directory descriptors. Revalidation that merely
compares path strings is insufficient. A `message_id`-based path remains private
and may be used only for temporary staging, recovery, and dedupe; it is never the
consumer-visible success name. The receiver stages mode-0600 bytes in a
receiver-owned, mode-0700 directory on the same filesystem but outside the inbox
namespace and watcher root, then atomically renames a successful delivery to the
validated logical path and fsyncs the destination directory.

An existing logical-path target is recoverable only when its durable
`delivery_id`, hash, size, and path prove it is the same delivery/content. A
different delivery or content collision fails closed and never overwrites the
target.

### Completion messages

A completion uses the same envelope in the reverse direction. Its
`correlation_id` is the original `message_id`; `causation_id` identifies the
terminal local action. Its stable ID is derived once from
`sha256("completion:v1" + original_message_id + terminal_outcome + result_hash)`.
Retries reuse that ID. The source machine deduplicates it exactly like a forward
delivery, so repeated completion sends cannot complete a workflow twice. If the
same original workflow receives different completion IDs or outcomes, the first
valid completion wins the receiver's terminal transition. Later completions are
recorded as audit anomalies but cannot reverse terminal state or rerun the
original work.

## Machine and pane identity: extending `expect:`

Stage 1 verifies a local pane using fields such as name, label, cwd, workspace,
and a preflight revision. Stage 2 adds two independent identity boundaries:

1. **Machine boundary before body materialization:** the queue claim and envelope
   must both name the receiver's configured `machine_id`, and
   `expect.machine_id` must match it. RLS is defense in depth, not a substitute
   for the local equality check. Mismatch means no body materialization, no
   logical-path file, and no spawn.
2. **Pane boundary before consumer-visible publication:** if pane delivery is
   requested, the verified body remains only in private staging while panewire
   resolves the local target and checks the envelope's local name, label, exact
   cwd, workspace identity, and preflight/send identity stability. On mismatch,
   the body never appears at the consumer-visible logical path: it is either
   safely deleted or moved to a mode-0600 quarantine outside the inbox namespace
   and watcher root, followed by a metadata-only terminal rejection. It never
   invokes a different pane.

Machine and pane checks are not collapsed into one identifier: a correct pane
name on the wrong machine is still a hard failure. Only a successful pane
delivery, or a successful delivery for which no pane was requested, is atomically
published at the logical path. Rejection produces a correlated failure
completion when safe and then terminally acknowledges the poison delivery so it
cannot loop forever.

## Transport adapter boundary

Core owns these conceptual operations:

```text
Publish(context, Envelope, PayloadReader) -> PublishReceipt
Receive(context, Destination, Handler(Delivery)) -> error
FetchPayload(context, Delivery, SizeLimit) -> PayloadReader
Ack(context, OpaqueDeliveryToken, AckDisposition) -> error
Health(context) -> TransportHealth
```

`Envelope`, `Destination`, `PayloadReader`, `PublishReceipt`,
`AckDisposition`, and `TransportHealth` use only core or standard-library
types. `OpaqueDeliveryToken` is uninterpreted by core. The Supabase adapter maps
these calls to authenticated RPC/query, row claims, visibility deadlines,
Realtime hints, and deletion. A future NATS adapter may map the same contract to
subjects, consumers, and acknowledgements without changing core.

The adapter cannot write the inbox, call `wrk`, classify company data, choose an
ack disposition, or persist a body in SQLite. Contract tests run the same
publish/claim/redeliver/ack/expiry cases against an in-memory fake and every
adapter. Dependency checks reject transport SDK imports from core packages.

## Durable idempotency and acknowledgement order

`message_id` is stable across every publish retry. `delivery_id` is stable for
the message/destination pair. A claim or lease token may change per attempt but
is never used as the dedupe key.

### Sender metadata-only outbox

Every sender has a durable SQLite metadata-only outbox with the minimum state
machine `SUBMITTED -> PUBLISHED -> COMPLETED | EXPIRED`. The submit API durably
commits `SUBMITTED` before returning success. Its row contains the stable
`message_id`, canonical source path, SHA-256, size, destination, logical path,
classification decision and policy version, attempts, created/updated/attempt
timestamps, expiry, and last error/transport receipt metadata. It contains no
body or body-bearing JSON field, preserving I5.

After daemon restart, the publisher recovers that same `message_id` and source
metadata from the outbox. Transport publication uses a unique constraint on
`(message_id, destination_machine_id)` (equivalently the stable `delivery_id`)
and an idempotent insert/upsert that accepts only the immutable original
metadata. A crash before transport publication retries the same ID and produces
one logical row. If the transport accepts the row but the daemon crashes before
committing local `PUBLISHED`, restart queries or upserts the same unique key and
does not create a second logical row. Completion advances the original row to
`COMPLETED`; elapsed expiry advances it to `EXPIRED`.

The outbox states are delivery metadata, not authoritative content. The sender's
local canonical source remains the system of record through completion or
expiry, just as the validated receiver file and local pane result remain the
receiver-side system of record.

### Receiver dedupe and publication order

Receiver SQLite keeps a durable metadata-only row keyed by `delivery_id` with
states such as `RECEIVED`, `STAGED`, `LOGICAL_PUBLISHED`, `GATE_DENIED`,
`SPAWN_REQUESTED`, `SPAWN_UNKNOWN`, `SPAWN_ACCEPTED`, `COMPLETED`, `ACKED`, and
`TERMINAL_REJECT`. It records the validated logical path, private staging or
quarantine disposition (never body bytes), hash, size, classification decision,
destination/pane identities, attempts, timestamps, and completion ID. There is
no body column or body-bearing JSON audit field.

The receiver order is:

1. Claim metadata with a visibility deadline. Before fetching body bytes,
   validate schema, exact machine destination, logical path, expiry,
   classification, and declared size.
2. Reserve or load the durable dedupe row. A terminal reject records metadata
   and skips body materialization.
3. Fetch the inline body into the private, `message_id`-based mode-0600 staging
   file while enforcing the size bound and SHA-256; fsync it and commit `STAGED`.
4. If pane/expect delivery was requested, resolve and validate the exact local
   target, including preflight/send identity stability, while the body remains
   consumer-invisible. On mismatch, quarantine outside the inbox/watch root or
   safely delete it, commit `TERMINAL_REJECT`, and create no logical-path file.
5. For a successful pane validation or a no-pane delivery, resolve the validated
   logical path with trusted dirfds/no-follow constraints, enforce the collision
   rule, atomically rename staging to that final path, fsync its directory, and
   commit `LOGICAL_PUBLISHED` with path/hash/size.
6. If spawn was requested, durably commit `SPAWN_REQUESTED` before calling only
   the `wrk` admission API with `delivery_id` as its job key. Persist
   `GATE_DENIED` on a durable denial or `SPAWN_ACCEPTED` only with a durable
   accepted receipt/query result; an ambiguous outcome becomes `SPAWN_UNKNOWN`.
7. Ack the transport only after logical publication and dedupe state, plus any
   required gate decision, are durable. A terminal rejection may instead be
   acked after its metadata/quarantine disposition is durable. Ack means locally
   accepted or terminally rejected; it does not mean the workflow completed.
8. Delete/null the transport body on ack and retain only bounded metadata until
   expiry. Workflow completion is a separate reverse message.

Crash recovery is deterministic:

- Before logical rename: remove only stale private staging files owned by the
  delivery and retry with the same `delivery_id`.
- After logical rename but before SQLite commit: verify that the validated
  logical file has the same delivery/hash/size, reconstruct
  `LOGICAL_PUBLISHED`, and do not rewrite it.
- After SQLite commit but before ack: redelivery reads the dedupe row, creates no
  second logical file or spawn, and repeats the ack.
- In `SPAWN_REQUESTED` or `SPAWN_UNKNOWN`, automatic respawn is forbidden;
  recovery fails closed for manual reconciliation unless the upstream durable
  lookup contract described below proves the accepted outcome.
- After a proven spawn acceptance, recovery queries the `wrk` job keyed by
  `delivery_id`; it never starts a second job.
- After completion publish but before its ack: resend the same completion ID.

Replay is allowed only while `expires_at` is valid. Manual replay creates a new
message ID and explicit `causation_id`; silently deleting dedupe history and
reusing an old ID is forbidden.

## Failure, retry, and poison policy

- A receiver combines Realtime wake-ups with a periodic claim poll. Missed or
  duplicated pushes change latency only, never correctness. That normal daemon
  poll/database access is the daily activity heartbeat; no separate heartbeat
  process exists.
- Transient publish, claim, and ack errors use full-jitter exponential backoff:
  1 second base, doubling to a 5-minute cap, bounded by `expires_at`. A claim
  visibility deadline exceeds the local write/validation budget and is renewed
  only while the same process owns the delivery. Project pause and rate-limit
  recovery use the same full-jitter/backoff contract rather than a tight
  activity loop.
- Default TTL is 72 hours and the hard maximum is 7 days. The default tolerates
  a normal weekend or holiday offline interval. Expired deliveries,
  unsupported schemas, destination mismatches, oversized payloads, tampered
  hashes, and disallowed classification receive a terminal rejection record.
  Untrusted bytes are not written to a consumer-visible logical path.
- Repeatedly failing otherwise valid deliveries move to a transport DLQ state
  and a local metadata-only quarantine record after 12 attempts or at expiry,
  whichever comes first. A safe, verified payload may be retained as a local
  mode-0600 quarantine file outside the inbox namespace and watcher root;
  SQLite still stores no body.
- During a Supabase outage or free-project pause, the sender's local source file
  remains canonical and outbox publish attempts wait. On recovery, publishers
  reuse the same message ID and receivers poll rows before relying on Realtime.
- If local disk write, `fsync`, rename, or SQLite commit fails, the receiver does
  not ack. If the failure outlives TTL, the sender observes terminal expiry.
- A completion reply uses the same retry, expiry, hash, destination, dedupe, and
  ack rules. Failure to deliver a completion never causes the original task to
  run again, and a later conflicting completion cannot replace the first valid
  terminal outcome.

## Security contract

- Each machine has a stable non-secret machine ID and a distinct revocable
  credential. Clients receive no Supabase `service_role` key. RLS allows a
  sender to insert only its own source identity and a receiver to select/claim
  only its destination rows. Ack/delete and completion permissions are equally
  scoped. Realtime channels are private and publish identifiers only.
- TLS certificate validation is mandatory. Secret files live below
  `~/.config/panewire/`, are mode 0600, and are loaded by path. Logs redact
  tokens, Authorization headers, and payloads.
- Classification is an allow-list: only `public` and
  `personal_non_company` may leave a machine. Missing, `unknown`, and `company`
  fail closed before adapter publish and again on receive. The classifier emits
  a decision code and policy version, not the inspected body, to SQLite.
- A NATS adapter, if reconsidered later, requires TLS plus per-machine NKey or
  mTLS credentials and publish/subscribe subject ACLs. The current design does
  not deploy its listener.
- Herdr remote attach continues to use normal OpenSSH authentication and a
  remote local socket. It is not used as the stage 2a transport.

### R1 pre-work: per-machine Supabase Auth identity

The Supabase anon key alone does not establish machine identity. Distributing
the project's JWT signing secret to each machine would create a root-equivalent
credential surface. R1 therefore assigns exactly one Supabase Auth user to each
machine and stores its credential/session material only under
`~/.config/panewire/` in mode-0600 files. A registry maps `auth.uid()` to one
stable machine ID; RLS and RPC authorization constrain source insertion,
destination visibility, claim, ack/delete, and completion operations through
that mapping.

Neither the project JWT signing secret nor `service_role` is distributed to a
client. Before any Supabase adapter implementation begins, R1 must fix the
enrollment, bootstrap, session-refresh, rotation, and revocation threat model
and its fixtures, including loss or compromise of one machine credential.

## R1 dependency: `wrk`/arbiter exact-once spawn recovery

Exact-once spawn recovery has a blocking cross-repository dependency on
agent-skills `wrk`/arbiter. Upstream must provide durable job lookup keyed by
`delivery_id`, fail-closed refusal of a duplicate job key, and reconciliation of
an accepted outcome through a durable receipt or query.

The adversarial review measured that respawning the same `--job` currently emits
an arbiter duplicate warning and then proceeds. That behavior is not an
idempotency guarantee and must not be cited as one. Until the upstream contract
is implemented and verified, receiver SQLite is the only duplicate barrier. A
crash in the `SPAWN_REQUESTED`/`SPAWN_UNKNOWN` window therefore forbids automatic
respawn and requires fail-closed manual reconciliation. The receiver may claim
`SPAWN_ACCEPTED` only from a durable upstream receipt/query result.

This D0 does not change agent-skills or the arbiter. The upstream implementation
and verification remain an R1 blocking dependency.

## R1 (stage 2a implementation) acceptance draft

R1 must add named fixtures before implementation. A manifest guard must list
every `TestR1...` name below and fail when any fixture is absent; this prevents
`go test -run` from returning a false green when it matched zero tests. Every
table command maps 1:1 to one exact fixture name. Each fixture runs against a
deterministic fake transport, with adapter contract cases repeated against the
Supabase adapter in an isolated test environment.

| Incident / failure fixture | Reproducible command | Required observation |
| --- | --- | --- |
| destination mismatch | `go test ./... -run '^TestR1DestinationMismatch$' -count=1 -v` | Machine mismatch materializes no body; pane/expect mismatch leaves zero logical-path and zero consumer-visible files, uses only safe deletion or mode-0600 out-of-namespace quarantine, commits a metadata-only terminal reject, and makes zero `wrk` calls. |
| logical-path traversal/symlink/race | `go test ./... -run '^TestR1LogicalPathTraversalRejected$' -count=1 -v` | Absolute, `..`, symlink, and rename-race escapes create zero files outside the final/staging namespaces and make zero `wrk` calls. |
| duplicate/redelivery | `go test ./... -run '^TestR1DuplicateRedelivery$' -count=1 -v` | Ten deliveries with one stable ID produce one logical-path file, at most one gate call, and repeatable ack. |
| sender crash before versus after publish | `go test ./... -run '^TestR1SenderCrashBeforeVsAfterPublish$' -count=1 -v` | A pre-publish crash restarts with the same ID and creates one logical transport publish; accept followed by a pre-`PUBLISHED` local crash creates zero duplicate logical publishes. |
| crash before atomic inbox write | `PW_FIXTURE_CRASH_AT=before_rename go test ./... -run '^TestR1CrashBeforeInboxWrite$' -count=1 -v` | Restart removes only owned private staging, writes one verified logical-path file, and then acks. |
| crash after atomic inbox write and before ack | `PW_FIXTURE_CRASH_AT=after_rename go test ./... -run '^TestR1CrashAfterInboxWriteBeforeAck$' -count=1 -v` | Redelivery verifies the logical-path file, reconstructs metadata, performs zero rewrite/spawn duplicates, and acks. |
| offline receiver then reconnect | `go test ./... -run '^TestR1OfflineReceiverReconnect$' -count=1 -v` | Message remains claimable while offline and is delivered once after polling resumes, even with no Realtime hint. |
| transport outage | `go test ./... -run '^TestR1TransportOutageBackoff$' -count=1 -v` | Stable ID is reused; observed delays stay inside full-jitter exponential bounds; local source remains canonical. |
| expired/oversized payload | `go test ./... -run '^TestR1ExpiredAndOversizedPayload$' -count=1 -v` | Both cases fail before body materialization/spawn and end in terminal reject/DLQ metadata. |
| tampered hash | `go test ./... -run '^TestR1TamperedHash$' -count=1 -v` | A one-byte mutation fails SHA-256, writes no consumer-visible logical-path file, and cannot ack success. |
| company-data classification fail-close | `go test ./... -run '^TestR1CompanyDataFailClosed$' -count=1 -v` | `company`, `unknown`, and missing values cause zero adapter publish and zero receive materialization. |
| secret/repo leak guard | `go test ./... -run '^TestR1SecretRepoLeakGuard$' -count=1 -v` | Credentials load only from a mode-0600 `~/.config/` fixture; repository and captured logs contain no fixture token or private-key marker. |
| SQLite body absence | `go test ./... -run '^TestR1SQLiteBodyAbsence$' -count=1 -v` | Schema and every text/blob value contain metadata only; inject an unknown-field body-smuggling marker into success, failure, and retry paths and verify that the marker is absent from SQLite, audit JSON, and logs. |
| wrk gate denial/no bypass | `go test ./... -run '^TestR1WrkGateDenialNoBypass$' -count=1 -v` | Denial produces no agent process, direct spawn call, or alternate path; retry does not call the gate twice. |
| reverse completion duplicate/correlation | `go test ./... -run '^TestR1ReverseCompletionIdempotent$' -count=1 -v` | Repeated and conflicting-ID/outcome completions yield one first-valid terminal transition, preserve original correlation/causation IDs, and record later values only as anomalies. |
| adapter contract | `go test ./... -run '^TestR1AdapterContract$' -count=1 -v` | Fake and Supabase adapters pass the same publish/claim/redeliver/ack/expiry contract. |
| core SDK leakage | `go test ./... -run '^TestR1CoreNoSDKLeakage$' -count=1 -v` | Core dependency/import output contains no Supabase, NATS, or Herdr SDK. |

The fixture manifest guard is itself required:

```sh
expected="$(printf '%s\n' \
  TestR1DestinationMismatch \
  TestR1LogicalPathTraversalRejected \
  TestR1DuplicateRedelivery \
  TestR1SenderCrashBeforeVsAfterPublish \
  TestR1CrashBeforeInboxWrite \
  TestR1CrashAfterInboxWriteBeforeAck \
  TestR1OfflineReceiverReconnect \
  TestR1TransportOutageBackoff \
  TestR1ExpiredAndOversizedPayload \
  TestR1TamperedHash \
  TestR1CompanyDataFailClosed \
  TestR1SecretRepoLeakGuard \
  TestR1SQLiteBodyAbsence \
  TestR1WrkGateDenialNoBypass \
  TestR1ReverseCompletionIdempotent \
  TestR1AdapterContract \
  TestR1CoreNoSDKLeakage | sort)"
actual="$(go test ./... -list '^TestR1' | grep -E '^TestR1[A-Za-z0-9_]+$' | sort)"
test "$actual" = "$expected"
```

The guard compares the complete output to all 17 exact test names, so a missing,
extra, duplicate, or zero-match result fails. R1 is not accepted on a test
report that omits a name, matches zero tests, uses a live company artifact, or
requires a production Supabase project.

## Explicit non-goals

- Stage 2b session hosting.
- Carrying company data.
- Opening a new inbound port.
- Treating the transport as a system of record.
- Implementing stage 2a, building a PoC, or creating a Supabase project in D0.
- Deploying, merging, or configuring OCI or Raspberry Pi.
- Pointer/object storage/remote locator delivery and its SSRF/fetch surface are
  explicit future work, not stage 2a. D0/R1 includes no locator fetch or pointer
  fixture.

## Measured Herdr boundary used in this decision

Local Herdr 0.8.2 help describes `--remote` as an SSH attach to a remote server.
The official v0.8.2 implementation in
`src/remote/attach.rs` starts an SSH stdio bridge and launches a local thin
client; `src/remote/host_unix.rs` connects that bridge to the remote local client
socket. The same release's `src/api/event_hub.rs` retains at most 512 events in
process memory. The NCP probe confirmed server/session survival after the exact
local attach process was terminated, successful reattach, 0.25-second fresh-SSH
snapshot latency, and in-memory replay of events emitted with no subscriber.

The exact `herdr api events.subscribe` CLI spelling is not implemented in 0.8.2
(exit 2); raw socket `events.subscribe` works. Remote non-TUI `agent.read`
worked. Positive `agent.prompt` execution was not established because NCP had no
supported coding agent installed, and installing one was outside D0 scope.
