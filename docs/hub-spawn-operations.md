# Durable spawn operations

`panewire spawn` and `work TASK` submit to the hub. The hub runs
placement and admission in-process; the selected node prepares the pinned
checkout and runs launch and landing in-process. No launcher binary or SSH
launcher is called. `--dry-run` remains the synchronous planning path.

The CLI prints `ACCEPTED job=… request=…` after the hub commits a reservation.
That confirms ownership of the operation, not a pane or brief delivery. Wait for
`SPAWNED` or `SPAWN_FAILED` and perform the usual read-only landing check.
`--wait` observes spawn state first, preserving admission/node refusal codes,
then uses the existing job-completion wait and timeout behavior. Delegate panes
use the same submission path.

## Acceptance and identity

`POST /v1/spawns` requires the operator credential. Per-host service grants do
not grant permission to let placement select an arbitrary host. The bounded
request contains a UUID, argv, UTF-8 brief (at most 64 KiB), originating machine,
pinned repository checkout and deadline seconds (default 600, maximum 3600).
The submitting CLI verifies the checkout is on a pushed origin ref. A canonical
checkout gets an isolated target worktree rather than running in the node's
canonical clone.

The response is HTTP 202 with `job_id`, `request_id`, `operation_url`, `status`
and `claim: durable_hub_reservation`. This is a hub DB reservation; the immutable
handoffkeep job claim is made later on the actual executing host. No reservation
is claimed under the hub's host and then transferred to a node.

The hub requires its durable inbox database (SQLite with FULL synchronization or
Postgres). With the store absent, aborted or unavailable it refuses acceptance.
The reservation and payload digest commit together. A request key and identical
payload replay the existing operation; a different payload returns 409. A live
job reservation or direct task/role reservation blocks a distinct request.
Inherited children do not take the parent's direct task/role slot. Completed
job IDs remain tombstones; a new direct task/role attempt requires evidence that
the prior landed job completed or was lost.

The reservation key is `<task>:<role>`, or `<task>:reviewer:p<N>` for a reviewer
spawned with `--review-pass N` (N is 1..9; reviewer only; any other role, 0 or 10
is a usage refusal). One helper builds it (`fleet.Spawn.RoleKey`), and the claim,
resume, owner lookup, release, reclaim and retryable-holder supersede paths all
use it, so the p1 and p2 reviewers of one task hold at once while a second p1 is
refused with the same 409 as any held slot. panewire reads the pass only from
the flag, never from a label. A reviewer without the flag keeps `<task>:reviewer`,
and stored keys without a pass need no migration. The held-reservation log line
prints the key as `role_key`. Worker and tester keys are unchanged.

A prior operation in `failed` whose hk job is still `claimed` (pane cleanup
unverified) is checked on a colliding submission: the hub asks the operation's
node for its herdr panes (`agents.list`). Only a complete pane list that lacks
the recorded pane has the holder's node mark the hk job `lost` and releases the
task/role slot. The holder's node writes the record with its own credential
(`job.mark_lost`), because the hub's hk client is not granted a remote host; the
hub writes itself only when the holder ran on the hub's own host. A present pane
keeps the slot (fail closed) unless it provably never started work (hk 3179),
which needs every one of these:

- the operation failed in the `landing` stage (the brief's delivery was never
  proven);
- the pane's agent row is `idle` or `done`, and its cwd is the prepared checkout
  of the request;
- the spawn's deadline passed more than the grace ago (10 minutes; hub config
  `UnworkedLandingGrace`);
- the node's `job.result` finds the job, with no `job.completed`, no report and
  `activity_events` of 0, so the ledger holds nothing after the spawner's own
  claim records. A node that predates `activity_events` proves nothing;
- `pane.capture` saves the screen, then `pane.close` closes the pane (it
  re-reads the pane and refuses one that went to work).

The hub then has the holder's node mark the hk job `lost`, logs
`landing_failed_unworked`, and the new submission claims the slot. Any failed or
unreadable check, an unreachable node, a read error or a missing pane id keeps
the slot. The 409 body then carries `request_id`, `job`, `host`,
`pane` and a `fix` (close that pane, then resubmit), and the CLI prints them
with exit 74.

A **reviewer** holder whose pane is present is also released when its spawn
`succeeded` or `failed`, and every one of these holds (hk 3315). Workers and
testers are never released this way, because an idle worker may be waiting on an
ESC answer.

- the new submission is for the same (task, role, review pass) key, not a replay
  of the holder's request;
- the pane is listed under the holder's pane id and agent label, with status
  `idle`, `done` or `blocked` (never `working`; any other status keeps the hold);
- the spawn's deadline passed more than the grace ago (the same 10 minute
  `UnworkedLandingGrace`; the node's agent list carries no time of the last
  state change);
- `pane.capture` saves the screen, then `pane.close` closes the pane (it
  re-reads the pane and refuses one that changed or whose composer is not
  provably empty).

The hub then has the holder's node mark the hk job `lost` and logs one INFO line
`hub closed a finished reviewer pane` with `branch` `reviewer_pane_finished`
(succeeded) or `reviewer_pane_stuck` (failed). An unreachable node or an
unreadable check keeps the hold.

When only that grace keeps a reviewer holder (the pane is present, its status is
closable and its hk job is `claimed` or `spawned`), the `job_already_claimed` 409
also carries `retry_after_s`, an integer of at least 1: the seconds until the
spawn's deadline plus the grace. The response sets the same value in an HTTP
`Retry-After` header, and the CLI appends ` retry_after_s=<n>` after the
`(held by ...)` part of its exit 74 message. Every other hold has neither,
because nothing time-based will free it.

A prior operation in `succeeded` whose hk job is still `claimed` or `spawned`
(a job revoked and reaped by an older binary, or one that ended without writing
its record, hk 3204) is checked the same way: only a complete pane list that
lacks the recorded pane, and lists no pane named like the holder's agent label
(herdr renumbers a pane moved across workspaces), has the holder's node mark the
hk job `lost` and releases the slot. A present pane, an unreadable pane list, an
unreachable node or no recorded pane keeps the slot and the 409 body as before.
A node that refuses `job.mark_lost`, because its local ledger holds a completion
or a newer claim or because herdr still lists the pane, keeps the slot too; the
hub logs `branch=release_write_failed` with the node's reason. When the holder
ran on the hub's own host the hub writes the record itself, and hk never
rewrites a terminal record, so a holder that completes during the probe keeps
its completed record: the refused `lost` write is followed by a re-read, and a
record now `completed` or `lost` releases the slot. A `retryable` operation holds
no slot (its reservation is deleted when it ends), but its hk job stays
`claimed`: a new submission for the same task and role under another job id has
that holder's node mark the job `lost` once it holds the slot. A replay of the
retryable request itself takes the replay path and writes nothing. `panewire
reap` now writes `lost` to the hk record of a job whose newest terminal event is
`job.revoked` or `job.lost` (see `docs/jobs-resilience.md`).

Before sending HTTP, the CLI saves `jobs/<job>/submission.json` privately and
synchronizes it. Repeating the same command reuses that key. A lost response is
not permission to generate a new key or try another host. A changed brief,
checkout or flags under that key is refused: inspect the original operation
before repairing the receipt or starting independent work under a new job ID.

`GET /v1/spawns/live[?except=<request_id>]` (operator credential) returns `{"profiles":{<placed profile>:<live spawns>}}`, the count the `[quota].pool_max_concurrent` cap reads ([placement](placement.md#pool-concurrency-cap)).

`GET /v1/spawns/<request_id>` returns job, request, status and a bounded result.
It never exposes the saved brief, argv, credentials or submitting paths. Its
`reason` is a stable code followed by the refusal text (`<code>: <text>`), which
is redacted, on one line and bounded, and can name a path on the executing node.

## State and recovery

The hub stores logical operation, attempt, absolute deadline, immutable node
request/destination, result, task/role reservation and notification outbox.
Four workers execute accepted work, with a bounded in-memory dispatch queue;
the durable queue is authoritative. HTTP cancellation does not cancel accepted
work. Shutdown ends worker contexts and waits for them before closing storage.

Only nodes advertising `spawn_ops_v1` can execute durable operations. Older nodes are refused with an upgrade fix, without a launcher fallback. Before sending node RPC, the hub commits the destination, node UUID and payload.
Durable node messages carry an operator-authorization marker and absolute deadline.
The legacy `/v1/spawn` endpoint rejects that marker, retains its original restricted
flag list and 300-second maximum, and cannot grant operator-only spawn overrides.
Unmarked node requests retain the legacy result shape, including on result replay.
Recovery replays that exact UUID on that exact node. An uncertain send never
falls through to another host. The node commits an executing journal entry
before launch and commits its completed result before replying. Duplicate
completed requests replay that result; active or restart-interrupted requests
answer reconciling instead of silently disappearing or running twice.

Node RPC includes the absolute deadline (`TestNodeSpawnExpiredAbsoluteDeadlineSkipsEngine`). A request first received after it
expires is refused without calling the engine. An execution already in progress
may still finish late; its reservation is retained. Timeout alone cannot close
a working or unverified pane. Failed-spawn cleanup re-reads id, name, cwd,
harness and idle/done status before closing; ambiguous cleanup keeps the pane,
quota and claim, and records a local `job.cleanup_pending` event rather than
declaring it lost. The hk job record stays `claimed`: hk takes a `result` only
with `state=completed`, so a fresh request may reuse a job id only when the job's
newest hub operation is `retryable`.

Under `--host auto`, a node's `cwd_unmapped` refusal returns the operation to
placement, which tries the next candidate. The node answers it before any
checkout or engine call. A first-run refusal (`hooks_review`, `trust_prompt`,
`omp_setup_wizard`, `auth_unavailable`) does the same once the node verified
its pane cleanup, and the hub tells `operator-desk` which one-time action the
host needs ([placement.md](placement.md#first-run-refusals-and-the-desk-notice)).
A pinned `--host` keeps any of these refusals as its result.

| Status | Meaning |
| --- | --- |
| `accepted` | Reservation durable; no node binding yet. |
| `dispatching` | Node UUID, payload and destination durable. |
| `reconciling` | Transport or execution observation unknown; ownership retained. |
| `deadline_exceeded` | Deadline observed; ownership retained, not terminal failure. |
| `succeeded` | Node landing receipt and fresh exact-pane identity verified. |
| `failed` | Definite refusal, including an existing job claim; never permission to reuse a live job. |
| `retryable` | Node attested cleanup, or admission stopped before dispatch, and a replay can succeed once the operator's fix is done; reservations released. |

A deliberate repeat of an identical cleanup-confirmed submission creates a new
attempt under the same logical key. Its node UUID changes, and a prior executing
host is pinned: handoffkeep host/task claims cannot move. Attempt CAS prevents
an old result or dispatcher from overwriting the new attempt. Prior notifications
remain available under distinct attempt/transition event IDs.

A late success is accepted only after replaying the node receipt and a fresh
`pane.read` match by pane id, name, prepared checkout path and harness. A changed,
missing or unreadable pane leaves ownership unresolved unless that node confirms the pane is gone and the completed handoffkeep job matches the same host, pane, label, profile and task. This covers a job that finished and was reaped while the hub was disconnected. No cleanup command is
sent merely because a deadline expired.

### Failure matrix and evidence

| Failure | Recovery contract | Test / explicit non-goal |
| --- | --- | --- |
| Hub restart after reservation | Reopen the same operation; no second acceptance/host claim. | `TestSpawnReservationSurvivesRestartAndReplaysLostResponse`, `TestLaneInboxStoreSpawnDurabilityContract` (SQLite and CI Postgres). |
| Deadline before dispatch | No node binding means no launch; release reservation and allow the same key/job to retry. | `TestAsyncSpawnUndispatchedTimeoutReleasesReservation` (queued and placement windows). |
| Node disconnect or uncertain send | Retain node UUID/host; replay its journal on reconnect. | `TestSpawnUnknownRetainsOwnershipAndLateResultCommitsOutbox`, `TestAsyncSpawnLateResultReconcilesAgainstSameNode`. |
| Duplicate accept / lost HTTP response | Identical key/body replays; changed body conflicts; CLI receipt preserves key. | Reservation test and `TestSubmissionReceiptReusesKeyAfterLostHTTPResponse`. |
| Timeout followed by late pane | Retain ownership; reconcile receipt with exact node identity; never close working/unknown. | Late-result test and `TestFailedSpawnCleanupRequiresExactIdlePane`. |
| Node restart while executing | Do not rerun or invent success from a pane's presence. | `TestNodeSpawnJournalRestartDedupAndLostResultReplay`. Automatic adoption/cleanup after this crash window is a non-goal; operator reconciliation is required. |
| Lost event or lost ACK | Commit state and outbox atomically; retry with a stable event id and actual job id. | `TestSpawnOutboxFailureRollsBackResult`, `TestAsyncSpawnLostEventRetriesWithStableJobIdentity`. |
| Process pressure / shutdown | Bounded execution and persisted pending work. | `TestAsyncSpawnWorkersBoundedAndIndependentOfHTTP`, `TestAsyncSpawnOutlivesSubmittingHTTPContext`; HTTP-independent execution lifetime is owned by maintenance/shutdown, never the POST context. |
| Backend migration | Never silently drop reservations or tombstones. | Spawn store contract refuses the inbox-only SQLite-to-Postgres migrator when spawn rows exist. Full backend transfer is a separate migration, not an inbox-only copy. |

The new spawn attempt binding is durable. Restoring the existing global legacy
job epoch/fence/revocation queue is explicitly outside this change; its documented
in-memory limits still apply. Multi-hub active/active ownership failover and
manual force-release of an uncertain live pane are not introduced.

Completed node journal entries are retained indefinitely as dedup tombstones.
There is no one-hour expiry and no retirement before a durable hub receipt.
Automatic journal GC and an ACK-driven retention policy are explicit non-goals.
Back up the node's private `spawn/spawn-inbox.sqlite3` under its database directory,
using SQLite's backup mechanism, and the hub database with all spawn tables. Library
clients without a node database use `$XDG_STATE_HOME/panewire/spawn` (default
`~/.local/state/panewire/spawn`), or an explicit absolute `SpawnJournalDir`.
The configuration directory can be read-only. The node checks journal access
before connecting. If a legacy journal exists beside `spawn.json`, stop the node
and migrate it using SQLite backup to the state directory before restarting;
startup refuses to silently abandon existing dedup records. Do not delete a
journal or change backend while unresolved ownership exists.

Before starting an upgraded node, remove the retired `runtug_path` key from
`spawn.json`; keep its workspace, session, repository mappings and work root.
The node checks this migration at startup and reports an actionable refusal before
connecting, rather than first discovering the retired key during a spawn. No
launcher executable setting or forwarding shim is supported.

A checkout-busy result completes one retryable attempt. The engine cannot bind a
fresh node UUID inside that completed attempt; replay the saved logical submission
to create the next attempt after the node's checkout operation finishes.

A checkout refusal whose fix is to inspect and correct the node's checkout
(`checkout_refused:branch-elsewhere`, `checkout_refused:origin-mismatch`) is
`failed`, whatever retryable flag the node sent: the same saved request meets the
same conflict. The other checkout refusals (`busy`, `dirty`, `sha-missing`,
`work_root_unset`, `git_too_old`) name a fix that ends in a replay and stay
`retryable`. `spawnRefusalReplayable` holds the mapping.

## Notifications and telemetry

State changes and notification records commit in one database transaction.
The outbox hands each record to lane delivery, which stores one handoffkeep
`lane.event` with the real `job_id`, a stable producer event id and the owner or
parent lane, then injects it. A restarted hub retries undelivered rows. Delivery
is at least once; consumers use the event id
(`<request_id>:<attempt>:<status>`) to deduplicate. Timeout emits
`SPAWN_FAILED … reason=timeout` as an observation with `deadline_exceeded`, not
proof of cleanup. A later verified landing may emit a distinct `SPAWNED`
reconciliation event.

`SPAWN_FAILED` names the job, request, exit status, stage, `replay=yes|no`
(whether replaying the saved request can help), reason and fix. It has
notify type `automation-failed`, so it interrupts a lane that digests its reports.
A `failed` or `retryable` spawn is also sent to the submitting lane when that
differs from the owner: `--requested-by`, else `--parent`. The owner's event id
stays `<request_id>:<attempt>:<status>`; the submitting lane's is the same id
with `:<lane>` appended.
`SPAWNED` stays type `report`.

| Reason code | Meaning |
| --- | --- |
| `parent_job_not_found` | `--parent-job` resolved to no record within the 30-second wait. A job store that cannot answer stays `admission_refused`. |
| `quota_unconfirmed:<cause>` | Admission could not confirm the target's quota. `<cause>` is the shared-observation reason, such as `no_usable_presence` or `stale`. Without one, the code is `quota_unconfirmed`. |
| `admission_refused` | Any other refusal before a node was bound. The text says why. |
| `spawn_failed:<code>` | The node's fleet App refused or failed, and the hub classified it. `<code>` comes from the closed table in `internal/fleet/refusal.go` (`spawnRefusals`), which also holds each refusal's fix. The text is the node's error, read from the result's stderr section. |
| `spawn_failed` | As above, but no row in that table matches the text or exit status. |

The hub derives `<code>` from the refusal text and exit status it already has, so
the node-to-hub wire still carries bare `spawn_failed`. Nodes and the hub deploy
at different times, and an older hub rejects any other error value. Status, retry
and notification decisions never read the code.

A private Sentry client records one always-sampled `runtug.spawn` transaction per
spawn attempt (the established telemetry name survives the CLI rename). Only this
spawn path is traced; other hub traffic and the crash reporter's budget are
unchanged. A transaction finishes on the first outcome observation; late reconciliation stays in the durable audit and each subsequent failure transition still gets its own deduplicated error event. Stage spans use measured claim/placement/checkout/launch/landing
elapsed durations. Durations from node clocks are end-aligned, not presented as
a synchronized cross-host causal timeline. Tags are job, host, profile, role and
rc. `SPAWN_FAILED`, including timeout, also produces an error event titled
`SPAWN_FAILED <stage>/<reason>` and fingerprinted by stage and normalized reason.
The normalized reason keeps `spawn_failed:<code>` only for a code in the closed
table, so tag and fingerprint cardinality stay bounded; the refusal text never
goes to Sentry. The error event also carries `request` and `attempt` tags, which
join it to the operation and to the node's `node_spawn` fleet event.
The client reports Environment `fleet`, Release `panewire@<version>` and the fixed
ServerName `panewire-hub`, never the OS hostname, and it drops user, request and
breadcrumbs from every event. Hub shutdown flushes it for at most 2 seconds after
the spawn workers stop. Briefs, argv, pane capture, stderr, checkout paths
and credentials are excluded. Export failure never authorizes a retry or changes
the committed business outcome. `TestSpawnSentryOnlySpawnAlwaysSampledAndNormalized`
uses a fake transport and also checks that ordinary HTTP traffic is untraced.
