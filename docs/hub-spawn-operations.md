# Durable spawn operations

`panewire spawn` and `work TASK` submit to the hub. The hub runs
placement and admission in-process; the selected node prepares the pinned
checkout and runs launch and landing in-process. No launcher binary or SSH
launcher is called. `--dry-run` remains the synchronous planning path.

`--dry-run` with a hub configured asks the hub for its own plan: `POST
/v1/spawns/plan` runs the same placement and selection the submission would and
gives the refusals a submission gives before placement (an invalid label, a host
the request cannot use, a job id hk already holds). It writes no operation row,
no reservation, no hk write and dispatches nothing. A refused plan may still
write a refusal or quota-observation line to the hub's local fleet event log;
those records never reach hk.

A dry run measures nothing: a quota refresh writes presence, lease and
observation to the shared quota store. An account that needs a measurement — a
stale or unobserved shared account on any host, or a stale account this machine
holds that the admission gate would refresh — is printed as
`would measure host=… pool=…` and left unmeasured. That rung is dropped, and a
plan with no other rung refuses and names the measurement. The plan can
therefore differ from the submission: the hub schedules the measurement and
may place the submission after a fresh observation arrives.

The CLI prints the hub's host and profile, labelled `hub plan:`. The hub may
choose a different profile from this machine's local plan. Without a hub the
receipt is labelled `local plan:`. A hub that predates the route (404 or 405)
gets a warning and the local plan, also labelled `local plan:`. Either local
plan keeps the same no-measure rule. A hub refusal exits with the hub's `rc` and
reason; an unreachable or failing hub is an error, not a silent fallback.

The CLI prints `ACCEPTED job=… request=…` after the hub commits a reservation.
That confirms ownership of the operation, not a pane or brief delivery. Wait for
`SPAWNED` or `SPAWN_FAILED` and perform the usual read-only landing check.
`--wait` observes spawn state first, preserving admission/node refusal codes,
keeps polling through `reconciling` (which sends no notice), then uses the
existing job-completion wait and timeout behavior. Delegate panes
use the same submission path.

For a stale or unobserved native quota account, the hub schedules a node
refresh independently of placement. If no confirmed eligible account can run
the request, it stays unbound in `accepted`, keeping its reservation until a
refresh permits placement or its existing deadline expires. A completed refresh
wakes another placement pass; a failed or timed-out refresh is retried while
the request remains pending. An already-confirmed account can be selected
without waiting for the stale accounts. A pending measurement produces no
terminal refusal event. If the pass already contacted a host, its failed
placement attempt remains recorded while the request stays accepted. The hub
logs a changed refresh failure with its machine, pool and reason; repeated
identical failures are not logged on every retry.

Refreshes coalesce by machine and pool. At most four run at once, with at most
one per node; waiting keys are considered in arrival order while busy nodes are
skipped. The configured `measure_timeout_s` (default 60 seconds, capped at 60)
starts when a key is queued and covers queueing, writer admission, the socket
write and the response. After writer admission, the socket write also has the
same five-second maximum as other hub node writes. An expired queued attempt
retains its place, but is sent only if a later placement pass requests it with a
new budget. On shutdown the hub cancels refreshes; accepted requests and their
reservations survive restart. Local, non-hub spawns keep their synchronous
refresh behavior. See [Quota service boundary](placement.md#quota-service-boundary).

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
prints the key as `role_key`. An implementer holds `<task>:implementer`; a hold written under an older role name is not matched by it. A consult
seat (`--role consult`) bound by `--task` takes no task/role slot at all
(`fleet.Spawn.HoldsTaskRole` is false for it), so a consult pair shares one task
(hk 4568); it works beside the claimant and never takes its exclusive claim.

A prior operation in `failed` whose hk job is still `claimed` (pane cleanup
unverified) is checked on a colliding submission: the hub asks the operation's
node for its herdr panes (`agents.list`). Only a complete pane list that lacks
the recorded pane has the holder's node mark the hk job `lost` and releases the
task/role slot. The holder's node writes the record with its own credential
(`job.mark_lost`), because the hub's hk client is not granted a remote host; the
hub writes itself only when the holder ran on the hub's own host. A present pane
keeps the slot (fail closed) unless it provably never started work (hk 3179),
which needs every one of these:

- the operation failed in the `launch` or `landing` stage, before brief delivery
  was proven;
- the pane row is `idle` or `done`, and its cwd is the prepared checkout of the
  request. If the failed launch never assigned an agent label, the hub uses the
  pane id and checkout cwd, and the node permits that unnamed identity only for
  this reclaim after re-reading the pane;
- the spawn's deadline passed more than the grace ago (10 minutes; hub config
  `UnworkedLandingGrace`);
- the node's `job.result` finds the job, with no `job.completed`, no report and
  `activity_events` of 0, so the ledger holds nothing after the spawner's own
  claim records. A node that predates `activity_events` proves nothing;
- `pane.capture` saves the screen, then `pane.close` closes the pane (it
  re-reads the pane and refuses one that went to work).

The hub then has the holder's node mark the hk job `lost`, logs
`failed_spawn_unworked`, and the new submission claims the slot. Any failed or
unreadable check, an unreachable node, a read error or a missing pane id keeps
the slot. The 409 body then carries `request_id`, `job`, `host`,
`pane` and a `fix` (close that pane, then resubmit), and the CLI prints them
with exit 74. A held (task, role) slot answers the error `task_role_claimed`,
which also names `task` and `role`; a refused job id still answers
`job_already_claimed` (hk 2944).

A **reviewer** holder whose pane is present is also released when its spawn
`succeeded` or `failed`, and every one of these holds (hk 3315). Implementers
and consults are never released this way, because an idle implementer may be waiting on an
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
closable and its hk job is `claimed` or `spawned`), the `task_role_claimed` 409
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

### Checks at submit

Mistakes in a spawn's own input are refused in the submitter's terminal, not
by a SPAWN_FAILED event after acceptance (hk 4270, decision 152). The engine
keeps every check, so a request that passes here is judged again on the node.

| Check | Where | Refusal |
| --- | --- | --- |
| Agent name: `-l LABEL` is a lowercase letter, then `[a-z0-9_-]`, at most 32 characters (herdr's rule, hk 4142) | `panewire spawn` and the hub (`400 label_invalid`) | `label "…" is not a valid agent name` |
| Task title marker `(grade X, Tn)`, and its scale against `--t` | `panewire spawn` | `task title lacks (grade X, Tn marker`, `task scale does not match --t` |
| Task claim: the task exists, is not `merged`, `dropped`, `hold` or `needs_decision`, and is bound to no other job or claimant | `panewire spawn` | `task does not exist`, `task unavailable: <state>`, `task bound to another job`, `task has another claimant …` |
| Host eligibility: some configured host is not excluded, is the `--host` pin when one is given, and declares every `--tags` capability | the hub (`400 no_eligible_host`) | `NO_ELIGIBLE_HOST: no host declares cap unity; …` |

The task checks read hk from the submitter. When hk cannot answer, the submit
goes on and the node decides, as before. They are skipped for a job that already
has a saved `submission.json` (a replay of an accepted request must reach the
hub's idempotent lookup even if the task has moved on) and for `--parent-job`
spawns; `--author-family`/`--builder-job` are translated as the node does, so a
review pass of a builder-bound task is accepted. Host eligibility reads the hub's own
fleet configuration (`local` and `hosts`), the same hosts placement ranks, so a
request no host can take is refused before it reserves a job id. A refused
request leaves no operation behind.

## State and recovery

The hub stores the logical operation, attempt, absolute deadline, node
request and destination, per-attempt placement exclusions, result, task/role
reservation and notification outbox. Eight workers perform placement; they
commit a binding and return without waiting for the node. A separate dispatcher
admits at most one request per node. A slow node therefore cannot occupy the
placement workers or another node's dispatcher. The durable rows are
authoritative; the in-memory work and node-slot indexes can be rebuilt after a
restart. HTTP cancellation does not cancel accepted work. Shutdown ends worker
contexts and waits for them before closing storage.

The coordinator scans past occupied node slots. It expires a bound request at
its absolute deadline without waiting for the node, releases its reservation,
and reports a retryable timeout. It also returns an unsent binding to placement
when the node disconnects or stops accepting. The unbind must commit before
placement can take the request; a store failure leaves the binding recoverable.
Neither case treats a `dispatching` row as unsent.

Only nodes advertising `spawn_ops_v1` can execute durable operations. Older nodes are refused with an upgrade fix, without a launcher fallback. The hub first commits the destination, node UUID and payload as `bound`; before sending node RPC it atomically changes the matching request, attempt and node UUID to `dispatching`. A recovered `bound` row is proof no send transition occurred. A recovered `dispatching` row may have reached the node, even if the hub crashed before the actual write.
Durable node messages carry an operator-authorization marker and absolute deadline.
The legacy `/v1/spawn` endpoint rejects that marker, retains its original restricted
flag list and 300-second maximum, and cannot grant operator-only spawn overrides.
Unmarked node requests retain the legacy result shape, including on result replay.
Recovery replays that exact UUID on that exact node. An uncertain send never
falls through to another host. The node commits an executing journal entry
before launch and commits its completed result before replying. Duplicate
completed requests replay that result; a request this node process is still
running answers `reconciling`, and an executing entry no live execution owns
(the node restarted mid-spawn) answers `unknown`. Neither runs it twice. See
[Stranded requests](#stranded-requests).

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
checkout or engine call. A spawn carrying a checkout on a node with a work
root is not answered that way: the node clones into the work root, following
[the placement rule](placement.md#the-filter). A first-run refusal (`hooks_review`, `trust_prompt`,
`omp_setup_wizard`, `auth_unavailable`) does the same once the node verified
its pane cleanup, and the hub tells `operator-desk` which one-time action the
host needs ([placement.md](placement.md#first-run-refusals-and-the-desk-notice)).
For agy, a verified rc=76 (`BRIEF_SUBMISSION_UNCONFIRMED`) whose newest saved
capture matches the harness login screen is sent as rc=88 with an
`AUTH_UNAVAILABLE` refusal; the unchanged hub classifies it as
`auth_unavailable`. Other rc=76 results and results with unverified cleanup
keep their original refusal. A pinned `--host` keeps any of these refusals as
its result. For an eligible auto-placement refusal, the hub commits the unbind
and the refused host and reason together, then wakes placement. The exclusion
survives hub restart within that attempt and is cleared by a cleanup-confirmed
new attempt. A cleaned launch-proof refusal can try another harness on the same
host before moving on. A request that already claimed its handoffkeep job must
not be moved to another host; that claim is host-bound.

### A job.spawn the node cannot accept

The node applies one argument rule (`internal/spawnargs`) to every `job.spawn`.
A value outside it, such as a `#` in `--operator-request`, used to drop the
whole message with a warn line, and the operation then waited out its deadline.
The node now answers such a frame: `invalid_args` with the flag and the rule in
the result's stderr section, or `invalid_request` for any other parse refusal
whose request id is readable. The operation ends `failed` with that reason at
once. A frame whose request id cannot be read stays a logged drop, and the warn
line now names the reason class (`json`, `unknown_type`, `argument_pattern`,
`invalid_request`).

The submitter applies the same rule before sending: `panewire spawn` (including
`--dry-run`) and `POST /v1/spawns` refuse the request and name the flag.

| Order | Result |
|---|---|
| Hub upgraded first, then nodes | A new node's refusal is parsed and ends the operation `failed`. |
| Node upgraded first (old hub) | The old hub's result parser does not know the new error codes, so it ignores the answer and the operation waits for its deadline as before. Nothing else changes. |
| Hub rolled back after nodes upgraded | Same as an old hub: refusals are ignored and operations wait out their deadline. |

The following are durable store states. For compatibility with older
submitters, operation and replayed-acceptance HTTP responses present `bound`
as the conservative pending status `dispatching`.

| Status | Meaning |
| --- | --- |
| `accepted` | Reservation durable; no node binding yet. Any refusals from this attempt remain recorded. |
| `bound` | Node UUID, payload and destination durable; no send transition has occurred. |
| `dispatching` | Send transition committed; the node may have received the request. |
| `reconciling` | Transport or execution observation unknown; ownership retained. |
| `deadline_exceeded` | Deadline observed; ownership retained, not terminal failure. |
| `succeeded` | Node landing receipt and fresh exact-pane identity verified. A landing the node refused rc 70 as "pane identity or status is unconfirmed" also settles here when a pane with the job's name and cwd is still present; the hub re-checks it once and launches nothing (hk 4534). |
| `failed` | Definite refusal, including an existing job claim; never permission to reuse a live job. |
| `retryable` | Node attested cleanup, or admission stopped before dispatch, and a replay can succeed once the operator's fix is done; reservations released. |

Before downgrading the hub to a version that does not understand `bound`, stop
the hub and ensure it cannot send or modify operations. Back up the durable
store, then use the store's normal SQL administration tool to return only
unsent bindings to placement:

```sql
UPDATE spawn_operations
SET node_id = NULL, node_payload = '', status = 'accepted'
WHERE status = 'bound';
```

Restart the older hub afterward. Do not rewrite `dispatching`, `reconciling`
or `deadline_exceeded`: those requests may already have reached a node and
must retain their UUID and destination.

A deliberate repeat of an identical cleanup-confirmed submission creates a new
attempt under the same logical key. Its node UUID changes, and a prior executing
host is pinned: handoffkeep host/task claims cannot move. Request, attempt, node
UUID and state checks prevent an old result or dispatcher from overwriting a
rebound request or a new attempt. Prior notifications
remain available under distinct attempt/transition event IDs.

A late success is accepted only after replaying the node receipt and a fresh
`pane.read` match by pane id, name, prepared checkout path and harness. A changed,
missing or unreadable pane leaves ownership unresolved unless that node confirms the pane is gone and the completed handoffkeep job matches the same host, pane, label, profile and task. This covers a job that finished and was reaped while the hub was disconnected. No cleanup command is
sent merely because a deadline expired.

### Failure matrix and evidence

| Failure | Recovery contract | Test / explicit non-goal |
| --- | --- | --- |
| Hub restart after reservation | Reopen the same operation; no second acceptance/host claim. | `TestSpawnReservationSurvivesRestartAndReplaysLostResponse`, `TestLaneInboxStoreSpawnDurabilityContract` (SQLite and CI Postgres). |
| Deadline before dispatch | An unbound or unsent bound request cannot have launched; release its reservation without waiting for a node slot and allow the same key/job to retry. | `TestAsyncSpawnUndispatchedTimeoutReleasesReservation`, `TestQueuedBoundDeadlineExpiresWithNodeSlotBusy_t4581`. |
| Containment or disconnect before send | Unbind unsent rows even when another request occupies the node slot; on a failed store write keep the original binding for recovery. | `TestContainmentUnbindsQueuedRowsDespiteStoreFailure_t4581`, `TestDisconnectUnbindsOnlyUnsentRowsWhenWriterIsStuck_t4581`. |
| Eligible refusal before a host-bound job claim | Atomically record the exclusion and unbind; recovery continues placement with the same logical attempt. | `TestRefusalExclusionSurvivesStoreRestartAndResetsOnRetry_t4581`, `TestRefusalWakesRealPlacementOnAnotherNode_t4581`. |
| Node disconnect or uncertain send | Retain node UUID/host; replay its journal on reconnect. | `TestSpawnUnknownRetainsOwnershipAndLateResultCommitsOutbox`, `TestAsyncSpawnLateResultReconcilesAgainstSameNode`. |
| Duplicate accept / lost HTTP response | Identical key/body replays; changed body conflicts; CLI receipt preserves key. | Reservation test and `TestSubmissionReceiptReusesKeyAfterLostHTTPResponse`. |
| Timeout followed by late pane | Retain ownership; reconcile receipt with exact node identity; never close working/unknown. | Late-result test and `TestFailedSpawnCleanupRequiresExactIdlePane`. |
| Timeout, and the node never started the request | Settle `retryable` with `deadline_not_started` (see [A request the node never started](#a-request-the-node-never-started)); an executing, restarted or silent node keeps ownership. | `TestLateNotStartedAnswerSettlesRequest_t4379`, `TestTimeoutWithoutNodeRowSettlesAsNotStarted_t4379`, `TestTimeoutKeepsOwnershipWithoutNotStartedProof_t4379`. |
| Node restart while executing | Never rerun the spawn. The restarted node settles the executing row from herdr and the job's local spawn record: the recorded pane alive as an agent completes it with the job's receipt (the hub still verifies the pane before accepting it), a pane in neither the agent nor the pane list completes it `lost` with a reason, and anything unproven (a pane only in the pane list, a label match without a record, herdr unreadable) leaves it executing and answers `unknown`. Past the deadline, `unknown` with no pane bearing the job label finishes the request `failed` (`orphaned_after_restart`) and releases its reservation; a labelled pane keeps it for the operator. The node's stop first waits (30 s) for a launch, injection or result upload in flight. See [node restart](runbooks/node-restart.md). | `TestSpawnSettle*`, `TestStopDrain*`, `TestNodeSpawnJournalRestartDedupAndLostResultReplay`, `TestNodeSpawnReplayAnswersUnknownAfterRestart`, `TestStrandedSpawnUnknownWithoutPaneFailsAndReleases`, `TestStrandedSpawnStaysPendingWithoutProof`. |
| Lost event or lost ACK | Commit state and outbox atomically; retry with a stable event id and actual job id. | `TestSpawnOutboxFailureRollsBackResult`, `TestAsyncSpawnLostEventRetriesWithStableJobIdentity`. |
| Process pressure / shutdown | Bounded execution and persisted pending work. | `TestAsyncSpawnWorkersBoundedAndIndependentOfHTTP`, `TestAsyncSpawnOutlivesSubmittingHTTPContext`; HTTP-independent execution lifetime is owned by maintenance/shutdown, never the POST context. |
| Backend migration | Never silently drop reservations or tombstones. | Spawn store contract refuses the inbox-only SQLite-to-Postgres migrator when spawn rows exist. Full backend transfer is a separate migration, not an inbox-only copy. |

### A request the node never started

A node that first sees a `job.spawn` after its deadline answers `rc=124
stage=claim reason=deadline_not_started retryable=true` without calling the
engine (a stalled read loop delivers frames minutes late, hk 4375). Because it
claims the request only then, that answer also means the node holds no journal
row for it. The hub settles on it:

| Node evidence | Hub outcome |
|---|---|
| `deadline_not_started`, while the request's own deadline still has room (the hub clock is behind the node's), auto placement | Unbind and try the next eligible host. |
| `deadline_not_started`, any other case (a pinned host, or the deadline passed) | `retryable`, reason `deadline_not_started`, stage `claim`, rc 124, with a fix text; the (task, role) slot is released, so the caller's replay starts a new attempt. The caller sees this refusal, not rc 75. |
| Landing wait ran out; the node answers the original frame or a replay with `deadline_not_started` within 5 s | Same `retryable` outcome. |
| Landing wait ran out; the node answers `reconciling` (row executing), `unknown` (stale executing row), `journal_unavailable` or `journal_conflict` | `deadline_exceeded`, ownership kept, as before. |
| Landing wait ran out; no answer within 5 s (disconnected node, stalled read loop) | `deadline_exceeded`, ownership kept. Silence is never read as not started; a later `deadline_not_started` from the coordinator's replay still settles it. |
| Completed row | Replayed result, judged as a late result (receipt plus exact pane read). |

When the landing wait ends, the dispatching worker replays the same request id
once and waits up to 5 s (`spawnNotStartedProbe`) before writing
`deadline_exceeded`. Past the deadline a node never launches a request, so the
replay cannot start a second one. The wire and journal formats are unchanged.

### Stranded requests

A request whose send transition committed stays `reconciling` or
`deadline_exceeded` until the node answers. The coordinator replays that
request to its node every few
seconds (the late-result path), so the hub asks about it well inside a minute
after the deadline. When a node restarted mid-spawn, its journal keeps the
entry executing until the node settles it from the pane (see
[node restart](runbooks/node-restart.md)); a node that cannot read herdr
answers such a replay `unknown` instead of `reconciling` (an older hub ignores
that answer and keeps the request pending, as before).

Past the deadline, an `unknown` answer from the bound, connected node makes the
hub read that node's panes (`agents.list`, at most once a minute per request):

- No pane bearing the job label (hk's recorded label, else the request's
  `-l`), from a complete pane list: the hub has that node mark the hk job
  `lost` with `end_reason` `orphaned_after_restart` when the record exists and
  is not terminal, finishes the request `failed` with reason
  `orphaned_after_restart` (`SPAWN_FAILED … replay=no`), and releases the
  (task, role) reservation. A new job id for the same task and role then
  claims at once. A crash after the lost write converges: a failed holder
  whose hk job is lost releases on the next claim.
- A labelled pane, an unreadable pane list, an hk read error or a node that
  refuses the lost write keep the request pending; the hub logs
  `hub kept a stranded spawn` with the reason.
- A real late success still wins: a completed journal result with a verified
  pane finishes the request `succeeded`, as above.
- A disconnected node, or one still running the request (`reconciling`),
  keeps it pending.

`panewire hub spawn abandon REQUEST_ID --reason TEXT` (operator token env;
`--hub-url`, `--hub-token-env`, `--hub-cf-env`, defaulting to
`PANEWIRE_HUB_URL`, `PANEWIRE_HUB_TOKEN_ENV`, `PANEWIRE_HUB_CF_ENV`) calls
`POST /v1/spawns/<request_id>/abandon` with `{"reason": TEXT}` (operator
credential). It applies to a pending request bound to a node. A `bound` request
has not crossed the send boundary, so the hub skips the pane probe. It still
checks for a non-terminal hk job from a prior attempt and, if one exists, marks
it lost through its owning node before releasing the reservation. For a possibly
sent request, only when that node lists no pane bearing the job label does the
hub mark the hk job lost (`end_reason` `operator_abandoned`), finish the request
`failed` with reason `operator_abandoned: <TEXT>` and release the reservation
(200, the CLI prints `ABANDONED request=… job=… status=failed`). The reason
travels in the `SPAWN_FAILED` event and the status API, and the hub logs one
WARN line `hub spawn abandon` with the request, job, host, verdict and reason.
A send or another abandonment in progress, a live labelled pane, an unreachable
node or a refused lost write answers 409
`spawn_abandon_refused` with the check that held it; a finished request answers
409 `spawn_not_pending`, an unbound one 409 `spawn_not_bound` (it releases
itself at its deadline). The CLI exits 5 on a refusal. Abandon does not stop a
spawn the node may still be running before its pane exists; use it for a
request whose node has restarted or whose deadline has passed.

A request still pending 30 minutes past its deadline warns once per attempt:
one Sentry warning `SPAWN_OWNERSHIP_STUCK <status>` fingerprinted
`spawn-ownership-stuck` (tags job, host, status, request, attempt), one fleet
event (`kind` `spawn`, `outcome` `stuck`, `reason` the status, `request_id`)
and one WARN line `hub spawn ownership stuck`. An outbox row
`<request_id>:<attempt>:stuck` (never relayed to a lane) is marked recorded
only after the warning is emitted, so a hub restart does not warn again, and a
hub that stops between writing the row and warning warns on the next pass: at
least once, never lost.

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

A terminal checkout refusal whose fix is to inspect and correct the node's
checkout is `failed` (`checkout_refused:branch-elsewhere` and
`checkout_refused:origin-mismatch`), whatever retryable flag the node sent. For
`--host auto`, a retryable `checkout_refused:dirty`,
`checkout_refused:sha-missing`, or `checkout_refused:branch-elsewhere` moves to
the next candidate after the hub unbinds the refused node. These three reasons
are not pinned to that host when the saved operation is replayed; placement
starts again. An exhaustion is the generic retryable `admission_refused` with
each tried host and its checkout outcome. `busy` still retries once on the same
host. The terminal checkout refusals that remain retryable (`dirty`,
`sha-missing`, `work_root_unset`, `git_too_old`) name a fix that ends in replay;
`spawnRefusalReplayable` holds that classification.

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

**Reviewer jobs a daemon started** (`--role reviewer`, job id starting `mwr-`;
[how the hub recognises them](quiet-delivery.md#records-kept-in-hk-only)) are
routed differently when the owner is a design lane (hk 4270, decision 152).
SPAWNED is kept in hk only. A failure the saved request can be replayed after
(`replay=yes`, status `retryable`) goes to the submitting lane at once, as
above, and is held for the owner when that lane differs from the owner (the
requester then has its own copy; with none, the owner's row is the only notice
and is delivered at once): the design lane hears nothing for that
attempt while the requester can replay it. A later attempt supersedes the held
row, so the design lane is told the request's last outcome once (a terminal
`replay=no` failure, or no event when the replay lands). A request nobody
replays within 30 minutes (`designQuietTimings.spawnFailureHold`) is delivered
to the design lane with the failure's own text, so a requester that gave up
cannot leave it unreported. The 30 minutes is the aggregation boundary: "after
the last replay" cannot mean a retry that may never come. A held row has
`delivered=2` in `spawn_notifications`; at expiry it is claimed (`delivered=3`)
in one statement conditioned on the request still being at the row's attempt
and `retryable`, so a replay that advances the attempt first wins and only the
current attempt's notice is published; a crash while publishing leaves `3`, and
the next flush publishes again (the event id makes that idempotent). A row is
closed (`delivered=1`) when delivered or superseded; every row stays there, and
the requester's copy is an ordinary lane event. A design session's own spawns, and every other owner, are unchanged.

`reconciling` (the node's answer is unknown, or the node is disconnected) writes
no `spawn_notifications` row, so neither lane is notified. The owner keeps
ownership. A request that later lands notifies one `SPAWNED`; one that fails or
passes its deadline notifies one `SPAWN_FAILED`, as above. The hub journal logs
the transition as `hub spawn reconciling`, not as a refusal, and captures no
Sentry event for it.

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
