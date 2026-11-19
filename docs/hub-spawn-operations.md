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
`--wait` observes the request state first, preserving admission/node refusal codes,
then uses the existing job-completion wait and timeout behavior. See
[Request states a client reads](#request-states-a-client-reads). Delegate panes
use the same submission path.

For a stale or unobserved native quota account, the hub schedules a node
refresh independently of placement. If no confirmed eligible account can run
the request, it stays unbound in `accepted`, keeping its reservation until a
refresh permits placement or its existing deadline expires. A completed refresh
wakes another placement pass; a failed or timed-out refresh is retried while
the request remains pending. An already-confirmed account can be selected
without waiting for the stale accounts. A pending measurement produces no
terminal refusal event or placement record. If the pass already contacted a
host, its refusal remains in the durable candidate trace while the request
stays accepted; the attempt's one record is posted when it ends. The hub
logs a changed refresh failure with its machine, pool and reason; repeated
identical failures are not logged on every retry. A request that times out
while parked on a refresh names that machine and pool and the last finished
attempt there in its outcome and fix text (`quota_refresh` on the saved
result), instead of the queue-starved cause: the spawn queue never held it.
That attempt is reported as a failure with its reason, as finished without one,
or as not yet finished, so the timeout never blames a measurement the hub did
not see fail. The request's refresh keys and the last attempt per key are
stored in the hub database, so both survive a restart; a pass that ends without
parking on a refresh clears the request's link.

A refresh wait never exceeds what is left of the request (hk 5456, narrowed by
hk 5480). Each queued measurement's deadline is the measurement window or the
attempt's remaining budget less a five second use margin, whichever is smaller,
so it cannot outlive the request that asked for it, and the hub reports whether
it queued or joined one: a request with less budget left than that, or one the
scheduler suppressed in its post-completion cooldown, does not start the wait.
That seat is then dropped for the request with reason `refresh_unavailable` and
placement takes the next candidate inside the same request. No refresh state
survives a hub restart: an accepted request re-evaluates from scratch, and may
ask for a refresh again, bounded by its remaining time.

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

## Request states a client reads

`GET /v1/spawns/<request>` returns `status`. Every client (`spawn --wait`, the
job-directory TTL pass) knows the design's state set
(hk design/2026-11-13/hub-owned-spawn-queue §2) before the hub emits any of it
(hk 4659). The hub emits `accepted`, `dispatching`, `reconciling`, `succeeded`,
`failed`, `retryable` and `deadline_exceeded`, since hk 4660 `given_up` too
([Time limit](#time-limit)), since hk 4736 `waiting`, `refused` and
`cancelled` for [Start conditions](#start-conditions), and since hk 4661
`parked`, with top-level `reason` and `next_check_at` ([Parked](#parked));
`pending`, `launching` and `landed` stay unemitted until their parts.
`TestHubSpawnExternalStatesUnchanged` keeps that set, and every state in it
is one the clients know. An answer also carries `expires_at` (RFC 3339,
the request's time limit) for a request that has one.

| State | `spawn --wait` |
|---|---|
| `accepted`, `waiting`, `pending`, `bound`, `dispatching`, `launching`, `reconciling` | keeps waiting, silently |
| `parked` | keeps waiting and prints `wait request=<id> state=parked reason=<r> next_check_at=<t>` on stderr when either changes. The client reads `reason` and `next_check_at` from the top level of the answer (`result.reason` as a fallback) and tolerates their absence. |
| `succeeded`, `landed` | the request phase ends; the job-completion wait follows |
| `failed`, `retryable`, `refused` | exits with the hub's recorded `result.rc` (81 when none) |
| `cancelled`, `given_up` | exits 84; nothing was spawned. The message names the state, the hub's reason and `expires_at`. |
| `deadline_exceeded` | exits 75, ownership unresolved |
| any other name | keeps waiting and prints `unknown state <x>: still waiting` once on stderr; never success, never failure |

`--wait-timeout` (default one hour) bounds the request phase, and then bounds
the job-completion wait again. A request still in flight at the bound exits 85,
and one still `parked` exits 91 (see [exit codes](config.md)); the request stays
queued on the hub either way. The bound uses the wall clock.

Observe an accepted request with `panewire spawn status <request_id>` (`--json`
for the record), never by rerunning `spawn`: a replay of a `retryable` request
starts a new attempt and restarts its time limit
([Time limit](#time-limit)). The command makes one read of
`GET /v1/spawns/<request_id>`, writes no saved submission and does not wait.
It prints one line — `request=`, `state=` and,
when the answer carries them, `reason=`, `next_check_at=`, `expires_at=`,
`rc=`, `stage=` and `job=` — and exits by the same mapping as `spawn --wait`:
0 for `succeeded`/`landed`, the hub's `result.rc` (81 when none) for `failed`,
`retryable` and `refused`, 84 for `cancelled`/`given_up`, 75 for
`deadline_exceeded`, 91 for `parked`, and 85 for any other in-flight or unknown
state.

A submitter-side job directory the hub never held a record for may be removed
once its request is `failed`, `refused`, `given_up` or `cancelled`.

## Time limit

hk 4660; design hk design/2026-11-13/hub-owned-spawn-queue §3 rule 2.
Start conditions through the queue are hk 4736; see
[Start conditions](#start-conditions).

Every accepted request has `expires_at` (`spawn_operations.expires_ms`,
reported as `expires_at` by `GET /v1/spawns/<request>` and in the 202 answer).
It is set at acceptance: 6 h for an implementer, 1 h for a reviewer or consult,
or `panewire spawn --expires DUR` (a whole-second Go duration such as `90m`,
from 1 minute to 24 h; the request carries `expires_seconds`). A hub that does
not advertise `spawn_expires_v1` in `GET /v1/spawns/features` makes the CLI
refuse `--expires` rather than drop it. A deliberate replay of a `retryable`
request starts its limit again. These are constants in this part
(`internal/fleet/spawn_expiry.go`), not hub config.

An attempt's `job.spawn` frame carries that absolute expiry as `expires_ms`, but
only to a node that advertises `spawn_expires_v1` (`hubSpawnCapabilities`): an
older node's strict frame decoder refuses an unknown field. The node's spawn
writes it into the `job.spawned` receipt as `expires_at`, which the node's pane
sweep reads to end a reviewer or consult that never posted a result
([pane-sweep.md](pane-sweep.md#reviewer-and-consult-ends), hk 4747).

A planned consult's selection stamp rides the `job.spawn` args as
`--consult-stamp` (hk 4875). The hub drops that flag for a node that does not
advertise `consult_stamp_v1` in its hello: an older node's argument rule
refuses an unknown flag and would drop the whole spawn. Such a node records the
consult without the stamp; its seat and the job are unaffected.

The 600 s landing wait (`deadline_seconds`) is now one attempt's bound, or the
limit when that comes first. A staged author-replacement successor
(`activateStagedSpawn`) is capped the same way; one activated at or past the
limit is given up unsent on its first pass. When the wait ends an attempt whose
start is *proven absent* (the hub never sent it, or the node attested
`deadline_not_started` or the start proof fenced it after the attempt's
deadline), the request is not over:

| Attempt ends, start proven absent | Result |
|---|---|
| before `expires_at` | the same row, reservation and request id start attempt N+1 as `accepted`, with a fresh wait of 600 s (at most to `expires_at`). No notice. The ended attempt's placement record is sealed first, as a client's replay does. |
| at or after `expires_at` | `given_up`: terminal, reason `expired: expires_at <t> passed after <n> attempt(s); no candidate could take it (tried: <pool/host: reason; …>)`, or `… ; it never found a candidate that could take it` when the attempt recorded none, rc 84, stage `queue`. The tried list names every candidate the attempt asked and why it was dropped (a pool dropped as `refresh_unavailable`, a host whose quota refusal or readiness check dropped it), and `panewire spawn status` prints it as `tried=[…]` (hk 5456). One `SPAWN_FAILED` for the owner (and the submitting lane); the job id and task/role reservation are freed as for `retryable`. |

A request whose start is **not** proven absent (`dispatching`, `reconciling`,
`deadline_exceeded`) is never given up, before or after `expires_at`: the limit
does not apply while the node may have started it. Part 1's proof
([Start proof](#start-proof)) or the node's own answer settles it, and only
then does the table above apply. A row with no limit (`expires_ms=0`, written
before the column or by an older hub) keeps the previous behaviour: its timeout
ends it `retryable`. A not-started answer inside the attempt's deadline is a
refusal and ends as before (next host, or `retryable`). The hand-over takes
the placement-trace lock a client's replay takes; if the trace cannot be
restored (a held legacy conflict) the end of the attempt is not recorded: the
request stays as it is with its reservation, and a later pass settles it once
the trace can be restored.

### Start conditions

hk 4736; design hk design/2026-11-13/hub-owned-spawn-queue §4 (decision 188).
`panewire spawn --after task:N=merged|deployed|done`, `--after blockers[=merged|
deployed]` and `--after time:RFC3339` (several are ANDed) submit as usual once
the hub advertises `start_conditions_v1` in `GET /v1/spawns/features`. A
conditioned request is accepted with status `waiting`; it keeps its reservation
and its `expires_at`, and holds no placement slot.

One coordinator check, at most every 60 s (the hub's clock), gathers every
`waiting` row and reads the tasks they name in ONE batched handoffkeep call
`GET /v1/tasks/states/snapshot`, chunked at 200 ids per call, so the number of
hk reads follows the ids and not the requests. `blockers` predicates read the
target's open blockers from that snapshot and settle those ids in a second
read of the same check. Each row then:

| Check result | Row |
|---|---|
| every predicate holds | `accepted`, with a fresh attempt deadline (`deadline_seconds`, capped at `expires_at`), picked up by the next placement scan |
| still waiting | stays `waiting`; no re-read until the next check |
| a named task is in hk's `missing` | `refused`, rc 78, reason `task:N=missing`; reservation freed, one `SPAWN_FAILED` for the owner and the submitting lane |
| the named task is `dropped` or `duplicate` | `cancelled`, rc 84, reason `task:N=<state>`; same release and notice |
| `expires_at` passed | `given_up` as in [Time limit](#time-limit), whether or not the condition holds. Expiry is evaluated before the hk read, so a row is ended at its limit even during an outage |

An hk call that fails fails the check's evaluations: every unexpired row stays
`waiting`, one warning is logged per outage (not per check or row), and the
failure clears once a read answers. A read that does not name every requested
id also fails closed as waiting, so a condition is never decided from a partial
answer. Each waiting row stores its last evaluation (`condition_result`), and
`GET /v1/spawns/<request>` returns it as `start_condition` with the predicates
that still block it.
`TestStartConditionWaitsThenReleasesOnMergedAndDeployed`,
`TestStartConditionTimeReleasesOnTheClock`,
`TestStartConditionMissingTaskIsRefused`,
`TestStartConditionHKFailureKeepsWaitingAndLogsOnce`,
`TestWaitingRowGivenUpDuringHKOutage`,
`TestStartConditions250IdsReadInTwoSnapshotCalls`,
`TestWaitingRequestGivenUpAtItsLimit`, `TestWaitingRowsSurviveRestart` and
`TestWaitingStatusReportsBlockingPredicates` cover each row of the table and
the batching.

The snapshot read comes from the pinned handoffkeep client `v0.0.295`
(`Client.TaskStatesSnapshot`, hk 4735). A hub on an older client cannot check
conditions and must not accept `--after`: it does not advertise the feature,
and the CLI refuses the flag before submitting.

A conditioned row that reaches an executable, unbound status without passing
the queue is returned to `waiting` instead of launched: a legacy row, a
replayed `retryable` request, a staged activation, or a bound row a node
refused (`unbindSpawn`, `resumeUndispatchedSpawn`). The invariant is
`conditions_met_ms`: it is zero until the queue sees every predicate hold, and
only a row with it set (or a bound legacy row, which keeps its own guard) may be
placed. `TestUnboundConditionedRowReturnsToWaiting` covers both `accepted` and
`deadline_exceeded`.

The startup migration (`0017_spawn_waiting_conditions.sql`, SQLite: an
idempotent `UPDATE` at open) is a fast path for unbound conditioned rows an
older hub left executable — `accepted`, `deadline_exceeded` — and moves them to
`waiting`. It never takes a row another owner holds: `staged` belongs to the
ladder (`activateStagedSpawn`/`cancelStagedSpawn`/`discardStagedSpawn` CAS on
that status), and a `retryable` row's reservation was released by
`completeSpawn`, so the replay path re-creates it and the execute guard waits
the row then. A bound legacy row is not moved either; it keeps the guard it had
(not replayed while its condition is unmet and its deadline is live).
`waiting` is not a settled state for any reader: `spawnStatusActive` counts it,
and an escalated conditioned successor reports `pending` rather than a false
quota refusal.

## Parked

hk 4661; design hk design/2026-11-13/hub-owned-spawn-queue §2 (`parked`),
§3 rules 3 and 4, §9a. Start conditions are [above](#start-conditions); the
owner notice for a long-parked request is [Parked notice](#parked-notice).

**Parked, not ended.** When placement finds no candidate for a request that
has a limit (`expires_ms>0`), the request is parked instead of ending
`retryable`. The refusal is read by `spawnParkReason` (`hub_spawn_park.go`):

| Placement refusal | `reason` | Woken early by |
|---|---|---|
| `launch_backoff` named, or the request's own back-off (below) | `launch_backoff` | its timer only |
| every eligible candidate is in a pool at its concurrency cap | `pool_cap` | a job leaving a node's active set |
| `QUOTA_*`, `quota_exhausted`, `quota_unconfirmed`, rc 86/87/89 | `quota` | a finished quota refresh |
| a node did not connect / is not connected (`hub_node_unavailable`) | `node_disconnected` | a node connect |
| rc 81: no awake host admitted / no host admitted / no candidate accepted | `host_full` | a node connect, a job leaving a node |
| `role_no_candidate` (no profile passes), `NO_ELIGIBLE_HOST` | `no_eligible_host` | a node connect |
| a free limit ended by a time that fits the request's limit (see [A full quota](#a-full-quota-moves-an-automatic-request-on)) | `quota_reset` | its timer only (the limit's end) |

Any other refusal ends the request as before. A row with no limit is never
parked. A parked row stays `status='accepted'`, unbound and holding its
reservation, with `park_reason` and `next_check_ms` (migration
`0018_spawn_parked.sql`). The coordinator scan skips an accepted row until
`next_check_ms`; `GET /v1/spawns/<request>` reads `parked` with `reason` and
`next_check_at` while it is ahead, and `accepted` once it is due. The next
check is one back-off base ahead, spread ±20 % (24–36 s by default), and never
past `expires_at`, so a parked request is looked at when its limit passes and
is given up then as in [Time limit](#time-limit). A parked request holds no
account and no placement count.

**Event wake.** A node connect, a job leaving a node's active set and a
finished quota refresh note the reasons in the table. On its next tick the
coordinator makes at most `spawn_wake_batch` (8) parked rows with a noted
reason due now, earliest next check first; when the batch was full the
reasons stay noted and the next batch waits for the next timer tick (2 s; the
tick is in memory, the batch's rows are read because the woken rows kick the
scan, hk 5302),
however often the coordinator is kicked meanwhile. An event noted for a
park's reason while its placement ran is noted again once the park lands, so
a drained wake is never lost; `launch_backoff` is never event-woken. A
refusal with several blockers (rc 80 `no remote candidate completed` with
mixed tried notes) stores every reason (`host_full,quota`) and wakes on any;
the status API shows the first. A mock lane's rc 80 is not parked. A parked,
unbound row holds no pool slot (`liveSpawns`), a claimed-harness retry is not
backed off (hk holds its claim on that host), and a parked pass emits no
terminal refusal event. The first cleanup-confirmed refusal after the hk job
claim is kept in `claim_result`: written with the result that proves it
(a definitive post-claim node result; never a timeout, which is uncertain or
the not-started evidence of [Time limit](#time-limit), and never a
pre-claim refusal or a not-started attestation) and never replaced. A request holds a claim when it has a `claimed_host` pin or a
`claim_result`: a pin cannot always tell a claim from a replay pin, so when
in doubt the claim is kept. A saved result that would pin a replay (a post-claim node
result, including one an older hub saved without `claimed_host`) holds too.
A hub timeout of an unsent attempt and a node timeout at a stage before the
claim pin nothing. A node's cleaned-up timeout from the claim stage on may
come after the job claim: it still renews the attempt, but pinned to that host (so the claim is
held from then on), and at the limit it ends `retryable` with its own result,
never `given_up`.
A request
holding one may park, but
it is never renewed or given up as unstarted: an unsent timeout, wherever the
deadline passed, ends it with that saved refusal. A replay pin alone
(`claimed_host` after a checkout refusal) proves no claim. Wakes are hub memory: a restart forgets them and the
`next_check_at` timer covers it.

**Back-off.** A failed attempt is a host refusal that hands the request to
another host (`unbindRefusedSpawn`, the next-host and claimed-harness paths)
or an attempt the node attested it never started after its deadline. It
increments `failed_attempts` and parks the request `launch_backoff` for
`spawn_backoff_seconds` × 2^(n-1) (30 s), at most
`spawn_backoff_max_seconds` (600 s), spread ±20 % and then capped at that
maximum again, and never past `expires_at`. The spread is drawn from
`HubServer.spawnRand` (tests inject it) and the times are on the hub clock.
A later pass that finds no other candidate never shortens a running back-off.

An attempt the hub never sent (the queue timeout) is not a failure and starts
the next attempt at once. The per-host `launch_backoff` of hk 4380
([placement.md](placement.md)) is separate and counts host faults only.

**Exclusions across attempts.** `exclusions` now carry into the request's
next attempt: a host (or, for a launch-proof or claimed-host refusal, a host
and harness) refused for a request stays excluded for every later attempt of
that request, and for no other request. Only a client's deliberate replay of
an ended `retryable` request starts them over. Exclusions have no `until` in
this part.

The numbers are `placement.json` keys (0 or absent: the defaults, unmeasured,
design §9a and §9b Q2): `{"spawn_backoff_seconds": 30,
"spawn_backoff_max_seconds": 600, "spawn_wake_batch": 8,
"spawn_park_notice_seconds": 1800}`.

### A full quota moves an automatic request on

hk 5358, operator decisions 433, 434 and 435. A node's quota gate can refuse the
seat selection chose even though the hub's selection admitted it: the hub reads
the account's quota from the shared store at selection, the node reads the store
plus its own free-limit record at the gate (the two differ while a limit is
recorded on the node and not yet published, or while the store is ahead of the
hub's read). Until hk 5358 that refusal (`spawn_failed`, rc 3,
`quota gate: free limit exceeded until <T>`, status `retryable`) ended the
request and the requester replayed it.

The refusal now has its own code, `quota_full` (reason `spawn_failed:quota_full`;
the gate's three full-quota reasons: `free limit exceeded until`,
`quota exhausted window=` and `credit_exhausted balance=`; an unmeasured reading
or a policy refusal is not one). For an **automatic** request (`-m auto`,
`--host auto`) that refuses before the job claim (stage empty, `placement` or
`checkout`) the hub keeps the request: the same request id, job and attempt stay,
and `nextHostAfterRefusal` (`hub_spawn_quota_full.go`) unbinds the attempt,
records an exclusion, and wakes placement. The requester sees one success. A
**pinned `-m`**, a pinned `--host`, a refusal after the claim and a refusal the
node marks not retryable keep today's behaviour: the refusal is the answer.

| The limit's end | What the hub does |
|---|---|
| ends inside the request's time limit (`--expires`, the requester's time for the job) less `spawnQuotaStartMargin` (1 min) | holds the request, park reason `quota_reset`, until the end (+1 s). Timer-only: no event wakes it, so it does not start on another pool first. At the end the same pool is selected again. The wait never passes `expires_at`. |
| ends later, names no end (a window or a credit balance), or the request has no time limit | moves on at once: no back-off, the refused seat (host and profile) recorded in the exclusion with the account it ran on, so the next candidate that fits the grade, role and tags is chosen inside the same request. For the SWE-2 free rungs the next attempt also adds `--exclude-pool devin-free`. |

The budget is the request's own `--expires`; no new flag. A requester that wants
a 25-minute budget for a review pass passes `--expires 25m` (the default is 1 h
for a reviewer, 6 h for an implementer). The refusal's end time is used at once:
the refused node records and publishes it (`panewire quota refresh devin`
merges the host's `devin-free-limit.json` over the account's shared reading,
[quota stop](quota-stop.md)), and the hub cools the free band in memory until
the end for every other request. The shared reading is the copy that survives a
hub restart. Cooling is by band (`devin-free`), as the exhausted-pool sweep does it. Only the
free band is excluded as a pool: it is the one Devin account's shared free
bucket. Every other pool holds several accounts and a refusal describes only the
selected one, so it excludes the seat and carries the account key, never the pool
(a CommandCode `credit_exhausted` also sets the hub's durable account mark, hk
5138, so selection skips that account alone). The exclusion is for this request
only; a refusal that names no end excludes the seat for the request's life. A
refusal of such a pool therefore moves on to the next host or account, and
reaches another pool only through the refreshed reading, until selection takes an
account exclusion of its own (not in hk 5358).

When no candidate is left the request ends with the placement's own refusal
plus one note per refused seat (host, profile, pool or account, the reset): a
`no candidate` answer caused by the request's own full-quota refusals is not
parked as a lack of hosts. It names every candidate tried and why it was dropped
(`hub_quota_full` for each refused seat). The refusal is not a launch fault: it does not count toward the
host's `launch_backoff`. Measure: the day's `spawn_failed:quota_full`
outcomes in the hub's spawn records are the refusals that still reached a
requester (before hk 5358 the same refusal read plain `spawn_failed` with the
`quota gate:` text); the continuations are the hub log's
`hub spawn refused by node; trying next candidate` lines with that reason.

### Parked notice

hk 4662; design §6 (notices to the owner lane) and §9b Q2. A sweep of the
parked requests tells an urgent request's owner once when its wait (below)
reaches `spawn_park_notice_seconds` (1800 = 30 min by default), as one `info`
line:

`[panewire] SPAWN_PARKED <job> request=<id> reason=quota parked_since=… next_check_at=… unpark=…`

`unpark` is the wake of every reason in the stored set (the table above).
Urgency is the request's own decision, exactly as placement read it at submit:
an `--urgency` flag the submitter passed wins (`urgent` tells, `normal` stays
quiet), and only a request without one asks its bound hk task's priority
(`park_priority`, read once per wait; 1 or 2 is urgent). A request with no
task and no flag is not urgent, and one with no owner lane is never told.

The sweep (`checkParkedSpawnNotices`, `hub_spawn_park_notice.go`) runs on the
coordinator's tick, 60 s apart, and examines at most 64 candidate rows from a
keyset cursor, earliest wait first. Its progress never depends on a row's
outcome: the cursor moves past every row it examines, whatever happens, so no
row — a P3 backlog, a task hk answers badly for, a rejected notice — can hold
the page. A row leaves the candidate set when it is judged: the durable
`park_judged_reason` names the reason set it was judged under, told or not. The
cursor wraps at the end of a sweep; it lives in hub memory, so a restart starts
the sweep at the head, which only repeats a sweep.

A row whose task priority cannot be read is handled by what failed:

| Read outcome | Effect |
|---|---|
| the task is gone, or the request names no task | reads as 0 (never urgent) and is judged |
| a 4xx refusal for this task, or a 2xx answer that cannot be parsed | `park_priority_failures` counts the sweep; after 3 the task reads as 0 and the row is judged |
| hk unreachable, 5xx, 429/408, 401 (an outage) | the row stays unjudged and the sweep stops after 2 such reads; the cursor resumes after them |

A notice hk refuses to persist for good judges the row as not told and the
sweep goes on; a transport failure, a 5xx, or a retryable 4xx (401, 403, 408,
425, 429 — the inbox batch's own permanent set) leaves it unjudged, and a later
sweep resends the same event.

The notice is at most one per reason set for the request's life: nothing
repeats while the request stays parked for the reason the notice named, a park
that names other blockers may send one more, and a later attempt that parks
again for an already-judged reason stays quiet. The judgement is durable —
`park_judged_reason` (the reason set last judged, told or not) and
`park_notice_ms` (when the owner was told; `0019_spawn_park_notice.sql`) — so a
restart neither repeats nor loses a notice: a row parked before the restart is
told by the hub that starts after it. The event id also names the wait start
and the reason set (`spawn-park:<request>:<park_since_ms>:<reason>`), so a
resend of the same notice is the same durable handoffkeep event.

`park_since_ms` is the request's wait: it starts at its first park, survives
every re-park and a bind a node then refuses (so the wait that is counted is
the whole one), and starts over with a new attempt — the expiry replay and a
deliberate replay of a retryable request both reset it, the priority read and
its failure count, keeping the judgement. A notice is recorded only after
handoffkeep took the durable row (or refused it for good).

### Rolling back past parking

`0018_spawn_parked.sql` only adds `park_reason`, `next_check_ms`,
`failed_attempts` and `claim_result` (default `''`, 0, 0, `''`); an older hub neither reads nor writes
them. A parked row is `status='accepted'` with `node_id IS NULL`, so an older
hub's coordinator scan lists it and its bind CAS matches it: the older hub
places it at once, without back-off or wake batch, and its GET shows
`accepted`. Nothing is stranded and nothing starts twice. A row it binds keeps
a stale `park_reason` that a newer hub ignores (a bound row never reads
parked) and clears at its next bind. `TestOlderHubPlacesParkedRowsAsAccepted_t4661`
runs those predicates against a parked row.

`0019_spawn_park_notice.sql` adds only `park_since_ms`, `park_priority` (-1),
`park_notice_ms`, `park_judged_reason` (0 and `''`) and
`park_priority_failures` (0); an older hub neither
reads nor writes them either, so a rollback loses no queue state. A newer hub
reads a stored judgement as already judged for that reason set, so it repeats
nothing, and it re-reads a priority only while `park_priority` is still -1.

**Roll back only with an empty spawn queue, and keep it empty.** The
pre-parking hub's time limit (part 3, #587) ignores `claimed_host`,
`claim_result` and a saved post-claim result. Past `expires_at` it gives up
an attempt it reads as unstarted, which can include a node timeout after
the job claim. It then frees the reservation while hk may still hold the
job. A snapshot of claim-holding rows is not enough, so the gate is the
whole queue, with every producer frozen:

1. Restart the newer hub with `--spawn-rollback-drain`. The hub keeps
   running and its coordinator keeps placing, parking, waking and expiring
   queued requests. But no new spawn row can be created
   (`claimSpawnReplacing` refuses every producer):
   - `POST /v1/spawns` and `POST /v1/consults` answer 503
     `spawn_rollback_drain`, the consult before it stores any intent;
   - `POST /v1/jobs/escalate-author` refuses a new ladder stage
     (`claim_refused`) before it touches the author pane, but a replay of a
     step already staged still completes it;
   - a quota-stop hand-off starts no successor;
   - a consult creates, recovers or respawns no spawn (the consult fails).

   Staged ladder steps already queued still settle. Do not SIGTERM-drain
   yet: that stops the coordinator, and a parked row would never settle.
2. Wait until no spawn row is nonterminal, any status included (`*_grant`
   variants too), and no consult is open:

   ```sql
   SELECT request_id, job, status, claimed_host, expires_ms FROM spawn_operations
   WHERE status NOT IN ('succeeded','failed','retryable','given_up','refused','cancelled');
   SELECT id, state FROM consult_requests WHERE state IN ('queued','running');
   ```

   Each row must land, end, or be cancelled. Do not raise limits to get
   past this step. Under the flag the hub logs this count at start and each
   minute as `hub spawn rollback rows rollback_rows=N`
   (`spawnRollbackBlockingRows`): the gate is `rollback_rows=0`.
3. Wait for every spawned job that is still running to finish. On the older
   hub, a running job's quota-stop would start a successor under part 3's
   claim-unaware expiry, and that hub has no flag to stop it.
4. Stop the newer hub ([hub-drain.md](hub-drain.md)) and re-run both queries
   from step 2: they must still return nothing. Remove
   `--spawn-rollback-drain` from the service command (the older binary
   rejects the unknown flag and would restart-loop), reload the service
   configuration, then start the older hub. Keep spawn submissions,
   consults and author escalations stopped for as long as it runs, and roll
   forward before reopening them.

### Rolling back past the time limit

The migration is additive: `0015_spawn_expires.sql` adds `expires_ms`
(SQLite: `ALTER TABLE` at start) with default 0, which an older hub neither
reads nor writes. Rows that hold `given_up` are a new value of `status`. An
older hub (checked against the code of e00bb952) lists it in no status query:

| Older-hub query | What it does with a `given_up` row |
|---|---|
| Coordinator replay, `status IN ('accepted','bound','dispatching','reconciling', …_grant)` | ignored |
| Stale-deadline replay, `status IN ('deadline_exceeded', …_grant)` | ignored |
| Outcome write, `status IN ('accepted','bound','dispatching','reconciling','deadline_exceeded', …_grant)` | matches no row: never mis-terminal |
| Placement recovery and live view (`bound`, `dispatching`, `reconciling`, `succeeded`, `accepted`) | not listed: no pool slot or placement count |
| Role release, `status IN ('succeeded','failed')` | not applied; the reservation was already freed |
| `GET /v1/spawns/<request>` | passes `status` through (`given_up`); a `spawn --wait` from before hk 4659 (#584) does not know the state and exits with the state-conflict code ("unknown spawn state; inspect operation"), never as success; the request is over on the hub. Upgrade clients before the hub, as #584 was ordered |

So a rollback strands nothing: `given_up` rows are terminal and ignored. One
thing differs from a newer hub, and it is safe (nothing starts twice): a
request that was renewed to attempt N has the older hub's single deadline
again, so its next timeout ends it `retryable` or `deadline_exceeded` rather
than starting another attempt. `TestOlderHubQueriesIgnoreGivenUpRows_t4660`
runs the predicates above against a `given_up` row.

### Rolling back past start conditions

Decision 254: the migration is additive. `0017_spawn_waiting_conditions.sql`
adds `conditions_met_ms` and `condition_result` (SQLite: `ALTER TABLE` at
start) with defaults an older hub neither reads nor writes, and moves unbound
`accepted` and `deadline_exceeded` conditioned rows that never passed the queue
into the `waiting` status (SQLite runs the same idempotent `UPDATE` at open).
It deliberately leaves `staged` (ladder-owned) and `retryable` (reservation
already released) rows to their owners. A released row carries its met
time, so a later open does not move it back to waiting. Rolling back is safe:
an older hub (checked against the code of e00bb952) lists `waiting`, `refused`
and `cancelled` in no status query, and a `waiting` row holds a reservation
without a placement slot, so nothing starts from it.

| Older-hub query | What it does with a `waiting` row |
|---|---|
| Coordinator replay (`accepted`, `bound`, `dispatching`, `reconciling`, …_grant) | ignored |
| Stale-deadline replay (`deadline_exceeded`, …_grant) | ignored |
| Outcome write (`accepted`, `bound`, `dispatching`, `reconciling`, `deadline_exceeded`, …_grant) | matches no row: never mis-terminal |
| Placement recovery and live view (`bound`, `dispatching`, `reconciling`, `succeeded`, `accepted`) | not listed: no pool slot or placement count |
| Role release (`succeeded`, `failed`) | not applied; the reservation stays held until the rollback is undone or the request ends on a newer hub |
| `GET /v1/spawns/<request>` | passes `status` through (`waiting`); since hk 4659 (#584) a waiting client keeps waiting silently. A client from before that exits with the state-conflict code for an unknown state, never as success |
| Submit with `--after` | the older hub answers `400 start_conditions_unsupported` and advertises no `start_conditions_v1`; the current CLI checks the feature first and refuses the flag rather than drop it |

So a rollback strands a waiting request rather than starting it early: the row,
its reservation and its `expires_at` remain for the next hub on this database.
It is given up at `expires_at` only on a hub that reads the column and the
queue, so a rollback leaves it waiting indefinitely until the upgrade returns.
`refused` and `cancelled` rows are terminal and ignored the same way.

## Acceptance and identity

`POST /v1/spawns` requires the operator credential. Per-host service grants do
not grant permission to let placement select an arbitrary host. The bounded
request contains a UUID, argv, UTF-8 brief (at most 64 KiB), originating machine,
pinned repository checkout and deadline seconds (default 600, maximum 3600).
The submitting CLI verifies the checkout is on a pushed origin ref. A canonical
checkout gets an isolated target worktree rather than running in the node's
canonical clone. A spawn that carries the spec itself
(`--checkout-origin`, `--checkout-commit`, `--checkout-ref`) names the same
pinned checkout without any local checkout, and the CLI asks for
`checkout_spec_v1` first so a hub that would ignore the spec refuses the flag
instead of running the job in the node's mapped path (hk 4738).

A spec may be sparse (hk 5098): `exclude[]` and `include[]` name the paths the
node's worktree leaves out and the changed paths it keeps. `GET
/v1/spawns/features` lists `checkout_sparse_v1`, which is how a client learns
the hub carries the fields; the CLI checks it for `--checkout-exclude` and
`--checkout-include` exactly as it checks `checkout_spec_v1`. The hub forwards
the lists only to a node whose hello advertises `checkout_sparse_v1` (its git
is 2.37 or newer) and drops them for every other node, whose strict frame
decoder would refuse the fields; that node prepares the full checkout, a
superset that is never wrong. The node answers `checkout_mode` (`sparse` or
`full`) and the same value lands in the job record. The hub names
`checkout_sparse_v1` in the once-per-connection `hub.features` frame too: a
node sends `checkout_mode` in its spawn result only to a hub that listed it,
because an older hub's strict result parser drops the whole answer for one
unknown key. The lists are not part of a spawn's base replay identity
(`spawnFingerprint`): they are added or dropped with the target node's
capability, so a replay after that node's upgrade or rollback still matches its
journaled request. The journal separately preserves the sparse checkout
identity (`sparse_hash`) and `checkout_mode` durably across capability
filtering, restart and replay (hk 5104 / hk 5211): a replay whose lists were
stripped or added across capability filtering (one side empty) is accepted,
preserving the truthful execution mode of the executed row (a stripped replay
of a sparse row does not report sparse, while a replay of a stripped row
reports full); a replay whose lists are present on both sides and differ
conflicts (`journal_conflict`).

A spec may also be a successor checkout (hk 5118): an implementer request whose
`--checkout-ref` is `refs/heads/<branch>` carries `checkout` mode `successor`,
with `branch` and the pinned `sha` required and no `fetch` (the node fetches the
whole remote). The node then prepares a local branch at the commit tracking
`origin/<branch>` instead of a detached HEAD, so the successor continues the
pushed branch. The same two refusals reach the submitter with the node's reason:
`checkout_refused:successor_stale: want=<sha> origin=<sha>` when origin moved
past the commit after the brief, and `checkout_refused:branch_in_use:
worktree=<dir>` when another worktree of the node's clone holds the branch.
`GET /v1/spawns/features` lists `checkout_branch_v1`; the CLI checks it before
submitting, and the hub names it in the `hub.features` frame so a node sends
`checkout_mode: branch` and the `: want=… origin=…` / `: worktree=…` detail
only to a hub that reads them — an older hub's strict parsers drop the whole
answer for either.

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

The reservation key is `<task>:<role>`. One helper builds it
(`fleet.Spawn.RoleKey`), and the claim, resume, owner lookup, release, reclaim
and retryable-holder supersede paths all use it. An implementer holds
`<task>:implementer`; a hold written under an older role name is not matched by
it. A consult seat (`--role consult`) bound by `--task` takes no task/role slot
at all (`fleet.Spawn.HoldsTaskRole` is false for it), so a consult pair shares
one task (hk 4568); it works beside the claimant and never takes its exclusive
claim.

A reviewer (`--role reviewer`) holds no task/role reservation either (hk 4737):
two submissions for the same task and review pass place side by side, and a
replay of one request id is still matched by that request id. `--review-pass N`
(N is 1..9; reviewer only; any other role, 0 or 10 is a usage refusal) is still
accepted and recorded on the operation, and `fleet.Spawn.RoleKey` still renders
`<task>:reviewer:p<N>`, but no reviewer slot is stored, so a reviewer never
answers `task_role_claimed` (exit 74). A hub upgrading from a build that stored
reviewer keys clears them once at startup: a data-only, idempotent pass that
runs with no request in flight, so no table or column changes. A rolled-back
older hub reads the cleared rows as no slot held; the older behaviour then
returns for reviewers the old binary spawns, and no row is left broken. The
held-reservation log line prints the key as `role_key`.

A node's final `job.lost` settles its job on the hub (hk 5351). The node sets
`final: true` only once its hk record is `lost`, and the relay carries that flag
on `job.lost`; a node that predates the field sends none, so its end settles
nothing. On a final `job.lost` from the job's current node and epoch, the hub
marks the job ended (the same owner and epoch fence as a revocation, I1/I2 and
the #703 owner fence included) and releases the (task, role) reservation of the
job's holder. It deletes no routes on the job's pane: pane-close and the
fresh-snapshot lane sweep retire those. A non-final pool action that still owns
the job keeps the reservation until it ends; admission then takes the settle
(the durable `Lost` receipt survives a restart), even when hk cannot be read, so
a respawn on the same task is not refused `task_role_claimed`. `job.reaped` is a
pane fact the node keeps: it does not end the job. The immediate `panewire emit`
path reads finality from the event file it names, so it sends the same payload
under the same outbox key as the scan.

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

A reviewer is never released through a submission (hk 4737). It holds no slot
for a colliding request to displace, so the hub never closes a reviewer's pane
on that path. Closing a finished reviewer's pane belongs to the node's ended-job
sweep (hk 4693), and ending a reviewer job that posted its result is the hub's
own pass (hk 4684); see
[Ending a reviewer job that posted a result](#ending-a-reviewer-job-that-posted-a-result).
Neither depends on a colliding submission.

A prior operation in `succeeded` whose hk job is still `claimed` or `spawned`
(a job revoked and reaped by an older binary, or one that ended without writing
its record, hk 3204) is checked the same way: only a complete pane list that
lacks the recorded pane, and lists no pane named like the holder's agent label
(herdr renumbers a pane moved across workspaces), has the holder's node mark the
hk job `lost` and releases the slot. A present pane, an unreadable pane list, an
unreachable node or no recorded pane keeps the slot and the 409 body as before.
A failed operation that recorded no pane id, whose label is no longer listed,
frees its slot when its node answers `job.mark_lost` with `no-live-attempt`: the
job dir or `events/` is gone (hk 3685), and the node's own spawn journal and
active-job list name no live attempt of the job under any label, so a claim
renewed under another label cannot be running (hk 4799). hk keeps its `claimed`
record, because the node still never writes `lost` over a record it cannot
check. A node that answers the bare `ledger_missing`, or whose journal or jobs
directory it cannot read, keeps the slot: a missing ledger alone cannot rule out
a newer attempt. A node that refuses `job.mark_lost` for any other reason, because its local ledger holds a
completion or a newer claim, or because herdr still lists the pane, keeps the
slot too; the hub logs `branch=mark_lost_refused` with the node's reason. A slot whose `release` write
itself fails logs `branch=release_write_failed`. When the holder
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

### Ending a reviewer job that posted a result

A daemon-started reviewer (`--role reviewer`, job id `mwr-*`) that posts its
result and stops without running `done` keeps a `claimed` or `spawned` hk
record and a lane row. The node's ended-job pane sweep needs a terminal record
([pane-sweep.md](pane-sweep.md)) and lane release needs the pane gone, so
neither takes it, and the node's 64-row lane cap fills (hk 4658). Every five
minutes the hub writes the end of each such job. The rule:

- **Which jobs.** The pass pages through every persisted `succeeded` operation
  created in the last 14 days (500 rows a page, by creation time and
  request id, so no newer row displaces an older orphan), not the nodes'
  active-job heartbeat: that view drops a pane idle for 30 minutes, caps each node at 32
  jobs and forgets a spawn two minutes after it started, so it would miss
  exactly the long-orphaned jobs. A job whose node is not connected waits for
  the next pass; a job found ended (a terminal hk record) is not read again.
- **A posted result is the proof, and it must have settled.** The node's
  `job.result` answers a findings object that is not JSON `null` (a
  `findings.json` holding `null`, or not matching the schema, is absent) or a
  `report.md` on disk. A reviewer writes `findings.json`, then `report.md`, then
  runs `done`, and hk never rewrites a lost record, so the node ends the job
  only when the newest result file has been unchanged for 10 minutes
  (`reviewerResultSettle`), the files did not change while the node read the
  result (it looks at them before and after, and refuses on any difference), and
  the pane is not `working`. An idle, done or
  blocked pane alone is never proof: an idle reviewer can be waiting on a
  background command, a wakeup or an answer.
- **The record is ended through the node.** The hub sends `job.mark_lost` with
  `end_reason` `reviewer_result_posted`, even for a holder on the hub's own
  host. The node proves everything above from its own job directory while it
  holds the job's operation lock, the lock `panewire done` and the watcher's
  quota-stop records take, so a completion or a quota-stop record cannot land
  between the proof and the hk write
  ([node-commands.md](node-commands.md#job-mark_lost)). The record is written
  `lost`, not `completed`: completing a job uploads its report through
  `panewire done`, which belongs to the job itself or to mergewake's reap.
- **The hub never closes the pane.** Closing it belongs to the node's ended-job
  sweep (hk 4693, PR #577), which reads a reviewer's end from the job's local
  ledger and closes on its own proof of pane ownership. So the node's end writes
  the hk record first and only then appends a final `job.lost` event (reason
  `reviewer_result_posted`) to the ledger: the sweep cannot see an end before hk
  holds this `end_reason`, so it cannot write its own generic lost record over
  it. If the node fails between the two, hk is lost with this `end_reason` and
  the ledger is not ended; the hub reads that on its next pass and asks the node
  again, and the node only appends the missing event. The owner lane receives
  that `job.lost` like any other.
- **A refused or failed write is retried on every pass.** A reviewer that is
  still finishing (`result-settling`, `pane-working`, `operation-busy`,
  `result-missing`) is asked again quietly.
- **A job with an announced quota stop is skipped.** The hub's quota-stop
  hand-over writes that record with the successor's pointer and hk never
  rewrites a lost record ([quota-stop.md](quota-stop.md)); the node refuses the
  end with `quota-stopped`. The watcher's own announce and intent records wait
  for the same lock and, once they hold it, re-read the job and write nothing if
  this end already won (the ledger's `job.lost` folded in sequence order, or hk
  lost with this `end_reason` while the ledger event is still missing), so a
  stale announcement cannot re-arm a hand-over for an ended job. For the
  announcement, which the hub acts on, an hk record that cannot be read is
  retried and then fails the record, never read as "not ended"; the close intent,
  recorded before any hk write so it survives an hk outage and invisible to the
  hub, is not held up by it, and its reconciliation reads hk first. The writer
  answers a distinct "superseded" result, and the watcher then
  activates no hand-over (a hand-over would re-prompt the pane after the end and
  make the ledger live again), ends its watch on the intent path, and starts no
  watcher from `AnnounceQuotaStop`.

The node also ends its own reviewer and consult jobs that never post a result:
a pane both herdr lists lost, or a live pane past the request's time limit plus
a grace. That rule and its end reasons are in
[pane-sweep.md](pane-sweep.md#reviewer-and-consult-ends) (hk 4747). It leaves a
posted result to this pass and never ends a job whose attempt announced a quota
stop.

The pass logs each job once per outcome, never once per pass:
`hub ended a reviewer job with a posted result` (INFO, with `job`,
`holder_host`, `end_reason`), `hub could not end a reviewer job` (WARN, with the
node's `reason`), and `hub kept a reviewer job` (INFO, `reason` `quota-stopped`
or `result_unreadable`).

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
request and destination, per-attempt placement exclusions, any claimed-host
retry pin, result, task/role reservation and notification outbox. Eight workers perform placement; they
persist the chosen placement snapshot, commit a binding and return without
waiting for the node. Placement waits if its snapshot cannot be saved. A separate dispatcher
admits at most one request per node. A slow node therefore cannot occupy the
placement workers or another node's dispatcher. The durable rows are
authoritative; the in-memory work and node-slot indexes can be rebuilt after a
restart. HTTP cancellation does not cancel accepted work. Shutdown ends worker
contexts and waits for them before closing storage.

The snapshot preserves the complete candidate trace across placement passes
and hub restarts. A separate bounded delivery worker posts the attempt's one
immutable placement record after the terminal result, and retries delivery
without holding a placement or node-dispatch slot. A resumed attempt keeps its
predecessor's finished record while it starts a fresh trace.

If the deferred placement-trace file cannot be read, the hub preserves it and
warns; requests with a durable snapshot can still dispatch or replay. Delivery
from snapshots waits for the file to be repaired. A replay without a saved
snapshot returns `store_unavailable` until the hub can recover its trace. A
legacy deferred trace with no saved base and no spooled record is checked
against hk by its existing job and attempt before it is reconciled. If hk has
no record, the hub merges the traces and posts the final record only after the
attempt ends. If hk already has the same final record, the hub seals its local
snapshot without posting again. A flat legacy trace does not contain the
producer version, so hk's existing version is authoritative when every
reconstructible field matches. A finished local record is compared in full,
including its version. Timestamps compare at hk's stored microsecond
precision, and the lookup includes all retained rows. A different record
leaves the attempt held and the trace on disk, and sends its owner a durable
`legacy_placement_record_conflict` notice. An unavailable or incomplete hk
lookup also leaves the trace held. A pending attempt cannot be compared to an
existing final record and remains held for inspection. Deferred recovery has
a bounded, rotating pass, so slow hk lookups do not starve other records.

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
falls through to another host. The node checks its journal before admission:
completed requests replay their result, a request this node process is still
running answers `reconciling`, and an executing entry no live execution owns
(the node restarted mid-spawn) is settled or answers `unknown`. A new UUID
waits, until its execution deadline and without an executing journal entry,
for node admission. Once admitted, the node rechecks the journal and
deadline, commits the executing entry and keeps the admission through
checkout, launch, brief delivery, landing proof and cleanup, releasing it
after the result is journaled and before the reply.

Admission (hk 4578): a node runs one spawn at a time unless `spawn.json` sets
`spawn_concurrency` above one, an operator opt-in whose two unproven steps are
listed in [config.md](config.md) (hk 4821). N is that value as of the node's
last hello; unset, or a `spawn.json` that does not load, gives 1, and a change
takes effect at the next registration (reconnect or restart). Opted in, a node
runs up to N spawns at once, but only checkout spawns overlap, each holding
its own job dir (`<work_root>/<checkout dir>`, case-folded; `-L live|mock`
stays inside it). A spawn without a checkout is admitted alone, and a waiting
one blocks new admissions. Preparation of one clone runs one at a time. A job
dir or `-L` lane that is a symlink is admitted alone, and a lane that resolves
outside its worktree after prepare is refused
(`checkout_refused:lane_outside_worktree`). Workspace lookup and creation for
one herdr session and label holds a file lock in the panewire state dir. The
node advertises `spawn_slots_v1=N` in hello only when N is above one. The hub
opens a second dispatch to a node only when it advertised slots and every
open dispatch and the candidate are checkout spawns into distinct dirs;
anything else stays queued at the hub. A node without the capability, or an
older hub, runs one at a time. While a request without a checkout is queued
behind open dispatches, the hub opens no new dispatch to that node until it
has run. It commits the result before
replying. A node drain wakes unclaimed waiters without a reply or journal row
for replay after restart; work admitted before the drain is allowed to finish
within the drain bound. Claimed executions retain the restart-settlement rules. See
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
[the placement rule](placement.md#the-filter). A first-run refusal
(`hooks_review`, `trust_prompt`, `omp_setup_wizard`, `auth_unavailable`) tells
the operator, through a Sentry warning and not a lane, which one-time action the
host needs
([placement.md](placement.md#first-run-refusals-and-the-desk-notice)). When the
node verifies cleanup and the refusal precedes the hk job claim (result stage
empty, `placement` or `checkout`), auto placement can try another host. Before
the claim, a launch-proof refusal tries an
alternate harness on the same host first, then another host. A refusal from
the `claim`, `launch` or `landing` stage (apart from `deadline_not_started`)
may leave the hk job record claimed by that node's host, so the hub treats the
host as bound. Handoffkeep answers another host's claim of that job id with 409
`job_id_taken` (rc 74, "job id already claimed"). After a cleanup-confirmed
`launch_proof_failed` or another post-launch harness refusal, an automatically
selected, non-delegated profile tries an alternate harness **on that same host
only**. A pinned profile is not changed. If no alternate is eligible, the
attempt ends with a refusal on that host. The hub atomically saves the host pin
and failed harness with the unbind; the pin survives hub restart and later
attempts, including a later refusal before the new attempt's claim. An eligible
harness refusal during such a later attempt can try another harness on that
same host. No result without confirmed cleanup permits another launch
(`spawnRefusalMovesOn`, `spawnReplayPinsHost`; hk 4614).
For agy, a verified rc=76 (`BRIEF_SUBMISSION_UNCONFIRMED`) whose newest saved
capture matches the harness login screen is sent as rc=88 with an
`AUTH_UNAVAILABLE` refusal; the hub classifies it as
`auth_unavailable`. Other rc=76 results and results with unverified cleanup
keep their original refusal. A pinned `--host` remains the destination; if its
profile was automatic, an eligible post-claim harness refusal may still try a
different harness there. For an eligible refusal, the hub commits the unbind
and the refused host, profile and reason together, then wakes placement. The
exclusion survives hub restart within that attempt and is cleared by a
cleanup-confirmed new attempt. A request that already claimed its handoffkeep
job must not be moved to another host; that claim is host-bound.

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
| `retryable` | Node attested cleanup, or admission stopped before dispatch, and a replay can succeed once the operator's fix is done; reservations released. An unstarted attempt's *timeout* no longer ends here for a request that has a limit: see [Time limit](#time-limit). |
| `given_up` | The request's `expires_at` passed with no attempt that could have started; terminal, reservation freed. |
| `waiting` | A start condition is not met ([Start conditions](#start-conditions)); reservation held, no placement slot. |
| `refused` | A start condition named a task handoffkeep does not have; terminal, reservation freed, one `SPAWN_FAILED` (rc 78). |
| `cancelled` | A start condition's task was dropped or duplicated; terminal, reservation freed, one `SPAWN_FAILED` (rc 84). |

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
| Deadline before dispatch | An unbound or unsent bound request cannot have launched. Its landing wait ends the attempt without waiting for a node slot: a request with a limit starts the next attempt under the same reservation, or is `given_up` at its limit ([Time limit](#time-limit)); a row with no limit ends `retryable` and releases its reservation as before. | `TestAsyncSpawnUndispatchedTimeoutStartsNextAttempt`, `TestQueuedBoundDeadlineExpiresWithNodeSlotBusy_t4581`, `TestUnstartedRequestGivenUpAtItsLimit_t4660`, `TestRequestWithoutExpiryKeepsRetryableTimeout_t4660`. |
| Containment or disconnect before send | Unbind unsent rows even when another request occupies the node slot; on a failed store write keep the original binding for recovery. | `TestContainmentUnbindsQueuedRowsDespiteStoreFailure_t4581`, `TestDisconnectUnbindsOnlyUnsentRowsWhenWriterIsStuck_t4581`. |
| Eligible refusal before a host-bound job claim | Atomically record the exclusion and unbind; recovery continues placement with the same logical attempt. | `TestRefusalExclusionSurvivesStoreRestartAndResetsOnRetry_t4581`, `TestRefusalWakesRealPlacementOnAnotherNode_t4581`. |
| Cleanup-confirmed harness refusal after a host-bound job claim | Atomically pin the claimed host, record the failed harness and unbind; restart or later attempts cannot move the job to another host. Without cleanup proof, keep the original result. | `TestClaimedHarnessRetrySurvivesHubRestart_t4581`, `TestClaimedHostStaysPinnedWhenAlternativesRunOut_t4581`, `TestClaimedHarnessRefusalKeepsBindingOnStoreFailure_t4581`, `TestRefusalAfterJobClaimDoesNotMoveToNextHost_t4614`. |
| Node disconnect or uncertain send | Retain node UUID/host; replay its journal on reconnect. | `TestSpawnUnknownRetainsOwnershipAndLateResultCommitsOutbox`, `TestAsyncSpawnLateResultReconcilesAgainstSameNode`. |
| Duplicate accept / lost HTTP response | Identical key/body replays; changed body conflicts; CLI receipt preserves key. | Reservation test and `TestSubmissionReceiptReusesKeyAfterLostHTTPResponse`. |
| Timeout followed by late pane | Retain ownership; reconcile receipt with exact node identity; never close working/unknown. | Late-result test and `TestFailedSpawnCleanupRequiresExactIdlePane`. |
| Timeout, and the node never started the request | Settle with `deadline_not_started` (see [A request the node never started](#a-request-the-node-never-started)): a request with a limit starts its next attempt, or is `given_up` past the limit; a row with no limit ends `retryable`. An executing, restarted or silent node keeps ownership, and is never given up (`TestReconcilingRequestIsNeverGivenUp_t4660`). | `TestLateNotStartedAnswerSettlesRequest_t4379`, `TestTimeoutWithoutNodeRowSettlesAsNotStarted_t4379`, `TestTimeoutKeepsOwnershipWithoutNotStartedProof_t4379`. |
| Node restart while executing | Never rerun the spawn. The restarted node settles the executing row from herdr and the job's local spawn record: the recorded pane alive as an agent completes it with the job's receipt (the hub still verifies the pane before accepting it), a pane in neither the agent nor the pane list completes it `lost` with a reason, and anything unproven (a pane only in the pane list, a label match without a record, herdr unreadable) leaves it executing and answers `unknown`. Past the deadline, `unknown` with no pane bearing the job label finishes the request `failed` (`orphaned_after_restart`) and releases its reservation; a labelled pane keeps it for the operator. The node's stop first waits (30 s) for a launch, injection or result upload in flight. See [node restart](runbooks/node-restart.md). | `TestSpawnSettle*`, `TestStopDrain*`, `TestNodeSpawnJournalRestartDedupAndLostResultReplay`, `TestNodeSpawnReplayAnswersUnknownAfterRestart`, `TestStrandedSpawnUnknownWithoutPaneFailsAndReleases`, `TestStrandedSpawnStaysPendingWithoutProof`. |
| Lost event or lost ACK | Commit state and outbox atomically; retry with a stable event id and actual job id. | `TestSpawnOutboxFailureRollsBackResult`, `TestAsyncSpawnLostEventRetriesWithStableJobIdentity`. |
| Process pressure / shutdown | Bounded execution and persisted pending work. | `TestAsyncSpawnWorkersBoundedAndIndependentOfHTTP`, `TestAsyncSpawnOutlivesSubmittingHTTPContext`; HTTP-independent execution lifetime is owned by maintenance/shutdown, never the POST context. |
| Backend migration | Never silently drop reservations or tombstones. | Spawn store contract refuses the inbox-only SQLite-to-Postgres migrator when spawn rows exist. Full backend transfer is a separate migration, not an inbox-only copy. |

### A request the node never started

A node that first sees a `job.spawn` after its absolute deadline, or whose
admission wait reaches that deadline, answers `rc=124 stage=claim
reason=deadline_not_started retryable=true` without calling the engine. It
first checks for an existing journal entry, and a new request that expires
while waiting leaves no journal row. A legacy request has only a relative
wait; a durable request can also have a shorter local wait. Either answers
`timeout` when that wait expires, even if the reply is delayed past the
absolute deadline: another copy could still have started. A node shutdown
before the absolute deadline leaves the waiter for replay and is not a
not-started attestation. The hub settles on the durable answer:

| Node evidence | Hub outcome |
|---|---|
| `deadline_not_started`, while the request's own deadline still has room (the hub clock is behind the node's), auto placement with no earlier host-bound claim | Unbind and try the next eligible host. |
| `deadline_not_started`, any other case (a pinned host, an earlier claim, or the deadline passed) | `retryable`, reason `deadline_not_started`, stage `claim`, rc 124, with a fix text; the (task, role) slot is released, so the caller's replay starts a new attempt. An earlier host-bound claim still pins that host. The caller sees this refusal, not rc 75. |
| Landing wait ran out; the node answers the original frame or a replay with `deadline_not_started` within 5 s | Same `retryable` outcome. |
| Landing wait ran out; the node answers `reconciling` (row executing), `unknown` (stale executing row), `journal_unavailable` or `journal_conflict` | `deadline_exceeded`, ownership kept, as before. |
| Landing wait ran out; no answer within 5 s (disconnected node, stalled read loop) | `deadline_exceeded`, ownership kept. Silence is never read as not started; a later `deadline_not_started` from the coordinator's replay still settles it. |
| Completed row | Replayed result, judged as a late result (receipt plus exact pane read). |

When the landing wait ends, the dispatching worker replays the same request id
once and waits up to 5 s (`spawnNotStartedProbe`) before writing
`deadline_exceeded`. Past the deadline a node never launches a request, so the
replay cannot start a second one. For a grant-required attempt, see [Start proof](#start-proof).

### Start proof

A request the hub sends to a node that advertises `spawn_grant_v1` is
grant-required (hk 4612; design `hub-owned-spawn-queue` §3 rule 6). The flag
is fixed when the attempt is bound and stored with it (`grant_required` in
the node payload). The hub never sends a grant-required attempt to a node
that stopped advertising the capability; the binding goes back to placement
unsent.

1. Every send of the attempt, including a replay after a hub restart, carries
   `start_by_ms` set to the send time plus `SpawnStartBy` (default 60 s).
2. Before it claims its journal row, the node sends
   `{"type":"job.spawn.claim","request_id":<node request id>}` and waits until
   `start_by` for `{"type":"job.spawn.grant","request_id":…,"granted":bool}`.
   The node calls no checkout, brief fetch or engine before the grant.
3. The hub grants only the current sent attempt (`dispatching`,
   `reconciling` or `deadline_exceeded`) bound to the asking machine. It
   commits the grant before it answers, and the grant is idempotent for that
   id. Anything else is refused.
4. A refusal, silence until `start_by`, a lost connection, or a frame first
   read after `start_by` makes the node journal the request as completed with
   `rc=124 stage=claim reason=deadline_not_started`. A later copy of the frame
   gets that answer and never starts.
5. At `start_by` plus `SpawnStartByMargin` (default 30 s) after a send, the
   hub tries to fence the attempt. A won fence is settled as the node's own
   `deadline_not_started` would be (the table above): with auto placement,
   the next host. A lost fence means the node holds a grant, so the hub keeps
   waiting; past the landing bound the attempt keeps ownership as before
   (`deadline_exceeded`). A late node answer for a fenced id is ignored.
   The fence is tried only while the request's landing deadline still has
   room for `start_by` plus the margin. A send or replay closer to the
   deadline, or past it, takes the earlier path (probe, then
   `deadline_exceeded` with ownership kept). An ungranted attempt whose node
   is disconnected stays owned until a replay reaches a connected node.
   The fence is armed when the frame has been written, and a store error
   while fencing is retried with backoff (1 s doubling to 30 s); only a
   recorded grant stops the retries. The node's grant wait also ends at the
   request deadline and when its hub connection ends, and a grant read at or
   after `start_by` is not taken.
6. A replay of a sent attempt whose fence was already committed (a hub
   stopped between fence and settlement) is settled as fenced without being
   sent again. The hub records the latest `start_by_ms` it sends for each
   attempt (`spawn_attempt_start_by`, written before the frame leaves). A
   replay that finds that `start_by` plus the margin already passed fences
   the attempt before it sends again. A short-lived replay, such as a
   recovery reprobe whose context ends long before its own fence timer,
   therefore still fences an ungranted attempt.

#### Stored statuses that older hubs do not know

A grant-required attempt is stored under its own status values from the
moment it is bound: `bound_grant`, `dispatching_grant`, `reconciling_grant`
and `deadline_exceeded_grant` refine `bound`, `dispatching`, `reconciling`
and `deadline_exceeded`. Every read maps them
back, so code, `GET /v1/spawns/<id>`, `spawn --wait`, spawn notices and every
other external surface show only the existing value. Clients learn no new
state here (hk 4659 is where they would).

A hub older than the start proof (checked against v0.0.404, 54345a76) lists
none of these values in any `spawn_operations` status query:

| Older-hub query (v0.0.404) | What it does with a `*_grant` row |
|---|---|
| Coordinator replay, `status IN ('accepted','bound','dispatching','reconciling')` (`hub_spawn_task_store.go:576`, `:599`) | ignored: never replayed, so no frame without `start_by_ms` |
| Stale-deadline replay, `status='deadline_exceeded'` (`:580`, `:603`) | ignored |
| Startup and orphan sweep, `status IN ('accepted','bound','dispatching','reconciling')` (`:645`) | ignored: never swept |
| Placement recovery, `status IN ('bound','dispatching','reconciling','succeeded') OR status='deadline_exceeded'` (`:554`) | ignored |
| Outcome write, `status IN ('accepted','bound','dispatching','reconciling','deadline_exceeded')` (`:369`) | matches no row: never mis-terminal |
| Live view, `status IN ('accepted','bound','dispatching') OR status='succeeded'` (`hub_spawn_live.go:92`) | not listed |
| Role release, `status IN ('succeeded','failed')` (`:783`, `:792`) | never released: reservation and role kept |
| Placement record seal (`spawnAttemptTerminal`) | not terminal: record kept pending |
| Send, unbind, retry and reconcile transitions, `status=?` or literal old statuses (`:327`, `:468`, `:486`, `:702`, `:820`) | never match: they take a status the older hub read, and it never reads a `*_grant` row through the queries above |
| Bind and first send, `status='accepted'` → `bound`, then `bound` → `dispatching` (`:301`, `:327`) | a `bound_grant` row is never sent by the older hub |
| Reviewer quota successors (`quotaSuccessorSpawns`, `spawnMayStillRun`) | read, but skipped as not running: see rollback step 1 |
| Staged and retryable sweeps (`:217`, `:237`, `:271`, `:744`, `:765`) | not listed |
| Role owner lookup (`:724`) | returns the raw value; a role conflict reply from the older hub can name `dispatching_grant` |

The stored status is also the provenance. A row whose payload says
`grant_required` but whose status is plain was bound or sent by an older hub
(only a hub before this suffix existed could leave one), so its frame may have
had no `start_by_ms`. Such a row is read as not grant-required: no
`start_by_ms`, no grant, no fence, and every later write keeps the plain
status. The payload flag alone never makes a row fenceable.

An older hub therefore holds such an attempt, failing closed. It never sends
a frame without `start_by_ms` for it, so any frame a node ever got for a
grant-required attempt carries `start_by_ms` and waits for the grant. That is
what lets any later hub, including after an ordinary restart, fence a silent,
ungranted attempt.

#### Rolling the hub back past this change, and forward again

An older hub's status-filtered queries skip `*_grant` rows (the table above),
so it never starts one without a grant. Other older-hub paths look a row up
by job or id and then read its status: `spawnStatusActive` (ladder
`escalate-author` retries), `spawnMayStillRun` (reviewer quota successors)
and `replaceQuotaStopped` (quota hand-offs). They would misread a `*_grant`
row as not running. So an older hub starts only once no `*_grant` row is
left. A rollback is a quiesced drain:

1. Restart the newer hub with `--spawn-grant-drain` (a hub flag, read at
   startup). While it is set, the hub:
   - binds no new grant-required attempt. Placement goes on, and a node with
     the grant capability gets a plain binding and a frame with no
     `start_by_ms`, as a node without the capability does.
   - starts no quota hand-off. A `quota_stop` is logged as
     `hub quota hand-off skipped while draining grant rows` and gets no
     successor.
   The `*_grant` rows already there go on as before: they are granted,
   fenced or settled from their nodes' answers.
2. Wait until no `*_grant` row is left:

   ```sql
   SELECT count(*) FROM spawn_operations WHERE status LIKE '%\_grant' ESCAPE '\';
   ```

   Run it with `sqlite3 <inbox store>` or `psql`. Every hub start also logs it,
   so a rollout script can gate on the line
   `hub spawn grant rows grant_rows=<n> spawn_grant_drain=<bool>`.
3. Only at 0, stop the newer hub. Then remove `--spawn-grant-drain` from the
   hub's command line before the older binary starts: v0.0.404 does not know
   the flag and refuses to start (`invalid hub flags`), and with
   `Restart=always` the unit would restart-loop. Under systemd, edit
   `ExecStart` in the unit (or its drop-in), run `systemctl daemon-reload`,
   then switch the binary and `systemctl restart` the unit. If the count does
   not reach 0, do not roll back: remove the flag the same way and restart the
   newer hub.

Roll forward by starting the newer hub without the flag. Rows the older hub
left in plain statuses stay plain and are never fenced (see provenance above).

Cost: the drain waits for grant-required spawns in flight, and that wait has
no bound. A granted attempt whose node stays silent keeps its row
(`deadline_exceeded_grant`, owned) past its landing deadline, until its
node answers or an operator resolves it, and until then the count stays
above 0. Quota-stopped jobs get no successor while the flag is set.

| Case | Test |
|---|---|
| Grant and fence exclude each other; the grant is idempotent; only the bound machine's grant-required sent attempt is granted | `TestSpawnGrantAndFenceExcludeEachOther_t4612` |
| An ungranted attempt is fenced and re-placed; a late claim is refused | `TestSpawnUngrantedAttemptIsFencedAndReplaced_t4612` |
| A granted silent attempt keeps ownership and its reservation, also after a restart | `TestSpawnGrantedSilentAttemptKeepsOwnership_t4612` |
| A hub restart's replay of a sent, ungranted attempt carries a fresh `start_by` and fences it | `TestSpawnRestartReplayFencesUngrantedAttempt_t4612` |
| Bound and sent grant-required statuses match no v0.0.404 status query; reads and `GET /v1/spawns/<id>` show the refined value | `TestSpawnGrantRequiredStatusHiddenFromOlderHubsAndClients_t4612` |
| A replay with a short context (recovery reprobe) fences an ungranted attempt past its recorded `start_by` + margin before resending | `TestSpawnShortReplayFencesPastStartBy_t4612` |
| `--spawn-grant-drain` binds no `*_grant` row (placement goes on without a grant) and starts no quota hand-off | `TestSpawnGrantDrainBindsNoGrantRows_t4612`, `TestSpawnGrantDrainStartsNoQuotaHandOff_t4612` |
| A grant-required row an older hub moved to a plain status is never granted, fenced or given `start_by` | `TestSpawnLegacySentGrantRowIsNeverFenced_t4612` |
| Coordinator placement on a real node: grant-required binding, recorded grant, delivered through the node's read loop | `TestSpawnGrantEndToEndWithRealNode_t4612` |
| A committed fence settles on replay without a resend | `TestSpawnCommittedFenceSettlesOnReplayWithoutResend_t4612` |
| A fence store error is retried | `TestSpawnFenceRetriesAfterStoreError_t4612` |
| The node's grant wait ends with its connection; a gate timeout or late frame still journals not-started; `granted:null` is rejected | `TestNodeSpawnGrantWaitEndsWithConnection_t4612`, `TestNodeSpawnGateTimeoutJournalsNotStarted_t4612`, `TestNodeSpawnGrantRejectsNullGranted_t4612` |
| A legacy node gets no `start_by` and is never fenced | `TestSpawnLegacyNodeIsNeverFenced_t4612` |
| A grant-required binding is not sent to a node without the capability | `TestSpawnGrantRequiredSkipsNodeWithoutCapability_t4612` |
| A node starts only with a grant; a refusal or silence is journaled as never started, also for replays | `TestNodeSpawnStartsOnlyWithHubGrant_t4612`, `TestNodeSpawnPastStartByIsNotStarted_t4612`, `TestNodeSpawnWithoutStartByNeedsNoGrant_t4612`, `TestNodeSpawnGrantWire_t4612` |

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

A completed hk job also settles a request past its deadline (hk 5267): when the
job record reads `completed` with a pane, the request's node host, and the
request's task identity (its `--task`, or for a `--parent-job` request a
`parent_job` naming that parent; a task id at or past 2^53 is refused, since
the record's number may be rounded), and its `ended_at` is at or after the
request's creation (job ids are reusable; a missing stamp refuses), the hub settles the request `succeeded` at the next probe
(`settleCompletedSpawn`), without a node connection. A landing receipt that
never reached the hub is therefore not required. Then a correcting `SPAWNED`
follows any unresolved notice already sent.

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
credential). It applies to a pending request bound to a node, and to an
unbound one (below). A `bound` request
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
409 `spawn_not_pending`.

An unbound pending request (`accepted`: no node has bound it yet) needs no pane
probe, since nothing was sent. It ends `failed` with reason
`operator_abandoned: <TEXT>`, its (task, role) reservation is released in the
same transaction, and its owner lane gets one `SPAWN_FAILED`. The owner lane
may withdraw it with its own node credential (`Authorization: Bearer <node
token>` and `X-Panewire-Machine-ID`) when the lane is routed to that node; a
node that does not route the lane gets 403 `lane_not_routed_to_caller`. The
operator CLI carries only the operator token, so a lane withdraws over the
HTTP route. A request that holds an hk job claim on a host answers 409
`spawn_abandon_refused` with `reason: claimed_host` and stays with that host.
A node credential on a bound request answers 401. A request that is already
withdrawn answers 409 `spawn_not_pending`. The CLI exits 5 on a refusal. Abandon does not stop a
spawn the node may still be running before its pane exists; use it for a
request whose node has restarted or whose deadline has passed.

## Withdrawing a request

`panewire spawn withdraw REQUEST_ID --reason TEXT [--requested-by LANE]` (hk
5429) lets the lane that submitted a pending request end it itself. `LANE`
defaults to `PANEWIRE_LANE`. The CLI sends `POST /v1/spawns/<request_id>/withdraw`
with `{"reason": TEXT, "requester": LANE}` under the operator credential, which
is the credential a submitter holds (submission needs it). The hub advertises
the route as the feature `spawn_withdraw_v1` in `GET /v1/spawns/features`; the
CLI checks that feature first and refuses on an older hub before it sends
anything.

- **Scope.** The requester must be the lane that submitted the request: its
  `--requested-by`, else `--parent`, else its owner lane. Any other lane gets
  403 `not_requester`, and the request's state is not read for that answer.
- **Accepted, unbound, no prior claim** (including a request parked by
  `quota_reset` or `launch_backoff`): it ends `cancelled` at once with reason
  `requester_withdrawn: <TEXT>`. There is no pane probe, and the owner lane and
  the submitter each get one notice.
- **Refused, unbound:** a `waiting` start-condition request is not pending and
  answers 409 `spawn_not_pending`. An accepted request that still holds an hk job
  claim on a host answers 409 `spawn_abandon_refused` with `reason: claimed_host`
  and stays with that host.
- **Bound (not sent):** the request has not crossed the send boundary, so it ends
  `cancelled` with no fence and no pane probe.
- **Sent (`dispatching`, `reconciling`, `deadline_exceeded`):** the hub must
  prove the node cannot start the attempt before it cancels it.
  - A grant-required attempt is fenced first. The fence commits only when no
    start grant was recorded (`spawn_attempt_grants`), and a fenced attempt can
    never start. A granted attempt answers 409 `spawn_abandon_refused` with
    `reason: granted` and is not withdrawn.
  - A legacy attempt (no start grant) has no fence. It answers 409
    `spawn_abandon_refused` with `reason: unfenced_launch` and stays pending
    until its deadline or the operator ends it.
  - Once fenced, the abandon rule applies: the request ends only when its node
    lists no pane bearing the job label. A live labelled pane answers 409
    `spawn_abandon_refused` with `reason: pane_present:<pane>`. Otherwise the hk
    job is marked lost with `end_reason` `requester_withdrawn`, and the request
    ends `cancelled` with its (task, role) reservation released in the same
    transaction as the outcome.
- **Already ended or landed:** 409 `spawn_not_pending`. A landed request keeps
  its result.
- **Credential:** only the operator credential may withdraw. A node credential
  gets 401 for every withdrawal. The unbound owner-lane withdrawal over a node
  credential (hk 5214, `abandon`) is unchanged.
- **Exit codes:** 0 withdrawn (the CLI prints `WITHDRAWN request=… job=… status=cancelled`);
  2 usage, or the lane is not the requester; 75 the hub kept the request or it
  had already ended. A transport failure after the send is reported as
  unconfirmed: read the request with `panewire spawn status REQUEST_ID`, where a
  withdrawn request exits 84.
- **Limit.** The operator credential is shared by the submitters on a host, so
  the requester check stops a lane from withdrawing another lane's request by
  mistake. It does not stop a holder of the operator credential, who can already
  run `abandon`.

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
`checkout_refused:sha-missing`, `checkout_refused:branch-elsewhere`, or
`checkout_refused:branch_in_use` moves to
the next candidate after the hub unbinds the refused node. These reasons
are not pinned to that host when the saved operation is replayed; placement
starts again. A stale successor (`checkout_refused:successor_stale`) names a
commit origin has moved past, so it is the same on every host and ends the
operation instead. An exhaustion is the generic retryable `admission_refused` with
each tried host and its checkout outcome. `busy` still retries once on the same
host. The terminal checkout refusals that remain retryable (`dirty`,
`sha-missing`, `work_root_unset`, `git_too_old`) name a fix that ends in replay;
`spawnRefusalReplayable` holds that classification.

## Owner notices by cause

hk 5360 (operator decision 433; split agreed with mergewake-design). A
retryable spawn failure that the requester replays by itself (`replay=yes`) is
recorded and counted by cause, and not sent to a design owner's inbox. The
owner hears it only when:

- the request is given up (`given_up`), or the failure cannot be replayed
  (`replay=no`);
- the same cause repeats for the same owner three times within 30 minutes: the
  third occurrence notifies once, and the window starts over;
- the cause is on `relay.spawn_notice_must_fix` (`delivery-policy.yaml`;
  default `workspace_unresolved`, `auth_unavailable`, `report_path`,
  `brief_upload`, `identity_unproven`). An explicit empty list has none.

Each notice's decision (deliver, file, count, repeat, settle) is made once, from
its inputs at first routing, and recorded in `owner_notice_decisions` before it
takes effect (hk 5360, packet `handoff/2026-11-19/panewire-design-hk5360-root-cause`).
Counts and the repeat window are read from those rows, so a hub restart keeps
both. A notice without an event id is never withheld.

A requester's own copy of a failure is never silenced, because the requester
replays from it. A design owner's own spawn, with no separate requester, keeps
its copy at once. The rules apply to design-lane owners; other lanes are
unchanged. Each silenced notice is counted in the daily loops row
(`spawn_notices`, [docs/loops.md](loops.md#spawn-notices-group)).

A requester can say what its owner hears with `--owner-notify final|never`
(`owner_notify_v1`: node and hub capabilities, `panewire version --json`, and
`GET /v1/capabilities`). The mode is read from the saved request and applies to
the owner lane of that spawn, whatever the owner's lane class:

- `never`: the owner gets none of the spawn's notices (`SPAWN_FAILED`,
  `SPAWNED`, `LANE_UNREGISTERED`, `job.completed`, `delivery-unconfirmed`).
  Each is recorded and counted, whatever its cause, must-fix included.
- `final`: the owner gets only a terminal notice: a failure with `replay=no`,
  or a given-up request. Every other notice is recorded and counted.
- absent: the rules above decide.

A `job.completed` withheld by a mode is filed in hk as `filtered:owner-notify:<event id>`:
its own record per event, since hk's idempotency key covers the record's reason.
A node that predates `owner_notify_v1` refuses the flag, so the hub strips it
before sending the spawn; the saved request keeps it. An `--owner-notify` value
other than `final` or `never` is a usage error. Code:
`hub_spawn_notice_silence.go`, `internal/fleet/owner_notify.go`.

## Notifications and telemetry

State changes and notification records commit in one database transaction.
The outbox hands each record to lane delivery, which stores one handoffkeep
`lane.event` with the real `job_id`, a stable producer event id and the owner or
parent lane, then injects it. A restarted hub retries undelivered rows. Delivery
is at least once; consumers use the event id
(`<request_id>:<attempt>:<status>`) to deduplicate. Timeout emits
a bound request past its deadline (`deadline_exceeded`) notifies nothing, because
it is not final (hk 5267). Its outcome is decided by one of three things, and
then the owner hears it once:

- a late node result, or a completed hk job whose record carries the request's
  host, a pane and the request's task identity (its `--task`, or for a
  `--parent-job` request a `parent_job` naming that parent) settles it: `SPAWNED`
  (`<request_id>:<attempt>:succeeded`); a late failure settles it `SPAWN_FAILED`
  with that failure;
- the stale probe's horizon, 30 minutes past the deadline
  (`spawnStuckAfter`), with no answer and no final proof: one unresolved
  `SPAWN_FAILED … stage=landing reason=deadline_exceeded_unresolved replay=no`
  (`<request_id>:<attempt>:deadline_unresolved`, plus `:<lane>` for the
  submitting lane). Ownership is kept. Its SPAWN_FAILED error event is recorded
  once.
- a landing after that unresolved notice: one correcting `SPAWNED … request=<id>
  (after an earlier unresolved notice)` under the same request id, as the
  request's last outcome, to every recipient the unresolved notice reached
  (owner and submitter). Its event id is the ordinary `…:succeeded` (plus
  `:<lane>` for a submitter copy). A correction is not routed while the
  unresolved notice it corrects is still undelivered for that recipient, so a
  lane never reads the success before the failure it corrects.

An unbound timeout (the node never took the request) is not held by this rule.
Each event id is fixed and unique per attempt, so a restart never repeats a
notice.

`SPAWN_FAILED` names the job, request, exit status, stage, `replay=yes|no`
(whether replaying the saved request can help), reason and fix. It has
notify type `automation-failed`, so it interrupts a lane that digests its reports.
A `failed`, `retryable` or `given_up` spawn is also sent to the submitting lane when that
differs from the owner: `--requested-by`, else `--parent`. The owner's event id
stays `<request_id>:<attempt>:<status>`; the submitting lane's is the same id
with `:<lane>` appended.
`SPAWNED` stays type `report`.

A spawn that names an hk task (`--task`) and reaches a final failure
(`failed` or `refused`) also posts one comment to that hk task (hk 5137):

`spawn failed request=<request_id> rc=<rc> stage=<stage> reason=<reason>`

That is the line the wake skill asks sessions to write by hand; the hub
writing it keeps the stuck-work wait accurate without a session in the loop.
The comment posts once per request id in every path the hub can observe: the
operation carries a posted-at mark (`task_fail_notice_ms`, migration
`0023_spawn_task_fail_notice.sql`) that survives a hub restart, and failed
and refused rows that predate the mark are stamped judged at migration time
and never post. The transaction that ends an operation as failed or refused
also writes its `task_fail_notice_outbox` row; the sweep reads only that
outbox, and recording the mark deletes the row. A post hk refuses for good is dropped loudly, while any other
failure stays pending and retries on later sweeps: one hk attempt per due
row per sweep, then a growing per-row delay persisted on the row's
`task_fail_notice_outbox` entry (`retry_ms`, migration
`0030_spawn_queue_outbox.sql`; 30s doubling to 10m; the older
`task_fail_retry_ms` column is no longer written), so an outage costs one bounded sweep and cheap skips
instead of stalling scheduling (one warn per failure streak). The mark and
the retry delay are written detached from the sweep budget, so a spent
budget cannot lose bookkeeping for a post that already succeeded. If the hub
stops between the hk post and the mark — or an hk answer is lost after hk
stored the write — the next sweep reposts once: at-least-once across that
window, exactly-once everywhere else. A hub that starts without an hk client
judges its pre-start backlog once at startup, so enabling hk later never
floods old tasks; failures that turn final afterwards stay owed and post
once a client exists. Succeeded spawns, spawns without `--task`, and
retryable outcomes
(which a later attempt may still land) post nothing. `deadline_exceeded` is
not final — a late node result can still land it as succeeded — so it posts
nothing; if the row is later finished as failed, the failed post fires then,
once.

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
ownership. A request that later lands notifies one `SPAWNED`; one that fails
notifies one `SPAWN_FAILED`. A bound deadline notifies nothing until it is final
(see Notifications and telemetry, hk 5267). The hub journal logs
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
and retry decisions never read the code. A notification decision reads it only
for the owner-notice rules in [Owner notices by cause](#owner-notices-by-cause).

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

## Read gates (hk 5302)

The coordinator and two other loops used to read their store on every tick even
with nothing owed: the coordinator every 2 s, the placement-record probe every
2 s, the consult loop every 5 s. A `readGate` (`read_gate.go`; design hk:doc
`design/2026-11-18/pw-4898-hub-store-mysql` §3) lets each loop read its own
queue or outbox only when one of these holds:

- **start:** the gate is unprimed, so the first pass after a hub start (or
  after a failed pass) reads the store and rebuilds the due time from the
  queue. Work a previous hub left is found by this pass, not by a kick.
- **kick:** a write that adds work for the loop called `noteWork` after its
  commit, or the coordinator's `spawnKick` fired. The write also wakes the
  loop's select, so the read does not wait for a tick.
- **due:** the earliest time the last pass left behind has passed.
- **backstop (60 s, `readBackstop`):** a write nobody taught the gate about is
  delayed at most this long. When a backstop pass finds work, the hub logs
  `hub read backstop found work that no kick announced` with `loop=<name>`;
  that line is the evidence that a write path misses its kick.

The loops still tick in memory every 2 s or 5 s (the liveness ledger, drain
marks, the wake of parked rows): only the store read is gated. `--spawn-rollback-drain`
still counts blocking rows once a minute.

| Gate | Loop | Reads (when open) | Kicked by (after commit) | Due time it keeps |
|---|---|---|---|---|
| `spawn_scan` | coordinator scan | start-condition check, parked notices, `pendingSpawnPage`, `pendingSpawns`; then, if nothing was runnable, one read of the queue | `claimSpawn`, `activateStagedSpawn`, `resumeCleanedSpawn`, `returnToWaiting`, `wakeParkedSpawns` (rows woken), every `spawnKick` | 2 s while a row is runnable or in flight; else the earliest parked `next_check_ms`, the next stale probe, the next start-condition poll while a row waits, the next parked-notice sweep while a row is parked |
| `spawn_notice` | `flushSpawnNotifications` | the pending, publishing and held rows of `spawn_notice_outbox` | `completeSpawn`, the waiting give-up and end, `noteUnresolvedSpawn`, `saveSpawnNotice`, `resumeCleanedSpawn` (a replay supersedes a held notice) | 2 s while a row was read (it may be owed; an empty read settles it); a held row's hold end |
| `spawn_task_fail` | `flushSpawnTaskFailNotices` | `task_fail_notice_outbox` rows due, then the earliest `retry_ms` | `completeSpawn`, the waiting give-up and end | the earliest row's `retry_ms`; 2 s while a due row stays unposted; none when the outbox is empty |
| `placement_record` | `flushPlacementRecords` | the outbox head, then the snapshots | `saveSpawnPlacementSnapshot`, the deferred trace save, every write that ends an operation, `placementKick` | 2 s while an over attempt's snapshot is unsealed or a deferred trace save waits; none for running attempts |
| `consult` | `reconcileConsults` | `activeConsults` | `insertConsult`, `updateConsult` | 5 s while a consult is active (a running one waits on a job record no write announces); none when none is |

A new writer that gives a loop work calls `noteWork` after its commit (or ends
its transaction with `commitNoting`), never before: a loop that took the mark
earlier would read the old state and spend it. `TestReadGateWritersNoteTheirLoops`
lists the writers; `TestReadGatesIdle` holds an idle hub to zero store calls
between backstops.

## Spawn queue and the terminal transitions (hk 5299)

`spawn_queue(request_id, next_check_ms)` holds one row per non-terminal
operation, so the coordinator's scans (`pendingSpawns`, `pendingSpawnPage`)
read the work still to do and not the whole of `spawn_operations`. The row is
inserted in the transaction that inserts the operation and deleted in the
transaction that ends it; `next_check_ms` mirrors the operation's parked wake
time and every writer of that column copies it across in the same
transaction. Migration `0030_spawn_queue_outbox.sql` copies the operations that
predate the tables on Postgres. On both dialects, every start also runs
`reconcileSpawnQueue`: it queues each non-terminal operation, refreshes the
queued `next_check_ms`, drops queue rows of terminal or removed operations, adds
an outbox row for each unjudged failed or refused operation, and drops outbox
rows whose operation is judged. That is what takes in a rollback: an older hub
binary writes operations without touching these tables, and the first start
after returning to this release makes them equal the operations again.

Terminal statuses are `succeeded`, `failed`, `refused`, `given_up` and
`cancelled`; `retryable`, `deadline_exceeded` and the `*_grant` refinements
are not. Every write of a terminal status calls `endSpawnQueue` (directly, or
through `releaseEndedSpawn`) in the same transaction, which also writes the
`task_fail_notice_outbox` row for `failed` and `refused`:

| Writer | Terminal status | Queue exit |
|---|---|---|
| `completeSpawn` | `succeeded`, `failed` | `endSpawnQueue` |
| `endUnstartedAttempt` (expiry) | `given_up` | `releaseEndedSpawn` |
| `giveUpWaitingSpawn` | `given_up` | `releaseEndedSpawn` |
| `endWaitingSpawn` | `refused`, `cancelled` | `releaseEndedSpawn` |
| `cancelStagedSpawn`, `discardStagedSpawn` | row deleted | `dequeueSpawn` |

A new writer of a terminal status must use one of these.

### Outboxes

The coordinator reads two small tables for the work it still owes, instead of
scanning the history tables, which keep every row they ever held (hk 5298;
design hk:doc `design/2026-11-18/pw-4898-hub-store-mysql` §2-§3).

| Outbox | Holds | Written | Deleted |
|---|---|---|---|
| `placement_record_outbox(request_id, attempt)` | each `spawn_placement_snapshots` row whose `recorded` is not 1 | in the transaction that writes the snapshot | in the transaction that sets `recorded=1` |
| `spawn_notice_outbox(event_id)` | each `spawn_notifications` row whose `delivered` is not 1 (not routed, held, or being published) | in the transaction that writes the notification | in the transaction that sets `delivered=1` |

- The recorder lists the outbox joined to the snapshot and the operation,
  oldest operation first, and the probe reads one outbox row when a write
  kicked it or a due time passed (hk 5302): an idle hub reads nothing. The notice router pages the outbox and joins the
  notification for its state.
- `spawn_placement_snapshots` and `spawn_notifications` keep their rows and
  their `recorded`, `delivered` and `telemetry_recorded` columns. The telemetry
  claim and the stuck-notice lookup still go by `event_id` on
  `spawn_notifications`; the outbox row does not wait for telemetry, which a
  successful notice never records. Hk 5297 prunes the history tables by age.
- A crash between the work and the delete leaves the row. The next pass redoes
  the work under its idempotency key: the hk placement record is keyed by
  job and attempt, and a lane event by its event id.
- Every start, on both dialects, makes each outbox equal the history rows still
  owed (`reconcileSpawnOutboxes`): it adds a row for every snapshot not yet
  recorded and every notification not yet delivered, and drops a row whose work
  is done. That carries over the pending rows at the upgrade, and after a
  rollback it takes in what an older hub binary left, since that binary writes
  and seals history rows without touching the outboxes. Migration
  `0028_spawn_outboxes.sql` creates the Postgres tables and does the same copy.
