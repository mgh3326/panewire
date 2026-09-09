# Idle-wake owner notification

Idle-wake observes a real herdr agent changing from `working` to `idle` or
`done`. After the pane stays continuously non-working for 60 seconds (or the
explicit `--idle-wake-settle` test override), the node produces one durable
R21 `lane.event` for the pane's owner lane. It is notification only: it does
not complete, join, lose, spawn, or reap a job.

## Observation and routing sources

The node reads status, pane, workspace, and revision from herdr `agent.list`
through `HerdrClient.AgentStates`; the optional display label is joined from
the existing herdr `tab.list` source. Status-change subscription events and a
five-second authoritative snapshot poll feed the same SQLite state journal.

The hub resolves an already-settled request at request time:

1. `loadReportRelayRoutesResult` hot-loads the existing `lanes.json` shape.
   A unique `reportRelayRoute` whose `Machine` and `Pane` match the source
   pane resolves to that route's existing `Parent` field.
2. A matching route with no parent is an operator/owner pane and is
   suppressed. A missing parent route or multiple matching routes also fails
   closed.
3. If no lane route directly names the pane, the only fallback is one exact
   pane match in `hubNodeRecord.activeJobs`. That map is the latest
   authenticated heartbeat produced by `scanHubActiveJobsWithPanes`: a valid,
   non-terminal `job.claimed`/`job.claim` record supplies the owner and a
   `job.spawned` record supplies the live pane. No match, multiple matches, an
   empty owner, or an owner absent from `lanes.json` produces no event.

A direct lane match takes precedence over active-job metadata, including when
the two sources disagree. Idle-wake does not add or reinterpret any
`lanes.json` field.

## Sequence and recovery contract

`state_change_seq` is a monotonically increasing, pane-local counter in the
panewire SQLite journal. It increments on each accepted status change. It is
not the herdr revision. The journal also owns a random 128-bit namespace that
survives an ordinary panewire restart. The producer event ID is:

```
idle-wake:<node-journal-namespace>:<base64url-pane>:<state_change_seq>
```

The existing R21 durable key remains `(owner_lane, event_id)`, so this gives
one durable event per `(owner_lane, pane:state_change_seq)`. Repeated snapshots,
replayed status events, route retries, file rescans, node restarts, and a
handoffkeep duplicate response all retain that same key. A later genuine
working-to-non-working change has a new local sequence and therefore a new
event.

Herdr revision is used only to reject stale observations and detect an
upstream namespace reset. When an authoritative snapshot moves backwards or
loses a previously nonzero revision entirely, an unsettled candidate is
cancelled because continuity across the restart is unknown. An event carrying
an older revision is ignored, and two different
states carrying the same nonzero revision fail closed. A candidate that was
already durably marked settled remains retryable. After the reset, a newly
observed `working -> idle|done` transition receives the next node-local
sequence. A pane missing from a complete `agent.list` snapshot is recorded as
`unknown` and its unsettled candidate is cancelled. Loss of the panewire
SQLite journal creates a new namespace; an
already-idle first snapshot is only a baseline and cannot synthesize a wake.

On node startup, recovery retries only candidates already marked settled and
already-assigned event files. It cannot advance a merely due, unverified
candidate while herdr is unavailable. Once a current herdr observation has
been accepted, the ordinary settle tick may advance a continuously
non-working candidate.

If the schema guard cannot prove herdr event support, the observation loop
re-probes on a bounded `1s, 2s, 4s, ... 30s` delay. It does not run the external
schema command on the ordinary 100-millisecond socket reconnect cadence, and a
successful capability probe proceeds to subscription without another delay.

Terminal candidate rows are retained for the existing
`PANEWIRE_RELAY_OUTBOX_MAX_AGE` horizon (24 hours by default) and then pruned.
Only cancelled or suppressed rows and assigned rows with a completed local
materialization timestamp are eligible. Unsettled, unassigned, and
unmaterialized retry rows are never removed by this retention pass;
`idle_wake_panes` and its monotonic sequence remain intact.

## Route changes and durable acceptance

Route lookup happens after settle, so a parent reassigned during the 60-second
window receives the event. The first valid route decision written to the node
journal is then pinned. A later route response or a route change during a
persistence retry cannot change it.

The node accepts route responses into a bounded FIFO handled by one worker,
keeping SQLite and file work off the WebSocket read loop while preserving
receive order. If that queue is full, the newest response is dropped and the
still-undecided candidate repeats its route request after the normal retry
interval.

The node writes the atomic mode-0600 `events-lane/` record before enqueueing
the WebSocket event. A local write failure leaves the assigned candidate
retryable. A queue failure leaves the file for the existing scanner. The R21
outbox records `sent_at` only after a successful WebSocket write and records
`persisted_at` only after `relay.persisted`; a handoffkeep failure therefore
spends no durable idempotency or pane delivery. Handoffkeep's first-writer-wins
response retires the same outbox row without a second injection.

Pane, label, lane, and text values cross process boundaries as JSON protocol
fields and `exec.Command` arguments. They are never concatenated into a shell
command. The generated text is bounded by the ordinary direct-lane 2048-byte
contract and rejects the same control characters as every R21 event.

## Isolated real-process E2E

This path is intentionally manual and test-only because it requires a real
agent status transition and a wall-clock 60-second settle. Never point it at a
fleet herdr socket, a normal panewire database, a shared inbox, or a deployed
hub.

1. Build the candidate binary and create a new temporary root. Under that
   root, provision a dedicated herdr config/server, panewire database and
   inbox, hub auth files, loopback listener, instrumented handoffkeep fixture,
   and logs. Every herdr command, including stop/restart, must carry the
   dedicated config selector. Resolve the test server's socket from that
   isolated instance rather than assuming the normal socket path.
2. Start a dedicated owner pane and worker pane. Use synthetic identities such
   as `machine-idle-e2e`, `lane-owner-e2e`, and `lane-worker-e2e`. The worker
   lane route names the worker pane and has `parent: lane-owner-e2e`; the owner
   lane names the owner pane and has no parent. Start a dedicated hub and node
   against those files with `--idle-wake-settle 60s`. A loopback hub still
   needs the repository's test-only insecure transport seam or a test TLS
   endpoint; production CLI transport validation must not be weakened.
3. Record the isolated process identity class, test-pane identity class,
   initial durable row count, and UTC timestamps. Re-prompt the worker so real
   herdr observations show `working` and then `idle`. At 59 seconds the owner
   count must remain zero. After the full 60 seconds, require one durable row
   with one owner-pane receipt.
4. Wait at least one more settle interval, restart only the isolated node using
   the same SQLite and inbox, and wait again. The durable row and owner receipt
   counts must remain one. Re-prompt the same pane through a new real
   `working -> idle` transition; both counts must become two.
5. Exercise `working -> done` once and require the same single-event result.
   Exercise a return to `working` before 60 seconds and require zero for that
   candidate, followed by one for the next stable transition.
6. Reassign the worker lane's parent before settle and confirm only the new
   parent receives one event. Separately, let the original route decision
   arrive, force handoffkeep persistence to fail, change the route, restore
   persistence, and confirm the retry produces one event in the originally
   pinned lane and zero in the later lane.
7. Restart only the isolated herdr server so its revision namespace changes.
   An event already settled remains at one; an unsettled event spanning the
   reset produces zero; the next fully observed working-to-idle transition
   adds exactly one. Restarting the node after that must not change the count.
8. Repeat against the parentless owner pane and require zero durable rows and
   zero receipts. Also test an unknown pane and two active-job records naming
   one pane; both must remain zero.
9. Capture actual timestamps and observed counts from the isolated processes
   and durable store. Expected-value printouts are not evidence. Keep the raw
   evidence private, redact installation-specific paths and identifiers in
   summaries, and tear down only the temporary test processes and root.

The countable minimum is therefore `1 -> 1 after wait/restart -> 2 after a new
sequence`, with `0` for a parentless pane. Route-reassignment, persistence
failure, and herdr-reset observations are separate required rows in the E2E
evidence table.
