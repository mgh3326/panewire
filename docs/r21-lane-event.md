# R21 direct lane events

`lane.event` lets an external producer put a durable notification into one
named operator lane. It is a notification transport, not a job lifecycle: it
does not create an active job, take part in late registration, or enter orphan
sweeping. The older report contracts remain in
[R19 lane routing](r19-lanes.md) and [R20 relay persistence](r20-relay-persistence.md).

The producer supplies a destination `lane`, an `event_id`, and `text`.
`lane` is a direct address in `lanes.json`; the hub does not consult that
lane's parent. The durable handoffkeep record uses `owner_lane` for this
destination, and its idempotency key is `(owner_lane, event_id)`. Job id,
epoch, report path, and reason are not part of that key. Reusing an event ID
in the same lane is a producer error; `panewire emit` exits nonzero and writes
`duplicate event_id` to standard error. The same event ID in a different lane
is independent.

`text` is required and must contain no NUL or C0/C1 control character
(including tab, CR, and LF). The lane's `lanes.json` route is authoritative:
ordinary pane routes permit 2048 bytes, while a sink route permits 8192 bytes.
See [R23 console surface](r23-console-surface.md) for the `--sink` producer
choice and the hub's route-based enforcement. Producers must escape structured
payloads before passing them as text. When text is too long, the node truncates
it once at a UTF-8 boundary, appends `[truncated]`, persists that final form,
and broadcasts `relay.truncated`; no later component applies the 240-rune
job-report rule. This remains the one-node truncation point: the producer
chooses only a 2048- or 8192-byte bound, and the hub validates rather than
truncating, so stored record, wire payload, and outbox acknowledgement stay
identical.

For example:

```sh
panewire emit --kind lane.event --lane lane-a --event-id producer-42 --text "approval requested" --host host-a --pane w1:p1
```

Without `--pane`, the command records the pane named by `HERDR_PANE_ID`, if any.

The node sends that pane to the hub as the wire payload's `pane_id`, the
emitting pane, so a design lane can keep a worker's own progress in its inbox
(see [quiet delivery](quiet-delivery.md), "Worker results stay in a design
lane's inbox"). It is bounded to 128 bytes with no control byte (below 0x20, or DEL); a pane over the
bound is sent as no pane, never cut. A node's own notice about a pane, a stall
notice (`stall:` event id) or an idle-wake, sends none. The hub reads a
`pane_id` it cannot use as no pane.

**Rollout: hub and nodes deploy in either order.** A hub before this change
refuses a `lane.event` payload with a key it does not know, so a node adds
`pane_id` only on a connection whose hub sent
`{"type":"hub.features","features":[…,"lane_event_pane"]}`, and it forgets
that on every new connection. The pane is added when the event is written to
that connection, not when it is queued. A refused event was never
acknowledged, so it stays in the node's outbox either way. An older node sends
no `pane_id`, and the hub delivers its events as before. The first events a node
sends on a new connection, before the hub's `hub.features` arrives, carry no
pane and are delivered as before.

The command writes an atomic mode-0600 file under `events-lane/` before it
tries the local daemon socket. That namespace is separate from `jobs/<id>/`:
there is no real job ID to collide with, and job scanners cannot mistake a
lane event for a claim, completion, or orphan. If the daemon is absent, the
file remains and the command reports the established file-only message with a
successful exit. If the daemon is reached but rejects the inbox namespace, the
file still remains but `emit` exits nonzero and prints the daemon's reason to
standard error.

The hub persists the event before injection. If no lane route is deliverable,
the hub uses the node and pane of the sole live job whose agent label matches
the lane. It logs the fallback with `route=job-record` and uses the normal
relay and receipt path. Orphaned and terminal jobs do not count as live.
Zero or multiple live matches remain unrouted.

If no route is available or the queue cannot accept an injection, the hub
still stores the undelivered record and sends
`relay.persisted` to the producer node so its outbox can retire. It also emits
`relay.unrouted` for observation. A later destination-node registration
replays undelivered lane events without restarting the hub. That replay asks
handoffkeep only for `lane.event` rows and advances by durable ID, so older
undelivered report rows cannot hide a newer direct notification. Once the hub
has the durable row ID, producer resends receive `relay.persisted` again but do
not re-append the row or spend delivery budget.

Replay uses the same `delivered_at` and `attempts < 3` gates as R20. A queued
injection that does not receive its delivery acknowledgement spends a delivery
attempt and releases the active dedupe claim; a later destination registration
can therefore retry it without a hub restart. This is deliberately bounded:
after three recorded delivery attempts, the hub broadcasts
`relay.replay_exhausted` and does not inject that undelivered row again. A
delivered row is never injected again.

**Deploy handoffkeep schema v7 before a hub that emits `lane.event`.** A hub
connected to an older server cannot durably append this kind, sends no
`relay.persisted`, logs an error, and increments the unpersisted relay counter;
the producer must retain its file until a v7 server is available.

Consumers receive exactly this prompt text:

`(같은 내용이 두 번 보이면 재실행 금지) [event] <lane> :: <text>`

They should treat it as an instruction notification, observe the header's
duplicate-execution warning, and acknowledge it through the existing relay
delivery path. That consumer delivery acknowledgement records the destination
pane in handoffkeep; it does not travel to or depend on the producer's
machine. The earlier `relay.persisted` acknowledgement is separately returned
to the producer node when the hub accepts durable responsibility.

## How a sender checks delivery

`panewire emit --kind lane.event` prints the event id it wrote and the command
that answers where it is. `--event-id` may be given; without it `emit` generates
one and prints it. The id is also in the durable `events-lane/` record.

```sh
$ panewire emit --kind lane.event --lane lane-a --event-id producer-42 --text "approval requested"
event_id=producer-42
panewire inbox status --event-id producer-42
```

`panewire inbox status --event-id ID` asks the hub's ack-state and prints one
line per event:

```text
event=producer-42 lane=lane-a stage=delivered reason=- machine=host-a pane=w1:p1 acked=no
```

The stages are:

| stage | meaning |
|---|---|
| `queued` | the hub accepted the event; no pane has proven it yet |
| `held` | the pane was busy, so the node holds the text until it can type it |
| `typed` | the node typed the text; proof is still pending |
| `delivered` | the pane proved it received the text |
| `unconfirmed` | the node could not prove reception; `reason` says why (`maybe_in_pane presend:composer_pending`, `ack_timeout`, `working_wait_expired`) |
| `dropped` | the node gave up, or the hub found no pane to take the event because every hub-known job in its lane has completed; `reason` says why (`job_ended` for the hub's case) |
| `inbox_only` | the lane reads this event from its inbox, not from its pane |

`--json` returns each event's `reason`, `machine`, `pane` and `at` as well; `at`
is the latest stage change. The stage is stored on the lane's inbox row, so a
hub restart does not lose it. Scoping is the same as the ack answer: a node
credential sees only the events its own node relayed, and another node's id is
answered exactly like an unknown one (401). The operator and a service holding
`inbox.ack_state` see every row. An id with no visible row prints `not found`
and exits 7.

## Bounded relay bookkeeping

The hub removes a `lanePersisted` entry and its `relayTimeouts` suppression
entry when the destination confirms delivery. Both are also bounded LRU caches.
Evicting `lanePersisted` is not delivery loss: a producer resend re-POSTs the
same first-writer-wins key and receives the same durable row, at the cost of one
POST. An evicted timeout suppression key can only cause one extra
`relay.unconfirmed` observation. `relay.replay_exhausted` is broadcast and
counted once per durable row (subject to a bounded remembered set); eviction can
only repeat that observation. The three-attempt exhausted contract is unchanged:
each acknowledgement timeout now records its attempt and releases the active
lane-event claim, ensuring the existing gate is actually reached.

## Job completion signal

A lane event stays outside the job lifecycle on the node and the hub, but the
completion watcher on the worker's host reads `events-lane/` for one purpose. It
completes a job when the report is stable across two polls and the current
attempt has a lane event from the job's pane, written at or after the attempt
started, whose text starts with `READY` or `DONE`, after an optional `[label] `
prefix.

A pane lane event starting with `ESC` asks a question and records
`job.waiting_decision` with its event ID and time; it does not complete the
job. The decision wait defaults to six hours and can be set in seconds with
`PANEWIRE_DECISION_TIMEOUT_S`. An unanswered decision becomes `lost` with
reason `decision_unanswered`. A proven `panewire prompt` delivery to the pane,
a `relay_delivered` record for its lane and pane, or a later lane event from
the pane resumes the job.
Pane redraws and status changes do not clear the wait. If the daemon cannot
be queried, the watcher warns and the decision deadline still applies.
An explicit `panewire escalate JOB --question ...` stops as escalated.

The attempt starts at the newest `job.spawned`, `job.reclaim` or `job.reprompted`
record. When `findings.json` beside the report is a regular file, the watcher
attaches it as the result using the same validation as `panewire done --result`.
Invalid results are rejected with a `job.result_rejected` event and a warning;
the job still completes. `panewire done` still completes a job directly. A report with no signal
leaves the job live until the watcher deadline, and the job stays live after it.

On node startup, live jobs with pane receipts and no watcher holding the watch
lock get a new watcher. Watchers run in their own session so a daemon restart
does not signal them. A re-armed watcher reads existing terminal signals and
completes once it observes the report stable across two polls. Terminal and
reaped jobs are left alone. A re-armed watcher waits to observe its pane before
it can declare the pane lost; if the pane never appears, expiry retains quota.
