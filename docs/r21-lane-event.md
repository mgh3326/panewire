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

`text` is required, must contain no NUL or C0/C1 control character (including
tab, CR, and LF), and is limited to 2048 bytes. Producers must escape
structured payloads before passing them as text. When text is too long, the
node truncates it once at a UTF-8 boundary, appends `[truncated]`, persists
that final form, and broadcasts `relay.truncated`; no later component applies
the 240-rune job-report rule. This one-node truncation point keeps the stored
record, wire payload, and outbox acknowledgement identical.

For example:

```sh
panewire emit --kind lane.event --lane lane-a --event-id producer-42 --text "approval requested" --host host-a --pane w1:p1
```

The command writes an atomic mode-0600 file under `events-lane/` before it
tries the local daemon socket. That namespace is separate from `jobs/<id>/`:
there is no real job ID to collide with, and job scanners cannot mistake a
lane event for a claim, completion, or orphan. If the daemon is absent, the
file remains and the command reports the established file-only message with a
successful exit. If the daemon is reached but rejects the inbox namespace, the
file still remains but `emit` exits nonzero and prints the daemon's reason to
standard error.

The hub persists the event before injection. If the lane is not present in
`lanes.json`, its target node is disconnected, or the queue cannot accept an
injection, the hub still stores the undelivered record and sends
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
