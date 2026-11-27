# R20 relay persistence

R19 relayed reports from files the node rescanned every ten seconds, and the
"already sent" mark lived in node memory. A restart therefore replayed
everything the inbox still held. R20 keeps the files but moves the canonical
record into Postgres, behind handoffkeep.

The same cursor machinery also carries direct producer notifications under the
[R21 lane-event contract](r21-lane-event.md); its separate key and isolated
file namespace keep those records out of job lifecycle processing.

## The flow

```
worker
  │  panewire emit --kind job.completed --job <id> --report <path>
  ▼
jobs/<job_id>/events/NNNNN-<kind>.json      (written first, mode 0600, atomic)
  │
  │  local unix socket, op "emit"
  ▼
panewired ── hub client immediate queue ──► hub (websocket)
                                             │
                                             │ 1. resolve lane route
                                             │ 2. POST /v1/relay/events   ◄── canonical record
                                             │ 3. relay.inject → node
                                             │ 4. relay.persisted → node
                                             ▼
                                        node injects into the pane
                                             │  relay.delivered
                                             ▼
                             POST /v1/relay/events/{id}/delivered
```

Persistence happens **before** injection. If handoffkeep refuses the record or
is unreachable, the hub does not inject: it broadcasts `relay.unpersisted`
with `{"job_id":…,"kind":…,"reason":"persist_failed"}` and increments
`unpersisted_relay_events`. The node never receives `relay.persisted`, so the
event stays in its outbox and is retried. Nothing is silently dropped.

## The node must always learn the answer

An outbox row is retired by `relay.persisted` and by nothing else. Every path
that ends without sending one leaves `persisted_at` NULL, so the node resends
the event after every restart — and if the hub also kept an in-memory dedupe
key for it, each of those resends is swallowed and the row is stuck for the
life of the hub process. Four paths used to end that way:

| path | before | now |
|---|---|---|
| the hub already took this event | dropped, key kept | re-POST (200) and `relay.persisted` again; **no** second injection |
| no route or no connected node | key kept, nothing stored | key deleted, `relay.unrouted` |
| `queueRelay` refused the injection | key kept, no acknowledgement | key deleted, `relay.persisted` sent, `relay.unrouted` |
| an event differing only in `reason` | folded into the earlier one | a separate record, because the key now counts `reason` |

The hub's dedupe map is `key → handoffkeep row id`. A key that stands for no
durable row is deleted rather than left behind, and a key that does stand for
one answers a resend instead of swallowing it.

**The event files are no longer the canonical record.** They are the offline
fallback: they let `panewire emit` succeed with no daemon running, and they let
a node that was offline hand its backlog to the hub on reconnect. The answer to
"was this report delivered" lives only in Postgres.

## What a node already delivered, and a gap after a restart

The node's `relay_delivered` table (`relay_delivered_store.go`) is the durable
fact that a pane received a row. The `relay.delivered` ack that carries it to
the hub is a socket event: a socket that dies between the delivery and the ack
left the hub row open, and the next restart replayed a batch the pane had
already read (hk 3435).

- **Applied report.** A node that advertises the capability `applied_v1` sends
  an `{"type":"applied","applied":{lane:[row ids]}}` message right behind its
  hello and ping: the row ids it delivered, newest first, at most 256 per lane,
  64 lanes and 1500 ids in all (`relayApplied*` constants; the hub reads a
  frame under 32 KiB and refuses a report outside the bounds). The hello itself
  is unchanged, so a hub that predates the report still registers the node
  (it ignores the capability name and replays at registration; the report is
  dropped as an unknown message type) and the capability-free fallback hello is
  followed by no report. The store read waits at most 1 s; past that the report
  goes out empty.
- **The report closes first, then the replay runs.** For a node advertising
  `applied_v1` the hub holds the registration replay until the report arrives.
  It then closes each undelivered row of a reported lane whose id is listed
  (`delivered_to` = `<machine>/applied-report`), and only if the row was routed
  to the reporting machine (the row's own machine, or the lane's current
  route — the entitlement a late `relay.delivered` ack needs); another
  machine's report closes nothing. The report is a set of exact ids, not a
  high-water mark: hub row ids are not commit-ordered per lane, and a row the
  node never saw may sit below one it did, so a max-id cursor would drop it
  silently. A node that dies before it sends the report replays at its next hello;
  a node without the capability replays at registration as before.
- **The settlement does not hold the replay lock** (hk 4981). The hub takes
  `replayMu` only to claim the report, to refuse one that
  `expireAppliedReport` already released, and to release the deferred replay;
  the handoffkeep listings run under a per-report budget
  (`hubAppliedSettleBudget`, 30 s). A report held across those listings in
  earlier builds delayed every other node's `connect()`, and the waiting nodes
  lost their sessions. The ordering survives through the registration's
  `appliedPending` flag, not the lock: it stays set until the last row the
  report names is closed, so a replay that runs while the report settles still
  skips every row routed to that node. A report that arrives after expiry is
  refused and its rows stay open for the next registration's report; the
  per-report expiry is suspended while a claimed report settles.
- **A gap is one line.** The hello replay and the startup replay hold every
  `lane.event` row that passed the replay gates until the walk has counted its
  lane. A lane owed more than `relayReplayGapThreshold` (20) rows gets one
  summary line — `[panewire] gap: N events for <lane> were not replayed after
  the restart (first …, last …) | panewire inbox list --lane <lane> --unread` — filed as
  an interrupt lane.event with id `replay-gap-<last row id>`, and the rows the
  lane inbox holds retire with `hub/replay-retired:gap`. The summary is filed
  first, so a failure replays the rows and a repeated walk reuses the same
  summary. A hub with no lane inbox (`--delivery-policy off`, no store) never
  summarizes, since the line points at `panewire inbox list`; a row the inbox
  lacks replays. A lane within
  the threshold replays row by row as before. Rows held for their one labelled
  `[replayed after hub restart]` replay (#1002) are outside the count.

## `--handoffkeep-env`

`panewire hub --handoffkeep-env <path>` enables persistence. The file must be a
regular mode-0600 file holding these keys:

```
HANDOFFKEEP_URL=…
HANDOFFKEEP_TOKEN_SVC_PANEWIRE=…
# optional, both or neither: a handoffkeep behind Cloudflare Access
HANDOFFKEEP_CF_ACCESS_CLIENT_ID=…
HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET=…
```

A file written before hk 3570 may carry `HANDOFFKEEP_TOKEN` instead of
`HANDOFFKEEP_TOKEN_SVC_PANEWIRE`; the svc key wins when both are present
([config.md](config.md#handoffkeep-service-credential)).

The same file format is the node's `spawn.json` `handoffkeep_env` (brief
fetch) and the hub's relay store. When the CF Access pair is present, every
handoffkeep request (relay, node brief fetch) sends it with the bearer; a
file with only one of the two is refused.

Values never appear in a log line or an error message. A plaintext `http://`
URL is accepted only for loopback and tailnet hosts, matching what the hub
already accepts for its own listener; everything else must be `https://`.

Without the flag the hub behaves exactly as it did before R20: it injects
without persisting and calls handoffkeep zero times.

At startup a configured hub reads `GET /v1/relay/events?undelivered=1` once and
re-routes what it finds. Those records are already stored, so they are never
re-persisted: no replay creates a row. A destination node that is not connected
yields `relay.unrouted`, as before. The contract exposes no cursor, so one
startup replays at most `limit` (200) distinct events, oldest first.

### `delivered_at` gates injection, not just replay

The same authority applies to a live send. When a POST comes back **200** (the
row already existed) **with a `delivered_at`**, the note is already in the pane:
the hub does not inject it again. It still sends `relay.persisted` — the node
retires its outbox row on that and nothing else, so withholding it is what
turns a settled record into a permanent resend — and broadcasts
`relay.already_delivered` with
`{"job_id":…,"kind":…,"event_id":…,"delivered_at":…,"reason":"already_delivered"}`,
counted in `already_delivered_relay_events`.

A **201** is a row nothing can have delivered yet, and a 200 with an empty
`delivered_at` is a row that never reached its pane; both inject as before.

The authority here is handoffkeep's `delivered_at`, never the node's `replay`
flag. `replay` says a node restarted, which is a different question from
whether this record ever reached the pane; it stays log and event metadata, as
the R19f contract requires.

The startup replay is gated. A row is re-injected only if `delivered_at` is
NULL **and** `attempts < 3`; without that bound a row whose destination never acknowledges is
re-injected on every hub restart, forever. A row over the limit is not dropped
silently — it is broadcast as `relay.replay_exhausted` with
`{"job_id":…,"kind":…,"event_id":…,"attempts":…,"reason":"attempts_exhausted"}`
and counted in `replay_exhausted_events`.

Two things spend an attempt, and both record it the same way: a startup replay
that queued an injection, and an injection the node never acknowledged
(`relay.unconfirmed`).

**A delivery is recorded even after its ack window is gone (#650).** The
window that matches `relay.delivered` to a row lives in hub memory. A
`relay.unconfirmed` retires it before the node's retry lands, an expiry drops
it, and a hub restart loses all of them. A `relay.delivered` that matches no
window is therefore checked against the row itself: its `original_event_id`
names the row, its `job_id` must be that row's transport id, and it must come
from the row's destination or the owner lane's current route. If all three
hold, the hub sets `delivered_at`. Otherwise it is counted as an unknown
message, as before.

**Replay retires rows that must not reach a pane (#650).** Before a replay
spends an attempt, the hub closes these rows. Each gets a `delivered_to` of
`hub/replay-retired:<reason>` and a `relay.replay_retired` broadcast
`{"event_id":…,"kind":…,"lane":…,"source_event_id":…,"reason":…}`:

- an idle-wake whose pane is no lane's pane in the lanes file any more,
  because the lane was removed or moved to another pane (`lane_gone`). If the
  lanes file cannot be read, no row is retired for this reason;
- an idle-wake whose `changed_at` is more than 30 minutes old (`stale`);
- any other row whose handoffkeep `received_at` is more than 24 hours old,
  which is the node outbox's own default bound (`stale`). A row with no
  `received_at` is never aged out. A row whose delivery obligation says
  `working_wait_expired` is aged from that drop instead: it is not retired
  while its [revived copy](#faults-and-copies) may still be asked for.

Legacy `chat-N` rows, written before the operator chat was removed, retire
at replay (`chat-terminal`) and are never injected.

**Store-backed retry.** An acknowledgement timeout, an unconfirmed node report
or a node's drop records a fault on the delivery obligation, and the fault
decides what copy is still owed ([Faults and copies](#faults-and-copies)). The
maintenance sweep can retry while the node remains connected. It commits the
spent attempt and the next check before queueing. A `lane.event` retry carries
the existing duplicate-warning label (a revived working-expired row carries the
idle re-delivery label) and requires handoffkeep to show at least three attempts
first. If
handoffkeep cannot commit that ceiling, the hub queues no copy. A `job.*` retry
has no label and its attempt update is best-effort. A retry leaves the original
handoffkeep row undelivered so later delivery proof can still close it.

The startup and node-registration walks convert legacy markers and defer
scheduled copies to the reconciler. An owed origin keeps its legacy marker
as a rollback breadcrumb.

**Compatibility with the store disabled.** An unproven pane submission is replayed at most once (#1002, #1034). A
`relay.unconfirmed` report that matches a live window still retires the
window, but first writes a durable marker: a `lane.event` row on the
`hub/unconfirmed` lane — a name that can match neither a real lane nor an
agent label, so it can never be routed or counted as producer backlog — with
`event_id` `unconfirmed-mark-<row id>`. The contract offers no column to set
on an existing row: a duplicate POST updates only `attempts`, and `/delivered`
would remove the row from the undelivered listing before its one replay and
would burn the first-write-wins `delivered_to` a late proof needs. The marker
row, not the original, therefore carries the mark, and the marked row stays an
ordinary undelivered row so a late `relay.delivered` still closes it under the
real machine/pane. If the marker write itself fails — or a read the mark
needs errors, the row it names or the transport-id lookup an id-less window
still requires — the window is kept, not retired: a repeated report retries
the mark, and the ack timeout decides otherwise, exactly as for any other
live window.

On startup and on the node-registration replay the hub collects the live
markers from the `lane.event` undelivered listing, then walks the pages as
before. A marked row gets **at most one** replay whose pane text is prefixed
`[replayed after hub restart - first sent <original time in KST> - may be a
duplicate, do not re-run]` — the original send named by the row's `event_time`
(`received_at` for older rows) — and no labelled copy is ever queued until
the durable retire for that row has already succeeded. Once the listed row
clears the pre-queue gates (age, attempts, dedupe, route) the
walk re-reads it — a listing page is stale the moment it is taken — and a
row that is delivered, retired, or gone is left to its marker rather than
injected. Only then is the retire written, `delivered_to`
`hub/replay-retired:unconfirmed`. A failed retire spends nothing: the mark
stays pending and the next trigger retries the whole sequence, so a store
that keeps refusing the write defers the replay instead of repeating it
every restart. The pane's injection queue is checked for room before that
retire: with no room the replay is deferred, the row stays unspent, and the
next trigger tries again (hk 6893). When the retire lands but the queue refuses
the directive anyway (it filled in the gap between the check and the queue
write), the one replay is spent without reaching a pane — the hub broadcasts
`relay.unrouted` and logs one warning naming the row, and never re-marks it. The spent marker then retires with
`hub/replay-retired:unconfirmed-mark`; a marker whose original is already
delivered, exhausted, or gone retires on sight. Every other row class —
never-attempted, held, exhausted, sink — replays exactly as before.

The lane-stall observation counts a marked row as ordinary backlog until its
one replay retires it — bounded by the next trigger that can route the row,
never forever. The marker row itself is on an unroutable lane and never
counts.

**A successful replay is announced too (#961).** Once a re-injection has
cleared every gate — the delivered and retired rows, the age
gates, `attempts < 3`, dedupe, route resolution, and the sink rejection —
and the directive is queued and the attempt recorded, the hub logs one Info
line and broadcasts `relay.replay_injected` with
`{"event_id":…,"kind":…,"lane":…,"machine":…,"pane":…,"attempts":…,"received_at":…,"source":…}`.
`attempts` is the count after that replay's own bump, and `source` is
`startup` for the once-at-boot replay or `hello` for the replay a node
registration runs. Before this broadcast the feed named every refusal
(`relay.replay_retired`, `relay.replay_exhausted`, `relay.unrouted`) but
never a success, so a restart that first-delivered rows that had sat for an
hour was invisible until the panes showed them.

**A stalled lane now has an alarm (#961).** Each maintenance sweep evaluates
every non-sink lane: when the oldest undelivered durable row for it — not
retired, `attempts` still below the replay bound — is older than 30 minutes
(`relayLaneStallAge`) **and** the lane has active work (it is an active
job's `owner_lane`, or the parent of one, in the nodes' active-jobs view),
the hub broadcasts `relay.lane_stalled` with
`{"lane":…,"oldest_event_id":…,"oldest_received_at":…,"undelivered_count":…,"active_jobs":…}`,
logs one Warn per stall episode, and routes the alert through the standard
dampening (key `lane-stall:<lane>`, check `relay.lane_stalled`, the same
grace and observation counts as the other hub alerts) so the notifier sends
one incident and a recovery when the backlog clears. A lane with no active
work is never stalled, and a handoffkeep read error neither starts nor ends
an episode. Neither event changes delivery: nothing is re-injected,
rerouted, or retired by the alarm — it observes and tells.

**handoffkeep exposes no endpoint that sets `attempts`.** The hub therefore
re-POSTs the row it already stored: the idempotency key collides, handoffkeep
answers 200 with `attempts + 1`, the row itself is unchanged (first-writer-wins,
below), and `delivered_at` is not touched by that path. The contract is
unchanged; this is the counter it already had.

## Delivery obligations

An obligation is the hub's own record that one relay row is still owed a
delivery (hk 4289, step T1: hk 6384). It lives in the hub store, beside
`lane_inbox`, and shares the inbox item's `event_key` and `relay_id`. The
handoffkeep row stays the record of the event and of its delivery.

Four tables, with the same columns on SQLite, Postgres (migrations 0040 and
0044) and MySQL (migrations 0033-0036 and 0041):

| Table | Holds |
|---|---|
| `delivery_obligations` | one row per tracked event: state, destination, settlement reason, next read |
| `delivery_reconciler_state` | the one row with the adopt cursor, seeded when the store opens |
| `delivery_incidents` | one row per notice episode: its key, members, announced count, last notice and close reason ("Incidents and notices") |
| `delivery_notice_slots` | when each (design lane, machine, fault) was last told, kept across incidents |

`panewire hub store-copy` carries all four. Retention keeps the record of a
finished delivery for 30 days past its close, then prunes it: a `confirmed`
or `settled` obligation whose `updated_at` is past the window, a closed
incident no obligation still names, and a notice slot past its `notice_at`
that no open incident still holds. An open obligation — `owed`, `noticed` or
`exhausted` — is never deleted, and neither is a closed one an open incident
still names.

### What the operator sees

`panewire inbox status --event-id` names, beside the delivery stage, the
obligation of the row the answer's delivery fields came from: `obligation`
(its state), `fault` (what it carries) and `settle` (the reason it closed
with). The fields are absent for an event with no obligation.

The hub feed's `hub.obligations` counts the tables as they stand — `open`,
`confirmed`, `settled`, `exhausted`, the copies `retries` and `revived`,
`incidents_open`, `incidents_closed` by reason, and `notices_folded` — so the
counts are read off the store and a restart loses nothing.

A `hub/unconfirmed` marker an obligation keeps as `marker_event_id` is
retired in the same pass that closes the obligation: once the origin is
terminal an older binary must not replay it. A marker whose retire write
failed stays linked and each pass retries it, at most 10 a pass. A marker
whose origin is still open stays pending.

### Which events have one

- The relay path creates one after handoffkeep answers 201 or 200 for a
  `lane.event` or a `job.*` event. A failed insert is logged with the row's
  `event_id` and does not hold the relay back.
- An event the delivery policy files in an inbox never reaches handoffkeep, so
  it has none: its inbox item is the record.
- A hub without an inbox store (`--delivery-policy off`, or no store) keeps
  none.

### The pass

Each maintenance sweep starts one pass in the background, unless one is still
running. A pass has 30 seconds to adopt and classify, and 30 more for the
incidents that follow ("Incidents and notices").

**Adopt** creates missing obligations. It reads at most 3 pages of 200 rows of
the undelivered listing, resumes at the stored cursor, and returns to the start
after a short page. It skips delivered and retired rows, marker rows, rows with
no lane or job, and kinds the hub does not relay. It covers a failed insert, a
handoffkeep or store outage, and rows stored before this step. An adopted row
starts with `spent` equal to handoffkeep's `attempts` and no destination.

**Classify** reads the rows of at most 50 due obligations. The first rule that
holds applies:

| Evidence | Obligation becomes |
|---|---|
| `delivered_at` set and a real `delivered_to`, `<machine>/applied-report` included | `confirmed` |
| `hub/replay-retired:<reason>`, except `unconfirmed` | `settled`, reason `<reason>` |
| `hub/filtered:<reason>` | `settled`, `filtered` |
| `hub/inbox`, `hub/chat-terminal` | `settled`, `inbox` or `chat-terminal` |
| `sink/sink:<lane>` | `settled`, `sink` |
| `hub/replay-retired:unconfirmed`, or a `hub/` destination this build does not know | `exhausted`, fault `unconfirmed` |
| the event's inbox item is superseded, acked, delivered or read by the receiver | `settled`, `inbox-delivered` |
| a digest or escalation batch that no unread item is still claimed by | `settled`, `digest-not-replayed` |
| none of these | unchanged |

- `delivered_at` alone never confirms: every `hub/` and `sink` settlement sets
  it too.
- An inbox delivery record counts only once it is a minute old, because the hub
  writes it just before the handoffkeep write that confirms the row.
- An unconfirmed digest releases its remaining claims so the next inbox flush
  can build a fresh digest.
- handoffkeep folds a job's later round into the first round's row when the
  idempotency key matches. That row then says nothing about the later event, so
  only the inbox rule can close the later obligation.
- A row handoffkeep no longer lists, and a read that fails, change no state.
- `confirmed` and `settled` are final. `exhausted` stays open.

A new obligation is first read after 30 seconds. An owed one is read again after
a tenth of its age, at least a minute and at most an hour; an exhausted or
missing one after 4 hours. A failed read is retried after a minute. A requested
revive is read by the next pass, ahead of the other due rows, and so is an
incident member whose delivery the hub just recorded.

An owed row with a scheduled fault uses its durable `spent` and `next_at` for
the retry budget and backoff. A pre-upgrade row retired as
`hub/replay-retired:unconfirmed` remains exhausted.

### Faults and copies

The budget is `spent` reaching 3, counted from handoffkeep's `attempts` when the
first fault is recorded. Backoff between budget copies is 1, 5 and 15 minutes.

| `fault` | Recorded from | What the pane may hold | Copies still owed |
|---|---|---|---|
| `ack_timeout` | no report inside the ack window | unknown | to the budget |
| `pane_read_failed` | the drop after a failed pre-send read | nothing | to the budget |
| `inject_failed_max_attempts` | the drop of a harness named and not devin | nothing | to the budget |
| `unrouted` | a pass that found no route for a row with one of the three faults above | nothing was queued | no spend; to the budget once the route returns |
| `working_wait_expired` | the report or drop | nothing | no budget copy; one revived copy at an idle wake |
| `maybe_in_pane`, `delivery_uncertain_check_pane` | the report or drop; also an `inject_failed_max_attempts` drop from devin or naming no harness | maybe the text | one labelled copy, ever |
| `pane_read_unproven` | a `maybe_in_pane presend:unproven:read_failed` report, until its drop says which | maybe the text | as above; the drop makes it `pane_read_failed` or `delivery_uncertain_check_pane` |
| `prompt_unproven` | a `prompt_failed` report, until a drop settles it | maybe the text (devin may type part of a rejected send) | as above, once the node no longer holds the row |
| `unproven` | any other node reason | maybe the text | one labelled copy, ever |
| `unconfirmed` | a legacy marker or a retired labelled replay | maybe the text | one labelled copy, ever |
| `job_ended` | the drop | the lane's job ended | none; the row stays open in handoffkeep |

- A recorded fault changes only toward what is known. A timeout, `unrouted` and
  `unconfirmed` replace only a timeout or `unrouted`. Among the safe faults and
  the working expiry the latest wins. A maybe-typed fault replaces those and is
  never replaced by them. `job_ended` replaces any fault.
- A node's drop is applied to the obligation even when its report already ended
  the ack window, so a working expiry after a held retry is learned.
- A `prompt_failed` report names no harness. A later drop from a harness named
  and not devin settles it: a safe fault or a working expiry takes its place.
  A drop from devin, or naming no harness, leaves it maybe typed. While the
  node still holds the row, a pass does not end it for this fault.
- An exhausted row is a verdict on the fault it held when it was exhausted. A
  drop that settles that maybe-typed fault to a safe one reopens the row while
  `spent` is below 3: it is owed again and the next pass queues the copy the
  exhaustion ended (hk 6935). The reopen also leaves its incident and withdraws
  a notice that is due and not yet posted; a notice already posted stays posted.
  A spent budget stays exhausted.
- A `job.*` copy carries no label and has no revive: the three safe faults retry
  to the budget, `job_ended` never, `working_wait_expired` gets no copy at all
  (hk 6935), and any other maybe-typed fault gets one.
- An idle wake sets `revive_requested` on one row. The pass takes the revive
  first, ahead of the budget: it commits `revived`, raises handoffkeep's
  attempts to the ceiling and queues one copy. A copy that was not queued gives
  the revive back. A maybe-typed or `job_ended` row never revives, and neither
  does a drop older than 24 hours.
- A build before the revive counted its idle copy in `labelled` alone and kept
  `working_wait_expired` when that copy failed. The first start of a build with
  the revive marks every labelled `working_wait_expired` row `revived`, once per
  store (`PRAGMA user_version` 3 on SQLite, a `panewire_schema_migrations` row
  elsewhere). Rows labelled after that start keep their revive.
  `hub store-copy` of `delivery_obligations` carries that record: the target
  holds it exactly when the source does.
- A row with no route spends nothing and is announced `relay.unrouted` once per
  process. The missing route is no notice of its own; a node's drop it replaces
  is still told ("Incidents and notices"). A `lane.event` is retired as
  `lane_gone` only when it is an idle-wake report (above), never because its own
  pane vanished.
- Exhausted is not settled: the obligation and the handoffkeep row stay open,
  and a late delivery still confirms them.

### Incidents and notices

hk 4289 T4 (hk 6821). Which rows the design lane is told of, and when, is in
[relay-harness-delivery.md](relay-harness-delivery.md), "Escalation". Migration
0044 (MySQL 0041, and an `ALTER TABLE` at open on SQLite) adds to
`delivery_obligations`:

| Column | Holds |
|---|---|
| `notice_due_at` | when the row got a fault no notice has covered; 0 when there is nothing to tell |
| `told_at` | when the row's notice was settled -- stored, withheld, or skipped for good (nobody to act, no design lane, nothing owed, a moot close) -- after which it never becomes due again |
| `fault_reason` | the fault a due row's notice reports; on a row that is not due, the fault an `unrouted` rename replaced |

- A row becomes due in the same write as the change that makes it so: a
  maybe-typed report or drop, the exhaustion of an owed row, or the rename to
  `unrouted` of a node's drop. A row found past the replay window is made due by
  the pass before anything else is done with it. An orphaned drop makes a row
  with no fault due. A report that replaces the fault also records its machine
  and pane, which key the incident.
- Closing an obligation clears its due notice, except a `stale` settlement,
  which makes an owed row with a failed copy due, and a `digest-not-replayed`
  one, which keeps a due notice.
- The pass joins at most 50 due rows to incidents, then for each open incident
  posts its notice if one is due, then closes it if it has ended. The join
  opens the key's incident if it has none (`open_slot`), and an exhausted
  member becomes `noticed`. An incident id starts at the clock in
  milliseconds, so a hub on a new store does not reuse a notice id handoffkeep
  already holds.
- `notice_event_id` is written before a notice is posted. A stored notice
  commits `announced`, `notice_at`, `notice_relay_id` (0 when the notice was
  filed in an inbox), the key's slot and the rows it told in one transaction;
  until then the posted id is unresolved and a row due for it holds the incident
  open.
- A close sets `open_slot` to NULL and detaches the members (`incident_id` 0,
  `noticed` back to `exhausted`). A `moot` close also clears their due notices.
- `hub store-copy` reads a source without the two new columns as having
  nothing due.

Rolling back to a build without incidents is safe: it ignores the columns and
the incident tables, and tells design lanes from the drop again. A row either
build told may be told once more.

## The relay kind set

`emitRelayKinds` in `emit.go` is the single location of the closed set: the
CLI validator, the daemon emit gate, and the node scanner all read that map.
The set's relationship to the shared `TERMINAL` set (the reap contract,
`{job.completed, job.joined, job.revoked}`):

| kind | reap TERMINAL | route | terminal for the hub job record |
|---|---|---|---|
| `job.completed` | yes | owner lane | yes |
| `job.joined` | yes | owner lane's parent | no — reap-terminal, hub record unchanged |
| `job.revoked` | yes | owner lane's parent | yes |
| `job.lost` | **no** | owner lane | **no** — a lost job may still complete |
| `job.escalate` | no | owner lane's parent | no |
| `lane.event` | n/a | named lane | no |

`wrk done` leaves two `job.completed` files in a job directory: the
`internal/fleet/lifecycle.go` envelope, whose `owner_lane` can be the job's own
lane, and the flat record `panewire emit` writes after it. The node scanner
relays the flat record and skips an envelope of the same epoch. An envelope
with no such record is the `relay=file-only` fallback. After one minute it is
relayed to the lane `lifecycle.go` would have given emit.

Every relayed `job.*` kind except `job.completed` must carry a non-empty
`reason`, and `job.lost`/`job.revoked` additionally require a routable
`owner_lane`; a record that cannot state why or name its lane is refused at
emit, at the daemon gate, and at the scanner. That requirement doubles as the
echo guard: the hub's own revocation marker (`writeHubRevocation`) carries
only `type`/`job_id`/`epoch`, so a hub→node `job.revoked` is never relayed
back — only the owner-declared record (`panewire job close`, which writes
`kind`/`owner_lane`/`reason`) is.

`job.lost` writes `report_path` empty (or `/dev/null`, which is normalized to
empty). Like `job.escalate`/`job.joined`, the durable event file then stands
in for the report path — the same substitution on the emit path and the scan
path, so one event cannot key two different outbox rows.

### Version skew

- **Old hub, new node:** a kind outside `knownHubEventKind` is rejected by the
  inbound parse and counted in `unknown_messages`. No injection, no
  handoffkeep call, no job record — harmless but invisible to the operator,
  which is why the hub ships before the nodes that produce the new kinds.
- **New hub, old handoffkeep:** the kind allowlist lives in the handoffkeep
  repository and must name the new kinds **before** nodes that produce them
  deploy. An old handoffkeep rejects the POST — on the deployed schema the
  app-level `relayEventKinds` allowlist answers `400 invalid_context` before
  the `relay_events` CHECK constraint is even reached. Any non-2xx reply takes
  the same path: the hub broadcasts `relay.unpersisted`, sends no
  acknowledgement, and the node retries within its normal outbox bounds —
  observable, never silently lost.
- **The handoffkeep migration is four places, not one.** Widening only the
  CHECK constraint is not enough; a resend must still collide with the row it
  already wrote. All four must name the new kinds:
  1. the app-level `relayEventKinds` allowlist (rejects with
     `400 invalid_context` first);
  2. the `relay_events` CHECK constraint;
  3. the `relay_events_idempotency` partial unique index
     (`WHERE kind IN (...)`);
  4. the insert's `ON CONFLICT` predicate (same `WHERE kind IN (...)`).
  If (3)/(4) stay on the old three kinds, every hub resend — reacknowledge,
  attempt bump, or post-restart replay — inserts a **new** row instead of
  colliding, and each new row is undelivered, so startup replay can inject the
  same note again. That change is a separate-repository task; it is
  documented here so the rollout cannot widen the CHECK alone.
- **No retrospective publication:** only event files inside the node outbox's
  24h window (and inside the event-identity migration cutoff) are ever
  offered. Widening the kind set does not replay history.

## Node outbox

The node keeps `relay_sent(kind, job_id, epoch, report_path, reason, sent_at,
persisted_at)` in its own SQLite database. Its primary key is the same dedupe
key handoffkeep uses for idempotency, assembled in the same field order.

- A row with `persisted_at` set is never sent again, across restarts.
- `sent_at` is stamped **after the write leaves the node**, never when the
  record is picked. A scan claims a whole batch at once and writes it one
  message at a time, so stamping at claim time marked every record behind a
  failed write as sent: they were then held by the 60-second backoff with no
  send to show for it, and a restart repeated the same half-batch. A record
  that was selected and not written carries no stamp and is offered again on
  the very next attempt. The same holds for a record the immediate-send queue
  had no room for.
- A row whose `sent_at` is under 60 seconds old is not retried yet; that
  backoff is what keeps a dead handoffkeep from being hammered every scan. It
  gates records that really did go out, and only those.
- The scan only considers event files whose mtime is within 24 hours. Override
  with `PANEWIRE_RELAY_OUTBOX_MAX_AGE` (a Go duration).
  `PANEWIRE_JOB_ACTIVE_MAX_AGE` (72h) is a different axis and is unaffected:
  it bounds the active-job heartbeat, not undelivered relay traffic.
- The first send after a process restart of a row that already had `sent_at`
  carries `"replay": true`. The hub logs it; routing never consults it.

The hub's own dedupe key counts the same five fields, in the same order. When
it counted only four (it omitted `reason` before R20T5), two events the node
and handoffkeep both treated as distinct were folded into one here, and the
second one's outbox row could never be retired.

### The key is what goes on the wire

The hub bounds every relay text field to 240 runes on receipt and rejects an
embedded newline outright, and `relay.persisted` can only echo what the hub
received. So the node normalizes **once**, before it builds either the key or
the payload, and keys the outbox by that same wire form:

- a `report_path` or `reason` past 240 runes is truncated identically on both
  sides, instead of the node keying the full value and the hub naming the
  truncated one;
- a newline anywhere in a relay text field becomes a space, instead of the hub
  refusing the whole message and the row waiting for an answer that can never
  come;
- a `job.completed` outbox row is keyed with an empty `reason`, because the
  `job.completed` payload carries no `reason` field and the hub can therefore
  only ever echo an empty one.

A row keyed by un-normalized text names something no acknowledgement will ever
match, which is `persisted_at` NULL for the life of the row.

## Two properties of the persistence contract

1. `owner_lane` is **not** part of handoffkeep's idempotency key
   (`kind, job_id, epoch, report_path, reason`). Two lanes submitting the same
   key share one row, and the later one gets back the first one's
   `owner_lane`. The hub logs one warning on a mismatch and keeps routing the
   way it already decided. Job ids are lane-unique, so this is rare.
2. A duplicate POST updates only `attempts`. It is first-writer-wins: a changed
   `report_last_line`, `question`, or `pane_id` on a resend is **not** stored,
   and the 200 response returns the original row. The hub trusts the response
   body but never assumes its own values were written.

## macOS: do not force recursive fsnotify

`InboxWatcher` supports two modes, chosen by `inboxWatchMode`:

| GOOS | `PANEWIRE_INBOX_WATCH` | mode |
|---|---|---|
| darwin | unset | `poll` |
| linux | unset | `fsnotify` |
| any | `fsnotify` | `fsnotify` |
| any | `poll` | `poll` |
| any | anything else | the platform default |

**macOS defaults to `poll` because the recursive fsnotify path caused a real
outage.** macOS fsnotify is kqueue, and kqueue does not recurse: the watcher
registers one watch — one file descriptor — per directory under the root, and
walks new directories in as they appear. On a machine with a large
`jobs/*/events` tree that reached roughly 20,000 open descriptors and wedged
the entire machine, not just panewired. Setting
`PANEWIRE_INBOX_WATCH=fsnotify` on macOS re-enables exactly that behavior; the
descriptor count grows with the number of job directories retained on disk, so
do not set it on a host with a large inbox.

Poll mode walks the root every `PANEWIRE_INBOX_POLL` (default 5s), takes a
baseline at construction, and records the same two event kinds as the fsnotify
path — `inbox.file_created` and `inbox.file_changed` — so nothing downstream
can tell the modes apart. It registers no watches at all.

## `panewire emit`

```
panewire emit --kind job.completed --job <job_id> --report <path>
              [--owner-lane <lane>] [--epoch <n>] [--label <l>] [--host <h>]
              [--pane <pane_id>] [--report-last-line <text>]
              [--reason <text>] [--question <text>] [--pr <url>] [--head <sha>]
              [--result-id <READY event id>]
              [--inbox-root <path>] [--timeout <dur, default 2s>]
```

`--result-id` (job.completed only) names the READY the completion concludes; see
[a READY and its completion](quiet-delivery.md#a-ready-and-its-completion). It
is part of the record: the same completion under another id is a duplicate
outbox key.

The inbox root comes from `--inbox-root`, else `PANEWIRE_INBOX_ROOT`, else the
per-OS default (`defaultInboxRoot` in `inbox_root.go`, which the daemon's own
fallback also uses):

| OS | default inbox root |
|---|---|
| macOS | `~/Library/Application Support/panewire/inbox` (the daemon data directory) |
| Linux and others | `~/work/herdr-inbox` |

Before this rule the macOS path was the default on every OS, so every emit on
a Linux node without `PANEWIRE_INBOX_ROOT` was refused as a mismatch. An
explicit `--inbox-root` or `PANEWIRE_INBOX_ROOT` still wins over the default.
`--kind` must be one of the relay kinds:
`job.completed`, `job.escalate`, `job.joined`, `job.lost`, `job.revoked`, or
`lane.event`. A missing `--job` is a usage error; `--report` is required for
`job.completed`, while the self-record kinds (`job.escalate`, `job.joined`,
`job.lost`, `job.revoked`) fall back to their own event file — and
`job.lost`/`job.revoked` additionally require `--reason` and `--owner-lane`.

The push carries the inbox root the file was written in, and a daemon relaying
from a different root refuses it. Redirecting `--inbox-root` alone does not
redirect the socket: without this guard a verification run against a scratch
directory still reached the operator's live daemon, whose hub client then wrote
a `relay_sent` row for it in the production journal. The event file stays
durable in the namespace the caller chose; `emit` exits with the usage code and
prints both roots, and the daemon logs the same pair as a warning:

```
emit: inbox root mismatch (daemon=<daemon's jobs root>, given=<emit's root>)
```

Make the two agree with `--inbox-root` or `PANEWIRE_INBOX_ROOT`.

The file is written first and the socket is called second. A record whose
dedupe key already exists in `jobs/<job_id>/events/` does not produce a second
file — the socket push still happens, since the writer records the event file itself
and then calls `emit`, and that path has to work. The pre-file key shares
(kind, job, epoch, empty report, reason) across events the scanner later keys
by their own file, so identity is refined per kind: `job.escalate` separates
by `question`, `job.lost` separates by the compared record fields (everything
but `created_at`/`agent_label`), while a record identical on those fields
reuses the existing file — emit cannot tell a same-pane re-loss from a retry
without a producer-supplied event id. `job.completed`, `job.joined` and
`job.revoked` keep one record per key: a second record under the same key is
refused with `emit: duplicate outbox key`. A daemon that is not running
is **not** an error: `emit` prints
`emit: panewired unavailable; event recorded to file only` to stderr and exits
0, leaving the record for the node outbox to pick up.

An emitted file carries the kind under both `"type"` and `"kind"` keys: the
scanner accepts either, but the reap reader (`document["kind"]`), `scanJobCloseState`,
and the fleet census read `"kind"` only. Emitting both is what makes
`emit --kind job.revoked` terminal for every consumer, not only the relay
path.

Example lane file (identifiers only — never real addresses, panes, or tokens):

```json
{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"parent-lane"}}}
```
