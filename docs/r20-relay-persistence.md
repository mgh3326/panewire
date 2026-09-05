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

## `--handoffkeep-env`

`panewire hub --handoffkeep-env <path>` enables persistence. The file must be a
regular mode-0600 file holding exactly two keys:

```
HANDOFFKEEP_URL=…
HANDOFFKEEP_TOKEN=…
```

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

**handoffkeep exposes no endpoint that sets `attempts`.** The hub therefore
re-POSTs the row it already stored: the idempotency key collides, handoffkeep
answers 200 with `attempts + 1`, the row itself is unchanged (first-writer-wins,
below), and `delivered_at` is not touched by that path. The contract is
unchanged; this is the counter it already had.

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
              [--inbox-root <path>] [--timeout <dur, default 2s>]
```

The inbox root comes from `--inbox-root`, else `PANEWIRE_INBOX_ROOT`, else the
daemon's default data directory. `--kind` must be `job.completed`,
`job.escalate`, or `job.joined`; a missing `--job` or `--report` is a usage
error.

The push carries the inbox root the file was written in, and a daemon relaying
from a different root refuses it. Redirecting `--inbox-root` alone does not
redirect the socket: without this guard a verification run against a scratch
directory still reached the operator's live daemon, whose hub client then wrote
a `relay_sent` row for it in the production journal. The refusal is not an
error for the caller — the event file is durable in the namespace it chose, and
`emit` reports `panewired unavailable; event recorded to file only`.

The file is written first and the socket is called second. A record whose
dedupe key already exists in `jobs/<job_id>/events/` does not produce a second
file, but the socket push still happens — `wrk` writes the event file itself
and then calls `emit`, and that path has to work. A daemon that is not running
is **not** an error: `emit` prints
`emit: panewired unavailable; event recorded to file only` to stderr and exits
0, leaving the record for the node outbox to pick up.

Example lane file (identifiers only — never real addresses, panes, or tokens):

```json
{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"captain"}}}
```
