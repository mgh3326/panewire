# R23 console surface

R23 adds observation and delivery surfaces for the operator console. It does
not change placement decisions: CPU projection is display-only and placement
continues to use its established Prometheus `load_ratio` source.

## A. CPU load in heartbeat and nodes

`host_load` retains the required numeric `load1`, `load5`, `swap_used_gb`, and
`worker_procs` fields. It may now carry `load15` (`number | null`) and `ncpu`
(`integer | null`). Both optional measurements are always serialized when a
new node emits `host_load`; `null` means the single local measurement failed or
was unavailable, never zero. Non-null `load15` is finite and at least zero;
non-null `ncpu` is at least one. A host_load object has exactly the legacy four
keys or exactly all six keys.

Darwin obtains the third `vm.loadavg` value and `sysctl -n hw.ncpu`; Linux
obtains the third `/proc/loadavg` value and `nproc`. Failure of either new
measurement leaves only that field null. Failure of a legacy host_load
measurement still omits host_load entirely.

`GET /v1/nodes` always exposes `load`. It is `null` until that node has sent a
host_load. Otherwise it has `load1`, `load5`, `load15`, and `ncpu`, each as a
number/integer or null. It deliberately excludes `swap_used_gb` and
`worker_procs`.

## B. Active job registry

`GET /v1/jobs` requires the operator token and returns
`{"jobs":[...]}`; an empty result is `[]`, never null. Results sort by
`job_id`, and `?machine=<id>` filters by a valid machine identifier (an invalid
identifier is HTTP 400). Each result has `machine`, `job_id`, and when known
`owner_lane`, `pane`, `tier`, `role`, `started_at`, `last_event_kind`, and
`last_event_at`. Unknown metadata is omitted rather than invented.

Active means a hub-registered job that is not completed. `/v1/jobs/orphaned`
uses that same active set and then selects orphaned records, so an orphan is
visible in both views and a completed record is in neither.

Heartbeat `active_jobs` retains required `job_id`, `agent_label`,
`last_event_seq`, and `epoch` (with optional `push_sha`). It additionally
accepts optional `owner_lane`, `pane`, `tier`, `role`, `started_at`,
`last_event_kind`, and `last_event_at`. Bad optional values are discarded one
field at a time: lane labels use the hub label grammar; pane is nonempty and at
most 128 bytes; tier is T0 through T3; role is worker or captain; timestamps
are RFC3339; and event kind is lowercase `^[a-z][a-z0-9_.]{0,31}$`.

Node scanning takes owner lane, role, tier, and claim time from a claim; pane
from `job.spawned`; and kind/time from the final event. Pane is a runtime jump
identifier for the console only: briefs and transcripts remain absent.

## C. Sink lanes

In `lanes.json`, `sink: true` makes a lane a durable operator sink. An empty
`pane` is the compatible shorthand for the same mode. Sink wins even if a
machine or pane is supplied: it is never a pane injection target. A sink still
requires a valid lane name and validates a nonempty parent by the existing
rules.

```json
{"lanes":{"lane-web":{"sink":true},"lane-a":{"machine":"host-a","pane":"pane-a"}}}
```

For a sink `lane.event`, the hub POSTs one durable row, immediately marks that
row delivered with `machine="sink"` and `pane="sink:<lane>"`, and returns the
ordinary `relay.persisted` acknowledgement to the producer. It queues no
`relay.inject`, has no acknowledgement timeout or retry attempt, and a later
node registration cannot replay the delivered row. If delivery marking fails,
the row remains undelivered and observable; the hub never falls back to pane
injection. Valid inbound events still use the normal `/v1/events` broadcast
path.

## E. Lane-event text bounds

`laneEventTextLimit` is 2048 bytes and `laneEventTextLimitSink` is 8192 bytes.
The daemon, scanner, and heartbeat decoder can carry valid text through 8192
bytes. `panewire emit --kind lane.event` truncates at 2048 by default; adding
`--sink` selects 8192. In both cases it truncates once at a UTF-8 boundary,
adds `[truncated]`, and emits `relay.truncated` when the hub receives the
flagged record.

Nodes do not read operator-owned `lanes.json`, so they cannot safely determine
the destination route's authority. The producer flag therefore selects only
its local truncation bound. The hub reads the route and rejects a non-sink
event over 2048 before persistence with reason `text_too_long`; only a sink
route can persist up to 8192. This preserves one node-side truncation point
while making the route file the final authority.

## Deployment order and compatibility

Deploy the hub before nodes. An older hub rejects a heartbeat whose host_load
has the six-key shape or whose active_jobs entries contain the new optional
keys: its decoder returns false and drops that whole heartbeat. The new hub
continues to accept older nodes' four-key host_load and legacy active_jobs;
their CPU measurements are null and their new job metadata is absent. After
the hub is updated, nodes may roll out normally.
