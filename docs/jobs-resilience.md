# Intermittent-node job resilience

This is an operator-assist path for jobs whose authoritative state remains the
local `jobs/<id>/events/` files and the corresponding pane injection. The hub
does not dispatch work, carry a brief, or bypass `wrk`.

## Node heartbeat

Run the node daemon with its local inbox root supplied explicitly:

```sh
panewire daemon --hub-url wss://hub.example --hub-token-env ~/.config/panewire/hub.env \
  --hub-jobs-root "$HOME/work/herdr-inbox"
```

Every normal hub heartbeat scans `jobs/*/events/` and sends at most 32 active
claim records. Each record contains only job ID, claim agent label, event
sequence, optional push SHA, and epoch. A claim is `job.claimed` (the
legacy-compatible `job.claim` is also read); a terminal `job.completed`,
`job.completion`, or `job.revoked` event removes it from the active set.
`job.completed`/`job.completion` also produce one fenced `job.completed` hub
event with the local epoch. Briefs, command text, and pane output are never
sent.

The hub waits `--hub-grace` after a disconnected/stale presence transition
before emitting one `job.orphaned` event. It appears in the authenticated
`/v1/events` stream, UI Recent events, and (when Telegram is configured) as
one metadata-only line.

## Operator flow

First inspect candidates using an operator token:

```sh
panewire jobs orphaned --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
```

The dispatcher remains the separate orch/wrk worker. After it has selected a
safe receiving node and submits through that node's ordinary `wrk` admission
gate, record the decision at the hub:

```sh
panewire jobs reassign --job-id example-42 --to node-b \
  --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
```

This increments a hub-issued job epoch and records `job.reassigned`. The hub
retains every prior owner in its in-memory fence history; a heartbeat cannot
raise its own epoch or retake ownership. While a fenced predecessor is online,
or after it reconnects and heartbeats, the hub retries `job.revoked` until that
node confirms its local marker write. Its daemon then writes
`NNNNN-job.revoked.json` locally. The wrk-side hook owns the resulting pane
message:

`[REVOKED — 다른 노드에 재배정됨, 즉시 중단·push 금지]`

An old-epoch `job.completed` event is rejected by the hub. A completion from a
node that returns before reassignment turns the prior orphan record into
`job.recovered`.

When an orphaned owner returns while its local claim is still active, the hub
clears the orphan marker and emits `job.recovered`; it no longer appears in the
reassignable list. The receiving node receives the hub-issued epoch as
`job.assigned` immediately when connected and again with heartbeat directives,
then uses that epoch in its active-job heartbeat and completion event.

The hub job/fence view and retry queue are intentionally in memory. A hub
restart loses orphan, epoch, reassignment, and pending-revocation state; local
job event files remain the system of record, but the hub does not yet
reconstruct fences from them. Avoid restarting the hub during an active
redispatch until a later durable reconstruction design is deployed.

## WIP push convention

Worker briefs should explicitly say: **push a `wip:` commit every 30 minutes,
and before any risky step.** This gives a redispatched worker a bounded,
inspectable resume point; it does not change the `wrk` gate or make the hub a
source of truth.
