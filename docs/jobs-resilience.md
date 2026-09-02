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
sequence, optional push SHA, and epoch. A claim is `job.claimed`; a terminal
`job.completed`, `job.completion`, or `job.revoked` event removes it from the
active set. Briefs, command text, and pane output are never sent.

The hub waits `--hub-grace` (or its configured `orphan_grace`) after a
disconnected/stale presence transition before emitting one `job.orphaned`
event. It appears in the authenticated `/v1/events` stream, UI Recent events,
and (when Telegram is configured) as one metadata-only line.

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

This increments the job epoch and records `job.reassigned`. If the old node
reconnects and reports its lower epoch, the hub sends `job.revoked`; its daemon
writes `NNNNN-job.revoked.json` locally. The wrk-side hook owns the resulting
pane message:

`[REVOKED — 다른 노드에 재배정됨, 즉시 중단·push 금지]`

An old-epoch `job.completed` event is rejected by the hub. A completion from a
node that returns before reassignment turns the prior orphan record into
`job.recovered`.

## WIP push convention

Worker briefs should explicitly say: **push a `wip:` commit every 30 minutes,
and before any risky step.** This gives a redispatched worker a bounded,
inspectable resume point; it does not change the `wrk` gate or make the hub a
source of truth.
