# R18 report relay compatibility

R18's report relay is superseded by the R19 lane contract in
[r19-lanes.md](r19-lanes.md). Existing `--report-relay-routes` deployments and
files using the `routes` JSON key remain readable, but new deployments should
use `--lanes /etc/panewire/lanes.json` and the `lanes` key.

## Relay is independent of hub job registration

R14 epoch fencing answers whether a job may be redispatched. R18 relay answers
whether a report reaches its lane. They are separate questions, so the hub does
not require `h.jobs` to hold the job before it relays a terminal record.

A `job.completed` message is relayed whenever it comes from an authenticated
node, decodes cleanly, and names an `owner_lane` present in the lanes file.
`job.escalate` and `job.joined` have always worked this way. A short job, a job
outside the node-side 32-entry active-scan window, or one completing right after
a node restart can finish before any heartbeat advertises it; gating relay on
registration silently dropped those reports.

When a completion arrives for a job the hub has no fenced record of, the hub:

- relays it,
- increments the `unfenced_completions` counter (not `unknown_messages`),
- logs one INFO line: `completion relayed without job registration job=<id> node=<m>`,
- late-registers the job with `Completed` set when it carries the claim's
  `agent_label` at epoch 1, so operators can still see it. A late-registered
  record is a receipt: it is never orphan-swept and never redispatched.

The node carries `agent_label` from the job's claim record onto its terminal
records for this purpose. Completed jobs are deliberately absent from the active
scan, so they are not forced back into the heartbeat instead.

Duplicate injection is prevented by the hub-side relay dedupe keyed on
kind, job id, epoch and report path, which is what makes a node restart's
re-sent terminal records safe.

## Restart retransmission policy

The node keeps the authoritative completion and escalation files in its inbox,
but records each successfully written relay key (`kind`, job id, epoch, report
path, and reason) in the daemon `--db` SQLite database. `relay_sent` records
the send time and whether the hub later returned `relay.delivered` or
`relay.unconfirmed`. On restart, acknowledged keys are never sent again;
unacknowledged writes are retried once on the next hub connection in case the
hub lost the first write. Records are garbage-collected after the same
`PANEWIRE_JOB_ACTIVE_MAX_AGE` window used for terminal event scanning (72 hours
by default).

The first scan after a node starts marks an event absent from `relay_sent` as a
replay when its file mtime predates node startup. It sends that mtime as
`event_time`; no change to the `wrk done` flat-record contract is required. A
hub that has just started suppresses a marked replay whose `event_time` is at
least `RELAY_REPLAY_GRACE` old (10 minutes by default), broadcasts
`relay.replayed` to the operator feed, and does not inject it into a pane.
Files created after node startup are normal relays, not replays.
