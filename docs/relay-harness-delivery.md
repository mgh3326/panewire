# Relay delivery per harness

A lane event addressed to a worker lane (review findings, an ESC answer, a
follow-up order) is typed into the worker's pane by the node relay. This page
is the contract for what the relay does per harness and when it may say the
text arrived. Hub-to-node transport is in [R20](r20-relay-persistence.md), the
busy hold in [R27](r27-busy-relay.md), the lane routes in [R19](r19-lanes.md).

## Adapter table

One entry per harness lives in `relayHarnessAdapters`
(`relay_harness_adapter.go`). A harness with no entry, or a name herdr did not
report, has no adapter and no delivery proof.

| Harness | Mid-turn | Proof (`relay.delivered` reason) | Late echo | Send attempts |
|---|---|---|---|---|
| claude | no, held until idle | nonce echo above the composer, or queue banner plus nonce (`nonce_echo`, `queued_banner`) | observed 8 s | 3 |
| codex | yes, when `working` (not `blocked`) | same as claude; the mid-turn case is `queued_banner` ("Messages to be submitted after next tool call") | observed 8 s | 3 |
| devin | no, held until idle | nonce in the transcript, or devin's own queue banner (`nonce_echo`, `devin_queue_banner`) | no | 2 |
| grok | no, held until idle | nonce on screen and herdr reports idle/done before the send and `working` after it (`nonce_echo+status_working`) | observed 8 s | 3 |
| omp | no, held until idle | same as grok | observed 8 s | 3 |
| anything else | n/a | none | n/a | n/a |

The nonce is the per-row `[r<id><check>]` token at the head of every typed text
(#687). It identifies this row, so a replay of another row never counts.

Grok and omp have no captured screens to read a composer or queue banner
from, so their proof needs neither: a paste that was never submitted shows the
nonce too, but it does not start a turn. If a real capture shows a transcript
echo that tells the two apart, replace the rule with a transcript one and
allow mid-turn there.

## What "delivered" means

`relay.delivered` is emitted, and the durable row closed, only on the proof in
the table. Everything else is `relay.unconfirmed`:

- a send herdr rejected is retried up to the budget above, then dropped as
  `inject_failed_max_attempts`;
- a send herdr accepted whose proof never shows is `maybe_in_pane`: it is never
  re-typed (it may already be in the pane) and is dropped at once;
- a harness with no adapter is `maybe_in_pane` with `unproven:no_adapter`.

Must-ack semantics are unchanged. An injected `findings`, `decision`, `ready`
or `blocked` item stays unread until the worker runs `panewire inbox ack <id>`.

## Escalation

When a node drops an event as `inject_failed_max_attempts` or
`maybe_in_pane`, the hub tells the job's design lane once. The design lane is
the parent of the worker lane's route (the lane `job.escalate` already reaches),
else the owner lane of the live job that runs in that lane. The escalation is
an `automation-failed` lane event with producer id
`delivery-unconfirmed-<durable event id>`; its text carries the durable event
id, the producer's event id, the worker lane, the node and pane, and the
reason. The id is fixed by the row, so a second drop of the same row (a replay
that also fails) is answered by the durable idempotency and sends nothing. An
event that is itself such an escalation never escalates again, and a lane with
no design lane is only logged.

The hub's own one-labelled-replay after a restart
([#1002](r27-busy-relay.md)) is unchanged and is not an escalation.

## Tests

`relay_harness_delivery_t3276_test.go` drives a real hub over a fake
handoffkeep, the real node relay and a shell `herdr` that models one pane, for
codex (idle and working), devin, grok and omp.
