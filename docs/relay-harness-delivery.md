# Relay delivery per harness

A lane event addressed to a worker lane (review findings, an ESC answer, a
follow-up order) is typed into the worker's pane by the node relay. This page
is the contract for what the relay does per harness and when it may say the
text arrived. Hub-to-node transport is in [R20](r20-relay-persistence.md), the
busy hold in [R27](r27-busy-relay.md), the lane routes in [R19](r19-lanes.md).

## Immediate delivery

A worker directive is typed into the pane at once on every harness. It never
waits for a turn boundary, and a `working` pane is not a reason to refuse or
defer it. A `blocked` pane is the exception: it is waiting on a prompt (a codex
approval) that the text could answer, so the R27 hold still applies and the
directive waits until the pane is released. A worker directive is a `lane.event` of type `findings`, `decision`
(an ESC answer) or `order` sent to a lane whose route names a parent, the
design lane it reports to. The hub gives it `deliver: directive` (typed at once, held only while `blocked`)
unless the event or the route already names a policy. An explicit `deliver: now`
still skips the hold entirely. A lane with no parent (the desk) and every
other type keep their delivery policy, and so still wait for an idle pane
([R27](r27-busy-relay.md)).

A Claude pane queues input typed mid-turn for its next turn, and codex takes
it directly. Both show it on screen, which is the proof.

`panewire prompt --uptake ...` refuses a working target (exit 6), because an
idle-to-working transition cannot be seen on a pane that is already working.
`--directive` marks the message as a worker directive: the working target is
not refused, no transition is required, and the prompt is judged on its
submission evidence alone (delivery record `uptake_result`
`submission_only_working`). Without the flag nothing changes.

## Adapter table

One entry per harness lives in `relayHarnessAdapters`
(`relay_harness_adapter.go`). A harness with no entry, or a name herdr did not
report, has no adapter and no delivery proof.

| Harness | Proof (`relay.delivered` reason) | Late echo |
|---|---|---|
| claude | nonce echo above the composer, or the queue banner plus nonce when the pane was working (`nonce_echo`, `queued_banner`) | observed 8 s |
| codex | same as claude; the working case is `queued_banner` ("Messages to be submitted after next tool call") | observed 8 s |
| devin | nonce in the transcript, or devin's own queue banner (`nonce_echo`, `devin_queue_banner`) | no |
| grok | nonce on screen and herdr reports idle/done before the send and `working` after it (`nonce_echo+status_working`) | observed 8 s |
| omp | same as grok | observed 8 s |
| anything else | none | n/a |

The nonce is the per-row `[r<id><check>]` token at the head of every typed text
(#687). It identifies this row, so a replay of another row never counts.

Grok and omp have no captured screens to read a composer or queue banner
from, so their proof needs neither: a paste that was never submitted shows the
nonce too, but it does not start a turn. That rule needs an idle or done pane
before the send. A directive typed into a *working* grok or omp pane is
therefore injected but stays unconfirmed (one retry, then the escalation below)
until a live capture gives a transcript rule that tells a queued message from a
pending paste.

## What "delivered" means

`relay.delivered` is emitted, and the durable row closed, only on the proof in
the table. Everything else is `relay.unconfirmed`:

- a send herdr rejected, or a send herdr accepted whose proof never shows
  (`unproven:...`, including a harness with no adapter and an unreadable pane),
  is typed once more at once (a directive) and then dropped
  as `inject_failed_max_attempts` or `maybe_in_pane`. The text already tells the
  reader that seeing it twice is not a reason to re-run it;
- any other `maybe_in_pane` verdict (a foreign draft in the composer, a changed
  harness, an unreadable pane) is final, because the text may already be in the
  pane;
- a harness with no adapter is `maybe_in_pane` with `unproven:no_adapter`.

Messages that are not worker directives keep their earlier budgets: a rejected
send is re-held until the pane is idle, up to 3 sends (devin 2), and an unproven
send is never re-typed.

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
