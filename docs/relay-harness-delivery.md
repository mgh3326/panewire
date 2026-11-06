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
directive waits until the pane is released, which is when it turns idle, done
or working again (the held directive's wait also ends on `working`, and only the
directive is typed then; a row on the idle policy keeps waiting). A directive
is also never filed as inbox or kept for a digest by a lane policy, and it never
shares a send with an ordinary held row. A worker directive is a `lane.event` of type `findings`, `decision`
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
| cmd | same as grok, typed through `pane send-text` plus `pane send-keys return`; the composer is found with claude's rule (below) | observed 8 s |
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

cmd uses the same proof, and its transport is different: herdr answers
`agent get` and `agent read` for a cmd pane but refuses `agent prompt` and
`agent send-keys` with `agent_not_ready` ("not an active named agent"), so the
relay types with `pane send-text` and submits with `pane send-keys return` —
the pair the spawn path already uses for a cmd brief. The text is not
submitted by `pane send-text` alone; a failed return leaves it typed and
unsubmitted, which is `return_failed` and `maybe_in_pane`, never a re-inject.
A failed `pane send-text` is `send_text_failed` and `maybe_in_pane` too: herdr
may have typed part of the text, so it is never typed again.

cmd draws claude's two-divider composer with the same `❯` prompt, so the relay
reads it with claude's rule. Before typing, the composer must be located and
empty; a draft in it is `presend:composer_pending` and an unlocated composer is
`presend:composer_unlocated`, both final, and nothing is typed. A composer that
already holds this row's own text (a replay after a failed return) is
submitted with the one return and not typed again, so exactly one copy lands.

The presend reads cmd's composer from the ANSI screen (`herdr pane read
--format ansi`), because an idle cmd pane draws claude's dim prompt suggestion
in its empty composer and a plain read cannot tell it from a draft. It applies
the reap's dim rule (`nodeComposerState`): a composer whose every visible
character is dim reads empty and the event is typed. The return after
`pane send-text` goes through the same gate as every other harness
(`relayComposerReturnSafe`, on a read taken after the text landed); a draft that
appears in between is `return_withheld:composer_foreign` and `maybe_in_pane`.
That is the one return of the inject: verify does not press a second one when
the composer still draws the text (echo lag), it observes until the echo lands.
`panewire prompt --to <cmd pane>` takes the same transport, so a hand relay
through panewire reaches a cmd worker too.

## What "delivered" means

`relay.delivered` is emitted, and the durable row closed, only on the proof in
the table. Everything else is `relay.unconfirmed`:

- a send herdr rejected (`agent prompt`), or a send herdr accepted whose proof never shows
  (`unproven:...`, including a harness with no adapter, and an unreadable pane
  before or after the send, devin's included),
  is typed once more at once (a directive) and then dropped
  as `inject_failed_max_attempts` or `maybe_in_pane`. The text already tells the
  reader that seeing it twice is not a reason to re-run it;
- any other `maybe_in_pane` verdict (a foreign draft in the composer, a changed
  harness, an unreadable pane, a cmd `pane send-text` failure) is final,
  because the text may already be in the pane;
- a harness with no adapter is `maybe_in_pane` with `unproven:no_adapter`.

Messages that are not worker directives keep their earlier budgets: a rejected
send is re-held until the pane is idle, up to 3 sends (devin 2), and an unproven
send is never re-typed.

Must-ack semantics are unchanged. An injected `findings`, `decision`, `ready`
or `blocked` item stays unread until the worker runs `panewire inbox ack <id>`.

## Escalation

When a node drops an event as `inject_failed_max_attempts` or
`maybe_in_pane`, the hub tells the job's design lane once. A handoffkeep outage
while the hub reads the row or writes the escalation is retried (2 s, 10 s,
1 min, 5 min) until the escalation is durable. The design lane is
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
codex (idle and working), devin, grok and omp. `relay_cmd_transport_t3330_test.go`
does the same for cmd, with a shell `herdr` that refuses the agent-level writes
the way herdr does; `relay_cmd_composer_t3330_test.go` pins the send-text
failure, the failed-return replay, the draft, dim-suggestion, draft-between and
echo-lag cases; `prompt_cmd_t3330_test.go` covers the direct prompt's pane
transport.
