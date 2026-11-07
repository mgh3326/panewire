# Relay delivery per harness

A lane event addressed to a worker lane (review findings, an ESC answer, a
follow-up order) is typed into the worker's pane by the node relay. This page
is the contract for what the relay does per harness and when it may say the
text arrived. Hub-to-node transport is in [R20](r20-relay-persistence.md), the
busy hold in [R27](r27-busy-relay.md), the lane routes in [R19](r19-lanes.md).

## Immediate delivery

A worker directive is typed into the pane at once on every harness. It never
waits for a turn boundary, and a `working` pane is not a reason to refuse or
defer it, except for a harness whose adapter sets `workingUnprovable` (today
only cmd, see the adapter table). A `blocked` pane is an exception too: it is waiting on a prompt (a codex
approval) that the text could answer, so the R27 hold still applies and the
directive waits until the pane is released, which is when it turns idle, done
or working again (the held directive's wait also ends on `working`, and only the
directive is typed then, except on a `workingUnprovable` harness, whose wait
ends on idle or done only; a row on the idle policy keeps waiting). A directive
is also never filed as inbox or kept for a digest by a lane policy, and it never
shares a send with an ordinary held row. A worker directive is a `lane.event` of type `findings`, `decision`
(an ESC answer) or `order` sent to a lane whose route names a parent, the
design lane it reports to. The hub gives it `deliver: directive` (typed at once, held while `blocked`, and for a `workingUnprovable` harness while `working`)
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
| cmd | same as grok, typed through `pane send-text` plus `pane send-keys return`; the composer is found with claude's rule (below); `workingUnprovable`: a directive waits for idle or done; nothing is typed into it while working, and a held row waits for idle while the job is live; one row per inject | observed 8 s |
| anything else | none | n/a |

`workingUnprovable` marks a harness that cannot prove a directive typed into a
working pane. cmd sets it: its proof needs the composer of an idle or done pane,
so a send into a working one stays unproven and the retry-once rule would type
the directive a second time (hk 3370). For such a harness the directive policy
holds while the pane is `working` as well as `blocked`, through the same R27
hold, and types when the pane turns idle or done. The wait ends on idle or done
only. A pane that is still working is never typed into, whatever the row's
policy (`directive`, `idle` or `max_wait=N`; hk 3384), and while the job is live
the row has no working-wait expiry (hk 3411): when `max_wait` runs out the row
stays held, its deadline moves 30 minutes on, and the same wait is armed again;
nothing new polls. The row is typed when the pane turns idle or done, one row
per inject. It is dropped only when:

- the pane is gone or holds another occupant, or the lane moved (the lease
  gate: `pane_occupant_gone`, `pane_occupant_changed`, `lease_unverifiable`,
  `lane_rerouted`);
- the node's own record of the lane's job (`jobs/<lane>/events`) shows the job
  ended (`job_ended`), checked at every release, so a row is not typed into
  the pane of an ended job even once it turns idle; a lane with no job record
  keeps its hold;
- the `relay.cmd_hold_max` ceiling passes, counted from when the row was held
  (default 24h; set it on the node with `PANEWIRE_RELAY_CMD_HOLD_MAX`, a Go
  duration; a ceiling below the policy wait also caps the first wait). Then the row is dropped as `working_wait_expired`, the design lane
  is told once (see below), and the durable row stays undelivered.

A row dropped as `working_wait_expired` was never typed. The hub remembers it
when its unconfirmed mark lands and re-delivers it when the idle-wake observer
next reports the pane idle: the oldest such row per idle wake, so one row per
inject, as the row's one labelled replay, so at most once. A wake that spends
nothing (a failed read or retire) keeps the row for the next wake, up to three
wakes. Its age is counted from the drop, not the receipt, since the node held it
up to `relay.cmd_hold_max`: a row dropped more than the replay window (24h) ago
is left to the restart replay. Its text starts with
`[held while this pane was working - first sent <KST time> - delivered now that
it is idle]`. The memory is not durable; after a hub restart the startup replay
gives every marked row its one labelled replay instead (#1002). Every other
harness is typed at once as before.

A pane-transport harness (cmd) is never sent a batch (hk 3384). Its return gate
reads the composer after the text lands, and a batch wrapped over several
composer rows did not match, so the return was withheld and the batch sat
unsent in the composer. A release types one held row per inject instead, each
with its own nonce, presend check, gated return and proof. A submitted row
turns the pane working, so before each later row the pane is read again; unless
it is idle or done, the rest stay held and the next idle wake types the next
one. Other harnesses keep the batch.

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
herdr returns from `pane send-text` before cmd has drawn the text, so the first
read can still show the composer as it was before the send, or only the head of
a long text. While the composer holds a strict head of this text (an empty
composer included, where a return would submit nothing), or exactly what it
held before the send, the gate re-reads it every 100 ms for up to 2 s
(hk 3419). These reads type nothing and press nothing. A gate that re-read logs
one line (`relay cmd return gate settled`) with its rule, the number of
re-reads and the time they took. A composer that holds anything else, such as a
draft or another nonce, is withheld at once. The composer is read across every
row between the dividers, without whitespace, so a text wrapped over several
rows matches itself.
That is the one return of the inject: verify does not press a second one when
the composer still draws the text (echo lag), it observes until the echo lands.
If it still draws the text when the observation window ends, the verdict is
`return_unconfirmed:composer_pending`, final and not retried: a retry's presend
would meet this row's own text and press a second return.
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

When a node drops an event as `inject_failed_max_attempts`,
`maybe_in_pane` or `working_wait_expired`, the hub tells the job's design lane once. A handoffkeep outage
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

hk 3431: the escalation goes out only when someone can act on it, and at most
once per window. The hub logs one INFO line (event id, lane, reason, `why`)
instead of sending when every hub job in the worker lane is completed or
orphaned (`why=job_ended`), or when the hk task of every live job there is in
an ended state (`why=task_ended`). A task is ended when hk's category is
`completed` or `canceled`, or, if the record has no category, when its state
is `merged`, `deployed`, `done`, `dropped` or `canceled`. The hub reads the job
record and the task through the client that hub-placed spawns use. The
escalation still goes out when the record or task cannot be read, when a live
job has no task, or when the hub knows no job in the lane. The last case covers
a hub restart, which forgets ended jobs. For a live job, the hub sends at most
one escalation per (design lane, worker lane) every 30 minutes, and sends the
first one at once. The window starts only when handoffkeep stores an
escalation, as a new row or as a retry that answers duplicate; nothing holds
the window before that. A later drop inside the window is logged
(`why=folded`) and counted. The next escalation after the window ends with
`(+N more unconfirmed for this lane since HH:MM UTC)`, where `HH:MM` is the
earliest counted drop; storing it clears the drops it names, and drops counted
while it was in flight stay counted. An escalation handoffkeep never stores
(every retry gives up, handoffkeep refuses it, or a retry stops early because
the row was delivered or its job ended) records nothing, so the next drop is
sent at once; a refused or given-up drop is counted. Two drops at the same
moment can both be sent: one extra escalation is accepted, a lost one is not.
A window tracks at most 64 counted rows and only counts the rest. A counted
row dropped again after the window is sent like any other drop and does not
count itself. The window is kept in memory only, so a hub restart resets it
and the first drop after a restart is sent at once.

The hub's own one-labelled-replay after a restart
([#1002](r27-busy-relay.md)) is unchanged and is not an escalation.

## Tests

`relay_harness_delivery_t3276_test.go` drives a real hub over a fake
handoffkeep, the real node relay and a shell `herdr` that models one pane, for
codex (idle and working), devin, grok and omp. `relay_cmd_transport_t3330_test.go`
does the same for cmd, with a shell `herdr` that refuses the agent-level writes
the way herdr does; `relay_cmd_composer_t3330_test.go` pins the send-text
failure, the failed-return replay, the draft, dim-suggestion, draft-between and
echo-lag cases; `relay_cmd_working_hold_t3370_test.go` pins the working hold,
its expiry and the unchanged claude pane; `relay_cmd_release_t3384_test.go` pins
the expiry of every policy, one row per inject and the unchanged claude batch;
`relay_cmd_hold_until_idle_t3411_test.go` pins the hold past `max_wait`, the
drops for a gone pane, an ended job and the ceiling, the unchanged claude pane,
and the idle re-delivery of a `working_wait_expired` row; `prompt_cmd_t3330_test.go` covers the direct prompt's pane
transport; `relay_cmd_wrapped_return_t3419_test.go` pins the return gate on a
wrapped composer, a draft or second nonce beside it, and a late-drawn text.
`relay_unconfirmed_quiet_t3431_test.go` pins the quiet escalation for an ended
job or task, the fail-open reads, the 30-minute window, the escalations that
are never stored, concurrent drops and the bound on counted rows.
