# Relay delivery per harness

A lane event addressed to a worker lane (review findings, an ESC answer, a
follow-up order) is typed into the worker's pane by the node relay. This page
is the contract for what the relay does per harness and when it may say the
text arrived. Hub-to-node transport is in [R20](r20-relay-persistence.md), the
busy hold in [R27](r27-busy-relay.md), the lane routes in [R19](r19-lanes.md).

An answer to a worker's ESC is a lane event to the job's lane, sent from any
host:

```sh
panewire emit --kind lane.event --lane <job> --type order --text "…"
panewire inbox status --event-id <id>
```

`panewire prompt` is not that path: it types through this host's one herdr
session and never reaches the hub, so a target on another host or herdr
session — a worker in m1's `fleet` session, say — is refused with the emit
command named (hk 4810).

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
it directly. Both show it on screen, which is the proof. When a Claude composer
still shows the text immediately after the gated return, the relay observes the
screen for the existing eight-second window without typing or pressing return
again. A nonce in the queue or transcript proves delivery.

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
| omp | nonce echo above its one-row composer, which must be empty after the send (`nonce_echo`); `workingUnprovable`: a directive waits for idle or done. Captures in `testdata/relay_omp` (hk 5413) | observed 8 s |
| cmd | same as grok, typed through `pane send-text` plus `pane send-keys return`; the composer is found with claude's rule (below); `workingUnprovable`: a directive waits for idle or done; nothing is typed into it while working, and a held row waits for idle while the job is live, unless its pane is proven stuck at working (below); one row per inject | observed 8 s |
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
stays held, its deadline moves 5 minutes on, and the same wait is armed again;
nothing new polls. The row is typed when the pane turns idle or done, one row
per inject, or earlier when its pane is proven stuck at `working` (below). The
row is dropped only when:

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
  hears of it (see "Escalation"), and the durable row stays undelivered.

A cmd pane stuck at `working` (hk 3517): cmd reports its own lifecycle to herdr
and sometimes never sends the report that ends a turn, so herdr keeps `working` for hours after the final answer
while the pane sits idle. At each re-check of a held row whose cmd pane is
`working`, the node reads the visible screen with SGR
(`herdr pane read --source visible --format ansi`) and remembers a digest of it
per pane and occupant, in memory only; a held row on a working cmd pane is
re-checked every 5 minutes (`relayCmdHoldRecheck`), so a stuck pane is found
about `relay.cmd_stale_after` plus one re-check after its screen froze. The pane
is **stale-working** only when all of these hold:

- herdr still says `working` and the occupant (the lease) is unchanged;
- the visible ANSI screen is byte-identical to the one remembered at an earlier
  re-check at least `relay.cmd_stale_after` ago (default 10 minutes; set it on
  the node with `PANEWIRE_RELAY_CMD_STALE_AFTER`, a Go duration);
- the composer reads empty (`composer_empty`, the same rule the presend check
  uses).

A real working turn redraws (spinner, timers), so a screen frozen for the whole
window is the proof; there is no second heuristic. A failed or unparsable read
is not evidence and the row keeps its hold. A pane that is not working, or
another occupant, starts the window over, as does any change of the screen.

A stale-working pane takes the idle path: the row is typed as one row per
inject, with the same presend check, gated return and nonce, and the rows behind
it stay held for the next re-check. herdr never shows the idle-to-working
transition the cmd proof needs, so for such a send `marker_observed` is the
nonce echo on screen and a visible ANSI screen that differs from the stale one
after the return. Without both it is `maybe_in_pane`, and the row is not typed a
second time (a directive's retry-once does not apply). The delivery evidence
(`relay.delivered` reason, or the `maybe_in_pane` reason) starts with
`stale_working:`, and the node logs one `relay cmd pane stale working` warning
per pane per stale episode with the pane, lane, event id and seconds frozen.
The proof only reads herdr; nothing is written back to it. The job-ended and
lease gates apply as before, and a stale pane's row is typed instead of dropped
at the `relay.cmd_hold_max` ceiling.

A row dropped as `working_wait_expired` was never typed. It gets no budget
retry. When the idle-wake observer next reports the pane idle, the hub asks for
the oldest such `lane.event` row of the lanes routed to that pane: one row per
idle wake, so one row per inject. A retried or older wake asks for nothing. The
request is stored on the row's delivery obligation (`revive_requested`), and the
next maintenance pass queues the copy, so a hub restart between the wake and the
copy loses neither. Each obligation gets this revived copy once (`revived`),
whatever its retry budget has spent, and never when its fault says the text may
be in the pane. A copy the hub could not queue (full queue, unrecorded attempt,
replaced connection) gives the revive back for a later pass. While a request is
untaken, no other row of the pane is asked for during five minutes. The row's
age is counted from the drop, not the receipt, since the node held it up to
`relay.cmd_hold_max`: a row dropped more than the replay window (24h) ago is not
revived, and until then the startup and node-registration walks leave it open.
Its text starts with `[held while this pane was working - first sent <KST time> -
delivered now that it is idle]`. The per-fault table is in
[R20, "Faults and copies"](r20-relay-persistence.md#faults-and-copies).

A hub without an inbox store (`--delivery-policy off`, or no store) keeps no
obligations. It remembers the row in memory when its unconfirmed mark lands and
re-delivers it at the next idle wake through the marked row's one labelled
replay. A wake that spends nothing (a failed read or retire) keeps the row for
the next wake, up to three wakes. That memory is not durable; after a restart
the startup replay gives every marked row its one labelled replay instead
(#1002). Every other harness is typed at once as before.

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

Grok has no captured screen to read a composer or queue banner from, so its
proof needs neither: a paste that was never submitted shows the nonce too, but
it does not start a turn. That rule needs an idle or done pane before the send.
A directive typed into a *working* grok pane is therefore injected but stays
unconfirmed until a live capture gives a transcript rule that tells a queued
message from a pending paste. A failed pre-send read can be retried safely
because no text has been sent.

omp has a captured composer (hk 5413, `testdata/relay_omp/README.md`): the row
that opens with `╰─` and holds the typed text after it, plus the rows below it
when the text wraps. Those rows are indented by three spaces, and blank rows
between them are allowed; any other row below the glyph row leaves the composer
unlocated. The right-aligned placeholder (`⇧⇥ to change thinking effort`) is
read as an empty composer only when it is the whole of the glyph row with nothing
below; a draft that contains that text is composer text. The glyph row must be inside the
visible read, so a composer taller than that read is unlocated too and presend
refuses it (`presend:composer_unlocated`), rather than typing into a pane it
cannot see. Its rules: a pending paste in that row is composer residue, and
the relay presses the one return only when the row holds nothing but this
inject's text; a submitted prompt is echoed in the transcript above the row,
which is empty after the send. The transcript is the rows above the row, so a
typed row never reads as an echo; a screen with no located row has no
transcript. Presend refuses to type into any composer that is not empty
(`presend:composer_pending`, `presend:composer_unlocated`). After a send, only
an unproven verdict with an empty composer (`unproven:composer_empty`) is
retried once; any other unproven omp verdict may already hold the message, and
is not retyped. A directive is held while the omp pane is working, since no
capture shows a message typed into a working omp pane.

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

## agy busy check (hk 5241)

herdr's `agent_status` for agy (Antigravity CLI, herdr agent `agy`) can read
`done` between long tool runs while the agent still works. Workers sat at
`done` between tool runs (pw-closeops-followups-5063), and a mergewake reviewer
panel was read as finished 6 minutes into a run that reported 10.5 minutes
later (mwr-490-1613996-p1/-p2, hk 5237). So a reap, idle-wake or pane sweep
that trusts herdr's status alone can end a working agy pane.

The check is `internal/agybusy`, and it has the same shape as devin's
(`devinBusy`, `hub_client.go`): herdr's status first, then the screen.

- Only an agy pane herdr reports `idle` or `done` is read. `working`,
  `blocked` and `unknown` are never changed, and no other harness is read.
- The visible screen (`agent read --source visible`, 40 lines, or the socket's
  `agent.read`) is busy when the bottom of it shows a working turn:
  `esc to cancel` as the footer under the prompt box (the last 2 non-empty
  rows), or a braille spinner row (`⠋ Running tools · 42s`) in the last 12
  non-empty rows.
- An unreadable screen, a read error or a blank screen is busy: nothing
  readable is not proof of an end of turn. An end-of-turn screen
  (`? for shortcuts` under the prompt box, no spinner, no footer) keeps `done`.
- A busy agy pane reads `working`. The rule only ever adds `working`; it never
  turns a `working` pane idle.

The judgement is applied where panewire learns a pane's status: the node's
agent and pane listing (`readNodeAgentsInSession` and `nodeAgentsListInSession`,
which the hub serves to reap and close checks), the local fleet's `App.agents`
(pane sweep, watch, hygiene), and the daemon's `pane.agent_status_changed`
path that feeds idle-wake and stall safety (`HerdrClient.agyEventStatus`).

Evidence. The markers are herdr 0.9.5's own agy rules: herdr's evidence
comment for `agy` says a working turn draws a braille spinner above the prompt
box and `esc to cancel` under its bottom border, and an idle prompt shows
`? for shortcuts`. No live capture of an agy pane during a long tool run was
available when this check was written, so the test screens in
`agy_busy_test.go`, `internal/fleet/agy_busy_test.go` and
`internal/agybusy/agybusy_test.go` are reconstructions from that evidence, not
captures. Whether the same markers appear in the bottom rows during a tool run,
where herdr reported `done`, is not yet shown by a capture. A live mid-tool-run
screen read (herdr's `agent_status` and `agent read` together) is what settles
it; until then this check is a conservative second source, not a proof.

## What "delivered" means

`relay.delivered` is emitted, and the durable row closed, only on the proof in
the table. Everything else is `relay.unconfirmed`:

- a send herdr rejected (`agent prompt`), or a send herdr accepted whose proof never shows
  (`unproven:...`, including a harness with no adapter, and an unreadable pane
  before or after the send, devin's included),
  is typed once more at once (a directive), except for an unproven omp send,
  and then dropped as `inject_failed_max_attempts`, `pane_read_failed`, or
  `delivery_uncertain_check_pane`. The text already tells the
  reader that seeing it twice is not a reason to re-run it;
- any other uncertain verdict (a foreign draft in the composer, a changed
  harness, an unreadable pane, a cmd `pane send-text` failure) is final,
  because the text may already be in the pane;
- a harness with no adapter is unconfirmed with `unproven:no_adapter`.

Messages that are not worker directives keep their earlier budgets: a rejected
send is re-held until the pane is idle, up to 3 sends (devin 2), and an unproven
send is never re-typed. devin may type part of a send herdr rejects, so a hub
with an inbox store gives such a devin row one labelled copy at most; a rejected
send on another harness retries to the hub's budget
([r20-relay-persistence.md](r20-relay-persistence.md), "Faults and copies").

Must-ack semantics are unchanged. An injected `findings`, `decision`, `ready`
or `blocked` item stays unread until the worker runs `panewire inbox ack <id>`.

## Escalation

The internal verdict `maybe_in_pane` is not the terminal inbox reason.
`pane_read_failed` means the pre-send pane read failed
and no text was sent. `delivery_uncertain_check_pane` means delivery could not
be proven; inspect the pane before resending. Both appear in inbox status.

When a delivery of a producer's `lane.event` stays unproven, the hub tells the
job's design lane: **one per host fault per incident** (hk 3276, hk 3431,
hk 6821). The design lane is the parent of the worker lane's route (the lane
`job.escalate` already reaches), else the owner lane of the live job that runs
in that lane. The notice is an `automation-failed` lane event; its text names
one durable event id, the producer's event id, the worker lane, the node and
pane, and the fault, and counts the other rows it speaks for. A `job.*` row, an
idle-wake report, an unconfirmed marker, and a notice that is itself lost never
start one.

A row whose design lane cannot be named waits for an hour, counted from the
later of its becoming due and the hub's start, then is logged and dropped: a
job's owner lane comes from job records held in memory, which a restarted hub
learns again from its nodes, and the lanes file can be unreadable for a while.

The incidents live in the hub's inbox store
([r20-relay-persistence.md](r20-relay-persistence.md), "Incidents and
notices"). A hub with no store is [the exception](#a-hub-with-no-inbox-store).

### When a row is told

| Fault of the row | Told |
|---|---|
| `maybe_in_pane`, `delivery_uncertain_check_pane`, `unproven` | at the node's first report, or at the drop that settles a `prompt_failed` or unproven pane read as maybe typed |
| `ack_timeout`, `pane_read_failed`, `inject_failed_max_attempts` | when the retries end and the row is exhausted |
| `working_wait_expired` | when the row is exhausted, which is the first pass after its drop |
| `unconfirmed` (a spent copy an older build retired) | when the pass exhausts it |
| `unrouted` | not on its own; see below for the failure it replaced |
| `job_ended` | never |

A safe fault does not wait for exhaustion when its retries cannot get there:

- A node's drop that typed nothing (`pane_read_failed`,
  `inject_failed_max_attempts`) is told as soon as a pass finds no route for the
  row, since nothing may ever exhaust it.
- An owed row with any failed copy whose retries end by age, found by the pass
  or retired `stale`, is told then. A row renamed `unrouted` is told under the
  fault it had, an `ack_timeout` included. A row exhausted earlier is not told
  again.

A row with an `ack_timeout` that only lost its route is not told while it can
still be retried. A released digest batch's row is settled, not exhausted, so a
safe fault on one is not told; a maybe-typed one still is.

A row is told once: after a notice covers it, nothing it does later makes it
due again. A row skipped when it was due -- nobody to act, no design lane after
the wait -- is settled the same way, so its age, a stale retirement or its
exhaustion does not bring it back once the ended job is forgotten. A row
confirmed before its notice is stored is not told, nor one the receiver already
read. A drop for a row that has no ack window and no recorded
fault -- one the node held across a hub restart -- makes the row due at once, as
the drop reports it, and changes no retry. A drop the hub cannot record, its row
unreadable or the store failing, is held in memory and retried after 2 s, 10 s,
1 min and then every 5 min until it is recorded; a restart loses it.

### Incidents

Rows due for one `(design lane, machine, fault)` are the members of one open
incident; the harness the node reports for the pane is text on it. A second
worker lane on the same host with the same fault joins the same incident.

- The obligation pass posts the notice, so it goes out on the next maintenance
  sweep (within about 10 s).
- A key's notices are at least 30 minutes apart. The first goes at once; rows
  that join inside the window are logged (`why=folded`) and wait. Once the
  window ends, the next pass posts one notice naming the row that has waited
  longest, ending `(+N more unconfirmed for this host since HH:MM UTC)`, where
  `HH:MM` is when that row became due. With no row waiting, nothing is sent.
- The window belongs to the key, not the incident: closing an incident and
  opening another does not shorten it. It is stored, so a restart keeps it.
- The notice, the window and the rows it told are committed only once the
  notice is durable (a handoffkeep row, or the design lane's inbox item). A
  post that fails, is refused, or finds only an in-flight claim commits nothing:
  the incident stays open and the same notice is posted again after 10 s, 1 min,
  5 min and then every 15 min, with whatever has joined, until it is stored. A
  restart posts at once.
- The producer id is `delivery-unconfirmed-i<incident>-n<announced>`, where
  `announced` counts the rows committed notices have covered: fixed until a
  notice is committed, so a repeat of a post whose reply was lost is answered as
  a duplicate. The id is recorded before the post, and while it is uncommitted
  and a row waits for it the incident stays open, whatever else holds, so no
  other incident of the key posts the same rows under another id. A duplicate
  takes the window and tells no row, since its stored text is the earlier
  post's; the next notice names the rows. One extra notice is accepted, a lost
  one is not.

### Only when someone can act

A row whose worker lane has nobody to act is not told: the hub logs one INFO
line (event id, lane, reason, `why`) and marks it told, for good. So it does
this only on an answer that cannot change back. The jobs counted are those the
hub knows in the lane (the route's own job, once the route names one).

| The lane's jobs | Answer |
|---|---|
| every one completed | skip (`why=job_ended`) |
| every one completed or with its hk task ended, at least one by its task | skip (`why=task_ended`) |
| a live job with an open task, or with no task | tell |
| otherwise, an orphaned job with an open task, or with no task | hold |
| a job record or task that had to be read and cannot be | tell |
| none known (a hub restart forgets ended jobs, and a lane may run work the hub never placed) | tell |

A task is ended when hk's category is `completed` or `canceled`, or, if the
record has no category, when its state is `merged`, `deployed`, `done`,
`dropped` or `canceled`. The hub reads the job record and the task through the
client that hub-placed spawns use.

An orphaned job -- its node away for longer than the hub's grace -- comes back
running or completed. A held row is not named and is not settled: it stays due
in its incident, which does not close as moot, and a later pass tells it when
the job runs again or skips it when the job completes.

A pass reads each lane's jobs once and uses that one reading to join, to name
and to close, so a job that changes while the pass runs is read by the next one.
A close reads no task of its own: for a lane the pass has read no task of, it
holds an orphaned job's rows and counts a live job as someone to act.

The design lane is the parent the worker lane's route names, else the owner of a
live job in the lane, else the owner of an orphaned one. A row with no design
lane is kept for an hour from its becoming due or the hub's start, whichever is
later, and for as long as an orphaned job in the lane names an owner; it is then
logged and marked told.

When the spawn's `--owner-notify` mode withholds the notice (hk 5360), the row
is counted once under `unconfirmed:<event id>` in the daily loops row, marked
told, and never sent; it takes no window and does not advance `announced`. The
mode is looked up by the row's own job id.

### How an incident ends

| Close reason | When |
|---|---|
| `recovered` | a member's own delivery was confirmed; a delivery of any other row, the same pane included, says nothing about this fault |
| `all-terminal` | every member is confirmed or settled |
| `moot` | every member still open or waiting is in a lane whose answer is skip; one tell or hold keeps it open |
| `quiet` | no member faulted, and none joined, for 60 minutes, and none is waiting to be told |

None of these closes an incident whose posted notice is uncommitted while a row
waits for it. A closed incident's members leave it: a `noticed` row is
`exhausted` again, and a row still waiting to be told joins the key's next
incident. Nothing is hidden: the obligation, the handoffkeep row and `relay.lane_stalled` remain.

### A hub with no inbox store

With `--delivery-policy off`, or no store, the hub keeps no obligations and so
no incidents. It tells the design lane from the node's drop
(`inject_failed_max_attempts`, `maybe_in_pane`, `pane_read_failed`,
`delivery_uncertain_check_pane` or `working_wait_expired`), as before hk 6821:
the producer id is `delivery-unconfirmed-<durable event id>`, a failed write is
retried after 2 s, 10 s, 1 min and 5 min, and one notice per (design lane,
worker lane) every 30 minutes counts the drops folded since, `(+N more
unconfirmed for this lane since HH:MM UTC)`. That window and those retries are
memory only: a restart forgets them. It asks once, at the drop, and holds
nothing: an orphaned job counts with the completed ones (`why=job_ended`), only
the live jobs' tasks are read, and only a parent or a live job's owner is told.

The hub's own one-labelled-replay after a restart
([#1002](r27-busy-relay.md)) is unchanged and is not an escalation.

## Stale replay notice (hk 6924)

A real message the replay gate retires as `stale` (its receipt is older than
`relayReplayMaxAge`, 24 h, when a startup or node-hello replay meets it) is one
its lane may never have seen. The hub keeps a daily account of those rows and
files one inbox item per owning design lane for each UTC day, once the day has
closed.

- **What is kept.** A `stale` retirement of a real message, except an
  idle-wake report and a row its lane already read or acked (the lane has it).
  A row is kept before its handoffkeep row is closed. If the row cannot be kept
  (the store write fails), the close is released and the row stays undelivered
  for the next replay. A failed job or owner lookup is retried on later replays
  for three tries (in memory, so each hub start gives it three more); then the
  row is kept on the hub's notice lane, so a persistent lookup fault does not hold
  it forever. There is no exclusion for a
  row received after its worker job's `job.completed` (hk 6900 is merged, so
  that case is no longer re-delivered), and a lost or revoked job's rows are kept.
- **Which job a row belongs to.** From durable state, in this order:
  1. a `job.*` row carries its job;
  2. a `lane.event` row takes the job its lane was bound to when the row
     arrived. The receipt records the lane's route job in
     `relay_stale_notice_lanes`: one binding per change of the lane's job, keyed
     by the receipt time handoffkeep stamped on the persisted row (the binding
     with the latest first sighting at or before the receipt wins, so a lane that
     moves A -> B -> A keeps a binding for each stint);
  3. for a row received before any binding was kept, the lanes.json route is a
     hint, used only when that route's job was spawned before the row arrived.
  The job's spawn record gives the design owner and the hk task. A job whose
  owner is not a design lane is left out, as before.
- **Unresolved rows are listed, not dropped.** A row with no job, or with no
  spawn record or owner, is filed on the hub's notice lane (`panewire-design`
  by default) with the job shown as `-`, so no retired row disappears silently.
- **The item.** Filed inbox-class (`notify_type` `info`) to the design lane,
  event id `stale-replay-<day>-<n>`, where `n` is the smallest handoffkeep row
  id the pass read, so a row retired after a pass read its rows is filed by the
  next pass under its own id. It names the rows the task did not overtake:
  sender prefix, message type (the relay kind), the worker lane and job (each
  cut to 40 bytes), received time, age at retirement, the handoffkeep row id
  (`row N`, the key to look a row up by) and the hk event id, at most ten of
  them, then a count of the rest. The item is hub-originated (`host` `hub`). The footer
  counts the overtaken rows; room for it is reserved, so it is never cut off.
- **Overtaken rows.** A row is overtaken when its lane's hk task reached an end
  state (merged, deployed, completed, dropped or duplicate; icebox is parked, not
  ended) and hk's `completed_at` or `canceled_at` falls within **6 h after the
  row's receipt**. Those stamps are kept across a later edit such as merged ->
  deployed, so `updated_at` (which moves with every edit) is not read. Overtaken
  rows are counted in the footer, not listed. A task that cannot be read, or one
  whose end is not in the window, leaves its row listed.
- **No event on an empty day.** A day with no listed row for a lane files
  nothing for that lane. Its rows are then marked filed.
- **Closing a filed day.** Only the rows the filing pass read are marked filed; a
  row retired while the pass runs stays owed for the next pass.
- **Survival.** The rows live in `relay_stale_notice_rows`, and the lane
  bindings in `relay_stale_notice_lanes` (hub store, SQLite, Postgres migrations
  0043 and 0045, MySQL migrations 0039 and 0042), so a restart mid-day keeps
  them. A filed day's rows are pruned after 14 days. Bindings are kept: one row
  per change of a lane's job, keyed `(worker_lane, first_ms)`. A SQLite store
  with the first key is rebuilt on open. Rolling back past this change is a
  known degradation: an older binary's binding writes fail, so rows received in
  that window fall back to the lanes.json hint or the notice lane.

Code: `relay_stale_notice.go`, `relay_stale_notice_store.go`, and the two hooks
in `relay.go` (`noteStaleReplayLane` once a lane.event is persisted,
`noteStaleReplayRetire` before the close in `retireRelayReplay`). Tests:
`relay_stale_notice_t6924_test.go`, `relay_stale_notice_t6976_test.go`,
`relay_stale_notice_t6994_test.go`, `relay_stale_notice_host_t6994_test.go`.

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
job or task, the fail-open reads, the 30-minute window, the notices that are
never stored and concurrent drops; `delivery_incident_t6821_test.go` pins the
incidents, their window across closes and restarts, the closes, the timing per
fault and the withheld notice, `delivery_incident_loss_t6821_test.go` the rows
a lost route, age, a restart or a failed read would otherwise end silently, and
`delivery_incident_store_t6821_test.go` the store on all three backends. `relay_unconfirmed_quiet_legacy_t3431_test.go`
pins the hub with no inbox store.
