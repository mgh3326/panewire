# Quiet event delivery

The hub CLI enables the embedded `delivery-policy.yaml` by default. Informational
notices enter a durable per-lane inbox; actionable notices keep the existing pane
injection and verification path. Nothing is installed by this document.

The reviewed `operator-desk` override keeps routine events in its inbox even
when idle. An external scheduler owns its digest. See [desk inbox delivery](desk-inbox.md)
for the type map and interrupt summary behavior. Design and desk lanes are
[inbox-only](#inbox-only-lanes): they get a one-line wake instead of bodies.

```sh
panewire emit --kind lane.event --lane desk --event-id build-ready \
  --deliver inbox --type report --text '[build] done | report posted'
panewire emit --kind lane.event --lane desk --event-id need-decision \
  --deliver interrupt --type decision --text 'Decision required: choose the release'
```

The optional `delivery_class` and sender-declared `notify_type` travel through
event files, the immediate local socket request, scanner, node outbox and hub
ingress. `--deliver` selects the class for **all emit kinds** and `--type`
declares one of the [notification types](desk-inbox.md#desk-inbox-delivery); an unknown
value is a usage error. New types include `merged`, `idle-wake`, `progress`,
`deploy-approval`, `watch-hold`, `culprit-hold`, and `automation-failed`. The existing JSON `deliver` field remains the
separate node timing policy (`idle`, `now`, `max_wait=...`). No new field is sent
in `relay.inject`; old daemons accept exactly their existing envelope. Missing
classes or types from old emitters/daemons are classified at the upgraded hub. An old
hub does not understand explicit classes or types: upgrade the hub first, then nodes and
emitters. An old daemon drops the explicit overrides, so only hub policy applies
until that node upgrades.

## Policy

Must-act tags always interrupt, ahead of explicit class, the declared type,
replay, and every inbox rule. Otherwise explicit class wins, then the `types:`
map resolves a declared `--type` (decision/order/ready/blocked/findings → interrupt;
report/ack/info → inbox, all overridable globally or per lane), then the first
matching rule, with lane
rules ahead of global rules. Escalation, joined and revoked job events use the
**receiving parent lane's** policy and inbox, following existing routing.

Inbox-class events deliver **immediately** instead of waiting for the deadline
when the receiving lane owns the completed job — `job.completed` to the owner
lane and `job.joined` to the owner lane's parent — when the receiving pane's
reported agent CLI is not `supported` under `agents:` (unknown or unreported
counts as unsupported), or when a supported pane is known idle/done by a fresh
heartbeat session snapshot. Unknown state is never treated as idle. Everything
else keeps the deadline digest as its safety net.

Set `idle_promote: false` on a lane to keep inbox items queued when its
pane is idle or unsupported, while retaining ordinary deadline escalation.
Owned job completions and interrupt-class events still deliver immediately.
Use this for lanes without an external digest scheduler; `inbox_summary`
disables inbox deadlines and requires an external scheduler instead. The two
`idle_promote: false` cannot be combined with `inbox_summary` on a lane.

```yaml
lanes:
  design:
    idle_promote: false
```

```yaml
agents:
  claude: supported   # built-in default; every other CLI defaults unsupported
```

| Signal | Default class |
| --- | --- |
| BLOCKER, READY, ESC, BOUNCE, URGENT, needs_decision tags | interrupt |
| decision request/required/needed, action required/needed/now | interrupt |
| Direct question (`?`, `？`, question/질문) | interrupt |
| job.escalate, job.lost, job.revoked | interrupt |
| Replay without an already saved decision | inbox |
| job.completed, job.joined; `[worker] done \|` | inbox |
| FYI, ack, acknowledged | inbox |
| install-done, installed, merged/please install | inbox |
| Info alert, quota notice, idle-wake | inbox |
| Unmatched message | interrupt |

Must-act vocabulary uses whole-word, case-insensitive literals compiled as
`(?i)\bTAG\b`. `Ready` interrupts; `READYING` and `ESCAPE` do not match.
The six tags above are always active, including with custom policies. Additional
`tags:` on interrupt rules in either scope have the same priority.
Text `match:` rules are raw regexes for phrase shapes (questions, Korean,
multi-word phrases); the shipped informational patterns are whole-word
bounded too, so a stray `info` inside a must-act message cannot route it to
the inbox. `kind:` stays an exact match against the closed event-kind set.

Action/question rules precede informational rules; an FYI containing a BLOCKER
therefore interrupts. This is heuristic text classification, not semantic intent
recognition. Explicit overrides apply when no must-act tag is present. A saved
decision wins over a changed retry body, replay flag or later policy edit;
startup recovery escalates previously misfiled unread must-act originals once.

`--delivery-policy default` uses the embedded file. A filesystem YAML path loads
an operator-owned copy at startup. `PANEWIRE_DELIVERY_POLICY` supplies the same
value from the environment — `PANEWIRE_DELIVERY_POLICY=off` is the kill switch
that restores pre-policy pane delivery without editing service arguments; an
explicit `--delivery-policy` flag always wins over the environment variable.
With the inbox enabled, invalid policy or an unavailable store prevents
hub startup. A runtime storage failure leaves inbox-class events unacknowledged;
interrupt-class events fall through to the normal durable relay and injection
path. YAML has strict keys, checked classes/regexps/durations, and one document. Per-lane
example:

```yaml
default: interrupt
deadlines: {inbox: 30m, interrupt: 0s}
digest_window: 0s
rules:
  - match: '(?i)FYI|\bdone\s*\|'
    class: inbox
lanes:
  desk:
    deadlines: {inbox: 10m}
    digest_window: 2s
    rules:
      - match: '(?i)urgent'
        class: interrupt
```

A deadline of `0s` disables escalation for that class. Digest windows must be
`0s` (off, the default) through `1m`. Policy changes require a hub restart; they
apply only to newly accepted identities, except startup must-act recovery.
`--delivery-policy off` or `PANEWIRE_DELIVERY_POLICY=off` completely disables the
inbox, exactly like `--inbox-store=`: no database open, put, startup recovery,
flush, or escalation. Every event uses the existing relay delivery path, even
with `delivery_class=inbox`. Stored items and queued batches remain untouched
until the inbox is enabled again.

For a full rollback to a pre-feature hub binary, account for its strict payload
field allowlist: it rejects `delivery_class`. Stop using explicit `--deliver`
and ensure queued node payloads omit that field before reverting the hub. The
kill switch on the upgraded hub accepts those payloads and requires no node
rollback. Preserve the inbox database and its WAL during either rollback.

### Must-ack types

A lane event declared `--type ready`, `decision`, `blocked` or `findings` must be
acknowledged by the receiving session. Injecting it into the pane does not mark it
read, and `inbox read` does not retire it: it stays unread — in `inbox count`,
`inbox list --unread` and `inbox wait` — until `panewire inbox ack <id>` (or
`ack --all`). `inbox list` shows it as `unacked` until then, and its JSON carries
`"must_ack": true`. This holds whatever class the policy gives the type.

Every other interrupt is still stored read on insert: injection is its delivery.
Idle-wake events (no declared type) and progress messages (`report`/`info`, or
undeclared) keep that behaviour. Must-act text tags such as `READY` decide the
class only; they do not make an undeclared event must-ack, so a producer that
needs the guarantee declares the type.

The hub adds the `must_ack` column to an existing inbox store on start; rows
stored before keep their recorded read state. An old hub rejects the new
`blocked` and `findings` types at ingress, and an old `panewire emit` rejects them
as a usage error: upgrade the hub, then nodes and emitters, before producers
declare them. Rollback: producers stop declaring `blocked`/`findings`, then the
hub binary reverts; the extra column is ignored by the old binary.

## Design lanes

A design-session lane (`handoffkeep-design`, `panewire-design`,
`mergewake-design`, …) gets decisions and failures only. The class comes from
`relay.design_lanes` in the delivery policy: exact lane names and shell-style
globs (`path.Match` syntax), default `["*-design"]`. An explicit empty list
disables the class. A malformed pattern refuses the policy at load.
`operator-desk` is never a design lane, even under a pattern that matches it,
because the desk's external scheduler depends on the inbox summary.

```yaml
relay:
  design_lanes: ["*-design", "handoffkeep-design"]
```

For a design lane the hub:

- answers the node's idle-wake route request with a `design_lane` suppression
  and logs one DEBUG line per suppressed event. The node never materializes
  the wake, so no event, pane injection or inbox item is produced;
- never appends the `inbox_summary` handoff to an interrupt. Unread items stay
  readable through `panewire inbox list --lane LANE`.

Since hk 3602 a design lane is also inbox-only: nothing in this section is
typed into its pane any more. ESC and decision questions, `automation-failed`
(a SPAWN_FAILED notice), `blocked`, desk orders and deploy approvals are filed
unread and announced by the lane's one-line wake; see
[Inbox-only lanes](#inbox-only-lanes). The rules below still decide which
items are routine: a routine item is filed without a wake.

### Worker results stay in a design lane's inbox

A worker's routine result does not interrupt a design lane (hk 3426). mergewake
reviews and merges from the PR, so the design session has nothing to do with it.
The hub keeps these in the lane's inbox, without a pane injection:

- `job.completed` for a job that has a PR (the event's `pr`) or a task (the
  `task_id` of the job's hk record, read through the hub's existing spawn
  client);
- a `lane.event` declared `--type ready`, also when the sender passed
  `--deliver interrupt`, unless its text starts with `[consult-done]`;
- a worker's own routine `lane.event` (hk 3434), also with
  `--deliver interrupt`:
  - one declared `--type progress`, `report`, `info`, `merged`, `ack` or
    `closed`;
  - an untyped one whose text carries none of the action tags `ESC`,
    `BLOCKER`, `BOUNCE`, `URGENT` and `needs_decision`, such as
    "hk 3430: PR #221 open" or "#221 merged; follow-up #222 open". These are
    the must-act tags without `READY`: an untyped worker READY is a
    completion record, so it stays in the inbox too.

Every other type a worker declares is classified as before, so it interrupts
by default: `decision`, `blocked`, `findings`, `order`, `automation-failed`,
`deploy-approval`, `watch-hold` and `culprit-hold`, and any type added to the
vocabulary later. So does a worker's untyped text with an action tag, such as
"ESC need a decision | options …". The fleet-v1 brief preamble
(`internal/fleet/brief-policy.json`) tells a worker to escalate with
`--type decision` and to report being stuck with `--type blocked`.

`designQuiet` (`delivery_policy.go`) is the one decision point. Once it keeps
an event in the inbox, nothing after it in `receiveInbox` (`hub_inbox.go`)
puts the event back in the pane: not an explicit `--deliver`, not a replay,
not the inbox-immediate rule and not a worker-directive route.

They are ordinary unread inbox items (a `ready` one stays unacked until
`panewire inbox ack`): readable through `panewire inbox list --lane LANE`, never
digested or escalated, and never replayed into the pane after a hub restart. The
job's own state, its hk record and `spawn --wait` are unchanged.

A `lane.event` is a worker's when the hub can attribute it to a live worker job
on the node that sent it. The node names the emitting pane in the wire payload's
`pane_id` (see [R21 direct lane events](r21-lane-event.md)), and the hub looks
for a job that the sending node's latest heartbeat lists in that pane and that
the hub holds as not completed and not orphaned on that node, with role
`worker` and the owner lane equal to the lane the event is for. A worker of
another design lane sharing the pane is not this lane's: its progress is not
quieted for this lane. Every `panewire spawn` job claims as `worker`, whether
it was spawned as a worker, tester, reviewer or counsel; a builder or captain
is not one. Design sessions, the desk and workers share hosts, so the node
match alone does not decide. Whatever the hub cannot attribute this way is
delivered as before:

- a `lane.event` with no `pane_id` (an older node, or a hub that has not
  advertised it to the node yet);
- a hub notice (SPAWN_FAILED, `[delivery unconfirmed]`), an HTTP ingress
  event and a restart replay, which have no sending node;
- a pane with no live job, a completed or orphaned job's pane, and a pane id
  that only a job on another node uses;
- a design session's or the desk's own `lane.event`, including the
  TASK_LINK_FAILED the spawning CLI sends from the design session's pane;
- a node's own notice about a worker pane (a stall notice or an idle-wake): it
  names the pane it is about, not a sender, so it carries no `pane_id`.

A completion of a job with neither a task nor a PR — a consult answer the
session waits for — is delivered as before, and so is one whose hk job record
cannot be read. A consult answer — a `lane.event` whose text starts with
`[consult-done]`, sent `--type ready` as rstack's ops consult reference gives
the answerer — is also delivered (hk 3434), from a counsel job's pane too: the
session that asked waits for it in its pane. `job.escalate`, a worker's
action events above, and the `decision`, `blocked`, `findings` and
`automation-failed` types from anyone else still interrupt. Lanes that are not
design lanes, `operator-desk` included, are unchanged, and so are `job.*`
kinds.

### Records kept in hk only

The lifecycle of a reviewer a daemon started is not an inbox item at all (hk
4270, operator decisions 152 and 153). The daemon consumes it, and 469 of
`mergewake-design`'s 500 listed items on 2026-11-12 were of this kind. For a
design lane the hub files `job.completed`, `job.revoked` and the SPAWNED notice
of such a job in hk as a relay row it marks delivered as `hub` /
`filtered:reviewer-lifecycle`, and writes no inbox item, so the lane's `inbox
list`, unread count and wake never see it.

**Recognised as daemon-started** when the hub's own saved spawn request for the
job has `--role reviewer` and a job id that starts `mwr-` (mergewake's reviewer
naming, as `nodeReviewerDirPattern` relies on). The prefix is a naming
convention, not authenticated provenance: a job the hub did not place, or whose
record cannot be read, is delivered as before. The submitter's `--requested-by`
is not part of the test (the value mergewake sends is not visible to the hub).

**Filed only against an exact durable receipt.** hk must hold the row: a hub
with no hk client files nothing, and a failed hk write leaves the node's outbox
row unacknowledged so it is sent again. The row hk returns must also be this
event's own. hk's `job.*` idempotency key is (kind, job, epoch, report path,
reason) and excludes the event id, so a second completion that reuses them gets
the first row back, which does not hold the second event; that event is
delivered to the inbox like any other, and both payloads are kept. The decision
depends only on the record and the saved spawn request, so a producer's retry
after a lost acknowledgement or a hub restart decides the same way.

Nothing is dropped: hk lists each filed row with `delivered_to`
`hub/filtered:reviewer-lifecycle`, and the job's own hk record is unchanged.
What still reaches a design lane is everything else: READY, BLOCKED, decision,
findings, `order`, every `job.completed` of a worker (including one after its
READY), a design session's own spawn notices and the failures in
[hub spawn operations](hub-spawn-operations.md#notifications-and-telemetry).
Lanes that are not design lanes are unchanged.

**Not done under hk 4270:** hiding a `job.completed` that repeats a READY, and
the retry/coalescing of unconfirmed deliveries. See the
[pull request](https://github.com/mgh3326/panewire/pull/448).

## Inbox-only lanes

Design lanes and desk lanes are inbox-only (hk 3602). A notification to one
is filed in the lane's inbox as an unread item and is never typed into the
pane. This covers every `lane.event` (a worker's, another session's, a hub
notice such as SPAWN_FAILED or `[delivery unconfirmed]`), every `job.*` kind,
an explicit `--deliver interrupt`, must-act tags and replays. Such an item is
never digested, held or escalated either. The lane's session reads it with
`panewire inbox list --lane LANE` and retires it with `panewire inbox ack`.

The lane classes come from the delivery policy:

```yaml
relay:
  design_lanes: ["*-design"]                         # default
  desk_lanes: ["operator-desk", "*-operator-desk"]   # default
  inbox_wake_window: 5m                              # default; 10s..1h
  drain_wake_interval: 1h                            # default; 10m..24h
  drain_wake_threshold: 0                            # default; 0..10
  worker_inbox_trial: []                             # default: no trial
  worker_inbox_trial_until: "2026-11-16T00:00:00Z"   # required with a trial
```

`desk_lanes` takes exact names and `path.Match` globs like `design_lanes`. The
default covers `operator-desk` and per-fleet desks such as
`work-operator-desk`. A desk lane is never a design lane. An explicit empty
list turns a class off.

One kind of traffic keeps its existing delivery in every lane:

- events the operator posts through the [R24 HTTP ingress](r24-http-ingress.md)
  with the operator token. This is the explicit operator path into a pane, so it
  is kept. A restart replay of such a row is filed like any other item.

The hub's digest and hold batches are not typed into an inbox-only lane
either. A batch built for one (an item stored before the lane became
inbox-only, for example) is filed: its items become unread inbox items, the
batch is quarantined with the reason `inbox-only lane: items filed unread`,
and the lane gets its wake. The inbox summary pointer (`inbox_summary`) is not
added to anything typed into an inbox-only lane, so anything typed to the desk
is typed as sent.

The rule holds where a body would be typed, not only where an event arrives.
An item stored under another class, by an older hub or before the lane became
inbox-only, is filed unread when a producer retry, a job resend or a restart
replay would type it, and its handoffkeep row is closed as delivered to the
inbox.

`panewire prompt` types directly through the local herdr and never reaches the
hub, so it is unaffected too.

### The one-line wake

The hub types one line into the lane's pane when the lane gets new unread
work, through the lane's node daemon like any relay:

```text
[inbox] <lane>: N unread (most urgent: <kind>) | panewire inbox list --lane <lane>
```

`N` is the lane's unread count, the same number `panewire inbox count` shows.
`<kind>` is the most urgent unread item, in this order: `decision` >
`blocked` > `automation-failed` (SPAWN_FAILED) > `findings` > `ready` >
`info`. The declared `--type` decides the kind. `order` and `deploy-approval`
rank as `decision`, and `watch-hold` and `culprit-hold` as `blocked`. An
untyped item is ranked by its kind (`job.escalate` is a decision, `job.lost`
and `job.revoked` are blocked, a completion is ready), by a leading
`[consult-done]` (ready), or by its tags (ESC, URGENT and needs_decision;
BLOCKER, BLOCKED and BOUNCE; SPAWN_FAILED; READY).
Everything else is `info`.

When the line is sent:

- An item asks for a wake only if it would have been typed into the pane
  before hk 3602 and it is not a declared info record. These items count
  toward `N` but never wake the lane by themselves: a routine item that
  [design-lane quieting](#worker-results-stay-in-a-design-lanes-inbox) keeps,
  an inbox-class item, and a declared `merged`, `closed`, `progress`,
  `report`, `ack`, `info` or `idle-wake` item, such as a SPAWNED or MERGED
  record. Untyped text that would have been typed still wakes the lane: the
  hub cannot tell that it is routine. It ranks as `info` unless a tag ranks
  it higher.
- The first such item is announced at once and opens a window of
  `relay.inbox_wake_window` (default 5 minutes).
- Further items inside the window add no line. At the window's end the hub
  sends exactly one more line, and only if the lane still has unread items.
  That line opens the next window. A window end that finds nothing unread
  closes the window instead, so the next item is announced at once.
- A design lane whose in-flight jobs fall to `relay.drain_wake_threshold`
  (default 0) while hk lists open backlog for its project gets one line,
  `N backlog, M in work: dispatch the next batch`. The project is the lane
  name without `-design`; the backlog is the hk tasks in state `backlog` or
  `unstarted`, counted per state with hk's project and state filters, page by
  page. The line goes out on the drain and at most once
  per `relay.drain_wake_interval` (default 1h). A lane first seen idle gets no
  line. This is an interim until the hub-owned queue (hk 4376) replaces the
  hk read.
- A line still owed to a [trial](#worker-trial) lane when its trial ends is
  dropped: the lane is a worker lane again.
- When the hub starts, every inbox-only lane with unread items above `info`
  gets one line after the same wake window, with `startup` as the fleet-event
  reason. Restarts inside the window coalesce into one line after the last
  restart. Repeated restarts less than one wake window apart can defer this
  line until the hub stays up for a full window; the window is not persisted.
  A lane whose unread items cannot be read gets none.

The wake decision lives in the hub, which owns the inbox store and sees each
arrival. The node does not keep an inbox copy, so it would have to poll the
hub for counts. The node daemon still types the line. The line is a plain
inject with no handoffkeep row, so a hub restart never replays it. The
window state is in memory, so a window open during a restart is dropped; the
startup line above announces what is still unread.

A failed count read never becomes `0 unread`. The hub skips the line, logs a
warning, records the skip and retries at the next window's end. A lane whose
pane cannot be reached (no route, no connected node) is retried the same way,
or at once when a node next says hello. Every wake writes one
[fleet event](config.md#fleet-events) of kind `inbox_wake`: outcome `sent`
with the lane class (`design`, `desk` or `trial`), `startup` or `desk_fallback`
as the reason, or `skipped` with `count_failed` or `unrouted`.

### A design lane with no live pane

When a design lane has no route row, its row has an empty pane, or a connected
node's fresh pane snapshot proves its pane is gone, the hub sends that lane's
one-line wake to `operator-desk` through the desk's own route. The line names
the design lane and its unread count, and says `no pane: desk takes it`. The
item remains unread in the design lane; it is not copied into the desk inbox.
The paneless lane keeps its own wake window and retries. Each attempt checks
the route again, so a pane that returns receives the next line itself.

A missing row is paneless only when there is no live job record for the lane.
A disconnected node or a stale, missing or incomplete snapshot does not prove
the pane is gone, so the hub keeps today's route behavior. A row can outlive
its pane when `laneHasForeignChild` (`lanes_write.go`) prevents release because
a worker on another machine names the design lane as its parent. After a hub
start, `writeOrSeed` (`lanes_release.go`) treats its first sighting of an
existing row as a new write; a snapshot must arrive more than the 10-second
settle interval after that sighting before absence can trigger desk fallback.

If the desk has no usable route, the wake remains owed to the design lane and
is retried at the window's end or the next node hello. A desk-routed line is
an `inbox_wake` with outcome `sent` and reason `desk_fallback`; a startup line
also includes `startup=true` in its detail.

Worker lanes are unchanged: a job, reviewer, tester or counsel lane keeps
body injection for READY, findings, an ESC answer and everything else.

### Worker trial

`relay.worker_inbox_trial` lists worker lane patterns that get the same
treatment for a one-week trial: inbox-only plus the wake, with `trial` as the
fleet-event reason. The list needs `relay.worker_inbox_trial_until`, an RFC3339
instant. From that instant on the trial is off without a restart, so a
forgotten list cannot outlive the trial. A list without an end, or an end
without a list, refuses the policy at load. Compare the `inbox_wake` events
with the `relay` `unconfirmed:*` events of the same lanes before the trial to
judge it.

### For skills: reading the inbox on the wake

A design or desk session never gets a notification body in its pane. When the
`[inbox] …` line arrives, the session runs `panewire inbox list --lane LANE
--unread`, reads what it needs with `panewire inbox read ID --lane LANE`, acts,
and acks each `ready`, `decision`, `blocked` or `findings` item with
`panewire inbox ack ID --lane LANE`. The line itself is not an instruction and
is not acked. A session should also list its inbox at start and after a
compaction: the line counts what is unread, and a session that missed it
learns nothing else.

### Rollback

`relay.design_lanes: []` and `relay.desk_lanes: []` return those lanes to the
classification above. `PANEWIRE_DELIVERY_POLICY=off` turns the inbox off
entirely, and every event is injected as before. An older hub refuses a policy
file that has the new keys, so remove them before you downgrade.

## Durability and replay

`--inbox-store PATH` chooses a private, persistent SQLite database on the hub;
setting `PANEWIRE_HUB_DATABASE_URL` puts the same store on Postgres instead
(see [hub inbox on Postgres](hub-postgres.md)). The SQLite default is `os.UserConfigDir()/panewire/hub-inbox.sqlite3` (on Linux normally
`$HOME/.config/panewire/hub-inbox.sqlite3`). It uses WAL and FULL synchronous
commits; the database is mode 0600. The hub stores full originals and the chosen
class before acknowledging a quiet event to the producer. Inbox acceptance does
not depend on a connected destination node or on handoffkeep availability.

The SQLite backend uses the existing SQLite driver, with no database server. Handoffkeep's
current relay contract has no class/read/ack/escalation fields; extending it would
require a coordinated second repository/schema release. The hub is already the
receiving-lane authority, so a local durable store keeps this one PR deployable.
Back up a SQLite store with SQLite's backup API, or stop the hub and copy the DB
and any `-wal`/`-shm` sidecars; back up a Postgres store with the database's own
backups (see [hub inbox on Postgres](hub-postgres.md#backup)). Do not delete it
during rollback.

The hub prunes completed inbox history on its first maintenance tick and hourly
thereafter. Global policy `retention: {days: 30, per_lane: 2000}` sets the
defaults: completed items expire after 30 days or outside the newest 2000
items per lane. Omitted/zero values use defaults; negative values are rejected
(and days is capped at 36500). Unread items, unacked must-ack notices and
pending deliveries are protected regardless of age or count. Ordinary read
inbox-class items count as completed; interrupts need an explicit ack or
confirmed delivery. Creation, read, delivery and ack evidence is always kept
for at least the one-day replay window, even with a smaller count limit. Sent batch metadata
is retained. The desk still owns backup, disk monitoring and archival. A future
hub failover must move this database with the hub, not start an empty store.

Original inbox items never become relay directives. On old handoffkeep replay
(including `hub_unconfirmed_once.go`), the hub saves informational rows in its
inbox before retiring those relay rows. A failed retirement retries harmlessly;
the inbox identity and class are stable. Delivered handoffkeep rows are not
imported. A settled inbox item — confirmed delivered, read by the receiver (a
must-ack item included), acked, or superseded — suppresses both ordinary and
labelled restart replay, for every lane, not only design lanes. An
interrupt's automatic read-on-insert timestamp alone never suppresses replay;
only genuinely unconfirmed delivery can receive the once-only labelled copy,
and a digest or escalation batch never receives it (see below).
A pre-upgrade active pane submission cannot be recalled.

An unread item past its deadline is claimed once into one short per-lane
interrupt digest. Reading/acking before the claim suppresses escalation. The
claim and outgoing digest are one database transaction. Restart retries the same
batch ID. The digest is a single line — a count and the inbox pointer — which
always satisfies the lane.event text contract (no control bytes, far under
the length cap); full originals remain readable by `inbox list`. An item is
never escalated into another batch while its batch is live, although the existing relay transport may
retry an unconfirmed submission as before. The one exception is a batch relay
whose pane submission was never proven (`relay.unconfirmed`): after a hub
restart it is not replayed, because its count is stale. The hub retires its row
as `digest-not-replayed` and releases the batch's items that are still unread
or unacked, so the next flush escalates them in a fresh batch with the current
count. A batch is recognised by its row in the batch table, never by its event
id; a hub with no inbox store open defers such a row until a store is open
again. Later unread items can cause a later digest. Escalations use the existing handoffkeep relay; during a
handoffkeep outage their durable batches wait and retry.

At startup the hub scans all unread inbox originals for must-act tags, including
rows misfiled by older versions and rows with disabled deadlines. It makes them
immediately eligible for escalation. Existing valid escalation claims survive
restart; claims on quarantined legacy batches can be recovered into a valid
single-line batch.

That retry is deliberately slow: a failed flush pass doubles its backoff from
1s to a 60s cap, stops after the first transient failure per window, and counts
failed passes (`InboxFlushFailureCount`). Every batch is validated before sending:
single-line, valid UTF-8, at most 2048 bytes, and no control characters. Invalid
batches and permanent per-batch HTTP 4xx rejections are quarantined with a reason
in `lane_inbox_batch_quarantine`; their originals remain in
`lane_inbox_batches`. The flush continues with later batches. At startup the
entire pending queue is scanned to quarantine legacy multiline batches before
sending. Authentication failures (401/403), retryable request failures
(408/425/429), transport errors, and server outages retain bounded backoff.
Failure to mark an accepted batch sent also stops the pass and uses this
backoff, with one error log per failed window. The stable relay identity
prevents another injection while the local sent marker is retried.

Only startup checks can latch all digest delivery off. A quarantine/recovery
write failure logs one loud error and disables digests while the hub continues
serving ordinary relay traffic, including must-act interrupts. Fix the inbox
store and restart to retry startup maintenance. Invalid policy or a store that
cannot be opened/initialized still prevents startup.

The read-only contract probe (`GET /v1/relay/events?kind=lane.event&limit=1`)
also disables digests on HTTP 400/404 indicating an unsupported kind filter or
missing route. Stored items wait for a compatible handoffkeep and a hub restart.
An inconclusive probe leaves the bounded retry path active. Individual rejected
batches never disable other digests.

When enabled, interrupt batching uses the same transactional outbox: arrivals
for one receiving lane within the oldest item's window become one turn. A
single item is also delayed by that window. Must-act tags bypass batching. The flush runs every 100ms; a batch is never sent early.

## Holding interrupts while a receiver works

A receiving lane can opt into `hold_while_working`. The hub holds interrupt-class
notices only while a fresh, complete heartbeat snapshot positively reports its
pane as `working`. Idle/done releases them on the next flush (normally 100 ms).
Missing, stale, truncated, disconnected or unknown state never starts or extends
a hold. The oldest arrival's `max_hold` caps the queue even if the pane stays
working; later arrivals cannot extend it. This setting does not change inbox-class
handling or require a supported `agents:` entry.

Recommended desk policy, added to an operator-owned copy of the default file:

```yaml
lanes:
  desk:
    hold_while_working:
      max_hold: 10m
      handoff_bytes: 1500
      handoff_items: 3
```

`max_hold` must be positive. Omitting the block disables holding; thresholds
must be nonnegative and zero/omitted values use the defaults shown above. A
policy-wide block supplies defaults, and a lane block replaces it. The existing
`--delivery-policy off` kill switch disables holding and document writes too.
Old nodes without working-state reports deliver as before.

READY, FINDINGS, BLOCKED and MERGED interrupt notices can be held. The READY tag
still selects the interrupt class, but does not bypass this hold. Types `decision`
and `order`, kinds `job.escalate`, `job.lost`, `job.revoked`,
and whole-word BLOCKER/URGENT/ESC tags always bypass the queue. BOUNCE and
needs_decision tags also bypass it. Original IDs and must-ack requirements remain
available through `inbox list` and `inbox ack`.

One release produces one injection with a first-line count such as
`[batch 7: BLOCKED 1, FINDINGS 1, MERGED 2, READY 3]`. Producer retries reuse the
saved identity; delivered replays do not enter the queue, and acknowledged items
are excluded before counting. Held items and claimed releases survive restart.

When full batch text **exceeds** `handoff_bytes`, or its count **exceeds**
`handoff_items`, the hub PUTs the full originals into an hk document under
`inbox/<lane>/<UTC timestamp>-<unique suffix>`. The injection contains the counts,
`hk:doc <key>` and bounded one-line titles. Document retries reuse the same key.
The relay's small durable reference resolves to the saved injection on replay;
keep the inbox database alongside the hub as described above.

If the document write fails, the injection falls back to full text. The existing
node directive ceiling is 24 KiB: a fallback too large for that envelope stays
durable and retries document handoff, rather than truncating or dropping items.
The normal relay persistence/retry requirements still apply during an hk outage.

## Inbox commands and API

By default, inbox commands use the local node daemon and its hub connection.
For direct access, set `PANEWIRE_HUB_URL`, `PANEWIRE_HUB_TOKEN_ENV` (path to the existing private
`HUB_MACHINE_ID`/`HUB_TOKEN` env file), optional `PANEWIRE_HUB_CF_ENV`, and
`PANEWIRE_LANE`, or pass the corresponding flags shown by `panewire inbox --help`.
Use the same approved hub origin as other operator commands.

```sh
panewire inbox list --lane desk --unread
panewire inbox read 123 --lane desk
panewire inbox ack 123 --lane desk
panewire inbox ack --all --lane desk
panewire inbox count --lane desk
panewire inbox wait --lane desk --timeout 10m
panewire inbox list --json
panewire inbox count --json
```

List is newest first, capped at 500 items, and includes the saved class; `--unread`
filters unread items: inbox-class items not yet read, and must-ack items
(above) not yet acked. Read marks read. Ack marks read and acknowledged;
neither removes the record. Other interrupt originals appear in unfiltered lists for
inspection, but do not increase the unread count. A read/ack is a hub write.
`wait` blocks until an unread item exists for the lane, prints it like `read`,
and exits 0; on timeout it prints nothing and exits 3. It is the wake primitive
an agent can run in the background instead of being typed into.

`read <id>` and `ack <id>` name one stored item. For operator tokens an ambient
`PANEWIRE_LANE` does not scope them — only an explicit `--lane` does — so a
mismatched ambient lane cannot hide an existing item behind a 404.

The authenticated API is `GET /v1/inbox?lane=L&unread=true`,
`GET /v1/inbox/count?lane=L`, `GET /v1/inbox/wait?lane=L&timeout=D`,
`POST /v1/inbox/{id}/read?lane=L`, and
`POST /v1/inbox/ack?lane=L` with `{ "id": 123 }` or
`{ "all": true, "lane": "L" }`. The wait endpoint is a bounded long-poll: it
answers `{"item": {...}}` as soon as the lane has an unread inbox item or
`{"item": null}` once the wait runs out, holds at most 60 seconds per request
(the CLI re-issues it for longer `--timeout` values), and refuses beyond 64
concurrent waiters with HTTP 429. Node tokens require an explicit lane currently
owned by that node, including read, wait, and ack operations. Operator tokens may list
or ack across lanes. The count path uses a partial index, reads no message body,
and neither reads nor acknowledges items. A zero count for a specified lane is
returned explicitly. Hub failures display as unavailable, never as zero.

The same node rule governs a lane's own jobs. `GET /v1/jobs/mine?lane=L` and
`POST /v1/jobs/reap` admit the operator for any lane, and a node credential
only for a lane routed to its own machine; a node naming another machine's
lane is refused `403 {"error":"lane_not_routed_to_caller"}`, and a service is
refused `403 {"error":"scope_forbidden"}` because no scope grants these
routes. `jobs close` additionally requires the job's owner lane to be L
(`403 owner_mismatch`). See [a lane's own jobs](jobs-own.md).

## Claude Code examples (not installed)

[Status line script](examples/claude-inbox-statusline.sh) prints `inbox: N unread`
to stdout. [Hook script](examples/claude-inbox-hook.sh) prints the same count as a
synchronous `systemMessage`; it never prints plain SessionStart output or
`additionalContext`, which would enter model context. The status line needs only
POSIX sh and panewire; the hook additionally uses Python 3 for JSON output.
Both use the current node's existing credentials and require `PANEWIRE_LANE`.

Example settings, after choosing absolute paths yourself:

```json
{
  "statusLine": {
    "type": "command",
    "command": "/path/to/claude-inbox-statusline.sh"
  },
  "hooks": {
    "SessionStart": [{
      "hooks": [{
        "type": "command",
        "command": "/path/to/claude-inbox-hook.sh",
        "timeout": 8
      }]
    }]
  }
}
```

These examples follow Claude Code's official
[status line contract](https://code.claude.com/docs/en/statusline) and
[synchronous hook JSON output contract](https://code.claude.com/docs/en/hooks#json-output).
Do not make the display hook async, because async `systemMessage` is delivered
as model context. Neither example injects through herdr. No example settings,
hooks, scripts, policy overrides or service changes are installed automatically.
