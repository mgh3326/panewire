# Node commands

Node commands let an operator tool read and act on a node's herdr panes and
git worktrees through the hub, instead of running `ssh <host> herdr ...` on
every session host. mergewake `reap` is the first caller: it lists agents,
inspects worktrees, captures a pane's output and closes the pane, all over
the connection each node already keeps open to the hub.

The hub keeps a pending command only in memory while the HTTP caller waits.
It never retries, persists, or acts on a result itself; a caller that times
out may re-issue, and every acting op re-checks on the node.

## Trust boundary

Both endpoints require the operator token, or a service token whose scopes
cover the call (`nodes.agents.read`, `node.command:<op>@<machine>`; see
[hub-scopes.md](hub-scopes.md)). `pane.read` and `job.done` are narrower
still: only a service token whose scopes name them is admitted, and the
operator token gets `403 scope_forbidden`. `job.mark_lost` is narrower again:
the hub issues it itself over the holder's connection when it releases a spawn
reservation, and the HTTP command endpoint refuses it for every caller. Nodes
need no extra opt-in: the hub can already type into any pane through
`relay.inject`, and every op here is narrower than that. Each op is a closed
shape; no op runs a shell, and no op takes a caller-chosen file path to write.
The one report path an op reads (`job.done`) must be one of two fixed files in
that job's directory. `job.close` and `job.reap` are issued by the hub after
its own ownership checks, and the HTTP command endpoint refuses them for every
caller, as it does `job.mark_lost`. The close operations
(`pane.close_op`, `pane.close_status`, `pane.close_release`) are hub-issued in
the same way.

## Task pane sweep

The node also closes ended jobs' panes by itself, without a hub command: see
[pane-sweep.md](pane-sweep.md). It proves the pane is the job's own by the
recorded pane id and harness session id, saves the last screen to hk as the
same screen document `pane.capture` uploads, then closes it. It does not run
`pane.close`'s status, composer, or `question_job` guards: the job's end plus
the session proof is the authority (see
[pane-sweep.md](pane-sweep.md#close)).

The hub's `pane.sweep` command runs a second, ended-job sweep on the hub's
schedule; see [pane-sweep.md](pane-sweep.md#hub-ordered-sweep).

## Hub endpoints

### `GET /v1/nodes/agents`

Fans `agents.list` out to every known node and answers one row per machine,
sorted by `machine_id`:

```json
{"nodes":[
  {"machine_id":"machine-a","state":"connected",
   "agents":[{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","agent":"claude","cwd":"/work/repo.feat"}],
   "panes":[{"pane_id":"w1:p1","agent_status":"idle","agent":"claude","cwd":"/work/repo.feat"},
            {"pane_id":"w1:p2","agent_status":"","cwd":"/work/other"}],
   "panes_ok":true},
  {"machine_id":"machine-b","state":"disconnected","agents":null,"panes":null,"panes_ok":false,"error":"node_unavailable"}
]}
```

A node that is not connected keeps its row with `error:"node_unavailable"`,
and one that does not answer within 10 seconds reads `error:"node_timeout"`,
so a caller never mistakes an unreachable node for one with no panes.

### `POST /v1/nodes/{machine}/command`

```json
{"op":"pane.close","args":{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","cwd":"/work/repo.feat"}}
```

`args` may be omitted for `agents.list`. Unknown ops, unknown or missing
argument fields, and invalid values are rejected with 400 before anything
reaches the node. The hub waits up to 30 seconds:

| Status | Body | Meaning |
| --- | --- | --- |
| 200 | `{"request_id","machine","op","result":{...}}` | The node answered. |
| 200 | `{"request_id","machine","op","error":"<code>","detail":"..."}` | The node answered with an error; detail is optional. |
| 400 | `{"error":"invalid_request"}` | Arguments rejected before dispatch. |
| 503 | `{"error":"node_unavailable","state":"..."}` | Not connected; nothing was sent. |
| 503 | `{...,"error":"node_lost"}` | The node disconnected while the hub waited. |
| 504 | `{...,"error":"node_timeout"}` | No answer in time; a late answer is dropped. |

A command needs only a live connection to the node. It does not require
`accepting` or a `connected` presence label: a draining node, or a `stale`
one whose heartbeat is late, still takes commands — reaping a draining node
is the common case — and the `state` the hub reports is that presence label.
A connection that has died unnoticed answers `node_lost` or `node_timeout`.

## Checkout pull

`checkout.pull` fast-forwards an allow-listed git checkout on a node and runs
its `install.sh` when `install/` or `bin/` changed. It is ordered through
`POST /v1/fleet/checkout-pull`, not this endpoint, which refuses it. See
[checkout-pull.md](checkout-pull.md) for the allow-list, refusal reasons, the
heartbeat revision and the deploy record.

## Ops

All validation is shared by hub and node. Except for the optional `worktree.list` cursor and limit, every listed argument is required
and must not be null — a zero value such as `write:false` or
`merged_after:0` has to be stated, never implied by omission. `pane_id` is
`^[A-Za-z0-9._:-]{1,128}$`; `name` is a single safe path segment (no `/`,
`\`, `..`, space, ASCII control byte, or leading `-` / `.`); `cwd` is an
absolute path. Every ref or branch name (`branch`, `base`, the branch in
`fetch`), `origin` and `regenerable` glob is refused as `invalid_args` if it
holds any ASCII control byte (below 0x20, or 0x7f) — checked on the value as
sent, before anything trims it.

### `agents.list`

`args: {}`. The node runs `herdr agent list` and `herdr pane list` and
returns `agents` and `panes` rows with herdr's own field names (`pane_id`,
`name`, `agent_status`, `agent`, `cwd`), so a caller that already decodes
`herdr agent list` reads them unchanged. `pane list` also names plain
shells; when it fails, `panes` is null and `panes_ok` is false, and a caller
must treat pane usage as unknown. A failed agent list answers
`herdr_failed`.

### `pane.close`

`args: {pane_id, name, agent_status, cwd[, allow_unnamed][, question_job]}` —
the caller's snapshot of the pane; `agent_status` must be `idle`, `done` or
`blocked` (a pane stopped at a prompt), never `working`. `question_job` names a
job whose own records the node must prove hold no unanswered question before it
closes: an idle pane that asked a question is waiting for an answer, not
finished. A `blocked` close also needs the job's own ledger
(`job.claimed` or `job.spawned` event) to name role `reviewer`, because a
blocked worker may be waiting on an ESC answer; otherwise the outcome is
`pane-changed`. herdr has no conditional close, so the node re-reads `herdr agent list`
twice, two seconds apart, and closes only when both reads show a row with
that `pane_id` and exactly that name, status and cwd, and — where herdr
reports it — the same `state_change_seq`, so a pane that went to work and
came back idle in between also survives. Each identity read is followed by a
fresh visible composer read. Both composers must be proven empty; draft text,
an unknown composer or a failed screen read refuses the close as `pane-changed`.
The second composer read immediately precedes `herdr pane close`; an unavoidable
read-to-close gap remains, as in the ssh reap path.

The hub sends `question_job` only to a node that advertised
`pane_close_question_v1`; against any other node it keeps the hold, because a
node that predates the field decodes the arguments strictly and would refuse
the whole call, and closing without the field would drop the guard. Every
caller that passes a job — the reviewer release, author escalation and the
quota-stop closes — takes that hold on an old node, so during a rolling
upgrade those releases wait for the node rather than close unguarded; callers
that pass no job are unaffected.

With `question_job`, the node makes one more decision at that same point, from
the node's own files rather than from a copy another process wrote earlier: it
reads the job's ledger for the pane and the start of its current attempt, then
the lane-event records (`<inbox>/events-lane`) that pane wrote since. A question
is open when the newest of those asks one — its `notify_type` is `decision` or
`blocked`, or its text starts with `ESC` — it is younger than the six-hour
`decisionTimeout`, and no `job.decision_resumed` follows it. A later lane event
from the same pane answers it, exactly as the watcher decides. The close is then
refused with `question-open`; an unreadable record answers `close_failed`, never
a close. A ledger whose newest spawned pane is another pane answers
`pane-changed`.

| `outcome` | Meaning |
| --- | --- |
| `closed` | Matched and `herdr pane close` succeeded. |
| `pane-changed` | Identity/status changed, or composer is not proven empty; not closed. |
| `pane-gone` | No such pane; nothing to close. |
| `question-open` | `question_job`'s newest lane event from this pane asks an unanswered question; not closed. |

A failed close answers `close_failed`. Acting ops run one at a time per node.

### Close operations

`pane.close_op` (hk 4594) is `pane.close` under a hub-chosen `request_id`
(`^[A-Za-z0-9_:-][A-Za-z0-9._:-]{0,127}$`, never a leading dot), a `job` and an
`attempt`: every argument of `pane.close` plus `request_id`, `job`, `epoch`,
`attempt` and `end_reason` (see "Hub close protocol"), all required
(`allow_unnamed` included).

`epoch` is the hub's ownership epoch for the job. The node refuses
`epoch_mismatch` before acting if it differs from its latest assigned epoch
(initially 1). After a node restart a new request at an epoch later than 1
waits for the assignment to be redelivered; an existing request recovers its
epoch from the synced intent or close journal. A durable hub revocation also
rejects that epoch and every older one after a restart. A registered request
records its epoch, which a replay must match.

**The attempt is the hub's decision.** `attempt` is an opaque token naming the
job attempt the hub decided to close. The hub reads it from `pane.close_status`
for that job (the `attempt` field, reported whether or not the operation
exists) at the moment it decides, and passes it unchanged. The node acts only
while that token is still the job's current attempt: a re-prompt, reclaim or
end recorded after the hub's decision makes the request `mismatch`, and a
resumed operation `preempted`. A repeat of a `request_id` must name the attempt
it was registered for (`invalid_args` otherwise, checked before any replay or
settlement). This is the contract the hub issuer's (hk 4745)
hub caller relies on. `job` names the job directory; `name` stays the
herdr agent label the close guards match, which a spawn may set apart from the
job id (`--job job-1 -l worker-a`). The blocked-reviewer guard reads the role
from the `job` ledger. The node journals the operation in that job's directory,
`jobs/<job>/close-ops/<request_id>.json` (mode 0600, written to a temporary
file, fsynced, renamed), against the job's current attempt (the newest claim,
spawn or re-prompt event), so its outcome survives a node restart.

Before any herdr call the node reads one proof from the job's own ledger, in
ledger (file-name) order: the current attempt (the newest claim, reclaim, spawn
or re-prompt event), the spawn receipt belonging to it (a claim or reclaim has
none until its own `job.spawned`), the receipt's pane, label, role and herdr
session, and whether the attempt has ended (`job.completed`, `job.completion`,
`job.record_settled`, `job.joined`, `job.revoked`, `job.handed_off`,
`job.escalate`, `job.reaped`, or a `job.lost` other than a quota stop announced
for the hub's hand-over). For an announced quota stop the job's hk record
decides: a terminal record means the hand-over grace already ended it, and an
unreadable record answers the error `record_unavailable`. Two answers open no
operation and close nothing: a `job` with no directory is `not-a-job`; a
request the proof does not bind (another pane or label, another attempt than
`attempt`, no receipt for the current attempt, a receipt that recorded no
`herdr_session`, or an ended attempt) is `mismatch`. A reviewer's open
`job.quota_stop_intent` asks hk the same way as an announced quota stop; one
the watcher cancelled as `superseded` (hk already terminal) is an end. Every herdr read and the
close run in the receipt's session, and the blocked-reviewer guard uses the
receipt's role (an empty role is unknown and refuses). The proof is read again
after the settle, before `closing` is written: an attempt that moved on or ended
meanwhile settles `preempted` and nothing is closed.

The journal moves `registered` → `closing` → one final state. `registered` is
written before any herdr read. `closing` is written after the first guarded
look and the settle, before the final look that immediately precedes
`herdr pane close`, so the journal write never sits between that look and the
close.

| Outcome | Meaning |
|---|---|
| `closed` | herdr reported the close done |
| `refused` | the pane no longer matched (`pane-changed`); it is still running |
| `gone` | the pane was absent before the close could run: it vanished on its own |
| `unknown` | the operation was found `closing` with no recorded result (a node restart or a failed herdr close); the close may have run, and the pane id may since name another pane |
| `cancelled` | released while still `registered`; the pane was never touched |
| `preempted` | a resumed `registered` operation whose attempt had moved on or ended; nothing was closed |

The answer is `{"outcome":"<state>","replayed":<bool>}`. A repeat of a
`request_id` whose state is final answers that state with `replayed:true` and
does not touch herdr, so a lost acknowledgement is recovered by asking again.
A repeat of one left `closing` answers `unknown`, also without touching herdr.
A repeat of one left `registered` re-checks the binding against the attempt it
was registered for, then resumes the guarded close or settles `preempted`. A
`request_id` names one attempt: after `refused` or `unknown`, a new attempt uses
a new id. Reusing an id for another pane is `invalid_args`.

`pane.close_status` (`request_id`, `job`) answers
`{"state":"none"|"superseded"|"intent_cancelled"|<state>,"released":<bool>,"settled":<bool>,"attempt":"<token>"}`, where
`attempt` is the job's current attempt token (empty when its ledger cannot be
read). `settled` proves a synced final `job.lost` closed this request's
intent; it also covers an older unknown intent closed by a later request on
the same attempt. A visible hk `lost` record alone does not prove settlement.
For an unknown, gone, or closed operation, `superseded` proves that a different
attempt is current. The node checks it under the job end lock and syncs the
events directory before answering. The operation journal stays intact: this
proof says no later hk write belongs to the old intent, not whether its pane
was closed.
It relies on the #662 `endMark` rule: a stale attempt's watcher decision is
dropped when rechecked under that lock (see the stale-decision rule below).
`intent_cancelled` proves under the same lock and sync that this request's
intent was cancelled. The operation journal remains unchanged. A cancelled
intent alone does not prove that its pane or watcher has stopped writing hk.

**The hub's acknowledgement** (hk 5073). `pane.close_status` also takes
`ack:true` (`{"request_id","job","ack":true}`; `pane.close_release` refuses
it). The hub sends it only for an action it has already stored `guarded`, so
the ack means "the hub durably stored your `intent_cancelled`": the node
records a synced `job.hub_close_acked` event for the request, once, and
answers the same status. An ack for a request that is not `intent_cancelled`
records nothing. The hub repeats it on every 10 s pass over the guarded action until the
node answers, and stores that the ack landed (`Acked`) so it asks no more. A
terminal hk record does not finish the guarded action before the node has
confirmed the ack: a node error, a lost command or a disconnected node leaves
it guarded and retried. Only a node that rejects the argument (`invalid_args`,
a build older than hk 5073, which never owed an ack) does not hold it.
A node that cancels a close in a rollback drain because the pane is still
present (`job.hub_close_cancelled`, reason `rollback_unproven`) marks the
event `hub_ack:true`: that is the one cancel the hub learns from this answer
alone, and the drain below waits for its ack. A cancel without the mark, such
as a ledger written before hk 5073, owes nothing. A settled close ends the
debt too: status then answers `settled`. A node of this build needs a hub of
this build, since an older hub never sends the ack.
`pane.close_release` takes the
same arguments, marks a final operation released (a `registered` one becomes
`cancelled`) and answers the same shape; an operation left `closing` stays
unreleased until a repeated `pane.close_op` settles it. All three ops take the
node's acting lock, and a visible journal state is fsynced before it is reported
or acted on. `journal_failed` answers a journal read, write or sync that failed;
the operation then stays as last recorded.

**Enabling close operations** (hk 4632). `close_ops_v1` is **off by
default**. A node advertises it, and admits a new close, only while
`fleet.CloseOpsReady` holds. A hub issues these ops only to a node whose
current registration advertises it. `CloseOpsReady` has three parts:

- **The migration barrier.** `panewire node close-ops enable
  [--inbox-root DIR]` is run once on the node host after its binary is
  upgraded. It records `jobs/.close-ops-enabled` only when no older watcher
  runs on the host (rc 5 otherwise, and nothing is recorded).
- **The advertise step.** `panewire node close-ops advertise
  [--inbox-root DIR]` records `jobs/.close-ops-advertise`, and refuses
  (rc 5) without the barrier. It is the explicit rollout step that turns
  `close_ops_v1` on. hk 4745's rollout runs it once the hub issuer is
  deployed; this PR's rollout does not.
- **A fresh process scan** (`fleet.HubCloseWatchersReady`): every live
  `_watch <job>` process of a job under the jobs root (whatever the
  executable is named) must be the job's aware watcher.
  - An aware watcher is one that holds
    `jobs/<job>/.panewire-watch-hubclose-v3.lock` for its whole life, and
    lends its watch lock (below).
  - A watcher of an older binary holds only `.panewire-watch.lock`, or that
    and the v1 or v2 lock. A v1 watcher lacks the stale-decision check below;
    a v2 one cannot lend its lock.
  - Kernel locks live exactly as long as their process, so a stale file or
    a reused pid certifies nothing.
  - Any listing or lock error fails closed.

Launchers (`spawn`, `work`, `restore`, `_spawn-stdin`, a daemon re-arm, a
renamed or retained binary) need no check of their own. **The node fences
the job**: while any hub-close intent of a job is open, the node holds that
job's `.panewire-watch.lock`. Every watcher of every binary takes that lock
first and exits when it is held.

- With no watcher running, the node takes the lock itself.
- With an aware watcher, the node borrows the watcher's own locked open file
  description over the watcher's unix socket, using SCM_RIGHTS. The socket
  is `jobs/<job>/.panewire-watch-sock/watch.sock`, or
  `/tmp/panewire-<uid>/<hash>/watch.sock` when that path is too long. Its
  directory is created 0700 before the socket binds, and is refused unless it
  is a real directory owned by the user with no group or other access. So no
  other local user can connect. flock stays held until every reference closes, so the watcher
  exiting or being killed mid-close cannot free the lock.

- The fence is reconciled under the job's end lock, which admission also
  holds. So a release can never drop the fence of a close admitted
  meanwhile, and a fence taken for an older, still-open intent survives a
  newer one that is cancelled.
- Reconciliation runs at admission, before and after every settlement
  step, and every 2 s for jobs with an open intent or a held fence. After a
  node restart, the fence is taken or borrowed again before settling.
- A settlement step runs only while the fence or an aware watcher holds the
  lock. It waits while a watcher that ignores hub closes holds it.
- Generic ends defer to an open close. `reap --apply`, the pane sweep and
  the abandoned-review end take the job's end lock and check for an open
  intent; they fail closed on an unreadable ledger. They keep the end lock
  through their hk write, pane close, lease release and `job.reaped`. The
  job-dir TTL keeps a dir whose intent is open (`hub-close-open`).
- A quota-stop announcement (`job.lost reason=quota_stop`, not final) takes
  the job's end lock too, then the operation lock, and writes nothing when
  the re-read ledger already holds another end (a revoke, a completion, a
  reap): as the newest end it would read as a live hand-over and mask that
  end. Both lock waits end when the caller's context does.
- When no intent is open any more (settled, cancelled, or made stale by a
  new attempt), the fence is released and the node re-arms the watchers of
  live jobs, since a watcher that met the fence exited.

The node re-checks readiness at every hello and every 30 s while connected,
including after a late hello acknowledgement. When the answer changes, it
sends a fresh hello (plus the heartbeat and relay burst a repeated hello
needs), so the hub's registration follows readiness both ways without a
reconnect. A hub of this build keeps the node's reported active jobs across
a hello on the same connection, so placement never sees its pools as free in
between.

Each new `pane.close_op` re-checks readiness too: an unready node refuses
`close_ops_not_ready` and journals nothing. An operation already accepted
still resumes and settles after a temporary readiness failure; the explicit
rollback drain below stops retries as well. Per close, the node
also refuses `watcher_incompatible` when the job's watch lock is held by
anything but an aware watcher. Both mean "not closed, retry later".

**Precondition: no old-binary panewire after the barrier.** Once a host's
barrier is recorded, no panewire binary older than this build may run there:
no watcher, launcher (`spawn`, `work`, `restore`, `_spawn-stdin`) or daemon.
The exclusion above covers watchers that are already running or that start
while a close is fenced. It cannot cover an old binary that starts while no
process holds the watch lock: during a node restart (until the restart pass
takes or borrows the fence again), or when every holder died while the node
was down. Do not intentionally restart the node while a close is open. If it
crashes, recover with this binary and keep older binaries excluded until the
open closes have drained.
**A rollback must first withdraw and drain close ops**, keeping the node on
the new binary running throughout the drain. Do not restart it for rollback
while any close is open: its process-held watcher fence must remain held.

1. Run `panewire node close-ops drain [--inbox-root DIR]` on the node host.
   Its admission barrier waits for closes already in flight to finish, then
   records withdrawal and removes both readiness markers. New closes and
   retries of registered closes are refused while the drain is active.
2. Repeat the same command until it prints `close ops drained; no pending
   jobs` and exits 0. Exit 5 lists pending jobs; the running node's settlement
   pass retries them. An unreadable ledger is pending, so repair storage and
   retry rather than treating an incomplete scan as an empty drain.
   Exit 5 also covers a cancelled close the hub has not acknowledged: it
   prints `waiting for the hub to acknowledge cancelled closes of jobs: [...]`.
   The hub acknowledges on its next 10 s pass (see "The hub's acknowledgement"
   above), so repeat the command; it succeeds once every acknowledgement is
   durable on the node. If a job stays listed, the hub is down, disconnected
   from this node, or its action for the request is gone: fix that, or
   keep the new binary running and use the manual check in step 3.
3. Keep the new node binary running for at least 30 seconds. If the inbox kill
   switch (`--delivery-policy off`) is active with a configured store, the
   hub does not open that store or emit inventory logs. Restore a non-off
   delivery policy using the same store and complete the checks below before
   rollback; missing inventory is not evidence that the store is empty.

   The drain's acknowledgement covers the `intent_cancelled` answer only.
   Every other close outcome the hub reads from the node (a refused, released
   or settled close) still needs the hub's own inventory check below.

   A hub configured with neither an inbox store nor a database URL creates no
   pool actions and emits no inventory; this assumes no store was removed with
   unresolved actions. Otherwise, there is no command to list pending pool
   actions by node. In the hub log, find a
   `pool close action inventory complete` line whose `started_at` is after the
   drain succeeded. For that line's `inventory` ID, verify that the number of
   distinct `pool close action inventory entry` lines matches its `count`;
   the entries enumerate the scan's snapshot of active actions, even
   after a hub restart. A slow node can delay the next scan, so do not treat
   30 seconds alone as proof that an inventory completed.

   For this node's entries, match each `action` to a `pool close action
   finalized` line for the same ID. Also match each `pool close action created`
   or `pool close action retry` line for this node after the scan began; retry
   lines include `retry_of`. An `outcome` of `guarded` means the close action
   is done but reassignment remains fenced. If any action or inventory entry
   cannot be accounted for, keep the new binary running.
4. Only then stop the node and replace the binary.

After withdrawal has quiesced admissions, settlement durably cancels an
unregistered intent or a registered operation, leaving a released journal
tombstone so a delayed retry cannot start it later. An interrupted `closing`
operation becomes `unknown`. If a `gone` operation's full pane inventory
shows the pane present, it can be cancelled: no close started. An `unknown`
operation can be cancelled only when its recorded harness session matches a
fresh agent row for the present pane. In either case the attempt returns to its
watcher without an hk lost write. If the pane is absent, settlement writes the
planned record. An unknown pane whose ownership cannot be proved stays pending:
inspect `jobs/<job>/events/*-job.hub_close_intent.json` and its
`jobs/<job>/close-ops/<request_id>.json`, then use the recorded herdr session
to inspect the pane. Wait for the original pane to disappear or, if its
identity is verified and it is safe to stop, close it before retrying; never
close a pane that belongs to another job. An
unreadable inventory or hk record, a failed journal write, or a failed
settlement also keeps the job pending until the service or storage is
available. A completed drain can be re-enabled by running `close-ops enable`
and then `advertise` again.

Rollout order: deploy the hub of this build and upgrade every node's
binary, then run `panewire node close-ops enable` on each node. Advertising
(`panewire node close-ops advertise`) waits for hk 4745's hub issuer.

#### Hub close protocol

A *hub close* is the hub ending a job by closing its pane with
`pane.close_op` (hk 4652). **The node owns an accepted close end to end:**
it binds and records the plan, closes, and then settles the job (hk
terminal record, quota lease, final ledger event). The job's watcher only
defers. Its rules, which the hub issuer (hk 4745) relies on:

1. **What is recorded before the close.** `pane.close_op` also takes a
   required `end_reason`: the hk `end_reason` of the `lost` record that ends
   the job (one printable ASCII line of at most 128 characters, the
   `job.mark_lost` bound). The planned terminal reason and any successor
   pointer live there, as the last `key=value` token
   (`replaced_by=<job>` for a reviewer, `handed_over_to=<job>` for an
   implementer; `QuotaStopEndReason`). A successor closing its parent
   (`supersede_close` rows, hk 5195, docs/config.md "--parent-job") uses
   `superseded_by=<job>`. On a new, bound registration the
   node appends `job.hub_close_intent` to the job's ledger — `request_id`,
   `attempt`, `pane_id`, `label` (the request's `name`) and `end_reason` —
   *before* it writes `registered`, and fsyncs the events directory. A
   failed intent write answers `journal_failed` and registers nothing. A
   replay or a `mismatch` records no intent. The binding check and the
   intent run under the job's end lock (`jobs/<job>/.panewire-end.lock`),
   which the watcher also takes around every terminal or expiry decision,
   re-reading the ledger under it; so no watcher end lands between the
   node's check and its intent. Before the intent the node also refuses
   `close_in_progress` when another operation for the attempt is still
   `registered` or `closing` (one open close per attempt), and
   `watcher_incompatible` when the job's watcher is an older binary that
   does not defer (it holds the watch lock but not
   `.panewire-watch-hubclose.lock`). A job with no watcher running is closed
   normally: the node settles it, and any watcher started later is of this
   binary and defers.
2. **Order.** Both the node and the watcher fold the ledger in sequence
   order (the numeric `<seq>-` prefix of the event file name,
   `fleet.CompareLedgerNames`), never by timestamp, so a clock stepped
   backwards cannot hide an intent. Unpublished dot files (`.tmp-*.json`)
   are not events.
3. **What the watcher does.** An intent is open while it names the job's
   current attempt and that attempt's receipt pane and label, and no
   cancellation of its own request, settlement (the node's final `job.lost`
   with reason `hub_close`) or new attempt follows it (an
   intent from an older attempt, pane or label is stale and ignored, the
   same binding the node checks). Any other local end of the attempt that
   follows the intent (`job.revoked`, a completion, another loss,
   `job.reaped`) leaves it
   open (#572 r4576225999): the node may already have closed the pane, so
   settlement decides from the journal. A `closed` operation is settled as
   usual: a live hk record takes the planned record, and another terminal
   record is kept. An operation never registered is cancelled (`ended`),
   since a repeat could no longer bind. A hub revocation record (a
   type-only `job.revoked` carrying the hub's `epoch`) means the hub
   reassigned the job: settlement releases this node's lease and records the
   final `job.lost` (`superseded`), but leaves the shared hk record to the
   new owner. While an intent is open the watcher
   writes no terminal record and releases nothing: completion from a
   report, the decision timeout, the deadline exit and the missing-pane
   fallback all wait. A decision the watcher took on a ledger snapshot is
   dropped when, under the end lock, the attempt changed or a hub close
   settled since that snapshot (#572 r4576226003), or when the attempt is
   already settled, folded in ledger order. A settled attempt also ends the
   watch, even during a quota hand-off. So a re-prompt or loss never follows
   the final `job.lost`. It never writes the hub close's record, never reads hk
   for it and needs no hk route of its own; a watcher that exits, or never
   ran, leaves nothing undone.
4. **Who writes the terminal record: the node.** Settlement
   (`fleet.HubCloseSettlement`, under the end lock, on the intent current
   under that lock — the newest one whose own request no cancellation
   closed, so a newer request that closed nothing hands the job back to an
   older unresolved one) reads the journal after fsyncing it:
   - The job's receipt (pane, herdr session) and reaped state are folded in
     the same ledger sequence order as the intent, never by timestamp.
   - `closed` proves the pane stopped. `gone` and `unknown` do only once a
     successful, strictly decoded `herdr pane list` in the receipt's herdr
     session shows the pane absent (a pane left as a plain shell is
     present); until then settlement stays pending, and the hub may open a
     new request id, which records a newer intent.
   - Then, through the node's own hk JobStore (spawn.json `handoffkeep_env`,
     the store `job.mark_lost` uses, never an inherited environment or
     `[quota].url`): a live record takes `state=lost` with exactly the
     intended `end_reason`; the identical record already there (the hub may
     write it with `job.mark_lost`) is accepted; any other terminal record
     is kept. The node then releases the job's quota lease (re-reading the
     pools still held) and records a final, fsynced `job.lost`
     (`reason: "hub_close"`, `final: true`, `request_id`, `end_reason`, and
     `superseded: true` when another record was kept). That event closes the
     intent and ends the watcher's watch.
   - `refused`, `cancelled` and `preempted` closed nothing: the node records
     `job.hub_close_cancelled` (`not_closed`) and the watcher resumes.
   - An intent with no operation on a job hk already ended is cancelled
     (`orphaned`): a repeat could no longer bind.
5. **When settlement runs (restart and liveness).** One step runs after
   every `pane.close_op` answer (a new close, a replay of a final one),
   every `pane.close_status` and every `pane.close_release`, and the daemon
   runs a pass over every job with an open intent at node start and every
   30 seconds (`runHubCloseSettle`). A job counts as pending from its intent
   event alone, before its journal directory exists. Each step is bounded
   (30 s, including the pane inventory read and the end-lock wait) and the
   daemon pass runs outside the node's acting lock. Settlement loads no
   `hosts.toml` or policy: it needs only the jobs root, the local quota
   ledger, the node's hk store and herdr. Every step is idempotent: a crash or
   failure between the hk write, the lease release and the final event
   leaves the intent open, and the next step repeats what is left. So an
   accepted close is settled while the node daemon runs, with no further
   hub command and no watcher; while the daemon is down nothing is
   settled, and nothing competes (the watcher still defers).
6. **Release and cancellation.** Release is hub bookkeeping; settlement
   acts on the final state alone, so an unreleased final operation is still
   settled. An operation left `registered` or `closing`, or an intent whose
   operation was never written (a node crash between the two), stays
   pending until the hub repeats the same `request_id` with the same
   arguments (a repeat settles `closing` as `unknown`, and registers or
   resumes a `registered` one) or releases it. Releasing a request the node
   never registered journals a released `cancelled` tombstone under its id
   and durably cancels its intent (`job.hub_close_cancelled`, `released`),
   and answers `cancelled`: a delayed duplicate `pane.close_op` with that id
   replays `cancelled` (or is `invalid_args` when no intent named its pane)
   and touches no pane. During an operator rollback drain, the node resolves
   interrupted operations as described under "Enabling close operations".

What the hub issuer can rely on from a node that advertises `close_ops_v1`:
the node writes the planned record (or keeps
another terminal record and still frees the lease) for every close it
accepted, while its daemon runs, without a watcher and without another hub
command; a watcher never writes a competing record while the close is open;
a `close_op`, `close_status` or `close_release` that succeeds runs one
settlement step inline, whose lock waits (the end lock and the job's events
lock) end with a 5 s context; one that fails (for example an end-lock wait
that expired) runs none and leaves it to the daemon pass. The bound covers
waits on locks, not the filesystem: an fsync of the job's journal or events
directory that stalls is not cancellable and can hold the acting lock until
it returns. The hub issuer in [hub_pool_action.go](../hub_pool_action.go)
persists each action before issuing it to `job.Node` at `job.Epoch`, and only
while that node advertises `close_ops_v1`. It reads the attempt from status,
sends the planned `end_reason`, and retries an unknown close under a new id
for the same attempt while retaining the original fence. A retry requires an
advertised node and a closable pane; retries after an uncertain or refused
successor back off from ten seconds to at most ten minutes. At node startup,
before advertising close support, the node recovers unfinished pre-epoch
journals under the close-admission lock: an unstarted operation is cancelled,
and a possibly executed close becomes unknown for normal settlement. A
timeout, unknown, or hk record read is not a final answer. Refused, preempted,
cancelled and released, settled, or superseded answers are final. Superseded
ends the old action without a notice and permits a later sweep of the new
attempt. `intent_cancelled` stops the close action without a notice or role
release but keeps its reassignment fence until hk is terminal or the node
reports synced settlement. A mismatch or `not-a-job` stays pending until release
provides a durable tombstone. `reassignJob` refuses while any action for the
job remains pending or guarded; a configured store error fails closed. An
inbox-less hub allows reassignment and logs that it cannot check close-action
finality. Do not remove an inbox store with unresolved actions: disabling it
discards visibility of their fences along with the other inbox state. A
settled action permits reassignment even before its notice is delivered. If the
hub's current ownership record has changed, that action releases no role and
sends no old notice. The store and reconciler resume after a hub restart
without a heartbeat. Tests in
[hub_pool_action_test.go](../hub_pool_action_test.go) and
[node_pane_close_epoch_test.go](../node_pane_close_epoch_test.go) exercise the
fence, terminal races, epoch, and restart rules.

Rollout order: deploy the hub with this guard, upgrade the node binaries,
run `panewire node close-ops enable` on each node, then run
`panewire node close-ops advertise` on each. Before any binary rollback, run
`panewire node close-ops drain` on every node and wait for it to report no
pending jobs; drain removes the advertisement and barrier markers. Keep the
new binary running during the drain and for at least 30 seconds afterwards;
check the hub's action inventory and `pool close action finalized` logs for
that node, as in the rollback steps above; the drain itself also waits for the
hub's acknowledgement of a cancelled close. Remove any remaining barrier marker
before rolling back. Do not advertise on a hub without the guard.

### `pane.capture`

`args: {pane_id, name, cwd, merged_after, write}`. Saves a pane's output into
its job directory `<inbox>/jobs/<name>` before a close, with the rules
mergewake reap applies over ssh:

| `outcome` | `saved` | Meaning |
| --- | --- | --- |
| `not-a-job` | — | The job directory does not exist, or is a symlink rather than a real directory: the fleet did not spawn this pane. Nothing is written. |
| `report` | `<dir>/report.md` | `report.md` is a regular file (not a symlink) whose mtime is strictly later than `merged_after` (unix seconds; 0 means never). |
| `would-screen` | `<dir>/pane-screen.txt` | `write:false`: where the screen would go. Nothing is read or written. |
| `screen` | `<dir>/pane-screen.txt` | The pane, re-read and still matching `name` and `cwd`, had its `herdr pane read` output written there atomically, mode 0600. The result also carries `doc` when the screen reached hk (below). |
| `pane-changed` / `pane-gone` | — | The pane no longer matches; nothing is written. |

`<inbox>` is the node daemon's jobs inbox root; a node without one answers
`jobs_root_unset`. Every read and write goes through an `os.Root` opened on
`<inbox>/jobs`, so no symlink at or below the job directory can lead outside
it; the screen is written to an `O_EXCL|O_NOFOLLOW` temporary file and
renamed over `pane-screen.txt`, which replaces a symlink there instead of
writing through it. A symlink above `<inbox>/jobs` is operator-provisioned
and honoured. Read or write failures answer `capture_failed`.

After a `screen` capture the node uploads the saved screen to hk as the
document `screens/<name>/pane-screen.txt`: kind `evidence`, job `<name>`,
`origin_class` `job`, and `task_id` when the job's ledger names a numeric hk
task (the newest claim's, else the spawn receipt's). On success the result
carries the key as `doc`, when the hub advertised `pane_capture_doc` (below):

```json
{"outcome": "screen", "saved": "<dir>/pane-screen.txt", "doc": "screens/<name>/pane-screen.txt"}
```

`doc` is additive and omitted when there was no upload, so every other result
is unchanged; the hub forwards it only on `screen` and only as the screen
document of the job `saved` names. A failed upload — hk unconfigured on the
node, a refusal such as hk's secret guard, an outage — is one node warning:
the local screen stays, the outcome stays `screen`, and `doc` is omitted. A
screen over hk's 512 KiB document cap is uploaded as its last bytes, cut on a
rune boundary, under a first line saying it was cut; the local file keeps the
whole screen. The upload runs inside the command's 20-second bound.

`doc` is gated on the connection, so a mixed-version rollout works in either
order. A hub before `doc` decodes a `pane.capture` result strictly and refuses
the whole result for an unknown field, which would leave the pane open. A hub
that understands `doc` lists `pane_capture_doc` in its `hub.features` event;
the node records that per connection and puts `doc` on the wire only on a
connection whose hub listed it. The upload to hk happens either way, so an
older hub gets the result it always got and the screen document still exists.

| hub \ node | node without `doc` | node with `doc` |
|---|---|---|
| hub without `doc` | `screen`, no upload | `screen` without `doc`; screen uploaded |
| hub with `doc` | `screen`, no upload; the node ignores the feature it does not know | `screen` with `doc`; screen uploaded |

### `pane.read`

`args: {pane_id, name, cwd, tail_lines}` — the caller's snapshot of the pane,
plus how many trailing screen lines to return (0 to 200; 0 returns none). The
node re-reads `herdr agent list`, and only a row with that `pane_id`, name and
cwd is read. Pane ids are host-local, so the same id on another node names
another pane; matching name and cwd keeps that pane's data on its node.

| `outcome` | Other fields | Meaning |
| --- | --- | --- |
| `read` | `agent_status`, `focused`, `composer`, optional `screen_tail` | The pane matched. |
| `pane-changed` | none | Present, but name or cwd differs. Nothing is read. |
| `pane-gone` | none | No such pane on this node. Nothing is read. |

`composer` comes from `herdr pane read <id> --source visible`:

| `composer` | Meaning |
| --- | --- |
| `empty` | The input line was located and holds nothing. |
| `has_text` | The input line holds text. A dim placeholder or a prompt suggestion reads as text too, because the plain-text read cannot tell them apart. |
| `unknown` | The screen could not be read, shows no composer, or the harness is not one of `claude`, `devin` or `codex`. |

Only `empty` means nothing is unsent. `screen_tail` holds at most 4 KiB of
valid UTF-8. A herdr row with no `focused` flag answers `herdr_failed`, so a
missing flag never reads as "not focused". A failed agent list also answers
`herdr_failed`.

### `job.done`

`args: {job, report}`. Runs the `panewire done <job> --report <report>` lifecycle engine in-process on the node
and answers panewire's last output line as `line`. `report` must be exactly
`<inbox>/jobs/<job>/report.md` or `<inbox>/jobs/<job>/pane-screen.txt` — the
paths `pane.capture` answers. panewire uploads the report it is given, so no other
path is accepted.

| `outcome` | `line` | Meaning |
| --- | --- | --- |
| `done` | panewire's line | panewire ran and completed the job. |
| `already-done` | the recorded line | This report already completed the job. panewire does not run again, or it answered `duplicate=yes`. |
| `not-a-job` | — | The job directory is missing or is a symlink. panewire does not run. |
| `report-invalid` | — | `report` is not one of the two paths, or is not a regular file and no done marker answers it. panewire does not run. |

A failed or timed-out panewire answers `done_failed` with optional `detail`: the last stderr line, redacted and bounded to 240 UTF-8 bytes. The command can be re-issued; a node
without a jobs root answers `jobs_root_unset`. A `done` that completed the job but left its report out of hk exits non-zero (`completed; report not in hk; rerun the same command`), so it answers `done_failed` too, and the re-issued command finishes the report ([completion receipts](config.md#completion-receipts)). The node records the report's
digest in `<inbox>/jobs/<job>/.node-job-done` after panewire succeeds. The digest
is the key because a released job id can be claimed again: a new report for that
id runs panewire, the same rule panewire's own duplicate check applies.
`panewire done` writes the same marker itself when it deletes the report after
the pin (hk 3685), so a `job.done` for a `<dir>/report.md` that is gone answers
`already-done` with the recorded line rather than `report-invalid`. A claim,
spawn or reprompt recorded after the marker starts a new round of that job id,
and the marker does not answer for it.

### `job.mark_lost`

`args: {job, pane_id, label}` — at least one of `pane_id` and `label` is
required. An optional `end_reason` (one printable ASCII line, at most 128
characters) is written with the record: `{"state":"lost","end_reason":...}`. The
hub sets it when it replaces a quota-stopped job (`replaced_by=<job>`,
[quota-stop.md](quota-stop.md)); a node older than this field refuses it, so
deploy nodes first. The hub issues this op itself when it releases a spawn reservation
whose holder ran on another host: the hub's handoffkeep client is not granted
that host, so the holder's node writes the record with its own credential
(`spawn.json` `handoffkeep_env`). The HTTP command endpoint refuses the op for
every caller, and a node credential is refused as for every other op.

The node first reads its local job ledger for the attempt named by `pane_id`
and `label` — the newest `job.spawned` receipt carrying that pane, else the
newest claim carrying that label. A `job.completed` event after that attempt,
or a claim newer than it, refuses, because the record has moved on. An earlier
released-and-reclaimed cycle's events say nothing about this attempt and do not
block it. The node then re-reads `herdr agent list` and `herdr pane list` and
refuses when either list still names `pane_id` or an agent row still names
`label`; a failed pane list refuses a pane check as unknown, while a label-only
check needs only the agent list. Only when both proofs hold does it write
`{"state":"lost"}` to handoffkeep.

`end_reason` `reviewer_result_posted` changes the proofs (hk 4684, see
[hub-spawn-operations.md](hub-spawn-operations.md#ending-a-reviewer-job-that-posted-a-result)).
The reviewer's own result stands in for the pane proof, because its pane is
still open when the hub ends the job and the node's ended-job sweep closes it
afterwards. The node takes the job's operation lock without waiting (the lock
`panewire done` and the watcher's quota-stop records hold; a held lock answers
`operation-busy`) and, under it, applies the `job.completed` and newer-claim
checks and reads the hk record: a record that is not `claimed` or `spawned`
answers `completed`, except a `lost` one carrying this `end_reason`, for which
the node only appends the ledger event it may be missing. For a live record it
refuses unless: the ledger has not already ended another way (`job.reaped`, `job.handed_off`, `job.joined`, `job.revoked`, `job.escalate` or another final `job.lost`: answers `completed`, because that end's own hk write may still be pending); the ledger names role `reviewer`; no quota stop is announced
(`job.lost` `quota_stop`, not final) or pending (`job.quota_stop_intent`); no
listed agent with that pane id and label is `working` (an unreadable agent list
fails the command); and the job directory holds a findings object that is not
JSON `null` or a `report.md`, the files unchanged between a look before and a
look after the node reads the result, and the newest of them unchanged for 10
minutes. Only then does it write the hk record lost and, after that, append a
final `job.lost` event (reason `reviewer_result_posted`) to the job's ledger,
which the ended-job pane sweep reads ([pane-sweep.md](pane-sweep.md)); a ledger
that already ended is not given a second event.

| `outcome` | Meaning |
| --- | --- |
| `lost` | The record was written lost. |
| `pane-listed` | herdr still lists `pane_id`; nothing is written. |
| `label-listed` | an agent row still carries `label`; nothing is written. |
| `panes-unknown` | `herdr pane list` failed, so a pane check proves nothing. |
| `completed` | the local ledger holds a `job.completed` event; nothing is written. |
| `newer-claim` | the local ledger holds a `job.reclaim` event; nothing is written. |
| `ledger_missing` | the job dir or its `events/` is gone, and the node cannot prove that no attempt of the job is live; nothing is written. |
| `no-live-attempt` | the job dir or its `events/` is gone, and the node's spawn journal and active-job list name no live attempt of the job under any label; nothing is written. |
| `result-missing` | `reviewer_result_posted` only: the job directory holds no findings object and no report; nothing is written. |
| `result-settling` | `reviewer_result_posted` only: a result file changed less than 10 minutes ago or while the node read it; nothing is written. |
| `pane-working` | `reviewer_result_posted` only: the job's pane is `working`; nothing is written. |
| `operation-busy` | `reviewer_result_posted` only: the job's operation lock is held (`done` or a quota-stop record in flight); nothing is written. |
| `quota-stopped` | `reviewer_result_posted` only: a quota stop is announced or pending, so the hub's hand-over owns the record; nothing is written. |
| `not-reviewer` | `reviewer_result_posted` only: the ledger names no `reviewer` role; nothing is written. |

A missing job dir or `events/` is a ledger that is unknown, not an empty one, so
it answers `ledger_missing` and the node never writes `lost` over a record it
cannot check. It does not free the caller either: a job id can be claimed again
under another label, and a bare missing ledger cannot rule that attempt out. The
node reads its own spawn journal and active-job list instead, and answers
`no-live-attempt` only when both are readable and neither names the job under
any label — an executing journal row is a spawn this node is starting, and an
active entry is a job whose ledger exists again. Only that proof lets the hub
free a failed no-pane holder's reservation (hk 4799, then
[hub-spawn-operations.md](hub-spawn-operations.md#acceptance-and-identity)). A
journal or jobs directory the node cannot read is no proof, so the record and
the reservation stay. A spawn journal row written before the strict identity
existed names no job the node can trust; while one is still executing the node
answers `ledger_missing` for any job, not only the job the row names, because
its real job cannot be re-derived. Restart settlement completes such a row as
soon as herdr proves the spawn landed or is gone; while the evidence keeps that
undecided the row can outlive the upgrade, and the hold with it (hk 4799).
The `reviewer_result_posted` end keeps answering
`ledger_missing` for a missing ledger: it ends a reviewer's job, which holds no
reservation (hk 4737). A failed ledger read or handoffkeep write answers
`mark_lost_failed`; a node without a jobs root answers `jobs_root_unset`.

### `job.reap`

`args: {job, lane, force, expect?}` — `job`, `lane` and `force` are required,
and `force:false` must be stated. `expect`, when present, must be a closable
status (`idle`, `done` or `blocked`); a node that predates it refuses the
frame as `invalid_args` without acting. This is the node half of
`panewire jobs close` ([jobs-own.md](jobs-own.md)): the hub has already
checked that the caller's lane owns the job, and the node re-proves it from
the job's own ledger and herdr before it closes a pane it did not spawn in
this process.

The node reads `<inbox>/jobs/<job>/events` and takes the owner lane from the
newest claim, and the pane, label and keep flag from the `job.spawned` receipt.
It then re-reads `herdr agent list` and requires the row for that pane id to
carry the receipt's label; a pane id is host-local and reused, so a different
label is another pane. `pane.close`'s settle applies to a non-forced close:
two matching reads one interval apart, an unchanged `state_change_seq` where
herdr reports one, and an empty composer both times. A forced close carrying
`expect` re-reads the verified pane just before closing and refuses when the
pane is no longer as proven: `working` reads as `job-working`, and any other
unexpected non-finished status as `pane-changed`. A finished pane (`idle` or
`done`) stays closable whatever was expected. The recheck narrows the race to
the close call itself: herdr's `pane close` takes only the pane id, so a pane
answered between the recheck and the close is still closed.

| `outcome` | `pane` | Meaning |
| --- | --- | --- |
| `closed` | the closed pane | Matched and `herdr pane close` succeeded. |
| `pane-gone` | the receipt's pane, when it names one | No such agent pane; nothing to close. With no pane, the ledger names none and the node verified nothing. |
| `pane-changed` | the receipt's pane | The pane carries another label, is in no closable status, its composer is not proven empty, or a forced `expect` recheck found it no longer as proven and not finished; not closed. |
| `job-working` | the receipt's pane | The pane is `working`; plain `force` closes it, but a forced close carrying `expect` refuses. Not closed. |
| `pane-blocked` | the receipt's pane | The pane waits at a prompt; `force` closes it. Not closed. |
| `protected` | the receipt's pane | The job was spawned with `--keep`; never closed, whatever `force` says. |
| `owner-mismatch` | the receipt's pane | The ledger's owner lane is not `lane`; not closed. |
| `not-a-job` | — | The job directory is missing or is a symlink; the fleet did not spawn it here. |

A node without a jobs root answers `jobs_root_unset`, a failed ledger read
answers `reap_failed`, and a failed agent list or close answers `herdr_failed`
or `close_failed`. The op takes the node's one-at-a-time acting lock, so it
cannot interleave with `pane.close`, `job.done` or `job.mark_lost`.

A node older than this op refuses it as `invalid_args` without acting; deploy
the nodes before the hub for `panewire jobs close` to work end to end.

### `pane.sweep`

`args: {age_s}`, an integer from `0` to one year in seconds (`365*24*60*60`).
The hub's ticker sends it (see [pane-sweep.md](pane-sweep.md#hub-ordered-sweep));
no `node.command` caller may issue it, and the public endpoint refuses it.

The node runs its sweep with `age_s` as the close deadline (see
[pane-sweep.md](pane-sweep.md#timing)): the pane of every job whose end lies at
least `age_s` seconds ago — and any pane past its idle grace — is proven the
job's own by pane id and harness session id and closed, whatever herdr shows
it doing. An implementer's pane still waits while its worktree is on an open
PR, and a `--keep` pane waits out its day. It replies `{"closed": [...]}`,
one `{job, pane, role, reason}` per pane closed, with `reason` `job_revoked`,
`job_completed`, `job_handed_off`, `job_lost`, the job's task end state, or
`pane_gone` when the pane had already vanished.

A node without a jobs root answers `jobs_root_unset`. A failed read of the
jobs, the agent list, or the hk record answers `sweep_failed`. An out-of-range
`age_s` answers `invalid_args`. The op does not take the node's acting lock:
each pane's close takes it, so the op never holds it across the whole pass.

### `checkout.sweep`

`args: {dry_run}`. The operator's stale-checkout pass (hk 5091; see
[pane-sweep.md](pane-sweep.md#checkout-sweep)): it lists every `*-checkout`
directory under the node's work root that is a linked worktree of one of its
clones, attributes each to a job — a readable job record's receipt cwd, then
the spawn journal for a record that is gone — and removes each non-live
job-owned checkout through the pane sweep's guarded worktree path. `dry_run`
reports without changing anything.

It replies `{"dry_run":bool,"checkouts":[...]}` with one
`{path, job, outcome, reason}` row per candidate in path order: `outcome` is
`removed`, `kept` or `would_remove` (dry run only); `reason` is the keep
reason (`job_live`, `pane_open`, `agents_unreadable`, `no_job`,
`job_unreadable`, or the tail's own such as `task_open_kept`, `shared_kept`,
`symlink_kept`, `patch_failed_kept`), `discard` on a `would_remove` row, or
empty. A
candidate that cannot be judged is kept with its reason; the pass never
removes blind.

The op holds the node's acting lock for the whole pass — the removals are git
mutations that must not interleave with another acting command — and runs
under its own 90-second budget, longer than the default command timeout. A
node without a jobs root or work root answers `jobs_root_unset` or
`work_root_unset`; a failed pass answers `sweep_failed`. The CLI is
`panewire node sweep-checkouts --host <machine> [--dry-run]`.

### `worktree.handover_snapshot`

The old node's half of the hub's implementer hand-over (hk 4633,
[quota-stop.md](quota-stop.md) "An exhausted pool"). Arguments
`{"job","cwd","branch","snapshot"}`; offered as `handover_snapshot_v1` with
`close_ops_v1`. From the top level of the worktree at `cwd` (which must be on
`branch`) the node reads the branch's last pushed commit with
`git ls-remote origin`, or the merge-base with origin's default branch when
nothing was pushed. Without `snapshot` it only reads. With `snapshot` it
commits the worktree's tracked changes (staged new files included, untracked
files never) through a temporary index copied from the user's, on top of
HEAD (the user's index is untouched), lists the untracked files (`untracked`,
at most 50, `untracked_more` counts the rest) and pushes the commit to
`refs/panewire/handover/<job>` on origin (no tags, no submodules, read back
with ls-remote), and writes `jobs/<job>/handover-keep`, which keeps the
worktree from every sweep. Result: `outcome` (`saved`, `not-a-job`,
`branch_mismatch`, `git_failed`, `diverged`), `branch`, `top`, `pushed`,
`base`, `base_ref`, `dirty`, `commit` and `ref` (when a snapshot was pushed),
`submodule`, `untracked`, `untracked_more`, `kept`.

### `worktree.inspect`

`args: {cwd, expected_dir?, verify_branch?}`. Returns `{"worktree":null}` unless `cwd` is inside a linked git
worktree (an ordinary directory, a bare repository, a clone's canonical
checkout, or a path git cannot read is null). The node asks git for absolute
paths (`rev-parse --path-format=absolute`, git 2.31 or later) rather than
resolving a relative common dir itself; an older git reads as null.
Otherwise:

```json
{"worktree":{"path":"/work/repo.feat","branch":"feat/x","head":"<sha>","origin_url":"https://github.com/owner/name.git","common":"/work/repo/.git"}}
```

`branch` is empty on a detached HEAD. Credentials are dropped from
`origin_url` on the node: a URL remote loses its userinfo, query and
fragment, and an scp-style remote (`[user[:secret]@]host:path`) loses
everything up to its last `@` before the first `/`, so a token embedded in
either form never leaves it.

When `expected_dir` is present, the resolved checkout must equal
`<work_root>/<expected_dir>`; a mismatch returns a null worktree and
`check:"mismatch"`. With `verify_branch:true`, `check` is `ok`, `dirty`,
`locked`, `unpushed`, or `unreadable`. The node reads Git status and the remote
branch without fetching or changing the checkout; an origin tip whose ancestry
cannot be verified locally is refused as `unpushed`.

### `worktree.list`

`args: {common, cursor?, limit?}`. `common` is an absolute path to the common
Git directory or main checkout. `limit` is 1 through 100 and defaults to 50.
The node sorts worktrees by path and returns `worktrees`, `total`, and
`next_cursor`. Each row has `path`, `branch`, `head`, `detached`, and `status`.
Status contains `dirty`, `untracked`, and `ignored`; `error` is present when
status cannot be read. Dirty includes tracked changes and untracked entries.
Ignored and untracked counts follow Git's porcelain output, including collapsed
directories. Missing worktrees remain in the list with a status error.

A cursor is valid only for the same repository snapshot and node process. If
Git's worktree list changes or the node restarts, start again without a cursor.
The node may return fewer than `limit` rows to keep the result under 28 KiB.
The `worktree.inspect` scope authorizes this operation on the same machines.
Status uses Git with optional locks disabled and refuses symlinked paths.

`panewire node worktree-list --hub-url <url> --hub-token-env <env> --host <machine> --common <path> [--all]`
prints one JSON object per worktree. Without `--all`, it prints the first page;
with `--all`, it reads every page. The optional `--hub-cf-env` flag supplies
Cloudflare Access credentials.

### `pr.status`

`args: {repo, pr, fresh}` — all three required. `repo` is `owner/name`
(`[A-Za-z0-9._-]`, each segment at most 100 bytes, no `.` or `..` name, no
`.git` suffix), `pr` is an integer from 1 through 2147483647, and `fresh` asks
mergewake for its one GraphQL read. The op is read-only: it takes no acting-command lock
and writes nothing. It reads two sources and answers each with its own closed
outcome, so a source that is down never fails the op; see
[pr-status.md](pr-status.md) for the CLI.

With `fresh: false` the node reads hk first. When hk's record has every section
fresh enough (`hk.outcome: "ok"` with a `snapshot`), the node answers from it alone
and never runs mergewake (`mergewake.outcome: "not_asked"`); otherwise it runs
mergewake too. With `fresh: true` it runs mergewake first, then reads hk.

The hub admits `pr.status` for exactly one machine, the one named by its
`--pr-status-host` flag (default `oci`); any other machine answers
`400 {"error":"wrong_machine"}` before a node sees the request, whatever the
caller's scope.

The node reads `GET /v1/changes/one?repo=<repo>&pr=<pr>` from the handoffkeep
named by its `spawn.json` `handoffkeep_env`, at most 5 seconds and 256 KiB, and
runs `mergewake pr-status --repo <repo> --pr <pr> --json [--fresh]` with a closed
environment, at most 12 seconds and 64 KiB of output, when it needs it.

```json
{"mergewake":{"outcome":"ok","snapshot":{"repo":"owner/name","pr":152,"head":"…","gate_head":"…","state":"blocked","reasons":["threads_open:1"],"checks":[{"name":"test","state":"green"}],"review":{"passes":[{"pass":1,"lens":"correctness","family":"openai"}],"done":true},"threads":{"blocking":1,"other":1,"unresolved":2},"findings":[{"id":"PRRT_…","location":"path:42","severity":"high","blocking":true,"summary":"…","url":"…","author_replied":false}],"base_fresh":true,"observed_at":"2026-11-06T00:12:06Z","fresh":null,"fresh_at":null,"source":"last_pass"}},
 "hk":{"outcome":"not_found","status":404}}
```

| Source | `outcome` | Meaning |
| --- | --- | --- |
| `mergewake` | `ok` | `snapshot` is set. |
| | `not_found` | mergewake has no snapshot for this PR. |
| | `not_asked` | hk answered a read without `fresh`, so mergewake was not run. Only with `hk.outcome: "ok"` and a `hk.snapshot`. |
| | `failed` / `unavailable` | No snapshot. `reason` is one of `not_installed`, `timeout`, `exit_status`, `unreadable_output`, `output_too_large`; mergewake's own error text is never forwarded. |
| `hk` | `ok` | Every section the snapshot needs is fresh. `change` is a bounded projection of hk's record (`change`, `gate`, `freshness`; at most 4 KiB, no thread excerpts). `snapshot` is the record in mergewake's snapshot shape with `source: "hk"` and a per-section `sections` map of `observed_at` and `source`; the node leaves it out when mergewake's own snapshot answers the same call. `unposted_findings` counts open hk findings that have no review thread. |
| | `stale` | hk holds a record that cannot be trusted. `stale` lists each section with `section`, `observed_at`, `source`, `problem` (`missing` or `older_than_bound`) and `bound_seconds`. No `snapshot`. |
| | `not_found` | hk answered 404: it has never seen the PR, or it predates `/v1/changes/one`. |
| | `unconfigured` | The node has no usable `handoffkeep_env`. |
| | `too_large` | The response exceeds 256 KiB; it is not truncated. |
| | `failed` | `reason` is `http_status`, `request_failed` or `unreadable_body` (which includes a record about another PR). |

mergewake's JSON is read leniently, so a field it adds later is dropped rather
than failing the op, and the node bounds every list and string (50 checks, 20
reasons, 10 passes, 50 findings of at most 240 bytes of summary) so the result
fits the reply cap. When the findings still do not fit, the node drops them from
the tail and reports the count as `findings_omitted`. The environment the
mergewake child receives is `PATH`, `HOME`, `USER`, `LOGNAME`, `LANG`, `LC_ALL`,
`TMPDIR` and `XDG_CONFIG_HOME`, `XDG_STATE_HOME`, `XDG_DATA_HOME`; no credential
of the node's own is passed on, and no credential appears in argv or in the
result. The scope is `node.command:pr.status@<machine>`. A node credential
(a machine's own `HUB_MACHINE_ID`/`HUB_TOKEN`) may also run this op, on the
PR-status host only and with no scope: it is how the local node daemon asks on
a worker's behalf ([pr-status.md](pr-status.md#transport)). A node credential
is refused every other op on this route.

### `worktree.prepare`

`args: {origin, mode, branch, sha, fetch, dir, exclude?, include?}`. Prepares a
worktree of a
clone under the node's work root — set in the node's own `spawn.json`
`work_root`, never by the caller — with the clone at `<work_root>/<repo>`
and the worktree at `<work_root>/<dir>`. `dir` is a single safe path
segment, `origin` carries no userinfo (URL userinfo, or an `@` outside an
absolute local path, is refused; an scp-style `host:owner/repo` needs a
host of at least two characters, since a one-character host such as
`C:owner/repo` is drive-letter shaped), `sha` is 40-hex, and `fetch` is a
closed shape: `refs/heads/<branch>` or `pull/<n>/head`.

- `mode:"branch"` — panewire#18's branch behaviour on the node. `branch` is
  required, `fetch` must be stated empty (the whole remote is fetched), and
  `sha`, when given, names the expected tip. A worktree already at `dir` is
  reused only when it is on that branch and clean; the branch checked out
  elsewhere and a `dir` parked on another branch refuse. A clean local branch
  behind the expected tip is fast-forwarded, including its worktree when reused;
  an ignored local file that would be overwritten is treated as dirty, and
  an ahead or diverged branch refuses. Without `sha` the expected tip is the
  freshly fetched `origin/<branch>`, which must exist. A missing local branch
  is created tracking `origin/<branch>`.
- `mode:"detached"` — `sha` and `fetch` are required and `branch` must be
  stated empty. The node runs `git fetch origin <fetch>` and requires `sha`
  to be a commit that is `FETCH_HEAD` or an ancestor of it (a ref that
  contains the sha; otherwise `sha-missing`). It then runs `git worktree add
  --detach <dir> <sha>`, so the worktree sits at `sha`, never at the fetched
  tip. An existing worktree is reused only when detached at exactly that
  commit and clean. `exclude` and `include` are accepted in this mode only
  (hk 5098): each entry is a plain path or pattern — one that starts with `!`
  or `/`, or contains `..`, a NUL or a newline, is `invalid_args`; at most 64
  excludes and 500 includes, each at most 4096 bytes, 128 KiB together. With
  either list
  present the node adds the worktree with `--no-checkout`, feeds its own
  pattern list to `git sparse-checkout set --no-cone --stdin` on stdin
  (`/*`, then `!/<exclude>`, then `/<include>` with a literal path's glob
  characters and trailing spaces escaped), and runs `git checkout --detach
  <sha>`. A non-sparse request that finds a sparse worktree at `dir` runs
  `git sparse-checkout disable` first, so a reuse reported as `full` really is. A failure after
  the add removes the half-made worktree the same forced way the
  interrupted-add cleanup does. A node whose git is older than 2.37, or one
  that never advertised `checkout_sparse_v1`, prepares the full checkout
  instead; `checkout_mode` says which happened.
- `mode:"successor"` — an implementer spec spawn on `refs/heads/<branch>` (hk
  5118). `branch` and `sha` are required, `fetch` must be stated empty (the
  whole remote is fetched), and the sparse lists are refused. The node fetches,
  then requires the freshly fetched `origin/<branch>` to be exactly `sha`:
  someone pushed after the brief and a successor must not start behind it, so
  it refuses `successor_stale` with `reason` `want=<sha> origin=<sha>`
  (`origin=none` when the branch is not on origin) before anything is
  prepared. A worktree of the clone that already holds the branch refuses
  `branch_in_use` with `reason` `worktree=<dir>`; the branch is never forced or
  stolen. A worktree at `dir` is reused only on that branch, clean, at the
  commit; a local branch behind the commit is fast-forwarded and one ahead or
  diverged refuses. The prepared worktree sits on the local branch, which
  tracks `origin/<branch>` so a plain `git push` pushes it, and the result
  carries `checkout_mode: branch`.

A hub-staged author replacement sets `verify_reuse` on its spawn checkout.
The node then requires an existing clean, unlocked checkout at exactly the
pinned SHA and an origin branch containing it; it never fast-forwards or
creates that worktree.

A clone that already exists keeps its identity: a `git remote get-url
origin` that decodes to a different `owner/repo` than the argument — a fork
sharing the name — is `origin-mismatch`; one that cannot be read at all is
refused the same way. The clone's own remote may carry a user: an scp-style
remote is split at its first `:`, only the host side before it is read as
`[user@]host`, and the path after it is kept whole, so
`git@host:owner/re@po.git` decodes to `owner/re@po` rather than being
refused. A missing clone is created with `git clone <origin>`.

| `outcome` | Meaning |
| --- | --- |
| `prepared` | The worktree was created; `worktree` describes it. |
| `reused` | The existing worktree at `dir` matched or was fast-forwarded. |
| `busy` | Another git op holds the per-clone lock; `retry_after_s` is set. |
| `cancelled` | The caller's context was cancelled while waiting for the per-clone lock; the lock was never taken, nothing ran, and there is no `retry_after_s`. |
| `branch-elsewhere` | The branch is checked out elsewhere, `dir` sits on another branch/commit, or the branch is not on origin where required. |
| `successor_stale` | A successor checkout's freshly fetched `origin/<branch>` is not the commit the request names; `reason` carries `want=<sha> origin=<sha>`. |
| `branch_in_use` | A successor checkout's branch is held by another worktree of the clone; `reason` carries `worktree=<dir>`. |
| `sha-missing` | The fetched ref does not resolve to `sha`, or the commit is not in the clone after fetching. |
| `origin-mismatch` | The existing clone tracks a different owner/repo. |
| `dirty` | The existing worktree at `dir` has uncommitted or untracked files. |
| `locked` | The existing worktree at `dir` is locked (`git worktree lock`); it is never reused. |
| `git-failed` | A git step failed. |
| `git_too_old` | The node's git is older than 2.31, the floor `worktree.inspect` documents. |

When a branch is fast-forwarded, the result includes `fast_forwarded: true`.
A tip mismatch that cannot be fast-forwarded includes `reason` with
`local=<sha7> expected=<sha7> relation=ahead|diverged`.
A detached prepare includes `checkout_mode`: `sparse` when it applied the
request's list, `full` when the whole tree is on disk (hk 5098). A successor
prepare includes `checkout_mode: branch` (hk 5118).

An add interrupted by a crash leaves its worktree locked `initializing`. The
node records the process group of the `git worktree add` child in the clone's
per-worktree intent file
(`.git/panewire-worktree-add-intent-<sha256 of the worktree path>.json`), and a
later prepare for that worktree reclaims it only once that group — git and the
checkout hooks it started — is gone, so the answer stays `locked` while the add
or a hook it started runs in that group. A checkout hook that deliberately
daemonizes with `setsid` leaves the group and is not covered by this check: the
node can reclaim the path while such a hook still writes into it. An intent
written before the node recorded add groups has none, so it is never reclaimed
automatically: the node logs `node worktree add intent records no add process
group` with the worktree path. Before recovery the operator confirms that no
`git worktree add` process and no checkout hook is still running for `<dir>`; a
clean tree does not show this. Then the operator runs `git -C <clone> worktree
unlock <dir>` and the `worktree.remove` op, or `git -C <clone> worktree remove
<dir>` directly once the tree is clean.

Every op that touches one clone (`worktree.prepare`, `worktree.remove`)
takes the exclusive per-clone lock `<clone>.git-op.lock`, waiting at most
60 s before answering `busy`. Both ops name `<clone>` the same way: the
clone path with symlinks resolved, or — before prepare has cloned it — its
resolved parent joined with the clone's name, which is what resolution gives
once it exists. A clone path that exists but cannot be resolved (a dangling
symlink, an unreadable component) answers `git-failed` without taking any
lock, so one clone is never locked under two names. A wait that runs out —
the 60 s budget or the node's own run deadline — answers `busy`; a caller
whose context is cancelled answers `cancelled` instead, and a caller already
cancelled never takes the lock. A node with no configured work root answers
`work_root_unset`.

### `worktree.remove`

`args: {cwd, expect_head, base, regenerable, detached?, disposable?}` — `cwd` inside the linked
worktree, `expect_head` the 40-hex commit it must still be on, `base` the
merged-into branch (a `regenerable` pattern must contain a literal
character: wildcard-only patterns such as `*` are invalid_args; a glob with a
`/` anywhere matches the whole path; a wholly ignored directory, reported by git as one `dir/` entry, is approved only by a pattern ending in `/`), and `regenerable` an allowlist of ignored paths that
may be deleted with the worktree (a glob containing `/` matches the whole
path, a slash-free one matches the last segment). The node re-resolves the
worktree under the per-clone lock, then removes it only when all hold: the
resolved worktree path and common git dir are under the node's configured
work root, it appears in `git worktree list`, is not locked, is not
detached, is clean, every ignored file matches `regenerable`, the branch is
merged into `origin/<base>` or fully pushed to `origin/<branch>` (both judged after a
`git fetch --prune origin`, whose failure answers `git-failed`; a local `base` ref is never proof, and a
`base` equal to the branch never counts), and — re-read immediately before
removal — no pane is live in it (the ignored-file scan and the pane check are the last reads; a non-pane process writing an ignored file between them and the removal is a residual window this op cannot fence). It then runs `git worktree remove` and
`git branch -d`; never `--force` or `-D`.

`disposable` is an optional list of exact basenames at the worktree root (at
most 16 entries; each entry must be a non-empty exact basename with no `/`, no
glob characters `*?[`, not `.` or `..`, at most 255 bytes). Any invalid entry
rejects the command with 400 `invalid_request` naming the entry index.
Untracked files (porcelain `?? `) whose path is exactly one of those basenames
at the worktree root do not count as dirty, and are deleted before `git worktree
remove` runs. Tracked changes, other untracked files, disposable names in
subdirectories, and untracked directories still cause `dirty`.

`detached` is an optional boolean, defaulting to false. When true, the
worktree must be detached and its resolved directory must be a direct child
of the work root named `mwr-` followed by one or more lowercase letters,
digits, dots, underscores, or hyphens. HEAD must match `expect_head`; the
registration, lock, cleanliness, ignored-file, work-root, and pane guards
still apply. The branch fetch, release check, and deletion are skipped. A
nonmatching directory returns `outside-work-root`; a branch returns
`not_detached`. `base` remains required.

The per-clone lock is `flock` on macOS and Linux, released by the kernel
when a holder dies. Other platforms use an exclusive-create file instead; a
crashed holder leaves `<clone>.git-op.lock` behind and every op on that clone
answers `busy`. Recovery: confirm no `panewire` process for that node is
running a git op on the clone, then delete the lock file.

| `outcome` | Meaning |
| --- | --- |
| `removed` | Worktree removed and, in branch mode, `git branch -d` attempted; `worktree` describes what was removed, and `disposable` names any deleted disposable files. |
| `worktree-gone` | `cwd` is not a linked worktree. |
| `worktree-changed` | HEAD or common repository differs from the first inspection or `expect_head`. |
| `outside-work-root` | The resolved path or common git dir is outside the work root, or a detached-mode directory fails the reviewer name or location guard. |
| `unregistered` | Resolves as a worktree but is not in `git worktree list`. |
| `locked` | `git worktree lock` holds it. |
| `detached` | Detached HEAD in the default branch mode. |
| `not_detached` | Detached mode requires a detached worktree. |
| `status-check-failed` | `git status` could not be read. |
| `dirty` | Uncommitted or untracked files. |
| `ignored-files` | Ignored files not in `regenerable`; `files` names them. |
| `unmerged-or-unpushed` | The branch is neither merged into `origin/<base>` nor fully pushed. |
| `pane-in-use` | A pane (agent or shell) still sits inside the worktree. |
| `pane-scan-failed` | `herdr pane list` failed; usage is unknown so removal refuses. |
| `busy` / `cancelled` / `git-failed` / `git_too_old` | As in `worktree.prepare`. |

The refusal outcomes mirror mergewake's reap receipt reasons verbatim.

### `quota.refresh`

`args: {pool}` — `pool` matches `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`. A
closed call: `claude`, `codex`, `commandcode`, `devin` and `agy` are measured
in the node's own process. Other pools are unmeasurable and return
`refresh_failed`; they are not executed or published. The retired
`[quota].native_pools` key does not change this fixed pool set.
Result `{"outcome":"refreshed"}`; a failed run answers `refresh_failed`.

## Wire

Hub to node:

```json
{"type":"node.command","request_id":"cmd-<hex>","op":"pane.close","args":{...}}
```

Node to hub, exactly one of `result` or `error`:

```json
{"type":"node.command.result","request_id":"cmd-<hex>","op":"pane.close","result":{"outcome":"closed"}}
{"type":"node.command.result","request_id":"cmd-<hex>","op":"pane.close","error":"close_failed"}
{"type":"node.command.result","request_id":"cmd-<hex>","op":"job.done","error":"done_failed","detail":"job record unreadable"}
```

The HTTP command endpoint returns 200 for node-reported errors, 504 for `node_timeout`, and 503 for `node_lost` or `node_unavailable`. Invalid requests are rejected with 400 before dispatch.

Node error codes are a closed set: `invalid_args`, `herdr_failed`,
`close_failed`, `capture_failed`, `jobs_root_unset`, `work_root_unset`,
`refresh_failed`, `done_failed`, `result_failed`, `mark_lost_failed`,
`result_too_large`, `sweep_failed`, `journal_failed`, `record_unavailable`. A result must fit the hub's 32 KiB per-message limit; a
larger one answers `result_too_large`. The hub re-encodes each result through
its op's shape, drops one outside it, and accepts an answer only from the
machine and connection it sent the command to. An older node ignores
`node.command`, so the caller sees `node_timeout`.

## mergewake integration

mergewake reap reaches `pane.read` and `job.done` through its `reap.HubHost`,
over the same `POST /v1/nodes/{machine}/command` as its other ops.

| mergewake call | Node command | Maps to |
| --- | --- | --- |
| `HubHost.Composer` | `pane.read` with `tail_lines: 0` | `empty` → `ComposerEmpty`; `has_text` → `ComposerText`; `unknown` → `ComposerUnknown`; `focused: true` → `operator-owned`; `pane-changed` and `pane-gone` → those reasons. |
| `HubHost.JobDone` | `job.done` with the path `pane.capture` answered | `done` and `already-done` succeed; any other outcome or error fails the receipt. |

`focused` must come from `pane.read`: `agents.list` rows carry no `focused`
field, so mergewake's `Pane.Focused` is always false over the hub.

Roll out in this order. A scope naming an op the hub does not know refuses the
whole auth file, and an older node answers `node_timeout`.

1. Deploy the hub, then the nodes.
2. Add `node.command:pane.read@*,node.command:job.done@*` to the
   `svc-mergewake` line ([hub-scopes.md](hub-scopes.md)).
3. Deploy mergewake.

A Go program can use `NodeReapClient` instead of its own HTTP call:

```go
client, err := panewire.NewNodeReapClient(hubURL, clientID, token, cfAccessClientID, cfAccessClientSecret)
read, err := client.PaneRead(ctx, machine, paneID, name, cwd, 0)
done, err := client.JobDone(ctx, machine, job, report)
```

The Cloudflare Access pair is optional: pass both values or neither. The client
only sends credentials to the approved hub origin. `ErrNodeReapRefused` is a 401
or 403, and `*NodeReapError` carries any other
hub or node error code. The CLI takes the same arguments and prints the result
as JSON:

```sh
panewire node pane-read --hub-url <url> --hub-token-env <service.env> \
  --machine <machine> --pane-id <id> --name <name> --cwd <cwd> [--tail-lines N]
panewire node job-done --hub-url <url> --hub-token-env <service.env> \
  --machine <machine> --job <job> --report <path>
```

The client and the CLI refuse the operator credential. Both log nothing; the hub
logs every `pane.read` and `job.done` call by client, machine, op, pane, job and
outcome, including refused calls, and the node logs each call it runs. Neither
logs screen contents or panewire's output.

### Reap composer adapter

`pane.read` reuses mergewake's composer rules: the last two Claude dividers or
the last Codex `›` line. The reap caller maps `has_text` to `composer-text` and
`unknown` to `composer-unknown`; only `empty` permits closing.

The node reads the screen with `herdr pane read --source visible --format ansi`
and keeps SGR attributes only for the Claude rule: when every visible character
between the last two dividers is dim (SGR 2), the composer is a prompt
suggestion and reads `empty`; one non-dim character is typed text and reads
`has_text`. `screen_tail` is returned as plain text.

cmd draws the same two-divider composer, so it takes the Claude rule, plus its
idle placeholder: the composer row reads `empty` when the text after the `❯` is
exactly `Ask your question...`, its first cell may be reverse video (the
cursor), and every other cell is dim or in the foreground colour the divider
rows use (cmd draws it muted, not dim). The style decides; the literal is a
guard, because that muted colour is shared by the dividers and footer and so
proves "not a draft" only for the one text known to be drawn in it. A draft in
the default foreground, or any other text, reads `has_text`. The relay's cmd
presend asks the same rule, so an idle cmd pane is typed into once (hk 3374).

Codex and Devin are classified from the plain text alone. That text cannot prove
a placeholder is dim, so a known idle placeholder
counts as `empty` only when it is the whole composer, matched exactly after the
glyph and surrounding spaces are trimmed:

| Harness | Placeholder | Source |
|---|---|---|
| Codex | `Ask Codex to do anything` | `PLACEHOLDER`, `codex-rs/tui/src/chatwidget.rs`, tag `rust-v0.166.0` |
| Devin | `Ask Devin to build features, fix bugs, or work on your code` | strings of the devin 3000.11.234 binary and finished-pane captures |

Text that reads the same as the placeholder cannot be told apart from it, so an
unsent draft of exactly those words reads `empty`. Every other prompt keeps the
pane:

- Messages queued above the composer read `has_text`, because the composer shows
  its placeholder while the queue holds them.
- Codex `Ask a follow-up question` (`SIDE_PLACEHOLDER`) and `Viewing sub-agent —
  direct input is disabled` read `has_text`, because the thread's own draft is
  stored out of sight while either view is open.
- Codex's disabled-input prompts and Devin's `Guide Devin while it works` read
  `has_text`, because they are not the idle state of a finished pane.
- The rotating Codex placeholders removed by `rust-v0.148.0`, such as `Explain
  this codebase`, read `has_text`. They are plausible drafts, and a pane on such
  an old Codex stays open until someone closes it.

`job.done` already runs the guarded host-local completion command. Both require
explicit scoped service grants as described above.

### `job.result`

`job.result` reads a job's local artifacts without changing them. Call it with
`{"job":"NAME"}`; the job name follows the node job-name rules. It is
available to the operator and services authorized for `worktree.inspect` on
that host. The CLI is `panewire node job-result --host HOST --job NAME`, with
the same hub connection flags as `node worktree-list`.

The result contains `found`, `findings`, `findings_error`, `report`, and
`completed`. `findings` is the JSON object from `findings.json` when it is at
most 24 KiB and contains a `findings` array. Otherwise it is null and
`findings_error` is `missing`, `too_large`, `not_json`, or `schema`. A missing
job has `found: false`. Files must be regular files inside the job directory.
`report` contains `present`, `bytes`, `sha256`, and `last_line`; the last line
is limited to 256 UTF-8 bytes. The report body is never returned. A local
`report.md` that is gone because `done` deleted it after the hk pin (hk 3685)
still reads `present: true`, with `sha256` taken from the job's newest
`job.completed` that named a document; `bytes` is 0 and `last_line` empty, both
having been facts of the deleted file. `completed`
is true when the local event ledger contains a `job.completed` event.
`activity_events` counts the ledger's `events/` files that are not spawn
bookkeeping: `job.claim`, `job.reclaim`, `job.placement`, `job.task_link`,
`job.duplicate_override`, `job.spawn_retryable`, `job.spawn_aborted`,
`job.record_failed`, `job.cleanup_pending`, `job.cleanup_refused`,
`job.brief_sent`, `lease.*` and
`quota_pool.*`. Any other kind, and any name that does not parse as
`NNNNN-<kind>.json` (exactly five digits), counts, so a job that shows 0 has had nothing after its
claim. A node that predates the field omits it; a caller reads that as unknown,
never as 0. The hub uses it to release the task reservation of a spawn whose
brief never landed ([hub-spawn-operations.md](hub-spawn-operations.md)).
