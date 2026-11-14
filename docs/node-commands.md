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

The node also closes finished worker panes by itself, without a hub command:
see [pane-sweep.md](pane-sweep.md). It matches a pane by id, name and tab and
requires an idle status, and uploads the screen to hk as the same screen
document `pane.capture` uploads. It writes `pane-screen.txt` only when that
upload fails (see [pane-sweep.md](pane-sweep.md)). It does not run
`pane.close`'s composer guard.

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

`args: {pane_id, name, agent_status, cwd}` — the caller's snapshot of the
pane; `agent_status` must be `idle`, `done` or `blocked` (a pane stopped at a
prompt), never `working`. A `blocked` close also needs the job's own ledger
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

| `outcome` | Meaning |
| --- | --- |
| `closed` | Matched and `herdr pane close` succeeded. |
| `pane-changed` | Identity/status changed, or composer is not proven empty; not closed. |
| `pane-gone` | No such pane; nothing to close. |

A failed close answers `close_failed`. Acting ops run one at a time per node.

### Close operations

`pane.close_op` (hk 4594) is `pane.close` under a hub-chosen `request_id`
(`^[A-Za-z0-9_:-][A-Za-z0-9._:-]{0,127}$`, never a leading dot), a `job` and an
`attempt`: every argument of `pane.close` plus `request_id`, `job`,
`attempt` and `end_reason` (see "Hub close protocol"), all required
(`allow_unnamed` included).

**The attempt is the hub's decision.** `attempt` is an opaque token naming the
job attempt the hub decided to close. The hub reads it from `pane.close_status`
for that job (the `attempt` field, reported whether or not the operation
exists) at the moment it decides, and passes it unchanged. The node acts only
while that token is still the job's current attempt: a re-prompt, reclaim or
end recorded after the hub's decision makes the request `mismatch`, and a
resumed operation `preempted`. A repeat of a `request_id` must name the attempt
it was registered for (`invalid_args` otherwise, checked before any replay or
settlement). This is the contract hk 4632's
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
`{"state":"none"|<state>,"released":<bool>,"attempt":"<token>"}`, where
`attempt` is the job's current attempt token (empty when its ledger cannot be
read). `pane.close_release` takes the
same arguments, marks a final operation released (a `registered` one becomes
`cancelled`) and answers the same shape; an operation left `closing` stays
unreleased until a repeated `pane.close_op` settles it. All three ops take the
node's acting lock, and a visible journal state is fsynced before it is reported
or acted on. `journal_failed` answers a journal read, write or sync that failed;
the operation then stays as last recorded.

A node advertises `close_ops_v1` in its hello capabilities once it runs these
ops and its watchers defer to them (hk 4652); a hub issues `pane.close_op` only
to a node that advertises it. No hub code issues them yet: that is hk 4632.

#### Hub close protocol

A *hub close* is the hub ending a job by closing its pane with
`pane.close_op` (hk 4652). Its rules, which hk 4632's caller relies on:

1. **What is recorded before the close.** `pane.close_op` also takes a
   required `end_reason`: the hk `end_reason` of the `lost` record that ends
   the job (one printable ASCII line of at most 128 characters, the
   `job.mark_lost` bound). The planned terminal reason and any successor
   pointer live there, as the last `key=value` token
   (`replaced_by=<job>` for a reviewer, `handed_over_to=<job>` for an
   implementer; `QuotaStopEndReason`). On a new, bound registration the
   node appends `job.hub_close_intent` to the job's ledger — `request_id`,
   `attempt`, `pane_id`, `label` (the request's `name`) and `end_reason` —
   *before* it writes `registered`, and fsyncs the events directory. A
   failed intent write answers `journal_failed` and registers nothing. A
   replay or a `mismatch` records no intent. The binding check and the intent
   run under the job's end lock (`jobs/<job>/.panewire-end.lock`), which the
   watcher also takes around every terminal or expiry decision, re-reading
   the ledger under it; so no watcher end lands between the node's check and
   its intent. Before the intent the node also refuses:
   `close_in_progress` when another operation for the attempt is still
   `registered` or `closing` (one open close per attempt);
   `watcher_incompatible` when the job's watcher is an older binary that does
   not read intents (it holds the watch lock but not
   `.panewire-watch-hubclose.lock`; the hub retries after that watcher ends or
   is replaced). With no watcher running, the node starts one before it
   registers, in the herdr session of the job's receipt and with the node's
   own `handoffkeep_env` file as `PANEWIRE_HK_ENV` (a file without the
   svc-panewire token key is refused); a failed start answers
   `watcher_unavailable` with nothing registered. A watcher decides to leave
   under the end lock and releases its hub-close lock under it, so a node
   that recorded an intent while the watcher still read as aware keeps it
   watching, and one that comes after starts a new watcher. A repeat that resumes a `registered` operation makes the same
   watcher check.
2. **What the watcher does while the close is open.** The intent is open
   while it names the job's current attempt and that attempt's receipt pane
   and label, and no `job.hub_close_cancelled` for its request, end of the
   attempt or new attempt follows it (an intent from an older attempt, pane
   or label is stale and ignored, the same binding the node checks).
   While it is open the watcher writes no terminal record of its own:
   completion from a report, the decision timeout, the deadline exit and the
   missing-pane fallback all wait, past the completion deadline, across
   restarts (a re-armed watcher reads the same ledger). It reads the
   operation from `jobs/<job>/close-ops/<request_id>.json`, never herdr.
3. **Who writes the terminal record.** The watcher does, under the end
   lock, on the intent current under that lock (the newest one whose own
   request no cancellation closed: a newer request that closed nothing hands
   the job back to an older unresolved one), after fsyncing the journal it
   read. `closed` proves the pane stopped; `gone` and `unknown` do only once
   a successful `herdr pane list` in the receipt's session shows the pane
   absent (the agent list misses a pane left as a plain shell), and the
   watcher waits while it is listed (the hub may open a new request id,
   which records a newer intent). Then the watcher writes hk
   `state=lost` with exactly the intended `end_reason`, releases the quota
   lease, then records a final `job.lost` (`reason: "hub_close"`,
   `final: true`, `request_id`, `end_reason`). A crash between those steps
   leaves the intent open and the next poll repeats them. The hub may write
   the identical record itself (with `job.mark_lost`); the watcher accepts
   exactly that record and finishes. Any other terminal hk record is kept:
   the pane is proven stopped, so the watcher still releases the lease and
   records the final `job.lost` with `superseded: true`.
   `refused`, `cancelled` and `preempted` closed nothing: the watcher
   records `job.hub_close_cancelled` (`not_closed`) and resumes its own
   duties.
4. **If the hub crashes before release.** Release is bookkeeping for the hub;
   the watcher acts on the final state alone, so an unreleased final
   operation still ends the job. An operation left `registered` or
   `closing`, or an intent whose operation was never written (a node crash
   between the two), keeps the watcher waiting with no timeout: the hub must
   repeat the same `request_id` with the same arguments until it is final
   (a repeat settles `closing` as `unknown`, and registers or resumes a
   `registered` one, which closes or settles `preempted`/`mismatch`), or
   release a `registered` one (`cancelled`). Releasing a request the node
   never registered (answered `none`) records `job.hub_close_cancelled`
   (`released`) for its intent, so the watcher stops waiting. An intent with
   no operation on a job hk already ended is cancelled by the watcher
   (`orphaned`): a repeat could no longer bind. A `mismatch` on a repeat means
   the attempt moved on, which already closed the intent.

What hk 4632 must still do: send `end_reason` on every `pane.close_op`; treat
`close_in_progress`, `watcher_incompatible` and `watcher_unavailable` as
"not closed, retry later"; after `unknown`, retry under a new request id while
the pane is still there; read
`attempt` from `pane.close_status` when it decides, drive every operation it
opens to a final state, and never write a terminal hk record other than the
planned one for a job whose close it opened.

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
| `report-invalid` | — | `report` is not one of the two paths, is missing, or is not a regular file. panewire does not run. |

A failed or timed-out panewire answers `done_failed` with optional `detail`: the last stderr line, redacted and bounded to 240 UTF-8 bytes. The command can be re-issued; a node
without a jobs root answers `jobs_root_unset`. A `done` that completed the job but left its report out of hk exits non-zero (`completed; report not in hk; rerun the same command`), so it answers `done_failed` too, and the re-issued command finishes the report ([completion receipts](config.md#completion-receipts)). The node records the report's
digest in `<inbox>/jobs/<job>/.node-job-done` after panewire succeeds. The digest
is the key because a released job id can be claimed again: a new report for that
id runs panewire, the same rule panewire's own duplicate check applies.

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

| `outcome` | Meaning |
| --- | --- |
| `lost` | The record was written lost. |
| `pane-listed` | herdr still lists `pane_id`; nothing is written. |
| `label-listed` | an agent row still carries `label`; nothing is written. |
| `panes-unknown` | `herdr pane list` failed, so a pane check proves nothing. |
| `completed` | the local ledger holds a `job.completed` event; nothing is written. |
| `newer-claim` | the local ledger holds a `job.reclaim` event; nothing is written. |
| `ledger_missing` | the job dir or its `events/` is gone, so the ledger cannot prove the attempt; nothing is written. |

A missing job dir or `events/` is a ledger that is unknown, not an empty one, so
it answers `ledger_missing` and the node never writes `lost` over a record it
cannot check. A failed ledger read or handoffkeep write answers
`mark_lost_failed`; a node without a jobs root answers `jobs_root_unset`.

### `job.reap`

`args: {job, lane, force}` — all three required, and `force:false` must be
stated. This is the node half of `panewire jobs close`
([jobs-own.md](jobs-own.md)): the hub has already checked that the caller's
lane owns the job, and the node re-proves it from the job's own ledger and
herdr before it closes a pane it did not spawn in this process.

The node reads `<inbox>/jobs/<job>/events` and takes the owner lane from the
newest claim, and the pane, label and keep flag from the `job.spawned` receipt.
It then re-reads `herdr agent list` and requires the row for that pane id to
carry the receipt's label; a pane id is host-local and reused, so a different
label is another pane. `pane.close`'s settle applies to a non-forced close:
two matching reads one interval apart, an unchanged `state_change_seq` where
herdr reports one, and an empty composer both times.

| `outcome` | `pane` | Meaning |
| --- | --- | --- |
| `closed` | the closed pane | Matched and `herdr pane close` succeeded. |
| `pane-gone` | the receipt's pane, when it names one | No such agent pane; nothing to close. With no pane, the ledger names none and the node verified nothing. |
| `pane-changed` | the receipt's pane | The pane carries another label, is in no closable status, or its composer is not proven empty; not closed. |
| `job-working` | the receipt's pane | The pane is `working`; `force` closes it. Not closed. |
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

The node closes the pane of every job whose own latest record ended at least
`age_s` seconds ago, whatever herdr shows the pane doing (no idle, grace or
composer check). It keeps an implementer's pane while its worktree is on
an open PR, as the idle sweep does. It replies `{"closed": [...]}`, one
`{job, pane, role, reason}` per pane closed, with `reason` `job_revoked`,
`job_completed`, `job_handed_off`, `job_lost`, or the job's outcome.

A node without a jobs root answers `jobs_root_unset`. A failed read of the
jobs, the agent list, or the hk record answers `sweep_failed`. An out-of-range
`age_s` answers `invalid_args`. The op does not take the node's acting lock:
each pane's close takes it, so the op never holds it across the whole pass.

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

`args: {origin, mode, branch, sha, fetch, dir}`. Prepares a worktree of a
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
  commit and clean.

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
| `sha-missing` | The fetched ref does not resolve to `sha`, or the commit is not in the clone after fetching. |
| `origin-mismatch` | The existing clone tracks a different owner/repo. |
| `dirty` | The existing worktree at `dir` has uncommitted or untracked files. |
| `locked` | The existing worktree at `dir` is locked (`git worktree lock`); it is never reused. |
| `git-failed` | A git step failed. |
| `git_too_old` | The node's git is older than 2.31, the floor `worktree.inspect` documents. |

When a branch is fast-forwarded, the result includes `fast_forwarded: true`.
A tip mismatch that cannot be fast-forwarded includes `reason` with
`local=<sha7> expected=<sha7> relation=ahead|diverged`.

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

`args: {cwd, expect_head, base, regenerable, detached?}` — `cwd` inside the linked
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
| `removed` | Worktree removed and, in branch mode, `git branch -d` attempted; `worktree` describes what was removed. |
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
is limited to 256 UTF-8 bytes. The report body is never returned. `completed`
is true when the local event ledger contains a `job.completed` event.
`activity_events` counts the ledger's `events/` files that are not spawn
bookkeeping: `job.claim`, `job.reclaim`, `job.placement`, `job.task_link`,
`job.duplicate_override`, `job.spawn_retryable`, `job.spawn_aborted`,
`job.record_failed`, `job.cleanup_pending`, `job.cleanup_refused`, `lease.*` and
`quota_pool.*`. Any other kind, and any name that does not parse as
`NNNNN-<kind>.json` (exactly five digits), counts, so a job that shows 0 has had nothing after its
claim. A node that predates the field omits it; a caller reads that as unknown,
never as 0. The hub uses it to release the task reservation of a spawn whose
brief never landed ([hub-spawn-operations.md](hub-spawn-operations.md)).
