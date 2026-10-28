# Quiet event delivery

The hub CLI enables the embedded `delivery-policy.yaml` by default. Informational
notices enter a durable per-lane inbox; actionable notices keep the existing pane
injection and verification path. Nothing is installed by this document.

```sh
panewire emit --kind lane.event --lane desk --event-id build-ready \
  --deliver inbox --text '[build] done | report ready'
panewire emit --kind lane.event --lane desk --event-id need-decision \
  --deliver interrupt --text 'Decision required: choose the release'
```

The optional `delivery_class` travels through event files, the immediate local
socket request, scanner, node outbox and hub ingress. `--deliver` selects this
class for **all emit kinds**. The existing JSON `deliver` field remains the
separate node timing policy (`idle`, `now`, `max_wait=...`). No new field is sent
in `relay.inject`; old daemons accept exactly their existing envelope. Missing
classes from old emitters/daemons are classified at the upgraded hub. An old
hub does not understand explicit classes: upgrade the hub first, then nodes and
emitters. An old daemon drops the explicit override, so only hub policy applies
until that node upgrades.

## Policy

First match wins, with lane rules ahead of global rules. Explicit class always
wins. Messages originating in the operator chat UI remain interrupts, including
declarative instructions. Escalation, joined and revoked job events use the
**receiving parent lane's** policy and inbox, following existing routing.

| Signal | Default class |
| --- | --- |
| BLOCKER, decision request, action required/needed/now | interrupt |
| Direct question (`?`, `？`, question/질문) | interrupt |
| job.escalate, job.lost, job.revoked | interrupt |
| Replay without an already saved decision | inbox |
| job.completed, job.joined; `[worker] done \|` | inbox |
| FYI, ack, acknowledged | inbox |
| install-done, installed, merged/please install | inbox |
| Info alert, quota notice, idle-wake | inbox |
| Unmatched message | interrupt |

Action/question rules precede informational rules; an FYI containing a BLOCKER
therefore interrupts. This is heuristic text classification, not semantic intent
recognition. Explicit overrides remove ambiguity. A saved decision wins over a
changed retry body, replay flag or later policy edit.

`--delivery-policy default` uses the embedded file. A filesystem YAML path loads
an operator-owned copy at startup. Invalid policy or an unavailable store prevents
hub startup; a runtime storage failure leaves the producer unacknowledged. YAML
has strict keys, checked classes/regexps/durations, and one document. Per-lane
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
apply only to newly accepted identities. `--delivery-policy off` restores legacy
classification for new unspecified events, still honors explicit classes and
keeps previously stored inbox items quiet. It also disables new interrupt
batching. Already queued digests/escalations continue draining.

## Durability and replay

`--inbox-store PATH` chooses a private, persistent SQLite database on the hub.
The default is `os.UserConfigDir()/panewire/hub-inbox.sqlite3` (on Linux normally
`$HOME/.config/panewire/hub-inbox.sqlite3`). It uses WAL and FULL synchronous
commits; the database is mode 0600. The hub stores full originals and the chosen
class before acknowledging a quiet event to the producer. Inbox acceptance does
not depend on a connected destination node or on handoffkeep availability.

This uses the existing SQLite driver, with no database server. Handoffkeep's
current relay contract has no class/read/ack/escalation fields; extending it would
require a coordinated second repository/schema release. The hub is already the
receiving-lane authority, so a local durable store keeps this one PR deployable.
Back up this store with SQLite's backup API, or stop the hub and copy the DB and
any `-wal`/`-shm` sidecars. Do not delete it during rollback. There is no automatic
retention/pruning; the desk owns backup, disk monitoring and archival. A future
hub failover must move this database with the hub, not start an empty store.

Original inbox items never become relay directives. On old handoffkeep replay
(including `hub_unconfirmed_once.go`), the hub saves informational rows in its
inbox before retiring those relay rows. A failed retirement retries harmlessly;
the inbox identity and class are stable. Delivered handoffkeep rows are not
imported. A pre-upgrade active pane submission cannot be recalled.

An unread item past its deadline is claimed once into one short per-lane
interrupt digest. Reading/acking before the claim suppresses escalation. The
claim and outgoing digest are one SQLite transaction. Restart retries the same
batch ID. Digest text has item IDs and bounded summaries; full originals remain
readable. An item is never escalated into another batch, although the existing
relay transport may retry an unconfirmed submission as before. Later unread
items can cause a later digest. Escalations use the existing handoffkeep relay;
during a handoffkeep outage their durable batches wait and retry.

When enabled, interrupt batching uses the same transactional outbox: arrivals
for one receiving lane within the oldest item's window become one turn. A
single item is also delayed by that window. Chat UI turns bypass batching. The
flush runs every 100ms; a batch is never sent early. Long batches abbreviate
summaries to the existing lane text bound, with originals available by ID.

## Inbox commands and API

Set `PANEWIRE_HUB_URL`, `PANEWIRE_HUB_TOKEN_ENV` (path to the existing private
`HUB_MACHINE_ID`/`HUB_TOKEN` env file), optional `PANEWIRE_HUB_CF_ENV`, and
`PANEWIRE_LANE`, or pass the corresponding flags shown by `panewire inbox --help`.
Use the same approved hub origin as other operator commands.

```sh
panewire inbox list --lane desk --unread
panewire inbox read 123 --lane desk
panewire inbox ack 123 --lane desk
panewire inbox ack --all --lane desk
panewire inbox count --lane desk
panewire inbox list --json
panewire inbox count --json
```

List is newest first, capped at 500 items, and includes the saved class; `--unread`
filters unread inbox-class items. Read marks read. Ack marks read and acknowledged;
neither removes the record. Interrupt originals appear in unfiltered lists for
inspection, but do not increase the unread inbox count. A read/ack is a hub write.

The authenticated API is `GET /v1/inbox?lane=L&unread=true`,
`GET /v1/inbox/count?lane=L`, `POST /v1/inbox/{id}/read?lane=L`, and
`POST /v1/inbox/ack?lane=L` with `{ "id": 123 }` or
`{ "all": true, "lane": "L" }`. Node tokens require an explicit lane currently
owned by that node, including read and ack operations. Operator tokens may list
or ack across lanes. The count path uses a partial index, reads no message body,
and neither reads nor acknowledges items. A zero count for a specified lane is
returned explicitly. Hub failures display as unavailable, never as zero.

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
