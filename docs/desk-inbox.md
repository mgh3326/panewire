# Desk inbox delivery

The embedded delivery policy defers routine events for `operator-desk`, including
when its pane is idle or its agent is unknown. Other lanes retain their existing
delivery timing.

| Declared type | Desk class |
| --- | --- |
| `ready`, `merged`, `blocked`, `findings` | inbox |
| `idle-wake`, `progress`, `report`, `ack`, `info` | inbox |
| `decision`, `order`, `deploy-approval` | interrupt |
| `culprit-hold`, `watch-hold`, `automation-failed` | interrupt |

Unclassified worker completions declare `report` at desk ingress. Escalations,
lost jobs, and revoked jobs interrupt. Safety tags such as `ESC`, `BLOCKER`, and
`needs_decision` still interrupt. The desk can defer the `READY` tag. Other lanes
keep its existing safety behavior.

## Scheduler document and interrupt summary

An external scheduler owns the 30-minute inbox digest and its notification. The
hub does not create digest documents or schedule desk digest interrupts. Inbox
deadlines are disabled for this lane, including existing queued items. Old desk
digest batches are quarantined instead of sent.

Each actual desk interrupt can include one additional line with the counts of
unread inbox items accumulated since the last confirmed delivery. The line
includes bounded titles for dots RESULT and report items, followed by a concrete
`hk:doc inbox/operator-desk/digest/<timestamp>` reference. It contains at most
300 Unicode characters and never includes report bodies.

The hub reads the newest scheduler document from the previous, current, and next
UTC date. These dates cover scheduler keys in every timezone. It does not write
documents. If the document store is unavailable or no recent document exists,
the interrupt still delivers and the items remain eligible for a later summary.

The snapshot attached to an interrupt persists across retries and restarts.
Receiver confirmation marks those items as surfaced. `inbox list --json` exposes
`surfaced_at_ms`; their read and acknowledgment timestamps remain unchanged.
The next interrupt summarizes only newer unread inbox items. A scheduled digest
may still include surfaced items because they remain unread.

The lane policy configures this behavior:

```yaml
lanes:
  operator-desk:
    inbox_summary:
      digest_prefix: inbox/operator-desk/digest/
      max_chars: 300
```

`max_chars` accepts 240 through 300. The digest prefix must start with `inbox/`.
Removing `inbox_summary` restores ordinary inbox timing for that lane.

The summary follows the interrupt text after a ` | ` separator, never a newline:
the node drops any `relay.inject` text that holds `\r`, `\n` or NUL. A node that
cannot parse a hub message logs a rate-limited WARN and answers
`relay.unconfirmed` for a `relay.inject`, so the hub marks the row for replay.

## Scheduler kind detection

Event prose remains compatible with existing detection for `[dots] RESULT`,
`[mergewake] READY`, `[mergewake] MERGED`, `[report]`, and the `idle-wake` JSON
kind. For complete counts, a scheduler can check `event.notify_type` after the
dots marker and before its text fallback. Job reports can carry their title in
`event.report_last_line` when `event.text` is empty. Surfacing does not require a
scheduler acknowledgment.

The synthetic replay test uses the supplied 500-event sample counts. Its named
groups total 414, so the remaining 86 events are routine information. It records
22 interrupts and 478 inbox items without private event text.

## Cause keys and supersession

`panewire emit --cause` accepts `pr:<owner>/<repo>#<number>`, `job:<job-id>`,
or `task:<hk-task-id>`. Keys are at most 256 bytes; components use ASCII
letters, digits, `_`, `-`, and `.`. PR numbers are positive decimal integers.
For a PR event, send `--head` to identify its revision. Keys are explicit;
the hub does not infer them from event IDs or text.

Within one lane and cause, a PR `merged` or `closed` event supersedes earlier
unacked items. A PR `ready`, `blocked`, or `findings` with a head supersedes
earlier items of those types with a different head. Job completion, escalation,
lost, revoked, or a job's `merged` lane event supersedes its earlier `progress`,
`idle-wake`, and `decision` items. Superseded items remain readable with their
replacement ID but are terminal for delivery and unread counts. Items without
a cause are never superseded.

Ordinary inbox escalation delay can be set per lane in delivery policy, for
example `lanes: {my-lane: {deadlines: {inbox: 30m}}}`. The external scheduler
setting above disables that deadline for its lane. Retention defaults to 30 days
and 2000 items per lane, subject to replay-window and pending-delivery guards.
