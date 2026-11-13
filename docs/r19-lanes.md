# R19 lane routing contract

The hub reads `/etc/panewire/lanes.json` (configured with `--lanes`) at every
relay decision. It contains identifiers only; do not put credentials,
addresses, or installation-specific pane names in a public configuration.

```json
{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"parent-lane"},"parent-lane":{"machine":"host-a","pane":"w1:p2","parent":null}}}
```

`job.completed` is injected into its `owner_lane` pane. `job.escalate` and
`job.joined` use the same flat metadata as a completion plus a non-empty
`reason`; they are injected into the owner lane's parent pane. A missing lane,
parent, or connected node yields `relay.unrouted` rather than a silent drop.
An escalation `question` longer than 240 characters is truncated for the hub;
the complete question remains in the referenced events file (or explicit
report file), which is shown in the relay text.
The older `{"routes":...}` form and the `--report-relay-routes` flag remain
read-compatible during migration.

Nodes reconcile live jobs with panes every five minutes. A missing hub lane gets one registration attempt per pass using the landing
registration path. If the hub listing is unavailable, an unresolved
`lane_unregistered` receipt also triggers an attempt. A listed lane is left
untouched, including when it points to a different pane. A node-scoped listing
contains only that node’s lanes; a lane owned by another node appears missing,
and the hub refuses an attempted overwrite. A removed lane is recreated while
the job remains live unless its registration is deferred. Success records
`job.lane_registered`; failure records a bounded, redacted reason. After
three consecutive failures that job waits 30 minutes between attempts.
A deliberately deferred lane is left to its designated owner.

For direct lane events, a unique live job can supply a missing route; see
[R21 direct lane events](r21-lane-event.md).

Lanes that carry control-plane authority cannot be written through this API: a
direct `PUT` **or `DELETE`** on one of them is refused with 409
`authority_lane_direct_write`. The authority set itself is deployment
configuration. See [authority-lanes.md](authority-lanes.md).

After a `relay.inject`, the hub waits `RELAY_ACK_TIMEOUT` (15 seconds by
default) for `relay.delivered` or `relay.unconfirmed`. On silence it broadcasts
one hub-generated `relay.unconfirmed` with `{"reason":"ack_timeout"}`. The
node gives its local herdr injection a bounded 10-second context and keeps its
websocket read loop available while it runs.

An operator can temporarily control eligibility without changing node startup
flags. Send `POST /v1/nodes/<machine>/accepting` with an operator bearer token:
`{"mode":"off","containment":"hk:12"}` disables accepting and requires an
open containment task ID matching `hk:` followed by 1–9 digits, starting with
1–9. Missing or invalid IDs return `400 containment_required`. With
`--handoffkeep-env` configured, the hub checks the task: a closed task returns
`409 containment_closed`, and a missing task returns `400 containment_not_found`.
If handoffkeep is unavailable, the off still succeeds with
`containment_verified: false`; check the task when service is restored.
`{"mode":"on"}` and `{"mode":"auto"}` need no containment; auto follows the
hello value. The UI prompts for the task when turning off. `/v1/nodes` and
`panewire nodes` show the containment, and the API also shows verification state.
Overrides disappear at restart unless `--accepting-overrides /path/overrides.json`
is set. The hub reads that file at startup and atomically updates it on each
successful POST. The mode map remains readable by older hub versions; after a rollback, those versions ignore containment metadata.

```json
{"overrides":{"host-a":"off"},"containments":{"host-a":{"containment":"hk:12","containment_verified":true}}}
```

The transport this contract rides on changed in R20: relay events are now
persisted to Postgres before injection and acknowledged back. See
[r20-relay-persistence.md](r20-relay-persistence.md).

Direct producer notifications use the related [R21 lane-event
contract](r21-lane-event.md). Unlike `job.escalate` and `job.joined`, an R21
event addresses its named lane directly and never follows `parent`.
