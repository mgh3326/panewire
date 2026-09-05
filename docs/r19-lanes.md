# R19 lane routing contract

The hub reads `/etc/panewire/lanes.json` (configured with `--lanes`) at every
relay decision. It contains identifiers only; do not put credentials,
addresses, or installation-specific pane names in a public configuration.

```json
{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"captain"},"captain":{"machine":"host-a","pane":"w1:p2","parent":null}}}
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

After a `relay.inject`, the hub waits `RELAY_ACK_TIMEOUT` (15 seconds by
default) for `relay.delivered` or `relay.unconfirmed`. On silence it broadcasts
one hub-generated `relay.unconfirmed` with `{"reason":"ack_timeout"}`. The
node gives its local herdr injection a bounded 10-second context and keeps its
websocket read loop available while it runs.

An operator can temporarily control eligibility without changing node startup
flags: `POST /v1/nodes/<machine>/accepting` with `{"mode":"on"}`, `off`, or
`auto` and an operator bearer token. `auto` follows the hello value. The UI
shows the effective value and provides the same controls. Overrides disappear
at restart unless `--accepting-overrides /path/overrides.json` is set; that
file is read at startup and atomically updated after each successful POST:

```json
{"overrides":{"host-a":"off"}}
```

The transport this contract rides on changed in R20: relay events are now
persisted to Postgres before injection and acknowledged back. See
[r20-relay-persistence.md](r20-relay-persistence.md).
