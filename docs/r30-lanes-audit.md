# R30 lanes audit — dead-lane check

`panewire lanes-audit` answers one question for every non-sink lane: is the
pane this lane routes to still present in the hub's latest session snapshot
for that lane's machine? Events sent to a lane whose pane is gone are stored
but never delivered — they sit `undelivered` until replay exhausts
([R21](r21-lane-event.md)). The audit makes those lanes visible before an
operator decides what to do with them.

The command reads exactly two operator endpoints — `GET /v1/lanes` and
`GET /v1/nodes` — and computes locally. Both inputs are data the hub already
holds; no node command is issued and nothing is written. There is no apply,
delete, or fix mode: the audit is display-only by construction.

## Verdicts

Each non-sink lane gets exactly one of three verdicts:

| verdict | meaning |
|---|---|
| `alive` | the lane's machine reports a fresh, complete snapshot that contains the lane's pane in `sessions[].pane_id` |
| `dead` | that same complete observation does **not** contain the pane |
| `indeterminate` | the observation is incomplete — the pane's presence cannot be judged from hub-held data |

`indeterminate` is a real answer, not a failed lookup. A stale or truncated
snapshot, an unavailable collector, a malformed snapshot, or a node that is
missing, stale, or disconnected all produce it — and each carries a `reason`
(`node_not_returned`, `snapshot_stale`, `truncated`, …). Folding
`indeterminate` into `dead` is the failure this command exists to prevent: a
lane deleted on a transient-outage reading loses every escalation routed to
it afterwards. `dead` therefore requires the observation to be *complete*:
`snapshot_status=ok`, node `state=connected`, `stale=false`,
`truncated=false`, `received_at` present, not in the future, and within the
120-second freshness bound `sessions find` uses.

The join is strict: only the node named by `lane.machine` decides, and only
`pane_id` equality counts. The same pane id on another machine, or a label
that echoes it, proves nothing. Sink lanes are skipped entirely and counted
in `summary.sink_skipped` — they carry no pane by design.

## Output and exit

```sh
panewire lanes-audit --hub-url https://hub.example.invalid --hub-token-env /etc/panewire/operator.env [--hub-cf-env f] [--json]
```

Default output is the tab-delimited operator format (`fetched_at`,
`outcome`, `summary`, one `lane` row per judged lane with `verdict`,
`reason`, `node_state`, `last_seen`); `--json` emits the same structure as
one JSON object. `outcome` is `ok` (all judged, none dead), `dead_lanes`
(all judged, at least one dead), or `partial` (at least one indeterminate).

Exit codes: `0` when every non-sink lane received a verdict (dead lanes are
a finding, not a command failure), `7` (`ExitPartial`) when any lane is
indeterminate, and the usual non-zero codes for usage, credential, and hub
errors.

## Relationship to `lanes self-check`

`lanes self-check` is a single-lane ownership hook: it asks "am I still the
owner of this route at this epoch" and encodes the answer in its exit code
(`0` proceed / `3` self-stop / `5` unknown). The audit is a fleet-wide,
cross-dataset observation with a three-way verdict; it shares nothing with
that contract beyond the `/v1/lanes` read. Keeping it a separate command
leaves the hook's exit-code semantics — and `lanes_cli.go` — untouched.
