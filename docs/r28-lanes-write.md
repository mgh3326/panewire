# R28 lanes write API and CLI

R28 adds lane registration and removal beside the R25 projection. The hub
reads and writes its configured `ReportRelayPath` (the `--lanes` file in the
hub CLI). `GET /v1/lanes` remains hot-reloaded and its entries contain exactly
`lane`, `machine`, `pane`, `parent`, `sink`, and `standby` (omitted when
empty). Internal route fields such as `deliver` and `protected` are never
projected.

## HTTP contract

Every lane route accepts the operator Bearer credential, which keeps the full
surface. The three lane routes also accept a node credential — the node
bearer plus the `X-Panewire-Machine-ID` header naming that node — scoped to
the lanes that machine owns. A missing credential, malformed credential,
wrong credential, or a machine-id header that does not match the bearer's
node receives the existing `401` response with `WWW-Authenticate: Bearer`. A
node credential is refused by every other operator route.

A node-scoped request is refused with `403` and `{"error":
"lane_machine_mismatch"}` whenever it would touch a lane that is not its own
machine's: a PUT whose `machine` is any value but the node's own id —
foreign, route-only, or unknown, all identical; a PUT that would replace
another machine's lane; a PUT naming a `parent` that is missing or is a
sink — the parent is only a routing pointer, so an existing non-sink lane
on any machine is accepted — or a `standby` machine other
than its own; a PUT creating a name another machine's reports already route
through — a lane, including a sink, that names it parent, or a live job on
another machine whose owner lane is that name; any `sink: true` write (a
sink has no machine and can never belong to a node); and a DELETE of a lane
that is absent, owned by another machine, or still referenced as the parent
or owner lane of another machine's rows and jobs — missing and foreign rows
share one refusal so a node cannot probe which it was, and a node can never
orphan or redirect another machine's reports. A malformed body answers the
same `400` whatever the lane, so a node PUT is not an existence oracle. A
lane configured as an authority lane is refused the same `403` unless the
node already owns it — only then does the request reveal the `409
authority_lane_direct_write` guard.

Lane names are 1–32 characters of lowercase ASCII letters, digits, hyphens,
and underscores (`^[a-z0-9_-]{1,32}$`). Before registering a lane with an
underscore, upgrade the nodes and CLIs that read lane projections: older
versions reject these names and can fail lane listing and self-check or omit
the lane from fleet census.

Before rolling the hub back to a version without this rule, remove any lane
whose name starts with `_` and its references. The older relay loader drops
such lanes or children naming them as parent, so reports cannot route to them
and the loss guard refuses lane writes with `500 lanes_invalid`. Names such
as `auto_trader-design`, which start with a letter or digit, are retained by
the older relay loader.

### Register or replace a lane

```http
PUT /v1/lanes/<lane>
Authorization: Bearer <token>
Content-Type: application/json
```

The request JSON is strict: unknown fields, a trailing JSON value, and a
body larger than 8 KiB are rejected.

```json
{"machine":"<machine>","pane":"w<id>:p<id>","parent":"<parent-lane>","sink":false}
```

`machine` and `pane` are required for a non-sink route. The machine must be a
known hub node (or already occur in a registered route), must not be
`operator`, and must satisfy the existing machine identifier contract. A pane
is an ASCII `w<alphanumeric>:p<alphanumeric>` identifier no longer than 128
bytes. `parent`, when present, must be another currently registered lane;
self-parenting is rejected.

When `sink` is true, the existing loader rule wins: transport fields are
normalized to empty values and the route is durable-only. The CLI still
requires `--machine` and `--pane` to keep its command shape uniform.

The response is the R25 projection. A new lane returns `201`; an
existing lane replacement returns `200`.

```json
{"lane":"<lane>","machine":"<machine>","pane":"w1:p1","parent":"","sink":false}
```

Validation errors return `400` with one of the stable error values
`invalid_lane` or `invalid_lane_request`.

Two quotas keep one node token from filling the shared file and blocking
every other machine. A node credential may own at most
`--lanes-node-row-cap` rows (default 64), counted by the `machine` field the
node scope already enforces; a PUT that would create a row beyond the cap
returns `429` with `{"error":"lane_quota_exceeded"}`, while updating a row
the node already owns stays allowed at the cap. A node write may also never
grow the file past `lanesFileMaxBytes` minus an 8 KiB operator reserve, so a
node PUT that would push it over that line returns `413` with
`{"error":"lanes_file_full"}`; a write that does not enlarge the file is
unaffected, so a node already past the line can still shrink back under it.
Finally, any write whose serialized result would exceed the 64 KiB file
limit — operator or node — returns `413` with `lanes_file_full` rather than
a `500`, and the current file is left byte-identical in every refusal.

An empty configured path, an unreadable configured file, a malformed or
oversized current file, or a failed safe replacement returns `500` with
`lanes_unconfigured`, `lanes_invalid`, or `lanes_write_failed` as
appropriate.

### Dead rows are released and reused

A row whose pane is gone would otherwise hold a slot until someone ran
`wrk hygiene`. The hub reads each node's latest heartbeat session snapshot and
treats a pane as gone only when the snapshot is `ok`, no more than 120 s old,
and from a connected node. A complete `pane_ids` list proves absence even if
the detailed sessions are truncated. When `pane_ids` is incomplete or absent,
the hub uses sessions only if they are not truncated. Anything less releases
nothing. The hub acts only on rows whose `machine` is the node's own, and
applies the `DELETE` rules (not protected, no other machine routes through the
name, not a control-plane authority lane).

- **Cap.** A PUT that would create a row at the cap first evicts the caller's
  own rows whose pane is absent from its snapshot, lowest lane name first, and
  returns `429 lane_quota_exceeded` only when too few qualify to bring the node
  back under the cap (the file is then unchanged). A row the hub wrote, or
  first saw (rows loaded from the file after a restart have no write time),
  less than 10 s before the snapshot was received is not evicted, because that
  snapshot may predate its pane.
- **Sweep.** Every 30 s the hub drops a node's row whose pane is absent from
  its snapshot once the job on that pane is finished, or once the pane has been
  absent for 5 minutes. A pane that reappears restarts the clock.
- **`pane.close`.** A node command that reports `closed` or `pane-gone`
  removes that machine's rows for the pane.

Each removal logs `lane released` with a reason and broadcasts `lanes.changed`
with `op: remove`. A relay event with no route logs one `relay unrouted` WARN
per lane per minute, with the lane, the event id and the number of warnings
suppressed since the last one. The hub tracks at most 256 lanes for this; past
that it forgets the lane that warned longest ago, so a new lane always gets its
first WARN.

### Remove a lane

```http
DELETE /v1/lanes/<lane>
Authorization: Bearer <token>
```

Successful removal returns `200`:

```json
{"lane":"<lane>","removed":true}
```

An absent lane returns `404` with `{"error":"lane_not_found"}` for the
operator; a node credential instead receives the `403`
`lane_machine_mismatch` refusal described above. A route
whose internal file entry has `"protected":true` returns `409` with
`{"error":"lane_protected"}`. The protected response leaves the original
file byte-identical. The write API has no field that can set or clear
`protected`; an existing protected value is retained when that lane is
replaced.

Only successful add, update, and remove operations broadcast on the existing
operator `/v1/events` websocket. The event kind is `lanes.changed`, with this
payload:

```json
{"lane":"<lane>","op":"add"}
```

`op` is `add`, `update`, or `remove`. Failed validation, protected removal,
missing removal, and file errors produce no `lanes.changed` event.

## File safety and reload behavior

Each write takes an OS exclusive advisory lock on `<lanes-path>.lock`. The
lock covers the complete read, parse, validation, backup, temporary write,
flush, close, and rename sequence, so concurrent hub instances cannot lose
one another's lane changes. Lock, temporary, and backup files use mode 0600
and contain no credentials.

If the original file exists, its exact bytes are copied to
`<lanes-path>.bak-<UTC-timestamp>` before replacement. Names are created
without overwrite, including timestamp collisions, and only the newest ten
regular backups are retained. If the file does not exist, the first
successful PUT creates it without a backup. New JSON is fully written and
synced to a same-directory temporary file, then made visible only by rename;
write failures clean up the temporary file and preserve the original.

The configured path must be non-empty for writes. A missing configured file
is valid for an initial PUT and is an empty route set for a missing DELETE.
The existing R25 GET behavior is unchanged: missing or unreadable files are
`200` with `{"lanes":[]}`, while malformed or over-64-KiB files are `500`
with `{"error":"lanes_invalid"}`. Every GET reloads the current file.

The loader accepts both the current top-level `lanes` object and the legacy
`routes` object. `protected` is an internal boolean route field only; it is
not accepted in the PUT schema and is not present in GET or write responses.

## CLI

The commands use the same credential and endpoint conventions as the other
hub commands. `--hub-token-env` names a mode-0600 file holding
`HUB_MACHINE_ID` and `HUB_TOKEN`; with a node file the CLI sends
`HUB_MACHINE_ID` as the `X-Panewire-Machine-ID` header on every lane
request. An operator file (`HUB_MACHINE_ID=operator`) sends no machine
header and behaves exactly as before. A node file sees and manages only the
lanes routed to that machine id: `ls` lists just those lanes, `add` is
refused unless `--machine` equals it, `--parent` names an existing non-sink
lane on any machine, and the new name is not already the parent or owner lane of another
machine's rows and jobs, and `rm` removes only its own lanes that no foreign
machine still references. `self-check` reads
the control-plane fields a node-filtered response never carries, so it keeps
requiring an operator file and refuses a node file before any request.

### Node registration route

A spawn registers its lane through the node's local panewire daemon when one
answers, instead of running the credentialled command. The spawn engine dials
the daemon socket (`PANEWIRE_SOCKET`, else the default socket under the home
directory) and sends a `lanes.put` request carrying the lane, machine, pane
and parent. The daemon makes the same `PUT /v1/lanes/<lane>` with the node
credential, hub URL list and Cloudflare Access pair it already holds, so a
host whose fleet config carries no `[hub]` token key can still register a
spawned lane.

The hub's node authorization is unchanged: the daemon's credential may write
only its own machine's lanes, and the parent rules above still apply. A
refusal keeps the command's text and exit status — `lane_machine_mismatch`,
`lane_quota_exceeded`, `lanes_file_full` and the transport failures all read
the same on either route — so the receipt's `lane_error` is identical.

The credentialled command above remains the fallback when no daemon socket
answers or the daemon has no hub client. A session that names its own lane
credential in `hub.session_lanes_token_envs` also keeps the command route,
because that session's hub identity is not the daemon's.

```sh
panewire lanes add <lane> (--machine <machine> --pane <wN:pN> | --sink) [--parent <lane>] \
  --hub-url <https-hub-url> --hub-token-env <token-env> [--hub-cf-env <access-env>]
panewire lanes rm <lane> \
  --hub-url <https-hub-url> --hub-token-env <token-env> [--hub-cf-env <access-env>]
panewire lanes ls \
  --hub-url <https-hub-url> --hub-token-env <token-env> [--hub-cf-env <access-env>]
```

Flags may follow the positional lane. `add` uses PUT, `rm` uses DELETE, and
`ls` uses GET. The base URL must be HTTPS (HTTP is available only through the
existing test dependency seam), requests have a bounded timeout, and the
optional Cloudflare Access env values are sent as headers. The token and
Cloudflare secret are never printed.

Each command prints stable JSON. `add` prints the five-field lane object,
`rm` prints `{"lane":"<lane>","removed":true}`, and `ls` prints the sorted
R25 envelope `{"lanes":[...]}`. Usage errors return `ExitUsage`, rejected
identifiers or hub `4xx` responses return `ExitConditionInvalid`, and
transport, malformed response, or hub `5xx` failures return `ExitInternal`.
When the hub's refusal body carries an error code the command names it —
`lanes rejected by hub: lane_quota_exceeded` or `lanes_file_full` — and an
authority refusal also echoes its `use` hint.
