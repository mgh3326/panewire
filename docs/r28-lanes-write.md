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
machine's: a PUT whose `machine` differs, a PUT that would replace another
machine's lane, a PUT naming a `parent` on another machine or a `standby`
machine other than its own, any `sink: true` write (a sink has no machine and
can never belong to a node), and a DELETE of a lane that is absent or owned
by another machine — the two share one refusal so a node cannot probe which
it was. The authority-lane write guard applies unchanged to a lane the node
owns.

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
`invalid_lane` or `invalid_lane_request`. An empty configured path, an
unreadable configured file, a malformed or oversized current file, or a
failed safe replacement returns `500` with `lanes_unconfigured`,
`lanes_invalid`, or `lanes_write_failed` as appropriate.

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
`HUB_MACHINE_ID` and `HUB_TOKEN`; the CLI sends `HUB_MACHINE_ID` as the
`X-Panewire-Machine-ID` header on every lane request. An operator file
(`HUB_MACHINE_ID=operator`) behaves exactly as before. A node file sees and
manages only the lanes routed to that machine id: `ls` lists just those
lanes, `add` is refused unless `--machine` equals it and `--parent` names a
lane on it, and `rm` removes only its own lanes. `self-check` reads the
control-plane fields a node-filtered response never carries, so it keeps
requiring an operator file and refuses a node file before any request.

```sh
panewire lanes add <lane> --machine <machine> --pane <wN:pN> [--parent <lane>] [--sink] \
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
