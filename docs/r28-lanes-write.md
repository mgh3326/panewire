# R28 lanes write API and CLI

R28 adds operator-only lane registration and removal beside the R25
projection. The hub reads and writes its configured `ReportRelayPath` (the
`--lanes` file in the hub CLI). `GET /v1/lanes` remains hot-reloaded and its
entries contain exactly `lane`, `machine`, `pane`, `parent`, and `sink`.
Internal route fields such as `deliver` and `protected` are never projected.

## HTTP contract

All write routes require the operator Bearer credential. A node credential,
missing credential, malformed credential, or wrong credential receives the
existing `401` response with `WWW-Authenticate: Bearer`.

### Register or replace a lane

```http
PUT /v1/lanes/<lane>
Authorization: Bearer <operator-token>
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

The response is the R25 five-field projection. A new lane returns `201`; an
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
Authorization: Bearer <operator-token>
```

Successful removal returns `200`:

```json
{"lane":"<lane>","removed":true}
```

An absent lane returns `404` with `{"error":"lane_not_found"}`. A route
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

The operator commands use the same credential and endpoint conventions as
the other hub commands:

```sh
panewire lanes add <lane> --machine <machine> --pane <wN:pN> [--parent <lane>] [--sink] \
  --hub-url <https-hub-url> --hub-token-env <operator-env> [--hub-cf-env <access-env>]
panewire lanes rm <lane> \
  --hub-url <https-hub-url> --hub-token-env <operator-env> [--hub-cf-env <access-env>]
panewire lanes ls \
  --hub-url <https-hub-url> --hub-token-env <operator-env> [--hub-cf-env <access-env>]
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
