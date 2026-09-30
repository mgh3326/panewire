# R25 lanes projection

`GET /v1/lanes` reads the hub's configured `lanes.json` path for each request.
It accepts either the operator bearer token or a node credential — the node
bearer plus the `X-Panewire-Machine-ID` header naming that node. Other
credentials receive the existing `401` response with `WWW-Authenticate:
Bearer`.

Successful operator JSON responses contain `lanes`, sorted by lane name, plus
the control-plane fields `control_epoch`, `control_owner`, `control_state`,
`last_request_id`, and `authority_lane_protection`. Every lane entry carries
`lane`, `machine`, `pane`, `parent`, `sink`, and `standby` (omitted when
empty). Empty values remain present, including `parent: ""` and `sink: false`.

A node-authenticated response contains only `lanes`, filtered to the rows
whose `machine` equals the authenticated machine id. The control-plane fields
are never sent to a node.

If the configured path is empty, absent, or unreadable, the response is `200`
with `{"lanes":[]}`. Malformed JSON or a file larger than 64 KiB returns `500`
with `{"error":"lanes_invalid"}`. This endpoint reloads the file for every
call, so changes require no hub restart.

The loader is the same code path as the R19 lanes loader, including its entry
validation and sink normalization rules.
