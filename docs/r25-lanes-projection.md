# R25 lanes projection

`GET /v1/lanes` reads the hub's configured `lanes.json` path for each request.
It requires the operator bearer token; other credentials receive the existing
`401` response with `WWW-Authenticate: Bearer`.

Successful JSON responses contain `lanes`, sorted by lane name. Every entry
has exactly these five fields: `lane`, `machine`, `pane`, `parent`, and `sink`.
Empty values remain present, including `parent: ""` and `sink: false`.

If the configured path is empty, absent, or unreadable, the response is `200`
with `{"lanes":[]}`. Malformed JSON or a file larger than 64 KiB returns `500`
with `{"error":"lanes_invalid"}`. This endpoint reloads the file for every
call, so changes require no hub restart.

The loader is the same code path as the R19 lanes loader, including its entry
validation and sink normalization rules.
