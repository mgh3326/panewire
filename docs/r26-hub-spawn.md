# R26 hub-directed spawn

This document quotes the T14 contract verbatim. The hub keeps request status
only in memory, just like presence; it does not persist a brief or execute a
command itself.

## A. hub — `POST /v1/spawn` (operator token)

Request:

```json
{"request_id":"<uuid4>","machine":"machine-a","cwd_key":"repo-a",
 "brief":{"inline":"<≤65536 bytes UTF-8>"},
 "args":["<wrk flag>","<value>"],
 "wait_seconds":120}
```

- Validation: `request_id` uuid, `machine` `^[a-z0-9-]{1,32}$`, `cwd_key`
  `^[a-z0-9._-]{1,64}$`, `brief.inline` required (empty is 400; **v0 is
  inline only** — handoffkeep document keys are follow-up), `args` accepts
  **only wrk flags** (`-m -w -l --t --effort --job --owner --report --role
  --lane --parent -L`; `--job-dup-ok` is forbidden), each value
  `^[A-Za-z0-9._:/@-]{1,200}$` (spaces and shell metacharacters rejected),
  and `wait_seconds` is 1..300.
- Processing: if the target node is not connected **or** not
  `accepting_effective`, return 503
  `{"error":"node_unavailable","state":...}`. A duplicate `request_id`
  returns 409 with its existing result. Send `job.spawn` to the node and wait
  up to `wait_seconds` for `job.spawn.result`.
- Response: 200
  `{"request_id","machine","rc":0,"job_id":"…","pane":"…","stdout_tail":"<≤4KiB>","completed_at"}`.
  If the node has not replied, return 504
  `{"request_id","status":"pending"}` (the node keeps running; query
  `GET /v1/spawn/{request_id}` for up to one hour — hub memory only, like
  presence). A disconnected node reads as `{"status":"lost"}`.
- Broadcast `/v1/events`: `spawn.requested` and
  `spawn.result` `{request_id, machine, rc}` (no brief body or argument
  values).

## B. node — `job.spawn` reception

- Node configuration `~/.config/panewire/spawn.json`:

  ```json
  {"cwd_map":{"repo-a":"/abs/path"},"workspace":"worker","herdr_session":"worker","enabled":true}
  ```

  `enabled` defaults to false (no file rejects with `spawn_disabled`), and an
  unmapped `cwd_key` rejects with `cwd_unmapped`. Rejections also reply as
  `job.spawn.result{rc:2, error}`.
- Execution: write the brief to a `mktemp` file (0600), then execute
  `env HERDR_SESSION=<s> wrk spawn -c <cwd> -p <tmp> --host local -w <workspace> <args...>`
  as argv (never a shell string). The timeout is `wait_seconds+60`; remove the
  temporary file afterwards. `stdout_tail` is at most 4 KiB.
- Result message:
  `job.spawn.result{request_id, rc, job_id, pane, stdout_tail}`. Parse
  `job_id` and `pane` from the same `wrk` OK-line rule:

  ```text
  OK pane=<pane> model=<m> label=<l> status=<s> landed=<y|n> job=<job-id> quota_record=<...>
  ```

  From a line beginning `^OK pane=`, split on spaces and take `pane=` and
  `job=` key/value fields. If there is no OK line, both values are empty.
- Concurrency: one spawn executes at a time per node (queued); a repeated
  `request_id` is ignored.

## Boundaries

The injected configuration path is available through `HubClientConfig` for
tests. The production default is the path above; no normal test reads it.
`spawn.json` is operator-provisioned and is not deployed by this change.
