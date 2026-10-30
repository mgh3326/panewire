# Node commands

Node commands let an operator tool read and act on a node's herdr panes and
git worktrees through the hub, instead of running `ssh <host> herdr ...` on
every session host. mergewake `reap` is the first caller: it lists agents,
inspects worktrees, captures a pane's output and closes the pane, all over
the connection each node already keeps open to the hub.

The hub keeps a pending command only in memory while the HTTP caller waits.
It never retries, persists, or acts on a result itself; a caller that times
out may re-issue, and every acting op re-checks on the node.

## Trust boundary

Both endpoints require the operator token. Nodes need no extra opt-in: the
hub can already type into any pane through `relay.inject`, and every op
here is narrower than that. Each op is a closed shape; no op runs a shell,
and no op takes a caller-chosen file path to write.

## Hub endpoints

### `GET /v1/nodes/agents`

Fans `agents.list` out to every known node and answers one row per machine,
sorted by `machine_id`:

```json
{"nodes":[
  {"machine_id":"machine-a","state":"connected",
   "agents":[{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","agent":"claude","cwd":"/work/repo.feat"}],
   "panes":[{"pane_id":"w1:p1","agent_status":"idle","agent":"claude","cwd":"/work/repo.feat"},
            {"pane_id":"w1:p2","agent_status":"","cwd":"/work/other"}],
   "panes_ok":true},
  {"machine_id":"machine-b","state":"disconnected","agents":null,"panes":null,"panes_ok":false,"error":"node_unavailable"}
]}
```

A node that is not connected keeps its row with `error:"node_unavailable"`,
and one that does not answer within 10 seconds reads `error:"node_timeout"`,
so a caller never mistakes an unreachable node for one with no panes.

### `POST /v1/nodes/{machine}/command`

```json
{"op":"pane.close","args":{"pane_id":"w1:p1","name":"job-a","agent_status":"idle","cwd":"/work/repo.feat"}}
```

`args` may be omitted for `agents.list`. Unknown ops, unknown or missing
argument fields, and invalid values are rejected with 400 before anything
reaches the node. The hub waits up to 30 seconds:

| Status | Body | Meaning |
| --- | --- | --- |
| 200 | `{"request_id","machine","op","result":{...}}` | The node answered. |
| 400 | `{"error":"invalid_request"}` or node `invalid_args` | Arguments rejected. |
| 502 | `{"request_id","machine","op","error":"<code>"}` | The node answered with an error code. |
| 503 | `{"error":"node_unavailable","state":"..."}` | Not connected; nothing was sent. |
| 503 | `{...,"error":"node_lost"}` | The node disconnected while the hub waited. |
| 504 | `{...,"error":"node_timeout"}` | No answer in time; a late answer is dropped. |

Commands do not require the node to be `accepting`: reaping a draining node
is the common case.

## Ops

All validation is shared by hub and node. Every listed argument is required
and must not be null — a zero value such as `write:false` or
`merged_after:0` has to be stated, never implied by omission. `pane_id` is
`^[A-Za-z0-9._:-]{1,128}$`; `name` is a single safe path segment (no `/`,
`\`, `..`, whitespace, or leading `-` / `.`); `cwd` is an absolute path.

### `agents.list`

`args: {}`. The node runs `herdr agent list` and `herdr pane list` and
returns `agents` and `panes` rows with herdr's own field names (`pane_id`,
`name`, `agent_status`, `agent`, `cwd`), so a caller that already decodes
`herdr agent list` reads them unchanged. `pane list` also names plain
shells; when it fails, `panes` is null and `panes_ok` is false, and a caller
must treat pane usage as unknown. A failed agent list answers
`herdr_failed`.

### `pane.close`

`args: {pane_id, name, agent_status, cwd}` — the caller's snapshot of the
pane. The node re-reads `herdr agent list` and closes the pane only when a
row with that `pane_id` still has exactly that name, status and cwd:

| `outcome` | Meaning |
| --- | --- |
| `closed` | Matched and `herdr pane close` succeeded. |
| `pane-changed` | Present, but name, status or cwd differs; not closed. |
| `pane-gone` | No such pane; nothing to close. |

A failed close answers `close_failed`. Acting ops run one at a time per node.

### `pane.capture`

`args: {pane_id, name, cwd, merged_after, write}`. Saves a pane's output into
its job directory `<inbox>/jobs/<name>` before a close, with the rules
mergewake reap applies over ssh:

| `outcome` | `saved` | Meaning |
| --- | --- | --- |
| `not-a-job` | — | The job directory does not exist, or is a symlink rather than a real directory: the fleet did not spawn this pane. Nothing is written. |
| `report` | `<dir>/report.md` | `report.md` is a regular file (not a symlink) whose mtime is strictly later than `merged_after` (unix seconds; 0 means never). |
| `would-screen` | `<dir>/pane-screen.txt` | `write:false`: where the screen would go. Nothing is read or written. |
| `screen` | `<dir>/pane-screen.txt` | The pane, re-read and still matching `name` and `cwd`, had its `herdr pane read` output written there atomically, mode 0600. |
| `pane-changed` / `pane-gone` | — | The pane no longer matches; nothing is written. |

`<inbox>` is the node daemon's jobs inbox root; a node without one answers
`jobs_root_unset`. Every read and write goes through an `os.Root` opened on
`<inbox>/jobs`, so no symlink at or below the job directory can lead outside
it; the screen is written to an `O_EXCL|O_NOFOLLOW` temporary file and
renamed over `pane-screen.txt`, which replaces a symlink there instead of
writing through it. A symlink above `<inbox>/jobs` is operator-provisioned
and honoured. Read or write failures answer `capture_failed`.

### `worktree.inspect`

`args: {cwd}`. Returns `{"worktree":null}` unless `cwd` is inside a linked git
worktree (an ordinary directory, a bare repository, a clone's canonical
checkout, or a path git cannot read is null). Otherwise:

```json
{"worktree":{"path":"/work/repo.feat","branch":"feat/x","head":"<sha>","origin_url":"https://github.com/owner/name.git","common":"/work/repo/.git"}}
```

`branch` is empty on a detached HEAD. URL userinfo is dropped from
`origin_url` on the node, so a token embedded in an https remote never leaves
it.

## Wire

Hub to node:

```json
{"type":"node.command","request_id":"cmd-<hex>","op":"pane.close","args":{...}}
```

Node to hub, exactly one of `result` or `error`:

```json
{"type":"node.command.result","request_id":"cmd-<hex>","op":"pane.close","result":{"outcome":"closed"}}
{"type":"node.command.result","request_id":"cmd-<hex>","op":"pane.close","error":"close_failed"}
```

Node error codes are a closed set: `invalid_args`, `herdr_failed`,
`close_failed`, `capture_failed`, `jobs_root_unset`, `result_too_large`. A
result must fit the hub's 32 KiB per-message limit; a larger one answers
`result_too_large`. The hub re-encodes each result through its op's shape,
drops one outside it, and accepts an answer only from the machine and
connection it sent the command to. An older node ignores `node.command`, so
the caller sees `node_timeout`.
