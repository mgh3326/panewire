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

Both endpoints require the operator token, or a service token whose scopes
cover the call (`nodes.agents.read`, `node.command:<op>@<machine>`; see
[hub-scopes.md](hub-scopes.md)). Nodes need no extra opt-in: the
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

A command needs only a live connection to the node. It does not require
`accepting` or a `connected` presence label: a draining node, or a `stale`
one whose heartbeat is late, still takes commands — reaping a draining node
is the common case — and the `state` the hub reports is that presence label.
A connection that has died unnoticed answers `node_lost` or `node_timeout`.

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
pane; `agent_status` must be `idle` or `done`, the only states a close is
for. herdr has no conditional close, so the node re-reads `herdr agent list`
twice, two seconds apart, and closes only when both reads show a row with
that `pane_id` and exactly that name, status and cwd, and — where herdr
reports it — the same `state_change_seq`, so a pane that went to work and
came back idle in between also survives. The second read immediately
precedes `herdr pane close`; the gap left between them is the same one the
ssh reap path has between its re-read and its close.

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
checkout, or a path git cannot read is null). The node asks git for absolute
paths (`rev-parse --path-format=absolute`, git 2.31 or later) rather than
resolving a relative common dir itself; an older git reads as null.
Otherwise:

```json
{"worktree":{"path":"/work/repo.feat","branch":"feat/x","head":"<sha>","origin_url":"https://github.com/owner/name.git","common":"/work/repo/.git"}}
```

`branch` is empty on a detached HEAD. Credentials are dropped from
`origin_url` on the node: a URL remote loses its userinfo, query and
fragment, and an scp-style remote (`[user[:secret]@]host:path`) loses
everything up to its last `@` before the first `/`, so a token embedded in
either form never leaves it.

### `worktree.prepare`

`args: {origin, mode, branch, sha, fetch, dir}`. Prepares a worktree of a
clone under the node's work root — set in the node's own `spawn.json`
`work_root`, never by the caller — with the clone at `<work_root>/<repo>`
and the worktree at `<work_root>/<dir>`. `dir` is a single safe path
segment, `origin` carries no userinfo (URL userinfo, or an `@` outside an
absolute local path, is refused), `sha` is 40-hex, and `fetch` is a closed
shape: `refs/heads/<branch>` or `pull/<n>/head`.

- `mode:"branch"` — runtug#18's branch behaviour on the node. `branch` is
  required, `fetch` must be stated empty (the whole remote is fetched), and
  `sha`, when given, names the expected tip. A worktree already at `dir` is
  reused only when it is on that branch and clean; the branch checked out
  elsewhere, a `dir` parked on another branch or commit, and a local branch
  pointing at a commit other than the expected tip all refuse; without
  `sha` the expected tip is the freshly fetched `origin/<branch>`, so a local
  branch that diverged from origin, or has no origin branch at all, refuses too. A missing
  local branch is created tracking `origin/<branch>`, which must exist.
- `mode:"detached"` — `sha` and `fetch` are required and `branch` must be
  stated empty. The node runs `git fetch origin <fetch>`, requires the
  fetched `FETCH_HEAD` to resolve to `sha`, and runs `git worktree add
  --detach <dir> <sha>`. An existing worktree is reused only when detached
  at exactly that commit and clean.

A clone that already exists keeps its identity: a `git remote get-url
origin` that decodes to a different `owner/repo` than the argument — a fork
sharing the name — is `origin-mismatch`; one that cannot be read at all is
refused the same way. A missing clone is created with `git clone <origin>`.

| `outcome` | Meaning |
| --- | --- |
| `prepared` | The worktree was created; `worktree` describes it. |
| `reused` | The existing worktree at `dir` already matched. |
| `busy` | Another git op holds the per-clone lock; `retry_after_s` is set. |
| `branch-elsewhere` | The branch is checked out elsewhere, `dir` sits on another branch/commit, or the branch is not on origin where required. |
| `sha-missing` | The fetched ref does not resolve to `sha`, or the commit is not in the clone after fetching. |
| `origin-mismatch` | The existing clone tracks a different owner/repo. |
| `dirty` | The existing worktree at `dir` has uncommitted or untracked files. |
| `locked` | The existing worktree at `dir` is locked (`git worktree lock`); it is never reused. |
| `git-failed` | A git step failed. |
| `git_too_old` | The node's git is older than 2.31, the floor `worktree.inspect` documents. |

Every op that touches one clone (`worktree.prepare`, `worktree.remove`)
takes the exclusive per-clone lock `<clone>.git-op.lock`, waiting at most
60 s before answering `busy`. A node with no configured work root answers
`work_root_unset`.

### `worktree.remove`

`args: {cwd, expect_head, base, regenerable}` — `cwd` inside the linked
worktree, `expect_head` the 40-hex commit it must still be on, `base` the
merged-into branch (a `regenerable` pattern must contain a literal
character: wildcard-only patterns such as `*` are invalid_args; a glob with a
`/` anywhere matches the whole path; a wholly ignored directory, reported by git as one `dir/` entry, is approved only by a pattern ending in `/`), and `regenerable` an allowlist of ignored paths that
may be deleted with the worktree (a glob containing `/` matches the whole
path, a slash-free one matches the last segment). The node re-resolves the
worktree under the per-clone lock, then removes it only when all hold: the
resolved worktree path and common git dir are under the node's configured
work root, it appears in `git worktree list`, is not locked, is not
detached, is clean, every ignored file matches `regenerable`, the branch is
merged into `origin/<base>` or fully pushed to `origin/<branch>` (both judged after a
`git fetch --prune origin`, whose failure answers `git-failed`; a local `base` ref is never proof, and a
`base` equal to the branch never counts), and — re-read immediately before
removal — no pane is live in it. It then runs `git worktree remove` and
`git branch -d`; never `--force` or `-D`.

The per-clone lock is `flock` on macOS and Linux, released by the kernel
when a holder dies. Other platforms use an exclusive-create file instead; a
crashed holder leaves `<clone>.git-op.lock` behind and every op on that clone
answers `busy`. Recovery: confirm no `panewire` process for that node is
running a git op on the clone, then delete the lock file.

| `outcome` | Meaning |
| --- | --- |
| `removed` | Worktree removed and `git branch -d` attempted; `worktree` describes what was removed. |
| `worktree-gone` | `cwd` is not a linked worktree. |
| `worktree-changed` | HEAD or common repository differs from the first inspection or `expect_head`. |
| `outside-work-root` | The resolved worktree path or common git dir is outside the node's configured work root. |
| `unregistered` | Resolves as a worktree but is not in `git worktree list`. |
| `locked` | `git worktree lock` holds it. |
| `detached` | Detached HEAD: no branch to delete. |
| `status-check-failed` | `git status` could not be read. |
| `dirty` | Uncommitted or untracked files. |
| `ignored-files` | Ignored files not in `regenerable`; `files` names them. |
| `unmerged-or-unpushed` | The branch is neither merged into `origin/<base>` nor fully pushed. |
| `pane-in-use` | A pane (agent or shell) still sits inside the worktree. |
| `pane-scan-failed` | `herdr pane list` failed; usage is unknown so removal refuses. |
| `busy` / `git-failed` / `git_too_old` | As in `worktree.prepare`. |

The refusal outcomes mirror mergewake's reap receipt reasons verbatim.

### `quota.refresh`

`args: {pool}` — `pool` matches `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`. A
closed call to `pacelever refresh <pool>`; no other argv is reachable.
Result `{"outcome":"refreshed"}`; a failed run answers `refresh_failed`.

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
`close_failed`, `capture_failed`, `jobs_root_unset`, `work_root_unset`,
`refresh_failed`, `result_too_large`. A
result must fit the hub's 32 KiB per-message limit; a larger one answers
`result_too_large`. The hub re-encodes each result through its op's shape,
drops one outside it, and accepts an answer only from the machine and
connection it sent the command to. An older node ignores `node.command`, so
the caller sees `node_timeout`.
