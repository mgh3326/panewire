# PR status and findings

`panewire pr status` and `panewire pr findings` give a worker a PR's state
without a GitHub request and without a GitHub token. They read two sources
that already hold the answer:

- **handoffkeep's change record** (the default): `GET /v1/changes/one?repo=&pr=`.
  handoffkeep is the source of truth for PR state; mergewake writes what its
  webhooks and passes carry. The record has the PR's state, the gate decision,
  the current head's checks, the review threads, and an `observed_at` and
  `source` for each section.
- **mergewake's last gate decision**: the per-PR snapshot mergewake writes at
  each pass (state, reasons, required checks, review passes, open findings,
  base freshness). It is read when `--fresh` is given, or when handoffkeep has
  no record it can trust.

Neither source costs a GitHub call without `--fresh`.

Writes (PR create, push, thread replies and resolves), CI logs
(`gh run view --log-failed`) and raw diffs stay on `gh`.

## Commands

```sh
panewire pr status   <owner/repo> <n> [--fresh] [--json]
panewire pr findings <owner/repo> <n> [--json]
```

Flags may follow the positional arguments. With no connection given, the
command asks the **local node daemon** over its unix socket, the way
`panewire inbox` does, so a worker needs no credential and no environment.
`--hub-url` or `--hub-token-env` (or `PANEWIRE_HUB_URL` or
`PANEWIRE_HUB_TOKEN_ENV`) selects a direct hub connection instead, with the
same names `panewire inbox` uses:

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--hub-url` | `PANEWIRE_HUB_URL` | HTTPS hub base URL |
| `--hub-token-env` | `PANEWIRE_HUB_TOKEN_ENV` | path of a mode-0600 `HUB_MACHINE_ID` / `HUB_TOKEN` env file |
| `--hub-cf-env` | `PANEWIRE_HUB_CF_ENV` | optional Cloudflare Access env file |
| `--machine` | `PANEWIRE_PR_STATUS_HOST` | machine that hosts mergewake (default `oci`) |

Both variables and flags carry a **path**, never a token: the credential is read
from the env file, so it appears in no argument list and in no environment the
worker is given.

### `pr status`

One line by default:

```
owner/name#152 blocked head=1c4231f blocking=1 checks=2/2 green observed=3m ago reasons=threads_open:1
```

`checks=G/T green` is followed by a `state:count` for every non-green state.
`gate_head=` appears when a fresh read saw a newer head than the gate decided
on, and `fresh=` carries mergewake's fresh read when `--fresh` was given.
`--fresh` makes mergewake spend one GraphQL request on that host, so it is the
only flag that costs GitHub budget; the gate itself is not recomputed.
`--fresh` runs mergewake first, then reads hk, and renders mergewake's snapshot.

A merged or closed PR shows `merged` or `closed` in the state position: hk's
`change.state` wins over the gate, which still says `ready` for a PR that
merged.

`--json` prints the merged view:

```json
{"source":"panewire","repo":"owner/name","pr":152,
 "mergewake":null,
 "hk":{"change":{...},"gate":{...},"freshness":{...}},
 "gh":null,
 "snapshot":{ ...the snapshot the line renders from... },
 "sections":{"state":{"observed_at":"...","source":"poll"},"checks":{...},"threads":{...},"gate":{...},"findings":{...}},
 "fallback":"","notes":[]}
```

A source that did not answer, or was not asked, is `null`, and `notes` says why.
`mergewake` is `null` on a default read that hk answered, because mergewake never
ran. `snapshot` and `sections` are additions to the first shape: `snapshot` is the
snapshot behind the status line (hk's, or mergewake's when it ran), and `sections`
is each hk section's `observed_at` and `source` (empty when mergewake answered).
`hk` is a bounded projection of hk's response, not the response: thread excerpts
are not forwarded.

#### How hk's record becomes a snapshot

hk stores facts, not mergewake's verdict on them, so the node derives the fields
`status` and `findings` render:

| Field | Derived from |
| --- | --- |
| `state` | `change.state` when it is `merged` or `closed`, else `gate.state` |
| `head`, `gate_head` | `change.head_sha`, `gate.head_sha` |
| `reasons` | `gate.reasons` |
| `checks` | the checks whose `head_sha` is the change's current head (hk keeps earlier heads' rows; a row with no `head_sha` is ignored) and `required: true` or no `required` yet (hk leaves it null for a check a webhook wrote, and only a mergewake pass sets it), so an unknown check is shown, not hidden; `green`, `red`, `pending`, `skipped` or `cancelled` from `conclusion`, else `state` |
| `threads` | unresolved threads: `blocking` when a human or mergewake wrote it, `other` when a bot did |
| `findings` | the unresolved threads, with `thread_id`, `path:line`, `severity`, `url`, `replied`, and the first line of the excerpt with HTML and emphasis markers removed |
| `review` | the review passes recorded for the current head (`pass`, `lens`, `family`); `done` when there is one |
| `observed_at` | the oldest of the `state`, `checks`, `threads` and `gate` sections |

The gate's `reasons` stay the authority on why a PR is blocked; the thread split
is a reading aid. Open findings that hk holds without a review thread are not
listed, and a note gives their count.

### `pr findings`

One open finding per line, tab-separated:

```
severity  blocking|nonblocking  path:line  summary  replied|unreplied  url
```

Tabs and newlines inside a field become spaces, so a line is always six fields.
With no open findings nothing is printed on stdout and a note goes to stderr.
`--json` prints `{source, repo, pr, head, observed_at, findings, findings_omitted, fallback, notes}`.
mergewake lists at most 50 findings; when they do not all fit the node's reply
cap, `findings_omitted` counts the rest and a note says to read them with `gh`.

## Sources and fallback

| hk | mergewake | Result |
| --- | --- | --- |
| record with every section fresh | not asked | hk answers alone: no mergewake run, no GitHub request |
| `404`, a stale or missing section, an error, too large, or not configured | answered | mergewake answers; a `note:` on stderr says why hk was not used |
| `404`, stale or failed | missing | **gh fallback**, and the output says so |
| any (`--fresh`) | answered | mergewake's snapshot is rendered; hk is read after it for the merged view |

A section is stale when its `observed_at` is older than its bound, and the note
names the section, its `observed_at` and `source`, and the bound:

```
note: hk record not used: state observed_at=2026-11-06T19:05:19+09:00 source=poll is older than 15m0s; used the mergewake snapshot
```

The bounds apply to an open PR only, since nothing changes a merged or closed
one: `state` 15 minutes (hk's poll rewrites it, so it is the heartbeat),
`checks`, `threads` and `gate` one hour. A section hk never wrote is `missing`.
`404` means hk has never seen the PR; it never fails the command. gh runs only when neither source
answered, which includes the hub being unreachable, refusing the credential, a
half-given direct connection, no local node running, or a local node daemon
that predates the op. The fallback output says that it fell back and why:

```
owner/name#152 open head=1c4231f blocking=? checks=1/3 green pending:1 red:1 observed=now source=gh (fallback: hub unreachable)
```

`blocking=?` and the `?` in a findings line mean gh does not know them. The
fallback runs `gh pr view --json state,headRefOid,isDraft,mergeable,statusCheckRollup`
(status) or a `gh api graphql` review-thread query (findings, 100 threads a
page, at most 10 pages; a longer PR gets a note that the list is incomplete)
with gh's own authentication; panewire adds nothing to its arguments or environment. If gh
fails too, the command exits 4 and names both failures.

## Transport

With no connection flags the CLI sends the op to the local node daemon
(`op: "pr.status"` on its socket, the one `panewire inbox` uses). The daemon
forwards it through the hub with its own node credential, so the worker holds
none, and answers with the node reply or the sentence that explains why it
could not (`hub unreachable`, `hub answered 503 node_unavailable`, ...). The
hub admits a node credential for `pr.status` on the PR-status host and for no
other op. If no daemon is listening, the note says `local node is not running`;
a daemon built before this op answers `local node does not support pr.status`.
Either way gh answers, as below.

With `--hub-url` and `--hub-token-env`, the CLI calls the hub's node-command endpoint
directly (`POST /v1/nodes/{machine}/command`, op `pr.status`; see
[node-commands.md](node-commands.md#prstatus)); the daemon uses the same
endpoint. The hub refuses the op for any
machine except its `--pr-status-host` (default `oci`), and a service
credential needs `node.command:pr.status@<machine>` ([hub-scopes.md](hub-scopes.md)).
The node on that machine reads hk with its own `spawn.json` `handoffkeep_env`,
and runs `mergewake pr-status --repo R --pr N --json` only when hk has nothing it
can trust or `--fresh` was given; neither credential reaches the worker. mergewake gets a closed environment (`PATH`, `HOME`, `USER`,
`LOGNAME`, `LANG`, `LC_ALL`, `TMPDIR`, `XDG_*_HOME`) and resolves its own
GitHub token for `--fresh`.

## Comparing against gh

`scripts/pr-status-compare.sh <owner/repo> <n> [<n>...]` runs `panewire pr status`,
`panewire pr findings`, `handoffkeep changes show` and the matching `gh` calls for
each PR and prints one tab-separated row per PR:

```
pr  panewire_state  gh_state  state_agrees  panewire_checks  gh_checks  checks_agree  panewire_findings  gh_threads  threads_agree  panewire_ms  gh_ms  gh_requests  panewire_source  panewire_origin  hk_state  hk_state_agrees  hk_checks  hk_checks_agree  hk_findings  hk_threads_agree  hk_ms
```

The `panewire_*` columns measure the command as a worker runs it, and
`panewire_origin` says which source answered (`hk`, or mergewake's `last_pass`).
The `hk_*` columns read hk's `changes show` directly, with no node, hub or
freshness rule in between, and map it the way the node does, so they show how
accurate hk's record is against gh. They read `-` without the `handoffkeep` CLI
(`HK_BIN` overrides it).

`gh_requests` is the change in the GitHub rate-limit counters the harness reads
before and after its own gh calls. The panewire and hk paths make none by
construction, since the node op never calls GitHub unless `--fresh` is given,
so the harness does not count them. State is compared as the open, merged or
closed class (mergewake's `ready`, `blocked` and `pending` are open). Run it
from a host that runs the node daemon (or pass hub flags) and has gh logged in. A `panewire_source`
of `gh` means the panewire path fell back and the row measures gh against gh.
