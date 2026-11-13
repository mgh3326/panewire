# Hub service scopes

A service — a tool that is neither the operator nor a node, such as the
mergewake reaper — gets its own hub credential that can do only its job. The
operator token stays with the operator.

## Auth file

The hub's `--hub-auth` file (mode `0600`, see [hub-r6.md](hub-r6.md)) takes
one more line shape:

```sh
HUB_TOKEN_<client>=<token>                 # operator, node, or service
HUB_SCOPES_<client>=<scope>[,<scope>...]   # makes <client> a service
```

- A client **with** a scopes line is a service. It gets exactly the listed
  scopes, and it is never a node: it cannot open the node websocket
  (`GET /v1/agent`), write lanes, count as a known machine, or be an alert node.
- A client **without** one is a node, with its rules unchanged.
- `operator` keeps full access. A scopes line for `operator`, a scopes line
  without a matching token, a malformed or unknown scope, a service token
  that equals any other client's token, and a file over 1 MiB each refuse the
  whole file.

The service sends the same headers a node does: `X-Panewire-Machine-ID: <client>`
and `Authorization: Bearer <token>`. The client env file is
`HUB_MACHINE_ID=<client>` plus `HUB_TOKEN=<token>`.

## Scopes

Each scope names its resource. `@*` means any machine. Inbox lane scopes
name one lane or use `*` for any valid lane; only the whole lane may be `*`.
The wildcard forms suit fleet-wide services such as mergewake. A service with
`inbox.ack:*` can acknowledge all unacknowledged, non-superseded items in any
lane with `{"all":true}`, including items intended for other agents.

| Scope | Grants |
| --- | --- |
| `nodes.agents.read` | `GET /v1/nodes`, `GET /v1/nodes/agents` |
| `node.command:<op>@<machine>` or `@*` | `POST /v1/nodes/{machine}/command` with that op (`agents.list`, `pane.capture`, `pane.close`, `pane.read`, `job.done`, `worktree.inspect` (also authorizes `worktree.list`), `worktree.prepare`, `worktree.remove`, `quota.refresh`, `pr.status` (only on the hub's `--pr-status-host`; see [pr-status.md](pr-status.md))). `pane.read` and `job.done` admit a service holding that scope and no other caller, not even the operator. `job.mark_lost` is issued by the hub itself when it releases a spawn reservation; the endpoint refuses it for every caller, so no scope grants it. A node credential is admitted for `pr.status` alone, with no scope, on the PR-status host. |
| `spawn@<machine>` or `@*` | `POST /v1/spawn` for that machine, and `GET /v1/spawn/{id}` for a request on it. A spawn request with `checkout` also authorizes the node's internal `worktree.prepare` preflight; no separate `node.command` grant is needed. Requests with `checkout` or `brief.doc` require the connected node to advertise `spawn_v2` in its hello; otherwise the hub returns `503 spawn_capability_required` immediately. |
| `wake@<machine>` or `@*` | Retired: the route is gone. An auth file may still list it; it grants nothing. |
| `jobs.close:<lane>` | `POST /v1/jobs/close` for the calling lane, without operator override; see [job-close.md](job-close.md) |
| `inbox.read:<lane>` or `inbox.read:*` | `GET /v1/inbox`, `/v1/inbox/count`, `/v1/inbox/wait` and `POST /v1/inbox/{id}/read` with `?lane=<lane>` |
| `inbox.ack:<lane>` or `inbox.ack:*` | `POST /v1/inbox/ack` with `?lane=<lane>` |
| `inbox.ack_state` | `GET /v1/inbox/ack-state?event_id=…` (see [Ack state](#ack-state)); no lane, no resource |
| `state.read` | `GET /v1/state` ([fleet-state.md](fleet-state.md)) |

The hub's ticker issues `pane.sweep` (see [pane-sweep.md](pane-sweep.md#hub-ordered-sweep)) the same way: the endpoint refuses it for every caller, so no scope grants it.

Every other route keeps the authentication it had: operator-only, node-only
for the websocket and a node's own lanes, CF Access where the UI and chat
accept it, and none for `GET /healthz`. A service credential satisfies none
of them. A service whose scopes do not cover a request gets
`403 {"error":"scope_forbidden"}`, so its client can report a scope error
rather than an empty result. An unknown or wrong credential still gets `401`.
On the routes whose resource is in the body (node command, spawn) the
body is validated before the scope check, so a malformed request answers `400`.
`GET /v1/spawn/{id}` answers `404` for an unknown id before the scope check.

`GET /v1/jobs/mine` and `POST /v1/jobs/reap` are the lane's own jobs routes
([jobs-own.md](jobs-own.md)). They take no scope: the operator may name any
lane, a node credential only a lane routed to its own machine, and a service
is refused. `jobs close` also requires the job's owner lane to be the named
lane.

The `svc-mergewake` line for hub reap (operator decision D1: every machine):

```sh
HUB_SCOPES_svc-mergewake=nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:pane.capture@*,node.command:pane.close@*,node.command:worktree.remove@*,node.command:pane.read@*,node.command:job.done@*,inbox.read:*,inbox.ack:*,inbox.ack_state
```

An initiating panewire service needs `spawn@<machine>` for one destination, or
`spawn@*` for every destination. The same grant authorizes POST and GET for
spawn requests, including the `worktree.prepare` preflight requested by the
optional `checkout` field. Requests with `checkout` or `brief.doc` require the
connected node to advertise `spawn_v2` in its hello; otherwise the hub returns
`503 spawn_capability_required` immediately. Nodes restrict `brief.doc` keys
to their configured `brief_doc_prefix` (default `briefs/`). Direct calls to the node command endpoint still
need their own `node.command:<op>@<machine>` grant. For example:

```sh
HUB_SCOPES_svc-panewire=spawn@*
```

Operator authorization is unchanged. A service without a matching spawn grant
gets `403 {"error":"scope_forbidden"}`; a grant for another machine does not
authorize this request.

## Ack state

A sender that needs to know whether its event was acked should not read a
lane's inbox. `GET /v1/inbox/ack-state?event_id=ID[&event_id=ID...]` answers,
for each id, only whether the event's inbox row was acked or superseded and when — never a
lane, kind, body or sender:

```json
{"events":[{"event_id":"ev-1","acked":true,"acked_at":"2026-11-04T03:00:00Z"},
           {"event_id":"ev-2","acked":false}],
 "not_found":["ev-3"]}
```

`acked_at` is RFC 3339 UTC and present only when `acked` is true. Answers keep
the request's order; a repeated id is answered once; one request names 1 to 100
ids. When one event id reached several lanes, `acked` is true once every row is
acked or superseded, and `acked_at` is the latest resolution time. A superseded row reports `acked: true` with its supersession time.

| Caller | Sees | Otherwise |
| --- | --- | --- |
| operator | every row | `404` with `not_found` listing the ids when none has a row |
| service with `inbox.ack_state` | every row | the same `404`; without the scope `403 {"error":"scope_forbidden"}` |
| node | rows its own node relayed (the hub records the sender when it stores the row) | `401` when none of the ids is one of its rows |

A node cannot tell another node's event from an unknown one: both are `401`
when no id is visible, and both are listed in `not_found` when some are. A row
stored before the hub recorded senders has none, so only the operator and a
service see it. The CLI is `panewire inbox status --event-id ID [--event-id ID...] [--json]`;
it prints the answer and exits `7` when any id has no visible row.

## Reload

The hub re-reads the auth file when it changes on disk (checked on each
authenticated request and on each maintenance tick, every keepalive
interval — 10 s by default — so an idle hub notices too; a change is any
difference in inode, mode, size, mtime or change time). A file that no longer loads — wrong mode, a symlink,
a parse or scope error, a missing operator token — keeps the previous
credentials and logs one `hub auth reload failed` error per bad version.
A reload that would turn an `--alert-nodes` node into a service is refused
the same way (restart the hub with a new list instead). Removing a client's
lines revokes its token on the next request. A node websocket whose token
the reload no longer holds as that machine's node token — lines removed,
token rotated, or the client turned into a service — is closed
(`1008 credential revoked`) and must reconnect with a current node token.

## Mint, rotate, revoke

On the hub VM:

```sh
deploy/hub-token-mint.sh svc-mergewake ./svc-mergewake.env \
  'nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:pane.capture@*,node.command:pane.close@*,node.command:worktree.remove@*,node.command:pane.read@*,node.command:job.done@*'
```

The script generates a 256-bit random token, writes the client env file
(mode `0600`, never overwritten) and prints the auth-file lines. Add them to
the hub auth file and copy the env file to the client host. Without a scope
argument it mints a node credential.

- **Rotate:** mint a new client id, install it, then delete the old lines.
- **Revoke:** delete the client's lines.

Two-slot rotation, a client listing endpoint and hashed-at-rest tokens are
deferred.


Normal `spawn` and `work` submissions use the operator-only `POST /v1/spawns` durable operation API. A per-host low-level `spawn@machine` grant does not authorize automatic placement across hosts. The older `/v1/spawn` interface is a synchronous destination-pinned RPC surface; its in-memory waiters do not own accepted operations. See [durable spawn operations](hub-spawn-operations.md).
