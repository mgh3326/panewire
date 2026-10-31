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

Each scope names its resource. `@*` means any machine; lane scopes always
name one lane.

| Scope | Grants |
| --- | --- |
| `nodes.agents.read` | `GET /v1/nodes`, `GET /v1/nodes/agents`, `GET /v1/session-reap` |
| `node.command:<op>@<machine>` or `@*` | `POST /v1/nodes/{machine}/command` with that op (`agents.list`, `pane.capture`, `pane.close`, `worktree.inspect`, `worktree.prepare`, `worktree.remove`, `quota.refresh`) |
| `spawn@<machine>` or `@*` | `POST /v1/spawn` for that machine, and `GET /v1/spawn/{id}` for a request on it. A spawn request with `checkout` also authorizes the node's internal `worktree.prepare` preflight; no separate `node.command` grant is needed. |
| `wake@<machine>` or `@*` | `POST /v1/nodes/wake` for that machine |
| `inbox.read:<lane>` | `GET /v1/inbox`, `/v1/inbox/count`, `/v1/inbox/wait` and `POST /v1/inbox/{id}/read` with `?lane=<lane>` |
| `inbox.ack:<lane>` | `POST /v1/inbox/ack` with `?lane=<lane>` |

Every other route keeps the authentication it had: operator-only, node-only
for the websocket and a node's own lanes, CF Access where the UI and chat
accept it, and none for `GET /healthz`. A service credential satisfies none
of them. A service whose scopes do not cover a request gets
`403 {"error":"scope_forbidden"}`, so its client can report a scope error
rather than an empty result. An unknown or wrong credential still gets `401`.
On the routes whose resource is in the body (node command, spawn, wake) the
body is validated before the scope check, so a malformed request answers `400`.
`GET /v1/spawn/{id}` answers `404` for an unknown id before the scope check.

The `svc-mergewake` line for hub reap (operator decision D1: every machine):

```sh
HUB_SCOPES_svc-mergewake=nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:pane.capture@*,node.command:pane.close@*,node.command:worktree.remove@*
```

An initiating runtug service needs `spawn@<machine>` for one destination, or
`spawn@*` for every destination. The same grant authorizes POST and GET for
spawn requests, including the `worktree.prepare` preflight requested by the
optional `checkout` field. Direct calls to the node command endpoint still
need their own `node.command:<op>@<machine>` grant. For example:

```sh
HUB_SCOPES_svc-runtug=spawn@*
```

Operator authorization is unchanged. A service without a matching spawn grant
gets `403 {"error":"scope_forbidden"}`; a grant for another machine does not
authorize this request.

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
  'nodes.agents.read,node.command:agents.list@*,node.command:worktree.inspect@*,node.command:pane.capture@*,node.command:pane.close@*,node.command:worktree.remove@*'
```

The script generates a 256-bit random token, writes the client env file
(mode `0600`, never overwritten) and prints the auth-file lines. Add them to
the hub auth file and copy the env file to the client host. Without a scope
argument it mints a node credential.

- **Rotate:** mint a new client id, install it, then delete the old lines.
- **Revoke:** delete the client's lines.

Two-slot rotation, a client listing endpoint and hashed-at-rest tokens are
deferred.
