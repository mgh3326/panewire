# Checkout pull

The hub orders every connected node to fast-forward one git checkout it is
allowed to touch, and records the outcome in hk. Operators run it with
`panewire fleet checkout-pull`; the node op is `checkout.pull`
([node commands](node-commands.md#ops)). This replaces hand-pulling the rstack
checkout on each host after a merge.

## Allow-list

A node acts only on checkouts its own `spawn.json` names under `checkouts`, an
object from name to absolute path:

```json
{"checkouts": {"rstack": "/srv/checkouts/rstack"}}
```

Without a `checkouts` key the list is `rstack` → `~/.agents/rstack`. A name must
match `^[a-z0-9][a-z0-9._-]{0,63}$` and a path must be absolute; an entry that
fails either refuses the whole spawn.json (`checkouts_invalid`), as an invalid
`cwd_map` does, so the mistake is visible. The key is part of the one strict
spawn.json schema (`hubSpawnConfig`), so a key the schema does not declare still
fails `config_json_invalid`. When spawn.json does not load, checkout.pull has
no allow-list and refuses every name (fail closed); a spawn.json that is absent
gives the default. An order's `checkout` is only ever a name: no path, no
command, and nothing outside this list is reachable.

## Node op: `checkout.pull`

`args: {checkout, rev}`. `rev` is a hex revision (7 to 40 characters) or `""`
for no target. Both fields are required.

Steps, in order. A refusal before the pull changes nothing. A refusal at the
pull reports the state it found: unchanged for `not-fast-forward`, and
`pull-interrupted` when the pull may have moved HEAD or the worktree:

1. `checkout` is not on the allow-list: refused `not-allow-listed`.
2. The path is missing, a symlink, or not the top of a git worktree: refused
   `not-a-checkout`.
3. The per-clone git-op lock (shared with worktree ops) is held past its wait:
   refused `busy`.
4. HEAD is detached: refused `detached`. HEAD has no upstream: refused
   `no-upstream`.
5. `git status --porcelain --untracked-files=normal` is not empty: refused
   `dirty`.
6. `git fetch --quiet`, then `git pull --ff-only --quiet`. A failed pull is
   read back before it is named: if HEAD moved or the worktree is dirty, the
   pull was interrupted mid-apply and is refused `pull-interrupted`, with `to`
   set to the HEAD found; the operator must inspect it. If HEAD cannot be read
   back after a pull that succeeded, the result is also `pull-interrupted`, with
   no `to`. No other refusal carries `to`. An unchanged pull that
   cannot fast-forward is refused `not-fast-forward`; any other unchanged pull
   failure is `pull-failed`. A failed fetch is `fetch-failed`.
7. If HEAD moved and the pulled range changed a file under `install/` or
   `bin/`, the checkout's `install.sh` runs with no arguments, no shell, the
   node user's environment, and a 2-minute bound. A missing or non-executable
   `install.sh` is `install-missing`; a run past the bound is killed and reports
   `install_exit: -1` with `install-timeout`.
8. With a `rev`, the result says whether HEAD contains it (`target_reached`).

Every git call is bounded (fetch 90 s, pull and other steps 60 s or less) and
runs with `GIT_TERMINAL_PROMPT=0`. Each git call and `install.sh` runs in its
own process group, which is killed whole when its bound ends, so no child
outlives the bound. The whole op is bounded by 4 minutes.

Result:

```json
{"outcome":"pulled","checkout":"rstack","from":"<40 hex>","to":"<40 hex>",
 "install_ran":true,"install_exit":0,"target":"<rev or empty>","target_reached":true}
```

`outcome` is `pulled` (HEAD moved), `current` (already at `to`) or `refused`
with a `reason` from the list above. `install_ran` is always present;
`install_exit` is `0` when install did not run.

## Heartbeat

A node reports each allow-listed checkout's revision in its heartbeat:

```json
"checkouts": {"rstack": {"sha": "abc1234", "branch": "main"}}
```

`sha` is `git rev-parse --short HEAD`; `branch` is empty when HEAD is detached.
At most 8 entries. A checkout that cannot be read is left out. The node sends
the field only on a connection whose hub listed `checkout_pull` in
`hub.features`, because an older hub rejects an unknown heartbeat field.

The hub keeps the last heartbeat's value per node and shows it in `GET
/v1/nodes` (`checkouts`) and `panewire hub-status` (`CHECKOUTS` column).

## Hub order: `POST /v1/fleet/checkout-pull`

Body: `{"checkout": "rstack", "rev": "<optional rev>"}`. Operator or service
credentials; a service needs `node.command:checkout.pull@<machine>` for each
machine it should reach. A node credential is refused.

The hub sends `checkout.pull` to every known node at once and waits up to 5
minutes for all of them together. A node that is not connected is listed with
`state` and `error: "node_unavailable"` and is not waited for. A node that is
not answered in time is `error: "node_timeout"`. A node the caller may not
command is `error: "scope_forbidden"`.

Response (200, one row per known node, sorted by machine):

```json
{"request_id":"cmd-…","checkout":"rstack","rev":"","record":{"key":"deploy/rstack/2026-11-13-<rev>","outcome":"written"},
 "hosts":[{"machine":"machine-a","state":"connected","result":{"outcome":"pulled", "…":"…"}},
          {"machine":"machine-b","state":"disconnected","error":"node_unavailable"}]}
```

`checkout.pull` is not accepted on the generic `POST /v1/nodes/{machine}/command`
endpoint, which would skip the record. A malformed body or unknown field is 400.

## Deploy record

Each order writes exactly one hk document through the hub's handoffkeep client
(`PUT /v1/documents/<key>`, kind `deploy`). Key:

    deploy/<checkout>/<YYYY-MM-DD>[-<rev>]-<order id>

`<rev>` is the order's `rev`. With no `rev` it is the first host's reached
revision (machine order) and is left out when no host reached one. `<order id>`
is the order's `request_id`, so every order has its own document, even when
two orders share a checkout, a revision and a day. The body is JSON:

```json
{"kind":"checkout-pull","checkout":"rstack","rev":"","request_id":"cmd-…",
 "at":"2026-11-13T02:00:00Z","hosts":[ … the response rows … ]}
```

It carries no token and no request body. `record.outcome` is `written`,
`failed` (hk refused or was unreachable; the host rows still stand), or
`unconfigured` (the hub has no handoffkeep client).

## CLI

    panewire fleet checkout-pull --hub-url <url> --hub-token-env <operator env> \
        [--hub-cf-env <file>] [--checkout rstack] [--rev SHA] [--json]

Prints one row per host and a `record:` line. Exit `0` only when every host
pulled or was current with no refusal and no failed install, the record was
written, and, with `--rev`, every host reached it. Otherwise exit `7`
(`ExitPartial`).

## Out of scope

Triggering the order from GitHub Actions in the rstack repo (decision 223
"when") is the rstack design's job. Binary updates by the hub and `rollout.sh`
are not part of this op.
