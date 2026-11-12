# Hub calls that ride through a restart

A hub restart should not fail a caller. This page is the contract for the
calls that retry through one. The design is hk:doc
`design/2026-11-13/panewire-uninterrupted-deploys`, rung R1 (operator decision
182); this slice is hk task 4362.

## What retries

A call retries only when it is idempotent. Repeating it must leave the hub where
one send would. For each retried answer the budget is `hubretry.Budget`, 60
seconds, with backoff from 250 ms doubling to 5 s. The retry is for:

- a **transport error**: a refused or reset connection, a connection that ended
  early, or an attempt that timed out;
- a **502** or a **503**.

Anything else is an answer, and is returned as it is: a 4xx, a 500, a redirect
the client refused, a rejected request.

| Caller | Retried calls | Sent once |
|---|---|---|
| `panewire inbox` (CLI, direct or through the node daemon) | list, read, ack, count, status, and the wait's long poll | none: every inbox command is idempotent |
| `panewire spawn` | the submit (`POST /v1/spawns`, keyed by its request id), the spawn-status reads that `--wait` follows | `POST /v1/spawn`, which is not keyed |
| node daemon → hub (`nodeHubClient`) | every GET, PUT and DELETE; `POST /v1/inbox/ack` and `POST /v1/inbox/{id}/read` | any other POST: job close and reap, node command, wake, burst request |

The daemon's inbox path turns its node-level retry off and lets the inbox
command retry, so one command makes one budget, not two.

## What the caller sees

- A call that succeeds after retries returns its answer as an unretried call
  would.
- A call whose budget ends returns the last answer, so the exit code and the
  message are the ones an unretried call gets (`inbox: hub unavailable`, or
  `inbox: hub returned HTTP 503`).
- A CLI that retried prints one line to stderr, after the command's answer
  lines, and only when it retried:

  ```
  hub: retried 3 times, waited 4.5s
  ```

  The line names the count and the total wait. Stdout is unchanged.
- A wait's retries never run past its `--timeout`.

## Test seams

`hubretry.Policy` carries the budget. The zero value is the production policy.
A negative `Budget` sends once, for fixtures that count exact sends. The CLI
takes it through `hubCLIDeps.HubRetry`, the node client through its `retry`
field, and the fleet transport through `hubHTTP.retry`.

## Where it lives

- `internal/hubretry/hubretry.go`: the retry loop, the classification, the
  stats line.
- `inbox_retry.go`, `hub_node_client.go`: the inbox and node-client wiring.
- `internal/fleet/hub_transport.go` (`doIdempotent`), `internal/fleet/submission.go`:
  the spawn wiring.
