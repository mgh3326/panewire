# Hub drain on SIGTERM

This page is the contract for how the hub stops and how a node comes back.
It is part of operator decision 182 (deploys that do not interrupt work),
slice R2. Callers that retry (R1) and node-side stop and settle (R3) are
separate pages and slices.

## What the hub does on SIGTERM (or SIGINT)

1. **Stop taking new work.** From the signal on, every new HTTP request except
   `GET /healthz` gets `503` with `Retry-After: 1` and this body:

   ```json
   {"error":"hub_draining","retry":true,"fix":"retry in a few seconds; the hub is restarting"}
   ```

   This covers new spawn submissions (`POST /v1/spawns`, `POST /v1/spawn`), new
   long polls (`GET /v1/inbox/wait`) and a new node or event socket
   (`/v1/agent`, `/v1/events`). Nothing is recorded for a refused request, so
   the restarted hub accepts the same request, with the same request id, as a
   first one. `/healthz` keeps its pinned body.
2. **End long polls.** A `GET /v1/inbox/wait` already blocked is answered at once
   with the same `503 hub_draining` body, instead of being held until the
   process exits.
3. **Finish work in flight.** A request that began before the signal runs to its
   answer. So does a spawn dispatch a worker already started: the node's result
   still arrives, because the node socket stays open. The spawn coordinator
   starts no new dispatch; a spawn still queued stays `accepted` in the durable
   store for the next hub.
4. **Bound the wait.** Work still running after `hubDrainTimeout` (20 s,
   `hub_drain.go`) is cut. A spawn dispatch cut here resumes on the next hub from
   its durable row and is replayed to the same node under the same request id
   ([hub-spawn-operations.md](hub-spawn-operations.md)). A second SIGTERM or
   SIGINT ends the process at once.
5. **Tell the nodes.** When the work is done, or the wait is spent, the hub sends
   each connected node `{"type":"redial"}` and closes its socket with status
   1001 (going away). Only then do the listeners close and the process exit.

### systemd

The unit must let the drain finish before systemd sends SIGKILL:

| Setting | Value | Why |
|---|---|---|
| `KillSignal` | `SIGTERM` (the default) | the drain starts on it |
| `TimeoutStopSec` | **45 s** at least | 20 s drain, 1 s node close, 2 s listener close, then the store close and flush (about 20 s of margin) |
| `Restart` | `always` | unchanged |

`KillMode=control-group` is fine for the hub: it has no detached children.

## A stale node socket

A node that sends no heartbeat for `StaleAfter` (30 s) is marked `stale` by the
sweep. At that moment the hub also wakes every spawn waiter bound to that
node's socket, as a disconnect does. The spawn operation is requeued at once
and does not wait for its deadline. If the node answers later, the replay
reaches it with the same request id.

## What a node does

- On `{"type":"redial"}` the node ends its session and dials again at once.
- When a session that was up ends for any reason, the node retries on a fast
  schedule for the next two minutes (`hubRedialFastWindow`, `hub_redial.go`):
  the first retry after a random pause of 0 to 250 ms, then a pause of 0.5 to
  1.5 s (the one-second `hubRedialFastDelay`, jittered) between tries. After
  the window the ordinary back-off (1 s doubling to 60 s) applies again.
- A node that has never connected keeps the ordinary back-off.
- A node from before this change drops the unknown `redial` message with its
  usual log line and reconnects on the socket close. A hub from before this
  change never sends it.

## Not covered here

Caller-side retry (R1), the node's own stop, settle and detached watcher (R3)
and a release that names the component that changed (R0). A caller without R1
sees the `503 hub_draining` answer once per attempt.
