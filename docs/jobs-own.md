# A lane's own jobs

```sh
panewire jobs mine --lane LANE [--json]
panewire jobs close --lane LANE --job JOB [--force]
```

A session that spawns panes on other hosts can list and close the jobs it
owns there without the operator token. Both commands reach the hub through the
local node daemon and its node credential, the way `panewire inbox` does; the
direct form takes `--hub-url` and `--hub-token-env` (a node credential file)
plus the optional `--hub-cf-env`. No new credential file exists.

`jobs mine` prints one row per job the lane owns on any machine:
`JOB_ID MACHINE PANE STATE TASK LAST_SEEN`. `STATE` is `active`, `completed`
or `orphaned`, the operator console's vocabulary. `TASK` is the task the hub's
spawn operation named; a job this hub did not spawn through its own operation
store lists none. `LAST_SEEN` is the hub's last observation of the job. Rows
are newest first and capped at 200. `--json` prints the hub's answer,
`{"jobs":[...]}`; `jobs close --json` prints its result object the same way.

`jobs close` asks the machine that runs the job to close its pane and retires
the lane route that pointed at it. It does not write a terminal record; use
[`panewire job close`](job-close.md) for that, and the reaper or this command
for the pane.

## Authorization

The hub decides, and every refusal names its reason.

| Caller | Rule |
| --- | --- |
| operator credential | any lane |
| node credential | the lane must be routed to the caller's own machine, the rule its inbox uses. Otherwise `403 {"error":"lane_not_routed_to_caller"}`. |
| service credential | refused: `403 {"error":"scope_forbidden"}`. No scope grants these routes. |

`jobs close` also requires the job's owner lane to be the named lane: the hub
answers `403 {"error":"owner_mismatch"}` for a job another lane owns, and
`404 {"error":"job_unknown"}` for a job it does not know. The node re-checks
the owner lane against its own ledger before it closes anything.

## What close does

The hub sends the job's machine the `job.reap` node command
([node-commands.md](node-commands.md)), which re-reads the job's ledger and
herdr on that node:

- the pane is the one the job's own `job.spawned` receipt names, and the herdr
  row for that pane id must still carry the receipt's label. A pane id is
  host-local and reused, so a different label means the pane is not the job's
  (`pane-changed`).
- a job spawned with `--keep` is never closed (`protected`), with or without
  `--force`.
- a pane that is `working`, or `blocked` at a prompt, is refused without
  `--force` (`job-working`, `pane-blocked`) and is reported, so the caller
  knows the job is still running.
- otherwise the close takes `pane.close`'s settle: the finished status and an
  empty composer must still hold one interval later, with an unchanged
  `state_change_seq` where herdr reports one. Draft text, an unknown composer
  or a failed screen read refuses the close (`pane-changed`).

`--force` skips the status and composer checks; it never skips the label check
or the keep flag.

On `closed` or `pane-gone` the hub drops the lane rows that pointed at the pane
the node named, under the same rules as a `pane.close` release: the row must be
that machine's own, not a sink, not protected, not referenced by another
machine, and not older than the command. The caller's own lane points at
another pane, so it stays. Only a pane the node verified is released: a job
whose ledger names no pane proves nothing about the pane the hub's record
names — that id may have been reused by a live unrelated pane since — so a
`pane-gone` with no pane retires nothing. The hub logs one line per close with
the caller, lane, job, machine, pane and outcome.

| Result | Meaning |
| --- | --- |
| `OK job=… machine=… pane=… outcome=closed released=<lane>` | The pane was closed; the named lane rows were retired. |
| `OK … outcome=pane-gone pane=<pane>` | The node verified that pane is gone; the lane rows pointing at it were retired. |
| `OK … outcome=pane-gone pane=-` | The job's ledger names no pane, so nothing was verified and nothing was retired. |
| `jobs close refused: … (job-working)` | The pane is still working; use `--force` to close it anyway. |
| `jobs close refused: … (pane-blocked)` | The pane waits at a prompt; use `--force`. |
| `jobs close refused: … (protected)` | The job was spawned with `--keep`. |
| `jobs close refused: … (pane-changed)` | The pane is no longer the job's, or its composer is not proven empty. |
| `jobs unavailable: … (close_unconfirmed)` | The node did not answer, or the command could not be written to it and may still have arrived; the pane may already be closed. Inspect it before retrying. |

## Rollout

The hub and the nodes deploy in either order, and neither misreads the other:

- **Nodes before the hub** is the order that closes jobs end to end. A hub
  before this change has no `/v1/jobs/mine` or `/v1/jobs/reap`, so both
  commands answer `404` and the CLI reports
  `jobs close refused: the hub does not serve this command; upgrade the hub
  (hub_route_missing)`.
- **An old node** does not know `job.reap`; it answers `invalid_args` without
  acting, and the hub reports `jobs unavailable: the job's node does not know
  job.reap; upgrade the nodes before the hub (invalid_args)`. Nothing is
  closed, and no lane row is retired.
- No existing message changed. The heartbeat's active-job set and every other
  node-command shape are untouched; `job.reap` is a new op, and a node that
  does not know it refuses it by name.

## Limits

The node compares the pane's herdr label, not its tab: herdr's agent list
carries no tab id, so a tab mismatch cannot be seen over the hub. A job whose
ledger names no pane has nothing to close and nothing the hub may retire
(`pane-gone`, no pane); check the machine's own `panewire reap` for it.
`jobs close` does not remove worktrees or release quota; run `panewire reap` on
the machine for those.
