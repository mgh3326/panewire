# Closing a job

```sh
panewire job close JOB --lane LANE --outcome completed|abandoned|superseded --reason TEXT [--operator-override]
```

The owner lane declares a job ended when its worker did not write a terminal
record. The command writes one `job.revoked` record. It does not close the
pane; the reaper may close it after its grace period, measured from the record's
`created_at`.

When the local events directory exists, close uses it directly. Otherwise the
local daemon forwards the request using its node credential. The hub checks
its job record and asks the node that owns the job to write the record. The
owning node checks its local events again and returns the result before the CLI
prints `OK`. A hub-routed record has source `panewire job close via hub`; the
local source remains `panewire job close`. If the hub does not know the job,
the lane does not match, or the owning node is disconnected, the command
refuses with `job_unknown`, `owner_mismatch`, or `node_offline` respectively.
A daemon with a hub connection and a connected owning node are required.

`--lane` must match the newest claim's `owner_lane` byte for byte. An unreadable
claim, a claim without an owner, or a terminal event after the latest claim
refuses the local write. `--operator-override` permits a different lane and
records `override: operator`; `closed_by` still names the caller. The record
contains `kind`, `job_id`, `owner_lane`, `closed_by`, `outcome`, `reason`,
optional `override`, `source`, `host`, `created_at`, and `epoch`. It has no
pane or tab ID. The check, sequence choice and no-clobber publication run under
`.wrk-events.lock`.

The node relays the terminal record to the hub. A role reservation remains
held while its pane is present; the existing release path can free it after
the pane is gone. Closing a job does not bypass that check.
