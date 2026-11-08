# Job dir TTL

A job directory under `~/work/herdr-inbox/jobs/<job>` is scratch once hk holds
the job's end and everything the directory recorded. The job dir TTL deletes
such a directory after nothing has written to it for the TTL (72 h by
default). It is the pass in `internal/fleet/job_dir_ttl.go` (hk 3624).

## The contract it keeps: a job dir means a fleet job

mergewake's reap (`internal/reap/reap.go`) and the node's `pane.capture`
(`node_command.go`) treat a pane whose job directory is missing as *not a
fleet job*, and then never close that pane. So the TTL never deletes the
directory of a job whose pane may still exist: condition (c) below waits until
herdr lists no pane of the job, and any doubt about the pane keeps the
directory. With that gate neither mergewake nor the node needs a change.

## Opt-ins

The pass is a dry run unless one of these asks it to delete:

| Where | How | Default |
| --- | --- | --- |
| CLI | `panewire reap --apply --expire-job-dirs [--job ID[,ID]]` | off; a plain `reap --apply` never deletes a dir |
| Node daemon | `[reap] job_dir_ttl = "72h"` in the node's hosts config ([config](config.md#reap)) | unset: the sweep loop runs no TTL pass |

`panewire reap --expire-job-dirs` without `--apply` is the dry run. The CLI
uses `[reap] job_dir_ttl` when it is set, else 72 h. The node runs the pass
after its [task pane sweep](pane-sweep.md), at most once an hour, and only
while the key is set, so a deploy changes nothing until panewire-design sets
it. A value under 24 h, or one that does not parse, is a config error.

## Dry-run output

One line per directory, then a summary:

```text
job=j1 action=delete reason=confirmed bytes=1503
job=scratch action=keep reason=not-a-job bytes=13
job-dir-ttl apply=false ttl=72h0m0s delete=1 keep=1 bytes=1503 reasons=confirmed:1,not-a-job:1
```

`bytes` on the summary is what the deletes free (or, in a dry run, would
free). An apply pass prints the same lines; a directory the locked re-check
keeps shows the re-check's reason.

## The delete gate

Every condition is required; the first that fails names the reason the
directory is kept. Any read error or doubt keeps it.

| | Condition | Kept as |
| --- | --- | --- |
| path | A plain directory directly under the jobs root, named a valid job id. A symlink whose real path leaves the root, or any other symlink, is never followed. | `outside-root`, `symlink`, `invalid-id`, `unreadable` |
| kind | It has `events/` (a job that ran here), or `submission.json` and no `events/` (a submitter-side dir, below). Anything else — lead scratch, a consult folder — is not a job. | `not-a-job`, `events-unreadable`, `submission-unreadable`, `submission-mismatch` |
| (a) | The job is not `--keep`, and no completion watcher holds it (`watchActive`). | `protected`, `watcher-active` |
| (d) | The newest file mtime and event time under it are at least the TTL old. | `too-young` |
| (c) | herdr lists no pane with the job's recorded pane id or label, in the job's own herdr session. A job that recorded neither, or a herdr that cannot be read, keeps it. | `pane-live`, `pane-unknown`, `pane-unreadable` |
| (b) | The hk job record is `completed` or `lost`. `escalated` and every live state keep it. | `hk-not-terminal`, `hk-missing`, `hk-unreadable` |
| (e) | hk confirms every artifact in the directory (below). | `report-…`, `result-unconfirmed`, `screen-…`, `evidence-unconfirmed` |

hk confirms an artifact when:

- **a report** (`report*.md`, the `-rN` rounds included): the record names a
  `report_doc`, and the sha256 of the local bytes equals the record's
  `report_sha256` for that document, else the `sha256` hk holds for
  `reports/<job>/<file>`;
- **`findings.json` or `result.json`**: the record's `result` is not empty;
- **`pane-screen.txt`**: the document `screens/<job>/pane-screen.txt` exists,
  and, for a screen of at most 512 KiB, its sha256 matches. A larger screen
  was stored cut to hk's cap, so the cut copy is accepted and the line says
  `note=screen-cut`;
- **`evidence/**`, `final/**`**: the check `panewire artifacts sweep` makes
  before it deletes such a file — an artifact record of this job on this host
  lists the file with its local bytes and sha256, and the archive is
  drive-confirmed (or uploaded, under `local_delete_at = "uploaded"`).

### Submitter-side dirs

A host that submits a spawn to the hub keeps `brief.md`, `spawn.out` and
`submission.json` under the job id, while the job runs, and keeps its events
and pane, on another node. Such a directory goes when the hk record of the
submission's job is `completed` or `lost`, or when hk never held the job and
the hub spawn operation (`GET /v1/spawns/<request>`) ended `failed`; in both
cases it must be at least the TTL old and pass (a) and (e). A hub that cannot
be read keeps it (`spawn-status-unreadable`). Its pane is not checked here:
it lives on the executing node, whose own directory for the job keeps the
contract.

## Deletion

Right before a delete the pass takes the job's `.panewire-operation.lock`
(the lock `panewire done` holds) without waiting, decides the whole gate
again with fresh herdr and hk reads, and deletes only when it still passes
and the directory's file count, bytes and newest mtime are those the scan
saw. A held lock keeps the directory (`locked`); a change keeps it
(`changed-during-pass`). The path is checked again, and `os.RemoveAll`
removes the directory without following a symlink.

Each delete is logged (`job dir expired`, with the job, reason and bytes) and
recorded as a `job_dir_expired` [fleet event](observability.md) in the state
directory, never inside the deleted directory.
