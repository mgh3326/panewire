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
`--expire-job-dirs` takes no `--lane` (a usage error): the pass has no lane
filter, so limit it with `--job`.

**Rollout order.** Set `[reap] job_dir_ttl` only after every host that reads
that hosts config runs a binary with this change. An older binary refuses
the unknown `[reap]` table: the whole config fails to load there, and that
node's sweep loop stops.

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
| path | A plain directory directly under the jobs root, named a valid job id. A symlink whose real path leaves the root, or any other symlink, is never followed. A `.expiring-*` tombstone (below) is listed and never touched. | `outside-root`, `symlink`, `invalid-id`, `unreadable`, `tombstone` |
| kind | It has `events/` (a job that ran here), or `submission.json` and no `events/` (a submitter-side dir, below), or neither and an hk record for its job (a leftover, below). Anything else — lead scratch, a consult folder — is not a job. | `not-a-job`, `events-unreadable`, `submission-unreadable`, `submission-mismatch` |
| (a) | The job is not `--keep`, and no completion watcher holds it (`watchActive`). | `protected`, `watcher-active` |
| (d) | The newest file mtime and event time under it are at least the TTL old. | `too-young` |
| (c) | In the herdr session the receipt recorded, `herdr pane list` (the full inventory, plain shells included) names no pane with the job's recorded pane id or label, and neither does `herdr agent list`. The agent list alone never proves a pane gone: a shell pane whose agent exited is only in the pane list. A job that recorded neither a pane nor a label, a receipt that names no `herdr_session` (one from before it was recorded), or a list that cannot be read keeps it. A pane list answer that carries an error (an `error` member or `"ok":false`), or any row that is not an object with a pane id (`null`, `{}`), cannot be read. Only the pane list is read that strictly; the agent list still vetoes. The agent list is decoded leniently: an `error` member is ignored, and any decoded agent with the job's pane id or label keeps the directory (`pane-live`), even beside an `error` member. `"agents": null`, a `null` row and a `{}` row read as no agent. An agent answer with no `agents` key, or malformed JSON, cannot be decoded and keeps the directory (`pane-unreadable`). | `pane-live`, `pane-unknown`, `pane-session-unknown`, `pane-unreadable` |
| (b) | The hk job record is `completed` or `lost`. `escalated` and every live state keep it. | `hk-not-terminal`, `hk-missing`, `hk-unreadable` |
| (e) | hk confirms every artifact in the directory (below). | `report-…`, `result-unconfirmed`, `evidence-unconfirmed`, `evidence-unclassified` |

hk confirms an artifact when:

- **a report** — every top-level `report*.md` (the `-rN` rounds included),
  and every report path the spawn receipt (`--report`), a completion
  (`done`/`escalate`) or the submission recorded, when it lies in the
  directory and exists: hk holds a document at the key `done` publishes the
  local bytes to, `reports/<job>/answer-<sha256>.md`, whose `sha256` equals
  the local bytes', else at the legacy key `reports/<job>/<basename>` (a
  report published before keys were content-addressed). The job record's pin
  and the job's events play no part, so a report published but never pinned
  is confirmed. A recorded path outside the directory (a
  parent job's) is not this pass's to delete and is skipped. A recorded
  path is judged by the file it really names: one that is not a string,
  not absolute, or not clean (`filepath.Clean` would change it: `..`, `.`,
  a doubled separator), and one whose symlinks resolve it to another path,
  keep the directory (`report-path-unreadable`). A missing file is resolved
  through its nearest existing parent. `link/../summary.md`, for one, names
  the file `..` reaches from where `link` points, not the cleaned
  `summary.md`. Only the resolved path is tested against the directory. A
  path recorded through the jobs root's own alias is read under the real
  root, which the pass resolves itself;
- **`findings.json` or `result.json`**: the record's `result` is not empty;
- **`pane-screen.txt`**: not confirmed. A leftover pane screen is an ordinary
  file of the dir: the pane sweep writes it only when hk did not store the
  screen, and the dir goes with the rest of its files once the other checks
  pass;
- **`evidence/**`, `final/**`**: the check `panewire artifacts sweep` makes
  before it deletes such a file — an artifact record of this job on this host
  lists the file with its local bytes and sha256, and the archive is
  drive-confirmed (or, under `local_delete_at = "uploaded"`, uploaded — for
  a final build only; evidence is mirror-required and waits for the drive).
  A file under `evidence/` or `final/` the policy gives no `evidence` or
  `final-build` class — a jobs root the policy's `herdr-inbox/jobs` root
  does not match, such as `/srv/jobs` — keeps the directory
  (`evidence-unclassified`), since its mirror requirement is unknown.

### Leftovers without `events/`

A dir with neither `events/` nor `submission.json` is a leftover when the job's
ledger is gone (hk 3685). It goes through this gate like any other dir, with
two differences. The hk job record is what proves the job ended: it must be
`completed` or `lost` (condition (b)); a dir hk holds no record for is
`not-a-job`, and a dir whose record read fails is `hk-unreadable`. And the
dir has no receipt, so its pane is taken from the record's `pane_id` and
`label` (hk 3700), with the herdr session this node runs in. The pane check (c)
then applies as for any job. A leftover whose record names no pane is kept as
`pane-unknown`, because its absence cannot be shown. The TTL age, the watcher
check (a), the `.panewire-operation.lock`, the tombstone and the dry run are
the same as for any other dir.

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

The receipt's `--report` is a recorded report path like any other. Its
`--report-relative` is relative to the jobs root of the node that ran the
job, and the same layout here names this directory: the pass records the
clean jobs root, `/` and the value as written, and judges that path as
above. It is never cleaned first, so a value that is absolute or not clean
keeps the directory (`report-path-unreadable`) at its place in the gate
order: a `.` or `..` segment, an empty segment (`s1//summary.md`), or a
trailing `/`. The check is by path segment, so `s1/report..md` and
`s1/.hidden/summary.md` are ordinary paths, and no hub charset or Windows
separator rule applies: `s1/sub\summary.md` names a file whose name holds a
backslash. An empty value records nothing. The locked re-check reads the
receipt again through the same code.

### Directories that never expire

Some directories fail the gate on every pass, so the TTL never deletes
them; the operator decides:

- A receipt that records a label but no `herdr_session` (a headless
  receipt, or one from before the session was recorded): `pane-session-unknown`.
- A receipt that records neither a pane nor a label: `pane-unknown`.
- A recorded report path that goes through a symlink other than the jobs
  root's own alias: `report-path-unreadable`. On macOS, for one, a path
  recorded under `/tmp` when the jobs root is configured by its real path
  under `/private/tmp`. A report recorded under the jobs root's own alias
  is still accepted.

A submitter-side directory skips the pane check, so the first two never
apply to it.

## Deletion

Right before a delete the pass takes the job's `.panewire-operation.lock`
(the lock `panewire done` holds) without waiting, decides the whole gate
again with fresh herdr and hk reads. A held lock keeps the directory
(`locked`). Then, with every read done:

1. It takes a fresh inventory — file count, bytes, newest mtime and the set
   of relative paths — and compares it to the scan's. A change keeps the
   directory (`changed-during-pass`).
2. It renames the directory, in one atomic rename on the same filesystem, to
   a tombstone in the jobs root: `.expiring-<job>-<unixnano>`. A leading dot
   is never a valid job id, so nothing reads a tombstone as a job.
3. It takes the tombstone's inventory. If it differs from step 1's, the
   tombstone is renamed back to the job's name and the directory is kept
   (`changed-during-pass`). If that rename back fails, the tombstone stays
   where it is, never deleted; the pass logs it at error, records a
   `job_dir_expired` fleet event with outcome `tombstone_left`, and keeps
   (`tombstone-left`).
4. Only then does `os.RemoveAll` remove the tombstone, without following a
   symlink.

After the rename, the job's name no longer exists: a writer that opens a
file under it fails, while a writer that first runs `MkdirAll` recreates a
fresh, empty directory under that name. That fresh directory is not
deleted; a later pass judges it as any other. The one exception is step 3:
when the tombstone changed and any directory exists under the job's name,
empty or not, the rename back fails (Go's `os.Rename` refuses an existing
directory target), so the tombstone stays as `tombstone-left` for the
operator and nothing is deleted. A
writer that wrote before the rename shows in the tombstone's inventory.
**Residual
window:** a writer holding a file descriptor it opened in the directory
before the rename can still write into the tombstone after step 3, and that
write is deleted with it.

A pass lists any `.expiring-*` tombstone it finds as `keep reason=tombstone`.
It never deletes one and never renames one back; the operator decides.
`panewire artifacts sweep` and `panewire lanes-missing` skip a tombstone: it
is not a job.

Each delete is logged (`job dir expired`, with the job, reason and bytes) and
recorded as a `job_dir_expired` [fleet event](observability.md) in the state
directory, never inside the deleted directory.
