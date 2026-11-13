# Job event index

The node daemon reads each job's event files (`<inbox>/jobs/<job>/events/*.json`)
and the lane-event directory (`<inbox>/events-lane`) on its hot paths. Before
the index (hk 4537) every one of those passes walked every job directory and
re-read and re-parsed every event file: on a node with about 2,000 retained job
directories that kept the daemon at 100 % CPU, and local CLI calls answered
"daemon unavailable". The index is `hubInboxIndex` in `hub_jobs_index.go`.

## What reads through it

| Path | Entry point |
| --- | --- |
| Heartbeat's active-job list | `scanHubActiveJobsForLimit` (`hub_jobs_client.go`) |
| Relay outbox scan (job events and lane events) | `scanHubRelayEventsWithin`, `scanHubLaneEventsWithin` |
| Quota stop's pane-to-job match (also the idle-wake materialisation) | `(*quotaStopManager).jobForPane` |
| Pane-to-herdr-session match (node commands, busy relay) | `nodeJobHerdrBindingByPaneFromRoot` |

The answers are the same as the full scans': the same events, the same job per
pane, the same heartbeat and relay bytes. `hub_jobs_legacy_scan_test.go` keeps
the pre-index scans verbatim, and `TestHubInboxIndexMatchesFullScan` compares
the two byte for byte on 2,000 job directories before and after every kind of
change to the inbox.

Not covered: `scanStallJobs` and the stall detector's own scan
(`stall_detect.go`), `scanHubCompletedJobs` (no daemon caller), and the fleet
package's per-job walks (`internal/fleet`). They were not in the hot samples.

## Refresh rule

An event file is written once, by create or rename, so a job's events can change
only when its events directory changes. The index holds, per job, the
directory's `os.FileInfo` and what was parsed from it, and each pass:

1. `stat`s the jobs directory; the list of job directories is re-read only when
   that stat changed.
2. `stat`s each job's events directory. If it is the same directory with the
   same mtime as the last read, the retained record is used. Otherwise the
   directory is read again, in full, and the record replaced.
3. The lane-event directory is handled the same way; after a change only new or
   modified files are parsed, a file being keyed by name, mtime and size.

A read taken within 2 s of the directory's mtime (`hubIndexRacyWindow`) is not
trusted: a write in the same timestamp tick would leave the mtime unchanged, so
that directory is read again on the next pass until its mtime is older than the
window. This is also what makes whole-second filesystems safe. A pass is
therefore one `stat` per job directory plus a read of the directories that
changed, not a read of every event file.

Writers that rewrite an event file in place long after creating it would not be
seen. None does: `panewire emit`, the completion and close writers in `job.go`
and `job_close.go`, and `writeHubRevocation` publish by link or rename, and a
create-then-write publish finishes inside the racy window above, which keeps
the directory being read again meanwhile.

A read that fails (an event file that cannot be opened, or whose stat fails) says
nothing about the file, unlike a record that is present but invalid. Such a
directory is never treated as final and is read again on the next pass; a lane
event whose read failed is not kept and is read again on the next pass.

Time-dependent decisions are never cached. What the index keeps is the parsed
events and their time-independent reduction; the active-window, stale-age,
relay-mtime and lifecycle-settle checks use the clock of each pass, so changing
`PANEWIRE_JOB_ACTIVE_MAX_AGE`, `PANEWIRE_JOB_STALE_AGE` or the relay age takes
effect at once.

## Dormant jobs

A job whose newest event is older than the active window
(`PANEWIRE_JOB_ACTIVE_MAX_AGE`, 72 h) can contribute nothing to the active-job
scan or the relay scan: its claim and every event fail the window. Its parsed
events are dropped; it keeps its directory stat and the few fields the pane
lookup needs, which has no age window. A dormant job costs one `stat` per pass.
If the window is later widened over it, or its directory changes, it is read
again. Job directories are never deleted or moved by the index; expiry stays
with the job dir TTL ([job dir TTL](job-dir-ttl.md)).

## Bounds

- One index per inbox root, at most `hubIndexMaxRoots` (8) at a time; a daemon
  has one, the rest is test fixtures.
- Parsed data is held to `hubIndexByteBudget` (48 MiB) per root. Over it, data
  is served for the pass that read it and read again the next time, which is the
  pre-index cost for that data and no worse.
- A lane-event directory of more than `hubIndexMaxLaneFiles` (200,000) files is
  not retained.
- Each index is guarded by one mutex; concurrent callers share a refresh.
  `TestHubInboxIndexConcurrentUse` runs under `-race`.

## Measured

`BenchmarkNodeHotPathJobScan` (`hub_jobs_index_bench_test.go`) times the
daemon's own entry points on 3,000 retained job directories on a quiet node:
about 60 % ended past every window, the rest live, recently ended or one of the
special shapes the equivalence test covers. It uses only existing entry
points, so the same file ran on the commit before the index (4590274b). Apple
M1 Pro, `-benchtime=5x` before, `-benchtime=100x` after, steady state (the pass
that fills the index is not timed):

| Pass | Before | After | Drop |
| --- | --- | --- | --- |
| `heartbeatEvent` | 524 ms | 13.9 ms | 38× |
| relay outbox (`jobCompletionEvents`) | 318 ms | 15.3 ms | 21× |
| `jobForPane` | 532 ms | 10.0 ms | 53× |
| pane-to-session lookup | 299 ms | 9.1 ms | 33× |
| heartbeat + relay outbox + `jobForPane` | 1,747 ms | 34.8 ms | 50× |

What is left is the one `stat` per job directory (about 3 µs each, in the
kernel). Run it with
`go test -run '^$' -bench NodeHotPathJobScan -benchtime 100x .`.
