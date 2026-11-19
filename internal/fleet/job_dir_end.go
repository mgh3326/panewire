package fleet

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// The at-end job dir delete (hk 3704, docs/job-dir-ttl.md "At job end"). A
// job whose end the node records (job.reaped, written by the task pane sweep
// in reapSweptPaneHeld) has its dir deleted without waiting for the TTL, once
// the TTL gate says it may go and the relay owes the hub nothing for it.
//
// It is not a second delete path: ExpireEnded runs the TTL pass's decide and
// expire, with the age wait off and two more gates on (the job's own
// job.reaped, and no relay row owed). The end is queued when job.reaped is
// written, and each sweep pass retries the queue. The retry is needed: the
// job.reaped event is itself a relay row, so the first attempt almost always
// finds it still owed to the hub. A queue lost to a restart leaves the dir to
// the TTL pass, the safety net job_dir_ttl.go keeps.

// ExpireEnded deletes job id's dir when the at-end gate lets it go, and returns
// the gate's reason ("confirmed" when deleted). It reads RelayOwed and
// Exclusive from p; the caller holds the job's end, the pane close or the reap.
func (p *JobDirTTL) ExpireEnded(ctx context.Context, a *App, id string) string {
	root, err := filepath.EvalSymlinks(a.Jobs)
	if err != nil {
		return "unreadable"
	}
	end := &JobDirTTL{RelayOwed: p.RelayOwed, Exclusive: p.Exclusive, atEnd: true}
	v := end.decide(ctx, a, root, id, jobDirPanes{})
	if !v.remove {
		return v.reason
	}
	return end.expire(ctx, a, root, id, v).reason
}

// queueEndedJobDir records that job id ended at at, so its dir is a candidate
// for the at-end delete. A sweeper with no RelayOwed reader queues nothing:
// without the relay gate the at-end delete must not run, and the TTL pass
// keeps the dir.
func (s *TaskPaneSweeper) queueEndedJobDir(id string, at time.Time) {
	if s.RelayOwed == nil {
		return
	}
	if s.endedDirs == nil {
		s.endedDirs = map[string]time.Time{}
	}
	if _, queued := s.endedDirs[id]; !queued {
		s.endedDirs[id] = at
	}
}

// expireEndedJobDirs makes one at-end attempt per queued dir and drops a
// dir once it is deleted, gone, or older than MinJobDirTTL, the relay hold
// ceiling: past that the TTL pass owns the dir. Each attempt takes Exclusive,
// as an expiry does; a pass holds no node command lock here.
func (s *TaskPaneSweeper) expireEndedJobDirs(ctx context.Context, a *App) {
	for id, at := range s.endedDirs {
		if a.Now().Sub(at) >= MinJobDirTTL {
			delete(s.endedDirs, id)
			continue
		}
		if _, err := os.Lstat(filepath.Join(a.Jobs, id)); errors.Is(err, fs.ErrNotExist) {
			delete(s.endedDirs, id)
			continue
		}
		expiry := &JobDirTTL{RelayOwed: s.RelayOwed, Exclusive: s.Exclusive}
		if expiry.ExpireEnded(ctx, a, id) == "confirmed" {
			delete(s.endedDirs, id)
			s.info("job dir deleted at job end", "job", id)
		}
	}
}
